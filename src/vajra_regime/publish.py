"""Publish the working store into ``D:\\VAJRA_DATA`` - the clean, hand-it-over folder.

Design rules, in priority order:

1. **Fail closed.** Every write is atomic (temp file + ``os.replace``). If anything in this
   module raises, the previously published dataset is left exactly as it was. A half-written
   dataset is worse than a stale one, because a stale one is obvious and a half-written one
   is not.
2. **Never overwrite newer with older.** If the published dataset already carries a later
   session than the store, the publish is refused rather than silently rewinding history.
3. **One direction only.** Nothing in the build pipeline reads from the published folder.
   Data flows store -> published, never back.
4. **Parquet and CSV must be the same data.** Not "similar": the same rows and the same
   values, asserted with a full multiset comparison, every year, every run.

The published folder contains data and documentation and nothing else - no code, no logs,
no temp files, no hidden folders.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file

PUBLISH_VERSION = "VAJRA_DATA_V1"

# Bump this whenever the published columns change. A year is only reused if its stamp
# matches: without that, adding a column silently leaves every past year on the old
# schema, because the reuse test only looks at whether the SOURCE changed.
PUBLISH_FORMAT_VERSION = "2026-08-30-tradability-columns"

# The first date on which official NIFTY 500 constituent evidence exists. Before this the
# membership panel is reconstructed by reversing later official index changes: the prices are
# real, the membership is an estimate. This single number drives every year label.
OFFICIAL_MEMBERSHIP_ANCHOR = date(2013, 4, 18)

# The VAJRA 750 is a mechanical monthly liquidity rule, not an index. It needs 252 sessions of
# warm-up, so its first rebalance is 2010-08-31.
VAJRA750_FIRST_REBALANCE = date(2010, 8, 31)

N500_DEFINITION = (
    "Official NSE NIFTY 500 index membership, point-in-time, joined to NSE bhavcopy OHLCV "
    "adjusted for splits, bonuses and face-value changes."
)
N750_DEFINITION = (
    "VAJRA 750: NOT an NSE index. A monthly top-750-by-60-day-median-turnover universe "
    "computed from point-in-time data, plus the supporting adjusted price master for every "
    "security that was ever selected."
)

# Published as NULL rather than the empty string the store uses, so that Parquet and CSV
# round-trip to identical values. Every other text column already uses NULL.
EMPTY_STRING_COLUMNS = ("CorporateActionQuarantineReason",)

# Why a year can legitimately have no CSV. See _write_year.
CSV_DELETED_REASON = (
    "CSV_REMOVED_BY_OPERATOR_TO_SAVE_DISK_NOT_BACKFILLED - the Parquet for this year is complete; "
    "the CSV mirror was deleted and is deliberately not recreated. Read the Parquet, or delete "
    "the Parquet too and the next run will rebuild both."
)


# --------------------------------------------------------------------------- atomic writes


def _atomic_text(path: Path, text: str) -> bool:
    """Write ``text``; return True if the file actually changed."""
    encoded = text.encode("utf-8")
    if path.is_file() and path.read_bytes() == encoded:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    with temporary.open("wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return True


def _is_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    attributes = getattr(path.lstat(), "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _assert_no_reparse(root: Path) -> None:
    """A junction would make the "physically independent copy" claim a lie."""
    if _is_reparse(root):
        raise RuntimeError(f"Published root is a reparse point: {root}")
    for path in root.rglob("*"):
        if _is_reparse(path):
            raise RuntimeError(f"Symlink/junction prohibited in the published dataset: {path}")


def _cleanup_orphan_partials(root: Path) -> list[str]:
    removed: list[str] = []
    if not root.exists():
        return removed
    for path in root.rglob("*.partial"):
        if path.is_file() and not _is_reparse(path):
            path.unlink()
            removed.append(str(path.relative_to(root)).replace("\\", "/"))
    return removed


def _sql(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


# --------------------------------------------------------------------------- year sources


@dataclass(frozen=True)
class YearSource:
    universe: str
    year: int
    source: Path


def _n500_year_sources() -> list[YearSource]:
    root = paths.NIFTY500_PIT / "08 Parquet" / "certified_adjusted"
    out: list[YearSource] = []
    for directory in sorted(root.glob("year=*")):
        source = directory / "nifty500_adjusted_daily.parquet"
        if source.is_file():
            out.append(YearSource("nifty500", int(directory.name.split("=")[1]), source))
    return out


def _n750_year_sources() -> list[YearSource]:
    out: list[YearSource] = []
    for source in sorted(paths.CLEAN_PARQUET_BY_YEAR.glob("EOD2_Clean_*.parquet")):
        out.append(YearSource("nifty750", int(source.stem.rsplit("_", 1)[-1]), source))
    return out


def year_label(universe: str, year: int) -> tuple[str, str]:
    """Return ``(label, membership_basis)`` for one universe-year.

    ``BACKTEST_SAFE``     real point-in-time membership for the whole year.
    ``PARTIAL``           the year straddles the date membership becomes trustworthy.
    ``PRICE_DATA_ONLY``   prices are real, membership for this year is not. Do not backtest.
    """
    if universe == "nifty500":
        anchor = OFFICIAL_MEMBERSHIP_ANCHOR
        if year > anchor.year:
            return "BACKTEST_SAFE", "OFFICIAL_POINT_IN_TIME"
        if year == anchor.year:
            return (
                "PARTIAL",
                f"OFFICIAL_POINT_IN_TIME_FROM_{anchor.isoformat()}_RECONSTRUCTED_BEFORE",
            )
        return "PRICE_DATA_ONLY", "RECONSTRUCTED_MEDIUM_CONFIDENCE"
    anchor = VAJRA750_FIRST_REBALANCE
    if year > anchor.year:
        return "BACKTEST_SAFE", "RULE_BASED_POINT_IN_TIME"
    if year == anchor.year:
        return "PARTIAL", f"RULE_BASED_POINT_IN_TIME_FROM_{anchor.isoformat()}"
    return "PRICE_DATA_ONLY", "NO_UNIVERSE_DEFINED_BEFORE_FIRST_REBALANCE"


# --------------------------------------------------------------------------- publishing


def _normalised_select(connection: duckdb.DuckDBPyConnection, source: Path) -> str:
    """SELECT that drops the redundant partition column, NULLs the empty-string column, and
    adds the two tradability columns.

    Corporate actions are clean now, so the remaining way a backtest goes wrong is subtler:
    the price is right, but nobody could have traded at it. These two columns hand the reader
    the filters for that, rather than the engine deciding for them and quietly dropping real
    data.
    """
    columns = [
        row[0]
        for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{_sql(source)}')"
        ).fetchall()
    ]
    parts: list[str] = []
    for name in columns:
        if name == "year":
            # Redundant with the filename, and a trap: it is the partition key, not data.
            continue
        if name in EMPTY_STRING_COLUMNS:
            parts.append(f'NULLIF("{name}", \'\') AS "{name}"')
        else:
            parts.append(f'"{name}"')

    # Open = High = Low = Close means the session had no intraday range at all: a circuit
    # limit, or a single trade. A backtest that assumes it filled anywhere other than that
    # one price is inventing a fill.
    parts.append('(Open = High AND High = Low AND Low = Close) AS "IsFrozenBar"')
    # Traded value in rupees. nifty750 carries it already; nifty500 has only the raw exchange
    # figure under a name that does not say what it is. Turnover is invariant under a split
    # (price x f, quantity / f), so the raw figure is the right one either way.
    turnover = (
        "Turnover"
        if "Turnover" in columns
        else ("RawTurnover" if "RawTurnover" in columns else "Close * Volume")
    )
    parts.append(f'CAST({turnover} AS DOUBLE) AS "TurnoverINR"')

    return f"SELECT {', '.join(parts)} FROM read_parquet('{_sql(source)}')"


def _write_year(
    connection: duckdb.DuckDBPyConnection,
    spec: YearSource,
    root: Path,
    previous: dict[str, dict[str, Any]] | None = None,
    *,
    active_year: int | None = None,
) -> dict[str, Any]:
    """Write one year, self-healing whatever is missing or damaged.

    Three things can be true of a published year, and each gets a different response.

    **The Parquet is the dataset.** If it is missing or its hash does not match what the
    manifest recorded, it is rebuilt from the store, no questions asked. Deleting one - by
    accident or otherwise - costs nothing but the time to write it again.

    **The CSV is a convenience mirror, and its absence can be deliberate.** The operator
    deletes CSV files when the laptop runs short of disk; they are roughly ten times the size
    of the Parquet for the same rows. Re-creating seventeen years of them on the next run
    would silently undo that. So a year whose **Parquet is present but whose CSV is not** is
    read as a deliberate deletion and left alone.

    **Both missing means a real gap**, not a disk-space decision, so both are rebuilt. That is
    what happens when the whole published folder is deleted: everything comes back.

    The one exception is the year currently being appended to. New data always gets a CSV, so
    that after a CSV wipe the engine resumes writing them from that day forward rather than
    stopping for good.

    Seventeen of the eighteen years never change, so an untouched year is reused rather than
    rewritten: a daily run touches the current year and nothing else.
    """
    universe_dir = root / spec.universe
    parquet_path = universe_dir / "parquet" / f"{spec.universe}_{spec.year}.parquet"
    csv_path = universe_dir / "csv" / f"{spec.universe}_{spec.year}.csv"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    source_hash = sha256_file(spec.source)
    prior = (previous or {}).get(f"{spec.universe}:{spec.year}")
    is_active = active_year is not None and spec.year >= active_year

    parquet_ok = parquet_path.is_file() and (
        prior is None or sha256_file(parquet_path) == prior.get("parquet", {}).get("sha256")
    )
    csv_exists = csv_path.is_file()
    csv_ok = csv_exists and (
        prior is None
        or not prior.get("csv")
        or sha256_file(csv_path) == prior["csv"]["sha256"]
    )

    # A deliberate CSV deletion: the data is intact, only the mirror is gone.
    csv_intentionally_absent = parquet_path.is_file() and not csv_exists and not is_active
    write_csv = not csv_intentionally_absent

    unchanged = (
        prior is not None
        and prior.get("source_sha256") == source_hash
        and prior.get("format_version") == PUBLISH_FORMAT_VERSION
        and parquet_ok
        and (csv_ok or csv_intentionally_absent)
    )
    if unchanged:
        reused = dict(prior)
        reused["reused_unchanged"] = True
        # Labels are computed, not stored data: recompute in case the rule changed.
        label, basis = year_label(spec.universe, spec.year)
        reused["label"] = label
        reused["membership_basis"] = basis
        if csv_intentionally_absent:
            reused.pop("csv", None)
            reused["csv_present"] = False
            reused["csv_absent_reason"] = CSV_DELETED_REASON
        return reused

    select = _normalised_select(connection, spec.source)

    parquet_tmp = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.partial")
    connection.execute(
        f"COPY ({select}) TO '{_sql(parquet_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    equality: dict[str, Any] | None = None
    csv_tmp: Path | None = None
    if write_csv:
        csv_tmp = csv_path.with_name(f".{csv_path.name}.{uuid4().hex}.partial")
        connection.execute(f"COPY ({select}) TO '{_sql(csv_tmp)}' (HEADER, DELIMITER ',')")
        equality = _assert_parquet_csv_identical(connection, parquet_tmp, csv_tmp)

    os.replace(parquet_tmp, parquet_path)
    if csv_tmp is not None:
        os.replace(csv_tmp, csv_path)

    label, basis = year_label(spec.universe, spec.year)
    stats = connection.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT Symbol), COUNT(DISTINCT Date),
               MIN(Date), MAX(Date), COUNT(DISTINCT ISIN)
        FROM read_parquet('{_sql(parquet_path)}')
        """
    ).fetchone()
    record: dict[str, Any] = {
        "year": spec.year,
        "label": label,
        "membership_basis": basis,
        "rows": int(stats[0]),
        "symbols": int(stats[1]),
        "sessions": int(stats[2]),
        "isins": int(stats[5]),
        "first_date": str(stats[3]),
        "last_date": str(stats[4]),
        "parquet": _file_record(parquet_path, root),
        "csv_present": write_csv,
        "source": str(spec.source),
        "source_sha256": source_hash,
        "format_version": PUBLISH_FORMAT_VERSION,
        "reused_unchanged": False,
    }
    if write_csv:
        record["csv"] = _file_record(csv_path, root)
        record["parquet_csv_identical"] = True
        record["parquet_csv_check"] = equality
    else:
        record["csv_absent_reason"] = CSV_DELETED_REASON
    return record


def _assert_parquet_csv_identical(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    csv_path: Path,
) -> dict[str, Any]:
    """Full multiset comparison. Not a row count, not a checksum that can cancel out.

    An earlier version of this check compared per-column ``BIT_XOR(hash(...))``. It passed
    while 117,504 rows actually differed, because an even number of identical mismatches
    cancels under XOR. Set difference in both directions cannot be fooled that way.
    """
    schema = connection.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{_sql(parquet_path)}')"
    ).fetchall()
    columns = "{" + ", ".join(f"'{n}': '{t}'" for n, t, *_ in schema) + "}"
    connection.execute(
        f"CREATE OR REPLACE TEMP VIEW _pq AS SELECT * FROM read_parquet('{_sql(parquet_path)}')"
    )
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW _csv AS "
        f"SELECT * FROM read_csv('{_sql(csv_path)}', header=true, columns={columns})"
    )
    only_parquet = connection.execute(
        "SELECT COUNT(*) FROM ((SELECT * FROM _pq) EXCEPT ALL (SELECT * FROM _csv))"
    ).fetchone()[0]
    only_csv = connection.execute(
        "SELECT COUNT(*) FROM ((SELECT * FROM _csv) EXCEPT ALL (SELECT * FROM _pq))"
    ).fetchone()[0]
    if only_parquet or only_csv:
        raise RuntimeError(
            f"PARQUET_CSV_MISMATCH for {parquet_path.name}: "
            f"{only_parquet} rows only in Parquet, {only_csv} rows only in CSV"
        )
    return {
        "method": "EXCEPT ALL in both directions",
        "rows_only_in_parquet": 0,
        "rows_only_in_csv": 0,
    }


def _file_record(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(root)).replace("\\", "/"),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _copy_file(source: Path, target: Path, root: Path) -> dict[str, Any]:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.partial")
    expected = sha256_file(source)
    with source.open("rb") as src, temporary.open("wb") as dst:
        for chunk in iter(lambda: src.read(4 * 1024 * 1024), b""):
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    if sha256_file(temporary) != expected:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Copy hash mismatch: {source}")
    os.replace(temporary, target)
    record = _file_record(target, root)
    record["source"] = str(source)
    record["source_sha256"] = expected
    return record


def _export_table(
    connection: duckdb.DuckDBPyConnection,
    select: str,
    target_stem: Path,
    root: Path,
) -> list[dict[str, Any]]:
    """Write one query as both Parquet and CSV next to each other, and verify they agree."""
    parquet_path = target_stem.with_suffix(".parquet")
    csv_path = target_stem.with_suffix(".csv")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_tmp = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.partial")
    csv_tmp = csv_path.with_name(f".{csv_path.name}.{uuid4().hex}.partial")
    connection.execute(
        f"COPY ({select}) TO '{_sql(parquet_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.execute(f"COPY ({select}) TO '{_sql(csv_tmp)}' (HEADER, DELIMITER ',')")
    _assert_parquet_csv_identical(connection, parquet_tmp, csv_tmp)
    os.replace(parquet_tmp, parquet_path)
    os.replace(csv_tmp, csv_path)
    return [_file_record(parquet_path, root), _file_record(csv_path, root)]


# --------------------------------------------------------------------------- calendar


def build_trading_calendar(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Derive the NSE equity trading calendar from the sessions actually observed.

    This is evidence, not a published NSE holiday list: a date is a trading session if and
    only if official EOD data exists for it. Weekdays with no session are therefore labelled
    ``INFERRED_NON_TRADING`` - almost always a market holiday, but the engine has not read an
    official holiday circular and does not pretend to have.
    """
    broad = _sql(paths.CLEAN_PARQUET_BY_YEAR / "EOD2_Clean_*.parquet")
    sessions_select = f"""
        SELECT Date AS SessionDate,
               EXTRACT(year FROM Date)::INTEGER AS Year,
               EXTRACT(month FROM Date)::INTEGER AS Month,
               strftime(Date, '%A') AS Weekday,
               COUNT(*)::BIGINT AS SecuritiesWithData,
               'DERIVED_FROM_OFFICIAL_EOD_PRESENCE' AS Evidence
        FROM read_parquet('{broad}')
        GROUP BY 1, 2, 3, 4
        ORDER BY 1
    """
    records = _export_table(
        connection, sessions_select, root / "calendar" / "nse_trading_sessions", root
    )

    bounds = connection.execute(
        f"SELECT MIN(Date), MAX(Date) FROM read_parquet('{broad}')"
    ).fetchone()
    non_trading_select = f"""
        WITH span AS (
            SELECT UNNEST(generate_series(
                (SELECT MIN(Date) FROM read_parquet('{broad}')),
                (SELECT MAX(Date) FROM read_parquet('{broad}')),
                INTERVAL 1 DAY
            ))::DATE AS CalendarDate
        ),
        sessions AS (SELECT DISTINCT Date FROM read_parquet('{broad}'))
        SELECT CalendarDate,
               strftime(CalendarDate, '%A') AS Weekday,
               CASE WHEN dayofweek(CalendarDate) IN (0, 6)
                    THEN 'WEEKEND' ELSE 'INFERRED_NON_TRADING' END AS Reason
        FROM span
        WHERE CalendarDate NOT IN (SELECT Date FROM sessions)
        ORDER BY 1
    """
    records += _export_table(
        connection, non_trading_select, root / "calendar" / "nse_non_trading_days", root
    )

    session_count = connection.execute(
        f"SELECT COUNT(DISTINCT Date) FROM read_parquet('{broad}')"
    ).fetchone()[0]
    weekday_holidays = connection.execute(
        f"""
        WITH span AS (
            SELECT UNNEST(generate_series(
                (SELECT MIN(Date) FROM read_parquet('{broad}')),
                (SELECT MAX(Date) FROM read_parquet('{broad}')),
                INTERVAL 1 DAY
            ))::DATE AS CalendarDate
        ),
        sessions AS (SELECT DISTINCT Date FROM read_parquet('{broad}'))
        SELECT COUNT(*) FROM span
        WHERE dayofweek(CalendarDate) NOT IN (0, 6)
          AND CalendarDate NOT IN (SELECT Date FROM sessions)
        """
    ).fetchone()[0]
    summary = {
        "first_session": str(bounds[0]),
        "last_session": str(bounds[1]),
        "trading_sessions": int(session_count),
        "inferred_weekday_holidays": int(weekday_holidays),
        "basis": "DERIVED_FROM_OFFICIAL_EOD_PRESENCE",
        "caveat": (
            "A date counts as a trading session only if official EOD data exists for it. "
            "Non-trading weekdays are inferred, not read from an NSE holiday circular."
        ),
    }
    return records, summary


# --------------------------------------------------------------------------- membership etc.


def _publish_membership(connection: duckdb.DuckDBPyConnection, root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    pit = paths.NIFTY500_PIT

    n500_map = {
        pit / "07 Point In Time Panels/nifty500_daily_membership_certified.parquet":
            root / "nifty500/membership/nifty500_daily_membership.parquet",
        pit / "07 Point In Time Panels/nifty500_monthly_members.parquet":
            root / "nifty500/membership/nifty500_monthly_members.parquet",
        pit / "02 Constituent History/nifty500_membership_intervals.csv":
            root / "nifty500/membership/nifty500_membership_intervals.csv",
        pit / "01 Raw Source Archives/Official Current Constituents/ind_nifty500list.csv":
            root / "nifty500/membership/nifty500_current_official_constituents.csv",
        pit / "03 Security Master/nifty500_security_master.parquet":
            root / "nifty500/membership/nifty500_security_master.parquet",
        pit / "03 Security Master/nifty500_symbol_history.parquet":
            root / "nifty500/membership/nifty500_symbol_history.parquet",
    }
    for source, target in n500_map.items():
        if not source.is_file():
            raise RuntimeError(f"Membership source missing: {source}")
        records.append(_copy_file(source, target, root))

    # CSV companions for the two Parquet-only membership panels, so the folder is usable
    # without a Parquet reader.
    for stem in ("nifty500_daily_membership", "nifty500_monthly_members", "nifty500_security_master"):
        src = root / "nifty500/membership" / f"{stem}.parquet"
        csv_target = root / "nifty500/membership" / f"{stem}.csv"
        tmp = csv_target.with_name(f".{csv_target.name}.{uuid4().hex}.partial")
        connection.execute(
            f"COPY (SELECT * FROM read_parquet('{_sql(src)}')) TO '{_sql(tmp)}' "
            "(HEADER, DELIMITER ',')"
        )
        os.replace(tmp, csv_target)
        records.append(_file_record(csv_target, root))

    for name in (
        "Monthly_Vajra_750_Universe.parquet",
        "Monthly_Vajra_750_Universe.csv",
        "Monthly_750_Coverage.parquet",
        "Monthly_750_Coverage.csv",
    ):
        source = paths.MONTHLY_750_UNIVERSE / name
        if not source.is_file():
            raise RuntimeError(f"VAJRA 750 membership source missing: {source}")
        target = root / "nifty750/membership" / name.lower().replace("monthly_", "vajra750_monthly_")
        records.append(_copy_file(source, target, root))
    return records


def _publish_repair_ledger(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    out: Path,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Every row the engine repaired or excluded, read from the data rather than from a log.

    Deriving this from the run log would be wrong: the repair pass is idempotent, so a run
    where nothing needed fixing reports zero events, and the published record would silently
    empty out. The marks live in the data itself, so that is where they are read from.
    """
    from vajra_regime.ca_repair import EXCLUSION_REASON, REPAIR_NOTE, year_files

    rec = _sql(
        paths.NIFTY500_PIT / "04 Corporate Actions"
        / "nifty500_corporate_action_reconciliation.parquet"
    )
    frames: list[str] = []
    for universe, classification_column in (("nifty500", "DiscontinuityClassification"), ("nifty750", None)):
        files = list(year_files(universe).values())
        if not files:
            continue
        listed = ", ".join(f"'{_sql(f)}'" for f in files)
        repaired = (
            f"""
            SELECT '{universe}' AS universe, 'REPAIRED_BY_ENGINE' AS handling,
                   Date AS ex_date, Symbol AS symbol, ISIN AS isin
            FROM read_parquet([{listed}])
            WHERE "{classification_column}" = '{REPAIR_NOTE}'
            """
            if classification_column
            else None
        )
        excluded = f"""
            SELECT '{universe}' AS universe, 'EXCLUDED_NOT_RESEARCH_ELIGIBLE' AS handling,
                   Date AS ex_date, Symbol AS symbol, ISIN AS isin
            FROM read_parquet([{listed}])
            WHERE CorporateActionQuarantineReason = '{EXCLUSION_REASON}'
        """
        frames.append(excluded)
        if repaired:
            frames.append(repaired)
    if not frames:
        return {"status": "NO_MARKED_ROWS"}

    union = " UNION ALL ".join(f"({f})" for f in frames)
    select = f"""
        SELECT m.universe, m.handling, m.ex_date, m.symbol, m.isin,
               r.ActionType AS action_type, r.Subject AS subject, r.PriceFactor AS price_factor
        FROM ({union}) m
        LEFT JOIN read_parquet('{rec}') r ON r.ISIN = m.isin AND r.ExDate = m.ex_date
        ORDER BY m.universe, m.ex_date, m.symbol
    """
    count = connection.execute(f"SELECT COUNT(*) FROM ({select})").fetchone()[0]
    if not count:
        return {"status": "NO_MARKED_ROWS"}
    records.extend(
        _export_table(connection, select, out / "engine_repairs_and_exclusions", root)
    )
    by_handling = connection.execute(
        f"SELECT handling, COUNT(*) FROM ({select}) GROUP BY 1"
    ).fetchall()
    return {
        "status": "PUBLISHED",
        "total_marked_rows": int(count),
        "by_handling": {row[0]: int(row[1]) for row in by_handling},
        "published_file": "corporate_actions/engine_repairs_and_exclusions.parquet",
        "note": (
            "REPAIRED_BY_ENGINE: a split/bonus/face-value change the source series had not "
            "applied; the history before the ex-date was rescaled. "
            "EXCLUDED_NOT_RESEARCH_ELIGIBLE: a demerger, rights issue or large special "
            "dividend where the price fell but the shareholder's wealth did not; prices are "
            "left alone and the row is marked ineligible."
        ),
    }


def _publish_corporate_actions(
    connection: duckdb.DuckDBPyConnection, root: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ca_dir = paths.NIFTY500_PIT / "04 Corporate Actions"
    all_events = ca_dir / "nifty500_official_corporate_actions_all_equities.parquet"
    reconciliation = ca_dir / "nifty500_corporate_action_reconciliation.parquet"
    quarantine = ca_dir / "nifty500_relisting_long_gap_quarantine.parquet"
    for path in (all_events, reconciliation, quarantine):
        if not path.is_file():
            raise RuntimeError(f"Corporate action source missing: {path}")

    out = root / "corporate_actions"
    records: list[dict[str, Any]] = []

    # RawJson is a full copy of the NSE payload per event. Useful for provenance, and the
    # single biggest column in the file; it stays in Parquet and is dropped from the CSV
    # companion so the CSV is actually openable.
    records += _export_table(
        connection,
        f"SELECT * FROM read_parquet('{_sql(all_events)}') ORDER BY ExDate, Symbol",
        out / "official_nse_corporate_actions_all",
        root,
    )
    records += _export_table(
        connection,
        f"""SELECT * FROM read_parquet('{_sql(reconciliation)}')
            WHERE Decision NOT LIKE 'QUARANTINE%' ORDER BY ExDate, Symbol""",
        out / "corporate_actions_applied",
        root,
    )
    records += _export_table(
        connection,
        f"""SELECT * FROM read_parquet('{_sql(reconciliation)}')
            WHERE Decision LIKE 'QUARANTINE%' ORDER BY ExDate, Symbol""",
        out / "corporate_actions_quarantined",
        root,
    )
    records += _export_table(
        connection,
        f"SELECT * FROM read_parquet('{_sql(quarantine)}') ORDER BY Symbol",
        out / "relisting_long_gap_quarantine",
        root,
    )

    # What the engine itself had to repair or exclude after the fact - the honest record of
    # where the upstream adjustment was wrong. It ships with the data rather than sitting in
    # a log the reader will never see.
    repair_summary = _publish_repair_ledger(connection, root, out, records)

    decisions = connection.execute(
        f"""SELECT Decision, ActionType, COUNT(*) AS n
            FROM read_parquet('{_sql(reconciliation)}')
            GROUP BY 1, 2 ORDER BY n DESC"""
    ).fetchall()
    totals = connection.execute(
        f"""SELECT COUNT(*),
                   SUM(CASE WHEN Decision LIKE 'QUARANTINE%' THEN 1 ELSE 0 END),
                   COUNT(DISTINCT Symbol)
            FROM read_parquet('{_sql(reconciliation)}')"""
    ).fetchone()
    all_total = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT Symbol) FROM read_parquet('{_sql(all_events)}')"
    ).fetchone()
    summary = {
        "official_events_archived": int(all_total[0]),
        "official_events_symbols": int(all_total[1]),
        "events_reconciled_against_prices": int(totals[0]),
        "events_quarantined": int(totals[1]),
        "reconciled_symbols": int(totals[2]),
        "by_decision_and_type": [
            {"decision": d, "action_type": a, "events": int(n)} for d, a, n in decisions
        ],
        "engine_repairs_and_exclusions": repair_summary,
    }
    return records, summary


# --------------------------------------------------------------------------- manifest


def _latest_session(connection: duckdb.DuckDBPyConnection, universe_dir: Path) -> str:
    files = sorted((universe_dir / "parquet").glob("*.parquet"))
    if not files:
        raise RuntimeError(f"No published parquet under {universe_dir}")
    value = connection.execute(
        "SELECT MAX(Date) FROM read_parquet(?)", [[str(p) for p in files]]
    ).fetchone()[0]
    return str(value)


def build_manifest(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    *,
    universes: dict[str, Any],
    extra_records: list[dict[str, Any]],
    calendar_summary: dict[str, Any],
    corporate_action_summary: dict[str, Any],
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for universe in universes.values():
        for year in universe["years"]:
            files.append({**year["parquet"], "kind": "OHLCV_PARQUET"})
            # A year whose CSV the operator deleted has none to list. Listing one would make
            # every integrity check fail on a file that is absent on purpose.
            if year.get("csv"):
                files.append({**year["csv"], "kind": "OHLCV_CSV"})
    files.extend(extra_records)

    manifest: dict[str, Any] = {
        "manifest_version": PUBLISH_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "published_root": str(root),
        "latest_session": max(u["last_date"] for u in universes.values()),
        "official_membership_anchor": OFFICIAL_MEMBERSHIP_ANCHOR.isoformat(),
        "price_return_only_no_dividends": True,
        "currency": "INR",
        "exchange": "NSE",
        "universes": universes,
        "calendar": calendar_summary,
        "corporate_actions": corporate_action_summary,
        "csv_policy": {
            "rule": (
                "Every year has a Parquet file. A year may have no CSV: the CSV is a "
                "convenience mirror roughly ten times the size, and deleting it to free disk "
                "is supported. The engine will not recreate a deleted CSV for a past year, "
                "and will not stop because one is missing."
            ),
            "to_get_a_deleted_csv_back": (
                "Delete that year's Parquet file as well. The next run rebuilds both."
            ),
            "years_without_csv": {
                name: [y["year"] for y in entry["years"] if not y.get("csv")]
                for name, entry in universes.items()
            },
        },
        "label_meanings": {
            "BACKTEST_SAFE": "Real point-in-time membership exists for the whole year.",
            "PARTIAL": "Membership becomes trustworthy part-way through this year. Backtests "
            "that start inside the untrusted part carry survivorship bias.",
            "PRICE_DATA_ONLY": "Prices are real. Membership for this year is reconstructed or "
            "undefined. Do not treat a backtest over these years as evidence.",
        },
        "files": sorted(files, key=lambda row: row["path"]),
        "file_count": len(files),
        "total_bytes": sum(int(row["bytes"]) for row in files),
    }
    manifest["manifest_payload_sha256"] = canonical_hash(
        {k: v for k, v in manifest.items() if k != "manifest_payload_sha256"}
    )
    return manifest


def verify_published(root: Path | None = None) -> dict[str, Any]:
    """Re-hash every file listed in MANIFEST.json. This is the integrity check the
    documentation tells a future reader to run."""
    root = Path(root) if root else paths.DATA_ROOT
    manifest_path = root / "MANIFEST.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"MANIFEST.json not found under {root}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    missing: list[str] = []
    mismatched: list[str] = []
    for record in manifest["files"]:
        path = root / record["path"]
        if not path.is_file():
            missing.append(record["path"])
            continue
        if sha256_file(path) != record["sha256"]:
            mismatched.append(record["path"])
    _assert_no_reparse(root)

    # Anything on disk that the manifest does not know about is a leftover, and the whole
    # point of this folder is that it has none.
    known = {record["path"] for record in manifest["files"]}
    documentation = {
        "MANIFEST.json",
        "CHANGELOG.md",
        "START_HERE_AI.md",
        "DATA_DICTIONARY.md",
        "DATA_QUALITY_REPORT.md",
    }
    unexpected = sorted(
        str(p.relative_to(root)).replace("\\", "/")
        for p in root.rglob("*")
        if p.is_file()
        and str(p.relative_to(root)).replace("\\", "/") not in known | documentation
    )
    return {
        "pass": not missing and not mismatched and not unexpected,
        "files_checked": len(manifest["files"]),
        "missing": missing,
        "hash_mismatched": mismatched,
        "unexpected_files": unexpected,
        "manifest_sha256": sha256_file(manifest_path),
        "latest_session": manifest["latest_session"],
    }


CHANGELOG_HEADER = (
    "# CHANGELOG\n\n"
    "One line per day, dated in IST (UTC+5:30). Newest at the bottom. Written by the engine "
    "on every run; do not edit by hand.\n\n"
)


def _append_changelog(root: Path, day: date, line: str) -> None:
    """One line per day, replaced in place when that day already has one.

    Two things were wrong before. The date came from ``datetime.now(UTC)``, so a run at
    03:18 IST was stamped with the *previous* day and the file appeared to run backwards -
    2026-08-30 followed by 2026-08-29, which reads as corruption. And every run appended a
    line, so a day with four runs produced four lines, three of them saying nothing changed.

    This dataset is a record of IST trading sessions, so its changelog is dated in IST too.
    """
    path = root / "CHANGELOG.md"
    lines: list[str] = []
    header = CHANGELOG_HEADER
    if path.is_file():
        text = path.read_text(encoding="utf-8")
        head, separator, body = text.partition("\n- ")
        if separator:
            header = head + "\n"
            lines = [f"- {row.rstrip()}" for row in body.split("\n- ") if row.strip()]
        else:
            header = text if text.endswith("\n") else text + "\n"

    stamp = day.isoformat()
    today = f"- {stamp} - {line}"
    if lines and lines[-1].startswith(f"- {stamp} "):
        # A day that saw a real change stays marked as changed, however many quiet runs
        # follow it.
        if "UPDATED" in lines[-1] and "NO CHANGE" in today:
            today = lines[-1]
        lines[-1] = today
    else:
        lines.append(today)

    _atomic_text(path, header + "\n".join(lines) + "\n")


# --------------------------------------------------------------------------- entry point


def publish_dataset(*, root: Path | None = None, write_docs: bool = True) -> dict[str, Any]:
    """Rebuild the published dataset from the store. Atomic, verified, fails closed."""
    from vajra_regime import publish_docs

    started = datetime.now(UTC)
    root = Path(root) if root else paths.DATA_ROOT
    root.mkdir(parents=True, exist_ok=True)
    status_path = paths.LOGS_ROOT / "publish" / "latest_publish_status.json"

    try:
        removed_partials = _cleanup_orphan_partials(root)
        _assert_no_reparse(root)

        foundation_status_path = paths.NIFTY500_PIT / "11 Logs" / "foundation_certification_status.json"
        if not foundation_status_path.is_file():
            raise RuntimeError("Foundation certification status missing; refusing to publish")
        foundation = json.loads(foundation_status_path.read_text(encoding="utf-8-sig"))
        if not str(foundation.get("status", "")).startswith("CERTIFIED_PASS"):
            raise RuntimeError(
                f"Foundation is not certified ({foundation.get('status')}); refusing to publish"
            )

        connection = duckdb.connect()
        connection.execute("SET enable_progress_bar=false")

        specs = _n500_year_sources() + _n750_year_sources()
        if not specs:
            raise RuntimeError("No source years found in the store")

        # Refuse to rewind: if what is already published is newer than the store, stop.
        previous = None
        manifest_path = root / "MANIFEST.json"
        if manifest_path.is_file():
            previous = json.loads(manifest_path.read_text(encoding="utf-8"))
            store_latest = connection.execute(
                "SELECT MAX(Date) FROM read_parquet(?)",
                [[str(s.source) for s in specs if s.universe == "nifty500"]],
            ).fetchone()[0]
            if str(previous.get("latest_session", "")) > str(store_latest):
                raise RuntimeError(
                    "PUBLISHED_NEWER_THAN_STORE_REFUSED: published="
                    f"{previous['latest_session']} store={store_latest}"
                )

        prior_years: dict[str, dict[str, Any]] = {}
        if previous:
            for name, entry in previous.get("universes", {}).items():
                for row in entry.get("years", []):
                    prior_years[f"{name}:{row['year']}"] = row

        # The year currently being appended to. New data always gets a CSV, so a CSV wipe
        # stops the mirror for old years without stopping it for good.
        store_latest_date = connection.execute(
            "SELECT MAX(Date) FROM read_parquet(?)",
            [[str(s.source) for s in specs if s.universe == "nifty500"]],
        ).fetchone()[0]
        active_year = int(str(store_latest_date)[:4])

        universes: dict[str, Any] = {}
        for spec in specs:
            entry = universes.setdefault(
                spec.universe,
                {
                    "definition": N500_DEFINITION if spec.universe == "nifty500" else N750_DEFINITION,
                    "is_official_nse_index": spec.universe == "nifty500",
                    "years": [],
                },
            )
            entry["years"].append(
                _write_year(connection, spec, root, prior_years, active_year=active_year)
            )

        for name, entry in universes.items():
            entry["years"].sort(key=lambda row: row["year"])
            entry["rows"] = sum(row["rows"] for row in entry["years"])
            entry["sessions"] = sum(row["sessions"] for row in entry["years"])
            entry["first_date"] = entry["years"][0]["first_date"]
            entry["last_date"] = entry["years"][-1]["last_date"]
            entry["backtest_safe_years"] = [
                row["year"] for row in entry["years"] if row["label"] == "BACKTEST_SAFE"
            ]
            entry["price_data_only_years"] = [
                row["year"] for row in entry["years"] if row["label"] == "PRICE_DATA_ONLY"
            ]
            entry["partial_years"] = [
                row["year"] for row in entry["years"] if row["label"] == "PARTIAL"
            ]
            entry["distinct_symbols"] = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT Symbol) FROM read_parquet(?)",
                    [[str(root / name / "parquet" / f"{name}_{row['year']}.parquet")
                      for row in entry["years"]]],
                ).fetchone()[0]
            )

        n500_latest = universes["nifty500"]["last_date"]
        n750_latest = universes["nifty750"]["last_date"]
        if n500_latest != n750_latest:
            raise RuntimeError(
                f"UNIVERSES_OUT_OF_STEP: nifty500={n500_latest}, nifty750={n750_latest}"
            )

        extra = _publish_membership(connection, root)
        calendar_records, calendar_summary = build_trading_calendar(connection, root)
        extra += calendar_records
        ca_records, ca_summary = _publish_corporate_actions(connection, root)
        extra += ca_records

        manifest = build_manifest(
            connection,
            root,
            universes=universes,
            extra_records=extra,
            calendar_summary=calendar_summary,
            corporate_action_summary=ca_summary,
        )
        atomic_json(root / "MANIFEST.json", manifest)

        if write_docs:
            publish_docs.write_all(root, manifest)

        verification = verify_published(root)
        if not verification["pass"]:
            raise RuntimeError(f"Published dataset failed verification: {verification}")

        # Compare the data, not the whole manifest: the manifest carries generated_at_utc, so
        # its own hash changes on every run and would report UPDATED even on a quiet Sunday.
        def _data_hashes(payload: dict[str, Any] | None) -> dict[str, str]:
            if not payload:
                return {}
            return {row["path"]: row["sha256"] for row in payload.get("files", [])}

        changed = _data_hashes(previous) != _data_hashes(manifest)
        finished = datetime.now(UTC)
        # IST observes no daylight saving, so a fixed offset is exact and needs no tz database.
        ist_day = (finished + timedelta(hours=5, minutes=30)).date()
        _append_changelog(
            root,
            ist_day,
            f"latest session {manifest['latest_session']} - "
            f"{manifest['file_count']} files, "
            f"{universes['nifty500']['rows']:,} NIFTY500 rows, "
            f"{universes['nifty750']['rows']:,} VAJRA750 rows - "
            f"{'UPDATED' if changed else 'NO CHANGE'}",
        )

        status = {
            "status": "SUCCESS",
            "outcome": "UPDATED" if changed else "NO_CHANGE",
            "version": PUBLISH_VERSION,
            "generated_at_utc": finished.isoformat(),
            "duration_seconds": round((finished - started).total_seconds(), 3),
            "published_root": str(root),
            "latest_session": manifest["latest_session"],
            "file_count": manifest["file_count"],
            "total_bytes": manifest["total_bytes"],
            "orphan_partials_recovered": removed_partials,
            "verification": verification,
            "reads_from_published_folder": False,
        }
        status["status_payload_sha256"] = canonical_hash(status)
        atomic_json(status_path, status)
        return status
    except Exception as error:
        failure = {
            "status": "FAILED",
            "version": PUBLISH_VERSION,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "published_root": str(root),
            "error": f"{type(error).__name__}: {error}",
            "previously_published_data_left_intact": True,
        }
        failure["status_payload_sha256"] = canonical_hash(failure)
        atomic_json(status_path, failure)
        raise


__all__ = [
    "OFFICIAL_MEMBERSHIP_ANCHOR",
    "VAJRA750_FIRST_REBALANCE",
    "build_manifest",
    "build_trading_calendar",
    "publish_dataset",
    "verify_published",
    "year_label",
]
