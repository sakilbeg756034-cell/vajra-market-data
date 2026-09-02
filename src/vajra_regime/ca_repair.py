"""Catch and repair corporate actions that the adjusted price series did not actually apply.

Why this exists
---------------
The certified adjusted series is built on top of a third-party end-of-day feed that is itself
supposed to be split- and bonus-adjusted. For almost every security it is. It is not always,
and the reconciliation ledger did not notice, because the ledger records what *should* have
been applied rather than checking the price series afterwards.

Found on 2026-08-29 by re-deriving quality from the published files: BHARTIARTL's 1:2
face-value split of 2009-07-24 was never applied to the pre-split history, leaving a -48.9%
one-day "return" in a large-cap stock that was still flagged research-eligible. ALLCARGO's
1:5 face-value split of 2009-11-19 had the same problem, at -80.7%.

A third failure mode, found on 2026-09-02
----------------------------------------
Both detections below start ``FROM _events r JOIN _prices p ON p.Date = r.ExDate``. They are
anchored to an ex-date, so a price break with **no corporate action at all** is not merely
unrepaired - it is never looked at. It passes through research-eligible with nothing recorded.

That is not hypothetical. On 2026-01-01, fourteen securities broke at once, each by exactly the
reciprocal of its own later-2026 bonus factor: ZFCVINDIA x5.94 against a 5:1 bonus on 06-24,
CUPID x5.07 against a 4:1 on 03-09, INFOBEAN x3.95 against a 3:1 on 02-27, ECLERX x2.05 against
a 1:1 on 03-13, TRENT x1.51 against a 1:2 on 06-04. Fourteen unrelated companies do not produce
ratios that line up with their own pending bonuses by chance, and the breaks land on the first
session of the year rather than on any ex-date. 2024 has three of these, 2026 has fourteen, and
every other year has none - the signature of a year-boundary problem upstream in the adjusted
master, not of fourteen real consolidations.

The root cause is upstream and is not fixed here. What is fixed here is that the module no
longer walks past it in silence.

Why these are excluded and not rescaled: the ratio is only inferable from the break itself, and
this module's own rule is that rescaling on a guess is worse than leaving the row flagged. Both
readings of the CUPID evidence - a real consolidation missing from the archive, or a year file
adjusted on a different basis - imply opposite corrections, and picking one would silently
falsify a whole year for the securities involved.

Two failure modes, two different responses
------------------------------------------
**Mechanical events** - splits, bonuses, face-value changes. These have an exact known ratio.
If the series does not carry it, the repair is arithmetic and safe: rescale the history before
the ex-date. That is what this module does.

**Non-mechanical events** - demergers, schemes of arrangement, rights issues, large special
dividends. The price genuinely falls, but the holder's wealth does not: they received shares
in the new entity, or the dividend, or discounted rights. There is no single correct ratio
without knowing the value of what was received, so nothing is rescaled. Instead the boundary
row is marked not research-eligible, because treating a -65% demerger print as a return is
wrong, and a momentum strategy will otherwise read it as a crash.

Properties
----------
Idempotent - it re-detects from the prices on every run and does nothing when they are already
correct, which matters because the VAJRA 750 supporting master is rebuilt from scratch daily.
Targeted - only affected ISINs, and only rows strictly before the ex-date. Atomic - each year
file is written to a temp file and moved into place, with the row count asserted unchanged.
Verified - it re-runs detection afterwards and raises if anything remains.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash

REPAIR_VERSION = "VAJRA_CA_REPAIR_V1"

# A mechanical event counts as "not applied" when the observed one-session move is large and
# lands close to what an entirely unadjusted event would produce.
UNAPPLIED_MOVE_THRESHOLD = 0.15
UNAPPLIED_MATCH_TOLERANCE = 0.10
# After repair the boundary must look like an ordinary session rather than a scale break.
REPAIRED_MOVE_TOLERANCE = 0.20
# Non-mechanical events are only excluded when the print is big enough to distort a backtest.
NON_MECHANICAL_MOVE_THRESHOLD = 0.35

EXCLUSION_REASON = "NON_MECHANICAL_CORPORATE_ACTION_PRICE_BREAK_NOT_A_RETURN"

# A break this large with no corporate action anywhere near it is not a return. NSE price
# bands top out at 20%; a one-day move past 50% that is not a corporate action essentially
# does not happen. Measured over the whole published history at several thresholds: 50% keeps
# 573 rows across both universes, 465 of them the first print after a suspension of 100+ days,
# and the 63 without a gap cluster on round ratios - 2.0 eleven times, 0.4 thirteen, 0.3 ten,
# 0.2 seven, 0.5 six - which is what corporate actions look like, not what markets look like.
# Loosening this to 35% would pull in 1,221 rows and start catching real moves.
UNEXPLAINED_MOVE_THRESHOLD = 0.50

# NSE files ex-dates a day or two off often enough, and the print can land either side of one.
# A break within this many calendar days of ANY archived event for that security - dividend,
# AGM, anything - is treated as explained and left alone. Deliberately generous: the cost of
# missing one break is small, the cost of excluding a real return is not.
UNEXPLAINED_EVENT_WINDOW_DAYS = 5

UNEXPLAINED_REASON = "UNEXPLAINED_PRICE_BREAK_NO_CORPORATE_ACTION_FOUND"
REPAIR_NOTE = "MECHANICAL_ADJUSTMENT_REPAIRED_BY_ENGINE"


@dataclass(frozen=True)
class UniverseSpec:
    """Which columns in a universe's year files play which role under a rescale."""

    name: str
    return_column: str
    price_columns: tuple[str, ...]
    volume_columns: tuple[str, ...]
    price_factor_column: str
    volume_factor_column: str
    eligibility_column: str
    reason_column: str
    classification_column: str | None


NIFTY500 = UniverseSpec(
    name="nifty500",
    return_column="AdjustedReturn1D",
    price_columns=("Open", "High", "Low", "Close", "PointInTimePriceEligibilityClose"),
    volume_columns=("Volume",),
    price_factor_column="PriceAdjustmentFactor",
    volume_factor_column="VolumeAdjustmentFactor",
    eligibility_column="IsResearchEligible",
    reason_column="CorporateActionQuarantineReason",
    classification_column="DiscontinuityClassification",
)

NIFTY750 = UniverseSpec(
    name="nifty750",
    return_column="Return1D",
    # Turnover, MedianTurnover60 and TotalTrades are invariant under a split rescale
    # (price x f, quantity / f), so they are deliberately absent from both lists.
    price_columns=("Open", "High", "Low", "Close", "PrevClose"),
    volume_columns=("Volume", "DeliveryQuantity", "QuantityPerTrade"),
    price_factor_column="CorporateActionPriceFactor",
    volume_factor_column="CorporateActionVolumeFactor",
    eligibility_column="IsResearchEligible",
    reason_column="CorporateActionQuarantineReason",
    classification_column=None,
)


def _sql(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _official_archive_path() -> Path:
    """The complete official NSE corporate-action archive - every event, unfiltered.

    Deliberately not the reconciliation file. The reconciliation joins events to the
    point-in-time panel by ISIN, and a face-value change issues a *new* ISIN, so the event is
    filed under the old one and the reconciliation drops it entirely. Nine real splits and
    bonuses were invisible that way, including HDFC's 2010 face-value split, which left a
    phantom -79.4% crash in a top-ten index constituent. A safety net that depends on the
    thing it is meant to catch is not a safety net.
    """
    return (
        paths.NIFTY500_PIT
        / "04 Corporate Actions"
        / "nifty500_official_corporate_actions_all_equities.parquet"
    )


def _parsed_events(con: duckdb.DuckDBPyConnection) -> str:
    """Register every archived event with its parsed ratio; return the view name."""
    from vajra_regime.nifty500_migration.corporate_action_reconciliation import (  # noqa: PLC0415
        classify_official_action,
    )

    archive = _official_archive_path()
    rows = con.execute(
        f"SELECT EventId, Symbol, ISIN, Subject, ExDate FROM read_parquet('{_sql(archive)}') "
        "WHERE ExDate IS NOT NULL"
    ).fetchall()
    records = []
    for event_id, symbol, isin, subject, ex_date in rows:
        parsed = classify_official_action(subject or "")
        records.append(
            {
                "EventId": event_id,
                "Symbol": symbol,
                "ISIN": isin,
                "Subject": subject,
                "ExDate": ex_date,
                "ActionType": parsed.action_type,
                "PriceFactor": parsed.price_factor,
                "VolumeFactor": parsed.volume_factor,
                "ParseStatus": parsed.parse_status,
            }
        )
    con.register("_events_frame", pd.DataFrame(records))
    con.execute("CREATE OR REPLACE TEMP VIEW _events AS SELECT * FROM _events_frame")
    return "_events"


def year_files(universe: str) -> dict[int, Path]:
    if universe == "nifty500":
        root = paths.NIFTY500_PIT / "08 Parquet" / "certified_adjusted"
        out: dict[int, Path] = {}
        for directory in sorted(root.glob("year=*")):
            path = directory / "nifty500_adjusted_daily.parquet"
            if path.is_file():
                out[int(directory.name.split("=")[1])] = path
        return out
    return {
        int(p.stem.rsplit("_", 1)[-1]): p
        for p in sorted(paths.CLEAN_PARQUET_BY_YEAR.glob("EOD2_Clean_*.parquet"))
    }


# --------------------------------------------------------------------------- detection


def detect(con: duckdb.DuckDBPyConnection, spec: UniverseSpec) -> dict[str, list[dict[str, Any]]]:
    paths_for_universe = list(year_files(spec.name).values())
    if not paths_for_universe:
        return {
            "mechanical": [],
            "non_mechanical": [],
            "unrepairable_residual": [],
            "unexplained": [],
        }
    files = ", ".join(f"'{_sql(p)}'" for p in paths_for_universe)
    _parsed_events(con)
    con.execute(
        "CREATE OR REPLACE TEMP VIEW _prices AS "
        f'SELECT Date, Symbol, ISIN, Close, "{spec.return_column}" AS Ret, '
        f'"{spec.reason_column}" AS Reason '
        f"FROM read_parquet([{files}])"
    )
    # DISTINCT is load-bearing. NSE republishes revised corporate-action entries, so the same
    # split can appear twice in the ledger for one (ISIN, ex-date). Without this the repair
    # compounds the factor and scales the history by f squared - which is exactly what
    # happened the first time drill 4 was run twice against the same store.
    mechanical = con.execute(
        f"""
        SELECT DISTINCT r.ExDate, p.Symbol, p.ISIN, r.ActionType, r.Subject,
               r.PriceFactor, r.VolumeFactor, p.Ret
        FROM _events r
        JOIN _prices p
          ON p.Date = r.ExDate
         -- Match on ISIN *or* symbol. A face-value change issues a NEW ISIN, so the event is
         -- filed under the old one while the price rows already carry the new one, and an
         -- ISIN-only join silently misses it. That is how HDFC kept a phantom -79.4% crash on
         -- 2010-08-18: event under INE001A01028, prices under INE001A01036. Nine such events
         -- were hiding this way, in HDFC, STERLITE, TULIP, SINTEX, GRUH, COX&KINGS and
         -- JMTAUTOLTD.
         AND (p.ISIN = r.ISIN OR p.Symbol = r.Symbol)
        WHERE r.PriceFactor IS NOT NULL AND r.PriceFactor <> 1.0
          AND p.Ret IS NOT NULL
          AND abs(p.Ret) > {UNAPPLIED_MOVE_THRESHOLD}
          -- The ratio guard is what makes the looser symbol join safe: a coincidental symbol
          -- collision would have to land on the exact ex-date AND produce almost exactly the
          -- move an unapplied event of that ratio would produce.
          AND abs(p.Ret - (r.PriceFactor - 1.0)) < {UNAPPLIED_MATCH_TOLERANCE}
        ORDER BY r.ExDate
        """
    ).fetchall()
    non_mechanical = con.execute(
        f"""
        SELECT DISTINCT r.ExDate, p.Symbol, p.ISIN, r.ActionType, r.Subject, p.Ret
        FROM _events r
        JOIN _prices p
          ON p.Date = r.ExDate AND (p.ISIN = r.ISIN OR p.Symbol = r.Symbol)
        WHERE (r.PriceFactor IS NULL OR r.PriceFactor = 1.0)
          AND p.Ret IS NOT NULL AND p.Ret <= -{NON_MECHANICAL_MOVE_THRESHOLD}
          -- Already handled on a previous run. Without this the pass would rewrite the same
          -- year files every single day and defeat the publisher's unchanged-year reuse.
          AND (p.Reason IS NULL OR p.Reason <> '{EXCLUSION_REASON}')
        ORDER BY r.ExDate
        """
    ).fetchall()
    # The third bucket: a break with no event anywhere near it. This one is anchored to the
    # PRICES, not to the event ledger, which is the whole point - it is the only query here
    # that can see a break the archive knows nothing about. NOT EXISTS rather than an anti-join
    # so that a security with many events cannot multiply rows.
    unexplained = con.execute(
        f"""
        SELECT DISTINCT p.Date, p.Symbol, p.ISIN, p.Ret
        FROM _prices p
        WHERE p.Ret IS NOT NULL
          AND abs(p.Ret) > {UNEXPLAINED_MOVE_THRESHOLD}
          -- Already recorded on a previous run; re-flagging would rewrite every year file
          -- daily and defeat the publisher's unchanged-year reuse.
          AND (p.Reason IS NULL OR p.Reason <> '{UNEXPLAINED_REASON}')
          AND (p.Reason IS NULL OR p.Reason <> '{EXCLUSION_REASON}')
          AND NOT EXISTS (
              SELECT 1 FROM _events r
              WHERE (r.ISIN = p.ISIN OR r.Symbol = p.Symbol)
                AND abs(date_diff('day', r.ExDate, p.Date))
                    <= {UNEXPLAINED_EVENT_WINDOW_DAYS}
          )
        ORDER BY p.Date
        """
    ).fetchall()

    seen: set[tuple[str, str]] = set()
    deduped = []
    for row in mechanical:
        key = (str(row[2]), str(row[0]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    mechanical = deduped

    # Only repair when the repair actually fixes it. If applying the ratio would still leave
    # an extreme move at the boundary, the ratio is wrong, the date is wrong, or two events
    # coincide - and rescaling on a guess is worse than leaving the row flagged. AHCL's
    # 2026-04-24 event is one of these: a 1:5 split against a -89% print, which would still
    # be -45% after the factor.
    repairable = []
    residual = []
    for row in mechanical:
        after = (1.0 + float(row[7])) / float(row[5]) - 1.0
        (repairable if abs(after) <= REPAIRED_MOVE_TOLERANCE else residual).append(row)
    mechanical = repairable

    return {
        "unexplained": [
            {
                "ex_date": str(r[0]),
                "symbol": r[1],
                "isin": r[2],
                "observed_move": round(float(r[3]), 6),
                "handling": "EXCLUDED_NO_CORPORATE_ACTION_EXPLAINS_THIS_BREAK",
            }
            for r in unexplained
        ],
        "unrepairable_residual": [
            {
                "ex_date": str(r[0]),
                "symbol": r[1],
                "isin": r[2],
                "action_type": r[3],
                "subject": r[4],
                "price_factor": float(r[5]),
                "observed_move": round(float(r[7]), 6),
                "move_if_repaired": round((1.0 + float(r[7])) / float(r[5]) - 1.0, 6),
                "handling": "EXCLUDED_NOT_REPAIRED_RATIO_DOES_NOT_EXPLAIN_THE_MOVE",
            }
            for r in residual
        ],
        "mechanical": [
            {
                "ex_date": str(r[0]),
                "symbol": r[1],
                "isin": r[2],
                "action_type": r[3],
                "subject": r[4],
                "price_factor": float(r[5]),
                "volume_factor": float(r[6]) if r[6] else 1.0 / float(r[5]),
                "observed_move": round(float(r[7]), 6),
                "move_after_repair": round((1.0 + float(r[7])) / float(r[5]) - 1.0, 6),
            }
            for r in mechanical
        ],
        "non_mechanical": [
            {
                "ex_date": str(r[0]),
                "symbol": r[1],
                "isin": r[2],
                "action_type": r[3],
                "subject": r[4],
                "observed_move": round(float(r[5]), 6),
            }
            for r in non_mechanical
        ],
    }


# --------------------------------------------------------------------------- repair


def _factor_expression(events: list[dict[str, Any]], *, price: bool) -> str:
    """Multiplier for a row given its ISIN and Date.

    With unapplied events on d1 then d2, a row before d1 needs both factors; a row between
    d1 and d2 needs only d2's; a row on or after d2 needs none.
    """
    by_isin: dict[str, list[tuple[date, float, float]]] = {}
    for event in events:
        by_isin.setdefault(event["isin"], []).append(
            (
                date.fromisoformat(event["ex_date"]),
                float(event["price_factor"]),
                float(event["volume_factor"]),
            )
        )
    branches: list[str] = []
    for isin, rows in by_isin.items():
        rows.sort()
        safe = isin.replace("'", "''")
        for index, (ex_date, _, _) in enumerate(rows):
            factor = 1.0
            for _, price_factor, volume_factor in rows[index:]:
                factor *= price_factor if price else volume_factor
            lower = "" if index == 0 else f"Date >= DATE '{rows[index - 1][0].isoformat()}' AND "
            branches.append(
                f"WHEN ISIN = '{safe}' AND {lower}Date < DATE '{ex_date.isoformat()}' "
                f"THEN {factor!r}"
            )
    return "CASE " + " ".join(branches) + " ELSE 1.0 END" if branches else "1.0"


def _key_list(events: list[dict[str, Any]]) -> str:
    pairs = ", ".join(
        f"('{e['isin']}', DATE '{e['ex_date']}')" for e in events if e.get("isin")
    )
    return f"(ISIN, Date) IN ({pairs})" if pairs else "FALSE"


def _return_patch_expression(events: list[dict[str, Any]], column: str) -> str:
    """The boundary row's stored return has to be recomputed, in closed form.

    Every return inside the pre-event period is unchanged, because both sides of the ratio are
    rescaled by the same factor. Only the return that straddles the ex-date changes, and it
    changes to ``(1 + old) / price_factor - 1``.
    """
    branches = [
        f"WHEN ISIN = '{e['isin']}' AND Date = DATE '{e['ex_date']}' "
        f"THEN (1.0 + \"{column}\") / {e['price_factor']!r} - 1.0"
        for e in events
        if e.get("isin")
    ]
    return "CASE " + " ".join(branches) + f' ELSE "{column}" END' if branches else f'"{column}"'


def _rewrite_year(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    spec: UniverseSpec,
    *,
    mechanical: list[dict[str, Any]],
    non_mechanical: list[dict[str, Any]],
    unexplained: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    columns = [
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_sql(path)}')").fetchall()
    ]
    types = {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_sql(path)}')").fetchall()
    }
    before_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_sql(path)}')").fetchone()[0]

    price_factor = _factor_expression(mechanical, price=True)
    volume_factor = _factor_expression(mechanical, price=False)
    repaired_keys = _key_list(mechanical)
    excluded_keys = _key_list(non_mechanical)
    unexplained_keys = _key_list(unexplained or [])

    projections: list[str] = []
    for name in columns:
        quoted = f'"{name}"'
        if name in spec.price_columns:
            projections.append(f"{quoted} * ({price_factor}) AS {quoted}")
        elif name in spec.volume_columns:
            expression = f"{quoted} * ({volume_factor})"
            if types[name].upper() in {"BIGINT", "INTEGER", "HUGEINT"}:
                expression = f"CAST(round({expression}) AS {types[name]})"
            projections.append(f"{expression} AS {quoted}")
        elif name == spec.price_factor_column:
            projections.append(f"{quoted} * ({price_factor}) AS {quoted}")
        elif name == spec.volume_factor_column:
            projections.append(f"{quoted} * ({volume_factor}) AS {quoted}")
        elif name == spec.return_column:
            projections.append(
                f"{_return_patch_expression(mechanical, spec.return_column)} AS {quoted}"
            )
        elif name == spec.eligibility_column:
            projections.append(
                f"CASE WHEN {excluded_keys} OR {unexplained_keys} THEN FALSE "
                f"ELSE {quoted} END AS {quoted}"
            )
        elif name == spec.reason_column:
            # Order matters: a row that is both is a known corporate action first.
            projections.append(
                f"CASE WHEN {excluded_keys} THEN '{EXCLUSION_REASON}' "
                f"WHEN {unexplained_keys} THEN '{UNEXPLAINED_REASON}' "
                f"ELSE {quoted} END AS {quoted}"
            )
        elif spec.classification_column and name == spec.classification_column:
            projections.append(
                f"CASE WHEN {repaired_keys} THEN '{REPAIR_NOTE}' ELSE {quoted} END AS {quoted}"
            )
        else:
            projections.append(quoted)

    select = f"SELECT {', '.join(projections)} FROM read_parquet('{_sql(path)}')"
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    con.execute(f"COPY ({select}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    after_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql(temporary)}')"
    ).fetchone()[0]
    if after_rows != before_rows:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(
            f"CA repair changed the row count of {path.name}: {before_rows} -> {after_rows}"
        )
    os.replace(temporary, path)
    return {"file": str(path), "rows": int(after_rows)}


def repair_universe(
    con: duckdb.DuckDBPyConnection, spec: UniverseSpec, *, dry_run: bool = False
) -> dict[str, Any]:
    files = year_files(spec.name)
    if not files:
        # Surfaced rather than swallowed: the production health gate is what decides whether a
        # missing universe is fatal, but the ledger must show that nothing was checked.
        return {
            "action": "NO_SOURCE_FILES",
            "mechanical_count": 0,
            "non_mechanical_count": 0,
            "unapplied_mechanical_events": [],
            "non_mechanical_price_breaks": [],
            "unexplained_price_breaks": [],
            "unexplained_count": 0,
            "files_rewritten": [],
            "verified": None,
        }
    found = detect(con, spec)
    mechanical = found["mechanical"]
    # An event whose ratio does not explain the move is handled like a demerger: nothing is
    # rescaled, the boundary row is simply marked not research-eligible.
    non_mechanical = found["non_mechanical"] + found.get("unrepairable_residual", [])
    unexplained = found.get("unexplained", [])
    entry: dict[str, Any] = {
        "unapplied_mechanical_events": mechanical,
        "non_mechanical_price_breaks": non_mechanical,
        "unrepairable_residual": found.get("unrepairable_residual", []),
        "unexplained_price_breaks": unexplained,
        "mechanical_count": len(mechanical),
        "non_mechanical_count": len(non_mechanical),
        "unexplained_count": len(unexplained),
        "files_rewritten": [],
    }
    if not mechanical and not non_mechanical and not unexplained:
        entry["action"] = "NO_CHANGE"
        entry["verified"] = True
        return entry
    if dry_run:
        entry["action"] = "DRY_RUN"
        entry["verified"] = None
        return entry

    # A mechanical event rescales every year up to and including its ex-date year - but only
    # the years that actually hold rows for that security. Rewriting a year in which the
    # security never traded produces a byte-different file with identical contents, which
    # churns the published dataset and defeats the publisher's unchanged-year reuse.
    touched: set[int] = set()
    for event in mechanical:
        ex_year = int(event["ex_date"][:4])
        for year, path in files.items():
            if year > ex_year:
                continue
            has_rows = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_sql(path)}') WHERE ISIN = ? AND Date < ?",
                [event["isin"], date.fromisoformat(event["ex_date"])],
            ).fetchone()[0]
            if has_rows:
                touched.add(year)
        touched.add(ex_year)  # the boundary row's stored return has to be patched
    touched.update(int(e["ex_date"][:4]) for e in non_mechanical)
    touched.update(int(e["ex_date"][:4]) for e in unexplained)

    for year in sorted(touched):
        path = files.get(year)
        if path is None:
            continue
        entry["files_rewritten"].append(
            _rewrite_year(
                con,
                path,
                spec,
                mechanical=mechanical,
                non_mechanical=non_mechanical,
                unexplained=unexplained,
            )
        )

    after = detect(con, spec)
    entry["residual_mechanical_after_repair"] = after["mechanical"]
    entry["residual_non_mechanical_after_repair"] = after["non_mechanical"]
    entry["residual_unexplained_after_repair"] = after.get("unexplained", [])
    entry["verified"] = not after["mechanical"]
    entry["action"] = "REPAIRED"
    if after["mechanical"]:
        raise RuntimeError(
            f"CA repair for {spec.name} left events unrepaired: {after['mechanical']}"
        )

    oversized = [
        e for e in mechanical if abs(e["move_after_repair"]) > REPAIRED_MOVE_TOLERANCE
    ]
    entry["boundaries_still_large_after_repair"] = oversized
    return entry


def repair(*, dry_run: bool = False) -> dict[str, Any]:
    started = datetime.now(UTC)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    result: dict[str, Any] = {
        "version": REPAIR_VERSION,
        "generated_at_utc": started.isoformat(),
        "dry_run": dry_run,
        "universes": {},
    }
    for spec in (NIFTY500, NIFTY750):
        result["universes"][spec.name] = repair_universe(con, spec, dry_run=dry_run)
    result["status"] = "SUCCESS"
    result["duration_seconds"] = round((datetime.now(UTC) - started).total_seconds(), 3)
    result["payload_sha256"] = canonical_hash(result)
    atomic_json(paths.LOGS_ROOT / "ca_repair" / "latest_ca_repair.json", result)
    return result


__all__ = ["NIFTY500", "NIFTY750", "UniverseSpec", "detect", "repair", "repair_universe"]
