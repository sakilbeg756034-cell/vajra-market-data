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


def _reconciliation_path() -> Path:
    return (
        paths.NIFTY500_PIT
        / "04 Corporate Actions"
        / "nifty500_corporate_action_reconciliation.parquet"
    )


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
        return {"mechanical": [], "non_mechanical": []}
    files = ", ".join(f"'{_sql(p)}'" for p in paths_for_universe)
    rec = _sql(_reconciliation_path())
    con.execute(
        "CREATE OR REPLACE TEMP VIEW _prices AS "
        f'SELECT Date, Symbol, ISIN, Close, "{spec.return_column}" AS Ret, '
        f'"{spec.reason_column}" AS Reason '
        f"FROM read_parquet([{files}])"
    )
    mechanical = con.execute(
        f"""
        SELECT r.ExDate, r.Symbol, r.ISIN, r.ActionType, r.Subject,
               r.PriceFactor, r.VolumeFactor, p.Ret
        FROM read_parquet('{rec}') r
        JOIN _prices p ON p.ISIN = r.ISIN AND p.Date = r.ExDate
        WHERE r.PriceFactor IS NOT NULL AND r.PriceFactor <> 1.0
          AND p.Ret IS NOT NULL
          AND abs(p.Ret) > {UNAPPLIED_MOVE_THRESHOLD}
          AND abs(p.Ret - (r.PriceFactor - 1.0)) < {UNAPPLIED_MATCH_TOLERANCE}
        ORDER BY r.ExDate
        """
    ).fetchall()
    non_mechanical = con.execute(
        f"""
        SELECT r.ExDate, r.Symbol, r.ISIN, r.ActionType, r.Subject, p.Ret
        FROM read_parquet('{rec}') r
        JOIN _prices p ON p.ISIN = r.ISIN AND p.Date = r.ExDate
        WHERE (r.PriceFactor IS NULL OR r.PriceFactor = 1.0)
          AND p.Ret IS NOT NULL AND p.Ret <= -{NON_MECHANICAL_MOVE_THRESHOLD}
          -- Already handled on a previous run. Without this the pass would rewrite the same
          -- year files every single day and defeat the publisher's unchanged-year reuse.
          AND (p.Reason IS NULL OR p.Reason <> '{EXCLUSION_REASON}')
        ORDER BY r.ExDate
        """
    ).fetchall()
    return {
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
            projections.append(f"CASE WHEN {excluded_keys} THEN FALSE ELSE {quoted} END AS {quoted}")
        elif name == spec.reason_column:
            projections.append(
                f"CASE WHEN {excluded_keys} THEN '{EXCLUSION_REASON}' ELSE {quoted} END AS {quoted}"
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
            "files_rewritten": [],
            "verified": None,
        }
    found = detect(con, spec)
    mechanical = found["mechanical"]
    non_mechanical = found["non_mechanical"]
    entry: dict[str, Any] = {
        "unapplied_mechanical_events": mechanical,
        "non_mechanical_price_breaks": non_mechanical,
        "mechanical_count": len(mechanical),
        "non_mechanical_count": len(non_mechanical),
        "files_rewritten": [],
    }
    if not mechanical and not non_mechanical:
        entry["action"] = "NO_CHANGE"
        entry["verified"] = True
        return entry
    if dry_run:
        entry["action"] = "DRY_RUN"
        entry["verified"] = None
        return entry

    # A mechanical event rescales every year up to and including its ex-date year. A
    # non-mechanical exclusion touches only its own year.
    touched: set[int] = set()
    for event in mechanical:
        ex_year = int(event["ex_date"][:4])
        touched.update(y for y in files if y <= ex_year)
    touched.update(int(e["ex_date"][:4]) for e in non_mechanical)

    for year in sorted(touched):
        path = files.get(year)
        if path is None:
            continue
        entry["files_rewritten"].append(
            _rewrite_year(
                con, path, spec, mechanical=mechanical, non_mechanical=non_mechanical
            )
        )

    after = detect(con, spec)
    entry["residual_mechanical_after_repair"] = after["mechanical"]
    entry["residual_non_mechanical_after_repair"] = after["non_mechanical"]
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
