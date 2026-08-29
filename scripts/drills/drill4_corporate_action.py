"""Drill 4 - a corporate action arrives for a held symbol.

Requirement: "simulate a split arriving for a held symbol, and show the affected year's
Parquet and CSV are both regenerated consistently."

Method: pick a security that is in the NIFTY 500 today, un-adjust its price history before a
chosen date in the store (exactly what an unapplied split looks like), add the matching event
to the corporate-action ledger, then run repair + publish and prove:
  1. the engine detects and repairs it without being told which symbol,
  2. the affected year's Parquet AND CSV are both rewritten,
  3. the two files still hold identical data,
  4. no other year is disturbed.

Runs against the drill copy only.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

STORE = Path(r"D:\VAJRA_ENGINE\temp\drill_store")
DATA = Path(r"D:\VAJRA_ENGINE\temp\drill_data")
ENGINE = Path(r"D:\VAJRA_ENGINE\temp\drill_engine")

os.environ["VAJRA_ENGINE_ROOT"] = str(ENGINE)
os.environ["VAJRA_STORE_ROOT"] = str(STORE)
os.environ["VAJRA_DATA_ROOT"] = str(DATA)
sys.path.insert(0, r"D:\VAJRA_ENGINE\code\src")

PIT = STORE / "02 Master Historical Data" / "NIFTY500 Point In Time"
YEAR = 2024
EX_DATE = date(2024, 6, 10)
SPLIT_FACTOR = 0.2  # a 1:5 split


def sq(p: Path) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


def sha(path: Path) -> str:
    d = hashlib.sha256()
    with path.open("rb") as h:
        for chunk in iter(lambda: h.read(4 * 1024 * 1024), b""):
            d.update(chunk)
    return d.hexdigest()


def main() -> int:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    year_file = PIT / "08 Parquet" / "certified_adjusted" / f"year={YEAR}" / "nifty500_adjusted_daily.parquet"
    if not year_file.is_file():
        print(f"missing {year_file}")
        return 2

    # Pick a liquid symbol that traded on both sides of the chosen date and has no real
    # corporate action there.
    pick = con.execute(
        f"""
        SELECT Symbol, ISIN, COUNT(*) AS n
        FROM read_parquet('{sq(year_file)}')
        WHERE Date BETWEEN DATE '2024-05-01' AND DATE '2024-07-31'
          AND IsResearchEligible AND Volume > 100000
        GROUP BY 1, 2 HAVING COUNT(*) > 50
        ORDER BY 3 DESC, 1
        LIMIT 1
        """
    ).fetchone()
    symbol, isin = pick[0], pick[1]
    print(f"chosen security: {symbol} / {isin}")

    before_prices = con.execute(
        f"""SELECT Date, Close FROM read_parquet('{sq(year_file)}')
            WHERE ISIN = '{isin}' AND Date BETWEEN DATE '2024-06-06' AND DATE '2024-06-12'
            ORDER BY Date"""
    ).fetchall()

    # Remove any event a previous run of this drill injected, so the ledger starts clean.
    rec_path = PIT / "04 Corporate Actions" / "nifty500_corporate_action_reconciliation.parquet"
    stale = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{sq(rec_path)}') WHERE EventId LIKE 'DRILL4%'"
    ).fetchone()[0]
    if stale:
        tmp0 = rec_path.with_name(f".{rec_path.name}.{uuid4().hex}.tmp")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{sq(rec_path)}') "
            f"WHERE EventId NOT LIKE 'DRILL4%') TO '{sq(tmp0)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(tmp0, rec_path)
        print(f"cleared {stale} stale drill event(s) from the ledger")

    # Publish once first, so the baseline snapshot reflects the store exactly. Otherwise a
    # year that was already out of sync would be counted as "changed by the corporate action".
    from vajra_regime.publish import publish_dataset as _sync  # noqa: PLC0415

    _sync()

    published_before = {
        p.name: sha(p)
        for p in sorted((DATA / "nifty500" / "parquet").glob("*.parquet"))
        + sorted((DATA / "nifty500" / "csv").glob("*.csv"))
    }

    # 1. Un-adjust the history before the ex-date: divide by the factor, which is what the
    #    prices look like when a split has happened and nobody has applied it.
    columns = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sq(year_file)}')").fetchall()]
    price_columns = {"Open", "High", "Low", "Close", "PointInTimePriceEligibilityClose"}
    projections = []
    condition = f"ISIN = '{isin}' AND Date < DATE '{EX_DATE.isoformat()}'"
    for name in columns:
        q = f'"{name}"'
        if name in price_columns:
            projections.append(f"CASE WHEN {condition} THEN {q} / {SPLIT_FACTOR} ELSE {q} END AS {q}")
        elif name == "Volume":
            projections.append(
                f"CASE WHEN {condition} THEN CAST(round({q} * {SPLIT_FACTOR}) AS BIGINT) "
                f"ELSE {q} END AS {q}"
            )
        elif name == "AdjustedReturn1D":
            projections.append(
                f"CASE WHEN ISIN = '{isin}' AND Date = DATE '{EX_DATE.isoformat()}' "
                f"THEN (1.0 + {q}) * {SPLIT_FACTOR} - 1.0 ELSE {q} END AS {q}"
            )
        else:
            projections.append(q)
    tmp = year_file.with_name(f".{year_file.name}.{uuid4().hex}.tmp")
    con.execute(
        f"COPY (SELECT {', '.join(projections)} FROM read_parquet('{sq(year_file)}')) "
        f"TO '{sq(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    os.replace(tmp, year_file)

    injected = con.execute(
        f"""SELECT Date, Close, AdjustedReturn1D FROM read_parquet('{sq(year_file)}')
            WHERE ISIN = '{isin}' AND Date BETWEEN DATE '2024-06-06' AND DATE '2024-06-12'
            ORDER BY Date"""
    ).fetchall()
    print("after injecting the unapplied split:")
    for row in injected:
        print(f"  {row[0]}  close={row[1]:.2f}  ret={row[2]}")

    # 2. Add the event to the reconciliation ledger, as the daily corporate-action fetch would.
    rec = PIT / "04 Corporate Actions" / "nifty500_corporate_action_reconciliation.parquet"
    rec_columns = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sq(rec)}')").fetchall()]
    values = {
        "EventId": f"'DRILL4-{uuid4().hex[:16]}'",
        "Symbol": f"'{symbol}'",
        "ISIN": f"'{isin}'",
        "Series": "'EQ'",
        "CompanyName": "'DRILL FOUR TEST'",
        "Subject": "'Fv Split Rs10 To Rs2'",
        "ExDate": f"DATE '{EX_DATE.isoformat()}'",
        "ActionType": "'SPLIT'",
        "PriceFactor": str(SPLIT_FACTOR),
        "VolumeFactor": str(1.0 / SPLIT_FACTOR),
        "CompoundPriceFactor": str(SPLIT_FACTOR),
        "ParseStatus": "'PARSED'",
        "Decision": "'AUTO_READY_VERIFIED_MECHANICAL'",
        "Note": "'injected by drill 4'",
    }
    select = ", ".join(f"{values.get(c, 'NULL')} AS \"{c}\"" for c in rec_columns)
    tmp = rec.with_name(f".{rec.name}.{uuid4().hex}.tmp")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{sq(rec)}') UNION ALL SELECT {select}) "
        f"TO '{sq(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    os.replace(tmp, rec)
    print(f"injected corporate action: {symbol} SPLIT {SPLIT_FACTOR} on {EX_DATE}")

    # 3. Run repair and publish, exactly as the daily job would.
    from vajra_regime.ca_repair import repair  # noqa: PLC0415
    from vajra_regime.publish import publish_dataset  # noqa: PLC0415

    repairs = repair()
    n500 = repairs["universes"]["nifty500"]
    detected = [e for e in n500["unapplied_mechanical_events"] if e["isin"] == isin]
    print(f"repair detected {len(detected)} event(s) for {symbol}: {detected}")

    published = publish_dataset()

    after_prices = con.execute(
        f"""SELECT Date, Close FROM read_parquet('{sq(year_file)}')
            WHERE ISIN = '{isin}' AND Date BETWEEN DATE '2024-06-06' AND DATE '2024-06-12'
            ORDER BY Date"""
    ).fetchall()

    published_after = {
        p.name: sha(p)
        for p in sorted((DATA / "nifty500" / "parquet").glob("*.parquet"))
        + sorted((DATA / "nifty500" / "csv").glob("*.csv"))
    }
    changed = sorted(k for k in published_after if published_before.get(k) != published_after[k])

    manifest = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))
    year_entry = next(
        y for y in manifest["universes"]["nifty500"]["years"] if y["year"] == YEAR
    )

    result = {
        "drill": "4_CORPORATE_ACTION_ARRIVES",
        "performed_at_utc": datetime.now(UTC).isoformat(),
        "symbol": symbol,
        "isin": isin,
        "ex_date": EX_DATE.isoformat(),
        "price_factor": SPLIT_FACTOR,
        "closes_original": [[str(d), round(c, 2)] for d, c in before_prices],
        "closes_after_injection": [[str(r[0]), round(r[1], 2)] for r in injected],
        "closes_after_repair": [[str(d), round(c, 2)] for d, c in after_prices],
        "repair_detected_the_event": bool(detected),
        "published_files_changed": changed,
        "published_parquet_and_csv_both_changed": (
            f"nifty500_{YEAR}.parquet" in changed and f"nifty500_{YEAR}.csv" in changed
        ),
        # A split rescales the security's whole history, so every prior year holding rows
        # for it is expected to change. What must NOT change is a year it never traded in.
        "years_changed": sorted({int(name.split("_")[1][:4]) for name in changed}),
        "parquet_csv_identical_after": year_entry["parquet_csv_identical"],
        "publish_status": published["status"],
        "publish_verification_pass": published["verification"]["pass"],
    }
    expected_years = sorted(
        r[0]
        for r in con.execute(
            "SELECT DISTINCT EXTRACT(year FROM Date)::INTEGER FROM read_parquet('"
            + sq(PIT / "08 Parquet" / "certified_adjusted" / "*" / "*.parquet")
            + f"') WHERE ISIN = '{isin}' AND Date <= DATE '{EX_DATE.isoformat()}'"
        ).fetchall()
    )
    result["years_holding_the_security_before_the_ex_date"] = expected_years
    result["no_unrelated_year_changed"] = set(result["years_changed"]) <= set(expected_years)
    restored = [round(c, 2) for _, c in after_prices] == [round(c, 2) for _, c in before_prices]
    result["prices_restored_to_original"] = restored
    result["verdict"] = (
        "PASS"
        if result["repair_detected_the_event"]
        and result["published_parquet_and_csv_both_changed"]
        and result["no_unrelated_year_changed"]
        and YEAR in result["years_changed"]
        and result["parquet_csv_identical_after"]
        and result["publish_verification_pass"]
        and restored
        else "FAIL"
    )
    out = Path(r"D:\VAJRA_ENGINE\logs\drills\drill4_corporate_action.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
