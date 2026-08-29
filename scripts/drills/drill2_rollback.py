"""Rewind a throwaway copy of the store to simulate a laptop that was off for 30 days.

Nothing here touches production. It operates only on D:/VAJRA_ENGINE/temp/drill_store.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import duckdb

STORE = Path(r"D:\VAJRA_ENGINE\temp\drill_store")
MHD = STORE / "02 Master Historical Data"
PIT = MHD / "NIFTY500 Point In Time"
DB = MHD / "05 Database" / "Vajra_Master_Market_Data.duckdb"

CUTOFF = date(2026, 7, 29)  # 30 calendar days before the real latest session, 2026-08-28


def sq(p: Path) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


def trim_parquet(path: Path, column: str = "Date") -> tuple[int, int]:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{sq(path)}')").fetchone()[0]
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{sq(path)}') "
        f"WHERE \"{column}\" <= DATE '{CUTOFF.isoformat()}') "
        f"TO '{sq(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    after = con.execute(f"SELECT COUNT(*) FROM read_parquet('{sq(tmp)}')").fetchone()[0]
    os.replace(tmp, path)
    return before, after


def main() -> int:
    report: dict = {"cutoff": CUTOFF.isoformat(), "parquet": {}, "duckdb": {}, "files_removed": {}}

    # 1. Source zips and their normalized/validation siblings.
    for folder, pattern in (
        (STORE / "03 Incoming NSE EOD" / "01 Official UDiFF ZIP" / "2026", "*.zip"),
        (STORE / "03 Incoming NSE EOD" / "02 Normalized EQ Parquet" / "2026", "*.parquet"),
        (STORE / "03 Incoming NSE EOD" / "03 Daily Validation Reports" / "2026", "*.json"),
    ):
        removed = 0
        if folder.is_dir():
            for path in folder.iterdir():
                digits = "".join(ch for ch in path.stem if ch.isdigit())[-8:]
                try:
                    session = datetime.strptime(digits, "%Y%m%d").date()
                except ValueError:
                    continue
                if session > CUTOFF:
                    path.unlink()
                    removed += 1
        report["files_removed"][folder.name + "/" + folder.parent.name] = removed

    # 2. The working databases.
    con = duckdb.connect(str(DB))
    for table, column in (
        ("nse_live_raw_daily", "Date"),
        ("nse_live_ingest_manifest", "Date"),
        ("clean_daily", "Date"),
    ):
        before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.execute(f"DELETE FROM {table} WHERE \"{column}\" > DATE '{CUTOFF.isoformat()}'")
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        latest = con.execute(f"SELECT MAX(\"{column}\") FROM {table}").fetchone()[0]
        report["duckdb"][table] = {"before": before, "after": after, "latest": str(latest)}
    con.close()

    # 3. The 2026 parquet partitions and the membership panels.
    targets = [
        MHD / "01 Daily Clean Parquet By Year" / "EOD2_Clean_2026.parquet",
        PIT / "08 Parquet" / "raw" / "year=2026" / "nifty500_raw_daily.parquet",
        PIT / "08 Parquet" / "certified_adjusted" / "year=2026" / "nifty500_adjusted_daily.parquet",
        PIT / "08 Parquet" / "adjusted" / "year=2026" / "nifty500_adjusted_daily.parquet",
        PIT / "08 Parquet" / "nifty500_daily_membership.parquet",
        PIT / "07 Point In Time Panels" / "nifty500_daily_membership_certified.parquet",
    ]
    for path in targets:
        if path.is_file():
            before, after = trim_parquet(path)
            report["parquet"][path.name if "year=" not in str(path) else f"{path.parent.name}/{path.name}"] = {
                "before": before,
                "after": after,
            }

    # 4. Status files. certified_adjusted_build_status.latest_date is what the catch-up reads
    #    to decide where to resume, so it is the one that actually matters.
    for name, keys in (
        ("certified_adjusted_build_status.json", ("latest_date",)),
        ("official_raw_ohlcv_build_status.json", ("latest_date",)),
        ("foundation_certification_status.json", ("latest_date",)),
        ("point_in_time_membership_build_status.json", ("latest_date",)),
    ):
        path = PIT / "11 Logs" / name
        if not path.is_file():
            continue
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        for key in keys:
            if key in payload:
                payload[key] = CUTOFF.isoformat()
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    catchup_path = PIT / "11 Logs" / "incremental_catchup_status.json"
    if catchup_path.is_file():
        payload = json.loads(catchup_path.read_text(encoding="utf-8-sig"))
        payload.update(
            {
                "status": "SIMULATED_30_DAY_GAP",
                "prior_last_clean": CUTOFF.isoformat(),
                "latest_completed_session": CUTOFF.isoformat(),
                "raw_latest_date": CUTOFF.isoformat(),
                "adjusted_latest_date": CUTOFF.isoformat(),
                "foundation_latest_date": CUTOFF.isoformat(),
                "sessions_caught_up": [],
                "session_count": 0,
            }
        )
        catchup_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    out = Path(r"D:\VAJRA_ENGINE\logs\drills\drill2_before.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
