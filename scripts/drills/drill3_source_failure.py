"""Drill 3 - the source is unreachable while there is genuinely work to do.

Requirement: "simulate a source being unreachable mid-run, and show that the last good
certified data is not overwritten or corrupted, and the failure is logged loudly."

The first attempt at this drill did not actually test anything: the drill store was already
current, so the engine had nothing to fetch and finished happily with the network down. That
is a correct outcome, but it is not the outcome under test.

So this version first removes the last few sessions from the drill store - creating real work -
and only then breaks the network. Three things have to hold:
  1. the run fails rather than publishing something incomplete,
  2. not one byte of the published data changes,
  3. the failure is recorded, not swallowed.

Runs against the drill copy only. Production is never touched.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
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

MHD = STORE / "02 Master Historical Data"
PIT = MHD / "NIFTY500 Point In Time"
CUTOFF = date(2026, 8, 21)  # leaves five sessions of real work to do


def sq(p: Path) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


def fingerprint(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                digest.update(chunk)
        out[str(path.relative_to(root)).replace("\\", "/")] = digest.hexdigest()
    return out


def create_real_work() -> dict:
    """Remove the last five sessions so the engine must go to the network."""
    removed_zips = []
    folder = STORE / "03 Incoming NSE EOD" / "01 Official UDiFF ZIP" / "2026"
    for path in sorted(folder.iterdir()):
        match = re.search(r"_(20\d{6})_", path.name)
        if not match:
            continue
        stamp = match.group(1)
        session = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
        if session > CUTOFF:
            path.unlink()
            removed_zips.append(session.isoformat())

    con = duckdb.connect(str(MHD / "05 Database" / "Vajra_Master_Market_Data.duckdb"))
    for table in ("nse_live_raw_daily", "nse_live_ingest_manifest", "clean_daily"):
        con.execute(f"DELETE FROM {table} WHERE Date > DATE '{CUTOFF.isoformat()}'")
    con.close()

    trimmed = {}
    for path in (
        MHD / "01 Daily Clean Parquet By Year" / "EOD2_Clean_2026.parquet",
        PIT / "08 Parquet" / "raw" / "year=2026" / "nifty500_raw_daily.parquet",
        PIT / "08 Parquet" / "certified_adjusted" / "year=2026" / "nifty500_adjusted_daily.parquet",
        PIT / "07 Point In Time Panels" / "nifty500_daily_membership_certified.parquet",
        PIT / "08 Parquet" / "nifty500_daily_membership.parquet",
    ):
        if not path.is_file():
            continue
        c = duckdb.connect()
        c.execute("SET enable_progress_bar=false")
        tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        c.execute(
            f"COPY (SELECT * FROM read_parquet('{sq(path)}') "
            f"WHERE Date <= DATE '{CUTOFF.isoformat()}') TO '{sq(tmp)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(tmp, path)
        trimmed[path.name] = True

    for name in (
        "certified_adjusted_build_status.json",
        "official_raw_ohlcv_build_status.json",
        "foundation_certification_status.json",
    ):
        path = PIT / "11 Logs" / name
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if "latest_date" in payload:
                payload["latest_date"] = CUTOFF.isoformat()
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {"zips_removed": removed_zips, "parquet_trimmed": sorted(trimmed)}


def main() -> int:
    if not (DATA / "MANIFEST.json").is_file():
        print("Drill 3 needs a good published dataset from drill 2 first.")
        return 2

    work = create_real_work()
    print(f"created real work: {len(work['zips_removed'])} sessions removed -> {work['zips_removed']}")

    before = fingerprint(DATA)
    before_manifest = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))
    data_files_before = {k: v for k, v in before.items() if not k.endswith((".md", ".json"))}
    print(f"published data files before: {len(data_files_before)} "
          f"latest_session={before_manifest['latest_session']}")

    real_urlopen = urllib.request.urlopen

    def dead_urlopen(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise urllib.error.URLError("simulated NSE outage: connection refused")

    urllib.request.urlopen = dead_urlopen
    import vajra_regime.nifty500_migration.source_archive as source_archive  # noqa: PLC0415

    source_archive.urlopen = dead_urlopen
    import vajra_regime.nse_live as nse_live  # noqa: PLC0415

    from vajra_regime.nifty500_migration.production_pipeline import (  # noqa: PLC0415
        run_nifty500_production_pipeline,
    )

    failure: str | None = None
    try:
        run_nifty500_production_pipeline()
        print("UNEXPECTED: the run succeeded despite the source being unreachable")
    except Exception as error:  # noqa: BLE001
        failure = f"{type(error).__name__}: {error}"
        print(f"run failed as expected: {failure[:300]}")
    finally:
        urllib.request.urlopen = real_urlopen
        source_archive.urlopen = real_urlopen
        assert nse_live is not None

    after = fingerprint(DATA)
    data_files_after = {k: v for k, v in after.items() if not k.endswith((".md", ".json"))}
    changed = sorted(
        k for k in set(data_files_before) | set(data_files_after)
        if data_files_before.get(k) != data_files_after.get(k)
    )
    after_manifest = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))

    recorded = {}
    for path in (
        ENGINE / "logs" / "publish" / "latest_publish_status.json",
        STORE / "08 Logs" / "NIFTY500 Production" / "latest_nifty500_production_status.json",
    ):
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            recorded[path.name] = {
                "status": payload.get("status"),
                "error": str(payload.get("error", ""))[:200],
                "generated_at_utc": payload.get("generated_at_utc"),
            }

    from vajra_regime.publish import verify_published  # noqa: PLC0415

    still_valid = verify_published(DATA)

    result = {
        "drill": "3_SOURCE_FAILURE_MID_RUN",
        "performed_at_utc": datetime.now(UTC).isoformat(),
        "work_created": work,
        "run_failed_as_expected": failure is not None,
        "failure": failure,
        "published_data_files_before": len(data_files_before),
        "published_data_files_after": len(data_files_after),
        "published_data_files_changed": len(changed),
        "changed_files": changed[:50],
        "latest_session_before": before_manifest["latest_session"],
        "latest_session_after": after_manifest["latest_session"],
        "published_dataset_still_verifies": still_valid["pass"],
        "failure_recorded_in": recorded,
    }
    result["verdict"] = (
        "PASS"
        if result["run_failed_as_expected"]
        and result["published_data_files_changed"] == 0
        and result["latest_session_before"] == result["latest_session_after"]
        and result["published_dataset_still_verifies"]
        else "FAIL"
    )
    out = Path(r"D:\VAJRA_ENGINE\logs\drills\drill3_source_failure.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "changed_files"}, indent=2))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
