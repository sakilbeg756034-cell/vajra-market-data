from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from typing import Any

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import CHECKPOINT_ROOT, DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.source_archive import _download, _write_csv


BASE_URL = "https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/"
FIRST_AVAILABLE_MONTH = date(2013, 4, 1)
LAST_COMPLETED_MONTH = date(2026, 7, 1)


def iter_months(start: date, end: date) -> list[date]:
    months: list[date] = []
    current = date(start.year, start.month, 1)
    final = date(end.year, end.month, 1)
    while current <= final:
        months.append(current)
        current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
    return months


def archive_name(month: date) -> str:
    return f"indices_data{month.strftime('%b')}{month.year}.zip"


def archive_official_monthly_snapshots() -> dict[str, Any]:
    raw_dir = DATA_ROOT / "01 Raw Source Archives" / "Official Monthly Index Weightage Archives"
    provenance_dir = DATA_ROOT / "10 Provenance"
    logs_dir = DATA_ROOT / "11 Logs"
    for directory in (raw_dir, provenance_dir, logs_dir, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)
    months = iter_months(FIRST_AVAILABLE_MONTH, LAST_COMPLETED_MONTH)
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {}
        for month in months:
            name = archive_name(month)
            url = BASE_URL + name
            futures[pool.submit(_download, url, raw_dir / name, expected_type="zip")] = (month, url)
        for completed, future in enumerate(as_completed(futures), start=1):
            month, url = futures[future]
            records.append(
                {
                    **future.result(),
                    "source_tier": "A_AUTHORITATIVE_NSE_INDICES",
                    "snapshot_month": month.isoformat(),
                    "title": "Official monthly indices market capitalisation and weightage archive",
                    "url": url,
                }
            )
            if completed % 24 == 0:
                print(f"Official monthly index archives: {completed}/{len(futures)}", flush=True)
    records.sort(key=lambda row: row["snapshot_month"])
    manifest_path = provenance_dir / "official_monthly_index_archive_manifest.csv"
    _write_csv(manifest_path, records)
    failures = [row for row in records if row["status"] == "FAILED"]
    successes = [row for row in records if row["status"] != "FAILED"]
    generated = datetime.now(UTC).isoformat()
    status = {
        "status": "COMPLETE" if not failures else "COMPLETE_WITH_MISSING_MONTHS",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "first_requested_month": FIRST_AVAILABLE_MONTH.isoformat(),
        "last_requested_month": LAST_COMPLETED_MONTH.isoformat(),
        "requested_months": len(months),
        "successful_or_cached_archives": len(successes),
        "failed_months": [row["snapshot_month"] for row in failures],
        "failure_records": failures,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "official_monthly_index_archive_status.json", status)
    checkpoint = {
        "phase": 5,
        "name": "OFFICIAL_MONTHLY_INDEX_SNAPSHOT_ARCHIVE",
        "status": status["status"],
        "recorded_at_utc": generated,
        "requested_months": len(months),
        "successful_months": len(successes),
        "failed_months": len(failures),
        "manifest_sha256": status["manifest_sha256"],
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_05_official_monthly_index_archive.json", checkpoint)
    return status
