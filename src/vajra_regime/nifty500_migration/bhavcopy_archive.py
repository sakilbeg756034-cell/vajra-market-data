from __future__ import annotations

import csv
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.source_archive import _download
from vajra_regime.nifty500_migration.timeline import MASTER_DB


ARCHIVE_URL = "https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{stamp}bhav.csv.zip"
UDIFF_START = date(2024, 7, 8)
UDIFF_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)


def bhavcopy_url(session: date) -> str:
    return ARCHIVE_URL.format(
        year=session.year,
        month=session.strftime("%b").upper(),
        stamp=session.strftime("%d%b%Y").upper(),
    )


def official_archive_url(session: date) -> str:
    if session >= UDIFF_START:
        return UDIFF_URL.format(yyyymmdd=session.strftime("%Y%m%d"))
    return bhavcopy_url(session)


def _sessions(*, start: date, end: date) -> list[date]:
    with duckdb.connect(str(MASTER_DB), read_only=True) as connection:
        return [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT Date FROM clean_daily WHERE Date BETWEEN ? AND ? ORDER BY Date",
                [start.isoformat(), end.isoformat()],
            ).fetchall()
        ]


def _validate_archive(path: Path, session: date) -> dict[str, Any]:
    with zipfile.ZipFile(path) as bundle:
        bad_member = bundle.testzip()
        names = [name for name in bundle.namelist() if name.casefold().endswith(".csv")]
        if session >= UDIFF_START:
            expected = f"BhavCopy_NSE_CM_0_0_0_{session.strftime('%Y%m%d')}_F_0000.csv".casefold()
        else:
            expected = f"cm{session.strftime('%d%b%Y').upper()}bhav.csv".casefold()
        matching = [name for name in names if Path(name).name.casefold() == expected]
    if bad_member or len(matching) != 1:
        raise RuntimeError(
            f"Invalid official bhavcopy archive {path.name}: bad_member={bad_member}, csv_matches={matching}"
        )
    return {"member_name": matching[0], "zip_validation": "PASS"}


def _archive_one(raw_root: Path, session: date) -> dict[str, Any]:
    url = official_archive_url(session)
    path = raw_root / str(session.year) / Path(url).name
    result = _download(url, path, expected_type="zip")
    result.update(
        {
            "session_date": session.isoformat(),
            "source_tier": "A_AUTHORITATIVE_NSE_ARCHIVE",
            "source_policy": "RAW_EXCHANGE_OHLCV_SOURCE_OF_TRUTH",
        }
    )
    if result["status"] != "FAILED":
        try:
            result.update(_validate_archive(path, session))
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            result["status"] = "FAILED_VALIDATION"
            result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def archive_official_bhavcopies(
    *,
    data_root: Path = DATA_ROOT,
    start: date = date(2009, 1, 1),
    end: date = date(2025, 12, 31),
    workers: int = 16,
) -> dict[str, Any]:
    raw_root = data_root / "01 Raw Source Archives" / "Official NSE Equity Bhavcopy"
    provenance = data_root / "10 Provenance"
    logs = data_root / "11 Logs"
    checkpoints = data_root / "12 Checkpoints"
    for directory in (raw_root, provenance, logs, checkpoints):
        directory.mkdir(parents=True, exist_ok=True)
    sessions = _sessions(start=start, end=end)
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_archive_one, raw_root, session): session for session in sessions}
        for completed, future in enumerate(as_completed(futures), start=1):
            records.append(future.result())
            if completed % 100 == 0:
                print(f"Official NSE bhavcopies archived: {completed}/{len(futures)}", flush=True)
    records.sort(key=lambda row: row["session_date"])
    manifest_path = provenance / "official_nse_bhavcopy_download_manifest.csv"
    _write_csv(manifest_path, records)
    failures = [row for row in records if not str(row["status"]).startswith(("DOWNLOADED", "REUSED"))]
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE" if not failures else "INCOMPLETE_DOWNLOADS",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "expected_sessions": len(sessions),
        "valid_archives": len(records) - len(failures),
        "failed_archives": len(failures),
        "failed_sessions": [row["session_date"] for row in failures],
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "raw_root": str(raw_root),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs / "official_bhavcopy_archive_status.json", status)
    checkpoint = {
        "phase": 7,
        "name": "OFFICIAL_RAW_BHAVCOPY_ARCHIVE_2009_2025",
        "status": status["status"],
        "recorded_at_utc": generated,
        "valid_archives": status["valid_archives"],
        "failed_archives": status["failed_archives"],
        "manifest_sha256": status["manifest_sha256"],
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(checkpoints / "phase_07_official_bhavcopy_archive.json", checkpoint)
    return status
