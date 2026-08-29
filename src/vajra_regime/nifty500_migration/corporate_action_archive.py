from __future__ import annotations

import csv
import hashlib
import json
import os
import time
import urllib.parse
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.corporate_actions import NSE_CA_API_URL, _fetch_ca_json, _nse_opener
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


def _chunks(start: date, end: date, *, days: int = 90) -> list[tuple[date, date]]:
    output: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=days - 1))
        output.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return output


def _response_rows(payload: bytes) -> list[dict[str, Any]]:
    decoded = json.loads(payload.decode("utf-8-sig"))
    if isinstance(decoded, dict):
        decoded = decoded.get("data", decoded.get("records", []))
    if not isinstance(decoded, list):
        raise ValueError("Unexpected NSE corporate-action response shape")
    return [row for row in decoded if isinstance(row, dict)]


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _atomic_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _event_id(row: dict[str, Any]) -> str:
    identity = "|".join(
        str(row.get(key) or "").strip().upper()
        for key in ("symbol", "isin", "exDate", "subject", "series")
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _clean(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    return "" if value is None else str(value).strip()


def archive_official_corporate_actions(
    *,
    data_root: Path = DATA_ROOT,
    start: date = date(2009, 1, 1),
    as_of: date = date(2026, 8, 13),
    retries: int = 4,
) -> dict[str, Any]:
    raw_root = data_root / "01 Raw Source Archives" / "Official NSE Corporate Actions"
    action_root = data_root / "04 Corporate Actions"
    provenance_root = data_root / "10 Provenance"
    logs_root = data_root / "11 Logs"
    checkpoint_root = data_root / "12 Checkpoints"
    for directory in (raw_root, action_root, provenance_root, logs_root, checkpoint_root):
        directory.mkdir(parents=True, exist_ok=True)

    opener = None
    manifest: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for index, (chunk_start, chunk_end) in enumerate(_chunks(start, as_of), start=1):
        path = raw_root / str(chunk_start.year) / (
            f"corporate_actions_{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}.json"
        )
        cache_reused = False
        payload: bytes | None = None
        if path.exists():
            try:
                payload = path.read_bytes()
                _response_rows(payload)
                cache_reused = True
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
                payload = None
        if payload is None:
            last_error: Exception | None = None
            for attempt in range(1, retries + 1):
                try:
                    if opener is None:
                        opener = _nse_opener()
                    payload, _ = _fetch_ca_json(opener, chunk_start, chunk_end)
                    _response_rows(payload)
                    _atomic_bytes(path, payload)
                    break
                except Exception as error:  # noqa: BLE001 - bounded source retry ledger
                    last_error = error
                    opener = None
                    if attempt < retries:
                        time.sleep(min(2 ** (attempt - 1), 8))
            if payload is None:
                raise RuntimeError(
                    f"NSE corporate-action download failed for {chunk_start}..{chunk_end}: {last_error}"
                )
        rows = _response_rows(payload)
        source_hash = sha256_file(path)
        query = urllib.parse.urlencode(
            {
                "index": "equities",
                "from_date": chunk_start.strftime("%d-%m-%Y"),
                "to_date": chunk_end.strftime("%d-%m-%Y"),
            }
        )
        manifest.append(
            {
                "chunk_start": chunk_start.isoformat(),
                "chunk_end": chunk_end.isoformat(),
                "source_url": f"{NSE_CA_API_URL}?{query}",
                "archive_path": str(path),
                "archive_sha256": source_hash,
                "archive_bytes": len(payload),
                "response_rows": len(rows),
                "cache_reused": cache_reused,
                "source_grade": "A_OFFICIAL_NSE",
            }
        )
        for row in rows:
            all_rows.append(
                {
                    "EventId": _event_id(row),
                    "Symbol": _clean(row, "symbol").upper(),
                    "ISIN": _clean(row, "isin").upper(),
                    "Series": _clean(row, "series").upper(),
                    "CompanyName": _clean(row, "comp"),
                    "Subject": _clean(row, "subject"),
                    "FaceValue": _clean(row, "faceVal"),
                    "ExDate": _clean(row, "exDate"),
                    "RecordDate": _clean(row, "recDate"),
                    "SourceArchive": path.name,
                    "SourceSha256": source_hash,
                    "RawJson": json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
                }
            )
        print(f"Official corporate actions: {index}/{len(_chunks(start, as_of))}", flush=True)

    manifest_path = provenance_root / "official_nse_corporate_action_source_manifest.csv"
    _atomic_csv(manifest_path, manifest)
    actions_csv = action_root / "nifty500_official_corporate_actions_all_equities.csv"
    deduplicated = {row["EventId"]: row for row in all_rows}
    action_rows = sorted(deduplicated.values(), key=lambda row: (row["ExDate"], row["Symbol"], row["EventId"]))
    _atomic_csv(actions_csv, action_rows)
    actions_parquet = action_root / "nifty500_official_corporate_actions_all_equities.parquet"
    temporary = actions_parquet.with_name(f".{actions_parquet.name}.{uuid4().hex}.partial")
    actions_csv_sql = str(actions_csv).replace("'", "''")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                SELECT EventId, Symbol, NULLIF(ISIN, '') AS ISIN, Series, CompanyName, Subject,
                       FaceValue, TRY_STRPTIME(ExDate, '%d-%b-%Y')::DATE AS ExDate,
                       TRY_STRPTIME(RecordDate, '%d-%b-%Y')::DATE AS RecordDate,
                       SourceArchive, SourceSha256, RawJson
                FROM read_csv_auto('{actions_csv_sql}', header=true, all_varchar=true)
                WHERE TRY_STRPTIME(ExDate, '%d-%b-%Y') IS NOT NULL
                ORDER BY ExDate, Symbol, EventId
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        summary = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT EventId), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                   MIN(ExDate), MAX(ExDate), SUM(ISIN IS NULL)
            FROM read_parquet(?)
            """,
            [str(temporary)],
        ).fetchone()
    os.replace(temporary, actions_parquet)
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "requested_start": start.isoformat(),
        "requested_end": as_of.isoformat(),
        "chunks": len(manifest),
        "events": summary[0],
        "unique_events": summary[1],
        "symbols": summary[2],
        "isins": summary[3],
        "earliest_ex_date": str(summary[4]),
        "latest_ex_date": str(summary[5]),
        "missing_isin_events": summary[6],
        "actions_parquet": str(actions_parquet),
        "actions_parquet_sha256": sha256_file(actions_parquet),
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "source_policy": "OFFICIAL_NSE_ALL_EQUITY_EVENTS; PIT NIFTY500 FILTERING OCCURS DURING RECONCILIATION",
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_root / "official_corporate_action_archive_status.json", status)
    atomic_json(
        checkpoint_root / "phase_05_official_corporate_actions.json",
        {**status, "checkpoint_status": "COMPLETE_HASH_VERIFIED"},
    )
    return status
