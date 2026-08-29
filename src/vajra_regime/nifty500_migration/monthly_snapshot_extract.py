from __future__ import annotations

import csv
import io
import os
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from pypdf import PdfReader

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import (
    CHECKPOINT_ROOT,
    DATA_ROOT,
    FOUNDATION_VERSION,
    INVALID_SYMBOL_TOKENS,
)


ARCHIVE_MANIFEST = DATA_ROOT / "10 Provenance" / "official_monthly_index_archive_manifest.csv"
NIFTY500_FILE = re.compile(r"^(?:CNX|NIFTY)[_-]?500_[A-Za-z]{3}\d{4}\.(?:csv|pdf)$", re.IGNORECASE)
PDF_ROW = re.compile(r"^([A-Z0-9][A-Z0-9&.-]{0,24})\s+(.+)$")
PDF_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),\s*(20\d{2})\b",
    re.IGNORECASE,
)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _candidate_names(names: list[str]) -> list[str]:
    return [name for name in names if NIFTY500_FILE.fullmatch(PurePosixPath(name).name)]


def _decode_csv(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", payload, 0, 1, "no supported encoding")


def _parse_csv(payload: bytes) -> tuple[str, list[dict[str, str]]]:
    rows = list(csv.DictReader(io.StringIO(_decode_csv(payload))))
    if not rows or "Symbol" not in rows[0]:
        raise RuntimeError("Official Nifty500 CSV has no Symbol field")
    date_field = next((name for name in ("Date", "DATE", "date") if name in rows[0]), None)
    if date_field is None:
        raise RuntimeError("Official Nifty500 CSV has no snapshot date")
    snapshot_date = datetime.strptime(rows[0][date_field].strip(), "%d-%m-%Y").date().isoformat()
    members = []
    for row in rows:
        symbol = row["Symbol"].strip().upper()
        if not symbol:
            continue
        members.append(
            {
                "symbol": symbol,
                "company_name": row.get("Security Name", "").strip(),
                "industry_as_published": row.get("Industry", "").strip(),
                "snapshot_date": snapshot_date,
            }
        )
    return snapshot_date, members


def _parse_pdf(payload: bytes, known_symbols: set[str] | None = None) -> tuple[str, list[dict[str, str]]]:
    reader = PdfReader(io.BytesIO(payload), strict=False)
    text = "\n".join((page.extract_text() or "") for page in reader.pages)
    date_match = PDF_DATE.search(text)
    if not date_match:
        raise RuntimeError("Official Nifty500 PDF snapshot date was not found")
    snapshot_date = datetime.strptime(" ".join(date_match.groups()), "%B %d %Y").date().isoformat()
    members: list[dict[str, str]] = []
    seen: set[str] = set()
    blocked = {"SYMBOL", "CONSTITUENTS", "INDEX", "CLOSE", "WEIGHTAGE", "SECURITY", "INDUSTRY"}
    for raw_line in text.splitlines():
        line = " ".join(raw_line.replace("\u00a0", " ").split())
        match = PDF_ROW.fullmatch(line)
        if not match:
            continue
        symbol, remainder = match.groups()
        if (
            symbol in blocked
            or symbol in INVALID_SYMBOL_TOKENS
            or symbol in seen
            or not any(character.isalpha() for character in symbol)
        ):
            continue
        # A long company name can wrap before numeric fields. Symbol-first lines remain distinguishable because
        # document headers and continuation lines use title case; the optional vocabulary is a corroboration aid.
        if known_symbols is not None and symbol not in known_symbols and not re.search(r"\d", remainder):
            # Retain unknown historical symbols only when the row itself carries a price/weight numeric field.
            continue
        seen.add(symbol)
        members.append(
            {
                "symbol": symbol,
                "company_name": "",
                "industry_as_published": "",
                "snapshot_date": snapshot_date,
            }
        )
    return snapshot_date, members


def extract_official_monthly_snapshots() -> dict[str, Any]:
    output_dir = DATA_ROOT / "02 Constituent History" / "Official Monthly Snapshots"
    logs_dir = DATA_ROOT / "11 Logs"
    for directory in (output_dir, logs_dir, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)
    with ARCHIVE_MANIFEST.open("r", encoding="utf-8-sig", newline="") as handle:
        archives = [row for row in csv.DictReader(handle) if row["status"] != "FAILED"]

    member_rows: list[dict[str, Any]] = []
    snapshot_rows: list[dict[str, Any]] = []
    for archive in archives:
        archive_path = Path(archive["path"])
        with zipfile.ZipFile(archive_path) as bundle:
            candidates = _candidate_names(bundle.namelist())
            if len(candidates) != 1:
                snapshot_rows.append(
                    {
                        "archive_month": archive["snapshot_month"],
                        "archive_path": str(archive_path),
                        "archive_sha256": archive["sha256"],
                        "status": "NIFTY500_FILE_ABSENT" if not candidates else "MULTIPLE_NIFTY500_FILES",
                        "candidate_count": len(candidates),
                        "candidate_name": "|".join(candidates),
                        "member_count": 0,
                    }
                )
                continue
            name = candidates[0]
            payload = bundle.read(name)
        try:
            if name.casefold().endswith(".csv"):
                snapshot_date, members = _parse_csv(payload)
                parse_method = "OFFICIAL_CSV"
            else:
                snapshot_date, members = _parse_pdf(payload)
                parse_method = "OFFICIAL_PDF_TEXT"
            symbols = [row["symbol"] for row in members]
            if len(symbols) in {500, 501, 502} and len(set(symbols)) == len(symbols):
                status = f"VERIFIED_OFFICIAL_COUNT_{len(symbols)}"
            else:
                status = "COUNT_OR_DUPLICATE_REVIEW"
        except Exception as exc:  # Individual official archives are quarantined rather than blocking all months.
            snapshot_date = ""
            members = []
            parse_method = "FAILED"
            status = f"PARSE_FAILED:{type(exc).__name__}:{exc}"
        for member in members:
            member_rows.append(
                {
                    **member,
                    "archive_month": archive["snapshot_month"],
                    "source_archive": archive_path.name,
                    "source_archive_sha256": archive["sha256"],
                    "source_member_file": name,
                    "parse_method": parse_method,
                    "source_grade": "A_OFFICIAL_MONTHLY_SNAPSHOT",
                }
            )
        snapshot_rows.append(
            {
                "archive_month": archive["snapshot_month"],
                "snapshot_date": snapshot_date,
                "archive_path": str(archive_path),
                "archive_sha256": archive["sha256"],
                "status": status,
                "candidate_count": len(candidates),
                "candidate_name": name,
                "member_count": len(members),
                "unique_member_count": len({row["symbol"] for row in members}),
                "parse_method": parse_method,
            }
        )

    member_rows.sort(key=lambda row: (row["snapshot_date"], row["symbol"]))
    snapshot_rows.sort(key=lambda row: row["archive_month"])
    members_path = output_dir / "nifty500_official_monthly_members.csv"
    snapshots_path = output_dir / "nifty500_official_monthly_snapshot_manifest.csv"
    _write_csv(members_path, member_rows)
    _write_csv(snapshots_path, snapshot_rows)
    verified = [row for row in snapshot_rows if row["status"].startswith("VERIFIED_OFFICIAL_COUNT_")]
    review = [
        row
        for row in snapshot_rows
        if not row["status"].startswith("VERIFIED_OFFICIAL_COUNT_") and row["status"] != "NIFTY500_FILE_ABSENT"
    ]
    absent = [row for row in snapshot_rows if row["status"] == "NIFTY500_FILE_ABSENT"]
    generated = datetime.now(UTC).isoformat()
    status = {
        "status": "COMPLETE_WITH_ARCHIVE_GAPS" if absent or review else "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "archives_inspected": len(archives),
        "verified_500_snapshots": len(verified),
        "nifty500_file_absent_months": len(absent),
        "parse_or_count_review_months": len(review),
        "earliest_verified_snapshot": min((row["snapshot_date"] for row in verified), default=""),
        "latest_verified_snapshot": max((row["snapshot_date"] for row in verified), default=""),
        "members_path": str(members_path),
        "members_sha256": sha256_file(members_path),
        "snapshots_path": str(snapshots_path),
        "snapshots_sha256": sha256_file(snapshots_path),
        "review_records": review,
        "absent_archive_months": [row["archive_month"] for row in absent],
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "official_monthly_snapshot_extraction_status.json", status)
    checkpoint = {
        "phase": 6,
        "name": "OFFICIAL_MONTHLY_NIFTY500_SNAPSHOT_EXTRACTION",
        "status": status["status"],
        "recorded_at_utc": generated,
        "verified_500_snapshots": len(verified),
        "absent_months": len(absent),
        "review_months": len(review),
        "members_sha256": status["members_sha256"],
        "snapshots_sha256": status["snapshots_sha256"],
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_06_official_monthly_snapshot_extraction.json", checkpoint)
    return status
