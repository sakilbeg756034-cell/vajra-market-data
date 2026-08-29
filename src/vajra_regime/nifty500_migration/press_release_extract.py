from __future__ import annotations

import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pypdf import PdfReader

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import CHECKPOINT_ROOT, DATA_ROOT, FOUNDATION_VERSION


RELEVANCE_PATTERN = re.compile(r"\b(?:S\s*&\s*P\s+)?CNX\s*500\b|\bNIFTY\s*500\b", re.IGNORECASE)
PDF_DATE_PATTERN = re.compile(r"ind_prs(\d{2})(\d{2})(\d{4})(?:_\d+)?\.pdf$", re.IGNORECASE)


def _extract_one(path: Path) -> dict[str, Any]:
    try:
        reader = PdfReader(path, strict=False)
        text = "\n".join((page.extract_text() or "") for page in reader.pages)
        text = text.replace("\x00", "")
        status = "EXTRACTED" if text.strip() else "EMPTY_TEXT"
        error = ""
    except Exception as exc:  # pypdf exposes several optional parser/decryption exceptions.
        text = ""
        status = "FAILED"
        error = f"{type(exc).__name__}: {exc}"
    match = PDF_DATE_PATTERN.search(path.name)
    announcement_date = ""
    if match:
        day, month, year = match.groups()
        announcement_date = f"{year}-{month}-{day}"
    relevant = bool(RELEVANCE_PATTERN.search(text))
    return {
        "file_name": path.name,
        "path": str(path),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
        "announcement_date_from_filename": announcement_date,
        "page_count": len(reader.pages) if status != "FAILED" else 0,
        "extraction_status": status,
        "extraction_error": error,
        "text_characters": len(text),
        "nifty500_relevant": relevant,
        "text": text,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def extract_press_release_evidence() -> dict[str, Any]:
    press_dir = DATA_ROOT / "01 Raw Source Archives" / "NSE Indices Press Releases"
    extracted_dir = DATA_ROOT / "02 Constituent History" / "Official Press Release Text"
    provenance = DATA_ROOT / "10 Provenance"
    logs = DATA_ROOT / "11 Logs"
    for directory in (extracted_dir, provenance, logs, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)
    pdfs = sorted(press_dir.glob("*.pdf"), key=lambda path: path.name.casefold())
    if len(pdfs) < 1_000:
        raise RuntimeError(f"Official PDF archive is unexpectedly incomplete: {len(pdfs)}")

    extracted: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_extract_one, path): path for path in pdfs}
        for completed, future in enumerate(as_completed(futures), start=1):
            extracted.append(future.result())
            if completed % 200 == 0:
                print(f"Press-release text extracted: {completed}/{len(futures)}", flush=True)
    extracted.sort(key=lambda item: item["file_name"].casefold())

    manifest_rows: list[dict[str, Any]] = []
    relevant_rows: list[dict[str, Any]] = []
    for item in extracted:
        text = item.pop("text")
        if item["nifty500_relevant"]:
            text_path = extracted_dir / f"{Path(item['file_name']).stem}.txt"
            text_path.write_text(text, encoding="utf-8")
            item["extracted_text_path"] = str(text_path)
            item["extracted_text_sha256"] = sha256_file(text_path)
            relevant_rows.append(dict(item))
        manifest_rows.append(dict(item))

    manifest_path = provenance / "press_release_text_extraction_manifest.csv"
    relevant_path = provenance / "nifty500_relevant_press_release_manifest.csv"
    _write_csv(manifest_path, manifest_rows)
    _write_csv(relevant_path, relevant_rows)
    failures = [row for row in manifest_rows if row["extraction_status"] == "FAILED"]
    empty = [row for row in manifest_rows if row["extraction_status"] == "EMPTY_TEXT"]
    generated = datetime.now(UTC).isoformat()
    status = {
        "status": "COMPLETE_WITH_EXTRACT_EXCEPTIONS" if failures or empty else "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "archived_pdf_count": len(pdfs),
        "nifty500_relevant_pdf_count": len(relevant_rows),
        "extract_failure_count": len(failures),
        "empty_text_pdf_count": len(empty),
        "extract_failures": failures,
        "empty_text_files": empty,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "relevant_manifest_path": str(relevant_path),
        "relevant_manifest_sha256": sha256_file(relevant_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    status_path = logs / "press_release_extraction_status.json"
    atomic_json(status_path, status)
    checkpoint = {
        "phase": 2,
        "name": "OFFICIAL_NIFTY500_PRESS_RELEASE_EXTRACTION",
        "status": status["status"],
        "recorded_at_utc": generated,
        "pdfs": len(pdfs),
        "relevant_pdfs": len(relevant_rows),
        "extract_failures": len(failures),
        "empty_text": len(empty),
        "outputs": {
            "manifest": str(manifest_path),
            "manifest_sha256": status["manifest_sha256"],
            "relevant_manifest": str(relevant_path),
            "relevant_manifest_sha256": status["relevant_manifest_sha256"],
        },
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_02_press_release_extraction.json", checkpoint)
    return status
