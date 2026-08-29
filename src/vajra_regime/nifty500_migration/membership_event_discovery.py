from __future__ import annotations

import csv
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import (
    CHECKPOINT_ROOT,
    DATA_ROOT,
    FOUNDATION_VERSION,
    INVALID_SYMBOL_TOKENS,
)
from vajra_regime.nifty500_migration.membership_events import (
    _symbols_between_markers,
    event_to_csv_row,
    extract_exact_nifty500_sections,
    infer_effective_date,
    load_symbol_vocabulary,
    parse_membership_events,
)


MASTER_DB = paths.MASTER_DB
CURRENT_CSV = (
    DATA_ROOT / "01 Raw Source Archives" / "Official Current Constituents" / "ind_nifty500list.csv"
)
SECONDARY_CSV = (
    DATA_ROOT
    / "01 Raw Source Archives"
    / "Secondary Niftyhistory Crosscheck"
    / "nifty500_2008-01-01_to_2026-08-13.csv"
)
OFFICIAL_2008_SYMBOLS = DATA_ROOT / "03 Security Master" / "nse_official_security_master_2008.csv"
OFFICIAL_PRESS_LAYOUT_SYMBOLS = DATA_ROOT / "03 Security Master" / "official_press_release_name_symbol_rows.csv"
RELEVANT_MANIFEST = DATA_ROOT / "10 Provenance" / "nifty500_relevant_press_release_manifest.csv"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _known_symbols(*, include_layout: bool = True) -> set[str]:
    symbols = load_symbol_vocabulary(CURRENT_CSV)
    if OFFICIAL_2008_SYMBOLS.exists():
        with OFFICIAL_2008_SYMBOLS.open("r", encoding="utf-8-sig", newline="") as handle:
            symbols.update(row["symbol"].strip().upper() for row in csv.DictReader(handle) if row.get("symbol"))
    if include_layout and OFFICIAL_PRESS_LAYOUT_SYMBOLS.exists():
        with OFFICIAL_PRESS_LAYOUT_SYMBOLS.open("r", encoding="utf-8-sig", newline="") as handle:
            symbols.update(row["symbol"].strip().upper() for row in csv.DictReader(handle) if row.get("symbol"))
    with duckdb.connect(str(MASTER_DB), read_only=True) as connection:
        symbols.update(
            row[0].strip().upper()
            for row in connection.execute(
                "SELECT DISTINCT Symbol FROM vajra_security_symbol_history WHERE Symbol IS NOT NULL"
            ).fetchall()
            if row[0]
        )
    if SECONDARY_CSV.exists():
        with SECONDARY_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                for column in ("symbols", "inclusions", "exclusions"):
                    symbols.update(token.strip().upper() for token in row[column].split(",") if token.strip())
    return symbols - set(INVALID_SYMBOL_TOKENS)


def discover_official_membership_events(*, as_of: date = date(2026, 8, 13)) -> dict[str, Any]:
    output_dir = DATA_ROOT / "02 Constituent History"
    provenance_dir = DATA_ROOT / "10 Provenance"
    logs_dir = DATA_ROOT / "11 Logs"
    for directory in (output_dir, provenance_dir, logs_dir, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)
    symbols = _known_symbols()
    with RELEVANT_MANIFEST.open("r", encoding="utf-8-sig", newline="") as handle:
        manifest_rows = list(csv.DictReader(handle))

    event_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    exact_sections = 0
    considered_documents = 0
    for row in manifest_rows:
        announcement = row["announcement_date_from_filename"]
        if not announcement or announcement < "2008-01-01" or announcement > as_of.isoformat():
            continue
        considered_documents += 1
        path = Path(row["extracted_text_path"])
        text = path.read_text(encoding="utf-8", errors="replace")
        sections = extract_exact_nifty500_sections(text)
        exact_sections += len(sections)
        events = parse_membership_events(
            text=text,
            source_file=row["file_name"],
            source_sha256=row["sha256"],
            announcement_date=announcement,
            known_symbols=symbols,
        )
        for event in events:
            parsed = event_to_csv_row(event)
            parsed["effective_on_or_before_as_of"] = event.effective_date <= as_of.isoformat()
            event_rows.append(parsed)
        if sections and not events:
            for start, section_number, section in sections:
                inferred_date = infer_effective_date("\n".join(text.splitlines()[:start]), section)
                exclusions, inclusions = _symbols_between_markers(section, symbols)
                review_rows.append(
                    {
                        "announcement_date": announcement,
                        "source_file": row["file_name"],
                        "source_sha256": row["sha256"],
                        "section_number": section_number,
                        "start_line": start,
                        "reason": "EXACT_SECTION_NOT_CONVERTED_TO_BALANCED_EVENT",
                        "has_exclusion_language": "exclud" in section.casefold(),
                        "has_inclusion_language": "includ" in section.casefold(),
                        "inferred_effective_date": inferred_date.isoformat() if inferred_date else "",
                        "parsed_exclusion_count": len(exclusions),
                        "parsed_inclusion_count": len(inclusions),
                        "parsed_exclusions": ",".join(exclusions),
                        "parsed_inclusions": ",".join(inclusions),
                        "section_text_sha256": canonical_hash(section),
                    }
                )

    event_rows.sort(key=lambda row: (row["effective_date"], row["source_file"], row["section_number"]))
    review_rows.sort(key=lambda row: (row["announcement_date"], row["source_file"], row["section_number"]))
    events_path = output_dir / "official_nifty500_membership_events_parsed_v1.csv"
    review_path = output_dir / "official_nifty500_membership_sections_requiring_review.csv"
    _write_csv(events_path, event_rows)
    _write_csv(review_path, review_rows)

    balanced = [row for row in event_rows if row["exclusion_count"] == row["inclusion_count"]]
    effective = [row for row in event_rows if row["effective_on_or_before_as_of"]]
    generated = datetime.now(UTC).isoformat()
    status = {
        "status": "DISCOVERY_COMPLETE_REVIEW_REQUIRED" if review_rows else "DISCOVERY_COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "as_of": as_of.isoformat(),
        "known_symbol_vocabulary": len(symbols),
        "considered_documents": considered_documents,
        "exact_nifty500_sections": exact_sections,
        "parsed_event_sections": len(event_rows),
        "parsed_balanced_event_sections": len(balanced),
        "parsed_effective_on_or_before_as_of": len(effective),
        "sections_requiring_review": len(review_rows),
        "events_path": str(events_path),
        "events_sha256": sha256_file(events_path),
        "review_path": str(review_path),
        "review_sha256": sha256_file(review_path),
        "source_use_policy": "OFFICIAL_EVENT_TEXT_FIRST; SECONDARY_SYMBOLS_VOCABULARY_ONLY",
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "official_membership_event_discovery_status.json", status)
    checkpoint = {
        "phase": 4,
        "name": "OFFICIAL_MEMBERSHIP_EVENT_DISCOVERY",
        "status": status["status"],
        "recorded_at_utc": generated,
        "events_sha256": status["events_sha256"],
        "review_sha256": status["review_sha256"],
        "parsed_events": len(event_rows),
        "review_sections": len(review_rows),
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_04_official_membership_event_discovery.json", checkpoint)
    return status
