from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from uuid import uuid4

from pypdf import PdfReader

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION, INVALID_SYMBOL_TOKENS
from vajra_regime.nifty500_migration.historical_name_map import (
    build_official_name_symbol_rows,
    normalize_company_name,
    unique_name_symbol_map,
)
from vajra_regime.nifty500_migration.membership_event_discovery import _known_symbols


ROW = re.compile(r"^\s*(\d{1,3})\s+(.+?)\s*$")
MAIN_HEADING = re.compile(r"^\d*(?:SP)?(?:CNX|NIFTY)500INDEX$")
ANY_HEADING = re.compile(r"^\d*(?:SP)?(?:CNX|NIFTY)[A-Z0-9&-]{0,60}INDEX$")


@dataclass(frozen=True)
class ResolvedName:
    symbol: str
    method: str
    normalized_name: str


def _compact(value: str) -> str:
    return re.sub(r"[^A-Z0-9&]+", "", value.upper()).replace("&", "")


def _layout_sections(pdf_path: Path) -> list[str]:
    reader = PdfReader(pdf_path, strict=False)
    text = "\n".join(page.extract_text(extraction_mode="layout") or "" for page in reader.pages)
    lines = text.splitlines()
    starts = [index for index, line in enumerate(lines) if MAIN_HEADING.fullmatch(_compact(line))]
    sections: list[str] = []
    for start in starts:
        end = len(lines)
        for candidate in range(start + 1, len(lines)):
            compact = _compact(lines[candidate])
            if compact != _compact(lines[start]) and ANY_HEADING.fullmatch(compact):
                end = candidate
                break
        sections.append("\n".join(lines[start:end]))
    return sections


def _marker_mode(line: str) -> str | None:
    compact = re.sub(r"[^A-Z]+", "", line.upper())
    if "FOLLOWING" not in compact:
        return None
    if any(token in compact for token in ("EXCLUDED", "EXCLUDING", "DELETED", "REMOVED")):
        return "exclude"
    if any(token in compact for token in ("INCLUDED", "INCLUDING", "ADDED", "REPLACED")):
        return "include"
    return None


def _strip_common_suffix(value: str) -> str:
    return re.sub(r"(?:PVT)?LTD$", "", value)


def _resolve_name(
    raw_name: str,
    *,
    exact_map: dict[str, str],
    known_symbols: set[str],
) -> ResolvedName | None:
    cleaned = " ".join(raw_name.split()).strip(" -:;,.")
    explicit_tokens = [
        token.strip("()[],:;")
        for token in cleaned.split()
        if token == token.upper() and token.strip("()[],:;").upper() in known_symbols
    ]
    if explicit_tokens:
        symbol = explicit_tokens[-1].upper()
        return ResolvedName(symbol, "OFFICIAL_LAYOUT_EXPLICIT_SYMBOL", normalize_company_name(cleaned))
    normalized = normalize_company_name(cleaned)
    if normalized in exact_map:
        return ResolvedName(exact_map[normalized], "OFFICIAL_NAME_EXACT", normalized)
    stripped = _strip_common_suffix(normalized)
    suffix_matches = {symbol for name, symbol in exact_map.items() if _strip_common_suffix(name) == stripped}
    if len(suffix_matches) == 1:
        return ResolvedName(next(iter(suffix_matches)), "OFFICIAL_NAME_SUFFIX_NORMALIZED", normalized)
    scored = sorted(
        (
            SequenceMatcher(None, stripped, _strip_common_suffix(candidate)).ratio(),
            symbol,
        )
        for candidate, symbol in exact_map.items()
    )
    if not scored or scored[-1][0] < 0.965:
        return None
    best_score, best_symbol = scored[-1]
    second_score = scored[-2][0] if len(scored) > 1 else 0.0
    if best_score - second_score < 0.015:
        return None
    return ResolvedName(best_symbol, f"OFFICIAL_NAME_HIGH_SIMILARITY_{best_score:.3f}", normalized)


def _rows_between_markers(
    section: str,
    *,
    exact_map: dict[str, str],
    known_symbols: set[str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    mode: str | None = None
    resolved: list[dict[str, str]] = []
    unresolved: list[dict[str, str]] = []
    for line in section.splitlines():
        marker = _marker_mode(line)
        if marker:
            mode = marker
            continue
        if mode is None:
            continue
        match = ROW.fullmatch(line)
        if not match:
            continue
        _, raw_name = match.groups()
        result = _resolve_name(raw_name, exact_map=exact_map, known_symbols=known_symbols)
        if result is None:
            unresolved.append({"mode": mode, "raw_name": " ".join(raw_name.split())})
            continue
        resolved.append(
            {
                "mode": mode,
                "raw_name": " ".join(raw_name.split()),
                "normalized_name": result.normalized_name,
                "symbol": result.symbol,
                "resolution_method": result.method,
            }
        )
    return resolved, unresolved


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def resolve_review_events(*, data_root: Path = DATA_ROOT) -> dict[str, Any]:
    history = data_root / "02 Constituent History"
    raw_pdfs = data_root / "01 Raw Source Archives" / "NSE Indices Press Releases"
    review_path = history / "official_nifty500_membership_sections_requiring_review.csv"
    with review_path.open("r", encoding="utf-8-sig", newline="") as handle:
        review_rows = list(csv.DictReader(handle))

    name_rows = build_official_name_symbol_rows(data_root=data_root)
    exact_map, conflicts = unique_name_symbol_map(name_rows)
    known_symbols = _known_symbols() - set(INVALID_SYMBOL_TOKENS)
    event_rows: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []
    section_cache: dict[str, list[str]] = {}
    for review in review_rows:
        source_file = review["source_file"]
        sections = section_cache.setdefault(source_file, _layout_sections(raw_pdfs / source_file))
        section_number = int(review["section_number"])
        if section_number > len(sections):
            unresolved_rows.append({**review, "layout_status": "LAYOUT_SECTION_NOT_FOUND"})
            continue
        resolved, unresolved = _rows_between_markers(
            sections[section_number - 1], exact_map=exact_map, known_symbols=known_symbols
        )
        exclusions = list(dict.fromkeys(row["symbol"] for row in resolved if row["mode"] == "exclude"))
        inclusions = list(dict.fromkeys(row["symbol"] for row in resolved if row["mode"] == "include"))
        effective_date = review["inferred_effective_date"]
        if effective_date and exclusions and len(exclusions) == len(inclusions) and not unresolved:
            event_rows.append(
                {
                    "announcement_date": review["announcement_date"],
                    "effective_date": effective_date,
                    "source_file": source_file,
                    "source_sha256": review["source_sha256"],
                    "section_number": section_number,
                    "exclusions": ",".join(exclusions),
                    "inclusions": ",".join(inclusions),
                    "exclusion_count": len(exclusions),
                    "inclusion_count": len(inclusions),
                    "parse_method": "OFFICIAL_PDF_LAYOUT_NAME_RESOLUTION_V1",
                    "confidence": "VERIFIED_OFFICIAL_LAYOUT_BALANCED",
                    "resolution_methods": ",".join(sorted({row["resolution_method"] for row in resolved})),
                }
            )
        else:
            unresolved_rows.append(
                {
                    **review,
                    "layout_status": "REVIEW_REQUIRED",
                    "layout_exclusions": ",".join(exclusions),
                    "layout_inclusions": ",".join(inclusions),
                    "layout_unresolved": "|".join(f"{row['mode']}:{row['raw_name']}" for row in unresolved),
                    "layout_resolved_rows": len(resolved),
                }
            )

    event_rows.sort(key=lambda row: (row["effective_date"], row["source_file"]))
    unresolved_rows.sort(key=lambda row: (row["announcement_date"], row["source_file"]))
    events_path = history / "official_nifty500_membership_events_layout_resolved_v1.csv"
    unresolved_path = history / "official_nifty500_membership_layout_unresolved_v1.csv"
    _write_csv(events_path, event_rows)
    _write_csv(unresolved_path, unresolved_rows)
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE_WITH_REVIEW" if unresolved_rows else "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "official_name_symbol_rows": len(name_rows),
        "unique_name_mappings": len(exact_map),
        "conflicted_name_mappings_quarantined": len(conflicts),
        "review_sections_input": len(review_rows),
        "layout_balanced_events_resolved": len(event_rows),
        "layout_sections_still_unresolved": len(unresolved_rows),
        "events_path": str(events_path),
        "events_sha256": sha256_file(events_path),
        "unresolved_path": str(unresolved_path),
        "unresolved_sha256": sha256_file(unresolved_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "official_membership_layout_resolution_status.json", status)
    return status
