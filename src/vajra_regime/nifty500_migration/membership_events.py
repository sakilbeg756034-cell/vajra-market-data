from __future__ import annotations

import csv
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


EXACT_NIFTY500_HEADING = re.compile(
    r"^\s*(?:\(?[A-Z0-9]+\)?[.)]?\s+)?(?:S\s*&\s*P\s+)?(?:CNX|NIFTY)\s*500(?:\s+INDEX)?\s*:?\s*$",
    re.IGNORECASE,
)
ANY_INDEX_HEADING = re.compile(
    r"^\s*(?:\(?[A-Z0-9]+\)?[.)]?\s+)?(?:S\s*&\s*P\s+)?(?:CNX|NIFTY)"
    r"(?:\s*[A-Z0-9&-]+){0,6}(?:\s+INDEX)?\s*:?\s*$",
    re.IGNORECASE,
)
EXCLUSION_MARKER = re.compile(r"\b(?:exclud(?:ed|ing)|delet(?:ed|ion|ing)|remov(?:ed|al|ing))\b", re.IGNORECASE)
INCLUSION_MARKER = re.compile(r"\b(?:includ(?:ed|ing)|add(?:ed|ition|ing)|replac(?:ed|ement))\b", re.IGNORECASE)
MONTH_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(20\d{2}|19\d{2})\b",
    re.IGNORECASE,
)
DAY_MONTH_DATE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*"
    r"(20\d{2}|19\d{2})\b",
    re.IGNORECASE,
)
EFFECTIVE_LANGUAGE = re.compile(r"(?:eff\s*ectiv\s*e|w\s*\.\s*e\s*\.\s*f\s*\.)", re.IGNORECASE)
@dataclass(frozen=True)
class MembershipEvent:
    announcement_date: str
    effective_date: str
    source_file: str
    source_sha256: str
    exclusions: tuple[str, ...]
    inclusions: tuple[str, ...]
    parse_method: str
    confidence: str
    section_number: int


def _clean_line(line: str) -> str:
    return " ".join(line.replace("\u00a0", " ").replace("Â", " ").split())


def _is_exact_heading(line: str) -> bool:
    return bool(EXACT_NIFTY500_HEADING.fullmatch(_clean_line(line)))


def _is_other_index_heading(line: str) -> bool:
    cleaned = _clean_line(line)
    if not cleaned or _is_exact_heading(cleaned):
        return False
    if "NIFTY" not in cleaned.upper() and "CNX" not in cleaned.upper():
        return False
    return bool(ANY_INDEX_HEADING.fullmatch(cleaned))


def extract_exact_nifty500_sections(text: str) -> list[tuple[int, int, str]]:
    """Return line-indexed main Nifty 500 sections, never Nifty500-derived indices."""

    lines = text.splitlines()
    headings = [index for index, line in enumerate(lines) if _is_exact_heading(line)]
    sections: list[tuple[int, int, str]] = []
    for ordinal, start in enumerate(headings, start=1):
        end = len(lines)
        for candidate in range(start + 1, len(lines)):
            if _is_other_index_heading(lines[candidate]):
                end = candidate
                break
        section = "\n".join(lines[start:end])
        if EXCLUSION_MARKER.search(section) or INCLUSION_MARKER.search(section):
            sections.append((start, ordinal, section))
    return sections


def _date_candidates(text: str) -> list[tuple[int, date]]:
    candidates: list[tuple[int, date]] = []
    for match in MONTH_DATE.finditer(text):
        parsed = datetime.strptime(" ".join(match.groups()), "%B %d %Y").date()
        candidates.append((match.start(), parsed))
    for match in DAY_MONTH_DATE.finditer(text):
        day, month, year = match.groups()
        parsed = datetime.strptime(f"{month} {day} {year}", "%B %d %Y").date()
        candidates.append((match.start(), parsed))
    return sorted(set(candidates), key=lambda item: item[0])


def infer_effective_date(text_before_section: str, section: str) -> date | None:
    """Prefer the nearest date attached to effective/w.e.f language, never a bare press date."""

    # Long multi-index releases can place Nifty 500 many pages after the shared effective-date sentence.
    # Keep the full prior text and choose the latest effective-language/date pair before this exact section.
    context = (text_before_section + "\n" + section).replace("\n", " ")
    date_candidates = _date_candidates(context)
    if not date_candidates:
        return None
    effective_positions = [match.start() for match in EFFECTIVE_LANGUAGE.finditer(context)]
    scored: list[tuple[int, int, date]] = []
    for position, value in date_candidates:
        prior_effective = [marker for marker in effective_positions if marker <= position]
        if not prior_effective:
            continue
        distance = position - prior_effective[-1]
        if distance <= 220:
            scored.append((prior_effective[-1], -distance, value))
    return max(scored, default=(0, 0, None), key=lambda item: (item[0], item[1]))[2]


def _symbol_at_line_end(line: str, known_symbols: set[str]) -> str | None:
    cleaned = _clean_line(line)
    if not cleaned:
        return None
    token = cleaned.rsplit(" ", 1)[-1].strip("()[],:;").upper()
    if token in known_symbols:
        return token
    return None


def _symbols_between_markers(section: str, known_symbols: set[str]) -> tuple[list[str], list[str]]:
    mode: str | None = None
    exclusions: list[str] = []
    inclusions: list[str] = []
    for line in section.splitlines():
        if EXCLUSION_MARKER.search(line):
            mode = "exclude"
            continue
        if INCLUSION_MARKER.search(line):
            mode = "include"
            continue
        if mode is None:
            continue
        symbol = _symbol_at_line_end(line, known_symbols)
        if symbol is None:
            continue
        target = exclusions if mode == "exclude" else inclusions
        if symbol not in target:
            target.append(symbol)
    return exclusions, inclusions


def parse_membership_events(
    *,
    text: str,
    source_file: str,
    source_sha256: str,
    announcement_date: str,
    known_symbols: Iterable[str],
) -> list[MembershipEvent]:
    symbols = {str(symbol).strip().upper() for symbol in known_symbols if str(symbol).strip()}
    lines = text.splitlines()
    events: list[MembershipEvent] = []
    for start_line, ordinal, section in extract_exact_nifty500_sections(text):
        effective = infer_effective_date("\n".join(lines[:start_line]), section)
        exclusions, inclusions = _symbols_between_markers(section, symbols)
        if effective is None or not exclusions or not inclusions:
            continue
        confidence = "PARSED_BALANCED" if len(exclusions) == len(inclusions) else "PARSED_UNBALANCED_REVIEW"
        events.append(
            MembershipEvent(
                announcement_date=announcement_date,
                effective_date=effective.isoformat(),
                source_file=source_file,
                source_sha256=source_sha256,
                exclusions=tuple(exclusions),
                inclusions=tuple(inclusions),
                parse_method="OFFICIAL_TEXT_EXACT_SECTION_V1",
                confidence=confidence,
                section_number=ordinal,
            )
        )
    return events


def event_to_csv_row(event: MembershipEvent) -> dict[str, str | int]:
    payload = asdict(event)
    payload["exclusions"] = ",".join(event.exclusions)
    payload["inclusions"] = ",".join(event.inclusions)
    payload["exclusion_count"] = len(event.exclusions)
    payload["inclusion_count"] = len(event.inclusions)
    return payload


def load_symbol_vocabulary(csv_path: Path) -> set[str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {row["Symbol"].strip().upper() for row in csv.DictReader(handle) if row.get("Symbol")}
