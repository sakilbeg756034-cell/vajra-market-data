from __future__ import annotations

from vajra_regime.nifty500_migration.membership_events import (
    extract_exact_nifty500_sections,
    infer_effective_date,
    parse_membership_events,
)


def test_exact_section_excludes_derived_nifty500_index() -> None:
    text = """
Date: 1 January 2025
Changes effective from March 31, 2025.
2) Nifty 500 Index
The following companies are being excluded:
1 Old Company OLD
The following companies are being included:
1 New Company NEW
3) Nifty500 Equal Weight Index
The following company is being excluded:
1 Noise Company NOISE
"""
    sections = extract_exact_nifty500_sections(text)
    assert len(sections) == 1
    assert "NOISE" not in sections[0][2]


def test_effective_date_requires_effective_language() -> None:
    before = "Date: 16 February 2017. Changes shall become effective from March 31, 2017."
    assert infer_effective_date(before, "Nifty 500 Index") is not None
    assert infer_effective_date("Date: 16 February 2017.", "Nifty 500 Index") is None


def test_parse_balanced_official_event() -> None:
    text = """
Date: 16 February 2017
These changes shall become effective from March 31, 2017 (close of March 30, 2017).
2) NIFTY 500 Index
The following companies are being excluded:
Sr. No. Company Name Symbol
1 Old Industries Ltd. OLD
2 Gone Ltd. GONE
The following companies are being included:
Sr. No. Company Name Symbol
1 New Industries Ltd. NEW
2 Fresh Ltd. FRESH
3) NIFTY 100
The following company is being excluded:
1 Ignore Ltd. IGNORE
"""
    events = parse_membership_events(
        text=text,
        source_file="official.pdf",
        source_sha256="a" * 64,
        announcement_date="2017-02-16",
        known_symbols={"OLD", "GONE", "NEW", "FRESH", "IGNORE"},
    )
    assert len(events) == 1
    assert events[0].effective_date == "2017-03-31"
    assert events[0].exclusions == ("OLD", "GONE")
    assert events[0].inclusions == ("NEW", "FRESH")
    assert events[0].confidence == "PARSED_BALANCED"
