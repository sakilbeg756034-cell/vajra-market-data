from __future__ import annotations

from vajra_regime.nifty500_migration.historical_name_map import (
    extract_name_symbol_from_layout_line,
    company_name_aliases,
    normalize_company_name,
    unique_name_symbol_map,
)


def test_normalize_company_name_handles_ocr_spacing_and_suffixes() -> None:
    assert normalize_company_name("P irama l H ea lth ca re L td.") == normalize_company_name(
        "Piramal Healthcare Limited"
    )
    assert normalize_company_name("Procter & Gamble Hygiene") == normalize_company_name(
        "Procter and Gamble Hygiene"
    )
    assert "BONGAIGAONREFINERYANDPETROCHEMICALS" in company_name_aliases(
        "Bongaigaon Refinery & Petrochemicals Ltd"
    )


def test_unique_name_symbol_map_quarantines_conflicts() -> None:
    rows = [
        {"normalized_company_name": "ABC", "symbol": "ONE"},
        {"normalized_company_name": "ABC", "symbol": "TWO"},
        {"normalized_company_name": "XYZ", "symbol": "THREE"},
    ]
    resolved, conflicts = unique_name_symbol_map(rows)
    assert resolved == {"XYZ": "THREE"}
    assert conflicts == {"ABC": {"ONE", "TWO"}}


def test_extract_name_symbol_from_layout_line_uses_explicit_official_symbol() -> None:
    line = " 14   Maharashtra Scooters Ltd.                MAHSCOOTER"
    assert extract_name_symbol_from_layout_line(line, {"MAHSCOOTER"}) == (
        "Maharashtra Scooters Ltd",
        "MAHSCOOTER",
    )
