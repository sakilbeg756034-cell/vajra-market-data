from __future__ import annotations

from vajra_regime.nifty500_migration.review_event_resolver import _marker_mode, _resolve_name, _rows_between_markers


def test_marker_mode_handles_layout_ocr_spacing() -> None:
    assert _marker_mode("T h e fo llo w in g co mp a n ies a re b e in gin c lu d e d :") == "include"
    assert _marker_mode("The following companies are being excluded:") == "exclude"


def test_rows_between_markers_resolves_layout_company_names() -> None:
    section = """
    The following company is being excluded:
      1    S a tya m C o m p u ter Services Ltd.
    The following company is being included:
      1    Cairn India Ltd.
    """
    exact = {
        "SATYAMCOMPUTERSERVICESLTD": "SATYAM",
        "CAIRNINDIALTD": "CAIRN",
    }
    rows, unresolved = _rows_between_markers(section, exact_map=exact, known_symbols={"SATYAM", "CAIRN"})
    assert unresolved == []
    assert [(row["mode"], row["symbol"]) for row in rows] == [("exclude", "SATYAM"), ("include", "CAIRN")]


def test_resolve_name_does_not_guess_ambiguous_fuzzy_match() -> None:
    exact = {"EXAMPLEALTD": "ONE", "EXAMPLEBLTD": "TWO"}
    assert _resolve_name("Example Ltd", exact_map=exact, known_symbols=set()) is None


def test_resolve_name_finds_explicit_symbol_before_effective_date() -> None:
    result = _resolve_name(
        "Zuari Industries Limited ZUARIAGRO April 9, 2012",
        exact_map={},
        known_symbols={"ZUARIAGRO"},
    )
    assert result is not None
    assert result.symbol == "ZUARIAGRO"
