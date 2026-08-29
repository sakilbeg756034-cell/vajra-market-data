from __future__ import annotations

from vajra_regime.nifty500_migration.press_release_extract import PDF_DATE_PATTERN, RELEVANCE_PATTERN


def test_relevance_matches_old_and_new_index_names() -> None:
    assert RELEVANCE_PATTERN.search("S&P CNX 500 Index")
    assert RELEVANCE_PATTERN.search("NIFTY 500")
    assert not RELEVANCE_PATTERN.search("NIFTY Midcap 150")


def test_pdf_filename_date_is_strict() -> None:
    match = PDF_DATE_PATTERN.search("ind_prs16022017_1.pdf")
    assert match is not None
    assert match.groups() == ("16", "02", "2017")
