from __future__ import annotations

from pathlib import Path

from vajra_regime.nifty500_migration.source_archive import _PressReleaseParser, _archive_name


def test_press_release_parser_keeps_title_and_pdf() -> None:
    parser = _PressReleaseParser()
    parser.feed('<a href="/Press_Release/ind_prs16022017.pdf"> Replacements in indices </a>')
    assert parser.links == [
        {
            "title": "Replacements in indices",
            "url": "https://www.niftyindices.com/Press_Release/ind_prs16022017.pdf",
        }
    ]


def test_archive_name_is_stable_and_drops_query() -> None:
    assert _archive_name("https://example.test/a/file.pdf?x=1") == "file.pdf"
    assert Path(_archive_name("https://example.test/path")).name == "path"
