from __future__ import annotations

from datetime import date

from vajra_regime.nifty500_migration.secondary_archive import _download_url


def test_secondary_download_url_is_bounded_and_encoded() -> None:
    url = _download_url(date(2026, 8, 13))
    assert url.startswith("https://niftyhistory.in/download?")
    assert "index_type=Nifty+500" in url
    assert "start_date=2008-01-01" in url
    assert "end_date=2026-08-13" in url
