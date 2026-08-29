from __future__ import annotations

from datetime import date

from vajra_regime.nifty500_migration.bhavcopy_archive import bhavcopy_url, official_archive_url


def test_bhavcopy_url_uses_official_archive_convention() -> None:
    assert bhavcopy_url(date(2009, 1, 1)) == (
        "https://nsearchives.nseindia.com/content/historical/EQUITIES/2009/JAN/cm01JAN2009bhav.csv.zip"
    )


def test_recent_sessions_use_official_udiff_archive() -> None:
    assert official_archive_url(date(2025, 1, 2)).endswith(
        "/BhavCopy_NSE_CM_0_0_0_20250102_F_0000.csv.zip"
    )
