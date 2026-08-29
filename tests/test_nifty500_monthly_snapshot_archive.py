from __future__ import annotations

from datetime import date

from vajra_regime.nifty500_migration.monthly_snapshot_archive import archive_name, iter_months


def test_month_iteration_and_official_name() -> None:
    months = iter_months(date(2013, 11, 1), date(2014, 2, 1))
    assert months == [date(2013, 11, 1), date(2013, 12, 1), date(2014, 1, 1), date(2014, 2, 1)]
    assert archive_name(months[-1]) == "indices_dataFeb2014.zip"
