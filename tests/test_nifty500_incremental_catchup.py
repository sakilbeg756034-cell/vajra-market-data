from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from vajra_regime.nifty500_migration import incremental_catchup


def test_session_filename_parser() -> None:
    path = Path("BhavCopy_NSE_CM_0_0_0_20260820_F_0000.csv.zip")
    assert date(2026, 8, 20) == incremental_catchup._session_from_zip(path)


@pytest.mark.parametrize("days", [1, 5, 22])
def test_gap_discovery_reports_missing_session(monkeypatch: pytest.MonkeyPatch, days: int) -> None:
    expected = [date(2026, 7, 1 + offset) for offset in range(days + 1)]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args):
            return self

        def fetchall(self):
            return [(value,) for value in expected]

    monkeypatch.setattr(incremental_catchup.duckdb, "connect", lambda *_args, **_kwargs: FakeConnection())
    missing = expected[0]
    available = expected[1:]
    fake_paths = [
        Path(f"BhavCopy_NSE_CM_0_0_0_{value:%Y%m%d}_F_0000.csv.zip")
        for value in available
    ]
    monkeypatch.setattr(Path, "glob", lambda *_args, **_kwargs: iter(fake_paths))
    result = incremental_catchup.discover_local_catchup_sessions(
        date(2026, 6, 30), today=date(2026, 8, 1)
    )
    assert result["missing_source_sessions"] == [missing]
