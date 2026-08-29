from __future__ import annotations

from pathlib import Path

from vajra_regime.nifty500_migration.certified_adjusted import _paths


def test_paths_only_select_expected_annual_files(tmp_path: Path) -> None:
    expected = tmp_path / "08 Parquet" / "raw" / "year=2020" / "nifty500_raw_daily.parquet"
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"x")
    ignored = expected.parent / "other.parquet"
    ignored.write_bytes(b"x")
    assert _paths(tmp_path, "raw") == [expected]
