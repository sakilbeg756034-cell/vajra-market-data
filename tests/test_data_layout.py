from pathlib import Path

from vajra_regime.data_layout import DataLayout


def test_data_layout_uses_stable_numbered_folders(tmp_path: Path) -> None:
    layout = DataLayout.from_root(tmp_path / "store")

    assert layout.protected_source.name == "01 Protected Source Data"
    assert layout.master_data.name == "02 Master Historical Data"
    assert layout.incoming_eod.name == "03 Incoming NSE EOD"
    assert layout.corporate_actions.name == "04 Corporate Actions"
    assert layout.logs.name == "08 Logs"
    assert layout.backups.name == "09 Backups"

    layout.create()
    assert all(path.is_dir() for path in layout.directories())


def test_data_layout_dict_contains_every_operational_area(tmp_path: Path) -> None:
    layout = DataLayout.from_root(tmp_path)
    values = layout.as_dict()

    assert set(values) == {
        "root",
        "protected_source",
        "master_data",
        "incoming_eod",
        "corporate_actions",
        "logs",
        "backups",
    }


def test_data_layout_holds_no_research_or_dashboard_folders(tmp_path: Path) -> None:
    """The engine builds OHLCV and nothing else. Strategy, backtest, scanner and dashboard
    folders were removed in the 2026-08-29 reset and must not come back by accident."""
    layout = DataLayout.from_root(tmp_path)
    for retired in ("backtest_export", "research_results", "dashboard_export", "nifty500_special"):
        assert not hasattr(layout, retired)
