from pathlib import Path

import duckdb

from vajra_regime.config import AppConfig, EnvironmentSettings
from vajra_regime.data_layout import DataLayout
from vajra_regime.doctor import build_doctor_report


def _config(database_path: Path, output_dir: Path) -> AppConfig:
    root = database_path.parent
    DataLayout.from_root(root).create()
    environment = EnvironmentSettings(
        root=root,
        duckdb_path=database_path,
        output_dir=output_dir,
        config_path=Path("config/default.yaml"),
    )
    raw = {
        "project": {},
        "data": {
            "clean_table": "clean_daily",
            "universe_table": "monthly_vajra_750_universe",
        },
        "features": {},
        "regime": {},
        "research": {},
    }
    return AppConfig(environment=environment, raw=raw)


def test_doctor_reports_missing_database(tmp_path: Path) -> None:
    config = _config(tmp_path / "missing.duckdb", tmp_path / "output")
    report = build_doctor_report(config)
    assert report["data_layout_ready"] is True
    assert report["database_exists"] is False
    assert report["ready"] is False
    assert report["tables"] == []


def test_doctor_reports_ready_database(tmp_path: Path) -> None:
    database_path = tmp_path / "clean.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE clean_daily (
                Date DATE,
                Symbol VARCHAR,
                ISIN VARCHAR,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Volume BIGINT,
                IsBacktestPeriod BOOLEAN
            )
            """
        )
        connection.execute(
            "INSERT INTO clean_daily VALUES "
            "('2010-08-02', 'TEST', 'INE000A01001', 10, 11, 9, 10.5, 1000, TRUE)"
        )
        connection.execute(
            """
            CREATE TABLE monthly_vajra_750_universe (
                RebalanceDate DATE,
                ISIN VARCHAR,
                LiquidityRank INTEGER
            )
            """
        )
        connection.execute(
            "INSERT INTO monthly_vajra_750_universe VALUES "
            "('2010-07-30', 'INE000A01001', 1)"
        )

    config = _config(database_path, tmp_path / "output")
    report = build_doctor_report(config)
    assert report["data_layout_ready"] is True
    assert report["database_exists"] is True
    assert report["ready"] is True
    assert len(report["table_summaries"]) == 2
