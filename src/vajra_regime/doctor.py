from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vajra_regime.config import AppConfig
from vajra_regime.data_layout import DataLayout
from vajra_regime.store import VajraStore


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_summary(store: VajraStore, table: str, date_column: str) -> dict[str, Any]:
    quoted_table = _quote_identifier(table)
    quoted_date = _quote_identifier(date_column)
    with store.connect() as connection:
        row = connection.execute(
            f"SELECT COUNT(*), MIN({quoted_date}), MAX({quoted_date}) FROM {quoted_table}"
        ).fetchone()
    return {
        "table": table,
        "rows": int(row[0]),
        "min_date": str(row[1]) if row[1] is not None else None,
        "max_date": str(row[2]) if row[2] is not None else None,
    }


def build_doctor_report(config: AppConfig) -> dict[str, Any]:
    """Inspect local paths and database readiness without changing market data."""
    database_path = Path(config.environment.duckdb_path)
    layout = DataLayout.from_root(config.environment.root)
    directory_status = {
        name: Path(path).exists() for name, path in layout.as_dict().items()
    }
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "root": str(config.environment.root),
        "published_data_root": str(config.environment.published_data_root),
        "published_data_root_exists": Path(config.environment.published_data_root).exists(),
        "master_data_root": str(config.environment.master_data_root),
        "database_path": str(database_path),
        "database_exists": database_path.exists(),
        "data_layout": layout.as_dict(),
        "data_layout_status": directory_status,
        "data_layout_ready": all(directory_status.values()),
        "tables": [],
        "schema": None,
        "table_summaries": [],
        "ready": False,
        "next_action": None,
    }

    if not report["data_layout_ready"]:
        report["next_action"] = (
            "Run SETUP_VAJRA_WINDOWS.bat again. It will create or resume the new "
            "VAJRA_ENGINE store folder without deleting the old data."
        )
        return report

    if not database_path.exists():
        report["next_action"] = (
            "The new rolling-master DuckDB was not found. Re-run SETUP_VAJRA_WINDOWS.bat "
            "to resume the safe legacy-data copy."
        )
        return report

    store = VajraStore(database_path)
    tables = store.list_tables()
    report["tables"] = tables
    daily_table = str(config.data["clean_table"])
    universe_table = str(config.data["universe_table"])
    schema = store.validate_schema(daily_table=daily_table, universe_table=universe_table)
    report["schema"] = schema

    table_set = set(tables)
    if daily_table in table_set:
        report["table_summaries"].append(_table_summary(store, daily_table, "Date"))
    if universe_table in table_set:
        report["table_summaries"].append(
            _table_summary(store, universe_table, "RebalanceDate")
        )

    report["ready"] = bool(schema["ok"])
    if report["ready"]:
        report["next_action"] = (
            "The rolling master is ready. Run `vajra build-features`; live EOD append "
            "will be added in the next phase."
        )
    else:
        report["next_action"] = (
            "The DuckDB exists but expected tables or columns are missing. Review the "
            "doctor report before changing any data."
        )
    return report
