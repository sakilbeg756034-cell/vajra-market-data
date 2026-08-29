from __future__ import annotations

import os
from pathlib import Path

import pytest

from vajra_regime.config import AppConfig, EnvironmentSettings
from vajra_regime.master_safety import (
    LEGACY_DATABASE_NAME,
    compare_protected_tree_fingerprints,
    ensure_mutable_master,
    fingerprint_protected_tree,
    guard_protected_tree,
)


def _config(tmp_path: Path, master: Path) -> AppConfig:
    environment = EnvironmentSettings(
        root=tmp_path,
        published_data_root=tmp_path / "legacy",
        master_data_root=tmp_path / "02 Master Historical Data",
        duckdb_path=master,
        backtest_export_dir=tmp_path / "05 Backtesting Export",
        output_dir=tmp_path / "06 Research Results",
        dashboard_export_dir=tmp_path / "07 Dashboard Export",
        logs_dir=tmp_path / "08 Logs",
        backup_dir=tmp_path / "09 Backups",
        config_path=Path("config/default.yaml"),
    )
    raw = {
        "project": {},
        "data": {},
        "features": {},
        "regime": {},
        "strategy": {},
        "research": {},
    }
    return AppConfig(environment=environment, raw=raw)


def test_ensure_mutable_master_breaks_hardlink(tmp_path: Path) -> None:
    database_dir = tmp_path / "02 Master Historical Data" / "05 Database"
    database_dir.mkdir(parents=True)
    legacy_copy = database_dir / LEGACY_DATABASE_NAME
    legacy_copy.write_bytes(b"vajra-database-placeholder")
    master = database_dir / "Vajra_Master_Market_Data.duckdb"
    os.link(legacy_copy, master)
    assert os.path.samefile(legacy_copy, master)

    report = ensure_mutable_master(_config(tmp_path, master))

    assert report["detached"] is True
    assert not os.path.samefile(legacy_copy, master)
    assert legacy_copy.read_bytes() == b"vajra-database-placeholder"
    assert master.read_bytes() == b"vajra-database-placeholder"


def test_ensure_mutable_master_leaves_independent_file(tmp_path: Path) -> None:
    database_dir = tmp_path / "02 Master Historical Data" / "05 Database"
    database_dir.mkdir(parents=True)
    legacy_copy = database_dir / LEGACY_DATABASE_NAME
    legacy_copy.write_bytes(b"legacy")
    master = database_dir / "Vajra_Master_Market_Data.duckdb"
    master.write_bytes(b"master")

    report = ensure_mutable_master(_config(tmp_path, master))

    assert report["detached"] is False
    assert report["status"] == "MASTER_ALREADY_INDEPENDENT"
    assert master.read_bytes() == b"master"
    assert legacy_copy.read_bytes() == b"legacy"


def test_fast_protected_tree_fingerprint_is_stable_and_detects_change(tmp_path: Path) -> None:
    protected = tmp_path / "Vajra Backtesting"
    nested = protected / "archive"
    nested.mkdir(parents=True)
    source = nested / "prices.csv"
    source.write_text("Date,Close\n2026-08-07,100\n", encoding="utf-8")

    first = fingerprint_protected_tree(protected)
    second = fingerprint_protected_tree(protected)
    stable = compare_protected_tree_fingerprints(first, second)

    assert stable["unchanged"] is True
    assert first["mode"] == "FAST_METADATA"
    assert first["file_count"] == 1

    source.write_text("Date,Close\n2026-08-07,101.25\n", encoding="utf-8")
    third = fingerprint_protected_tree(protected)
    changed = compare_protected_tree_fingerprints(first, third)

    assert changed["modified"] is True


def test_content_fingerprint_detects_same_size_same_timestamp_change(tmp_path: Path) -> None:
    protected = tmp_path / "Vajra Backtesting"
    protected.mkdir()
    source = protected / "same-size.bin"
    source.write_bytes(b"alpha")
    original_stat = source.stat()
    before = fingerprint_protected_tree(protected, include_content_hashes=True)

    source.write_bytes(b"bravo")
    os.utime(
        source,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    after = fingerprint_protected_tree(protected, include_content_hashes=True)

    assert before["mode"] == "CONTENT_SHA256"
    assert compare_protected_tree_fingerprints(before, after)["modified"] is True


def test_protected_tree_guard_fails_closed_on_mutation(tmp_path: Path) -> None:
    protected = tmp_path / "Vajra Backtesting"
    protected.mkdir()
    source = protected / "source.txt"
    source.write_text("original", encoding="utf-8")

    with pytest.raises(RuntimeError, match="fingerprint changed"), guard_protected_tree(protected):
        source.write_text("changed", encoding="utf-8")
