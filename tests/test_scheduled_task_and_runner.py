"""The daily entry point and the scheduled task that runs it.

The two tasks this replaces both failed in ways that were invisible to the operator, so the
settings that fix them are asserted here rather than left to a comment.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "windows" / "run_vajra_data_engine.ps1"
INSTALLER = REPO_ROOT / "scripts" / "windows" / "install_vajra_data_engine_task.ps1"


def test_config_holds_no_filesystem_paths() -> None:
    """Locations belong in paths.py, driven by environment variables. A path in the YAML would
    quietly re-hardcode the store and break the gap-recovery drill."""
    text = (REPO_ROOT / "config" / "default.yaml").read_text(encoding="utf-8")
    config = yaml.safe_load(text)
    assert set(config) >= {"project", "data"}
    assert config["data"]["clean_table"] == "clean_daily"
    assert config["data"]["expected_universe_size"] == 750
    for line in text.splitlines():
        if line.strip().startswith("#"):
            continue
        assert "D:\\" not in line and "D:/" not in line, line


def test_runner_invokes_only_the_data_engine() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "production_pipeline_runner" in source
    for retired in (
        "daily_research_refresh_runner",
        "google_dashboard_sync",
        "telegram_notifications",
        "weekly_backtest",
        "share_center_runner",
    ):
        assert retired not in source, retired


def test_runner_reports_failure_without_touching_published_data() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "exit 1" in source
    assert "FAILED" in source
    assert "previously published dataset is left untouched" in source


def test_scheduled_task_settings_fix_the_two_ways_the_old_tasks_failed() -> None:
    source = INSTALLER.read_text(encoding="utf-8")
    # 0x800710E0: Windows refused to start the task on battery power.
    assert "-AllowStartIfOnBatteries" in source
    assert "-DontStopIfGoingOnBatteries" in source
    # Four missed runs that were never made up.
    assert "-StartWhenAvailable" in source
    # Killed mid-run with no limit and no retry.
    assert "-ExecutionTimeLimit" in source
    assert "-RestartCount" in source
    # Exactly one task.
    assert source.count("Register-ScheduledTask") == 1
    assert '$TaskName = "VAJRA Data Engine"' in source


def test_only_one_scheduled_task_script_exists() -> None:
    scripts = sorted(p.name for p in (REPO_ROOT / "scripts" / "windows").glob("*.ps1"))
    assert scripts == [
        "install_vajra_data_engine_task.ps1",
        "run_vajra_data_engine.ps1",
    ], scripts
