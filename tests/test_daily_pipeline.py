from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from vajra_regime import daily_pipeline


IST = ZoneInfo("Asia/Kolkata")


def _config(tmp_path):
    root = tmp_path / "Vajra Market System"
    protected = tmp_path / "Vajra Backtesting"
    protected.mkdir()
    return SimpleNamespace(
        environment=SimpleNamespace(
            duckdb_path=root / "02 Master Historical Data" / "05 Database" / "master.duckdb",
            logs_dir=root / "08 Logs",
            published_data_root=protected,
        ),
        data={"clean_table": "clean_daily"},
    )


def _raw(last_date: str, rows: int = 307241, duplicates: int = 0):
    return {
        "raw_table_exists": True,
        "raw_rows": rows,
        "raw_dates": 146,
        "raw_first_date": "2026-01-01",
        "raw_last_date": last_date,
        "duplicate_date_isin_groups": duplicates,
    }


def _clean(last_date: str, rows: int = 5501297, duplicates: int = 0):
    return {
        "clean_table_exists": True,
        "rows": rows,
        "dates": 4400,
        "first_date": "2009-01-01",
        "last_date": last_date,
        "duplicate_date_isin_groups": duplicates,
        "live_rows": 307241,
        "quarantine_rows": 8053,
    }


def _universe():
    return {
        "status": "SUCCESS",
        "outcome": "UPDATED",
        "historical_months_preserved": 185,
        "live_completed_months": 7,
        "partial_live_months": 0,
    }


def test_target_date_before_evening_cutoff_uses_yesterday():
    now = datetime(2026, 8, 7, 4, 24, tzinfo=IST)
    assert daily_pipeline._target_date(now).isoformat() == "2026-08-06"


def test_no_new_eod_skips_corporate_action_and_master_rebuild(tmp_path, monkeypatch):
    config = _config(tmp_path)
    raw = _raw("2026-08-06")
    clean = _clean("2026-08-06")
    monkeypatch.setattr(daily_pipeline, "summarize_live_eod", lambda _config: raw)
    monkeypatch.setattr(daily_pipeline, "summarize_clean_master", lambda _config: clean)
    monkeypatch.setattr(
        daily_pipeline,
        "catch_up_nse_eod",
        lambda *_args, **_kwargs: {"errors_this_run": 0, "validated_this_run": 0},
    )
    monkeypatch.setattr(daily_pipeline, "continue_monthly_750_universe", lambda _config: _universe())

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("expensive rebuild should have been skipped")

    monkeypatch.setattr(daily_pipeline, "run_corporate_action_audit", should_not_run)
    monkeypatch.setattr(daily_pipeline, "rebuild_rolling_clean_data", should_not_run)

    result = daily_pipeline.run_daily_pipeline(
        config,
        now_ist=datetime(2026, 8, 7, 4, 24, tzinfo=IST),
    )

    assert result["status"] == "SUCCESS"
    assert result["outcome"] == "NO_NEW_EOD"
    assert result["alignment"]["ok"] is True
    assert result["monthly_750_universe"]["live_completed_months"] == 7
    assert result["published_data_changed_during_build"] is False


def test_new_eod_refreshes_corporate_actions_master_and_universe(tmp_path, monkeypatch):
    config = _config(tmp_path)
    raw_states = iter(
        [
            _raw("2026-08-06", 307241),
            _raw("2026-08-07", 309300),
            _raw("2026-08-07", 309300),
        ]
    )
    clean_states = iter(
        [
            _clean("2026-08-06", 5501297),
            _clean("2026-08-07", 5503356),
        ]
    )
    monkeypatch.setattr(daily_pipeline, "summarize_live_eod", lambda _config: next(raw_states))
    monkeypatch.setattr(daily_pipeline, "summarize_clean_master", lambda _config: next(clean_states))
    monkeypatch.setattr(
        daily_pipeline,
        "catch_up_nse_eod",
        lambda *_args, **_kwargs: {"errors_this_run": 0, "validated_this_run": 1},
    )

    calls = {"corporate": 0, "master": 0, "universe": 0}

    def corporate(*_args, **_kwargs):
        calls["corporate"] += 1
        return {"events": 1041}

    def master(*_args, **_kwargs):
        calls["master"] += 1
        return {"rows": 5503356}

    def universe(*_args, **_kwargs):
        calls["universe"] += 1
        return _universe()

    monkeypatch.setattr(daily_pipeline, "run_corporate_action_audit", corporate)
    monkeypatch.setattr(daily_pipeline, "rebuild_rolling_clean_data", master)
    monkeypatch.setattr(daily_pipeline, "continue_monthly_750_universe", universe)

    result = daily_pipeline.run_daily_pipeline(
        config,
        now_ist=datetime(2026, 8, 7, 21, 30, tzinfo=IST),
    )

    assert result["outcome"] == "UPDATED"
    assert result["raw_after"]["raw_last_date"] == "2026-08-07"
    assert result["clean_after"]["last_date"] == "2026-08-07"
    assert calls == {"corporate": 1, "master": 1, "universe": 1}


def test_final_duplicate_guard_fails_closed(tmp_path, monkeypatch):
    config = _config(tmp_path)
    raw_states = iter([_raw("2026-08-06"), _raw("2026-08-06"), _raw("2026-08-06")])
    clean_states = iter([_clean("2026-08-06"), _clean("2026-08-06", duplicates=1)])
    monkeypatch.setattr(daily_pipeline, "summarize_live_eod", lambda _config: next(raw_states))
    monkeypatch.setattr(daily_pipeline, "summarize_clean_master", lambda _config: next(clean_states))
    monkeypatch.setattr(
        daily_pipeline,
        "catch_up_nse_eod",
        lambda *_args, **_kwargs: {"errors_this_run": 0, "validated_this_run": 0},
    )

    with pytest.raises(RuntimeError, match="alignment validation failed"):
        daily_pipeline.run_daily_pipeline(
            config,
            now_ist=datetime(2026, 8, 7, 4, 24, tzinfo=IST),
        )


def test_daily_pipeline_guard_fails_if_protected_root_changes(
    tmp_path, monkeypatch
) -> None:
    config = _config(tmp_path)
    protected_file = config.environment.published_data_root / "source.txt"
    protected_file.write_text("before", encoding="utf-8")

    def mutate_protected_tree(*_args, **_kwargs):
        protected_file.write_text("after-and-longer", encoding="utf-8")
        return {
            "status": "SUCCESS",
            "published_data_changed_during_build": None,
        }

    monkeypatch.setattr(
        daily_pipeline,
        "_run_daily_pipeline",
        mutate_protected_tree,
    )

    with pytest.raises(RuntimeError, match="fingerprint changed"):
        daily_pipeline.run_daily_pipeline(config)
