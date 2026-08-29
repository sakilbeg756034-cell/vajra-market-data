from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from vajra_regime.config import AppConfig
from vajra_regime.corporate_actions import run_corporate_action_audit
from vajra_regime.master_safety import guard_protected_tree
from vajra_regime.monthly_universe import continue_monthly_750_universe
from vajra_regime.nse_live import catch_up_nse_eod, summarize_live_eod
from vajra_regime.rolling_master import rebuild_rolling_clean_data


IST = ZoneInfo("Asia/Kolkata")
LIVE_START = date(2026, 1, 1)
DEFAULT_EVENING_CUTOFF = time(19, 0)
DEFAULT_RECHECK_DAYS = 7


def _as_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return date.fromisoformat(text[:10])


def _target_date(now_ist: datetime | None = None) -> date:
    """Return the last calendar date worth checking for a completed NSE EOD file.

    Before 7 PM IST we stop at yesterday. The scheduled task runs at/after 7:30 PM,
    so its normal evening runs are allowed to check the current calendar date.
    """
    now = now_ist or datetime.now(IST)
    now = now.replace(tzinfo=IST) if now.tzinfo is None else now.astimezone(IST)
    if now.timetz().replace(tzinfo=None) < DEFAULT_EVENING_CUTOFF:
        return now.date() - timedelta(days=1)
    return now.date()


def summarize_clean_master(config: AppConfig) -> dict[str, object]:
    database = Path(config.environment.duckdb_path)
    clean_table = str(config.data["clean_table"])
    result: dict[str, object] = {
        "database": str(database),
        "clean_table_exists": False,
        "rows": 0,
        "dates": 0,
        "first_date": None,
        "last_date": None,
        "duplicate_date_isin_groups": 0,
        "live_rows": 0,
        "quarantine_rows": 0,
    }
    if not database.exists():
        return result

    with duckdb.connect(str(database), read_only=True) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if clean_table not in tables:
            return result
        result["clean_table_exists"] = True
        columns = {
            row[1]
            for row in connection.execute(f"PRAGMA table_info('{clean_table}')").fetchall()
        }
        has_live_flag = "IsLiveOutOfSample" in columns
        has_quarantine_flag = "CorporateActionQuarantineFlag" in columns
        live_sql = (
            "SUM(CASE WHEN IsLiveOutOfSample THEN 1 ELSE 0 END)"
            if has_live_flag
            else "SUM(CASE WHEN CAST(Date AS DATE) >= DATE '2026-01-01' THEN 1 ELSE 0 END)"
        )
        quarantine_sql = (
            "SUM(CASE WHEN CorporateActionQuarantineFlag THEN 1 ELSE 0 END)"
            if has_quarantine_flag
            else "0"
        )
        row = connection.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT CAST(Date AS DATE)),
                MIN(CAST(Date AS DATE)),
                MAX(CAST(Date AS DATE)),
                {live_sql},
                {quarantine_sql}
            FROM {clean_table}
            """
        ).fetchone()
        duplicate_groups = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT CAST(Date AS DATE) AS TradingDate, ISIN, COUNT(*) AS n
                    FROM {clean_table}
                    GROUP BY TradingDate, ISIN
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )

    result.update(
        {
            "rows": int(row[0] or 0),
            "dates": int(row[1] or 0),
            "first_date": str(row[2]) if row[2] is not None else None,
            "last_date": str(row[3]) if row[3] is not None else None,
            "live_rows": int(row[4] or 0),
            "quarantine_rows": int(row[5] or 0),
            "duplicate_date_isin_groups": duplicate_groups,
        }
    )
    return result


def _status_path(config: AppConfig) -> Path:
    path = Path(config.environment.logs_dir) / "Daily Pipeline" / "latest_daily_pipeline_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_status(config: AppConfig, status: dict[str, object]) -> Path:
    path = _status_path(config)
    path.write_text(json.dumps(status, indent=2, default=str), encoding="utf-8")
    return path


def _validate_alignment(
    raw_status: dict[str, object],
    clean_status: dict[str, object],
) -> dict[str, object]:
    raw_last = _as_date(raw_status.get("raw_last_date"))
    clean_last = _as_date(clean_status.get("last_date"))
    checks = {
        "raw_table_exists": bool(raw_status.get("raw_table_exists")),
        "clean_table_exists": bool(clean_status.get("clean_table_exists")),
        "raw_duplicate_date_isin_groups_zero": int(
            raw_status.get("duplicate_date_isin_groups", 0) or 0
        )
        == 0,
        "clean_duplicate_date_isin_groups_zero": int(
            clean_status.get("duplicate_date_isin_groups", 0) or 0
        )
        == 0,
        "raw_and_clean_last_date_match": raw_last is not None and raw_last == clean_last,
        "clean_starts_2009_01_01": str(clean_status.get("first_date")) == "2009-01-01",
    }
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "raw_last_date": str(raw_last) if raw_last else None,
        "clean_last_date": str(clean_last) if clean_last else None,
    }


def _run_daily_pipeline(
    config: AppConfig,
    *,
    now_ist: datetime | None = None,
    recheck_days: int = DEFAULT_RECHECK_DAYS,
) -> dict[str, object]:
    """Maintain the rolling OHLCV master and point-in-time monthly 750 universe.

    Flow:
    1. Recheck a short recent NSE EOD window and idempotently ingest anything missing.
    2. If new raw data exists, refresh corporate-action evidence and rebuild clean_daily.
    3. Verify raw/clean last dates and zero Date+ISIN duplicates.
    4. Preserve the exact 2010-2025 monthly 750 universe and extend completed live months.
    5. Never freeze the current partial month; it becomes eligible only after next-month data exists.

    The published dataset is never written by this routine.
    """
    started = datetime.now(UTC)
    target = _target_date(now_ist)
    before_raw = summarize_live_eod(config)
    before_clean = summarize_clean_master(config)

    previous_raw_last = _as_date(before_raw.get("raw_last_date"))
    if previous_raw_last is None:
        intake_start = LIVE_START
    else:
        intake_start = max(LIVE_START, previous_raw_last - timedelta(days=max(1, recheck_days)))

    if target < LIVE_START:
        raise ValueError(f"Daily target {target} is before live-data start {LIVE_START}.")

    intake = catch_up_nse_eod(
        config,
        start_date=intake_start,
        end_date=target,
    )
    if int(intake.get("errors_this_run", 0) or 0) > 0:
        status = {
            "status": "FAILED_NSE_EOD_INTAKE",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "target_date": target.isoformat(),
            "intake": intake,
            "published_data_changed_during_build": None,
        }
        status["status_path"] = str(_write_status(config, status))
        raise RuntimeError(
            "NSE EOD intake reported one or more real errors. Read latest_daily_pipeline_status.json."
        )

    after_raw = summarize_live_eod(config)
    current_raw_last = _as_date(after_raw.get("raw_last_date"))
    if current_raw_last is None:
        raise ValueError("Raw NSE table is empty after the daily intake check.")

    raw_changed = (
        str(before_raw.get("raw_last_date")) != str(after_raw.get("raw_last_date"))
        or int(before_raw.get("raw_rows", 0) or 0) != int(after_raw.get("raw_rows", 0) or 0)
    )
    clean_last_before = _as_date(before_clean.get("last_date"))
    master_needs_sync = clean_last_before != current_raw_last

    corporate_action_summary: dict[str, object] | None = None
    rolling_summary: dict[str, object] | None = None

    if raw_changed or master_needs_sync:
        corporate_action_summary = run_corporate_action_audit(
            config,
            start_date=LIVE_START,
            end_date=current_raw_last,
        )
        rolling_summary = rebuild_rolling_clean_data(config)

    final_raw = summarize_live_eod(config)
    final_clean = summarize_clean_master(config)
    alignment = _validate_alignment(final_raw, final_clean)
    if not alignment["ok"]:
        status = {
            "status": "FAILED_FINAL_ALIGNMENT",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "target_date": target.isoformat(),
            "intake_start": intake_start.isoformat(),
            "intake": intake,
            "raw_before": before_raw,
            "clean_before": before_clean,
            "raw_after": final_raw,
            "clean_after": final_clean,
            "alignment": alignment,
            "corporate_action": corporate_action_summary,
            "rolling_master": rolling_summary,
            "published_data_changed_during_build": None,
        }
        status["status_path"] = str(_write_status(config, status))
        raise RuntimeError("Daily pipeline final raw/clean alignment validation failed.")

    try:
        universe_summary = continue_monthly_750_universe(config)
    except Exception as exc:
        status = {
            "status": "FAILED_MONTHLY_750_UNIVERSE",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "target_date": target.isoformat(),
            "intake_start": intake_start.isoformat(),
            "intake": intake,
            "raw_after": final_raw,
            "clean_after": final_clean,
            "alignment": alignment,
            "corporate_action": corporate_action_summary,
            "rolling_master": rolling_summary,
            "monthly_750_universe_error": f"{type(exc).__name__}: {exc}",
            "published_data_changed_during_build": None,
        }
        status["status_path"] = str(_write_status(config, status))
        raise RuntimeError("Monthly 750 universe continuation failed after clean-data validation.") from exc

    outcome = "UPDATED" if (raw_changed or master_needs_sync) else "NO_NEW_EOD"
    finished = datetime.now(UTC)
    status = {
        "status": "SUCCESS",
        "outcome": outcome,
        "generated_at_utc": finished.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "target_date": target.isoformat(),
        "intake_start": intake_start.isoformat(),
        "raw_changed": raw_changed,
        "master_needed_sync": master_needs_sync,
        "intake": intake,
        "raw_before": before_raw,
        "clean_before": before_clean,
        "raw_after": final_raw,
        "clean_after": final_clean,
        "alignment": alignment,
        "corporate_action": corporate_action_summary,
        "rolling_master": rolling_summary,
        "monthly_750_universe": universe_summary,
        "published_data_changed_during_build": None,
        "manual_corporate_action_edit_required": False,
        "manual_universe_edit_required": False,
        "next_boundary": (
            "Daily OHLCV and completed-month 750-universe continuity are automated. "
            "Live point-in-time breadth, momentum-health and forecast research are next."
        ),
    }
    status["status_path"] = str(_write_status(config, status))
    return status


def run_daily_pipeline(
    config: AppConfig,
    *,
    now_ist: datetime | None = None,
    recheck_days: int = DEFAULT_RECHECK_DAYS,
) -> dict[str, object]:
    """Run the daily pipeline and prove the protected legacy tree stayed unchanged."""
    protected_root = Path(config.environment.published_data_root)
    with guard_protected_tree(protected_root) as protected_guard:
        status = _run_daily_pipeline(
            config,
            now_ist=now_ist,
            recheck_days=recheck_days,
        )

    status["published_data_changed_during_build"] = bool(protected_guard["modified"])
    status["published_data_guard"] = protected_guard
    status["status_path"] = str(_write_status(config, status))
    return status
