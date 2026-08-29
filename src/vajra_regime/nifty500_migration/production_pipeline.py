from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.config import AppConfig, load_config
from vajra_regime.corporate_actions import run_corporate_action_audit
from vajra_regime.daily_pipeline import (
    DEFAULT_RECHECK_DAYS,
    LIVE_START,
    _as_date,
    _target_date,
    _validate_alignment,
    summarize_clean_master,
)
from vajra_regime import ca_repair
from vajra_regime.master_safety import guard_protected_tree
from vajra_regime.monthly_universe import continue_monthly_750_universe
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.incremental_catchup import run_incremental_catchup
from vajra_regime.nse_live import catch_up_nse_eod, summarize_live_eod
from vajra_regime.publish import publish_dataset
from vajra_regime.quality import write_quality_report
from vajra_regime.rolling_master import rebuild_rolling_clean_data


ACTIVE_SWITCH = paths.ACTIVE_UNIVERSE_DIR / "active_universe.json"
PRODUCTION_STATUS = paths.PRODUCTION_LOGS / "latest_nifty500_production_status.json"


def _load_active_switch() -> dict[str, Any]:
    if not ACTIVE_SWITCH.exists():
        raise RuntimeError("Active-universe switch is missing; production fails closed")
    switch = json.loads(ACTIVE_SWITCH.read_text(encoding="utf-8-sig"))
    if switch.get("active_universe") != "NIFTY500":
        raise RuntimeError("Active-universe switch is not NIFTY500; production fails closed")
    if switch.get("foundation_version") != FOUNDATION_VERSION:
        raise RuntimeError("Active-universe foundation version mismatch")
    return switch


def _refresh_shared_official_master(
    config: AppConfig,
    *,
    now_ist: datetime | None,
    recheck_days: int,
) -> dict[str, Any]:
    target = _target_date(now_ist)
    before_raw = summarize_live_eod(config)
    before_clean = summarize_clean_master(config)
    previous_raw_last = _as_date(before_raw.get("raw_last_date"))
    intake_start = (
        LIVE_START
        if previous_raw_last is None
        else max(LIVE_START, previous_raw_last - timedelta(days=max(1, recheck_days)))
    )
    intake = catch_up_nse_eod(config, start_date=intake_start, end_date=target)
    if int(intake.get("errors_this_run", 0) or 0) > 0:
        raise RuntimeError("Official NSE EOD intake reported real errors")
    after_raw = summarize_live_eod(config)
    current_raw_last = _as_date(after_raw.get("raw_last_date"))
    if current_raw_last is None:
        raise RuntimeError("Official NSE live raw table is empty")
    raw_changed = (
        str(before_raw.get("raw_last_date")) != str(after_raw.get("raw_last_date"))
        or int(before_raw.get("raw_rows", 0) or 0)
        != int(after_raw.get("raw_rows", 0) or 0)
    )
    clean_last_before = _as_date(before_clean.get("last_date"))
    master_needs_sync = clean_last_before != current_raw_last
    corporate_action = None
    rolling = None
    if raw_changed or master_needs_sync:
        corporate_action = run_corporate_action_audit(
            config, start_date=LIVE_START, end_date=current_raw_last
        )
        rolling = rebuild_rolling_clean_data(config)
    final_raw = summarize_live_eod(config)
    final_clean = summarize_clean_master(config)
    alignment = _validate_alignment(final_raw, final_clean)
    if not alignment["ok"]:
        raise RuntimeError("Shared official raw/clean master alignment failed")
    return {
        "status": "SUCCESS",
        "outcome": "UPDATED" if raw_changed or master_needs_sync else "NO_NEW_EOD",
        "target_date": target.isoformat(),
        "intake_start": intake_start.isoformat(),
        "intake": intake,
        "raw_before": before_raw,
        "clean_before": before_clean,
        "raw_after": final_raw,
        "clean_after": final_clean,
        "alignment": alignment,
        "corporate_action": corporate_action,
        "rolling_master": rolling,
        "monthly_750_continuation_run": False,
    }


def catchup_status_for_report(catchup: dict[str, Any]) -> str:
    return str(catchup.get("status", "UNKNOWN"))


def _data_health_gate(
    *,
    catchup: dict[str, Any],
    latest_clean: str,
) -> dict[str, Any]:
    """The build is only allowed to publish if the data is genuinely current and certified.

    This replaces the old scanner-based gate. The scanner was removed with the rest of the
    strategy code: whether a top-50 list can be produced says nothing about whether the OHLCV
    underneath it is complete, which is the only thing this engine is responsible for.
    """
    foundation_path = DATA_ROOT / "11 Logs" / "foundation_certification_status.json"
    if not foundation_path.is_file():
        raise RuntimeError("Foundation certification status is missing; failing closed")
    foundation = json.loads(foundation_path.read_text(encoding="utf-8-sig"))

    # The catch-up reports one of two shapes: it did work (CATCHUP_COMPLETE_CERTIFIED, with
    # adjusted_latest_date) or there was nothing to do (ALREADY_CURRENT..., with last_clean).
    # Both are healthy; only the first has an adjusted_latest_date.
    catchup_status = str(catchup.get("status", ""))
    catchup_date = str(catchup.get("adjusted_latest_date") or catchup.get("last_clean") or "")
    checks = {
        "catchup_certified": catchup_status
        in {"CATCHUP_COMPLETE_CERTIFIED", "ALREADY_CURRENT_TO_LATEST_LOCAL_COMPLETED_NSE_SESSION"},
        "no_missing_source_sessions": not catchup.get("missing_source_sessions"),
        "foundation_certified": str(foundation.get("status", "")).startswith("CERTIFIED_PASS"),
        "foundation_current": str(foundation.get("latest_date")) == latest_clean,
        "catchup_current": catchup_date == latest_clean,
        "membership_present": int(foundation.get("daily_membership_rows", 0)) > 0,
        "no_duplicate_membership": int(
            foundation.get("duplicate_date_membership_symbol_rows", 1)
        ) == 0,
        "member_count_in_range": 490
        <= int(foundation.get("min_daily_members", 0))
        <= int(foundation.get("max_daily_members", 0))
        <= 510,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"Data health gate failed: {failed}")
    return {
        "pass": True,
        "checks": checks,
        "foundation_status": foundation.get("status"),
        "foundation_latest_date": foundation.get("latest_date"),
        "certified_adjusted_rows": foundation.get("certified_adjusted_rows"),
        "research_eligible_rows": foundation.get("certified_adjusted_research_eligible_rows"),
    }


def run_nifty500_production_pipeline(
    *,
    config: AppConfig | None = None,
    now_ist: datetime | None = None,
    recheck_days: int = DEFAULT_RECHECK_DAYS,
    publish: bool = True,
) -> dict[str, Any]:
    """Fetch, rebuild, repair, certify and publish. One job, in that order.

    The published dataset is fingerprinted before and after the build phase. It must not
    change while data is being fetched - it may only change in the publish step at the end.
    That is what makes a mid-run source failure safe: the build aborts, and the last good
    published dataset is still standing, untouched.
    """
    started = datetime.now(UTC)
    config = config or load_config()
    switch = _load_active_switch()

    with guard_protected_tree(Path(config.environment.published_data_root)) as published_guard:
        base = _refresh_shared_official_master(
            config, now_ist=now_ist, recheck_days=recheck_days
        )
        latest_clean = date.fromisoformat(str(base["clean_after"]["last_date"]))
        catchup = run_incremental_catchup(
            data_root=DATA_ROOT, today=latest_clean + timedelta(days=1)
        )
        universe_750 = continue_monthly_750_universe(config)
        repairs = ca_repair.repair()
    if published_guard["modified"]:
        raise RuntimeError("Published dataset changed during the build phase")

    health = _data_health_gate(catchup=catchup, latest_clean=latest_clean.isoformat())

    published: dict[str, Any] | None = None
    quality: dict[str, Any] | None = None
    if publish:
        published = publish_dataset()
        report = write_quality_report()
        quality = {"overall": report["overall"], "verdicts": report["verdicts"]}
        if report["overall"] == "FAIL":
            raise RuntimeError(f"Published dataset failed quality checks: {report['verdicts']}")

    finished = datetime.now(UTC)
    status: dict[str, Any] = {
        "status": "SUCCESS",
        "version": "vajra_data_engine_v1",
        "generated_at_utc": finished.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "active_universe": "NIFTY500",
        "foundation_version": FOUNDATION_VERSION,
        "latest_clean_market_date": latest_clean.isoformat(),
        "catchup_outcome": catchup_status_for_report(catchup),
        "sessions_caught_up": catchup.get("sessions_caught_up", []),
        "data_health": "CURRENT_COMPLETE",
        "health_gate": health,
        "base_official_master": base,
        "nifty500_catchup": catchup,
        "vajra750_universe": universe_750,
        "corporate_action_repairs": {
            name: {
                "action": entry["action"],
                "mechanical_repaired": entry["mechanical_count"],
                "non_mechanical_excluded": entry["non_mechanical_count"],
                "verified": entry["verified"],
            }
            for name, entry in repairs["universes"].items()
        },
        "published": published,
        "quality": quality,
        "active_switch_path": str(ACTIVE_SWITCH),
        "active_switch_sha256": sha256_file(ACTIVE_SWITCH),
        "published_data_changed_during_build": False,
        "published_data_guard": published_guard,
        "switch_payload": switch,
    }
    status["status_payload_sha256"] = canonical_hash(status)
    PRODUCTION_STATUS.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(PRODUCTION_STATUS, status)
    return status
