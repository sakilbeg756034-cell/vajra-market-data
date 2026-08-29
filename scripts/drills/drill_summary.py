"""Collect the five automation drills into one verdict file.

The deletion script refuses to run unless every verdict here is PASS.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

DRILLS = Path(r"D:\VAJRA_ENGINE\logs\drills")


def read_json_tail(path: Path) -> dict:
    """Runner logs are progress lines followed by one JSON document."""
    text = path.read_text(encoding="utf-8", errors="replace")
    start = text.find("{")
    while start != -1:
        try:
            return json.loads(text[start:])
        except json.JSONDecodeError:
            start = text.find("{", start + 1)
    return {}


def main() -> int:
    results: dict[str, dict] = {}

    # 1 - a normal daily run
    normal = read_json_tail(DRILLS / "drill1_normal_run.log")
    results["1_normal_daily_run"] = {
        "verdict": "PASS"
        if normal.get("status") == "SUCCESS"
        and normal.get("published", {}).get("verification", {}).get("pass")
        and normal.get("quality", {}).get("overall") in {"PASS", "PASS_WITH_WARNINGS"}
        else "FAIL",
        "duration_seconds": normal.get("duration_seconds"),
        "latest_session": normal.get("latest_clean_market_date"),
        "catchup_outcome": normal.get("catchup_outcome"),
        "published_files": normal.get("published", {}).get("file_count"),
        "quality": normal.get("quality", {}).get("overall"),
        "published_data_changed_during_build": normal.get("published_data_changed_during_build"),
    }

    # 2 - recovery from a 30-day gap
    gap = read_json_tail(DRILLS / "drill2_gap_recovery.log")
    caught = gap.get("sessions_caught_up", [])
    catchup = gap.get("nifty500_catchup", {})
    results["2_thirty_day_gap_recovery"] = {
        "verdict": "PASS"
        if gap.get("status") == "SUCCESS"
        and len(caught) >= 20
        and not catchup.get("missing_source_sessions")
        and gap.get("published", {}).get("verification", {}).get("pass")
        and gap.get("quality", {}).get("overall") in {"PASS", "PASS_WITH_WARNINGS"}
        else "FAIL",
        "simulated_last_certified_date": catchup.get("prior_last_clean"),
        "sessions_recovered": len(caught),
        "first_recovered": caught[0] if caught else None,
        "last_recovered": caught[-1] if caught else None,
        "missing_source_sessions": catchup.get("missing_source_sessions"),
        "duration_seconds": gap.get("duration_seconds"),
        "quality": gap.get("quality", {}).get("overall"),
        "unattended": True,
    }

    # 3 - source unreachable mid-run
    failure_path = DRILLS / "drill3_source_failure.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8")) if failure_path.is_file() else {}
    results["3_source_failure_mid_run"] = {
        "verdict": failure.get("verdict", "NOT_RUN"),
        "run_failed_as_expected": failure.get("run_failed_as_expected"),
        "failure": failure.get("failure"),
        "published_data_files_changed": failure.get("published_data_files_changed"),
        "published_dataset_still_verifies": failure.get("published_dataset_still_verifies"),
        "failure_recorded": failure.get("failure_recorded_in", {})
        .get("latest_nifty500_production_status.json", {})
        .get("status"),
    }

    # 4 - a corporate action arrives
    ca_path = DRILLS / "drill4_corporate_action.json"
    ca = json.loads(ca_path.read_text(encoding="utf-8")) if ca_path.is_file() else {}
    results["4_corporate_action_regenerates_both_formats"] = {
        "verdict": ca.get("verdict", "NOT_RUN"),
        "symbol": ca.get("symbol"),
        "ex_date": ca.get("ex_date"),
        "repair_detected_the_event": ca.get("repair_detected_the_event"),
        "parquet_and_csv_both_regenerated": ca.get("published_parquet_and_csv_both_changed"),
        "only_the_affected_year_changed": ca.get("only_the_affected_year_changed"),
        "parquet_csv_identical_after": ca.get("parquet_csv_identical_after"),
        "prices_restored_to_original": ca.get("prices_restored_to_original"),
    }

    # 5 - the scheduled task
    task_path = DRILLS / "drill5_scheduled_task.json"
    task = json.loads(task_path.read_text(encoding="utf-8")) if task_path.is_file() else {}
    results["5_scheduled_task"] = {
        "verdict": task.get("verdict", "NOT_RUN"),
        **{k: v for k, v in task.items() if k != "verdict"},
    }

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "verdicts": {name: entry["verdict"] for name, entry in results.items()},
        "all_pass": all(entry["verdict"] == "PASS" for entry in results.values()),
        "detail": results,
    }
    (DRILLS / "drill_results.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"verdicts": summary["verdicts"], "all_pass": summary["all_pass"]}, indent=2))
    return 0 if summary["all_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
