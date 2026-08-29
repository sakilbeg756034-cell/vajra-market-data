"""Drill 5 - the single scheduled task is registered, runs, and exits cleanly.

Requirement: "confirm the single new task is registered, runs, and exits cleanly."

This starts the real task through the Windows scheduler - not the script directly - so that
what is proved is the thing that will actually happen at 19:30 every day.
"""

from __future__ import annotations

import codecs
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

TASK = r"\VAJRA\VAJRA Data Engine"
STATUS = Path(r"D:\VAJRA_ENGINE\logs\latest_engine_run.json")


def ps(script: str) -> str:
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout or "") + (result.stderr or "")


def task_json() -> dict:
    raw = ps(
        f"$t = Get-ScheduledTask -TaskPath '\\VAJRA\\' -TaskName 'VAJRA Data Engine'; "
        "$i = $t | Get-ScheduledTaskInfo; "
        "[PSCustomObject]@{ "
        "  path = $t.TaskPath; name = $t.TaskName; state = [string]$t.State; "
        "  lastRun = [string]$i.LastRunTime; lastResult = $i.LastTaskResult; "
        "  nextRun = [string]$i.NextRunTime; missed = $i.NumberOfMissedRuns; "
        "  triggers = @($t.Triggers | ForEach-Object { $_.CimClass.CimClassName }); "
        "  battery = -not $t.Settings.DisallowStartIfOnBatteries; "
        "  stopOnBattery = $t.Settings.StopIfGoingOnBatteries; "
        "  startWhenAvailable = $t.Settings.StartWhenAvailable; "
        "  timeLimit = $t.Settings.ExecutionTimeLimit; "
        "  restartCount = $t.Settings.RestartCount "
        "} | ConvertTo-Json -Depth 4"
    )
    start = raw.find("{")
    return json.loads(raw[start:]) if start != -1 else {}


def main() -> int:
    registered = task_json()
    if not registered:
        print("task not registered")
        return 1
    print(json.dumps(registered, indent=2))

    all_vajra = ps(
        "Get-ScheduledTask | Where-Object { $_.TaskName -match 'VAJRA' -or "
        "$_.TaskPath -match 'VAJRA' } | Select-Object -ExpandProperty TaskName"
    )
    task_names = [line.strip() for line in all_vajra.splitlines() if line.strip()]

    status_before = STATUS.read_bytes() if STATUS.is_file() else None

    print("starting the task through the Windows scheduler...")
    started_at = datetime.now(UTC)
    ps(f"Start-ScheduledTask -TaskPath '\\VAJRA\\' -TaskName 'VAJRA Data Engine'")

    deadline = time.time() + 3600
    state = "Unknown"
    while time.time() < deadline:
        time.sleep(15)
        info = task_json()
        state = info.get("state", "Unknown")
        if state == "Ready" and info.get("lastRun"):
            break
    final = task_json()

    status_after = json.loads(STATUS.read_text(encoding="utf-8-sig")) if STATUS.is_file() else {}

    result = {
        "drill": "5_SCHEDULED_TASK",
        "performed_at_utc": started_at.isoformat(),
        "task_path": final.get("path"),
        "task_name": final.get("name"),
        "exactly_one_vajra_task": len(task_names) == 1,
        "vajra_tasks_found": task_names,
        "final_state": final.get("state"),
        "last_run_time": final.get("lastRun"),
        "last_task_result": final.get("lastResult"),
        "next_run_time": final.get("nextRun"),
        "missed_runs": final.get("missed"),
        "starts_on_battery": final.get("battery"),
        "stops_on_battery": final.get("stopOnBattery"),
        "start_when_available": final.get("startWhenAvailable"),
        "execution_time_limit": final.get("timeLimit"),
        "restart_count": final.get("restartCount"),
        "engine_status_after": status_after.get("status"),
        "engine_status_message": status_after.get("message"),
        "status_file_is_plain_utf8": STATUS.is_file()
        and not STATUS.read_bytes().startswith(codecs.BOM_UTF8),
        "engine_status_changed": status_before != STATUS.read_bytes() if STATUS.is_file() else False,
        "duration_seconds": round((datetime.now(UTC) - started_at).total_seconds(), 1),
    }
    result["verdict"] = (
        "PASS"
        if result["exactly_one_vajra_task"]
        and result["final_state"] == "Ready"
        and result["last_task_result"] == 0
        and result["engine_status_after"] == "SUCCESS"
        and result["starts_on_battery"]
        and not result["stops_on_battery"]
        and result["start_when_available"]
        and result["status_file_is_plain_utf8"]
        else "FAIL"
    )
    out = Path(r"D:\VAJRA_ENGINE\logs\drills\drill5_scheduled_task.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
