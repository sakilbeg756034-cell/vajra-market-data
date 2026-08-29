# Install the single scheduled task that keeps D:\VAJRA_DATA current.
#
# The settings here are the point of this file. The two tasks it replaces both failed, and
# they failed for reasons this configuration fixes:
#
#   * The old daily task last exited 0x800710E0 - "the operator or administrator has refused
#     the request". On a laptop that is almost always the default battery condition: Windows
#     will not start the task unless the machine is on mains power. Fixed by
#     -AllowStartIfOnBatteries and -DontStopIfGoingOnBatteries.
#
#   * The old share task had four missed runs and no way to make them up. A missed run stayed
#     missed. Fixed by -StartWhenAvailable, which runs the schedule as soon as the machine is
#     next awake. That, plus the engine's own catch-up, is what makes "laptop off for 15 days"
#     recover by itself.
#
#   * The old share task was killed mid-run (0xC000013A) with no time limit and no restart.
#     Fixed by -ExecutionTimeLimit and -RestartCount.

$ErrorActionPreference = "Stop"

$TaskName = "VAJRA Data Engine"
$EngineRoot = "D:\VAJRA_ENGINE"
$Script = Join-Path $EngineRoot "code\scripts\windows\run_vajra_data_engine.ps1"

if (-not (Test-Path -LiteralPath $Script)) { throw "Engine runner not found: $Script" }

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$Script`"" `
    -WorkingDirectory (Join-Path $EngineRoot "code")

# 19:30 IST: comfortably after the 15:30 close and after NSE has published the day's bhavcopy.
$Trigger = New-ScheduledTaskTrigger -Daily -At 19:30
# A second trigger at logon so a laptop that was off all day catches up as soon as it is used,
# without waiting for the next 19:30.
$LogonTrigger = New-ScheduledTaskTrigger -AtLogOn
$LogonTrigger.Delay = "PT5M"

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 20)

$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger @($Trigger, $LogonTrigger) `
    -Settings $Settings `
    -Principal $Principal `
    -Description "Keeps D:\VAJRA_DATA current: fetches every uncertified NSE session, rebuilds, repairs corporate actions, verifies and republishes. Self-heals gaps of any length." | Out-Null

$Task = Get-ScheduledTask -TaskName $TaskName
Write-Host "Registered: $($Task.TaskName)  state=$($Task.State)"
Write-Host "Triggers  : daily 19:30, plus at logon (5 min delay)"
Write-Host "Battery   : starts on battery, does not stop on battery"
Write-Host "Missed run: StartWhenAvailable = true"
