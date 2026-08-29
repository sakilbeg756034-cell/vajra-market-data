# The one entry point. Fetch every session not yet certified, rebuild, repair, verify,
# publish to D:\VAJRA_DATA, and write a certification record.
#
# Exit 0 = the published dataset is current and passed every check.
# Exit 1 = something failed. The previously published dataset is untouched and still valid;
#          the failure is written loudly to the log and to latest_engine_run.json.

$ErrorActionPreference = "Stop"

$EngineRoot = if ($env:VAJRA_ENGINE_ROOT) { $env:VAJRA_ENGINE_ROOT } else { "D:\VAJRA_ENGINE" }
$RepoRoot   = Join-Path $EngineRoot "code"
$PythonExe  = Join-Path $EngineRoot "venv\Scripts\python.exe"
$LogsRoot   = Join-Path $EngineRoot "logs\daily"
$StatusPath = Join-Path $EngineRoot "logs\latest_engine_run.json"

New-Item -ItemType Directory -Force -Path $LogsRoot | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $StatusPath) | Out-Null

$Timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LogPath = Join-Path $LogsRoot "vajra_data_engine_$Timestamp.txt"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Engine Python not found: $PythonExe"
}

function Write-Status {
    param([string]$Status, [string]$Message, [int]$ExitCode)
    $Payload = [ordered]@{
        status            = $Status
        version           = "vajra_data_engine_v1"
        generated_at_utc  = (Get-Date).ToUniversalTime().ToString("o")
        generated_at_local = (Get-Date).ToString("s")
        message           = $Message
        exit_code         = $ExitCode
        log_path          = $LogPath
        published_root    = if ($env:VAJRA_DATA_ROOT) { $env:VAJRA_DATA_ROOT } else { "D:\VAJRA_DATA" }
        note              = "On failure the previously published dataset is left untouched."
    }
    $Temporary = "$StatusPath.$([guid]::NewGuid().ToString('N')).tmp"
    $Payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $Temporary -Encoding UTF8
    Move-Item -LiteralPath $Temporary -Destination $StatusPath -Force
}

Start-Transcript -Path $LogPath -Force | Out-Null
try {
    Push-Location $RepoRoot
    try {
        $env:PYTHONWARNINGS = "ignore::FutureWarning"
        $env:PYTHONIOENCODING = "utf-8"
        & $PythonExe -m vajra_regime.nifty500_migration.production_pipeline_runner
        if ($LASTEXITCODE -ne 0) { throw "VAJRA data engine exited $LASTEXITCODE" }
    }
    finally {
        Pop-Location
    }
    Write-Status -Status "SUCCESS" -Message "Dataset current, verified and published." -ExitCode 0
    Write-Host "VAJRA DATA ENGINE: PASS"
    exit 0
}
catch {
    Write-Status -Status "FAILED" -Message $_.Exception.Message -ExitCode 1
    Write-Host "VAJRA DATA ENGINE: FAILED - $($_.Exception.Message)"
    Write-Error $_
    exit 1
}
finally {
    Stop-Transcript | Out-Null
}
