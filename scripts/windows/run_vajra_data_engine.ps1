# VAJRA -- roz ka data run. Task Scheduler isi ko chalata hai.
#
# DO KAAM, ISI KRAM ME
# ====================
#   1. Engine: NSE se naya bhavcopy utaaro aur master store sambhaalo.
#      (Ye ab dataset PUBLISH NAHI karta -- dekho production_pipeline_runner.py)
#   2. Dataset banao: D:\VAJRA_DATA  -- ekmatra dataset, survivorship-free,
#      parquet + CSV.
#
# Exit 0 = dono ho gaye.
# Exit 1 = kuch fail hua. Purana dataset waise ka waisa hai aur ab bhi valid
#          hai; shikayat log me aur latest_engine_run.json me likhi jaati hai.

$ErrorActionPreference = "Stop"

$EngineRoot = if ($env:VAJRA_ENGINE_ROOT) { $env:VAJRA_ENGINE_ROOT } else { "D:\VAJRA_ENGINE" }
$RepoRoot   = Join-Path $EngineRoot "code"
$PythonExe  = Join-Path $EngineRoot "venv\Scripts\python.exe"
$LogsRoot   = Join-Path $EngineRoot "logs\daily"
$StatusPath = Join-Path $EngineRoot "logs\latest_engine_run.json"
$BuildData  = Join-Path (Join-Path $RepoRoot "scripts") "build_vajra_data.py"

New-Item -ItemType Directory -Force -Path $LogsRoot | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $StatusPath) | Out-Null

$Timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LogPath = Join-Path $LogsRoot "vajra_data_engine_$Timestamp.txt"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Engine Python not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $BuildData)) {
    throw "Dataset builder not found: $BuildData"
}

function Write-Status {
    param([string]$Status, [string]$Message, [int]$ExitCode)
    $Payload = [ordered]@{
        status             = $Status
        version            = "vajra_data_engine_v2"
        generated_at_utc   = (Get-Date).ToUniversalTime().ToString("o")
        generated_at_local = (Get-Date).ToString("s")
        message            = $Message
        exit_code          = $ExitCode
        log_path           = $LogPath
        dataset_root       = "D:\VAJRA_DATA"
        note               = "Fail hone par purana dataset waise ka waisa chhoda jaata hai."
    }
    $Temporary = "$StatusPath.$([guid]::NewGuid().ToString('N')).tmp"
    # Set-Content -Encoding UTF8 Windows PowerShell par BOM likhta hai aur
    # json.loads BOM ko reject karta hai. Isliye BOM ke bina likhte hain.
    $Json = $Payload | ConvertTo-Json -Depth 5
    [System.IO.File]::WriteAllText($Temporary, $Json, (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $Temporary -Destination $StatusPath -Force
}

Start-Transcript -Path $LogPath -Force | Out-Null
try {
    Push-Location $RepoRoot
    try {
        $env:PYTHONWARNINGS = "ignore::FutureWarning"
        $env:PYTHONIOENCODING = "utf-8"

        Write-Host "=== 1/2  ENGINE: naya bhavcopy + master store ==="
        & $PythonExe -m vajra_regime.nifty500_migration.production_pipeline_runner
        if ($LASTEXITCODE -ne 0) { throw "Engine pipeline exited $LASTEXITCODE" }

        Write-Host ""
        Write-Host "=== 2/2  DATASET: D:\VAJRA_DATA dobara banao ==="
        & $PythonExe $BuildData
        if ($LASTEXITCODE -ne 0) { throw "build_vajra_data.py exited $LASTEXITCODE" }
    }
    finally {
        Pop-Location
    }
    Write-Status -Status "SUCCESS" -Message "Bhavcopy taaza, D:\VAJRA_DATA dobara ban gaya." -ExitCode 0
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
