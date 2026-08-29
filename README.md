# VAJRA Data Engine

Builds and maintains one thing: a point-in-time NSE equity OHLCV dataset, published to
`D:\VAJRA_DATA\` in a form another person or another AI can use without verifying it first.

It does not backtest, screen, score, rank, alert, or trade. That code was removed on
2026-08-29 and lives in the archive bundles under `D:\VAJRA_ENGINE\archive\`.

## Running it

```powershell
D:\VAJRA_ENGINE\code\scripts\windows\run_vajra_data_engine.ps1
```

Or directly:

```powershell
cd D:\VAJRA_ENGINE\code
D:\VAJRA_ENGINE\venv\Scripts\python.exe -m vajra_regime.nifty500_migration.production_pipeline_runner
```

A scheduled task named **VAJRA Data Engine** runs it daily at 19:30 and again five minutes
after logon. Install or reinstall it with:

```powershell
D:\VAJRA_ENGINE\code\scripts\windows\install_vajra_data_engine_task.ps1
```

There is exactly one scheduled task, and a test asserts that.

## What one run does

1. **Fetch** every NSE bhavcopy session not yet ingested, however far back the gap goes.
2. **Rebuild** the adjusted price master and the DuckDB working tables.
3. **Catch up** the NIFTY 500 point-in-time membership and certified adjusted series.
4. **Rebuild** the VAJRA 750 monthly universe for any completed month.
5. **Repair** corporate actions the source series failed to apply, and exclude the
   non-mechanical price breaks (demergers, rights, large special dividends) that are not
   returns. See `src/vajra_regime/ca_repair.py`.
6. **Gate** on data health. If the data is not current and certified, the run stops here and
   nothing is published.
7. **Publish** to `D:\VAJRA_DATA\` — Parquet and CSV per year, membership, corporate actions,
   calendar, `MANIFEST.json`, `CHANGELOG.md`, and regenerated documentation.
8. **Verify** the published dataset independently and write `DATA_QUALITY_REPORT.md`.

Steps 1–5 run inside a fingerprint guard on the published folder: if anything writes to
`D:\VAJRA_DATA\` during the build, the run aborts. The published dataset may only change in
step 7.

## Where things are

| | |
|---|---|
| `D:\VAJRA_DATA\` | the published dataset. The only folder anyone else needs. |
| `D:\VAJRA_ENGINE\code\` | this repository |
| `D:\VAJRA_ENGINE\venv\` | Python 3.12 environment |
| `D:\VAJRA_ENGINE\store\` | working data: raw bhavcopy, corporate actions, checkpoints, DuckDB |
| `D:\VAJRA_ENGINE\logs\` | run logs, `latest_engine_run.json`, quality and repair records |
| `D:\VAJRA_ENGINE\progress\` | reports written for the operator |
| `D:\VAJRA_ENGINE\archive\` | git bundles of every repository that existed before the reset |

**No module hardcodes a path.** Everything routes through `src/vajra_regime/paths.py`, which
reads three environment variables:

```
VAJRA_ENGINE_ROOT   default D:\VAJRA_ENGINE
VAJRA_STORE_ROOT    default <engine>\store
VAJRA_DATA_ROOT     default D:\VAJRA_DATA
```

That is how the gap-recovery drill runs against a throwaway copy without going anywhere near
production.

## Failure behaviour

Every write is atomic: temp file, then `os.replace`. If a run fails at any point — NSE
unreachable, a source file corrupt, a check failing — the previously published dataset is left
exactly as it was, and the failure is recorded in `D:\VAJRA_ENGINE\logs\latest_engine_run.json`
with `status: FAILED`.

A stale dataset is obvious and self-correcting. A half-written one is neither. The engine is
built to produce the first and never the second.

## Development

```powershell
cd D:\VAJRA_ENGINE\code
D:\VAJRA_ENGINE\venv\Scripts\python.exe -m pytest --maxfail=0
D:\VAJRA_ENGINE\venv\Scripts\python.exe -m ruff check src tests
```

`pyproject.toml` sets `--maxfail=1` in `addopts`; pass `--maxfail=0` to see every failure
rather than just the first.

## The two universes

**`nifty500`** — the real NSE NIFTY 500 index, point-in-time. Official constituent evidence
begins **2013-04-18**; before that the membership panel is reconstructed by reversing later
index changes and is labelled accordingly.

**`nifty750`** — **not an index.** A monthly top-750-by-60-session-median-turnover universe
defined here, first rebalance 2010-08-31, plus the supporting adjusted price master for every
security ever selected. Never describe it as an NSE index.

Every published year carries a `BACKTEST_SAFE` / `PARTIAL` / `PRICE_DATA_ONLY` label in
`MANIFEST.json`, and `START_HERE_AI.md` explains what each means.
