"""Single source of truth for every filesystem location the engine touches.

Nothing else in this package may hardcode an absolute path. Two roots exist and they
have different jobs:

``STORE_ROOT``  (default ``D:/VAJRA_ENGINE/store``)
    Working data. Raw NSE bhavcopy archives, corporate-action JSON, index press
    releases, checkpoints, the DuckDB working databases, panels. The operator never
    opens this. It exists so history can be rebuilt and audited without re-downloading
    seventeen years of source files.

``DATA_ROOT``   (default ``D:/VAJRA_DATA``)
    The published, clean dataset. Parquet + CSV + membership + corporate actions +
    calendar + documentation, and nothing else. This is the folder that gets handed to
    another person or another AI.

Both can be overridden with environment variables, which is how the tests and the
gap-recovery drill run against a throwaway copy instead of production:

    VAJRA_ENGINE_ROOT   overrides D:/VAJRA_ENGINE
    VAJRA_STORE_ROOT    overrides <engine>/store
    VAJRA_DATA_ROOT     overrides D:/VAJRA_DATA

The subfolder names inside ``STORE_ROOT`` deliberately keep the original numbered
layout ("02 Master Historical Data" and friends). They are ugly, but every checkpoint,
manifest and status file written over the past year records those exact strings, and
renaming them would invalidate that provenance trail for no benefit.
"""

from __future__ import annotations

import os
from pathlib import Path


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else default


ENGINE_ROOT: Path = _env_path("VAJRA_ENGINE_ROOT", Path("D:/VAJRA_ENGINE"))
STORE_ROOT: Path = _env_path("VAJRA_STORE_ROOT", ENGINE_ROOT / "store")
DATA_ROOT: Path = _env_path("VAJRA_DATA_ROOT", Path("D:/VAJRA_DATA"))

# --- engine-side ---------------------------------------------------------------
CODE_ROOT: Path = ENGINE_ROOT / "code"
LOGS_ROOT: Path = ENGINE_ROOT / "logs"
TEMP_ROOT: Path = ENGINE_ROOT / "temp"
PROGRESS_ROOT: Path = ENGINE_ROOT / "progress"
ARCHIVE_ROOT: Path = ENGINE_ROOT / "archive"

# --- store-side ----------------------------------------------------------------
PROTECTED_SOURCE = STORE_ROOT / "01 Protected Source Data"
MASTER_DATA = STORE_ROOT / "02 Master Historical Data"
INCOMING_EOD = STORE_ROOT / "03 Incoming NSE EOD"
CORPORATE_ACTIONS = STORE_ROOT / "04 Corporate Actions"
STORE_LOGS = STORE_ROOT / "08 Logs"
BACKUPS = STORE_ROOT / "09 Backups"

CLEAN_PARQUET_BY_YEAR = MASTER_DATA / "01 Daily Clean Parquet By Year"
MONTHLY_750_UNIVERSE = MASTER_DATA / "02 Monthly 750 Universe"
QUALITY_REPORTS = MASTER_DATA / "03 Quality Reports"
DATABASE_DIR = MASTER_DATA / "05 Database"
BENCHMARK_TRI = MASTER_DATA / "06 Official Benchmark TRI"
ACTIVE_UNIVERSE_DIR = MASTER_DATA / "Active Universe"
NIFTY500_PIT = MASTER_DATA / "NIFTY500 Point In Time"

MASTER_DB = DATABASE_DIR / "Vajra_Master_Market_Data.duckdb"
NIFTY500_PIT_CHECKPOINTS = NIFTY500_PIT / "12 Checkpoints"

EXTRACTED_ORIGINAL_DATA = PROTECTED_SOURCE / "02 Extracted Original Data"
LIVE_UDIFF_ROOT = INCOMING_EOD / "01 Official UDiFF ZIP"
LIVE_VALIDATION_ROOT = INCOMING_EOD / "03 Daily Validation Reports"

PRODUCTION_LOGS = STORE_LOGS / "NIFTY500 Production"

# --- published data ------------------------------------------------------------
PUBLISHED_N500 = DATA_ROOT / "nifty500"
PUBLISHED_N750 = DATA_ROOT / "nifty750"
PUBLISHED_CORPORATE_ACTIONS = DATA_ROOT / "corporate_actions"
PUBLISHED_CALENDAR = DATA_ROOT / "calendar"
MANIFEST_PATH = DATA_ROOT / "MANIFEST.json"
CHANGELOG_PATH = DATA_ROOT / "CHANGELOG.md"
QUALITY_REPORT_PATH = DATA_ROOT / "DATA_QUALITY_REPORT.md"
START_HERE_PATH = DATA_ROOT / "START_HERE_AI.md"
DATA_DICTIONARY_PATH = DATA_ROOT / "DATA_DICTIONARY.md"


def published_universe(universe: str) -> Path:
    """``universe`` is ``"nifty500"`` or ``"nifty750"``."""
    if universe not in {"nifty500", "nifty750"}:
        raise ValueError(f"Unknown universe: {universe!r}")
    return DATA_ROOT / universe


def as_dict() -> dict[str, str]:
    """Every resolved root, for status files and the doctor command."""
    return {
        "engine_root": str(ENGINE_ROOT),
        "store_root": str(STORE_ROOT),
        "data_root": str(DATA_ROOT),
        "master_data": str(MASTER_DATA),
        "nifty500_pit": str(NIFTY500_PIT),
        "master_db": str(MASTER_DB),
        "logs_root": str(LOGS_ROOT),
        "store_logs": str(STORE_LOGS),
    }
