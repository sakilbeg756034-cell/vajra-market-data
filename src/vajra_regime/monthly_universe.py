from __future__ import annotations

import json
import os
import re
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from vajra_regime.config import AppConfig
from vajra_regime.data_layout import DataLayout


HISTORICAL_END = date(2025, 12, 31)
LIVE_START = date(2026, 1, 1)
HISTORICAL_FIRST_REBALANCE = date(2010, 8, 31)
HISTORICAL_MONTHS = 185
HISTORICAL_UNIVERSE_ROWS = 138_750
UNIVERSE_SIZE = 750
STALE_CALENDAR_DAYS = 7
MIN_HISTORY_SESSIONS = 252
MIN_TURNOVER_OBSERVATIONS = 40
LEGACY_UNIVERSE_TABLE = "monthly_vajra_750_universe_legacy_2010_2025"
LEGACY_COVERAGE_TABLE = "monthly_750_coverage_legacy_2010_2025"
COVERAGE_TABLE = "monthly_750_coverage"
LIVE_CANDIDATE_TABLE = "monthly_candidate_ranking_live_2026"
NEXT_UNIVERSE_TABLE = "monthly_vajra_750_universe_next"
NEXT_COVERAGE_TABLE = "monthly_750_coverage_next"

UNIVERSE_COLUMNS = (
    "RebalanceDate",
    "MonthStart",
    "SecurityLastDate",
    "StaleCalendarDays",
    "Symbol",
    "ISIN",
    "Close",
    "Volume",
    "Turnover",
    "HistoryCount",
    "MedianTurnover60",
    "TurnoverObservations60",
    "LiquidityRank",
    "LargeReturnAnomalyFlag",
    "LongGapOver30DaysFlag",
)


def _safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _table_exists(connection: duckdb.DuckDBPyConnection, table: str) -> bool:
    return table in {row[0] for row in connection.execute("SHOW TABLES").fetchall()}


def _table_columns(connection: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    }


def _require_columns(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    required: set[str],
) -> None:
    columns = _table_columns(connection, table)
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(f"Table {table} is missing required columns: {missing}")


def _months_between(first_month: date, last_month: date) -> int:
    if last_month < first_month:
        return 0
    return (last_month.year - first_month.year) * 12 + last_month.month - first_month.month + 1


def _previous_month_start(month_start: date) -> date:
    if month_start.month == 1:
        return date(month_start.year - 1, 12, 1)
    return date(month_start.year, month_start.month - 1, 1)


def _historical_universe_summary(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> dict[str, object]:
    row = connection.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT CAST(RebalanceDate AS DATE)),
            MIN(CAST(RebalanceDate AS DATE)),
            MAX(CAST(RebalanceDate AS DATE))
        FROM {table}
        WHERE CAST(RebalanceDate AS DATE) <= DATE '{HISTORICAL_END.isoformat()}'
        """
    ).fetchone()
    duplicate_memberships = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT CAST(RebalanceDate AS DATE) AS RebalanceDate, ISIN, COUNT(*) AS n
                FROM {table}
                WHERE CAST(RebalanceDate AS DATE) <= DATE '{HISTORICAL_END.isoformat()}'
                GROUP BY RebalanceDate, ISIN
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    bad_months = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT
                    CAST(RebalanceDate AS DATE) AS RebalanceDate,
                    COUNT(*) AS Members,
                    MIN(LiquidityRank) AS MinRank,
                    MAX(LiquidityRank) AS MaxRank,
                    COUNT(DISTINCT LiquidityRank) AS DistinctRanks
                FROM {table}
                WHERE CAST(RebalanceDate AS DATE) <= DATE '{HISTORICAL_END.isoformat()}'
                GROUP BY RebalanceDate
                HAVING Members <> {UNIVERSE_SIZE}
                    OR MinRank <> 1
                    OR MaxRank <> {UNIVERSE_SIZE}
                    OR DistinctRanks <> {UNIVERSE_SIZE}
            )
            """
        ).fetchone()[0]
    )
    return {
        "rows": int(row[0] or 0),
        "months": int(row[1] or 0),
        "first_rebalance": str(row[2]) if row[2] is not None else None,
        "last_rebalance": str(row[3]) if row[3] is not None else None,
        "duplicate_membership_groups": duplicate_memberships,
        "bad_full_750_months": bad_months,
    }


def _validate_historical_universe(summary: dict[str, object]) -> None:
    expected = {
        "rows": HISTORICAL_UNIVERSE_ROWS,
        "months": HISTORICAL_MONTHS,
        "first_rebalance": HISTORICAL_FIRST_REBALANCE.isoformat(),
        "last_rebalance": HISTORICAL_END.isoformat(),
        "duplicate_membership_groups": 0,
        "bad_full_750_months": 0,
    }
    mismatches = {
        key: {"expected": value, "actual": summary.get(key)}
        for key, value in expected.items()
        if summary.get(key) != value
    }
    if mismatches:
        raise ValueError(
            "Protected 2010-2025 monthly 750 universe failed parity validation: "
            + json.dumps(mismatches, default=str)
        )


def _prepare_legacy_snapshots(
    connection: duckdb.DuckDBPyConnection,
    universe_table: str,
) -> None:
    if not _table_exists(connection, universe_table):
        raise ValueError(f"Missing historical monthly universe table: {universe_table}")
    _require_columns(connection, universe_table, set(UNIVERSE_COLUMNS))

    historical = _historical_universe_summary(connection, universe_table)
    _validate_historical_universe(historical)

    if not _table_exists(connection, LEGACY_UNIVERSE_TABLE):
        connection.execute(
            f"""
            CREATE TABLE {LEGACY_UNIVERSE_TABLE} AS
            SELECT *
            FROM {universe_table}
            WHERE CAST(RebalanceDate AS DATE) <= DATE '{HISTORICAL_END.isoformat()}'
            ORDER BY RebalanceDate, LiquidityRank
            """
        )
    legacy = _historical_universe_summary(connection, LEGACY_UNIVERSE_TABLE)
    _validate_historical_universe(legacy)

    if not _table_exists(connection, COVERAGE_TABLE):
        raise ValueError(f"Missing historical coverage table: {COVERAGE_TABLE}")
    _require_columns(
        connection,
        COVERAGE_TABLE,
        {"RebalanceDate", "EligibleCandidates", "SelectedMembers", "UniverseStatus"},
    )
    if not _table_exists(connection, LEGACY_COVERAGE_TABLE):
        connection.execute(
            f"""
            CREATE TABLE {LEGACY_COVERAGE_TABLE} AS
            SELECT *
            FROM {COVERAGE_TABLE}
            WHERE CAST(RebalanceDate AS DATE) <= DATE '{HISTORICAL_END.isoformat()}'
            ORDER BY RebalanceDate
            """
        )
    legacy_coverage = connection.execute(
        f"""
        SELECT
            COUNT(*),
            MIN(CAST(RebalanceDate AS DATE)),
            MAX(CAST(RebalanceDate AS DATE)),
            SUM(CASE WHEN SelectedMembers = {UNIVERSE_SIZE} THEN 1 ELSE 0 END)
        FROM {LEGACY_COVERAGE_TABLE}
        """
    ).fetchone()
    coverage_ok = (
        int(legacy_coverage[0] or 0) == HISTORICAL_MONTHS
        and str(legacy_coverage[1]) == HISTORICAL_FIRST_REBALANCE.isoformat()
        and str(legacy_coverage[2]) == HISTORICAL_END.isoformat()
        and int(legacy_coverage[3] or 0) == HISTORICAL_MONTHS
    )
    if not coverage_ok:
        raise ValueError("Protected 2010-2025 monthly coverage table failed validation.")


def _latest_clean_date(
    connection: duckdb.DuckDBPyConnection,
    clean_table: str,
) -> date:
    maximum = connection.execute(
        f"SELECT MAX(CAST(Date AS DATE)) FROM {clean_table}"
    ).fetchone()[0]
    if maximum is None:
        raise ValueError(f"Clean table {clean_table} is empty.")
    if isinstance(maximum, date):
        return maximum
    return date.fromisoformat(str(maximum)[:10])


def _build_live_candidates(
    connection: duckdb.DuckDBPyConnection,
    clean_table: str,
    current_data_month_start: date,
) -> None:
    _require_columns(
        connection,
        clean_table,
        {
            "Date",
            "Symbol",
            "ISIN",
            "Close",
            "Volume",
            "Turnover",
            "HistoryCount",
            "MedianTurnover60",
            "TurnoverObservations60",
            "LargeReturnAnomalyFlag",
            "LongGapOver30DaysFlag",
            "IsResearchEligible",
            "Series",
        },
    )
    connection.execute(f"DROP TABLE IF EXISTS {LIVE_CANDIDATE_TABLE}")
    connection.execute(
        f"""
        CREATE TABLE {LIVE_CANDIDATE_TABLE} AS
        WITH month_security AS (
            SELECT
                DATE_TRUNC('month', Date)::DATE AS MonthStart,
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE_TRUNC('month', Date), ISIN
                    ORDER BY Date DESC, Volume DESC, Symbol ASC
                ) AS MonthSecurityRank
            FROM {clean_table}
            WHERE CAST(Date AS DATE) >= DATE '{LIVE_START.isoformat()}'
              AND DATE_TRUNC('month', Date)::DATE
                    < DATE '{current_data_month_start.isoformat()}'
        ),
        latest AS (
            SELECT * FROM month_security
            WHERE MonthSecurityRank = 1
        ),
        market_end AS (
            SELECT
                MonthStart,
                MAX(CAST(Date AS DATE)) AS RebalanceDate
            FROM latest
            GROUP BY MonthStart
        ),
        candidate AS (
            SELECT
                l.MonthStart,
                m.RebalanceDate,
                CAST(l.Date AS DATE) AS SecurityLastDate,
                DATE_DIFF('day', CAST(l.Date AS DATE), m.RebalanceDate) AS StaleCalendarDays,
                l.Symbol,
                l.ISIN,
                l.Series,
                l.Close,
                l.Volume,
                l.Turnover,
                l.HistoryCount,
                l.MedianTurnover60,
                l.TurnoverObservations60,
                l.LargeReturnAnomalyFlag,
                l.LongGapOver30DaysFlag,
                l.IsResearchEligible AS ResearchEligibleAtSnapshot,
                CASE
                    WHEN DATE_DIFF('day', CAST(l.Date AS DATE), m.RebalanceDate)
                            <= {STALE_CALENDAR_DAYS}
                     AND l.HistoryCount >= {MIN_HISTORY_SESSIONS}
                     AND l.TurnoverObservations60 >= {MIN_TURNOVER_OBSERVATIONS}
                     AND l.MedianTurnover60 > 0
                     AND l.IsResearchEligible
                     -- Rebalance ke din stock EQ me hona chahiye.
                     --
                     -- BE/BZ surveillance segment hai: wahan har sauda delivery
                     -- me settle karna padta hai, intraday mana hai, aur aksar
                     -- 100% margin lagta hai. Us daur ka DATA hum rakhte hain
                     -- (warna price series me hole ban jaata hai aur R12 jhutha
                     -- ho jaata hai), par us din KHAREEDNA alag baat hai.
                     --
                     -- Ye rok yahan hai, LiquidityRank se PEHLE -- isliye aisa
                     -- naam top-750 ki ek jagah bhi nahi ghera.
                     AND l.Series = 'EQ'
                    THEN TRUE ELSE FALSE
                END AS EligibleForUniverse
            FROM latest l
            JOIN market_end m USING (MonthStart)
        )
        SELECT
            *,
            CASE
                WHEN EligibleForUniverse
                THEN ROW_NUMBER() OVER (
                    PARTITION BY RebalanceDate, EligibleForUniverse
                    ORDER BY MedianTurnover60 DESC, ISIN ASC, Symbol ASC
                )
                ELSE NULL
            END AS LiquidityRank
        FROM candidate
        ORDER BY RebalanceDate, LiquidityRank, ISIN
        """
    )


def _build_next_tables(
    connection: duckdb.DuckDBPyConnection,
    universe_table: str,
) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {NEXT_UNIVERSE_TABLE}")
    connection.execute(
        f"CREATE TABLE {NEXT_UNIVERSE_TABLE} AS SELECT * FROM {LEGACY_UNIVERSE_TABLE}"
    )
    connection.execute(
        f"""
        INSERT INTO {NEXT_UNIVERSE_TABLE} ({', '.join(UNIVERSE_COLUMNS)})
        SELECT
            RebalanceDate,
            MonthStart,
            SecurityLastDate,
            StaleCalendarDays,
            Symbol,
            ISIN,
            Close,
            Volume,
            Turnover,
            HistoryCount,
            MedianTurnover60,
            TurnoverObservations60,
            LiquidityRank,
            LargeReturnAnomalyFlag,
            LongGapOver30DaysFlag
        FROM {LIVE_CANDIDATE_TABLE}
        WHERE EligibleForUniverse
          AND LiquidityRank <= {UNIVERSE_SIZE}
        ORDER BY RebalanceDate, LiquidityRank
        """
    )

    connection.execute(f"DROP TABLE IF EXISTS {NEXT_COVERAGE_TABLE}")
    connection.execute(
        f"CREATE TABLE {NEXT_COVERAGE_TABLE} AS SELECT * FROM {LEGACY_COVERAGE_TABLE}"
    )
    connection.execute(
        f"""
        INSERT INTO {NEXT_COVERAGE_TABLE}
        SELECT
            c.RebalanceDate,
            COUNT(CASE WHEN c.EligibleForUniverse THEN 1 ELSE NULL END) AS EligibleCandidates,
            COUNT(u.ISIN) AS SelectedMembers,
            CASE
                WHEN COUNT(u.ISIN) = {UNIVERSE_SIZE} THEN 'FULL_750'
                ELSE 'PARTIAL_' || CAST(COUNT(u.ISIN) AS VARCHAR)
            END AS UniverseStatus
        FROM {LIVE_CANDIDATE_TABLE} c
        LEFT JOIN {NEXT_UNIVERSE_TABLE} u
          ON CAST(u.RebalanceDate AS DATE) = CAST(c.RebalanceDate AS DATE)
         AND u.ISIN = c.ISIN
        WHERE CAST(c.RebalanceDate AS DATE) >= DATE '{LIVE_START.isoformat()}'
        GROUP BY c.RebalanceDate
        ORDER BY c.RebalanceDate
        """
    )


def _validate_next_tables(
    connection: duckdb.DuckDBPyConnection,
    current_data_month_start: date,
) -> dict[str, object]:
    historical = _historical_universe_summary(connection, NEXT_UNIVERSE_TABLE)
    _validate_historical_universe(historical)

    expected_last_live_month = _previous_month_start(current_data_month_start)
    expected_live_months = _months_between(LIVE_START, expected_last_live_month)

    live_row = connection.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT CAST(RebalanceDate AS DATE)),
            MIN(CAST(RebalanceDate AS DATE)),
            MAX(CAST(RebalanceDate AS DATE))
        FROM {NEXT_UNIVERSE_TABLE}
        WHERE CAST(RebalanceDate AS DATE) >= DATE '{LIVE_START.isoformat()}'
        """
    ).fetchone()
    live_rows = int(live_row[0] or 0)
    live_months = int(live_row[1] or 0)
    live_first = str(live_row[2]) if live_row[2] is not None else None
    live_last = str(live_row[3]) if live_row[3] is not None else None

    duplicate_memberships = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT CAST(RebalanceDate AS DATE) AS RebalanceDate, ISIN, COUNT(*) AS n
                FROM {NEXT_UNIVERSE_TABLE}
                GROUP BY RebalanceDate, ISIN
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    duplicate_ranks = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT
                    CAST(RebalanceDate AS DATE) AS RebalanceDate,
                    LiquidityRank,
                    COUNT(*) AS n
                FROM {NEXT_UNIVERSE_TABLE}
                GROUP BY RebalanceDate, LiquidityRank
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    partial_live_months = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {NEXT_COVERAGE_TABLE}
            WHERE CAST(RebalanceDate AS DATE) >= DATE '{LIVE_START.isoformat()}'
              AND SelectedMembers <> {UNIVERSE_SIZE}
            """
        ).fetchone()[0]
    )
    live_coverage_months = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {NEXT_COVERAGE_TABLE}
            WHERE CAST(RebalanceDate AS DATE) >= DATE '{LIVE_START.isoformat()}'
            """
        ).fetchone()[0]
    )
    current_partial_month_rows = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {NEXT_UNIVERSE_TABLE}
            WHERE DATE_TRUNC('month', CAST(RebalanceDate AS DATE))::DATE
                    >= DATE '{current_data_month_start.isoformat()}'
            """
        ).fetchone()[0]
    )
    quarantine_excluded = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {LIVE_CANDIDATE_TABLE}
            WHERE NOT ResearchEligibleAtSnapshot
            """
        ).fetchone()[0]
    )

    expected_live_rows = expected_live_months * UNIVERSE_SIZE
    ok = (
        live_months == expected_live_months
        and live_coverage_months == expected_live_months
        and live_rows == expected_live_rows
        and partial_live_months == 0
        and duplicate_memberships == 0
        and duplicate_ranks == 0
        and current_partial_month_rows == 0
    )
    if not ok:
        raise ValueError(
            "Live monthly 750 continuation failed validation. "
            f"Expected months={expected_live_months}, actual months={live_months}, "
            f"expected rows={expected_live_rows}, actual rows={live_rows}, "
            f"partial months={partial_live_months}, duplicate memberships={duplicate_memberships}, "
            f"duplicate ranks={duplicate_ranks}, current-partial rows={current_partial_month_rows}."
        )

    return {
        "ok": True,
        "historical_months_preserved": HISTORICAL_MONTHS,
        "historical_rows_preserved": HISTORICAL_UNIVERSE_ROWS,
        "expected_live_completed_months": expected_live_months,
        "live_completed_months": live_months,
        "live_selected_rows": live_rows,
        "live_first_rebalance": live_first,
        "live_last_rebalance": live_last,
        "partial_live_months": partial_live_months,
        "duplicate_rebalance_isin_groups": duplicate_memberships,
        "duplicate_rebalance_rank_groups": duplicate_ranks,
        "current_partial_month_rows": current_partial_month_rows,
        "quarantine_excluded_candidate_rows": quarantine_excluded,
    }


def _swap_tables(
    connection: duckdb.DuckDBPyConnection,
    universe_table: str,
) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DROP TABLE {universe_table}")
        connection.execute(f"ALTER TABLE {NEXT_UNIVERSE_TABLE} RENAME TO {universe_table}")
        connection.execute(f"DROP TABLE {COVERAGE_TABLE}")
        connection.execute(f"ALTER TABLE {NEXT_COVERAGE_TABLE} RENAME TO {COVERAGE_TABLE}")
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_universe_rebalance ON {universe_table}(RebalanceDate)"
        )
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_universe_isin ON {universe_table}(ISIN)"
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _atomic_copy_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    destination: Path,
    format_name: str,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    if temporary.exists():
        temporary.unlink()
    options = "FORMAT PARQUET, COMPRESSION ZSTD" if format_name == "parquet" else "FORMAT CSV, HEADER TRUE"
    connection.execute(
        f"COPY ({query}) TO '{_sql_path(temporary)}' ({options})"
    )
    os.replace(temporary, destination)


def _export_outputs(
    connection: duckdb.DuckDBPyConnection,
    config: AppConfig,
    universe_table: str,
) -> dict[str, str]:
    layout = DataLayout.from_root(config.environment.root)
    universe_dir = layout.master_data / "02 Monthly 750 Universe"
    universe_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = layout.backups / "Monthly 750 Universe Legacy 2010-2025"
    backup_dir.mkdir(parents=True, exist_ok=True)

    legacy_parquet = backup_dir / "Monthly_Vajra_750_Universe_Legacy_2010_2025.parquet"
    legacy_csv = backup_dir / "Monthly_Vajra_750_Universe_Legacy_2010_2025.csv"
    if not legacy_parquet.exists():
        _atomic_copy_query(
            connection,
            f"SELECT * FROM {LEGACY_UNIVERSE_TABLE} ORDER BY RebalanceDate, LiquidityRank",
            legacy_parquet,
            "parquet",
        )
    if not legacy_csv.exists():
        _atomic_copy_query(
            connection,
            f"SELECT * FROM {LEGACY_UNIVERSE_TABLE} ORDER BY RebalanceDate, LiquidityRank",
            legacy_csv,
            "csv",
        )

    universe_parquet = universe_dir / "Monthly_Vajra_750_Universe.parquet"
    universe_csv = universe_dir / "Monthly_Vajra_750_Universe.csv"
    coverage_csv = universe_dir / "Monthly_750_Coverage.csv"
    coverage_parquet = universe_dir / "Monthly_750_Coverage.parquet"
    live_candidate_parquet = universe_dir / "Monthly_Live_Candidate_Ranking_2026.parquet"
    live_candidate_csv = universe_dir / "Monthly_Live_Candidate_Ranking_2026.csv"

    _atomic_copy_query(
        connection,
        f"SELECT * FROM {universe_table} ORDER BY RebalanceDate, LiquidityRank",
        universe_parquet,
        "parquet",
    )
    _atomic_copy_query(
        connection,
        f"SELECT * FROM {universe_table} ORDER BY RebalanceDate, LiquidityRank",
        universe_csv,
        "csv",
    )
    _atomic_copy_query(
        connection,
        f"SELECT * FROM {COVERAGE_TABLE} ORDER BY RebalanceDate",
        coverage_csv,
        "csv",
    )
    _atomic_copy_query(
        connection,
        f"SELECT * FROM {COVERAGE_TABLE} ORDER BY RebalanceDate",
        coverage_parquet,
        "parquet",
    )
    _atomic_copy_query(
        connection,
        f"SELECT * FROM {LIVE_CANDIDATE_TABLE} ORDER BY RebalanceDate, LiquidityRank, ISIN",
        live_candidate_parquet,
        "parquet",
    )
    _atomic_copy_query(
        connection,
        f"SELECT * FROM {LIVE_CANDIDATE_TABLE} ORDER BY RebalanceDate, LiquidityRank, ISIN",
        live_candidate_csv,
        "csv",
    )
    return {
        "universe_parquet": str(universe_parquet),
        "universe_csv": str(universe_csv),
        "coverage_csv": str(coverage_csv),
        "coverage_parquet": str(coverage_parquet),
        "live_candidate_parquet": str(live_candidate_parquet),
        "live_candidate_csv": str(live_candidate_csv),
        "legacy_backup_parquet": str(legacy_parquet),
        "legacy_backup_csv": str(legacy_csv),
    }


def continue_monthly_750_universe(config: AppConfig) -> dict[str, object]:
    """Preserve the exact 2010-2025 750 universe and extend only completed live months.

    The historical algorithm is copied from the final data-foundation notebook:
    latest observation per security/month, <=7 calendar days stale, >=252 history
    sessions, >=40 turnover observations in the latest 60 sessions, positive
    MedianTurnover60, then rank by MedianTurnover60 DESC, ISIN ASC, Symbol ASC.

    For 2026 onward, the rolling master's IsResearchEligible flag is an additional
    safety gate so unresolved complex corporate-action/relisting cases are excluded
    automatically. A month is considered complete only after the clean master contains
    at least one observation in the following calendar month. This prevents a partial
    current month from being frozen as a final 750 snapshot.
    """
    database = Path(config.environment.duckdb_path)
    if not database.exists():
        raise FileNotFoundError(f"Rolling master DuckDB not found: {database}")

    clean_table = _safe_identifier(str(config.data["clean_table"]))
    universe_table = _safe_identifier(str(config.data["universe_table"]))
    expected_size = int(config.data.get("expected_universe_size", UNIVERSE_SIZE))
    if expected_size != UNIVERSE_SIZE:
        raise ValueError(
            f"This continuation engine is locked to 750 members; config requested {expected_size}."
        )

    with duckdb.connect(str(database), read_only=False) as connection:
        if not _table_exists(connection, clean_table):
            raise ValueError(f"Missing rolling clean table: {clean_table}")
        _prepare_legacy_snapshots(connection, universe_table)

        latest_clean = _latest_clean_date(connection, clean_table)
        current_data_month_start = latest_clean.replace(day=1)
        desired_last_live_month = _previous_month_start(current_data_month_start)

        existing_max = connection.execute(
            f"SELECT MAX(CAST(RebalanceDate AS DATE)) FROM {universe_table}"
        ).fetchone()[0]
        existing_max_date = (
            date.fromisoformat(str(existing_max)[:10]) if existing_max is not None else None
        )
        already_current = (
            desired_last_live_month < LIVE_START
            or (
                existing_max_date is not None
                and existing_max_date.year == desired_last_live_month.year
                and existing_max_date.month == desired_last_live_month.month
            )
        )

        _build_live_candidates(connection, clean_table, current_data_month_start)
        _build_next_tables(connection, universe_table)
        validation = _validate_next_tables(connection, current_data_month_start)
        _swap_tables(connection, universe_table)
        outputs = _export_outputs(connection, config, universe_table)

    layout = DataLayout.from_root(config.environment.root)
    status_dir = layout.logs / "Monthly Universe"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / "latest_monthly_750_universe_status.json"
    quality_path = (
        layout.master_data
        / "03 Quality Reports"
        / "10_Monthly_750_Continuation_Summary.json"
    )
    quality_path.parent.mkdir(parents=True, exist_ok=True)

    status = {
        "status": "SUCCESS",
        "outcome": "ALREADY_CURRENT_REBUILT_AND_VERIFIED" if already_current else "UPDATED",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "latest_clean_date": latest_clean.isoformat(),
        "current_partial_data_month": current_data_month_start.isoformat(),
        "last_completed_live_month_start": (
            desired_last_live_month.isoformat()
            if desired_last_live_month >= LIVE_START
            else None
        ),
        **validation,
        "outputs": outputs,
        "published_data_changed_during_build": False,
        "manual_universe_edit_required": False,
        "rule": {
            "stale_calendar_days_max": STALE_CALENDAR_DAYS,
            "minimum_history_sessions": MIN_HISTORY_SESSIONS,
            "minimum_turnover_observations_60": MIN_TURNOVER_OBSERVATIONS,
            "liquidity_rank_order": "MedianTurnover60 DESC, ISIN ASC, Symbol ASC",
            "live_additional_gate": "IsResearchEligible = TRUE",
            "month_completion_rule": "next calendar month must exist in clean_daily",
        },
    }
    status_path.write_text(json.dumps(status, indent=2, default=str), encoding="utf-8")
    quality_path.write_text(json.dumps(status, indent=2, default=str), encoding="utf-8")
    status["status_path"] = str(status_path)
    status["quality_summary_path"] = str(quality_path)
    return status
