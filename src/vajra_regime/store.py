from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd


REQUIRED_DAILY_COLUMNS = {
    "Date",
    "Symbol",
    "ISIN",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "IsBacktestPeriod",
}


def _safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


class VajraStore:
    """Read-only interface to the local clean DuckDB database."""

    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)

    def assert_exists(self) -> None:
        if not self.database_path.exists():
            raise FileNotFoundError(
                f"DuckDB database not found: {self.database_path}. "
                "Check VAJRA_DUCKDB_PATH in .env."
            )

    @contextmanager
    def connect(self, read_only: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
        self.assert_exists()
        connection = duckdb.connect(str(self.database_path), read_only=read_only)
        try:
            yield connection
        finally:
            connection.close()

    def list_tables(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute("SHOW TABLES").fetchall()
        return [row[0] for row in rows]

    def validate_schema(self, daily_table: str, universe_table: str) -> dict[str, object]:
        daily_table = _safe_identifier(daily_table)
        universe_table = _safe_identifier(universe_table)
        tables = set(self.list_tables())
        missing_tables = sorted({daily_table, universe_table}.difference(tables))
        if missing_tables:
            return {"ok": False, "missing_tables": missing_tables, "missing_columns": []}

        with self.connect() as connection:
            columns = {
                row[1]
                for row in connection.execute(f"PRAGMA table_info('{daily_table}')").fetchall()
            }
        missing_columns = sorted(REQUIRED_DAILY_COLUMNS.difference(columns))
        return {
            "ok": not missing_columns,
            "missing_tables": [],
            "missing_columns": missing_columns,
        }

    def load_daily(
        self,
        table: str,
        start_date: str,
        end_date: str,
        columns: tuple[str, ...] = ("Date", "Symbol", "ISIN", "Close", "Volume"),
    ) -> pd.DataFrame:
        table = _safe_identifier(table)
        allowed = {
            "Date", "Year", "Symbol", "ISIN", "Open", "High", "Low", "Close",
            "Volume", "Turnover", "Return1D", "GapDays", "HistoryCount",
            "MedianTurnover60", "TurnoverObservations60", "LargeReturnAnomalyFlag",
            "LongGapOver30DaysFlag", "IsWarmupPeriod", "IsBacktestPeriod",
        }
        unknown = set(columns).difference(allowed)
        if unknown:
            raise ValueError(f"Unsupported columns requested: {sorted(unknown)}")
        projection = ", ".join(f'"{column}"' for column in columns)
        query = f"""
            SELECT {projection}
            FROM {table}
            WHERE Date BETWEEN ? AND ?
            ORDER BY Date, ISIN
        """
        with self.connect() as connection:
            return connection.execute(query, [start_date, end_date]).df()

    def load_point_in_time_daily(
        self,
        daily_table: str,
        universe_table: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Load stocks valid in the latest completed monthly 750 snapshot.

        A month-end universe becomes usable from the next calendar day and remains
        active through the next rebalance date. This prevents using a month-end list
        earlier in the same month.
        """
        daily_table = _safe_identifier(daily_table)
        universe_table = _safe_identifier(universe_table)
        query = f"""
            WITH rebalance_dates AS (
                SELECT
                    RebalanceDate,
                    LEAD(RebalanceDate) OVER (ORDER BY RebalanceDate) AS NextRebalanceDate
                FROM (
                    SELECT DISTINCT RebalanceDate
                    FROM {universe_table}
                )
            ),
            membership AS (
                SELECT
                    u.RebalanceDate,
                    r.NextRebalanceDate,
                    u.ISIN,
                    u.LiquidityRank
                FROM {universe_table} u
                JOIN rebalance_dates r USING (RebalanceDate)
            )
            SELECT
                d.Date,
                d.Symbol,
                d.ISIN,
                d.Open,
                d.High,
                d.Low,
                d.Close,
                d.Volume,
                d.Turnover,
                d.Return1D,
                d.GapDays,
                d.LargeReturnAnomalyFlag,
                d.LongGapOver30DaysFlag,
                m.RebalanceDate AS UniverseSnapshotDate,
                m.LiquidityRank
            FROM {daily_table} d
            JOIN membership m
              ON d.ISIN = m.ISIN
             AND d.Date > m.RebalanceDate
             AND (m.NextRebalanceDate IS NULL OR d.Date <= m.NextRebalanceDate)
            WHERE d.Date BETWEEN ? AND ?
            ORDER BY d.Date, m.LiquidityRank
        """
        with self.connect() as connection:
            return connection.execute(query, [start_date, end_date]).df()

    def load_weekly_rank_panel(
        self,
        daily_table: str,
        universe_table: str,
        warmup_start: str,
        backtest_start: str,
        backtest_end: str,
        minimum_history_sessions: int = 252,
        minimum_price: float = 50.0,
        minimum_turnover_60: float = 100_000_000.0,
        maximum_distance_from_high: float = 0.25,
        readiness_ratio: float = 0.95,
    ) -> pd.DataFrame:
        """Build the locked weekly APEX rank panel directly inside DuckDB.

        The current 750-stock point-in-time universe is a research proxy. Return windows
        use trading-session equivalents and the 52-week-high gate currently uses raw Close;
        outputs must remain marked provisional until split-adjusted prices are available.
        """
        daily_table = _safe_identifier(daily_table)
        universe_table = _safe_identifier(universe_table)
        query = f"""
            WITH history AS (
                SELECT
                    Date,
                    Symbol,
                    ISIN,
                    Close,
                    MedianTurnover60,
                    HistoryCount,
                    LAG(Close, 63) OVER (
                        PARTITION BY ISIN ORDER BY Date
                    ) AS Close63,
                    LAG(Close, 126) OVER (
                        PARTITION BY ISIN ORDER BY Date
                    ) AS Close126,
                    LAG(Close, 252) OVER (
                        PARTITION BY ISIN ORDER BY Date
                    ) AS Close252,
                    MAX(Close) OVER (
                        PARTITION BY ISIN ORDER BY Date
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS High252,
                    REGR_R2(
                        CASE WHEN Close > 0 THEN LN(Close) END,
                        EPOCH(Date)
                    ) OVER (
                        PARTITION BY ISIN ORDER BY Date
                        ROWS BETWEEN 125 PRECEDING AND CURRENT ROW
                    ) AS R2_6M
                FROM {daily_table}
                WHERE Date BETWEEN ? AND ?
            ),
            weekly_dates AS (
                SELECT
                    DATE_TRUNC('week', Date) AS WeekStart,
                    MAX(Date) AS SignalDate
                FROM {daily_table}
                WHERE Date BETWEEN ? AND ?
                GROUP BY DATE_TRUNC('week', Date)
            ),
            rebalance_dates AS (
                SELECT
                    RebalanceDate,
                    LEAD(RebalanceDate) OVER (ORDER BY RebalanceDate) AS NextRebalanceDate
                FROM (
                    SELECT DISTINCT RebalanceDate
                    FROM {universe_table}
                )
            ),
            membership AS (
                SELECT
                    u.RebalanceDate,
                    r.NextRebalanceDate,
                    u.ISIN,
                    u.LiquidityRank
                FROM {universe_table} u
                JOIN rebalance_dates r USING (RebalanceDate)
            ),
            base AS (
                SELECT
                    w.WeekStart,
                    w.SignalDate,
                    h.Symbol,
                    h.ISIN,
                    h.Close,
                    h.MedianTurnover60,
                    h.HistoryCount,
                    h.R2_6M,
                    h.Close / NULLIF(h.Close252, 0) - 1 AS Return12M,
                    h.Close / NULLIF(h.Close126, 0) - 1 AS Return6M,
                    h.Close / NULLIF(h.Close63, 0) - 1 AS Return3M,
                    h.Close / NULLIF(h.High252, 0) - 1 AS DistanceFrom52WHigh,
                    m.RebalanceDate AS UniverseSnapshotDate,
                    m.LiquidityRank
                FROM weekly_dates w
                JOIN history h ON h.Date = w.SignalDate
                JOIN membership m
                  ON h.ISIN = m.ISIN
                 AND h.Date > m.RebalanceDate
                 AND (m.NextRebalanceDate IS NULL OR h.Date <= m.NextRebalanceDate)
            ),
            scored AS (
                SELECT
                    *,
                    0.50 * Return12M + 0.30 * Return6M + 0.20 * Return3M
                        AS WeightedMomentum,
                    CASE
                        WHEN HistoryCount >= ?
                         AND Close > ?
                         AND MedianTurnover60 > ?
                         AND DistanceFrom52WHigh >= -?
                         AND Return12M IS NOT NULL
                         AND Return6M IS NOT NULL
                         AND Return3M IS NOT NULL
                         AND R2_6M IS NOT NULL
                        THEN TRUE ELSE FALSE
                    END AS Eligible
                FROM base
            ),
            percentiles AS (
                SELECT
                    *,
                    PERCENT_RANK() OVER (
                        PARTITION BY SignalDate
                        ORDER BY WeightedMomentum ASC NULLS FIRST
                    ) * 100 AS MomentumPercentile,
                    PERCENT_RANK() OVER (
                        PARTITION BY SignalDate
                        ORDER BY R2_6M ASC NULLS FIRST
                    ) * 100 AS R2Percentile
                FROM scored
            ),
            apex AS (
                SELECT
                    *,
                    0.60 * MomentumPercentile + 0.40 * R2Percentile AS APEXScore
                FROM percentiles
            ),
            ranked AS (
                SELECT
                    *,
                    CASE WHEN Eligible THEN
                        ROW_NUMBER() OVER (
                            PARTITION BY SignalDate
                            ORDER BY
                                CASE WHEN Eligible THEN APEXScore END DESC NULLS LAST,
                                WeightedMomentum DESC NULLS LAST,
                                R2_6M DESC NULLS LAST,
                                LiquidityRank ASC,
                                ISIN ASC
                        )
                    ELSE 9999 END AS FinalRank,
                    COUNT(*) OVER (PARTITION BY SignalDate) AS UniverseCount,
                    COUNT(APEXScore) OVER (PARTITION BY SignalDate) AS ValidScoreCount,
                    SUM(CASE WHEN Eligible THEN 1 ELSE 0 END) OVER (
                        PARTITION BY SignalDate
                    ) AS EligibleCount
                FROM apex
            )
            SELECT
                *,
                ValidScoreCount >= CEIL(UniverseCount * ?) AND EligibleCount > 0
                    AS SystemReady,
                'MONTHLY_750_PROXY' AS ResearchUniverse,
                'RAW_CLOSE_PROXY' AS HighFilterPriceBasis
            FROM ranked
            ORDER BY SignalDate, FinalRank, LiquidityRank, ISIN
        """
        parameters = [
            warmup_start,
            backtest_end,
            backtest_start,
            backtest_end,
            int(minimum_history_sessions),
            float(minimum_price),
            float(minimum_turnover_60),
            float(maximum_distance_from_high),
            float(readiness_ratio),
        ]
        with self.connect() as connection:
            return connection.execute(query, parameters).df()

    def load_backtest_daily_prices(
        self,
        daily_table: str,
        warmup_start: str,
        backtest_start: str,
        backtest_end: str,
    ) -> pd.DataFrame:
        """Load a minimal daily execution panel with a five-session emergency return."""
        daily_table = _safe_identifier(daily_table)
        query = f"""
            WITH history AS (
                SELECT
                    Date,
                    Symbol,
                    ISIN,
                    Open,
                    Close,
                    Return1D,
                    LargeReturnAnomalyFlag,
                    LongGapOver30DaysFlag,
                    Close / NULLIF(
                        LAG(Close, 5) OVER (PARTITION BY ISIN ORDER BY Date), 0
                    ) - 1 AS Return5D
                FROM {daily_table}
                WHERE Date BETWEEN ? AND ?
            )
            SELECT *
            FROM history
            WHERE Date BETWEEN ? AND ?
            ORDER BY Date, ISIN
        """
        with self.connect() as connection:
            return connection.execute(
                query,
                [warmup_start, backtest_end, backtest_start, backtest_end],
            ).df()

    def load_monthly_universe(
        self,
        table: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        table = _safe_identifier(table)
        query = f"""
            SELECT *
            FROM {table}
            WHERE RebalanceDate BETWEEN ? AND ?
            ORDER BY RebalanceDate, LiquidityRank
        """
        with self.connect() as connection:
            return connection.execute(query, [start_date, end_date]).df()
