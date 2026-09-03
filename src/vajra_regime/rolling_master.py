from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from vajra_regime.config import AppConfig
from vajra_regime.corporate_actions import RECONCILIATION_TABLE
from vajra_regime.data_layout import DataLayout
from vajra_regime.nse_delivery import DELIVERY_TABLE
from vajra_regime.master_safety import ensure_mutable_master
from vajra_regime.nse_live import RAW_TABLE


LEGACY_TABLE = "clean_daily_legacy_2009_2025"
NEXT_TABLE = "clean_daily_next"
QUARANTINE_SESSIONS = 252
LIVE_START = "2026-01-01"
HISTORICAL_END = "2025-12-31"


def _table_exists(connection: duckdb.DuckDBPyConnection, table: str) -> bool:
    return table in {row[0] for row in connection.execute("SHOW TABLES").fetchall()}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _prepare_legacy_snapshot(connection: duckdb.DuckDBPyConnection, clean_table: str) -> None:
    if _table_exists(connection, LEGACY_TABLE):
        maximum = connection.execute(f"SELECT MAX(Date) FROM {LEGACY_TABLE}").fetchone()[0]
        if maximum is None or str(maximum) > HISTORICAL_END:
            raise ValueError(f"{LEGACY_TABLE} is not a valid 2009-2025 immutable snapshot.")
        return

    if not _table_exists(connection, clean_table):
        raise ValueError(f"Missing canonical historical table: {clean_table}")
    maximum = connection.execute(f"SELECT MAX(Date) FROM {clean_table}").fetchone()[0]
    if maximum is None:
        raise ValueError(f"Historical table {clean_table} is empty.")
    if str(maximum) > HISTORICAL_END:
        raise ValueError(
            "Refusing to create the immutable legacy snapshot from a table that already extends beyond 2025. "
            "Restore the protected historical database or the previously created legacy snapshot first."
        )
    connection.execute(
        f"CREATE TABLE {LEGACY_TABLE} AS SELECT * FROM {clean_table} WHERE Date <= DATE '{HISTORICAL_END}'"
    )


def _build_next_table(connection: duckdb.DuckDBPyConnection, clean_table: str) -> None:
    if not _table_exists(connection, RAW_TABLE):
        raise ValueError(f"Missing raw live table: {RAW_TABLE}. Run the NSE catch-up first.")
    if not _table_exists(connection, RECONCILIATION_TABLE):
        raise ValueError(
            f"Missing corporate-action reconciliation table: {RECONCILIATION_TABLE}. "
            "Run the corporate-action audit first."
        )

    # Delivery aur trade-count UDiFF bhavcopy me hote hi nahi -- wo alag file
    # (`sec_bhavdata_full`) se aate hain, jo `nse_delivery.py` laata hai. Wo
    # table abhi bhara na ho to purane bartaav par rehte hain (NULL); bhara ho
    # to live rows me bhi delivery aa jaati hai, jaisa 2009-2025 me pehle se hai.
    #
    # Join (Date, Symbol) par hai kyunki us file me ISIN hai hi nahi. Ye is
    # project ke "Symbol par kabhi join mat karo" niyam ka jaan-boojh kar liya
    # gaya apwaad hai: poore 17 saal me ek bhi (Date, Symbol) aisa nahi mila
    # jispar do ISIN hon (0 case). Symbol saalon me badalte hain, ek din ke
    # andar nahi.
    #
    # QuantityPerTrade wahi formula se banta hai jo legacy feed me hai --
    # Volume / TotalTrades, 2 dashamlav. Alag tarike se ginne par seam par ek
    # chup-chaap fark aa jaata, jo dikhta nahi aur galat hota.
    if _table_exists(connection, DELIVERY_TABLE):
        delivery_columns = (
            "CAST(d.TotalTrades AS DOUBLE) AS TotalTrades,"
            " ROUND(CAST(r.Volume AS DOUBLE)"
            " / NULLIF(CAST(d.TotalTrades AS DOUBLE), 0), 2) AS QuantityPerTrade,"
            " CAST(d.DeliveryQuantity AS DOUBLE) AS DeliveryQuantity"
        )
        delivery_join = (
            f"LEFT JOIN {DELIVERY_TABLE} d"
            " ON d.Date = r.Date AND d.Symbol = r.Symbol"
        )
    else:
        delivery_columns = (
            "NULL::DOUBLE AS TotalTrades,"
            " NULL::DOUBLE AS QuantityPerTrade,"
            " NULL::DOUBLE AS DeliveryQuantity"
        )
        delivery_join = ""

    # Legacy snapshot ab apni Series khud rakhta hai (EQ + surveillance BE/BZ).
    #
    # Pehle wo EQ-only banaya gaya tha aur yahan seedha 'EQ' likh diya jaata
    # tha. Ab wo snapshot dobara banaya ja chuka hai: 51,94,056 EQ rows bilkul
    # waisi ki waisi, aur 4,54,779 BE/BZ rows jodi gayin jo pehle gayab thin.
    #
    # Jaanch phir bhi rakhi hai. Agar kabhi purana snapshot restore hua to wo
    # bina Series ke aayega, aur us halat me 'EQ' likhna sach hi hai -- kyunki
    # us snapshot me BE rows thi hi nahi.
    legacy_columns = {
        row[0] for row in connection.execute(f"DESCRIBE {LEGACY_TABLE}").fetchall()
    } if _table_exists(connection, LEGACY_TABLE) else set()
    legacy_series = "Series" if "Series" in legacy_columns else "'EQ' AS Series"

    connection.execute(f"DROP TABLE IF EXISTS {NEXT_TABLE}")
    connection.execute(
        f"""
        CREATE TABLE {NEXT_TABLE} AS
        WITH historical_base AS (
            SELECT
                Date,
                Symbol,
                UPPER(ISIN) AS ISIN,
                CAST(Open AS DOUBLE) AS OpenRaw,
                CAST(High AS DOUBLE) AS HighRaw,
                CAST(Low AS DOUBLE) AS LowRaw,
                CAST(Close AS DOUBLE) AS CloseRaw,
                CAST(Volume AS DOUBLE) AS VolumeRaw,
                CAST(TotalTrades AS DOUBLE) AS TotalTrades,
                CAST(QuantityPerTrade AS DOUBLE) AS QuantityPerTrade,
                CAST(DeliveryQuantity AS DOUBLE) AS DeliveryQuantity,
                {legacy_series},
                -- EOD2's feed is already split- and bonus-adjusted. The factor
                -- below must not touch it; see the join condition in
                -- adjusted_base.
                FALSE AS IsLiveSource
            FROM {LEGACY_TABLE}
        ),
        live_base AS (
            SELECT
                r.Date,
                r.Symbol,
                UPPER(r.ISIN) AS ISIN,
                CAST(r.Open AS DOUBLE) AS OpenRaw,
                CAST(r.High AS DOUBLE) AS HighRaw,
                CAST(r.Low AS DOUBLE) AS LowRaw,
                CAST(r.Close AS DOUBLE) AS CloseRaw,
                CAST(r.Volume AS DOUBLE) AS VolumeRaw,
                {delivery_columns},
                r.Series,
                -- NSE's bhavcopy is as-traded. This is the half that genuinely
                -- needs the corporate-action factor applied.
                TRUE AS IsLiveSource
            FROM {RAW_TABLE} r
            {delivery_join}
            WHERE r.Date >= DATE '{LIVE_START}'
        ),
        base AS (
            SELECT * FROM historical_base
            UNION ALL
            SELECT * FROM live_base
        ),
        verified_adjustments AS (
            SELECT DISTINCT
                UPPER(ISIN) AS ISIN,
                CAST(ExDate AS DATE) AS ExDate,
                CAST(PriceFactorForPreExHistory AS DOUBLE) AS PriceFactor,
                CAST(VolumeFactorForPreExHistory AS DOUBLE) AS VolumeFactor,
                ActionType,
                Subject
            FROM {RECONCILIATION_TABLE}
            WHERE Decision = 'AUTO_READY_SPLIT_BONUS'
              AND ISIN IS NOT NULL
              AND TRIM(ISIN) <> ''
              AND PriceFactorForPreExHistory > 0
              AND VolumeFactorForPreExHistory > 0
        ),
        adjusted_base AS (
            SELECT
                b.Date,
                b.Symbol,
                b.ISIN,
                b.OpenRaw,
                b.HighRaw,
                b.LowRaw,
                b.CloseRaw,
                b.VolumeRaw,
                b.TotalTrades,
                b.QuantityPerTrade,
                b.DeliveryQuantity,
                b.Series,
                COALESCE(EXP(SUM(LN(a.PriceFactor))), 1.0) AS CorporateActionPriceFactor,
                COALESCE(EXP(SUM(LN(a.VolumeFactor))), 1.0) AS CorporateActionVolumeFactor
            FROM base b
            LEFT JOIN verified_adjustments a
              ON b.ISIN = a.ISIN
             AND b.Date < a.ExDate
             -- Only the live half. The legacy half comes from a feed that has
             -- already applied these ratios to its own history, so multiplying
             -- again adjusts twice.
             --
             -- Invisible until 2026-01-01 because every row used to come from
             -- that one feed: a uniform extra factor across a whole series
             -- changes no return and leaves no break. The moment a second,
             -- as-traded source began at LIVE_START, the two halves stopped
             -- agreeing and the seam appeared on the first session of the year,
             -- of size exactly 1/factor. Found on 2026-09-02 in fourteen
             -- securities at once, each matching its own pending-2026 bonus:
             -- ZFCVINDIA x5.94 (5:1), CUPID x5.07 (4:1), ECLERX x2.05 (1:1).
             -- RELIANCE and INFY, which have no 2026 action, were untouched.
             AND b.IsLiveSource
            GROUP BY ALL
        ),
        adjusted AS (
            SELECT
                Date,
                Symbol,
                ISIN,
                OpenRaw * CorporateActionPriceFactor AS Open,
                HighRaw * CorporateActionPriceFactor AS High,
                LowRaw * CorporateActionPriceFactor AS Low,
                CloseRaw * CorporateActionPriceFactor AS Close,
                CAST(ROUND(VolumeRaw * CorporateActionVolumeFactor) AS BIGINT) AS Volume,
                TotalTrades,
                QuantityPerTrade,
                DeliveryQuantity,
                Series,
                CorporateActionPriceFactor,
                CorporateActionVolumeFactor
            FROM adjusted_base
        ),
        base_metrics AS (
            SELECT
                *,
                Close * Volume AS Turnover,
                LAG(Close) OVER (PARTITION BY ISIN ORDER BY Date) AS PrevClose,
                LAG(Date) OVER (PARTITION BY ISIN ORDER BY Date) AS PrevDate,
                ROW_NUMBER() OVER (PARTITION BY ISIN ORDER BY Date) AS HistoryCount
            FROM adjusted
        ),
        rolling AS (
            SELECT
                *,
                Close / NULLIF(PrevClose, 0) - 1.0 AS Return1D,
                DATE_DIFF('day', PrevDate, Date) AS GapDays,
                MEDIAN(Turnover) OVER (
                    PARTITION BY ISIN ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS MedianTurnover60,
                COUNT(Turnover) OVER (
                    PARTITION BY ISIN ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS TurnoverObservations60
            FROM base_metrics
        ),
        review_events AS (
            SELECT DISTINCT
                EventId,
                UPPER(ISIN) AS ISIN,
                CAST(ExDate AS DATE) AS EventDate,
                ActionType,
                Subject,
                Decision
            FROM {RECONCILIATION_TABLE}
            WHERE Decision LIKE 'REVIEW%'
              AND MatchStatus = 'MATCHED_UNIQUE_ISIN'
              AND ISIN IS NOT NULL
              AND TRIM(ISIN) <> ''
              AND ActionType IN ('RIGHTS', 'MERGER', 'DEMERGER', 'SPLIT', 'BONUS')
        ),
        review_sessions AS (
            SELECT
                e.EventId,
                e.ISIN,
                e.EventDate,
                e.ActionType,
                e.Subject,
                r.Date,
                ROW_NUMBER() OVER (
                    PARTITION BY e.EventId ORDER BY r.Date
                ) AS SessionNumber
            FROM review_events e
            JOIN rolling r
              ON r.ISIN = e.ISIN
             AND r.Date >= e.EventDate
        ),
        relisting_events AS (
            SELECT
                'RELIST-' || ISIN || '-' || CAST(Date AS VARCHAR) AS EventId,
                ISIN,
                Date AS EventDate,
                'RELISTING_OR_LONG_GAP' AS ActionType,
                'Long gap with large return; automatic quarantine.' AS Subject
            FROM rolling
            WHERE Date >= DATE '{LIVE_START}'
              AND GapDays > 30
              AND ABS(Return1D) > 0.20
        ),
        relisting_sessions AS (
            SELECT
                e.EventId,
                e.ISIN,
                e.EventDate,
                e.ActionType,
                e.Subject,
                r.Date,
                ROW_NUMBER() OVER (
                    PARTITION BY e.EventId ORDER BY r.Date
                ) AS SessionNumber
            FROM relisting_events e
            JOIN rolling r
              ON r.ISIN = e.ISIN
             AND r.Date >= e.EventDate
        ),
        quarantine_rows AS (
            SELECT * FROM review_sessions WHERE SessionNumber <= {QUARANTINE_SESSIONS}
            UNION ALL
            SELECT * FROM relisting_sessions WHERE SessionNumber <= {QUARANTINE_SESSIONS}
        ),
        quarantine AS (
            SELECT
                Date,
                ISIN,
                TRUE AS CorporateActionQuarantineFlag,
                STRING_AGG(DISTINCT ActionType, ' | ' ORDER BY ActionType) AS CorporateActionQuarantineReason
            FROM quarantine_rows
            GROUP BY Date, ISIN
        )
        SELECT
            r.Date,
            EXTRACT(YEAR FROM r.Date)::INTEGER AS Year,
            r.Symbol,
            r.ISIN,
            r.Series,
            r.Open,
            r.High,
            r.Low,
            r.Close,
            r.Volume,
            r.TotalTrades,
            r.QuantityPerTrade,
            r.DeliveryQuantity,
            r.Turnover,
            r.PrevClose,
            r.Return1D,
            r.GapDays,
            r.HistoryCount,
            r.MedianTurnover60,
            r.TurnoverObservations60,
            CASE WHEN ABS(r.Return1D) > 0.20 THEN TRUE ELSE FALSE END AS LargeReturnAnomalyFlag,
            CASE WHEN r.GapDays > 30 THEN TRUE ELSE FALSE END AS LongGapOver30DaysFlag,
            CASE WHEN r.Date < DATE '2010-08-01' THEN TRUE ELSE FALSE END AS IsWarmupPeriod,
            CASE
                WHEN r.Date BETWEEN DATE '2010-08-01' AND DATE '{HISTORICAL_END}' THEN TRUE
                ELSE FALSE
            END AS IsBacktestPeriod,
            CASE WHEN r.Date >= DATE '{LIVE_START}' THEN TRUE ELSE FALSE END AS IsLiveOutOfSample,
            r.CorporateActionPriceFactor,
            r.CorporateActionVolumeFactor,
            COALESCE(q.CorporateActionQuarantineFlag, FALSE) AS CorporateActionQuarantineFlag,
            COALESCE(q.CorporateActionQuarantineReason, '') AS CorporateActionQuarantineReason,
            CASE
                WHEN r.Date >= DATE '2010-08-01'
                 AND NOT COALESCE(q.CorporateActionQuarantineFlag, FALSE)
                THEN TRUE ELSE FALSE
            END AS IsResearchEligible
        FROM rolling r
        LEFT JOIN quarantine q USING (Date, ISIN)
        ORDER BY r.Date, r.ISIN
        """
    )


def _validate_next_table(
    connection: duckdb.DuckDBPyConnection,
    expected_live_rows: int,
    expected_live_last_date: object,
) -> dict[str, object]:
    row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS Rows,
            COUNT(DISTINCT Date) AS Dates,
            COUNT(DISTINCT ISIN) AS ISINs,
            MIN(Date) AS FirstDate,
            MAX(Date) AS LastDate,
            SUM(CASE WHEN Date >= DATE '{LIVE_START}' THEN 1 ELSE 0 END) AS LiveRows,
            SUM(CASE WHEN CorporateActionQuarantineFlag THEN 1 ELSE 0 END) AS QuarantineRows,
            SUM(CASE WHEN CorporateActionPriceFactor <> 1.0 THEN 1 ELSE 0 END) AS AdjustedRows
        FROM {NEXT_TABLE}
        """
    ).fetchone()
    duplicate_groups = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT Date, ISIN, COUNT(*) AS n
                FROM {NEXT_TABLE}
                GROUP BY Date, ISIN
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    invalid_rows = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {NEXT_TABLE}
            WHERE Open <= 0 OR High <= 0 OR Low <= 0 OR Close <= 0 OR Volume < 0
               OR High < GREATEST(Open, Close, Low)
               OR Low > LEAST(Open, Close, High)
            """
        ).fetchone()[0]
    )
    live_rows = int(row[5] or 0)
    last_date = row[4]
    ok = (
        duplicate_groups == 0
        and invalid_rows == 0
        and live_rows == int(expected_live_rows)
        and str(last_date) == str(expected_live_last_date)
    )
    return {
        "ok": ok,
        "rows": int(row[0]),
        "dates": int(row[1]),
        "unique_isin": int(row[2]),
        "first_date": str(row[3]),
        "last_date": str(last_date),
        "live_rows": live_rows,
        "expected_live_rows": int(expected_live_rows),
        "duplicate_date_isin_groups": duplicate_groups,
        "invalid_ohlcv_rows": invalid_rows,
        "quarantine_rows": int(row[6] or 0),
        "corporate_adjusted_rows": int(row[7] or 0),
    }


def _swap_clean_table(connection: duckdb.DuckDBPyConnection, clean_table: str) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DROP TABLE {clean_table}")
        connection.execute(f"ALTER TABLE {NEXT_TABLE} RENAME TO {clean_table}")
        connection.execute(f"CREATE INDEX idx_clean_date ON {clean_table}(Date)")
        connection.execute(f"CREATE INDEX idx_clean_isin ON {clean_table}(ISIN)")
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _export_yearly_parquet(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    parquet_dir: Path,
) -> list[dict[str, object]]:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    first_year, last_year = connection.execute(
        f"SELECT MIN(Year), MAX(Year) FROM {table}"
    ).fetchone()
    rows: list[dict[str, object]] = []
    for year in range(int(first_year), int(last_year) + 1):
        destination = parquet_dir / f"EOD2_Clean_{year}.parquet"
        temporary = parquet_dir / f"EOD2_Clean_{year}.parquet.tmp"
        if temporary.exists():
            temporary.unlink()
        sql_path = str(temporary).replace("\\", "/").replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT * FROM {table}
                WHERE Year = {year}
                ORDER BY Date, ISIN
            ) TO '{sql_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        os.replace(temporary, destination)
        summary = connection.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT ISIN), MIN(Date), MAX(Date),
                   SUM(CASE WHEN IsLiveOutOfSample THEN 1 ELSE 0 END),
                   SUM(CASE WHEN CorporateActionQuarantineFlag THEN 1 ELSE 0 END)
            FROM {table}
            WHERE Year = {year}
            """
        ).fetchone()
        rows.append(
            {
                "Year": year,
                "Rows": int(summary[0]),
                "UniqueISIN": int(summary[1]),
                "FirstDate": str(summary[2]),
                "LastDate": str(summary[3]),
                "LiveOutOfSampleRows": int(summary[4] or 0),
                "QuarantineRows": int(summary[5] or 0),
                "FileName": destination.name,
                "FileSizeBytes": destination.stat().st_size,
                "Sha256": _sha256(destination),
            }
        )
    return rows


def rebuild_rolling_clean_data(config: AppConfig) -> dict[str, object]:
    """Build the permanent rolling adjusted OHLCV master without manual event editing.

    The published dataset is never touched here. Inside the
    rolling master, an immutable 2009-2025 snapshot is created once. Every run rebuilds the
    canonical clean table deterministically from that snapshot + raw 2026+ NSE rows + the
    latest corporate-action reconciliation, so split/bonus factors can never compound twice.
    """
    safety = ensure_mutable_master(config)
    layout = DataLayout.from_root(config.environment.root)
    clean_table = str(config.data["clean_table"])
    database = Path(config.environment.duckdb_path)

    with duckdb.connect(str(database), read_only=False) as connection:
        _prepare_legacy_snapshot(connection, clean_table)
        expected_live_rows, expected_live_last_date = connection.execute(
            f"SELECT COUNT(*), MAX(Date) FROM {RAW_TABLE} WHERE Date >= DATE '{LIVE_START}'"
        ).fetchone()
        if int(expected_live_rows) == 0 or expected_live_last_date is None:
            raise ValueError("No 2026+ raw NSE rows are available for rolling-master rebuild.")

        _build_next_table(connection, clean_table)
        validation = _validate_next_table(
            connection,
            expected_live_rows=int(expected_live_rows),
            expected_live_last_date=expected_live_last_date,
        )
        if not validation["ok"]:
            connection.execute(f"DROP TABLE IF EXISTS {NEXT_TABLE}")
            raise ValueError(f"Rolling clean validation failed: {validation}")

        adjustment_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {RECONCILIATION_TABLE} WHERE Decision = 'AUTO_READY_SPLIT_BONUS'"
            ).fetchone()[0]
        )
        complex_review_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM {RECONCILIATION_TABLE}
                WHERE Decision LIKE 'REVIEW%'
                  AND ActionType IN ('RIGHTS', 'MERGER', 'DEMERGER', 'SPLIT', 'BONUS')
                """
            ).fetchone()[0]
        )
        _swap_clean_table(connection, clean_table)

        parquet_dir = Path(config.environment.master_data_root) / "01 Daily Clean Parquet By Year"
        yearly = _export_yearly_parquet(connection, clean_table, parquet_dir)
        final_row = connection.execute(
            f"SELECT COUNT(*), MIN(Date), MAX(Date) FROM {clean_table}"
        ).fetchone()

    quality_dir = Path(config.environment.master_data_root) / "03 Quality Reports"
    quality_dir.mkdir(parents=True, exist_ok=True)
    manifest_csv = quality_dir / "08_Rolling_Adjusted_Yearly_Parquet_Manifest.csv"
    pd.DataFrame(yearly).to_csv(manifest_csv, index=False)

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "database": str(database),
        "canonical_table": clean_table,
        "immutable_historical_snapshot": LEGACY_TABLE,
        "published_data_unchanged": True,
        "master_safety": safety,
        "verified_split_bonus_events_applied": adjustment_count,
        "complex_review_events_auto_quarantined_or_logged": complex_review_count,
        "quarantine_sessions": QUARANTINE_SESSIONS,
        "manual_corporate_action_edit_required": False,
        "historical_backtest_flag_ends": HISTORICAL_END,
        "live_out_of_sample_starts": LIVE_START,
        "validation": validation,
        "final_rows": int(final_row[0]),
        "final_first_date": str(final_row[1]),
        "final_last_date": str(final_row[2]),
        "yearly_parquet_manifest": str(manifest_csv),
        "yearly_files": yearly,
        "note": (
            "Verified split/bonus factors are back-adjusted across all pre-ex history. "
            "High-risk rights/merger/demerger or unparsed split/bonus cases are not guessed; "
            "matched securities are automatically quarantined for 252 observations. "
            "2026+ remains flagged as live out-of-sample."
        ),
    }
    summary_path = quality_dir / "09_Rolling_Adjusted_Master_Summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary
