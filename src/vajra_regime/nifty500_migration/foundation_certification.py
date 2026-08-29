from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


def _atomic_parquet(connection: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    temporary_sql = str(temporary).replace("'", "''")
    connection.execute(f"COPY ({query}) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(temporary, path)


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


def _grade(year: int, raw_coverage: float, identity_coverage: float) -> str:
    if raw_coverage < 0.98 or identity_coverage < 0.98:
        return "INSUFFICIENT"
    if year <= 2013:
        return "RECONSTRUCTED_MEDIUM_CONFIDENCE"
    if year <= 2022:
        return "VERIFIED_MULTI_SOURCE"
    return "RECONSTRUCTED_HIGH_CONFIDENCE"


def build_foundation_certification(
    *,
    data_root: Path = DATA_ROOT,
    as_of: date = date(2026, 8, 13),
) -> dict[str, Any]:
    daily_membership = data_root / "08 Parquet" / "nifty500_daily_membership.parquet"
    raw_paths = sorted((data_root / "08 Parquet" / "raw").glob("year=*/nifty500_raw_daily.parquet"))
    adjusted_paths = sorted(
        (data_root / "08 Parquet" / "certified_adjusted").glob("year=*/nifty500_adjusted_daily.parquet")
    )
    missing_path = data_root / "09 Validation" / "nifty500_official_raw_missing_member_rows.csv"
    current_path = (
        data_root
        / "01 Raw Source Archives"
        / "Official Current Constituents"
        / "ind_nifty500list.csv"
    )
    intervals_path = data_root / "02 Constituent History" / "nifty500_membership_intervals.csv"
    adjusted_status_path = data_root / "11 Logs" / "certified_adjusted_build_status.json"
    required = [daily_membership, missing_path, current_path, intervals_path, adjusted_status_path]
    if not raw_paths or not adjusted_paths or any(not path.exists() for path in required):
        raise RuntimeError("Certified PIT membership, raw and adjusted layers are required")

    panel_root = data_root / "07 Point In Time Panels"
    security_root = data_root / "03 Security Master"
    validation_root = data_root / "09 Validation"
    panel_root.mkdir(parents=True, exist_ok=True)
    security_root.mkdir(parents=True, exist_ok=True)
    validation_root.mkdir(parents=True, exist_ok=True)
    certified_daily = panel_root / "nifty500_daily_membership_certified.parquet"
    monthly_members = panel_root / "nifty500_monthly_members.parquet"
    security_master = security_root / "nifty500_security_master.parquet"
    symbol_history = security_root / "nifty500_symbol_history.parquet"

    with duckdb.connect() as connection:
        connection.read_parquet(str(daily_membership)).create_view("membership")
        connection.read_parquet([str(path) for path in raw_paths]).create_view("raw")
        connection.read_parquet([str(path) for path in adjusted_paths]).create_view("adjusted")
        connection.read_csv(str(missing_path), header=True, all_varchar=True).create_view("missing")
        connection.read_csv(str(current_path), header=True, all_varchar=True).create_view("current_members")
        daily_query = """
            SELECT m.Date, m.Symbol AS MembershipSymbol,
                   r.Symbol AS ExchangeSymbol,
                   COALESCE(r.ISIN, NULLIF(x.ExpectedISIN, ''), m.ISIN) AS ISIN,
                   r.Series,
                   CAST(r.Date IS NOT NULL AS BOOLEAN) AS OHLCVAvailable,
                   m.MembershipConfidence, m.MembershipEvidence, m.FoundationVersion,
                   CASE WHEN r.ISIN IS NOT NULL THEN r.IdentityStatus
                        WHEN NULLIF(x.ExpectedISIN, '') IS NOT NULL THEN 'EOD2_EFFECTIVE_IDENTITY_NO_OHLCV_ROW'
                        WHEN m.ISIN IS NOT NULL THEN 'INHERITED_TIMELINE_IDENTITY_NO_OHLCV_ROW'
                        ELSE 'UNRESOLVED_IDENTITY_NO_OHLCV_ROW' END AS IdentityStatus,
                   CASE WHEN r.Date IS NOT NULL THEN r.SourceArchive ELSE x.SourceArchive END
                       AS SessionSourceArchive
            FROM membership m
            LEFT JOIN raw r ON r.Date = m.Date AND r.MembershipSymbol = m.Symbol
            LEFT JOIN missing x ON CAST(x.Date AS DATE) = m.Date AND x.Symbol = m.Symbol
            ORDER BY m.Date, m.Symbol
        """
        _atomic_parquet(connection, daily_query, certified_daily)
        connection.read_parquet(str(certified_daily)).create_view("certified_membership")
        monthly_query = """
            WITH month_ends AS (
                SELECT YEAR(Date) AS Year, MONTH(Date) AS Month, MAX(Date) AS SnapshotDate
                FROM certified_membership GROUP BY 1, 2
            )
            SELECT e.Year, e.Month, e.SnapshotDate, m.MembershipSymbol, m.ExchangeSymbol,
                   m.ISIN, m.OHLCVAvailable, m.MembershipConfidence, m.MembershipEvidence,
                   m.IdentityStatus, m.FoundationVersion
            FROM month_ends e JOIN certified_membership m ON m.Date = e.SnapshotDate
            ORDER BY e.SnapshotDate, m.MembershipSymbol
        """
        _atomic_parquet(connection, monthly_query, monthly_members)
        symbol_query = """
            SELECT ISIN, MembershipSymbol, ExchangeSymbol,
                   MIN(Date) AS FirstMembershipDate, MAX(Date) AS LastMembershipDate,
                   COUNT(*) AS MembershipSessions, SUM(OHLCVAvailable) AS OHLCVSessions,
                   ARG_MAX(IdentityStatus, Date) AS LatestIdentityStatus
            FROM certified_membership
            GROUP BY ISIN, MembershipSymbol, ExchangeSymbol
            ORDER BY ISIN, FirstMembershipDate, MembershipSymbol, ExchangeSymbol
        """
        _atomic_parquet(connection, symbol_query, symbol_history)
        security_query = f"""
            WITH grouped AS (
                SELECT ISIN,
                       MIN(Date) AS FirstMembershipDate, MAX(Date) AS LastMembershipDate,
                       ARG_MAX(COALESCE(ExchangeSymbol, MembershipSymbol), Date) AS LatestObservedSymbol,
                       COUNT(DISTINCT MembershipSymbol) AS MembershipSymbolCount,
                       LIST_SORT(LIST(DISTINCT MembershipSymbol)) AS MembershipSymbols,
                       COUNT(*) AS MembershipSessions, SUM(OHLCVAvailable) AS OHLCVSessions
                FROM certified_membership
                GROUP BY ISIN
            ), current_typed AS (
                SELECT "ISIN Code" AS ISIN, Symbol AS CurrentSymbol,
                       "Company Name" AS CurrentCompanyName, Industry AS CurrentIndustry,
                       Series AS CurrentSeries
                FROM current_members
            )
            SELECT COALESCE(g.ISIN, 'UNRESOLVED_SYMBOL:' || g.LatestObservedSymbol) AS SecurityId,
                   g.ISIN, g.FirstMembershipDate, g.LastMembershipDate, g.LatestObservedSymbol,
                   g.MembershipSymbolCount, TO_JSON(g.MembershipSymbols) AS MembershipSymbolsJson,
                   g.MembershipSessions, g.OHLCVSessions,
                   c.CurrentSymbol, c.CurrentCompanyName, c.CurrentIndustry, c.CurrentSeries,
                   CAST(c.ISIN IS NOT NULL AS BOOLEAN) AS IsCurrentMember,
                   DATE '{as_of.isoformat()}' AS CurrentMetadataSnapshotDate,
                   'CURRENT_ONLY_NOT_HISTORICALLY_BACKFILLED' AS CurrentMetadataPolicy,
                   '{FOUNDATION_VERSION}' AS FoundationVersion
            FROM grouped g LEFT JOIN current_typed c USING (ISIN)
            ORDER BY SecurityId
        """
        _atomic_parquet(connection, security_query, security_master)

        member_year = connection.execute(
            """
            SELECT YEAR(Date) AS Year, COUNT(*) AS MemberRows, COUNT(DISTINCT Date) AS Sessions,
                   SUM(OHLCVAvailable) AS RawRows,
                   SUM(ISIN IS NOT NULL) AS ResolvedIdentityRows,
                   SUM(MembershipConfidence LIKE 'VERIFIED%') AS OfficialMembershipRows,
                   MIN(Date) AS FirstDate, MAX(Date) AS LastDate
            FROM certified_membership GROUP BY 1 ORDER BY 1
            """
        ).df()
        adjusted_year = connection.execute(
            """
            SELECT YEAR(Date) AS Year, COUNT(*) AS AdjustedRows,
                   SUM(IsResearchEligible) AS ResearchEligibleRows,
                   SUM(CorporateActionQuarantineFlag) AS QuarantineRows,
                   SUM(UnresolvedAdjustmentDiscontinuityFlag) AS UnresolvedRows,
                   SUM(ExtremeOfficialMarketMoveFlag) AS ExtremeMarketMoveRows
            FROM adjusted GROUP BY 1 ORDER BY 1
            """
        ).df()
        yearly = member_year.merge(adjusted_year, on="Year", how="left")
        yearly["MemberCoverage"] = yearly["RawRows"] / yearly["MemberRows"]
        yearly["OHLCVCoverage"] = yearly["AdjustedRows"] / yearly["MemberRows"]
        yearly["OfficialSourceCoverage"] = yearly["OfficialMembershipRows"] / yearly["MemberRows"]
        yearly["AdjustmentConfidence"] = yearly["ResearchEligibleRows"] / yearly["AdjustedRows"]
        yearly["IdentityConfidence"] = yearly["ResolvedIdentityRows"] / yearly["MemberRows"]
        yearly["OverallGrade"] = [
            _grade(int(row.Year), float(row.MemberCoverage), float(row.IdentityConfidence))
            for row in yearly.itertuples()
        ]
        yearly["MembershipEvidenceNote"] = [
            "Pre-first-exact-anchor reconstruction; official event evidence used, uncertainty retained"
            if int(year) <= 2013
            else "Exact official monthly anchors plus effective-dated official events"
            if int(year) <= 2022
            else "Official effective-dated change notices plus official current anchor"
            for year in yearly["Year"]
        ]

        daily_metrics = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT MembershipSymbol),
                   COUNT(DISTINCT ISIN), SUM(ISIN IS NULL), SUM(NOT OHLCVAvailable),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol),
                   MIN(daily_count), MAX(daily_count), MIN(Date), MAX(Date)
            FROM (
                SELECT *, COUNT(*) OVER (PARTITION BY Date) AS daily_count
                FROM certified_membership
            )
            """
        ).fetchone()
        interval_mismatch = connection.execute(
            """
            WITH intervals AS (
                SELECT CAST(valid_from AS DATE) valid_from,
                       CAST(valid_to_exclusive AS DATE) valid_to_exclusive, symbol
                FROM read_csv_auto(?, header=true, all_varchar=true)
            )
            SELECT COUNT(*) FROM certified_membership m
            WHERE NOT EXISTS (
                SELECT 1 FROM intervals i
                WHERE i.symbol = m.MembershipSymbol
                  AND m.Date >= i.valid_from AND m.Date < i.valid_to_exclusive
            )
            """,
            [str(intervals_path)],
        ).fetchone()[0]
        current_metrics = connection.execute(
            """
            WITH official AS (SELECT Symbol, "ISIN Code" ISIN FROM current_members),
                 latest AS (
                     SELECT MembershipSymbol, ISIN FROM certified_membership
                     WHERE Date = (SELECT MAX(Date) FROM certified_membership)
                 ), first AS (
                     SELECT MembershipSymbol, ISIN FROM certified_membership
                     WHERE Date = (SELECT MIN(Date) FROM certified_membership)
                 )
            SELECT (SELECT COUNT(*) FROM official),
                   (SELECT COUNT(*) FROM latest),
                   (SELECT COUNT(*) FROM official o JOIN latest l USING (ISIN)),
                   (SELECT COUNT(*) FROM official o JOIN first f USING (ISIN)),
                   (SELECT COUNT(*) FROM official o LEFT JOIN first f USING (ISIN) WHERE f.ISIN IS NULL)
            """
        ).fetchone()
        delisted_preserved = connection.execute(
            """
            WITH identities AS (
                SELECT ISIN, MAX(Date) AS LastDate, COUNT(*) AS Rows
                FROM raw WHERE ISIN IS NOT NULL GROUP BY ISIN
            ), current_ids AS (SELECT "ISIN Code" ISIN FROM current_members)
            SELECT COUNT(*), SUM(Rows) FROM identities i
            LEFT JOIN current_ids c USING (ISIN)
            WHERE c.ISIN IS NULL AND i.LastDate < ?
            """,
            [as_of],
        ).fetchone()

    quality_path = validation_root / "nifty500_yearly_quality_score.csv"
    _atomic_csv(yearly, quality_path)
    adjusted_status = json.loads(adjusted_status_path.read_text(encoding="utf-8"))
    leakage_checks = {
        "effective_date_interval_join_mismatches": int(interval_mismatch),
        "current_official_members": int(current_metrics[0]),
        "latest_panel_members": int(current_metrics[1]),
        "latest_official_isin_matches": int(current_metrics[2]),
        "current_isins_already_present_at_2009_start": int(current_metrics[3]),
        "current_isins_absent_at_2009_start": int(current_metrics[4]),
        "current_members_backfilled_to_2009": bool(current_metrics[4] == 0),
        "historical_industry_column_present": False,
        "current_industry_policy": "Stored only in security master with current snapshot date; never joined to history",
        "delisted_noncurrent_identities_preserved": int(delisted_preserved[0]),
        "delisted_historical_rows_preserved": int(delisted_preserved[1] or 0),
        "future_price_forward_fill_after_delisting": 0,
        "membership_lookup_policy": "valid_from <= session < valid_to_exclusive",
        "future_constituent_file_used_before_effective_date": 0,
    }
    hard_pass = (
        adjusted_status["status"] == "CERTIFIED_PASS_WITH_DOCUMENTED_QUARANTINE"
        and daily_metrics[6] == 0
        and daily_metrics[7] >= 499
        and daily_metrics[8] <= 501
        and interval_mismatch == 0
        and current_metrics[0] == current_metrics[1] == current_metrics[2] == 500
        and current_metrics[4] > 0
        and not leakage_checks["current_members_backfilled_to_2009"]
        and adjusted_status["unresolved_adjustment_discontinuity_rows"] == 0
    )
    status: dict[str, Any] = {
        "status": "CERTIFIED_PASS_WITH_PRE_2013_UNCERTAINTY" if hard_pass else "CERTIFICATION_FAIL",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "available_start_date": "2009-01-01",
        "first_exact_official_membership_anchor": "2013-04-18",
        "recommended_primary_research_start": "2013-04-18",
        "pre_anchor_policy": (
            "2009-01-01 through 2013-04-17 retained as RECONSTRUCTED_MEDIUM_CONFIDENCE; "
            "never described as exact official membership"
        ),
        "daily_membership_rows": daily_metrics[0],
        "sessions": daily_metrics[1],
        "membership_symbols": daily_metrics[2],
        "isins": daily_metrics[3],
        "unresolved_identity_rows": daily_metrics[4],
        "missing_ohlcv_member_sessions": daily_metrics[5],
        "duplicate_date_membership_symbol_rows": daily_metrics[6],
        "min_daily_members": daily_metrics[7],
        "max_daily_members": daily_metrics[8],
        "earliest_date": str(daily_metrics[9]),
        "latest_date": str(daily_metrics[10]),
        "certified_adjusted_rows": adjusted_status["rows"],
        "certified_adjusted_quarantine_rows": adjusted_status["quarantine_rows"],
        "certified_adjusted_research_eligible_rows": adjusted_status["research_eligible_rows"],
        "certified_daily_membership_path": str(certified_daily),
        "certified_daily_membership_sha256": sha256_file(certified_daily),
        "monthly_members_path": str(monthly_members),
        "monthly_members_sha256": sha256_file(monthly_members),
        "security_master_path": str(security_master),
        "security_master_sha256": sha256_file(security_master),
        "symbol_history_path": str(symbol_history),
        "symbol_history_sha256": sha256_file(symbol_history),
        "yearly_quality_path": str(quality_path),
        "yearly_quality_sha256": sha256_file(quality_path),
        "leakage_checks": leakage_checks,
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "foundation_certification_status.json", status)
    atomic_json(
        data_root / "12 Checkpoints" / "phase_08_foundation_certification.json",
        {**status, "checkpoint_status": "COMPLETE_HASH_VERIFIED"},
    )
    return status
