from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


def _paths(data_root: Path, layer: str) -> list[Path]:
    return sorted((data_root / "08 Parquet" / layer).glob("year=*/nifty500_*_daily.parquet"))


def _build_relisting_intervals(data_root: Path, raw_paths: list[Path]) -> tuple[Path, dict[str, Any]]:
    output = data_root / "04 Corporate Actions" / "nifty500_relisting_long_gap_quarantine.parquet"
    temporary = output.with_name(f".{output.name}.{uuid4().hex}.partial")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                WITH sequenced AS (
                    SELECT Date, ISIN, Symbol, Close AS RawClose,
                           LAG(Date) OVER (PARTITION BY COALESCE(ISIN, 'SYMBOL:' || Symbol) ORDER BY Date)
                               AS PriorDate,
                           LAG(Close) OVER (
                               PARTITION BY COALESCE(ISIN, 'SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorRawClose,
                           LEAD(Date, 251) OVER (
                               PARTITION BY COALESCE(ISIN, 'SYMBOL:' || Symbol) ORDER BY Date
                           ) AS Session252Date,
                           MAX(Date) OVER (
                               PARTITION BY COALESCE(ISIN, 'SYMBOL:' || Symbol)
                           ) AS LastObservedDate
                    FROM read_parquet(?)
                )
                SELECT SHA256(
                           COALESCE(ISIN, 'SYMBOL:' || Symbol) || '|' || CAST(Date AS VARCHAR)
                       ) AS EventId,
                       Date AS EventDate, ISIN, Symbol,
                       PriorDate, DATE_DIFF('day', PriorDate, Date) AS GapDays,
                       PriorRawClose, RawClose,
                       RawClose / NULLIF(PriorRawClose, 0) - 1.0 AS RawReturnAcrossGap,
                       COALESCE(Session252Date, LastObservedDate) AS QuarantineEndDate,
                       'RELISTING_OR_LONG_GAP' AS Reason,
                       '252_VALID_OBSERVATIONS_FROM_RETURN_DATE' AS Policy
                FROM sequenced
                WHERE DATE_DIFF('day', PriorDate, Date) > 30
                  AND ABS(RawClose / NULLIF(PriorRawClose, 0) - 1.0) > 0.20
                ORDER BY EventDate, ISIN, Symbol
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [[str(path) for path in raw_paths]],
        )
        metrics = connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT ISIN), MIN(EventDate), MAX(EventDate) FROM read_parquet(?)",
            [str(temporary)],
        ).fetchone()
    os.replace(temporary, output)
    status = {
        "events": metrics[0],
        "isins": metrics[1],
        "earliest": str(metrics[2]),
        "latest": str(metrics[3]),
        "path": str(output),
        "sha256": sha256_file(output),
    }
    return output, status


def _build_year(
    *,
    raw_path: Path,
    prior_raw_path: Path | None,
    eod2_path: Path,
    reconciliation_path: Path,
    relisting_path: Path,
    output_path: Path,
    year: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid4().hex}.partial")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                WITH raw_rows AS (
                    SELECT * FROM read_parquet(?)
                    UNION ALL BY NAME
                    SELECT * FROM read_parquet(?)
                    WHERE Date >= MAKE_DATE(?, 1, 1) - INTERVAL 15 DAY
                      AND Date < MAKE_DATE(?, 1, 1)
                ), eod2_all AS (
                    SELECT *, MAX(Date) OVER (PARTITION BY SourceFile) AS SourceLastDate
                    FROM read_parquet(?)
                ), eod2_rows AS (
                    SELECT * FROM eod2_all
                    WHERE YEAR(Date) IN (?, ?)
                ), raw_aliases AS (
                    SELECT Date, MembershipSymbol, Symbol AS RawSymbol,
                           'SYMBOL' AS AliasType, Symbol AS AliasValue
                    FROM raw_rows
                    UNION ALL
                    SELECT Date, MembershipSymbol, Symbol AS RawSymbol,
                           'ISIN' AS AliasType, ISIN AS AliasValue
                    FROM raw_rows WHERE ISIN IS NOT NULL
                ), eod2_aliases AS (
                    SELECT e.*, 'SYMBOL' AS AliasType, e.Symbol AS AliasValue
                    FROM eod2_rows e
                    UNION ALL
                    SELECT e.*, 'SYMBOL' AS AliasType, alias.AliasValue
                    FROM eod2_rows e,
                         UNNEST(string_split(e.HistoricalSymbols, '|')) AS alias(AliasValue)
                    WHERE alias.AliasValue <> e.Symbol
                    UNION ALL
                    SELECT e.*, 'ISIN' AS AliasType, alias.AliasValue
                    FROM eod2_rows e,
                         UNNEST(string_split(e.IdentityISINs, '|')) AS alias(AliasValue)
                    WHERE alias.AliasValue <> ''
                ), eod2_matches AS (
                    SELECT r.Date, r.MembershipSymbol,
                           e.Symbol AS Eod2Symbol, e.Open AS Eod2Open, e.High AS Eod2High,
                           e.Low AS Eod2Low, e.Close AS Eod2Close, e.Volume AS Eod2Volume,
                           e.SourceFile AS Eod2SourceFile, e.SourceSha256 AS Eod2SourceSha256,
                           e.IdentityMapSha256 AS Eod2IdentityMapSha256,
                           e.SourceLastDate AS Eod2SourceLastDate,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.Date, r.MembershipSymbol
                               ORDER BY CASE WHEN e.Symbol = r.RawSymbol THEN 0
                                             WHEN e.AliasType = 'ISIN' THEN 1 ELSE 2 END,
                                        e.Symbol
                           ) AS MatchChoice
                    FROM raw_aliases r
                    JOIN eod2_aliases e
                      ON e.Date = r.Date AND e.AliasType = r.AliasType AND e.AliasValue = r.AliasValue
                    QUALIFY MatchChoice = 1
                ), base AS (
                    SELECT r.*,
                           e.Eod2Symbol, e.Eod2SourceFile, e.Eod2SourceSha256,
                           e.Eod2IdentityMapSha256, e.Eod2SourceLastDate,
                           COALESCE(e.Eod2Open, r.Open) AS BaseOpen,
                           COALESCE(e.Eod2High, r.High) AS BaseHigh,
                           COALESCE(e.Eod2Low, r.Low) AS BaseLow,
                           COALESCE(e.Eod2Close, r.Close) AS BaseClose,
                           COALESCE(e.Eod2Volume, r.Volume) AS BaseVolume,
                           CASE WHEN e.Eod2Close IS NULL THEN 'OFFICIAL_NSE_RAW'
                                WHEN e.Eod2Symbol = r.Symbol THEN 'EOD2_DIRECT_SECONDARY_ADJUSTED'
                                WHEN r.ISIN IS NOT NULL THEN 'EOD2_ISIN_ALIAS_SECONDARY_ADJUSTED'
                                ELSE 'EOD2_SYMBOL_HISTORY_ALIAS_SECONDARY_ADJUSTED' END AS BaseSource
                    FROM raw_rows r
                    LEFT JOIN eod2_matches e
                      ON e.Date = r.Date AND e.MembershipSymbol = r.MembershipSymbol
                ), with_mechanical_adjustments AS (
                    SELECT b.*,
                           COALESCE(EXP(SUM(LN(a.PriceFactor))), 1.0) AS PostSourcePriceFactor,
                           COALESCE(EXP(SUM(LN(a.VolumeFactor))), 1.0) AS PostSourceVolumeFactor,
                           STRING_AGG(a.EventId, '|' ORDER BY a.ExDate, a.EventId) AS AppliedOfficialCAEventIds
                    FROM base b
                    LEFT JOIN read_parquet(?) a
                      ON a.ISIN = b.ISIN
                     AND a.Decision LIKE 'AUTO_READY_%'
                     AND a.ExDate > b.Date
                     AND (b.Eod2SourceLastDate IS NULL OR a.ExDate > b.Eod2SourceLastDate)
                    GROUP BY ALL
                ), adjusted AS (
                    SELECT *,
                           BaseOpen * PostSourcePriceFactor AS AdjustedOpen,
                           BaseHigh * PostSourcePriceFactor AS AdjustedHigh,
                           BaseLow * PostSourcePriceFactor AS AdjustedLow,
                           BaseClose * PostSourcePriceFactor AS AdjustedClose,
                           CAST(ROUND(BaseVolume * PostSourceVolumeFactor) AS BIGINT) AS AdjustedVolume
                    FROM with_mechanical_adjustments
                ), quarantined AS (
                    SELECT a.*,
                           BOOL_OR(q.EventId IS NOT NULL) AS OfficialCAQuarantine,
                           STRING_AGG(
                               DISTINCT CASE WHEN q.EventId IS NOT NULL
                                   THEN q.ActionType || ':' || q.Decision ELSE NULL END,
                               '|' ORDER BY CASE WHEN q.EventId IS NOT NULL
                                   THEN q.ActionType || ':' || q.Decision ELSE NULL END
                           ) AS OfficialCAQuarantineReason,
                           BOOL_OR(g.EventId IS NOT NULL) AS RelistingQuarantine,
                           STRING_AGG(
                               DISTINCT CASE WHEN g.EventId IS NOT NULL THEN g.Reason ELSE NULL END,
                               '|' ORDER BY CASE WHEN g.EventId IS NOT NULL THEN g.Reason ELSE NULL END
                           ) AS RelistingQuarantineReason
                    FROM adjusted a
                    LEFT JOIN read_parquet(?) q
                      ON q.ISIN = a.ISIN AND q.Decision LIKE 'REVIEW_%'
                     AND a.Date BETWEEN q.ExDate AND COALESCE(q.QuarantineEndDate, DATE '9999-12-31')
                    LEFT JOIN read_parquet(?) g
                      ON (g.ISIN = a.ISIN OR (g.ISIN IS NULL AND g.Symbol = a.Symbol))
                     AND a.Date BETWEEN g.EventDate AND g.QuarantineEndDate
                    GROUP BY ALL
                ), lagged AS (
                    SELECT *,
                           LAG(AdjustedClose) OVER (
                               PARTITION BY COALESCE(ISIN, 'UNRESOLVED_SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorAdjustedClose,
                           LAG(Close) OVER (
                               PARTITION BY COALESCE(ISIN, 'UNRESOLVED_SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorRawClose,
                           LAG(Date) OVER (
                               PARTITION BY COALESCE(ISIN, 'UNRESOLVED_SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorDate
                    FROM quarantined
                ), classified AS (
                    SELECT *,
                           AdjustedClose / NULLIF(PriorAdjustedClose, 0) - 1.0 AS AdjustedReturn1D,
                           Close / NULLIF(PriorRawClose, 0) - 1.0 AS RawReturn1D,
                           DATE_DIFF('day', PriorDate, Date) AS GapDays,
                           CASE
                               WHEN PriorAdjustedClose IS NULL OR DATE_DIFF('day', PriorDate, Date) > 10
                                   THEN 'NOT_TESTED_FIRST_ROW_OR_LONG_GAP'
                               WHEN ABS(AdjustedClose / PriorAdjustedClose - 1.0) <= 0.45 THEN 'NONE'
                               WHEN ABS(
                                   (AdjustedClose / PriorAdjustedClose - 1.0)
                                   - (Close / NULLIF(PriorRawClose, 0) - 1.0)
                               ) <= 0.05 THEN 'OFFICIAL_RAW_EXTREME_MARKET_MOVE'
                               WHEN OfficialCAQuarantine OR RelistingQuarantine
                                   THEN 'COMPLEX_CA_OR_RELISTING_QUARANTINED'
                               ELSE 'UNRESOLVED_ADJUSTMENT_DISCONTINUITY'
                           END AS DiscontinuityClassification
                    FROM lagged
                )
                SELECT Date, Symbol, MembershipSymbol, ISIN, ExchangeISIN, Series,
                       AdjustedOpen AS Open, AdjustedHigh AS High, AdjustedLow AS Low,
                       AdjustedClose AS Close, AdjustedVolume AS Volume,
                       Open AS RawOpen, High AS RawHigh, Low AS RawLow, Close AS RawClose,
                       Volume AS RawVolume, PrevClose AS RawPrevClose, Turnover AS RawTurnover,
                       TotalTrades AS RawTotalTrades,
                       AdjustedClose / NULLIF(Close, 0) AS PriceAdjustmentFactor,
                       CAST(AdjustedVolume AS DOUBLE) / NULLIF(Volume, 0) AS VolumeAdjustmentFactor,
                       CASE WHEN BaseSource = 'OFFICIAL_NSE_RAW' AND PostSourcePriceFactor <> 1.0
                                THEN 'OFFICIAL_NSE_RAW_PLUS_VERIFIED_MECHANICAL_CA'
                            WHEN BaseSource LIKE 'EOD2_%' AND PostSourcePriceFactor <> 1.0
                                THEN BaseSource || '_PLUS_POST_SOURCE_OFFICIAL_CA'
                            WHEN BaseSource = 'OFFICIAL_NSE_RAW'
                                THEN 'OFFICIAL_NSE_RAW_NO_MECHANICAL_ADJUSTMENT_REQUIRED'
                            ELSE BaseSource END AS AdjustmentSource,
                       CASE WHEN BaseSource = 'OFFICIAL_NSE_RAW'
                                THEN 'A_OFFICIAL_RAW_WITH_OFFICIAL_CA_AUDIT'
                            ELSE 'B_NSE_DERIVED_ADJUSTED_CROSSCHECKED_TO_OFFICIAL_RAW' END
                            AS AdjustmentConfidence,
                       COALESCE(Eod2SourceFile, SourceArchive) AS AdjustmentSourceFile,
                       COALESCE(Eod2SourceSha256, SourceSha256) AS AdjustmentSourceSha256,
                       Eod2IdentityMapSha256 AS AdjustmentIdentityMapSha256,
                       AppliedOfficialCAEventIds,
                       SourceArchive AS RawSourceArchive, SourceSha256 AS RawSourceSha256,
                       MembershipConfidence, MembershipEvidence, FoundationVersion, IdentityStatus,
                       OfficialCAQuarantine, OfficialCAQuarantineReason,
                       RelistingQuarantine, RelistingQuarantineReason,
                       CAST(OfficialCAQuarantine OR RelistingQuarantine AS BOOLEAN)
                           AS CorporateActionQuarantineFlag,
                       CONCAT_WS('|', OfficialCAQuarantineReason, RelistingQuarantineReason)
                           AS CorporateActionQuarantineReason,
                       AdjustedReturn1D, RawReturn1D, GapDays, DiscontinuityClassification,
                       CAST(DiscontinuityClassification = 'UNRESOLVED_ADJUSTMENT_DISCONTINUITY' AS BOOLEAN)
                           AS UnresolvedAdjustmentDiscontinuityFlag,
                       CAST(DiscontinuityClassification = 'OFFICIAL_RAW_EXTREME_MARKET_MOVE' AS BOOLEAN)
                           AS ExtremeOfficialMarketMoveFlag,
                       CAST(
                           AdjustedOpen > 0
                           AND AdjustedHigh >= GREATEST(AdjustedOpen, AdjustedClose)
                           AND AdjustedLow <= LEAST(AdjustedOpen, AdjustedClose)
                           AND AdjustedVolume >= 0
                           AND NOT OfficialCAQuarantine AND NOT RelistingQuarantine
                           AND DiscontinuityClassification <> 'UNRESOLVED_ADJUSTMENT_DISCONTINUITY'
                           AS BOOLEAN
                       ) AS IsResearchEligible,
                       Close AS PointInTimePriceEligibilityClose
                FROM classified
                WHERE YEAR(Date) = ?
                ORDER BY Date, MembershipSymbol
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """,
            [
                str(raw_path),
                str(prior_raw_path or raw_path),
                year,
                year,
                str(eod2_path),
                year - 1,
                year,
                str(reconciliation_path),
                str(reconciliation_path),
                str(relisting_path),
                year,
            ],
        )
    os.replace(temporary, output_path)


def build_certified_adjusted(*, data_root: Path = DATA_ROOT) -> dict[str, Any]:
    raw_paths = _paths(data_root, "raw")
    eod2_path = data_root / "08 Parquet" / "secondary_adjusted_cache" / "eod2_relevant_adjusted_daily.parquet"
    reconciliation_path = data_root / "04 Corporate Actions" / "nifty500_corporate_action_reconciliation.parquet"
    if not raw_paths or not eod2_path.exists() or not reconciliation_path.exists():
        raise RuntimeError("Raw, EOD2 cache and corporate-action reconciliation are required")
    relisting_path, relisting_status = _build_relisting_intervals(data_root, raw_paths)
    output_root = data_root / "08 Parquet" / "certified_adjusted"
    checkpoint_root = data_root / "12 Checkpoints" / "certified_adjusted_years"
    output_root.mkdir(parents=True, exist_ok=True)
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    eod2_sha = sha256_file(eod2_path)
    reconciliation_sha = sha256_file(reconciliation_path)
    code_sha = sha256_file(Path(__file__))
    yearly: list[dict[str, Any]] = []
    for index, raw_path in enumerate(raw_paths):
        year = int(raw_path.parent.name.split("=", 1)[1])
        output = output_root / f"year={year}" / "nifty500_adjusted_daily.parquet"
        checkpoint_path = checkpoint_root / f"certified_adjusted_{year}.json"
        fingerprint = canonical_hash(
            {
                "foundation_version": FOUNDATION_VERSION,
                "raw_sha256": sha256_file(raw_path),
                "eod2_sha256": eod2_sha,
                "reconciliation_sha256": reconciliation_sha,
                "relisting_sha256": relisting_status["sha256"],
                "builder_code_sha256": code_sha,
                "year": year,
            }
        )
        if output.exists() and checkpoint_path.exists():
            prior = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            if prior.get("input_fingerprint_sha256") == fingerprint and prior.get("output_sha256") == sha256_file(
                output
            ):
                yearly.append(prior)
                print(f"Certified adjusted {year}: reused hash-verified cache", flush=True)
                continue
        _build_year(
            raw_path=raw_path,
            prior_raw_path=raw_paths[index - 1] if index else None,
            eod2_path=eod2_path,
            reconciliation_path=reconciliation_path,
            relisting_path=relisting_path,
            output_path=output,
            year=year,
        )
        with duckdb.connect() as connection:
            metrics = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || COALESCE(ISIN, Symbol)),
                       SUM(Open <= 0 OR High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) OR Volume < 0),
                       SUM(CorporateActionQuarantineFlag), SUM(UnresolvedAdjustmentDiscontinuityFlag),
                       SUM(ExtremeOfficialMarketMoveFlag), SUM(IsResearchEligible)
                FROM read_parquet(?)
                """,
                [str(output)],
            ).fetchone()
        checkpoint = {
            "year": year,
            "status": "COMPLETE",
            "input_fingerprint_sha256": fingerprint,
            "output_path": str(output),
            "output_sha256": sha256_file(output),
            "rows": metrics[0],
            "sessions": metrics[1],
            "isins": metrics[2],
            "duplicate_date_membership_symbol_rows": metrics[3],
            "duplicate_date_identity_rows": metrics[4],
            "invalid_bar_rows": metrics[5],
            "quarantine_rows": metrics[6],
            "unresolved_adjustment_discontinuity_rows": metrics[7],
            "extreme_official_market_move_rows": metrics[8],
            "research_eligible_rows": metrics[9],
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        }
        checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
        atomic_json(checkpoint_path, checkpoint)
        yearly.append(checkpoint)
        print(f"Certified adjusted {year}: {metrics[0]:,} rows", flush=True)

    outputs = sorted(output_root.glob("year=*/nifty500_adjusted_daily.parquet"))
    with duckdb.connect() as connection:
        total = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN), MIN(Date), MAX(Date),
                   SUM(CorporateActionQuarantineFlag), SUM(UnresolvedAdjustmentDiscontinuityFlag),
                   SUM(ExtremeOfficialMarketMoveFlag), SUM(IsResearchEligible),
                   SUM(Open <= 0 OR High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) OR Volume < 0),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol)
            FROM read_parquet(?)
            """,
            [[str(path) for path in outputs]],
        ).fetchone()
        source_counts = dict(
            connection.execute(
                "SELECT AdjustmentSource, COUNT(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                [[str(path) for path in outputs]],
            ).fetchall()
        )
        anomalies = connection.execute(
            """
            SELECT Date, Symbol, ISIN, AdjustedReturn1D, RawReturn1D, AdjustmentSource,
                   DiscontinuityClassification, CorporateActionQuarantineReason
            FROM read_parquet(?)
            WHERE DiscontinuityClassification NOT IN ('NONE', 'NOT_TESTED_FIRST_ROW_OR_LONG_GAP')
            ORDER BY Date, Symbol
            """,
            [[str(path) for path in outputs]],
        ).df()
    anomaly_path = data_root / "09 Validation" / "nifty500_certified_adjusted_discontinuity_audit.csv"
    anomaly_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_anomaly = anomaly_path.with_name(f".{anomaly_path.name}.{uuid4().hex}.partial")
    anomalies.to_csv(temporary_anomaly, index=False)
    os.replace(temporary_anomaly, anomaly_path)
    hard_pass = total[6] == 0 and total[9] == 0 and total[10] == 0 and total[0] == sum(row["rows"] for row in yearly)
    status: dict[str, Any] = {
        "status": "CERTIFIED_PASS_WITH_DOCUMENTED_QUARANTINE" if hard_pass else "CERTIFICATION_FAIL",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "rows": total[0],
        "sessions": total[1],
        "isins": total[2],
        "earliest_date": str(total[3]),
        "latest_date": str(total[4]),
        "quarantine_rows": total[5],
        "unresolved_adjustment_discontinuity_rows": total[6],
        "extreme_official_market_move_rows": total[7],
        "research_eligible_rows": total[8],
        "invalid_bar_rows": total[9],
        "duplicate_date_membership_symbol_rows": total[10],
        "adjustment_source_counts": source_counts,
        "relisting_audit": relisting_status,
        "anomaly_audit_path": str(anomaly_path),
        "anomaly_audit_sha256": sha256_file(anomaly_path),
        "eod2_cache_sha256": eod2_sha,
        "corporate_action_reconciliation_sha256": reconciliation_sha,
        "yearly_status": yearly,
        "point_in_time_price_gate": "RawClose only; adjusted close never used for historical price eligibility",
        "future_information_policy": (
            "Backward mechanical scaling changes units only; membership and eligibility remain effective-dated. "
            "Event identity/ratio is never a predictive feature."
        ),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "certified_adjusted_build_status.json", status)
    atomic_json(
        data_root / "12 Checkpoints" / "phase_07_certified_adjusted.json",
        {**status, "checkpoint_status": "COMPLETE_HASH_VERIFIED"},
    )
    return status
