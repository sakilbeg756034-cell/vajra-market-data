from __future__ import annotations

import csv
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.raw_ohlcv import EOD2_MAP
from vajra_regime.nifty500_migration.timeline import MASTER_DB


EOD2_ROOT = paths.EXTRACTED_ORIGINAL_DATA / "EOD2 Historical Data/eod2_data-main/daily"


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _raw_paths(data_root: Path) -> list[Path]:
    return sorted((data_root / "08 Parquet" / "raw").glob("year=*/nifty500_raw_daily.parquet"))


def _raw_identities(paths: list[Path]) -> tuple[set[str], set[str]]:
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT DISTINCT UPPER(Symbol), ISIN FROM read_parquet(?) ORDER BY 1, 2",
            [[str(path) for path in paths]],
        ).fetchall()
    return {row[0] for row in rows}, {row[1] for row in rows if row[1]}


def _eod2_identity_metadata(
    identity_payload: dict[str, Any],
    *,
    available_symbols: set[str],
) -> dict[str, dict[str, set[str]]]:
    metadata = {
        symbol: {"identity_isins": set(), "historical_symbols": {symbol}}
        for symbol in available_symbols
    }
    histories_by_isin: dict[str, set[str]] = {}
    for isin, history in identity_payload["isin2hist"].items():
        history_symbols = {row["symbol"].strip().upper() for row in history if row.get("symbol")}
        histories_by_isin[isin] = history_symbols
    for isin, history_symbols in histories_by_isin.items():
        for source_symbol in history_symbols & available_symbols:
            metadata[source_symbol]["identity_isins"].add(isin)
            metadata[source_symbol]["historical_symbols"].update(history_symbols)
    for source_symbol in available_symbols:
        fallback_isin = identity_payload["sym2isin"].get(source_symbol)
        if fallback_isin:
            metadata[source_symbol]["identity_isins"].add(fallback_isin)
            metadata[source_symbol]["historical_symbols"].update(
                histories_by_isin.get(fallback_isin, set())
            )
    return metadata


def _relevant_eod2_sources(
    symbols: set[str],
    raw_isins: set[str],
    identity_payload: dict[str, Any],
    *,
    eod2_root: Path = EOD2_ROOT,
) -> tuple[list[Path], set[str], dict[str, dict[str, set[str]]]]:
    available = {path.stem.upper(): path for path in eod2_root.glob("*.csv")}
    metadata = _eod2_identity_metadata(identity_payload, available_symbols=set(available))
    relevant_symbols = {
        source_symbol
        for source_symbol, source_metadata in metadata.items()
        if source_symbol in symbols
        or bool(source_metadata["identity_isins"] & raw_isins)
        or bool(source_metadata["historical_symbols"] & symbols)
    }
    matched = [available[symbol] for symbol in sorted(relevant_symbols)]
    covered_raw_symbols: set[str] = set()
    for source_symbol in relevant_symbols:
        covered_raw_symbols.add(source_symbol)
        covered_raw_symbols.update(metadata[source_symbol]["historical_symbols"])
    return matched, symbols - covered_raw_symbols, metadata


def _build_eod2_cache(
    *,
    data_root: Path,
    raw_paths: list[Path],
    start: date,
    as_of: date,
) -> tuple[Path, dict[str, Any]]:
    provenance = data_root / "10 Provenance"
    cache_root = data_root / "08 Parquet" / "secondary_adjusted_cache"
    logs = data_root / "11 Logs"
    for directory in (provenance, cache_root, logs):
        directory.mkdir(parents=True, exist_ok=True)
    symbols, raw_isins = _raw_identities(raw_paths)
    identity_payload = json.loads(EOD2_MAP.read_text(encoding="utf-8"))
    paths, unmatched_symbols, identity_metadata = _relevant_eod2_sources(
        symbols,
        raw_isins,
        identity_payload,
    )
    manifest_rows: list[dict[str, Any]] = []
    for index, path in enumerate(paths, start=1):
        manifest_rows.append(
            {
                "symbol": path.stem.upper(),
                "source_path": str(path),
                "source_file": path.name,
                "source_sha256": sha256_file(path),
                "source_grade": "B_EOD2_NSE_DERIVED_SPLIT_BONUS_ADJUSTED",
                "identity_isins": "|".join(sorted(identity_metadata[path.stem.upper()]["identity_isins"])),
                "historical_symbols": "|".join(
                    sorted(identity_metadata[path.stem.upper()]["historical_symbols"])
                ),
                "identity_map_sha256": sha256_file(EOD2_MAP),
            }
        )
        if index % 250 == 0:
            print(f"EOD2 relevant-source hashing: {index}/{len(paths)}", flush=True)
    manifest_path = provenance / "nifty500_relevant_eod2_adjusted_source_manifest.csv"
    _write_csv(
        manifest_path,
        manifest_rows,
        [
            "symbol",
            "source_path",
            "source_file",
            "source_sha256",
            "source_grade",
            "identity_isins",
            "historical_symbols",
            "identity_map_sha256",
        ],
    )
    fingerprint = canonical_hash(
        {
            "foundation_version": FOUNDATION_VERSION,
            "start": start.isoformat(),
            "as_of": as_of.isoformat(),
            "builder_code_sha256": sha256_file(Path(__file__)),
            "identity_map_sha256": sha256_file(EOD2_MAP),
            "sources": [(row["source_path"], row["source_sha256"]) for row in manifest_rows],
        }
    )
    cache_path = cache_root / "eod2_relevant_adjusted_daily.parquet"
    status_path = logs / "eod2_relevant_adjusted_cache_status.json"
    if cache_path.exists() and status_path.exists():
        prior = json.loads(status_path.read_text(encoding="utf-8"))
        if prior.get("input_fingerprint_sha256") == fingerprint and prior.get("cache_sha256") == sha256_file(
            cache_path
        ):
            print("EOD2 relevant adjusted cache: reused hash-verified cache", flush=True)
            return cache_path, prior

    if cache_path.exists():
        with duckdb.connect() as connection:
            orphan_summary = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), MIN(Date), MAX(Date),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || Symbol),
                       COUNT(DISTINCT SourceFile), COUNT(DISTINCT IdentityMapSha256),
                       MIN(IdentityMapSha256)
                FROM read_parquet(?)
                """,
                [str(cache_path)],
            ).fetchone()
            source_mismatches = connection.execute(
                """
                SELECT COUNT(*) FROM (
                    (SELECT DISTINCT SourceFile, SourceSha256 FROM read_parquet(?)
                     EXCEPT
                     SELECT source_file, source_sha256 FROM read_csv_auto(?, header=true, all_varchar=true))
                    UNION ALL
                    (SELECT source_file, source_sha256 FROM read_csv_auto(?, header=true, all_varchar=true)
                     EXCEPT
                     SELECT DISTINCT SourceFile, SourceSha256 FROM read_parquet(?))
                )
                """,
                [str(cache_path), str(manifest_path), str(manifest_path), str(cache_path)],
            ).fetchone()[0]
        if (
            orphan_summary[0] > 0
            and orphan_summary[5] == 0
            and orphan_summary[6] == len(paths)
            and orphan_summary[7] == 1
            and orphan_summary[8] == sha256_file(EOD2_MAP)
            and source_mismatches == 0
        ):
            status = {
                "status": "COMPLETE_RECOVERED_AFTER_INTERRUPTION",
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "input_fingerprint_sha256": fingerprint,
                "relevant_raw_symbols": len(symbols),
                "relevant_raw_isins": len(raw_isins),
                "matched_eod2_files": len(paths),
                "raw_symbols_without_same-name_eod2_file": sorted(unmatched_symbols),
                "rows": orphan_summary[0],
                "sessions": orphan_summary[1],
                "symbols": orphan_summary[2],
                "earliest_date": str(orphan_summary[3]),
                "latest_date": str(orphan_summary[4]),
                "duplicate_date_symbol_rows": orphan_summary[5],
                "cache_path": str(cache_path),
                "cache_sha256": sha256_file(cache_path),
                "manifest_path": str(manifest_path),
                "manifest_sha256": sha256_file(manifest_path),
                "recovery_validation": "SOURCE_HASH_SET_AND_IDENTITY_MAP_AND_UNIQUENESS_VERIFIED",
            }
            status["status_payload_sha256"] = canonical_hash(status)
            atomic_json(status_path, status)
            print("EOD2 relevant adjusted cache: recovered verified atomic cache", flush=True)
            return cache_path, status

    temporary = cache_path.with_name(f".{cache_path.name}.{uuid4().hex}.partial")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                SELECT CAST(d.Date AS DATE) AS Date,
                       UPPER(m.symbol) AS Symbol,
                       CAST(d.Open AS DOUBLE) AS Open,
                       CAST(d.High AS DOUBLE) AS High,
                       CAST(d.Low AS DOUBLE) AS Low,
                       CAST(d.Close AS DOUBLE) AS Close,
                       CAST(d.Volume AS BIGINT) AS Volume,
                       d.Series,
                       CAST(d.TOTAL_TRADES AS DOUBLE) AS TotalTrades,
                       m.source_file AS SourceFile,
                       m.source_sha256 AS SourceSha256,
                       NULLIF(m.identity_isins, '') AS IdentityISINs,
                       NULLIF(m.historical_symbols, '') AS HistoricalSymbols,
                       m.identity_map_sha256 AS IdentityMapSha256
                FROM read_csv(?, header=true, filename=true, union_by_name=true) d
                JOIN read_csv_auto(?, header=true, all_varchar=true) m
                  ON d.filename = m.source_path
                WHERE CAST(d.Date AS DATE) BETWEEN ? AND ?
                ORDER BY Date, Symbol
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """,
            [[str(path) for path in paths], str(manifest_path), start, as_of],
        )
    os.replace(temporary, cache_path)
    with duckdb.connect() as connection:
        summary = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), MIN(Date), MAX(Date),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || Symbol)
            FROM read_parquet(?)
            """,
            [str(cache_path)],
        ).fetchone()
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "input_fingerprint_sha256": fingerprint,
        "relevant_raw_symbols": len(symbols),
        "relevant_raw_isins": len(raw_isins),
        "matched_eod2_files": len(paths),
        "raw_symbols_without_same-name_eod2_file": sorted(unmatched_symbols),
        "rows": summary[0],
        "sessions": summary[1],
        "symbols": summary[2],
        "earliest_date": str(summary[3]),
        "latest_date": str(summary[4]),
        "duplicate_date_symbol_rows": summary[5],
        "cache_path": str(cache_path),
        "cache_sha256": sha256_file(cache_path),
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(status_path, status)
    return cache_path, status


def _build_adjusted_year(
    *,
    raw_path: Path,
    prior_raw_path: Path | None,
    eod2_cache: Path,
    output_path: Path,
    year: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid4().hex}.partial")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(f"ATTACH '{MASTER_DB.as_posix()}' AS master (READ_ONLY)")
        connection.execute(
            f"""
            COPY (
                WITH raw_rows AS (
                    SELECT * FROM read_parquet(?)
                    UNION ALL BY NAME
                    SELECT * FROM read_parquet(?)
                    WHERE Date >= MAKE_DATE(?, 1, 1) - INTERVAL 15 DAY
                      AND Date < MAKE_DATE(?, 1, 1)
                ), master_rows AS (
                    SELECT * FROM master.clean_daily
                    WHERE Year IN (?, ?)
                ), eod2_rows AS (
                    SELECT * FROM read_parquet(?)
                    WHERE YEAR(Date) IN (?, ?)
                ), raw_aliases AS (
                    SELECT Date, MembershipSymbol, Symbol AS RawSymbol,
                           'SYMBOL' AS AliasType, Symbol AS AliasValue
                    FROM raw_rows
                    UNION ALL
                    SELECT Date, MembershipSymbol, Symbol AS RawSymbol,
                           'ISIN' AS AliasType, ISIN AS AliasValue
                    FROM raw_rows
                    WHERE ISIN IS NOT NULL
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
                           e.Symbol, e.Open, e.High, e.Low, e.Close, e.Volume,
                           e.SourceFile, e.SourceSha256, e.IdentityISINs,
                           e.HistoricalSymbols, e.IdentityMapSha256,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.Date, r.MembershipSymbol
                               ORDER BY CASE WHEN e.Symbol = r.RawSymbol THEN 0
                                             WHEN e.AliasType = 'ISIN' THEN 1
                                             ELSE 2 END,
                                        e.Symbol
                           ) AS eod2_choice
                    FROM raw_aliases r
                    JOIN eod2_aliases e
                      ON e.Date = r.Date
                     AND e.AliasType = r.AliasType
                     AND e.AliasValue = r.AliasValue
                    QUALIFY eod2_choice = 1
                ), candidates AS (
                    SELECT r.*,
                           c.Symbol AS MasterSymbol,
                           c.Open AS MasterOpen,
                           c.High AS MasterHigh,
                           c.Low AS MasterLow,
                           c.Close AS MasterClose,
                           c.Volume AS MasterVolume,
                           c.CorporateActionQuarantineFlag AS MasterQuarantine,
                           c.CorporateActionQuarantineReason AS MasterQuarantineReason,
                           c.IsResearchEligible AS InheritedMasterResearchEligible,
                           e.Open AS Eod2Open,
                           e.High AS Eod2High,
                           e.Low AS Eod2Low,
                           e.Close AS Eod2Close,
                           e.Volume AS Eod2Volume,
                           e.SourceFile AS Eod2SourceFile,
                           e.SourceSha256 AS Eod2SourceSha256,
                           e.Symbol AS Eod2Symbol,
                           e.IdentityISINs AS Eod2IdentityISINs,
                           e.HistoricalSymbols AS Eod2HistoricalSymbols,
                           e.IdentityMapSha256 AS Eod2IdentityMapSha256,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.Date, r.MembershipSymbol
                               ORDER BY CASE WHEN c.Date IS NOT NULL THEN 0 ELSE 1 END,
                                        CASE WHEN c.Symbol = r.Symbol THEN 0 ELSE 1 END,
                                        c.Symbol
                           ) AS master_choice
                    FROM raw_rows r
                    LEFT JOIN master_rows c ON c.Date = r.Date AND c.ISIN = r.ISIN
                    LEFT JOIN eod2_matches e
                      ON e.Date = r.Date AND e.MembershipSymbol = r.MembershipSymbol
                ), chosen AS (
                    SELECT *,
                           COALESCE(MasterOpen, Eod2Open, Open) AS AdjustedOpen,
                           COALESCE(MasterHigh, Eod2High, High) AS AdjustedHigh,
                           COALESCE(MasterLow, Eod2Low, Low) AS AdjustedLow,
                           COALESCE(MasterClose, Eod2Close, Close) AS AdjustedClose,
                           COALESCE(MasterVolume, Eod2Volume, Volume) AS AdjustedVolume,
                           CASE WHEN MasterClose IS NOT NULL THEN 'CERTIFIED_EXISTING_MASTER'
                                WHEN Eod2Close IS NOT NULL AND Eod2Symbol = Symbol
                                    THEN 'EOD2_DIRECT_SECONDARY_ADJUSTED'
                                WHEN Eod2Close IS NOT NULL AND ISIN IS NOT NULL
                                    THEN 'EOD2_ISIN_ALIAS_SECONDARY_ADJUSTED'
                                WHEN Eod2Close IS NOT NULL
                                    THEN 'EOD2_SYMBOL_HISTORY_ALIAS_SECONDARY_ADJUSTED'
                                ELSE 'OFFICIAL_RAW_NO_VALIDATED_ADJUSTMENT' END AS AdjustmentSource,
                           CASE WHEN MasterClose IS NOT NULL THEN 'VERIFIED_EXISTING_MASTER'
                                WHEN Eod2Close IS NOT NULL THEN 'VERIFIED_SECONDARY_AGAINST_OFFICIAL_RAW'
                                ELSE 'INSUFFICIENT_ADJUSTMENT_EVIDENCE' END AS AdjustmentConfidence
                    FROM candidates
                    WHERE master_choice = 1
                ), with_lag AS (
                    SELECT *,
                           LAG(AdjustedClose) OVER (
                               PARTITION BY COALESCE(ISIN, 'UNRESOLVED_SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorAdjustedClose,
                           LAG(Date) OVER (
                               PARTITION BY COALESCE(ISIN, 'UNRESOLVED_SYMBOL:' || Symbol) ORDER BY Date
                           ) AS PriorDate
                    FROM chosen
                ), final_rows AS (
                    SELECT Date, Symbol, MembershipSymbol, ISIN, ExchangeISIN, Series,
                           AdjustedOpen AS Open, AdjustedHigh AS High, AdjustedLow AS Low,
                           AdjustedClose AS Close, CAST(AdjustedVolume AS BIGINT) AS Volume,
                           Open AS RawOpen, High AS RawHigh, Low AS RawLow, Close AS RawClose,
                           Volume AS RawVolume, PrevClose AS RawPrevClose, Turnover AS RawTurnover,
                           TotalTrades AS RawTotalTrades,
                           CASE WHEN Close > 0 THEN AdjustedClose / Close ELSE NULL END AS PriceAdjustmentFactor,
                           CASE WHEN Volume > 0 THEN CAST(AdjustedVolume AS DOUBLE) / Volume ELSE NULL END
                               AS VolumeAdjustmentFactor,
                           AdjustmentSource, AdjustmentConfidence,
                           CASE WHEN AdjustmentSource = 'CERTIFIED_EXISTING_MASTER' THEN 'INHERITED_MASTER:' || MasterSymbol
                                 WHEN AdjustmentSource LIKE 'EOD2_%' THEN Eod2SourceFile
                                 ELSE SourceArchive END AS AdjustmentSourceFile,
                           CASE WHEN AdjustmentSource LIKE 'EOD2_%' THEN Eod2SourceSha256
                                ELSE SourceSha256 END AS AdjustmentSourceSha256,
                           CASE WHEN AdjustmentSource LIKE 'EOD2_%' THEN Eod2IdentityMapSha256
                                ELSE NULL END AS AdjustmentIdentityMapSha256,
                           SourceArchive AS RawSourceArchive, SourceSha256 AS RawSourceSha256,
                           MembershipConfidence, MembershipEvidence, FoundationVersion, IdentityStatus,
                           CAST(COALESCE(MasterQuarantine, false) AS BOOLEAN) AS InheritedCorporateActionQuarantine,
                           COALESCE(MasterQuarantineReason, '') AS InheritedCorporateActionQuarantineReason,
                           InheritedMasterResearchEligible,
                           CASE WHEN PriorAdjustedClose IS NULL OR DATE_DIFF('day', PriorDate, Date) > 10 THEN false
                                WHEN ABS(AdjustedClose / PriorAdjustedClose - 1.0) > 0.45 THEN true
                                ELSE false END AS UnexplainedDiscontinuityFlag
                    FROM with_lag
                )
                SELECT *,
                       CAST(InheritedCorporateActionQuarantine OR UnexplainedDiscontinuityFlag AS BOOLEAN)
                           AS CorporateActionQuarantineFlag,
                       CASE WHEN InheritedCorporateActionQuarantine THEN InheritedCorporateActionQuarantineReason
                            WHEN UnexplainedDiscontinuityFlag THEN 'UNEXPLAINED_ADJUSTED_CLOSE_JUMP_GT_45_PERCENT'
                            ELSE '' END AS CorporateActionQuarantineReason,
                       CAST(
                           Open > 0 AND High >= GREATEST(Open, Close) AND Low <= LEAST(Open, Close)
                           AND Volume >= 0 AND NOT InheritedCorporateActionQuarantine
                           AND NOT UnexplainedDiscontinuityFlag
                           AS BOOLEAN
                       ) AS IsResearchEligible,
                       RawClose AS PointInTimePriceEligibilityClose
                FROM final_rows
                WHERE YEAR(Date) = ?
                ORDER BY Date, MembershipSymbol
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """,
            [
                str(raw_path),
                str(prior_raw_path or raw_path),
                year,
                year,
                year - 1,
                year,
                str(eod2_cache),
                year - 1,
                year,
                year,
            ],
        )
    os.replace(temporary, output_path)


def build_adjusted_ohlcv(
    *,
    data_root: Path = DATA_ROOT,
    start: date = date(2009, 1, 1),
    as_of: date = date(2026, 8, 13),
) -> dict[str, Any]:
    raw_paths = _raw_paths(data_root)
    if not raw_paths:
        raise RuntimeError("Official raw Nifty500 Parquet does not exist")
    eod2_cache, eod2_status = _build_eod2_cache(
        data_root=data_root,
        raw_paths=raw_paths,
        start=start,
        as_of=as_of,
    )
    adjusted_root = data_root / "08 Parquet" / "adjusted"
    checkpoint_root = data_root / "12 Checkpoints" / "adjusted_ohlcv_years"
    for directory in (adjusted_root, checkpoint_root):
        directory.mkdir(parents=True, exist_ok=True)
    yearly: list[dict[str, Any]] = []
    for raw_index, raw_path in enumerate(raw_paths):
        year = int(raw_path.parent.name.split("=", 1)[1])
        if not (start.year <= year <= as_of.year):
            continue
        output_path = adjusted_root / f"year={year}" / "nifty500_adjusted_daily.parquet"
        checkpoint_path = checkpoint_root / f"adjusted_ohlcv_{year}.json"
        fingerprint = canonical_hash(
            {
                "foundation_version": FOUNDATION_VERSION,
                "raw_sha256": sha256_file(raw_path),
                "eod2_cache_sha256": eod2_status["cache_sha256"],
                "builder_code_sha256": sha256_file(Path(__file__)),
                "master_database_frozen_reference": "PRECHANGE_MANIFEST_SHA256:07e0a845256b9c0d944820abf5af1b3069139966c08e0b40123e70e5da847464",
                "year": year,
            }
        )
        if output_path.exists() and checkpoint_path.exists():
            prior = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            if prior.get("input_fingerprint_sha256") == fingerprint and prior.get(
                "output_sha256"
            ) == sha256_file(output_path):
                yearly.append(prior)
                print(f"Adjusted OHLCV {year}: reused hash-verified cache", flush=True)
                continue
        prior_raw_path = raw_paths[raw_index - 1] if raw_index > 0 else None
        _build_adjusted_year(
            raw_path=raw_path,
            prior_raw_path=prior_raw_path,
            eod2_cache=eod2_cache,
            output_path=output_path,
            year=year,
        )
        with duckdb.connect() as connection:
            metrics = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || COALESCE(ISIN, Symbol)),
                       SUM(Open <= 0 OR High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) OR Volume < 0),
                       SUM(CorporateActionQuarantineFlag), SUM(UnexplainedDiscontinuityFlag),
                       SUM(AdjustmentSource = 'CERTIFIED_EXISTING_MASTER'),
                       SUM(AdjustmentSource = 'EOD2_DIRECT_SECONDARY_ADJUSTED'),
                       SUM(AdjustmentSource = 'EOD2_ISIN_ALIAS_SECONDARY_ADJUSTED'),
                       SUM(AdjustmentSource = 'EOD2_SYMBOL_HISTORY_ALIAS_SECONDARY_ADJUSTED'),
                       SUM(AdjustmentSource = 'OFFICIAL_RAW_NO_VALIDATED_ADJUSTMENT')
                FROM read_parquet(?)
                """,
                [str(output_path)],
            ).fetchone()
        checkpoint: dict[str, Any] = {
            "year": year,
            "status": "COMPLETE",
            "input_fingerprint_sha256": fingerprint,
            "output_path": str(output_path),
            "output_sha256": sha256_file(output_path),
            "rows": metrics[0],
            "sessions": metrics[1],
            "isins": metrics[2],
            "duplicate_date_membership_symbol_rows": metrics[3],
            "duplicate_date_identity_rows": metrics[4],
            "invalid_bar_rows": metrics[5],
            "corporate_action_quarantine_rows": metrics[6],
            "unexplained_discontinuity_rows": metrics[7],
            "existing_master_rows": metrics[8],
            "direct_eod2_rows": metrics[9],
            "isin_alias_eod2_rows": metrics[10],
            "symbol_history_alias_eod2_rows": metrics[11],
            "raw_only_rows": metrics[12],
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        }
        checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
        atomic_json(checkpoint_path, checkpoint)
        yearly.append(checkpoint)
        print(f"Adjusted OHLCV {year}: {metrics[0]:,} rows", flush=True)

    adjusted_paths = sorted(adjusted_root.glob("year=*/nifty500_adjusted_daily.parquet"))
    with duckdb.connect() as connection:
        total = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN), MIN(Date), MAX(Date),
                   SUM(CorporateActionQuarantineFlag), SUM(UnexplainedDiscontinuityFlag),
                   SUM(IsResearchEligible)
            FROM read_parquet(?)
            """,
            [[str(path) for path in adjusted_paths]],
        ).fetchone()
        source_counts = dict(
            connection.execute(
                "SELECT AdjustmentSource, COUNT(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                [[str(path) for path in adjusted_paths]],
            ).fetchall()
        )
    status: dict[str, Any] = {
        "status": "COMPLETE_WITH_QUARANTINE" if total[5] else "COMPLETE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "rows": total[0],
        "sessions": total[1],
        "isins": total[2],
        "earliest_date": str(total[3]),
        "latest_date": str(total[4]),
        "corporate_action_quarantine_rows": total[5],
        "unexplained_discontinuity_rows": total[6],
        "research_eligible_rows": total[7],
        "adjustment_source_counts": source_counts,
        "adjusted_root": str(adjusted_root),
        "yearly_status": yearly,
        "eod2_cache_status_sha256": eod2_status["status_payload_sha256"],
        "price_threshold_policy": "USE PointInTimePriceEligibilityClose (official raw), never back-adjusted Close",
        "future_action_prediction_policy": "Adjustment scaling is never a predictive feature or future membership input",
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "adjusted_ohlcv_build_status.json", status)
    return status
