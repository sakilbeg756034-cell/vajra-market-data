from __future__ import annotations

import bisect
import csv
import io
import json
import os
import zipfile
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.bhavcopy_archive import UDIFF_START
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


EOD2_MAP = (
    paths.EXTRACTED_ORIGINAL_DATA / "EOD2 Historical Data/eod2_data-main/isin_symbol_map.json"
)
# Every year folder, not just the current one. The previous code globbed a single hardcoded
# "2026" directory, which would have silently stopped seeing new bhavcopy on 2027-01-01.
LIVE_UDIFF_ROOT = paths.LIVE_UDIFF_ROOT
LIVE_UDIFF_GLOB = "*/BhavCopy_NSE_CM_0_0_0_*_F_0000.csv.zip"


def _decode(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", payload, 0, 1, "no supported encoding")


def _value(row: dict[str, str], *aliases: str) -> str:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and value.strip() not in {"", "-"}:
            return value.strip()
    return ""


def parse_official_bhavcopy(zip_path: Path, session: date) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(zip_path) as bundle:
        names = [name for name in bundle.namelist() if name.casefold().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(f"Expected one CSV in {zip_path}, found {names}")
        payload = bundle.read(names[0])
    for row in csv.DictReader(io.StringIO(_decode(payload))):
        symbol = _value(row, "SYMBOL", "TckrSymb").upper()
        series = _value(row, "SERIES", "SctySrs").upper()
        if not symbol or series not in {"EQ", "BE", "BZ"}:
            continue
        yield {
            "Date": session.isoformat(),
            "Symbol": symbol,
            "ISIN": _value(row, "ISIN"),
            "Series": series,
            "Open": _value(row, "OPEN", "OpnPric"),
            "High": _value(row, "HIGH", "HghPric"),
            "Low": _value(row, "LOW", "LwPric"),
            "Close": _value(row, "CLOSE", "ClsPric"),
            "PrevClose": _value(row, "PREVCLOSE", "PrvsClsgPric"),
            "Volume": _value(row, "TOTTRDQTY", "TtlTradgVol"),
            "Turnover": _value(row, "TOTTRDVAL", "TtlTrfVal"),
            "TotalTrades": _value(row, "TOTALTRADES", "TtlNbOfTxsExctd"),
            "SourceFormat": "OFFICIAL_NSE_UDIFF" if session >= UDIFF_START else "OFFICIAL_NSE_LEGACY_BHAVCOPY",
            "SourceMember": names[0],
        }


def _membership_states(data_root: Path) -> tuple[list[date], dict[date, dict[str, dict[str, str]]]]:
    path = data_root / "02 Constituent History" / "nifty500_membership_intervals.csv"
    grouped: dict[date, dict[str, dict[str, str]]] = defaultdict(dict)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            effective = date.fromisoformat(row["valid_from"])
            grouped[effective][row["symbol"]] = row
    return sorted(grouped), grouped


def _membership_for_session(
    session: date,
    *,
    state_dates: list[date],
    states: dict[date, dict[str, dict[str, str]]],
) -> dict[str, dict[str, str]]:
    position = bisect.bisect_right(state_dates, session) - 1
    if position < 0:
        return {}
    return states[state_dates[position]]


def _source_records(data_root: Path, *, start: date, as_of: date) -> list[dict[str, Any]]:
    manifest_path = data_root / "10 Provenance" / "official_nse_bhavcopy_download_manifest.csv"
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        records = [
            {
                "session": date.fromisoformat(row["session_date"]),
                "path": Path(row["path"]),
                "sha256": row["sha256"],
                "provenance": "ARCHIVED_IN_NIFTY500_FOUNDATION",
            }
            for row in csv.DictReader(handle)
            if start <= date.fromisoformat(row["session_date"]) <= as_of
        ]
    if as_of.year >= 2026:
        for path in sorted(LIVE_UDIFF_ROOT.glob(LIVE_UDIFF_GLOB)):
            token = path.name.split("_")[6]
            session = datetime.strptime(token, "%Y%m%d").date()
            if start <= session <= as_of:
                records.append(
                    {
                        "session": session,
                        "path": path,
                        "sha256": sha256_file(path),
                        "provenance": "INHERITED_CERTIFIED_OFFICIAL_2026_ARCHIVE",
                    }
                )
    return sorted({row["session"]: row for row in records}.values(), key=lambda row: row["session"])


def _year_fingerprint(records: list[dict[str, Any]], intervals_hash: str) -> str:
    return canonical_hash(
        {
            "foundation_version": FOUNDATION_VERSION,
            "intervals_sha256": intervals_hash,
            "sources": [(row["session"].isoformat(), row["sha256"]) for row in records],
        }
    )


def _identity_history(payload: dict[str, Any]) -> tuple[dict[str, list[tuple[date, date, str]]], dict[str, str]]:
    histories: dict[str, list[tuple[date, date, str]]] = defaultdict(list)
    for isin, rows in payload["isin2hist"].items():
        for row in rows:
            histories[row["symbol"].strip().upper()].append(
                (date.fromisoformat(row["from_date"]), date.fromisoformat(row["to_date"]), isin)
            )
    for rows in histories.values():
        rows.sort()
    return dict(histories), {symbol.strip().upper(): isin for symbol, isin in payload["sym2isin"].items()}


def _resolve_isin(
    symbol: str,
    session: date,
    *,
    histories: dict[str, list[tuple[date, date, str]]],
    fallback: dict[str, str],
) -> str:
    normalized = symbol.strip().upper()
    for valid_from, valid_to, isin in histories.get(normalized, []):
        if valid_from <= session <= valid_to:
            return isin
    return fallback.get(normalized, "")


def _write_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _write_year_parquet(csv_path: Path, parquet_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.partial")
    csv_sql = str(csv_path).replace("'", "''")
    output_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                SELECT CAST(Date AS DATE) AS Date,
                       Symbol,
                       MembershipSymbol,
                       NULLIF(ISIN, '') AS ISIN,
                       NULLIF(ExchangeISIN, '') AS ExchangeISIN,
                       Series,
                       CAST(Open AS DOUBLE) AS Open,
                       CAST(High AS DOUBLE) AS High,
                       CAST(Low AS DOUBLE) AS Low,
                       CAST(Close AS DOUBLE) AS Close,
                       CAST(NULLIF(PrevClose, '') AS DOUBLE) AS PrevClose,
                       CAST(Volume AS BIGINT) AS Volume,
                       CAST(NULLIF(Turnover, '') AS DOUBLE) AS Turnover,
                       CAST(NULLIF(TotalTrades, '') AS BIGINT) AS TotalTrades,
                       SourceFormat, SourceArchive, SourceSha256, SourceMember,
                       MembershipConfidence, MembershipEvidence, FoundationVersion,
                       IdentityStatus
                FROM read_csv_auto('{csv_sql}', header=true, all_varchar=true)
                ORDER BY Date, Symbol
            ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """
        )
    os.replace(temporary, parquet_path)


def build_official_raw_ohlcv(
    *,
    data_root: Path = DATA_ROOT,
    start: date = date(2009, 1, 1),
    as_of: date = date(2026, 8, 13),
) -> dict[str, Any]:
    parquet_root = data_root / "08 Parquet" / "raw"
    validation_root = data_root / "09 Validation"
    checkpoint_root = data_root / "12 Checkpoints" / "raw_ohlcv_years"
    temp_root = data_root / "12 Checkpoints" / "temporary_raw_build"
    for directory in (parquet_root, validation_root, checkpoint_root, temp_root):
        directory.mkdir(parents=True, exist_ok=True)
    interval_path = data_root / "02 Constituent History" / "nifty500_membership_intervals.csv"
    intervals_hash = sha256_file(interval_path)
    state_dates, states = _membership_states(data_root)
    source_records = _source_records(data_root, start=start, as_of=as_of)
    eod2_payload = json.loads(EOD2_MAP.read_text(encoding="utf-8"))
    identity_histories, identity_fallback = _identity_history(eod2_payload)
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for record in source_records:
        by_year[record["session"].year].append(record)

    yearly_status: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    for year, records in sorted(by_year.items()):
        fingerprint = _year_fingerprint(records, intervals_hash)
        output_path = parquet_root / f"year={year}" / "nifty500_raw_daily.parquet"
        checkpoint_path = checkpoint_root / f"raw_ohlcv_{year}.json"
        year_missing_path = validation_root / "raw_missing_by_year" / f"nifty500_raw_missing_{year}.csv"
        if output_path.exists() and checkpoint_path.exists():
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            if checkpoint.get("input_fingerprint_sha256") == fingerprint and checkpoint.get(
                "output_sha256"
            ) == sha256_file(output_path) and checkpoint.get("missing_sha256") and year_missing_path.exists() and (
                checkpoint["missing_sha256"] == sha256_file(year_missing_path)
            ):
                yearly_status.append(checkpoint)
                with year_missing_path.open("r", encoding="utf-8-sig", newline="") as handle:
                    missing_rows.extend(csv.DictReader(handle))
                print(f"Official raw OHLCV {year}: reused hash-verified cache", flush=True)
                continue

        csv_path = temp_root / f"raw_{year}_{uuid4().hex}.csv"
        fieldnames = [
            "Date",
            "Symbol",
            "MembershipSymbol",
            "ISIN",
            "ExchangeISIN",
            "Series",
            "Open",
            "High",
            "Low",
            "Close",
            "PrevClose",
            "Volume",
            "Turnover",
            "TotalTrades",
            "SourceFormat",
            "SourceArchive",
            "SourceSha256",
            "SourceMember",
            "MembershipConfidence",
            "MembershipEvidence",
            "FoundationVersion",
            "IdentityStatus",
        ]
        row_count = 0
        year_missing_rows: list[dict[str, Any]] = []
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                membership = _membership_for_session(record["session"], state_dates=state_dates, states=states)
                priority = {"EQ": 0, "BE": 1, "BZ": 2}
                available: dict[str, dict[str, str]] = {}
                for row in parse_official_bhavcopy(record["path"], record["session"]):
                    row["ResolvedISIN"] = row["ISIN"] or _resolve_isin(
                        row["Symbol"],
                        record["session"],
                        histories=identity_histories,
                        fallback=identity_fallback,
                    )
                    current = available.get(row["Symbol"])
                    if current is None or priority[row["Series"]] < priority[current["Series"]]:
                        available[row["Symbol"]] = row
                available_by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
                for row in available.values():
                    if row["ResolvedISIN"]:
                        available_by_isin[row["ResolvedISIN"]].append(row)
                used_exchange_symbols: set[str] = set()
                selected_members: set[str] = set()
                for membership_symbol, member in membership.items():
                    expected_isin = _resolve_isin(
                        membership_symbol,
                        record["session"],
                        histories=identity_histories,
                        fallback=identity_fallback,
                    )
                    row = available.get(membership_symbol)
                    alias_match = False
                    if row is None and expected_isin:
                        choices = [
                            candidate
                            for candidate in available_by_isin.get(expected_isin, [])
                            if candidate["Symbol"] not in used_exchange_symbols
                        ]
                        if choices:
                            row = min(
                                choices,
                                key=lambda candidate: (priority[candidate["Series"]], candidate["Symbol"]),
                            )
                            alias_match = True
                    if row is None or row["Symbol"] in used_exchange_symbols:
                        continue
                    used_exchange_symbols.add(row["Symbol"])
                    selected_members.add(membership_symbol)
                    resolved_isin = row["ResolvedISIN"] or expected_isin
                    writer.writerow(
                        {
                            **{key: value for key, value in row.items() if key != "ResolvedISIN"},
                            "MembershipSymbol": membership_symbol,
                            "ISIN": resolved_isin,
                            "ExchangeISIN": row["ISIN"],
                            "SourceArchive": record["path"].name,
                            "SourceSha256": record["sha256"],
                            "MembershipConfidence": member["confidence_grade"],
                            "MembershipEvidence": member["evidence_source"],
                            "FoundationVersion": FOUNDATION_VERSION,
                            "IdentityStatus": (
                                "OFFICIAL_BHAVCOPY_ISIN_ALIAS_MATCH"
                                if row["ISIN"] and alias_match
                                else "OFFICIAL_BHAVCOPY_ISIN_SYMBOL_MATCH"
                                if row["ISIN"]
                                else "EOD2_EFFECTIVE_IDENTITY_ALIAS_MATCH"
                                if resolved_isin and alias_match
                                else "EOD2_NSE_DERIVED_SYMBOL_ISIN_FALLBACK"
                                if resolved_isin
                                else "UNRESOLVED_ISIN"
                            ),
                        }
                    )
                    row_count += 1
                for symbol in sorted(set(membership) - selected_members):
                    expected_isin = _resolve_isin(
                        symbol,
                        record["session"],
                        histories=identity_histories,
                        fallback=identity_fallback,
                    )
                    year_missing_rows.append(
                        {
                            "Date": record["session"].isoformat(),
                            "Symbol": symbol,
                            "ExpectedISIN": expected_isin,
                            "MembershipConfidence": membership[symbol]["confidence_grade"],
                            "Reason": "NO_EQ_BE_BZ_ROW_IN_OFFICIAL_BHAVCOPY",
                            "SourceArchive": record["path"].name,
                        }
                    )
        _write_year_parquet(csv_path, output_path)
        csv_path.unlink(missing_ok=True)
        missing_fieldnames = [
            "Date",
            "Symbol",
            "ExpectedISIN",
            "MembershipConfidence",
            "Reason",
            "SourceArchive",
        ]
        _write_rows(year_missing_path, year_missing_rows, missing_fieldnames)
        missing_rows.extend(year_missing_rows)
        with duckdb.connect() as connection:
            metrics = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                       SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END),
                       COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || Symbol),
                       SUM(CASE WHEN Open <= 0 OR High <= 0 OR Low <= 0 OR Close <= 0 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) THEN 1 ELSE 0 END),
                       SUM(CASE WHEN Volume < 0 THEN 1 ELSE 0 END)
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
            "symbols": metrics[2],
            "isins": metrics[3],
            "unresolved_isin_rows": metrics[4],
            "duplicate_date_symbol_rows": metrics[5],
            "invalid_price_rows": metrics[6],
            "invalid_high_low_rows": metrics[7],
            "negative_volume_rows": metrics[8],
            "missing_member_session_rows": len(year_missing_rows),
            "missing_path": str(year_missing_path),
            "missing_sha256": sha256_file(year_missing_path),
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        }
        checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
        atomic_json(checkpoint_path, checkpoint)
        yearly_status.append(checkpoint)
        print(f"Official raw OHLCV {year}: {row_count:,} PIT rows", flush=True)

    missing_path = validation_root / "nifty500_official_raw_missing_member_rows.csv"
    missing_fieldnames = [
        "Date",
        "Symbol",
        "ExpectedISIN",
        "MembershipConfidence",
        "Reason",
        "SourceArchive",
    ]
    _write_rows(missing_path, missing_rows, missing_fieldnames)
    parquet_glob = str(parquet_root / "year=*" / "nifty500_raw_daily.parquet")
    with duckdb.connect() as connection:
        totals = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                   SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END), MIN(Date), MAX(Date)
            FROM read_parquet(?, hive_partitioning=true)
            """,
            [parquet_glob],
        ).fetchone()
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "source_sessions": len(source_records),
        "years": len(yearly_status),
        "rows": totals[0],
        "sessions": totals[1],
        "symbols": totals[2],
        "isins": totals[3],
        "unresolved_isin_rows": totals[4],
        "earliest_date": str(totals[5]),
        "latest_date": str(totals[6]),
        "missing_member_session_rows": len(missing_rows),
        "missing_path": str(missing_path),
        "missing_sha256": sha256_file(missing_path),
        "parquet_root": str(parquet_root),
        "yearly_status": yearly_status,
        "eod2_identity_map_path": str(EOD2_MAP),
        "eod2_identity_map_sha256": sha256_file(EOD2_MAP),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "official_raw_ohlcv_build_status.json", status)
    return status
