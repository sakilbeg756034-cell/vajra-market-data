from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from vajra_regime.config import AppConfig
from vajra_regime.data_layout import DataLayout


NSE_UDIFF_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
)
RAW_TABLE = "nse_live_raw_daily"
INGEST_TABLE = "nse_live_ingest_manifest"

# KYUN SIRF 'EQ' KAAFI NAHI HAI
# -----------------------------
# NSE jab kisi stock ko surveillance me daalta hai to uski series EQ se BE
# (trade-for-trade) ya BZ (non-compliant) ho jaati hai. Stock roz trade hota
# rehta hai -- bas segment badalta hai.
#
# Pehle yahan sirf 'EQ' liya jaata tha, isliye aisa stock us poore daur ke liye
# data se GAYAB ho jaata tha. Koi error nahi -- bas rows nadarad. Jab wo wapas
# EQ me aata, us din ka "1-din ka return" asal me poore gap ka nikalta:
# SUZLON 2024-01-15 par +58.6% dikha, jabki wo 98 din ka move tha.
# 17 saal me aise 306 hole, 206 alag naam.
#
# SME (SM/ST) aur government securities (GS/GB/SG) yahan jaan-boojh kar nahi
# hain -- wo is strategy ka universe hai hi nahi.
#
# Ye sirf DATA ke liye hai. BE/BZ ka din tradeable NAHI mana jaata; wo rok
# universe wali layer me lagti hai (monthly_universe.py), kyunki BE me har
# sauda delivery me settle karna padta hai.
TRADEABLE_SERIES: tuple[str, ...] = ("EQ", "BE", "BZ")

# Ek hi din ek ISIN do series me mile to EQ jeetta hai, phir BE, phir BZ.
SERIES_PRIORITY: dict[str, int] = {series: rank for rank, series in enumerate(TRADEABLE_SERIES)}


@dataclass(frozen=True)
class DownloadOutcome:
    trading_date: date
    status: str
    url: str
    zip_path: Path | None
    parquet_path: Path | None
    sha256: str | None
    raw_rows: int
    kept_rows: int
    message: str


def official_bhavcopy_url(trading_date: date) -> str:
    return NSE_UDIFF_URL.format(yyyymmdd=trading_date.strftime("%Y%m%d"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_download(url: str, destination: Path, timeout_seconds: int = 45) -> tuple[str, str]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size > 0:
        return "EXISTS", _sha256(destination)

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/zip,application/octet-stream,*/*",
            "Referer": "https://www.nseindia.com/",
        },
    )
    partial = destination.with_suffix(destination.suffix + ".part")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "NOT_PUBLISHED", ""
        raise RuntimeError(f"NSE download HTTP {exc.code}: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"NSE download failed: {url}: {exc}") from exc

    if len(payload) < 100:
        raise RuntimeError(f"NSE download was unexpectedly small ({len(payload)} bytes): {url}")
    partial.write_bytes(payload)
    partial.replace(destination)
    return "DOWNLOADED", _sha256(destination)


def _pick_csv_from_zip(zip_path: Path) -> tuple[str, bytes]:
    try:
        with zipfile.ZipFile(zip_path) as archive:
            candidates = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(candidates) != 1:
                raise ValueError(
                    f"Expected exactly one CSV inside {zip_path.name}; found {len(candidates)}."
                )
            name = candidates[0]
            return name, archive.read(name)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Invalid NSE ZIP file: {zip_path}") from exc


def _column_lookup(columns: Iterable[str]) -> dict[str, str]:
    return {str(column).strip().lower(): str(column) for column in columns}


def _required_column(lookup: dict[str, str], *aliases: str) -> str:
    for alias in aliases:
        found = lookup.get(alias.lower())
        if found is not None:
            return found
    raise ValueError(f"Required NSE UDiFF column missing. Tried aliases: {aliases}")


def normalize_udiff_bhavcopy(
    zip_path: Path,
    expected_date: date,
    *,
    minimum_kept_rows: int = 500,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Normalize one official NSE CM UDiFF bhavcopy to company EQ OHLCV rows.

    The function intentionally keeps raw NSE prices raw. Corporate-action-adjusted history
    is built in a later controlled layer; this intake must never silently rewrite the
    existing adjusted `clean_daily` table.
    """
    member_name, payload = _pick_csv_from_zip(zip_path)
    frame = pd.read_csv(io.BytesIO(payload), low_memory=False)
    raw_rows = int(len(frame))
    lookup = _column_lookup(frame.columns)

    date_col = _required_column(lookup, "TradDt", "TradeDate", "Date")
    symbol_col = _required_column(lookup, "TckrSymb", "Symbol")
    isin_col = _required_column(lookup, "ISIN")
    series_col = _required_column(lookup, "SctySrs", "Series")
    open_col = _required_column(lookup, "OpnPric", "Open")
    high_col = _required_column(lookup, "HghPric", "High")
    low_col = _required_column(lookup, "LwPric", "Low")
    close_col = _required_column(lookup, "ClsPric", "Close")
    volume_col = _required_column(lookup, "TtlTradgVol", "Volume")
    turnover_col = _required_column(lookup, "TtlTrfVal", "Turnover")

    normalized = pd.DataFrame(
        {
            "Date": pd.to_datetime(frame[date_col], errors="coerce").dt.normalize(),
            "Symbol": frame[symbol_col].astype("string").str.strip(),
            "ISIN": frame[isin_col].astype("string").str.strip(),
            "Series": frame[series_col].astype("string").str.strip().str.upper(),
            "Open": pd.to_numeric(frame[open_col], errors="coerce"),
            "High": pd.to_numeric(frame[high_col], errors="coerce"),
            "Low": pd.to_numeric(frame[low_col], errors="coerce"),
            "Close": pd.to_numeric(frame[close_col], errors="coerce"),
            "Volume": pd.to_numeric(frame[volume_col], errors="coerce"),
            "Turnover": pd.to_numeric(frame[turnover_col], errors="coerce"),
        }
    )

    expected_ts = pd.Timestamp(expected_date)
    normalized = normalized.loc[
        normalized["Date"].eq(expected_ts)
        & normalized["Series"].isin(TRADEABLE_SERIES)
        & normalized["ISIN"].str.startswith("INE", na=False)
        & normalized["Symbol"].notna()
    ].copy()

    price_cols = ["Open", "High", "Low", "Close"]
    finite_prices = np.isfinite(normalized[price_cols]).all(axis=1)
    positive_prices = normalized[price_cols].gt(0).all(axis=1)
    valid_volume = normalized["Volume"].notna() & normalized["Volume"].ge(0)
    valid_turnover = normalized["Turnover"].notna() & normalized["Turnover"].ge(0)
    valid_ohlc = (
        normalized["High"].ge(normalized[["Open", "Close", "Low"]].max(axis=1))
        & normalized["Low"].le(normalized[["Open", "Close", "High"]].min(axis=1))
    )
    normalized = normalized.loc[
        finite_prices & positive_prices & valid_volume & valid_turnover & valid_ohlc
    ].copy()

    normalized["Volume"] = normalized["Volume"].round().astype("int64")
    normalized["SourceFile"] = zip_path.name
    normalized["SourceMember"] = member_name
    normalized["SourceSha256"] = _sha256(zip_path)
    normalized["IngestedAtUTC"] = datetime.now(UTC)

    # Ek ISIN ek din me ek hi baar. Series ka darja sabse upar hai: ek hi
    # security agar EQ aur BE dono me dikhe to EQ jeetega, kyunki tradeable
    # wahi hai. Uske baad purana niyam waisa hi hai -- zyada volume, phir
    # symbol -- taki chunav file ki tarteeb par kabhi na chhoote.
    normalized = normalized.assign(
        _series_rank=normalized["Series"].map(SERIES_PRIORITY)
    ).sort_values(
        ["Date", "ISIN", "_series_rank", "Volume", "Symbol"],
        ascending=[True, True, True, False, True],
    ).drop_duplicates(["Date", "ISIN"], keep="first").drop(columns="_series_rank")
    normalized = normalized.sort_values(["Date", "ISIN"]).reset_index(drop=True)

    if len(normalized) < minimum_kept_rows:
        raise ValueError(
            f"Only {len(normalized)} valid company rows remained for {expected_date}; "
            f"minimum safety threshold is {minimum_kept_rows}."
        )

    if normalized.duplicated(["Date", "ISIN"]).any():
        raise AssertionError("Date + ISIN duplicates remained after NSE normalization.")

    report = {
        "date": expected_date.isoformat(),
        "source_member": member_name,
        "raw_rows": raw_rows,
        "kept_rows": int(len(normalized)),
        "unique_isin": int(normalized["ISIN"].nunique()),
        "minimum_kept_rows": int(minimum_kept_rows),
    }
    return normalized, report


def _manifest_path(layout: DataLayout) -> Path:
    return layout.logs / "NSE EOD" / "nse_eod_download_manifest.csv"


def _load_manifest(layout: DataLayout) -> pd.DataFrame:
    path = _manifest_path(layout)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_manifest(layout: DataLayout, rows: list[dict[str, object]]) -> Path:
    path = _manifest_path(layout)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_manifest(layout)
    incoming = pd.DataFrame(rows)
    combined = pd.concat([existing, incoming], ignore_index=True) if not existing.empty else incoming
    if not combined.empty:
        combined["Date"] = combined["Date"].astype(str)
        combined = combined.sort_values(["Date", "RecordedAtUTC"]).drop_duplicates(
            ["Date"], keep="last"
        )
    combined.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    return path


def _ensure_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            Date DATE,
            Symbol VARCHAR,
            ISIN VARCHAR,
            Series VARCHAR,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume BIGINT,
            Turnover DOUBLE,
            SourceFile VARCHAR,
            SourceMember VARCHAR,
            SourceSha256 VARCHAR,
            IngestedAtUTC TIMESTAMP
        )
        """
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INGEST_TABLE} (
            Date DATE,
            Status VARCHAR,
            SourceURL VARCHAR,
            ZipPath VARCHAR,
            ParquetPath VARCHAR,
            SourceSha256 VARCHAR,
            RawRows BIGINT,
            KeptRows BIGINT,
            Message VARCHAR,
            RecordedAtUTC TIMESTAMP
        )
        """
    )


def append_raw_day_to_master(
    database_path: Path,
    normalized: pd.DataFrame,
    manifest_row: dict[str, object],
) -> int:
    """Idempotently append a normalized raw NSE day into separate live tables."""
    database_path = Path(database_path)
    if not database_path.exists():
        raise FileNotFoundError(f"Rolling master DuckDB not found: {database_path}")
    if normalized.empty:
        return 0

    day = pd.Timestamp(normalized["Date"].iloc[0]).date()
    with duckdb.connect(str(database_path), read_only=False) as connection:
        _ensure_raw_tables(connection)
        connection.register("incoming_day", normalized)
        before = int(
            connection.execute(f"SELECT COUNT(*) FROM {RAW_TABLE} WHERE Date = ?", [day]).fetchone()[0]
        )
        connection.execute(
            f"""
            INSERT INTO {RAW_TABLE}
            SELECT
                i.Date, i.Symbol, i.ISIN, i.Series, i.Open, i.High, i.Low, i.Close,
                i.Volume, i.Turnover, i.SourceFile, i.SourceMember, i.SourceSha256,
                i.IngestedAtUTC
            FROM incoming_day i
            WHERE NOT EXISTS (
                SELECT 1 FROM {RAW_TABLE} e
                WHERE e.Date = i.Date AND e.ISIN = i.ISIN
            )
            """
        )
        after = int(
            connection.execute(f"SELECT COUNT(*) FROM {RAW_TABLE} WHERE Date = ?", [day]).fetchone()[0]
        )
        connection.execute(f"DELETE FROM {INGEST_TABLE} WHERE Date = ?", [day])
        manifest_frame = pd.DataFrame([manifest_row])
        connection.register("manifest_row", manifest_frame)
        connection.execute(
            f"""
            INSERT INTO {INGEST_TABLE}
            SELECT
                CAST(Date AS DATE), Status, SourceURL, ZipPath, ParquetPath,
                SourceSha256, RawRows, KeptRows, Message, CAST(RecordedAtUTC AS TIMESTAMP)
            FROM manifest_row
            """
        )
    return max(0, after - before)


def _date_range(start_date: date, end_date: date) -> list[date]:
    if end_date < start_date:
        return []
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def catch_up_nse_eod(
    config: AppConfig,
    *,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 45,
    request_pause_seconds: float = 0.12,
    minimum_kept_rows: int = 500,
) -> dict[str, object]:
    """Download, validate, archive and stage official NSE CM UDiFF EOD data.

    This phase writes immutable source ZIPs, normalized yearly Parquet day files and a
    separate raw live table in the rolling DuckDB. It does NOT alter `clean_daily` yet.
    That boundary is deliberate until corporate-action adjustment/reconciliation is built.
    """
    layout = DataLayout.from_root(config.environment.root)
    layout.create()
    source_root = layout.incoming_eod / "01 Official UDiFF ZIP"
    normalized_root = layout.incoming_eod / "02 Normalized EQ Parquet"
    report_root = layout.incoming_eod / "03 Daily Validation Reports"
    report_root.mkdir(parents=True, exist_ok=True)

    outcomes: list[DownloadOutcome] = []
    manifest_rows: list[dict[str, object]] = []

    for trading_date in _date_range(start_date, end_date):
        if trading_date.weekday() >= 5:
            continue

        yyyymmdd = trading_date.strftime("%Y%m%d")
        url = official_bhavcopy_url(trading_date)
        zip_path = source_root / str(trading_date.year) / f"BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
        parquet_path = normalized_root / str(trading_date.year) / f"NSE_EQ_{yyyymmdd}.parquet"
        report_path = report_root / str(trading_date.year) / f"NSE_EQ_{yyyymmdd}_validation.json"
        recorded_at = datetime.now(UTC).isoformat()

        try:
            download_status, checksum = _atomic_download(
                url, zip_path, timeout_seconds=timeout_seconds
            )
            if download_status == "NOT_PUBLISHED":
                row = {
                    "Date": trading_date.isoformat(),
                    "Status": "NOT_PUBLISHED",
                    "SourceURL": url,
                    "ZipPath": "",
                    "ParquetPath": "",
                    "SourceSha256": "",
                    "RawRows": 0,
                    "KeptRows": 0,
                    "Message": "No official UDiFF file at this URL (holiday/non-session or unavailable).",
                    "RecordedAtUTC": recorded_at,
                }
                manifest_rows.append(row)
                outcomes.append(
                    DownloadOutcome(
                        trading_date, "NOT_PUBLISHED", url, None, None, None, 0, 0, row["Message"]
                    )
                )
                time.sleep(request_pause_seconds)
                continue

            normalized, validation = normalize_udiff_bhavcopy(
                zip_path, trading_date, minimum_kept_rows=minimum_kept_rows
            )
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            normalized.to_parquet(parquet_path, index=False)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(validation, indent=2), encoding="utf-8")

            row = {
                "Date": trading_date.isoformat(),
                "Status": "VALIDATED",
                "SourceURL": url,
                "ZipPath": str(zip_path),
                "ParquetPath": str(parquet_path),
                "SourceSha256": checksum,
                "RawRows": int(validation["raw_rows"]),
                "KeptRows": int(validation["kept_rows"]),
                "Message": f"{download_status}; official raw preserved; normalized EQ data validated.",
                "RecordedAtUTC": recorded_at,
            }
            inserted = append_raw_day_to_master(config.environment.duckdb_path, normalized, row)
            row["Message"] += f" Raw master rows newly inserted: {inserted}."
            manifest_rows.append(row)
            outcomes.append(
                DownloadOutcome(
                    trading_date,
                    "VALIDATED",
                    url,
                    zip_path,
                    parquet_path,
                    checksum,
                    int(validation["raw_rows"]),
                    int(validation["kept_rows"]),
                    str(row["Message"]),
                )
            )
        except Exception as exc:  # keep a durable per-date failure trail
            row = {
                "Date": trading_date.isoformat(),
                "Status": "ERROR",
                "SourceURL": url,
                "ZipPath": str(zip_path) if zip_path.exists() else "",
                "ParquetPath": str(parquet_path) if parquet_path.exists() else "",
                "SourceSha256": _sha256(zip_path) if zip_path.exists() else "",
                "RawRows": 0,
                "KeptRows": 0,
                "Message": f"{type(exc).__name__}: {exc}",
                "RecordedAtUTC": recorded_at,
            }
            manifest_rows.append(row)
            outcomes.append(
                DownloadOutcome(
                    trading_date,
                    "ERROR",
                    url,
                    zip_path if zip_path.exists() else None,
                    parquet_path if parquet_path.exists() else None,
                    row["SourceSha256"] or None,
                    0,
                    0,
                    row["Message"],
                )
            )
        time.sleep(request_pause_seconds)

    manifest_path = _write_manifest(layout, manifest_rows)
    summary = summarize_live_eod(config)
    summary.update(
        {
            "requested_start": start_date.isoformat(),
            "requested_end": end_date.isoformat(),
            "validated_this_run": sum(item.status == "VALIDATED" for item in outcomes),
            "not_published_this_run": sum(item.status == "NOT_PUBLISHED" for item in outcomes),
            "errors_this_run": sum(item.status == "ERROR" for item in outcomes),
            "manifest_path": str(manifest_path),
            "clean_daily_modified": False,
            "next_boundary": "Corporate-action reconciliation before adjusted clean_daily append.",
        }
    )
    summary_path = layout.logs / "NSE EOD" / "latest_nse_eod_status.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary


def summarize_live_eod(config: AppConfig) -> dict[str, object]:
    database_path = Path(config.environment.duckdb_path)
    result: dict[str, object] = {
        "database": str(database_path),
        "raw_table_exists": False,
        "raw_rows": 0,
        "raw_dates": 0,
        "raw_first_date": None,
        "raw_last_date": None,
        "duplicate_date_isin_groups": 0,
    }
    if not database_path.exists():
        return result

    with duckdb.connect(str(database_path), read_only=True) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if RAW_TABLE not in tables:
            return result
        result["raw_table_exists"] = True
        row = connection.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT Date), MIN(Date), MAX(Date) FROM {RAW_TABLE}"
        ).fetchone()
        result["raw_rows"] = int(row[0])
        result["raw_dates"] = int(row[1])
        result["raw_first_date"] = str(row[2]) if row[2] is not None else None
        result["raw_last_date"] = str(row[3]) if row[3] is not None else None
        dupes = connection.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT Date, ISIN, COUNT(*) AS n
                FROM {RAW_TABLE}
                GROUP BY Date, ISIN
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        result["duplicate_date_isin_groups"] = int(dupes)
    return result
