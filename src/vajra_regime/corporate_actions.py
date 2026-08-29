from __future__ import annotations

import hashlib
import http.cookiejar
import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from vajra_regime.config import AppConfig
from vajra_regime.data_layout import DataLayout
from vajra_regime.master_safety import ensure_mutable_master
from vajra_regime.nse_live import RAW_TABLE


NSE_CA_PAGE_URL = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
NSE_CA_API_URL = "https://www.nseindia.com/api/corporates-corporateActions"
SECURITY_MASTER_TABLE = "vajra_security_master"
SYMBOL_HISTORY_TABLE = "vajra_security_symbol_history"
CORPORATE_ACTION_TABLE = "nse_corporate_actions"
RECONCILIATION_TABLE = "nse_corporate_action_reconciliation"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
)
ACTION_COLUMNS = [
    "EventId",
    "Symbol",
    "Series",
    "CompanyName",
    "Subject",
    "FaceValue",
    "ExDate",
    "RecordDate",
    "RawJson",
]
RECONCILIATION_COLUMNS = [
    "EventId",
    "Symbol",
    "ISIN",
    "CompanyName",
    "Series",
    "Subject",
    "ActionType",
    "ExDate",
    "RecordDate",
    "FaceValue",
    "PriceFactorForPreExHistory",
    "VolumeFactorForPreExHistory",
    "ParseStatus",
    "MatchStatus",
    "PreDate",
    "PreClose",
    "PostDate",
    "PostClose",
    "PostVsAdjustedPreGap",
    "Decision",
    "Note",
]


@dataclass(frozen=True)
class AdjustmentParse:
    action_type: str
    price_factor: float | None
    volume_factor: float | None
    parse_status: str
    note: str


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _event_id(symbol: str, ex_date: object, subject: str) -> str:
    raw = f"{symbol}|{ex_date}|{subject}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()  # noqa: S324 - stable non-security identifier


def _first_value(row: dict[str, Any], aliases: Iterable[str]) -> Any:
    lower = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        if alias.lower() in lower:
            return lower[alias.lower()]
    return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _parse_nse_date(value: Any) -> pd.Timestamp | None:
    text = _clean_text(value)
    if not text or text in {"-", "--"}:
        return None
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _chunk_dates(
    start_date: date,
    end_date: date,
    days: int = 90,
) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(end_date, cursor + timedelta(days=days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _nse_opener() -> urllib.request.OpenerDirector:
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookies)
    )
    warmup = urllib.request.Request(
        NSE_CA_PAGE_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with opener.open(warmup, timeout=60) as response:
        response.read(1024)
    return opener


def _fetch_ca_json(
    opener: urllib.request.OpenerDirector,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 60,
) -> tuple[bytes, list[dict[str, Any]]]:
    query = urllib.parse.urlencode(
        {
            "index": "equities",
            "from_date": start_date.strftime("%d-%m-%Y"),
            "to_date": end_date.strftime("%d-%m-%Y"),
        }
    )
    request = urllib.request.Request(
        f"{NSE_CA_API_URL}?{query}",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_CA_PAGE_URL,
        },
    )
    with opener.open(request, timeout=timeout_seconds) as response:
        payload = response.read()

    decoded = json.loads(payload.decode("utf-8-sig"))
    if isinstance(decoded, dict):
        rows = decoded.get("data", decoded.get("records", []))
    else:
        rows = decoded
    if not isinstance(rows, list):
        raise ValueError("Unexpected NSE corporate-action response shape.")
    return payload, [row for row in rows if isinstance(row, dict)]


def normalize_corporate_action_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        symbol = _clean_text(_first_value(row, ["symbol", "tckrsymb"])).upper()
        series = _clean_text(_first_value(row, ["series", "sctysrs"])).upper()
        subject = _clean_text(
            _first_value(row, ["subject", "purpose", "description"])
        )
        ex_date = _parse_nse_date(
            _first_value(row, ["exDate", "ex-date", "ex_date"])
        )
        if not symbol or not subject or ex_date is None:
            continue

        company = _clean_text(
            _first_value(
                row,
                ["comp", "companyName", "company name", "company"],
            )
        )
        record_date = _parse_nse_date(
            _first_value(
                row,
                ["recDate", "recordDate", "record date", "record_date"],
            )
        )
        face_value = _clean_text(
            _first_value(row, ["faceVal", "faceValue", "face value"])
        )
        normalized.append(
            {
                "EventId": _event_id(symbol, ex_date.date(), subject),
                "Symbol": symbol,
                "Series": series,
                "CompanyName": company,
                "Subject": subject,
                "FaceValue": face_value,
                "ExDate": ex_date,
                "RecordDate": record_date,
                "RawJson": json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    if not normalized:
        return pd.DataFrame(columns=ACTION_COLUMNS)
    frame = pd.DataFrame(normalized, columns=ACTION_COLUMNS)
    return (
        frame.sort_values(["ExDate", "Symbol", "Subject"])
        .drop_duplicates(["EventId"], keep="last")
        .reset_index(drop=True)
    )


def download_corporate_actions(
    config: AppConfig,
    *,
    start_date: date,
    end_date: date,
) -> tuple[pd.DataFrame, dict[str, object]]:
    layout = DataLayout.from_root(config.environment.root)
    source_root = layout.corporate_actions / "01 Official NSE Responses"
    source_root.mkdir(parents=True, exist_ok=True)
    opener = _nse_opener()
    all_rows: list[dict[str, Any]] = []
    archives: list[dict[str, object]] = []

    for chunk_start, chunk_end in _chunk_dates(start_date, end_date):
        payload, rows = _fetch_ca_json(opener, chunk_start, chunk_end)
        year_dir = source_root / str(chunk_start.year)
        year_dir.mkdir(parents=True, exist_ok=True)
        name = (
            f"corporate_actions_{chunk_start:%Y%m%d}_"
            f"{chunk_end:%Y%m%d}.json"
        )
        path = year_dir / name
        path.write_bytes(payload)
        archives.append(
            {
                "start": chunk_start.isoformat(),
                "end": chunk_end.isoformat(),
                "path": str(path),
                "sha256": _sha256_bytes(payload),
                "rows": len(rows),
            }
        )
        all_rows.extend(rows)

    normalized = normalize_corporate_action_rows(all_rows)
    report = {
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "normalized_events": int(len(normalized)),
        "archives": archives,
    }
    return normalized, report


def classify_adjustment(subject: str) -> AdjustmentParse:
    upper = subject.upper().replace("–", "-").replace("—", "-")
    if "DEMERGER" in upper or "DE-MERGER" in upper:
        return AdjustmentParse(
            "DEMERGER",
            None,
            None,
            "REVIEW",
            "Demerger needs event-specific review.",
        )
    if "MERGER" in upper or "AMALGAM" in upper:
        return AdjustmentParse(
            "MERGER",
            None,
            None,
            "REVIEW",
            "Merger/amalgamation needs event-specific review.",
        )
    if "RIGHT" in upper:
        return AdjustmentParse(
            "RIGHTS",
            None,
            None,
            "REVIEW",
            "Rights issue is not auto-adjusted.",
        )
    if "BUY BACK" in upper or "BUYBACK" in upper:
        return AdjustmentParse(
            "BUYBACK",
            1.0,
            1.0,
            "NO_ADJUST",
            "Buyback does not mechanically rescale OHLCV history.",
        )
    if "DIVIDEND" in upper or "DISTRIBUTION" in upper:
        return AdjustmentParse(
            "DIVIDEND",
            1.0,
            1.0,
            "NO_ADJUST",
            "Price history is not converted to a dividend total-return series.",
        )
    if "BONUS" in upper:
        match = re.search(
            r"BONUS[^0-9]*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)",
            upper,
        )
        if not match:
            return AdjustmentParse(
                "BONUS",
                None,
                None,
                "REVIEW",
                "Bonus ratio could not be parsed safely.",
            )
        new_shares = float(match.group(1))
        old_shares = float(match.group(2))
        if new_shares <= 0 or old_shares <= 0:
            return AdjustmentParse(
                "BONUS",
                None,
                None,
                "REVIEW",
                "Bonus ratio is invalid.",
            )
        price_factor = old_shares / (old_shares + new_shares)
        return AdjustmentParse(
            "BONUS",
            price_factor,
            1.0 / price_factor,
            "PARSED",
            f"Bonus {new_shares:g}:{old_shares:g}.",
        )
    if "SPLIT" in upper or "SUB-DIVISION" in upper or "SUB DIVISION" in upper:
        match = re.search(
            r"FROM[^0-9]*(\d+(?:\.\d+)?).*?TO[^0-9]*(\d+(?:\.\d+)?)",
            upper,
        )
        if not match:
            return AdjustmentParse(
                "SPLIT",
                None,
                None,
                "REVIEW",
                "Split face-value change could not be parsed safely.",
            )
        old_face = float(match.group(1))
        new_face = float(match.group(2))
        if old_face <= 0 or new_face <= 0 or new_face >= old_face:
            return AdjustmentParse(
                "SPLIT",
                None,
                None,
                "REVIEW",
                "Split ratio is invalid or looks like consolidation.",
            )
        return AdjustmentParse(
            "SPLIT",
            new_face / old_face,
            old_face / new_face,
            "PARSED",
            f"Face value {old_face:g} -> {new_face:g}.",
        )
    return AdjustmentParse(
        "OTHER",
        None,
        None,
        "REVIEW",
        "Unclassified corporate action.",
    )


def _table_exists(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> bool:
    tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    return table in tables


def build_security_master(config: AppConfig) -> dict[str, object]:
    database = Path(config.environment.duckdb_path)
    clean_table = str(config.data["clean_table"])
    updated_at = datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")

    with duckdb.connect(str(database), read_only=False) as connection:
        if not _table_exists(connection, clean_table):
            raise ValueError(f"Missing historical table: {clean_table}")

        sources = [
            (
                "SELECT Date, UPPER(Symbol) AS Symbol, UPPER(ISIN) AS ISIN, "
                f"NULL::VARCHAR AS Series FROM {clean_table}"
            )
        ]
        if _table_exists(connection, RAW_TABLE):
            sources.append(
                (
                    "SELECT Date, UPPER(Symbol) AS Symbol, UPPER(ISIN) AS ISIN, "
                    f"UPPER(Series) AS Series FROM {RAW_TABLE}"
                )
            )
        union_sql = " UNION ALL ".join(sources)

        connection.execute(f"DROP TABLE IF EXISTS {SYMBOL_HISTORY_TABLE}")
        connection.execute(
            f"""
            CREATE TABLE {SYMBOL_HISTORY_TABLE} AS
            WITH all_rows AS ({union_sql})
            SELECT
                ISIN,
                Symbol,
                MIN(Date) AS FirstDate,
                MAX(Date) AS LastDate,
                COUNT(*) AS Observations,
                ARG_MAX(Series, Date) AS LatestSeries
            FROM all_rows
            WHERE ISIN LIKE 'INE%'
              AND Symbol IS NOT NULL
              AND Symbol <> ''
            GROUP BY ISIN, Symbol
            ORDER BY ISIN, FirstDate, Symbol
            """
        )

        connection.execute(f"DROP TABLE IF EXISTS {SECURITY_MASTER_TABLE}")
        connection.execute(
            f"""
            CREATE TABLE {SECURITY_MASTER_TABLE} AS
            WITH all_rows AS ({union_sql}),
            grouped AS (
                SELECT
                    ISIN,
                    MIN(Date) AS FirstDate,
                    MAX(Date) AS LastDate,
                    ARG_MAX(Symbol, Date) AS LatestSymbol,
                    ARG_MAX(Series, Date)
                        FILTER (WHERE Series IS NOT NULL) AS LatestSeries,
                    COUNT(DISTINCT Symbol) AS SymbolCount,
                    LIST_SORT(LIST(DISTINCT Symbol)) AS Symbols
                FROM all_rows
                WHERE ISIN LIKE 'INE%'
                  AND Symbol IS NOT NULL
                  AND Symbol <> ''
                GROUP BY ISIN
            )
            SELECT
                ISIN,
                FirstDate,
                LastDate,
                LatestSymbol,
                COALESCE(LatestSeries, 'EQ') AS LatestSeries,
                SymbolCount,
                TO_JSON(Symbols) AS SymbolsJson,
                TIMESTAMP '{updated_at}' AS UpdatedAtUTC
            FROM grouped
            ORDER BY ISIN
            """
        )

        security_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {SECURITY_MASTER_TABLE}"
            ).fetchone()[0]
        )
        symbol_rows = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {SYMBOL_HISTORY_TABLE}"
            ).fetchone()[0]
        )

    return {
        "security_count": security_count,
        "symbol_history_rows": symbol_rows,
    }


def _load_symbol_history(config: AppConfig) -> pd.DataFrame:
    with duckdb.connect(
        str(config.environment.duckdb_path),
        read_only=True,
    ) as connection:
        return connection.execute(
            f"""
            SELECT ISIN, Symbol, FirstDate, LastDate, LatestSeries
            FROM {SYMBOL_HISTORY_TABLE}
            """
        ).df()


def _load_live_prices(config: AppConfig) -> pd.DataFrame:
    with duckdb.connect(
        str(config.environment.duckdb_path),
        read_only=True,
    ) as connection:
        if not _table_exists(connection, RAW_TABLE):
            return pd.DataFrame(
                columns=["Date", "ISIN", "Symbol", "Close"]
            )
        return connection.execute(
            f"""
            SELECT
                Date,
                UPPER(ISIN) AS ISIN,
                UPPER(Symbol) AS Symbol,
                Close
            FROM {RAW_TABLE}
            ORDER BY Date, ISIN
            """
        ).df()


def _resolve_isin(
    symbol: str,
    ex_date: pd.Timestamp,
    history: pd.DataFrame,
) -> tuple[str, str]:
    candidates = history.loc[history["Symbol"].eq(symbol)].copy()
    overlap = candidates.loc[
        candidates["FirstDate"].le(ex_date + pd.Timedelta(days=7))
        & candidates["LastDate"].ge(ex_date - pd.Timedelta(days=30))
    ]
    if not overlap.empty:
        candidates = overlap

    if len(candidates) == 1:
        return str(candidates.iloc[0]["ISIN"]).upper(), "MATCHED_UNIQUE_ISIN"
    if len(candidates) > 1:
        return "", "AMBIGUOUS_SYMBOL"
    return "", "UNMATCHED_SYMBOL"


def _price_bridge(
    isin: str,
    ex_date: pd.Timestamp,
    prices: pd.DataFrame,
    price_factor: float | None,
) -> tuple[object, float | None, object, float | None, float | None]:
    if not isin or prices.empty:
        return None, None, None, None, None

    panel = prices.loc[prices["ISIN"].eq(isin)].sort_values("Date")
    pre = panel.loc[panel["Date"].lt(ex_date)].tail(1)
    post = panel.loc[panel["Date"].ge(ex_date)].head(1)

    pre_date: object = None
    pre_close: float | None = None
    post_date: object = None
    post_close: float | None = None
    adjusted_gap: float | None = None

    if not pre.empty:
        pre_date = pre.iloc[0]["Date"]
        pre_close = float(pre.iloc[0]["Close"])
    if not post.empty:
        post_date = post.iloc[0]["Date"]
        post_close = float(post.iloc[0]["Close"])
    if (
        pre_close is not None
        and post_close is not None
        and price_factor is not None
        and price_factor > 0
    ):
        adjusted_gap = post_close / (pre_close * price_factor) - 1.0

    return pre_date, pre_close, post_date, post_close, adjusted_gap


def _decision_for_event(
    parsed: AdjustmentParse,
    match_status: str,
    adjusted_gap: float | None,
) -> str:
    if parsed.parse_status == "NO_ADJUST":
        return "INFORMATIONAL_NO_PRICE_ADJUSTMENT"
    if parsed.action_type not in {"SPLIT", "BONUS"}:
        return "REVIEW_COMPLEX_OR_UNPARSED"
    if parsed.parse_status != "PARSED":
        return "REVIEW_COMPLEX_OR_UNPARSED"
    if match_status != "MATCHED_UNIQUE_ISIN":
        return "REVIEW_IDENTITY"
    if adjusted_gap is None:
        return "REVIEW_MISSING_PRICE_BRIDGE"
    if abs(adjusted_gap) <= 0.35:
        return "AUTO_READY_SPLIT_BONUS"
    return "REVIEW_PRICE_BRIDGE"


def reconcile_corporate_actions(
    actions: pd.DataFrame,
    symbol_history: pd.DataFrame,
    live_prices: pd.DataFrame,
) -> pd.DataFrame:
    if actions.empty:
        return pd.DataFrame(columns=RECONCILIATION_COLUMNS)

    history = symbol_history.copy()
    history["Symbol"] = history["Symbol"].astype(str).str.upper()
    history["FirstDate"] = pd.to_datetime(history["FirstDate"])
    history["LastDate"] = pd.to_datetime(history["LastDate"])

    prices = live_prices.copy()
    if not prices.empty:
        prices["Date"] = pd.to_datetime(prices["Date"])
        prices["ISIN"] = prices["ISIN"].astype(str).str.upper()

    rows: list[dict[str, object]] = []
    for event in actions.itertuples(index=False):
        symbol = str(event.Symbol).upper()
        ex_date = pd.Timestamp(event.ExDate).normalize()
        isin, match_status = _resolve_isin(symbol, ex_date, history)
        parsed = classify_adjustment(str(event.Subject))
        bridge = _price_bridge(
            isin,
            ex_date,
            prices,
            parsed.price_factor,
        )
        pre_date, pre_close, post_date, post_close, adjusted_gap = bridge
        decision = _decision_for_event(parsed, match_status, adjusted_gap)

        rows.append(
            {
                "EventId": event.EventId,
                "Symbol": symbol,
                "ISIN": isin,
                "CompanyName": event.CompanyName,
                "Series": event.Series,
                "Subject": event.Subject,
                "ActionType": parsed.action_type,
                "ExDate": ex_date,
                "RecordDate": event.RecordDate,
                "FaceValue": event.FaceValue,
                "PriceFactorForPreExHistory": parsed.price_factor,
                "VolumeFactorForPreExHistory": parsed.volume_factor,
                "ParseStatus": parsed.parse_status,
                "MatchStatus": match_status,
                "PreDate": pre_date,
                "PreClose": pre_close,
                "PostDate": post_date,
                "PostClose": post_close,
                "PostVsAdjustedPreGap": adjusted_gap,
                "Decision": decision,
                "Note": parsed.note,
            }
        )

    frame = pd.DataFrame(rows, columns=RECONCILIATION_COLUMNS)
    return frame.sort_values(
        ["ExDate", "Symbol", "Subject"]
    ).reset_index(drop=True)


def _replace_table(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    frame: pd.DataFrame,
) -> None:
    connection.register("incoming_frame", frame)
    try:
        connection.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming_frame"
        )
    finally:
        connection.unregister("incoming_frame")


def _decision_count(reconciliation: pd.DataFrame, decision: str) -> int:
    if reconciliation.empty:
        return 0
    return int(reconciliation["Decision"].eq(decision).sum())


def run_corporate_action_audit(
    config: AppConfig,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    safety = ensure_mutable_master(config)
    security = build_security_master(config)
    actions, download_report = download_corporate_actions(
        config,
        start_date=start_date,
        end_date=end_date,
    )
    symbol_history = _load_symbol_history(config)
    live_prices = _load_live_prices(config)
    reconciliation = reconcile_corporate_actions(
        actions,
        symbol_history,
        live_prices,
    )

    layout = DataLayout.from_root(config.environment.root)
    output_root = layout.corporate_actions / "02 Reconciliation"
    output_root.mkdir(parents=True, exist_ok=True)

    actions_csv = output_root / "nse_corporate_actions_2026.csv"
    reconciliation_csv = output_root / "corporate_action_reconciliation_2026.csv"
    review_csv = output_root / "corporate_action_review_required_2026.csv"
    security_csv = output_root / "security_master_2026.csv"

    actions.to_csv(actions_csv, index=False)
    reconciliation.to_csv(reconciliation_csv, index=False)
    review = reconciliation.loc[
        reconciliation["Decision"].astype(str).str.startswith("REVIEW")
    ]
    review.to_csv(review_csv, index=False)

    with duckdb.connect(
        str(config.environment.duckdb_path),
        read_only=False,
    ) as connection:
        _replace_table(connection, CORPORATE_ACTION_TABLE, actions)
        _replace_table(connection, RECONCILIATION_TABLE, reconciliation)
        security_frame = connection.execute(
            f"SELECT * FROM {SECURITY_MASTER_TABLE}"
        ).df()
    security_frame.to_csv(security_csv, index=False)

    if reconciliation.empty:
        decisions: dict[str, int] = {}
        matched_unique = 0
        review_required = 0
    else:
        decisions = {
            str(key): int(value)
            for key, value in reconciliation["Decision"].value_counts().items()
        }
        matched_unique = int(
            reconciliation["MatchStatus"].eq("MATCHED_UNIQUE_ISIN").sum()
        )
        review_required = int(
            reconciliation["Decision"].astype(str).str.startswith("REVIEW").sum()
        )

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "master_safety": safety,
        "security_master": security,
        "download": download_report,
        "events": int(len(actions)),
        "reconciled_rows": int(len(reconciliation)),
        "matched_unique_isin": matched_unique,
        "auto_ready_split_bonus": _decision_count(
            reconciliation,
            "AUTO_READY_SPLIT_BONUS",
        ),
        "review_required": review_required,
        "informational_no_adjustment": _decision_count(
            reconciliation,
            "INFORMATIONAL_NO_PRICE_ADJUSTMENT",
        ),
        "decision_counts": decisions,
        "clean_daily_modified": False,
        "historical_parquet_modified": False,
        "next_boundary": (
            "Review the reconciliation. Only verified split/bonus events may be "
            "applied to pre-ex history before adjusted 2026 rows are appended."
        ),
        "actions_csv": str(actions_csv),
        "reconciliation_csv": str(reconciliation_csv),
        "review_csv": str(review_csv),
        "security_master_csv": str(security_csv),
    }

    summary_path = output_root / "latest_corporate_action_audit.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, default=str),
        encoding="utf-8",
    )
    summary["summary_path"] = str(summary_path)
    return summary
