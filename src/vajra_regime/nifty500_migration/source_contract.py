"""NSE SOURCE CONTRACT + CHANGE DETECTOR (05-Oct-2026).

Kyun bana: 29-Sep-2026 ko NSE ne Nifty 500 me 3 REIT (EMBASSY, BIRET, BAGMANE; series "RR") jode.
Laptop catch-up unhe "current-member equity bar gayab" samjha aur 6 din chup-chaap ruka raha.
Sabak: naya series / naya asset type ek *badlaav* hai, aam "missing bar" nahi -- use alag pehchano.

Is file ke niyam:
  * OFFICIAL MEMBERSHIP (PIT truth) kabhi nahi badalti. Yahan sirf VAJRA *equity eligibility* tay hoti hai.
  * Series registry: EQUITY = EQ/BE/BZ (wahi jo raw_ohlcv.parse_official_bhavcopy padhta hai).
    NON_EQUITY = saabit non-equity (RR = REIT, IV = InvIT). Inke liye equity bar na hona galti nahi --
    wo "official member, VAJRA equity-ineligible" ke roop me alag gine aur logged hote hain.
    UNKNOWN series = kabhi chup-chaap accept ya skip nahi: fail closed (owner/AI review).
  * Bhavcopy contract: file ki ANDAR ki tareekh = maangi gayi tareekh, zaroori columns (naam se), equity
    rows ki samajhdaar ginti, duplicate symbol nahi, close > 0. Header naam badle par matlab wahi ho to
    alias se padho aur drift log karo; zaroori column gayab = fail.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

EQUITY_SERIES = frozenset({"EQ", "BE", "BZ"})
NON_EQUITY_SERIES: dict[str, str] = {"RR": "REIT", "IV": "INVIT"}
EXCLUSION_REASON = {"REIT": "NON_EQUITY_SERIES_REIT", "INVIT": "NON_EQUITY_SERIES_INVIT"}

# bhavcopy column aliases (legacy cm*bhav.csv, UDiFF) -- name-based, order-independent
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "symbol": ("SYMBOL", "TckrSymb"),
    "series": ("SERIES", "SctySrs"),
    "isin": ("ISIN",),
    "open": ("OPEN", "OpnPric"),
    "high": ("HIGH", "HghPric"),
    "low": ("LOW", "LwPric"),
    "close": ("CLOSE", "ClsPric"),
    "volume": ("TOTTRDQTY", "TtlTradgVol"),
}
DATE_COLUMNS = ("TradDt", "TIMESTAMP", "BizDt")
MIN_EQUITY_ROWS = 500          # NSE has ~2,000+ EQ/BE/BZ rows a day; < 500 = truncated / wrong file
MAX_BAD_PRICE_SHARE = 0.01


class SourceContractError(RuntimeError):
    """A source no longer matches its contract. Message starts with a stable error class."""


# ------------------------------------------------------------------------------------------ members
def classify_official_members(members: list[dict[str, str]]) -> dict[str, Any]:
    """Split the official constituent list into equity-eligible / known non-equity / unknown-series."""
    equity, non_equity, unknown = [], [], []
    for row in members:
        symbol = str(row.get("Symbol", "")).strip().upper()
        series = str(row.get("Series", "")).strip().upper()
        isin = str(row.get("ISIN Code", "")).strip()
        item = {"Symbol": symbol, "ISIN": isin, "Series": series,
                "CompanyName": str(row.get("Company Name", "")).strip()}
        if series in EQUITY_SERIES:
            equity.append(item)
        elif series in NON_EQUITY_SERIES:
            asset = NON_EQUITY_SERIES[series]
            non_equity.append({**item, "AssetType": asset, "OfficialMember": True,
                               "VajraEquityEligible": False, "ExclusionReason": EXCLUSION_REASON[asset]})
        else:
            unknown.append(item)
    return {"official": len(members), "equity": equity, "non_equity": non_equity, "unknown": unknown}


def membership_diff(prior: list[dict[str, str]] | None, current: list[dict[str, str]]) -> dict[str, Any]:
    """Yesterday vs today official list: additions, removals, series / ISIN changes, category changes."""
    def index(rows):
        return {str(r.get("Symbol", "")).strip().upper(): r for r in rows or []}
    a, b = index(prior), index(current)
    added = sorted(set(b) - set(a))
    removed = sorted(set(a) - set(b))
    series_changed, isin_changed = [], []
    for symbol in sorted(set(a) & set(b)):
        sa, sb = str(a[symbol].get("Series", "")).upper(), str(b[symbol].get("Series", "")).upper()
        ia, ib = str(a[symbol].get("ISIN Code", "")), str(b[symbol].get("ISIN Code", ""))
        if sa != sb:
            series_changed.append({"Symbol": symbol, "from": sa, "to": sb})
        if ia != ib:
            isin_changed.append({"Symbol": symbol, "from": ia, "to": ib})
    category = []
    for symbol in added:
        series = str(b[symbol].get("Series", "")).upper()
        if series not in EQUITY_SERIES:
            category.append({"Symbol": symbol, "Series": series,
                             "class": NON_EQUITY_SERIES.get(series, "UNKNOWN")})
    for change in series_changed:
        if change["to"] not in EQUITY_SERIES:
            category.append({"Symbol": change["Symbol"], "Series": change["to"],
                             "class": NON_EQUITY_SERIES.get(change["to"], "UNKNOWN")})
    return {"prior_available": prior is not None, "added": added, "removed": removed,
            "series_changed": series_changed, "isin_changed": isin_changed,
            "asset_category_changes": category,
            "review_needed": any(c["class"] == "UNKNOWN" for c in category)}


# ------------------------------------------------------------------------------------------ bhavcopy
def _decode(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise SourceContractError("SOURCE_UNDECODABLE: no supported text encoding")


def _parse_date(text: str) -> date | None:
    text = (text or "").strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d%b%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def check_bhavcopy(zip_path: Path, session: date, *, known_headers: list[str] | None = None) -> dict[str, Any]:
    """Pre-ingest canary for one official NSE CM bhavcopy. Raises SourceContractError on a broken contract."""
    path = Path(zip_path)
    if not path.exists() or path.stat().st_size == 0:
        raise SourceContractError(f"SOURCE_EMPTY: {path.name} missing or zero bytes")
    try:
        with zipfile.ZipFile(path) as bundle:
            names = [n for n in bundle.namelist() if n.casefold().endswith(".csv")]
            if len(names) != 1:
                raise SourceContractError(f"SOURCE_LAYOUT: expected one CSV in {path.name}, found {names}")
            payload = bundle.read(names[0])
    except zipfile.BadZipFile as exc:
        raise SourceContractError(f"SOURCE_CORRUPT: {path.name} is not a valid zip ({exc})") from exc
    text = _decode(payload)
    if text.lstrip().startswith("<"):
        raise SourceContractError(f"SOURCE_HTML: {path.name} contains HTML, not CSV")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    resolved, missing = {}, []
    for key, aliases in REQUIRED_COLUMNS.items():
        hit = next((a for a in aliases if a in headers), None)
        (resolved.__setitem__(key, hit) if hit else missing.append(key))
    if missing:
        raise SourceContractError(f"SCHEMA_REQUIRED_COLUMN_MISSING: {missing} in {path.name}; headers={headers}")
    date_col = next((c for c in DATE_COLUMNS if c in headers), None)
    drift = None
    if known_headers is not None and known_headers != headers:
        drift = {"added": [h for h in headers if h not in known_headers],
                 "removed": [h for h in known_headers if h not in headers],
                 "reordered": sorted(headers) == sorted(known_headers)}
    rows = list(reader)
    if not rows:
        raise SourceContractError(f"SOURCE_EMPTY_ROWS: {path.name} has a header but no rows")
    internal_dates = set()
    series_counts: dict[str, int] = {}
    equity_seen: dict[tuple[str, str], int] = {}
    bad_price = equity_rows = 0
    for row in rows:
        series = (row.get(resolved["series"]) or "").strip().upper()
        series_counts[series] = series_counts.get(series, 0) + 1
        if date_col:
            parsed = _parse_date(row.get(date_col, ""))
            if parsed:
                internal_dates.add(parsed)
        if series in EQUITY_SERIES:
            equity_rows += 1
            key = ((row.get(resolved["symbol"]) or "").strip().upper(), series)
            equity_seen[key] = equity_seen.get(key, 0) + 1
            try:
                if not (float(row.get(resolved["close"]) or 0) > 0):
                    bad_price += 1
            except ValueError:
                bad_price += 1
    if date_col:
        if not internal_dates:
            raise SourceContractError(f"SOURCE_DATE_UNREADABLE: column {date_col} in {path.name}")
        if internal_dates != {session}:
            raise SourceContractError(
                f"SOURCE_WRONG_DATE: requested {session}, file says {sorted(str(d) for d in internal_dates)} ({path.name})")
    if equity_rows < MIN_EQUITY_ROWS:
        raise SourceContractError(f"SOURCE_TRUNCATED: only {equity_rows} EQ/BE/BZ rows in {path.name}")
    duplicates = sorted(f"{s}/{r}" for (s, r), n in equity_seen.items() if n > 1)
    if duplicates:
        raise SourceContractError(f"SOURCE_DUPLICATE_SYMBOL: {duplicates[:10]} in {path.name}")
    if bad_price / equity_rows > MAX_BAD_PRICE_SHARE:
        raise SourceContractError(f"SOURCE_BAD_PRICES: {bad_price}/{equity_rows} equity closes <= 0 in {path.name}")
    return {"file": path.name, "session": session.isoformat(), "date_column": date_col,
            "internal_date_checked": bool(date_col), "headers": headers, "schema_drift": drift,
            "equity_rows": equity_rows, "series_counts": dict(sorted(series_counts.items())),
            "bad_equity_prices": bad_price, "status": "PASS"}


def raw_series_facts(zip_path: Path, symbols: set[str]) -> dict[str, dict[str, str]]:
    """Raw facts for named symbols in ANY series (e.g. REIT units in RR) -- preserved, never used as equity bars."""
    with zipfile.ZipFile(zip_path) as bundle:
        name = next(n for n in bundle.namelist() if n.casefold().endswith(".csv"))
        text = _decode(bundle.read(name))
    out: dict[str, dict[str, str]] = {}
    for row in csv.DictReader(io.StringIO(text)):
        symbol = (row.get("TckrSymb") or row.get("SYMBOL") or "").strip().upper()
        if symbol in symbols:
            out[symbol] = {"Series": (row.get("SctySrs") or row.get("SERIES") or "").strip().upper(),
                           "ISIN": (row.get("ISIN") or "").strip(),
                           "Close": (row.get("ClsPric") or row.get("CLOSE") or "").strip()}
    return out


# ------------------------------------------------------------------------------------------ schema memory
def load_schema_fingerprint(path: Path, source: str) -> list[str] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get(source, {}).get("headers")
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_schema_fingerprint(path: Path, source: str, headers: list[str], drift: dict | None) -> None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    entry = data.get(source, {})
    history = entry.get("drift_history", [])
    if drift:
        history = (history + [{"at_utc": datetime.now(UTC).isoformat(timespec="seconds"), **drift}])[-20:]
    data[source] = {"headers": headers, "updated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                    "drift_history": history}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1), encoding="utf-8")
    tmp.replace(path)
