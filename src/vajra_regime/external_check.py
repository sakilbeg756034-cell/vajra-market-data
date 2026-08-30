"""Cross-check the published prices against an independent free source.

Why bother: every other check in `quality.py` compares the dataset against itself or against
the NSE corporate-action ledger it was built from. A systematic error in the source feed - a
wrong adjustment convention, a stale mapping, a shifted date - would pass all of them. Only an
outside opinion catches that.

Sources are tried in order and the first that answers for a symbol is used. None of them are
authoritative; the point is agreement, not truth. Where they disagree with us the disagreement
is reported with its size rather than resolved.

The comparison is on **daily returns**, not price levels. Two providers can hold the same
series on different adjustment bases - one back-adjusted to today, one to some earlier date -
and their levels will differ by a constant factor while their returns match exactly. Comparing
levels would flag that as an error when nothing is wrong; comparing returns catches the thing
that actually matters, which is whether the shape of the series agrees.
"""

from __future__ import annotations

import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json

CHECK_VERSION = "VAJRA_EXTERNAL_CROSSCHECK_V1"

# Large, liquid, long-listed names across sectors, plus the two the engine had to repair.
DEFAULT_SYMBOLS = (
    "RELIANCE",
    "TCS",
    "HDFCBANK",
    "INFY",
    "ITC",
    "LT",
    "SBIN",
    "MARUTI",
    "SUNPHARMA",
    "HINDUNILVR",
    "AXISBANK",
    "KOTAKBANK",
    "TATAMOTORS",  # renamed in the 2025 demerger; often unavailable externally
    "BHARTIARTL",  # the 2009 split the engine repaired
    "ALLCARGO",  # the other one
)

# Two providers agreeing to within this on a median daily return are telling the same story.
RETURN_TOLERANCE = 0.005  # 0.5 percentage points
# Below this share of sessions matching closely, something is structurally different.
MIN_AGREEMENT_FRACTION = 0.95


def _sql(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _our_returns(
    con: duckdb.DuckDBPyConnection, root: Path, symbol: str, start: date, end: date
) -> dict[date, float]:
    """Returns for one symbol, computed strictly inside one ISIN.

    146 symbols in this dataset carry more than one ISIN over time, because a face-value
    change issues a new one. Lagging by symbol alone therefore computes a return *across* an
    identity boundary and invents a move - it reported ADANIPOWER at +428% on 2023-03-31,
    which is an artefact of this query, not of the data. The identity with the most sessions
    in the window is the one the outside source will be carrying.

    Only research-eligible rows are compared, because those are the only rows a backtest
    uses. Comparing the excluded ones measures data the dataset itself says not to trust, and
    produces a disagreement that means nothing.
    """
    glob = _sql(root / "nifty500" / "parquet" / "nifty500_*.parquet")
    isin_row = con.execute(
        f"""
        SELECT ISIN FROM read_parquet('{glob}')
        WHERE Symbol = ? AND Date BETWEEN ? AND ? AND ISIN IS NOT NULL
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1
        """,
        [symbol, start, end],
    ).fetchone()
    if not isin_row:
        return {}
    rows = con.execute(
        f"""
        SELECT Date, Close, LAG(Close) OVER (ORDER BY Date) AS Prev
        FROM read_parquet('{glob}')
        WHERE ISIN = ? AND Date BETWEEN ? AND ? AND IsResearchEligible
        ORDER BY Date
        """,
        [isin_row[0], start, end],
    ).fetchall()
    return {
        r[0]: r[1] / r[2] - 1.0
        for r in rows
        if r[2] not in (None, 0) and r[1] is not None
    }


def _yfinance_returns(symbol: str, start: date, end: date) -> dict[date, float] | None:
    """Try NSE, then BSE. A name that has been renamed or demerged often survives on only one
    of the two, and giving up after the first miss would quietly shrink the sample."""
    try:
        import yfinance  # noqa: PLC0415
    except ImportError:
        return None
    for suffix in (".NS", ".BO"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                frame = yfinance.Ticker(f"{symbol}{suffix}").history(
                    start=start.isoformat(), end=end.isoformat(), auto_adjust=True
                )
            except Exception:  # noqa: BLE001
                continue
        if frame is None or frame.empty or "Close" not in frame:
            continue
        closes = frame["Close"].dropna()
        if len(closes) < 30:
            continue
        returns = closes.pct_change().dropna()
        return {idx.date(): float(value) for idx, value in returns.items()}
    return None


def _stooq_returns(symbol: str, start: date, end: date) -> dict[date, float] | None:
    import csv  # noqa: PLC0415
    import io  # noqa: PLC0415
    import urllib.error  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    url = (
        f"https://stooq.com/q/d/l/?s={symbol.lower()}.in"
        f"&d1={start.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}&i=d"
    )
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:  # noqa: S310
            payload = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError):
        return None
    if "Date" not in payload.splitlines()[0] if payload else True:
        return None
    closes: dict[date, float] = {}
    for row in csv.DictReader(io.StringIO(payload)):
        try:
            closes[date.fromisoformat(row["Date"])] = float(row["Close"])
        except (KeyError, ValueError):
            continue
    if len(closes) < 30:
        return None
    ordered = sorted(closes)
    return {
        ordered[i]: closes[ordered[i]] / closes[ordered[i - 1]] - 1.0
        for i in range(1, len(ordered))
        if closes[ordered[i - 1]]
    }


SOURCES = (("yfinance", _yfinance_returns), ("stooq", _stooq_returns))


def compare_symbol(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    symbol: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    ours = _our_returns(con, root, symbol, start, end)
    if not ours:
        return {"symbol": symbol, "verdict": "NOT_IN_DATASET", "source": None}

    attempts: list[dict[str, Any]] = []
    for name, fetch in SOURCES:
        theirs = fetch(symbol, start, end)
        if not theirs:
            attempts.append({"source": name, "outcome": "NO_DATA"})
            continue
        shared = sorted(set(ours) & set(theirs))
        if len(shared) < 30:
            attempts.append(
                {"source": name, "outcome": "TOO_FEW_OVERLAPPING_SESSIONS", "overlap": len(shared)}
            )
            continue
        differences = [abs(ours[d] - theirs[d]) for d in shared]
        differences.sort()
        median = differences[len(differences) // 2]
        close_enough = sum(1 for x in differences if x <= RETURN_TOLERANCE)
        agreement = close_enough / len(differences)
        worst = sorted(
            ({"date": str(d), "ours": round(ours[d], 5), "theirs": round(theirs[d], 5)}
             for d in shared),
            key=lambda row: abs(row["ours"] - row["theirs"]),
            reverse=True,
        )[:5]
        return {
            "symbol": symbol,
            "source": name,
            "overlapping_sessions": len(shared),
            "first_session": str(shared[0]),
            "last_session": str(shared[-1]),
            "median_abs_return_difference": round(median, 6),
            "sessions_agreeing_within_tolerance": round(agreement, 4),
            "largest_disagreements": worst,
            "verdict": "AGREES"
            if median <= RETURN_TOLERANCE and agreement >= MIN_AGREEMENT_FRACTION
            else "DISAGREES",
            "earlier_attempts": attempts,
        }
    return {"symbol": symbol, "verdict": "NO_EXTERNAL_SOURCE_ANSWERED", "attempts": attempts}


def run_external_crosscheck(
    *,
    root: Path | None = None,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    start: date = date(2010, 1, 1),
    end: date | None = None,
) -> dict[str, Any]:
    root = Path(root) if root else paths.DATA_ROOT
    end = end or date.today()
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")

    results = [compare_symbol(con, root, s, start, end) for s in symbols]
    agreeing = [r for r in results if r["verdict"] == "AGREES"]
    disagreeing = [r for r in results if r["verdict"] == "DISAGREES"]
    unavailable = [r for r in results if r["verdict"] not in {"AGREES", "DISAGREES"}]

    report = {
        "version": CHECK_VERSION,
        "status": "RUN",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "method": (
            "Daily returns, not price levels. Two providers can hold the same series on "
            "different adjustment bases, which shifts every level by a constant factor while "
            "leaving returns identical. Comparing returns tests the thing that matters."
        ),
        "tolerance": {
            "per_session_return_difference": RETURN_TOLERANCE,
            "minimum_fraction_of_sessions_within_tolerance": MIN_AGREEMENT_FRACTION,
        },
        "symbols_checked": len(results),
        "agreeing": len(agreeing),
        "disagreeing": len(disagreeing),
        "no_external_data": len(unavailable),
        "symbols": results,
    }
    report["summary"] = (
        f"{len(agreeing)} of {len(results)} sampled securities agree with an independent free "
        f"source on daily returns; {len(disagreeing)} disagree; {len(unavailable)} had no "
        "usable external data."
    )
    atomic_json(paths.LOGS_ROOT / "quality" / "external_crosscheck.json", report)
    return report


__all__ = ["DEFAULT_SYMBOLS", "compare_symbol", "run_external_crosscheck"]
