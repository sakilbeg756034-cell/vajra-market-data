"""Re-verify the published prices against NSE's own bhavcopy - the source of truth itself.

Every other check compares this dataset to a third party. Third parties disagree for reasons
that have nothing to do with correctness: Yahoo carries the *last traded* price where NSE
publishes an official *closing* price (a weighted average of the final half hour), so on a
volatile day the two differ by tenths of a percent and a naive comparison calls that an error.
That is exactly what happened with ADANIPOWER, where Yahoo disagreed on 6.5% of sessions and
the exchange's own file matched us to the paisa on every one of them.

So this check goes to the exchange. It picks sessions at random across the whole history,
re-downloads the original bhavcopy NSE published that day, and compares every raw value in
the dataset against it. There is no higher authority to appeal to.

It verifies the RAW columns, not the adjusted ones - the adjusted series is a deliberate
transformation of the raw, and it is the raw that has to match the exchange.
"""

from __future__ import annotations

import csv
import io
import random
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json

VERIFY_VERSION = "VAJRA_SOURCE_VERIFY_V1"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

# Prices are quoted in paise, so anything under half a paisa is a rounding artefact and not a
# disagreement. Volume is an integer count and must match exactly.
PRICE_TOLERANCE = 0.005

# NSE moved to the UDiFF format in July 2024. Both schemes are still served for their eras.
UDIFF_FROM = date(2024, 7, 8)


def _legacy_url(session: date) -> str:
    month = MONTHS[session.month - 1]
    return (
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{session.year}/{month}/"
        f"cm{session.day:02d}{month}{session.year}bhav.csv.zip"
    )


def _udiff_url(session: date) -> str:
    return (
        "https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv.zip"
    )


def _fetch(url: str, *, timeout: int = 60) -> bytes | None:
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Referer": "https://www.nseindia.com/"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return response.read()
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return None


def exchange_rows(session: date) -> dict[tuple[str, str], dict[str, float]] | None:
    """Every row the exchange published for one session, keyed by (symbol, series).

    Series matters. An earlier version of this compared only the EQ series and reported 89
    rows as "absent from the exchange" - all of which were there under BE, the trade-to-trade
    segment, exactly as the dataset records them. Keying on the pair removes that false alarm
    and keeps the comparison honest about which instrument is which.
    """
    for url in ([_udiff_url(session), _legacy_url(session)]
                if session >= UDIFF_FROM
                else [_legacy_url(session), _udiff_url(session)]):
        payload = _fetch(url)
        if not payload:
            continue
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                name = archive.namelist()[0]
                with archive.open(name) as handle:
                    rows = list(csv.DictReader(io.TextIOWrapper(handle, "utf-8")))
        except (zipfile.BadZipFile, KeyError, UnicodeDecodeError):
            continue
        if not rows:
            continue
        # The two formats use different column names for the same numbers.
        udiff = "TckrSymb" in rows[0]
        out: dict[tuple[str, str], dict[str, float]] = {}

        def value(row: dict[str, str], key: str) -> str:
            return (row.get(key) or "").strip()

        for row in rows:
            symbol = value(row, "TckrSymb") if udiff else value(row, "SYMBOL")
            series = value(row, "SctySrs") if udiff else value(row, "SERIES")
            if not symbol or not series:
                continue
            try:
                out[(symbol, series)] = {
                    "Open": float(value(row, "OpnPric") if udiff else value(row, "OPEN")),
                    "High": float(value(row, "HghPric") if udiff else value(row, "HIGH")),
                    "Low": float(value(row, "LwPric") if udiff else value(row, "LOW")),
                    "Close": float(value(row, "ClsPric") if udiff else value(row, "CLOSE")),
                    "Volume": float(
                        value(row, "TtlTradgVol") if udiff else value(row, "TOTTRDQTY")
                    ),
                }
            except ValueError:
                continue
        if out:
            return out
    return None


def verify_sessions(
    *,
    root: Path | None = None,
    sessions: int = 30,
    seed: int = 20260830,
) -> dict[str, Any]:
    """Compare the dataset's raw values against the exchange file for random sessions."""
    root = Path(root) if root else paths.DATA_ROOT
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    glob = str(root / "nifty500" / "parquet" / "nifty500_*.parquet").replace("\\", "/")

    available = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT Date FROM read_parquet('{glob}') ORDER BY Date"
        ).fetchall()
    ]
    rng = random.Random(seed)
    # Spread the sample across the history rather than clustering it, so a problem confined to
    # one era cannot hide behind a lucky draw.
    buckets = max(1, len(available) // sessions)
    sample = sorted(
        rng.choice(available[i : i + buckets])
        for i in range(0, len(available) - buckets + 1, buckets)
    )[:sessions]

    checked = matched = 0
    unreachable: list[str] = []
    absent_from_exchange: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []

    for session in sample:
        exchange = exchange_rows(session)
        if exchange is None:
            unreachable.append(str(session))
            continue
        ours = con.execute(
            f"""
            SELECT Symbol, Series, RawOpen, RawHigh, RawLow, RawClose, RawVolume
            FROM read_parquet('{glob}') WHERE Date = DATE '{session.isoformat()}'
            """
        ).fetchall()
        for symbol, series, o, h, low, close, volume in ours:
            theirs = exchange.get((symbol, series))
            checked += 1
            if theirs is None:
                absent_from_exchange.append(
                    {"date": str(session), "symbol": symbol, "series": series}
                )
                continue
            problems = []
            for label, mine, yours in (
                ("Open", o, theirs["Open"]),
                ("High", h, theirs["High"]),
                ("Low", low, theirs["Low"]),
                ("Close", close, theirs["Close"]),
            ):
                if mine is None or abs(float(mine) - yours) > PRICE_TOLERANCE:
                    problems.append({"field": label, "ours": mine, "exchange": yours})
            if volume is not None and float(volume) != theirs["Volume"]:
                problems.append(
                    {"field": "Volume", "ours": float(volume), "exchange": theirs["Volume"]}
                )
            if problems:
                mismatches.append(
                    {"date": str(session), "symbol": symbol, "fields": problems}
                )
            else:
                matched += 1

    report = {
        "version": VERIFY_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "method": (
            "Random sessions spread across the history. The original NSE bhavcopy for each is "
            "re-downloaded and every raw value in the dataset is compared against it. The raw "
            "columns are checked, not the adjusted ones: the adjusted series is a deliberate "
            "transformation, and it is the raw that must match the exchange."
        ),
        "sessions_sampled": len(sample),
        "sessions_unreachable": unreachable,
        "first_session": str(sample[0]) if sample else None,
        "last_session": str(sample[-1]) if sample else None,
        "rows_checked": checked,
        "rows_matching_the_exchange": matched,
        "rows_absent_from_the_exchange_file": len(absent_from_exchange),
        "rows_mismatched": len(mismatches),
        "match_rate": round(matched / checked, 6) if checked else 0.0,
        "price_tolerance_rupees": PRICE_TOLERANCE,
        "mismatch_examples": mismatches[:25],
        "absent_examples": absent_from_exchange[:15],
    }
    report["pass"] = not mismatches and checked > 0
    atomic_json(paths.LOGS_ROOT / "quality" / "source_verify.json", report)
    return report


__all__ = ["exchange_rows", "verify_sessions"]
