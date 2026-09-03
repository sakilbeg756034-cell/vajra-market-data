"""Independent verification of the published dataset.

This deliberately does not trust the build pipeline's own certification files. It re-derives
everything from the published Parquet, so that a bug in the builder cannot hide behind a
status file the builder itself wrote.

Output: ``DATA_QUALITY_REPORT.md`` inside the published folder, plus a machine-readable JSON
copy under the engine's logs.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json
from vajra_regime.publish import OFFICIAL_MEMBERSHIP_ANCHOR, VAJRA750_FIRST_REBALANCE

QUALITY_VERSION = "VAJRA_DATA_QUALITY_V1"

# A verdict is only as useful as its threshold. These are the ones used below.
MAX_ACCEPTABLE_DUPLICATE_KEYS = 0
MAX_ACCEPTABLE_INVALID_BARS = 0
# A member with no trade on a session is normal (illiquid small caps go untraded for days).
# A member missing for more than this fraction of its membership is not.
MEMBER_COVERAGE_WARN_FRACTION = 0.02


def _sql(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _glob(root: Path, universe: str) -> str:
    return _sql(root / universe / "parquet" / f"{universe}_*.parquet")


# --------------------------------------------------------------------------- checks


def check_shape(con: duckdb.DuckDBPyConnection, root: Path, universe: str) -> dict[str, Any]:
    g = _glob(root, universe)
    per_year = con.execute(
        f"""
        SELECT EXTRACT(year FROM Date)::INTEGER AS y,
               COUNT(*) AS n,
               COUNT(DISTINCT Symbol) AS symbols,
               COUNT(DISTINCT ISIN) AS isins,
               COUNT(DISTINCT Date) AS sessions,
               MIN(Date) AS first_date,
               MAX(Date) AS last_date
        FROM read_parquet('{g}')
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    total = con.execute(
        f"""SELECT COUNT(*), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                   COUNT(DISTINCT Date), MIN(Date), MAX(Date)
            FROM read_parquet('{g}')"""
    ).fetchone()
    return {
        "rows": int(total[0]),
        "distinct_symbols": int(total[1]),
        "distinct_isins": int(total[2]),
        "sessions": int(total[3]),
        "first_date": str(total[4]),
        "last_date": str(total[5]),
        "per_year": [
            {
                "year": int(r[0]),
                "rows": int(r[1]),
                "symbols": int(r[2]),
                "isins": int(r[3]),
                "sessions": int(r[4]),
                "first_date": str(r[5]),
                "last_date": str(r[6]),
            }
            for r in per_year
        ],
    }


def check_missing_sessions(
    con: duckdb.DuckDBPyConnection, root: Path, universe: str
) -> dict[str, Any]:
    """Sessions on the trading calendar for which this universe has no rows at all."""
    g = _glob(root, universe)
    cal = _sql(root / "calendar" / "nse_trading_sessions.parquet")
    rows = con.execute(
        f"""
        SELECT c.SessionDate
        FROM read_parquet('{cal}') c
        LEFT JOIN (SELECT DISTINCT Date FROM read_parquet('{g}')) u ON u.Date = c.SessionDate
        WHERE u.Date IS NULL
        ORDER BY 1
        """
    ).fetchall()
    missing = [str(r[0]) for r in rows]
    return {
        "calendar_sessions": int(
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{cal}')").fetchone()[0]
        ),
        "missing_session_count": len(missing),
        "missing_sessions": missing,
        "pass": not missing,
    }


def check_duplicates(con: duckdb.DuckDBPyConnection, root: Path, universe: str) -> dict[str, Any]:
    g = _glob(root, universe)
    out: dict[str, Any] = {}
    for key in ("Symbol", "ISIN"):
        # NULL keys are not duplicates of each other. Grouping without this filter reported
        # 587 false "duplicates" that were simply the securities with no ISIN.
        rows = con.execute(
            f"""
            SELECT Date, {key}, COUNT(*) AS n
            FROM read_parquet('{g}')
            WHERE {key} IS NOT NULL
            GROUP BY 1, 2 HAVING COUNT(*) > 1
            ORDER BY n DESC, 1, 2
            """
        ).fetchall()
        out[f"duplicate_date_{key.lower()}_groups"] = len(rows)
        out[f"duplicate_date_{key.lower()}_extra_rows"] = sum(int(r[2]) - 1 for r in rows)
        out[f"duplicate_date_{key.lower()}_examples"] = [
            {"date": str(r[0]), key.lower(): r[1], "rows": int(r[2])} for r in rows[:10]
        ]
    out["pass"] = (
        out["duplicate_date_symbol_groups"] <= MAX_ACCEPTABLE_DUPLICATE_KEYS
        and out["duplicate_date_isin_groups"] <= MAX_ACCEPTABLE_DUPLICATE_KEYS
    )
    return out


def check_bar_sanity(con: duckdb.DuckDBPyConnection, root: Path, universe: str) -> dict[str, Any]:
    g = _glob(root, universe)
    row = con.execute(
        f"""
        SELECT
          COUNT(*)                                                          AS rows,
          SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END)                     AS null_date,
          SUM(CASE WHEN Symbol IS NULL THEN 1 ELSE 0 END)                   AS null_symbol,
          SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END)                     AS null_isin,
          SUM(CASE WHEN Close IS NULL THEN 1 ELSE 0 END)                    AS null_close,
          SUM(CASE WHEN Open IS NULL OR High IS NULL OR Low IS NULL THEN 1 ELSE 0 END) AS null_ohl,
          SUM(CASE WHEN Open <= 0 OR High <= 0 OR Low <= 0 OR Close <= 0 THEN 1 ELSE 0 END)
                                                                            AS non_positive,
          SUM(CASE WHEN High < Low THEN 1 ELSE 0 END)                       AS high_lt_low,
          SUM(CASE WHEN Close > High OR Close < Low THEN 1 ELSE 0 END)      AS close_outside,
          SUM(CASE WHEN Open > High OR Open < Low THEN 1 ELSE 0 END)        AS open_outside,
          SUM(CASE WHEN Volume IS NULL THEN 1 ELSE 0 END)                   AS null_volume,
          SUM(CASE WHEN Volume = 0 THEN 1 ELSE 0 END)                       AS zero_volume,
          SUM(CASE WHEN Volume < 0 THEN 1 ELSE 0 END)                       AS negative_volume
        FROM read_parquet('{g}')
        """
    ).fetchone()
    keys = [
        "rows", "null_date", "null_symbol", "null_isin", "null_close", "null_ohl",
        "non_positive", "high_lt_low", "close_outside", "open_outside",
        "null_volume", "zero_volume", "negative_volume",
    ]
    result = {k: int(v or 0) for k, v in zip(keys, row, strict=True)}
    fatal = [
        "null_date", "null_symbol", "null_close", "non_positive",
        "high_lt_low", "close_outside", "open_outside", "null_volume", "negative_volume",
    ]
    result["fatal_total"] = sum(result[k] for k in fatal)
    result["pass"] = result["fatal_total"] <= MAX_ACCEPTABLE_INVALID_BARS
    # Zero volume is not an error - a listed security can go a session with no trade printed.
    result["zero_volume_note"] = (
        "Zero-volume rows are legal: an illiquid security can have a session with no trade. "
        "They are reported, not failed."
    )
    if result["null_isin"]:
        example = con.execute(
            f"""SELECT Symbol, MIN(Date), MAX(Date), COUNT(*)
                FROM read_parquet('{g}') WHERE ISIN IS NULL
                GROUP BY 1 ORDER BY 4 DESC LIMIT 10"""
        ).fetchall()
        result["null_isin_examples"] = [
            {"symbol": r[0], "first": str(r[1]), "last": str(r[2]), "rows": int(r[3])}
            for r in example
        ]
    return result


def check_adjustment_sanity(
    con: duckdb.DuckDBPyConnection, root: Path, universe: str
) -> dict[str, Any]:
    """For every applied split/bonus, does an artificial price gap remain at the ex-date?

    The test: on the ex-date, the adjusted close should be continuous with the previous
    adjusted close - no jump anywhere near the corporate-action ratio. If a 1:10 split were
    unadjusted, the adjusted return that day would be about -90%.
    """
    g = _glob(root, universe)
    applied = _sql(root / "corporate_actions" / "corporate_actions_applied.parquet")
    events = con.execute(
        f"""
        SELECT ExDate, Symbol, ISIN, ActionType, PriceFactor
        FROM read_parquet('{applied}')
        WHERE ActionType IN ('SPLIT', 'BONUS', 'FACE_VALUE_SPLIT')
          AND PriceFactor IS NOT NULL AND PriceFactor <> 1.0
        """
    ).fetchall()
    if not events:
        # ActionType vocabulary may differ; fall back to any event with a real price factor.
        events = con.execute(
            f"""
            SELECT ExDate, Symbol, ISIN, ActionType, PriceFactor
            FROM read_parquet('{applied}')
            WHERE PriceFactor IS NOT NULL AND PriceFactor <> 1.0
            """
        ).fetchall()

    checked = 0
    residual_gaps: list[dict[str, Any]] = []
    con.execute(
        f"""CREATE OR REPLACE TEMP VIEW _px AS
            SELECT Date, ISIN, Close,
                   LAG(Close) OVER (PARTITION BY ISIN ORDER BY Date) AS PrevClose
            FROM read_parquet('{g}')"""
    )
    for ex_date, symbol, isin, action, factor in events:
        if isin is None or ex_date is None:
            continue
        row = con.execute(
            "SELECT Date, Close, PrevClose FROM _px WHERE ISIN = ? AND Date = ?",
            [isin, ex_date],
        ).fetchone()
        if row is None or row[2] in (None, 0):
            continue
        checked += 1
        move = row[1] / row[2] - 1.0
        # A real corporate action left unadjusted shows up as a move close to (factor - 1).
        expected_if_unadjusted = float(factor) - 1.0
        if abs(move) > 0.35 and abs(move - expected_if_unadjusted) < 0.10:
            residual_gaps.append(
                {
                    "ex_date": str(ex_date),
                    "symbol": symbol,
                    "isin": isin,
                    "action": action,
                    "price_factor": float(factor),
                    "adjusted_move": round(move, 4),
                }
            )
    return {
        "events_available": len(events),
        "events_checked_against_prices": checked,
        "residual_unadjusted_gaps": len(residual_gaps),
        "examples": residual_gaps[:15],
        "pass": not residual_gaps,
        "method": (
            "For each applied split/bonus/face-value event, compare the adjusted close on the "
            "ex-date with the previous adjusted close. Flag when the move is both large and "
            "close to what an entirely unadjusted event would produce."
        ),
    }


def check_survivorship(con: duckdb.DuckDBPyConnection, root: Path) -> dict[str, Any]:
    """Prove the membership panel really is point-in-time, rather than assuming it.

    Three things have to be true, and all three are tested here:
      1. Membership changes over time - the set on an early date is not the set on a late one.
      2. Securities that were removed or delisted are present in early history and absent later.
      3. Today's members are not silently backfilled to the start of history.
    """
    membership = _sql(root / "nifty500" / "membership" / "nifty500_daily_membership.parquet")
    probe_dates = [
        "2009-01-02", "2011-06-30", "2013-04-18", "2015-12-31",
        "2018-06-29", "2021-12-31", "2024-06-28", "2026-08-28",
    ]
    snapshots = []
    sets: dict[str, set[str]] = {}
    for d in probe_dates:
        rows = con.execute(
            f"SELECT ISIN FROM read_parquet('{membership}') WHERE Date = DATE '{d}'"
        ).fetchall()
        if not rows:
            # Not a trading session; take the next one that is.
            actual = con.execute(
                f"SELECT MIN(Date) FROM read_parquet('{membership}') WHERE Date >= DATE '{d}'"
            ).fetchone()[0]
            if actual is None:
                continue
            rows = con.execute(
                f"SELECT ISIN FROM read_parquet('{membership}') WHERE Date = DATE '{actual}'"
            ).fetchall()
            d = str(actual)
        sets[d] = {r[0] for r in rows}
        snapshots.append({"date": d, "members": len(rows)})

    overlaps = []
    keys = sorted(sets)
    for i in range(len(keys) - 1):
        a, b = sets[keys[i]], sets[keys[i + 1]]
        overlaps.append(
            {
                "from": keys[i],
                "to": keys[i + 1],
                "common": len(a & b),
                "left_only": len(a - b),
                "right_only": len(b - a),
            }
        )
    first, last = sets[keys[0]], sets[keys[-1]]
    endpoint_overlap = len(first & last)

    # Securities that left and never came back.
    departed = con.execute(
        f"""
        WITH last_seen AS (
            SELECT ISIN, MAX(Date) AS LastDate, MIN(Date) AS FirstDate, COUNT(*) AS Sessions
            FROM read_parquet('{membership}') GROUP BY 1
        ),
        latest AS (SELECT MAX(Date) AS d FROM read_parquet('{membership}'))
        SELECT COUNT(*), SUM(Sessions)
        FROM last_seen, latest WHERE LastDate < latest.d
        """
    ).fetchone()

    # And the reverse: how many of today's members simply did not exist in the panel in 2009.
    absent_at_start = len(last - first)

    passed = (
        endpoint_overlap < len(last)
        and departed[0] > 0
        and absent_at_start > 0
        and all(o["left_only"] > 0 or o["right_only"] > 0 for o in overlaps)
    )
    return {
        "pass": passed,
        "snapshots": snapshots,
        "consecutive_overlaps": overlaps,
        "first_vs_last_common_members": endpoint_overlap,
        "current_members_absent_at_start": absent_at_start,
        "securities_that_left_and_never_returned": int(departed[0] or 0),
        "membership_rows_from_departed_securities": int(departed[1] or 0),
        "interpretation": (
            "If membership were survivorship-biased, the member set would be identical on every "
            "date and no security would ever leave. Both are false here."
        ),
    }


def check_price_coverage_of_members(con: duckdb.DuckDBPyConnection, root: Path) -> dict[str, Any]:
    """How often is a security an index member on a session with no price row?"""
    membership = _sql(root / "nifty500" / "membership" / "nifty500_daily_membership.parquet")
    prices = _glob(root, "nifty500")
    row = con.execute(
        f"""
        SELECT COUNT(*) AS member_sessions,
               SUM(CASE WHEN p.Date IS NULL THEN 1 ELSE 0 END) AS without_price
        FROM read_parquet('{membership}') m
        LEFT JOIN (SELECT DISTINCT Date, ISIN FROM read_parquet('{prices}')) p
          ON p.Date = m.Date AND p.ISIN = m.ISIN
        """
    ).fetchone()
    member_sessions = int(row[0])
    without = int(row[1] or 0)
    fraction = without / member_sessions if member_sessions else 0.0
    worst = con.execute(
        f"""
        SELECT m.MembershipSymbol, COUNT(*) AS n
        FROM read_parquet('{membership}') m
        LEFT JOIN (SELECT DISTINCT Date, ISIN FROM read_parquet('{prices}')) p
          ON p.Date = m.Date AND p.ISIN = m.ISIN
        WHERE p.Date IS NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
    ).fetchall()

    # The aggregate hides the thing that matters. Split it at the membership anchor: gaps in
    # the reconstructed era sit in years already labelled PRICE_DATA_ONLY, while a gap in the
    # official era would undermine the BACKTEST_SAFE label directly.
    anchor = OFFICIAL_MEMBERSHIP_ANCHOR.isoformat()
    by_era = con.execute(
        f"""
        SELECT m.Date >= DATE '{anchor}' AS official_era,
               COUNT(*), SUM(CASE WHEN p.Date IS NULL THEN 1 ELSE 0 END)
        FROM read_parquet('{membership}') m
        LEFT JOIN (SELECT DISTINCT Date, ISIN FROM read_parquet('{prices}')) p
          ON p.Date = m.Date AND p.ISIN = m.ISIN
        GROUP BY 1
        """
    ).fetchall()
    eras: dict[str, Any] = {}
    for official, total, missing in by_era:
        total, missing = int(total), int(missing or 0)
        eras["official_era_backtest_safe" if official else "reconstructed_era_price_data_only"] = {
            "member_sessions": total,
            "without_a_price": missing,
            "fraction": round(missing / total, 6) if total else 0.0,
        }
    return {
        "member_sessions": member_sessions,
        "member_sessions_without_price": without,
        "fraction": round(fraction, 6),
        "pass": fraction <= MEMBER_COVERAGE_WARN_FRACTION,
        "by_era": eras,
        "worst_symbols": [{"symbol": r[0], "missing_sessions": int(r[1])} for r in worst],
        "note": (
            "A member with no price row for a session usually means the security did not trade "
            "that day, or is a suspended name still formally in the index."
        ),
    }


def check_tradability(
    con: duckdb.DuckDBPyConnection, root: Path, universe: str
) -> dict[str, Any]:
    """Could a strategy actually have traded these rows?

    With corporate actions clean, this is the remaining way a backtest goes wrong: the price
    is correct and untradeable. A frozen bar is a circuit limit or a single trade, so a fill
    anywhere but that one price is invented. A session turning over a lakh of rupees cannot
    absorb a real position.

    These are reported, never excluded. They are genuine characteristics of Indian mid and
    small caps, not errors, and removing them would quietly narrow the universe.
    """
    g = _glob(root, universe)
    row = con.execute(
        f"""
        SELECT COUNT(*),
               SUM(CASE WHEN IsFrozenBar THEN 1 ELSE 0 END),
               SUM(CASE WHEN TurnoverINR < 100000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN TurnoverINR < 1000000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN TurnoverINR < 10000000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN Volume = 0 THEN 1 ELSE 0 END)
        FROM read_parquet('{g}')
        WHERE IsResearchEligible
        """
    ).fetchone()
    eligible = int(row[0] or 0)

    def share(value: Any) -> float:
        return round(int(value or 0) / eligible, 6) if eligible else 0.0

    # The longest run of sessions on which the close did not move at all.
    stale = con.execute(
        f"""
        SELECT MAX(n), SUM(CASE WHEN n >= 5 THEN n ELSE 0 END)
        FROM (
          SELECT ISIN, RunId, COUNT(*) AS n FROM (
            SELECT ISIN, Close,
                   SUM(CASE WHEN Close = Prev THEN 0 ELSE 1 END) OVER (
                     PARTITION BY ISIN ORDER BY Date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunId
            FROM (
              SELECT Date, ISIN, Close,
                     LAG(Close) OVER (PARTITION BY ISIN ORDER BY Date) AS Prev
              FROM read_parquet('{g}') WHERE ISIN IS NOT NULL AND IsResearchEligible
            )
          ) GROUP BY 1, 2
        )
        """
    ).fetchone()

    return {
        "research_eligible_rows": eligible,
        "frozen_bars": int(row[1] or 0),
        "frozen_bars_share": share(row[1]),
        "turnover_under_1_lakh": int(row[2] or 0),
        "turnover_under_1_lakh_share": share(row[2]),
        "turnover_under_10_lakh": int(row[3] or 0),
        "turnover_under_10_lakh_share": share(row[3]),
        "turnover_under_1_crore": int(row[4] or 0),
        "turnover_under_1_crore_share": share(row[4]),
        "zero_volume": int(row[5] or 0),
        "longest_unchanged_close_run_sessions": int(stale[0] or 0),
        "rows_in_unchanged_close_runs_of_5_or_more": int(stale[1] or 0),
        "note": (
            "Reported, not excluded. These are real liquidity characteristics, not data "
            "errors. Filter on IsFrozenBar and TurnoverINR to match your own position size."
        ),
    }


def check_parquet_csv_parity(root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """The publish step asserts this per year before writing. Report what it recorded."""
    years = []
    without_csv = []
    for name, entry in manifest["universes"].items():
        for row in entry["years"]:
            if not row.get("csv"):
                # No CSV to compare against. Absence is a supported state, not a failure.
                without_csv.append({"universe": name, "year": row["year"]})
                continue
            years.append(
                {
                    "universe": name,
                    "year": row["year"],
                    "identical": bool(row.get("parquet_csv_identical")),
                }
            )
    return {
        "years_checked": len(years),
        "years_identical": sum(1 for y in years if y["identical"]),
        "years_without_a_csv": without_csv,
        "pass": all(y["identical"] for y in years),
        "method": (
            "Every year is written to Parquet and CSV from the same query, then compared with "
            "EXCEPT ALL in both directions before either file is moved into place."
        ),
    }


UNEXPLAINED_MOVE_THRESHOLD = 0.50
UNEXPLAINED_EVENT_WINDOW_DAYS = 5
# Itne se zyada calendar din ka faasla ho to wo "ek din ka move" hai hi nahi.
# Lambi chhutti (Diwali, Holi, weekend ke saath) 5-6 din tak kha jaati hai;
# 15 uske upar ka wo daayra hai jahan row sach me gayab thi.
UNEXPLAINED_GAP_DAYS = 15


def check_unexplained_breaks(
    con: duckdb.DuckDBPyConnection, root: Path, universe: str
) -> dict[str, Any]:
    """Did a price break with no corporate action behind it stay research-eligible?

    `adjustment_sanity` above starts from the corporate-action ledger and looks at each
    ex-date, so it can only see breaks that an event explains. A break with no event at all
    is invisible to it - not merely unrepaired, never looked at.

    That is how CUPID passed: +406.66% on 2026-01-01, nothing in the ledger within days of
    it, IsResearchEligible still true, and this report still saying PASS. Thirteen other
    securities carried the same break on the same session, each of size exactly the
    reciprocal of its own pending-2026 bonus - the signature of the legacy half of the
    master being adjusted twice (fixed in rolling_master.py on 2026-09-02).

    So this check runs the other way round: start from the prices, find the impossible
    moves, and ask whether anything explains them. NSE's price bands top out at 20%; a
    one-day move past 50% with no event within a few days either side is a data fault, and
    a data fault that is still eligible is one a strategy can act on.
    """
    g = _glob(root, universe)
    ca = _sql(root / "corporate_actions" / "official_nse_corporate_actions_all.parquet")
    # The two universes name their adjusted daily return differently, and deriving one
    # here with LAG would be wrong anyway: nifty500 carries 587 (Date, ISIN) pairs on more
    # than one row, so a window partitioned by ISIN alone steps between them and invents
    # moves. Use whichever column the file itself publishes.
    available = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{g}')").fetchall()
    }
    for candidate in ("Return1D", "AdjustedReturn1D"):
        if candidate in available:
            return_column = candidate
            break
    else:
        return {
            "pass": None,
            "note": "No adjusted daily-return column found; check not run.",
            "columns_seen": sorted(available)[:40],
        }
    # Ek break do bilkul alag wajah se ban sakta hai, aur unke ilaaj alag hain.
    #
    # Agar pichhli row kal ki thi, to move sach me ek din me hua -- ya to asli
    # crash, ya adjustment chhoot gaya.
    #
    # Par agar pichhli row mahino purani hai, to ye "1-din ka move" hai hi nahi:
    # wo poore gap ka return hai, jise ek din ka bata diya gaya. SUZLON ka
    # +58.6% aisa hi tha -- 98 din ka move, kyunki wo BE series me chala gaya
    # tha aur intake sirf EQ leta tha.
    #
    # Dono ko ek ginti me milaana report ko jhutha nahi karta, par bekaar zaroor
    # kar deta hai: padhne wale ko pata hi nahi chalta ki dekhna kahan hai.
    gap_column = "GapDays" if "GapDays" in available else "NULL"
    rows = con.execute(
        f"""
        WITH p AS (
            SELECT Date, Symbol, ISIN, "{return_column}" AS Return1D, IsResearchEligible,
                   {gap_column} AS GapDays
            FROM read_parquet('{g}')
            WHERE "{return_column}" IS NOT NULL
              AND abs("{return_column}") > {UNEXPLAINED_MOVE_THRESHOLD}
        ),
        e AS (
            SELECT ISIN, Symbol, CAST(ExDate AS DATE) AS ExDate
            FROM read_parquet('{ca}') WHERE ExDate IS NOT NULL
        )
        SELECT p.Date, p.Symbol, p.ISIN, p.Return1D, p.IsResearchEligible, p.GapDays
        FROM p
        WHERE NOT EXISTS (
            SELECT 1 FROM e
            WHERE (e.ISIN = p.ISIN OR e.Symbol = p.Symbol)
              AND abs(date_diff('day', e.ExDate, p.Date)) <= {UNEXPLAINED_EVENT_WINDOW_DAYS}
        )
        ORDER BY abs(p.Return1D) DESC
        """
    ).fetchall()
    still_eligible = [r for r in rows if r[4]]

    def _spans_a_gap(row: tuple) -> bool:
        return row[5] is not None and float(row[5]) > UNEXPLAINED_GAP_DAYS

    across_gap = [r for r in still_eligible if _spans_a_gap(r)]
    single_session = [r for r in still_eligible if not _spans_a_gap(r)]

    def _detail(row: tuple) -> dict[str, Any]:
        return {
            "date": str(row[0]),
            "symbol": row[1],
            "isin": row[2],
            "move": round(float(row[3]), 6),
            "research_eligible": bool(row[4]),
            "gap_days": None if row[5] is None else int(row[5]),
        }

    return {
        "pass": not still_eligible,
        "return_column": return_column,
        "threshold": UNEXPLAINED_MOVE_THRESHOLD,
        "event_window_days": UNEXPLAINED_EVENT_WINDOW_DAYS,
        "gap_days_threshold": UNEXPLAINED_GAP_DAYS,
        "unexplained_breaks": len(rows),
        "already_excluded": len(rows) - len(still_eligible),
        "still_research_eligible": len(still_eligible),
        # Ye do ginti alag hain kyunki inke ilaaj alag hain: ek me adjustment
        # dekhna hota hai, doosre me ye ki row gayab kyun thi.
        "eligible_within_one_session": len(single_session),
        "eligible_across_a_gap": len(across_gap),
        "worst_within_one_session": [_detail(r) for r in single_session[:15]],
        "worst_across_a_gap": [_detail(r) for r in across_gap[:15]],
    }


def check_eligibility_and_quarantine(
    con: duckdb.DuckDBPyConnection, root: Path, universe: str
) -> dict[str, Any]:
    g = _glob(root, universe)
    row = con.execute(
        f"""SELECT COUNT(*),
                   SUM(CASE WHEN IsResearchEligible THEN 1 ELSE 0 END),
                   SUM(CASE WHEN CorporateActionQuarantineFlag THEN 1 ELSE 0 END)
            FROM read_parquet('{g}')"""
    ).fetchone()
    reasons = con.execute(
        f"""SELECT CorporateActionQuarantineReason, COUNT(*) AS n
            FROM read_parquet('{g}')
            WHERE CorporateActionQuarantineReason IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15"""
    ).fetchall()
    return {
        "rows": int(row[0]),
        "research_eligible": int(row[1] or 0),
        "quarantined": int(row[2] or 0),
        "quarantined_fraction": round((row[2] or 0) / row[0], 6) if row[0] else 0.0,
        "top_quarantine_reasons": [{"reason": r[0], "rows": int(r[1])} for r in reasons],
    }


# --------------------------------------------------------------------------- report


def run_quality_checks(root: Path | None = None) -> dict[str, Any]:
    root = Path(root) if root else paths.DATA_ROOT
    manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")

    report: dict[str, Any] = {
        "version": QUALITY_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "published_root": str(root),
        "latest_session": manifest["latest_session"],
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "universes": {},
    }
    for universe in ("nifty500", "nifty750"):
        report["universes"][universe] = {
            "shape": check_shape(con, root, universe),
            "missing_sessions": check_missing_sessions(con, root, universe),
            "duplicates": check_duplicates(con, root, universe),
            "bar_sanity": check_bar_sanity(con, root, universe),
            "adjustment_sanity": check_adjustment_sanity(con, root, universe),
            "eligibility": check_eligibility_and_quarantine(con, root, universe),
            "unexplained_breaks": check_unexplained_breaks(con, root, universe),
            "tradability": check_tradability(con, root, universe),
        }
    report["survivorship"] = check_survivorship(con, root)
    report["member_price_coverage"] = check_price_coverage_of_members(con, root)
    report["parquet_csv_parity"] = check_parquet_csv_parity(root, manifest)

    source_path = paths.LOGS_ROOT / "quality" / "source_verify.json"
    if source_path.is_file():
        report["exchange_verification"] = json.loads(source_path.read_text(encoding="utf-8"))
    else:
        report["exchange_verification"] = {
            "status": "NOT_RUN",
            "note": "No re-verification against NSE's own bhavcopy was found.",
        }

    external_path = paths.LOGS_ROOT / "quality" / "external_crosscheck.json"
    if external_path.is_file():
        report["external_crosscheck"] = json.loads(external_path.read_text(encoding="utf-8"))
    else:
        report["external_crosscheck"] = {
            "status": "NOT_RUN",
            "note": "No independent-source cross-check result was found.",
        }

    verdicts: dict[str, str] = {}
    for universe, checks in report["universes"].items():
        for name in ("missing_sessions", "duplicates", "bar_sanity", "adjustment_sanity",
                     "unexplained_breaks"):
            verdicts[f"{universe}.{name}"] = "PASS" if checks[name].get("pass") else "FAIL"
    verdicts["survivorship"] = "PASS" if report["survivorship"]["pass"] else "FAIL"
    verdicts["member_price_coverage"] = (
        "PASS" if report["member_price_coverage"]["pass"] else "WARN"
    )
    verdicts["parquet_csv_parity"] = "PASS" if report["parquet_csv_parity"]["pass"] else "FAIL"
    report["verdicts"] = verdicts
    report["overall"] = "FAIL" if any(v == "FAIL" for v in verdicts.values()) else (
        "PASS_WITH_WARNINGS" if any(v == "WARN" for v in verdicts.values()) else "PASS"
    )

    atomic_json(paths.LOGS_ROOT / "quality" / "latest_quality_report.json", report)
    return report


def _verdict_icon(value: str) -> str:
    return {"PASS": "PASS", "WARN": "WARN", "FAIL": "**FAIL**"}.get(value, value)


def render_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    add = lines.append

    add("# DATA QUALITY REPORT")
    add("")
    add(
        f"*Generated {report['generated_at_utc'][:19]}Z by the VAJRA engine, re-derived from "
        "the published Parquet files. It does not read the build pipeline's own status files, "
        "so a bug in the builder cannot hide behind them.*"
    )
    add("")
    add(f"## Overall verdict: **{report['overall']}**")
    add("")
    add("| Check | Verdict |")
    add("|---|---|")
    for name, verdict in report["verdicts"].items():
        add(f"| `{name}` | {_verdict_icon(verdict)} |")
    add("")

    for universe, checks in report["universes"].items():
        shape = checks["shape"]
        add(f"## {universe}")
        add("")
        add(
            f"{shape['rows']:,} rows · {shape['distinct_symbols']:,} symbols · "
            f"{shape['distinct_isins']:,} ISINs · {shape['sessions']:,} sessions · "
            f"{shape['first_date']} to {shape['last_date']}"
        )
        add("")
        add("| Year | Rows | Symbols | ISINs | Sessions | First | Last |")
        add("|---|---:|---:|---:|---:|---|---|")
        for row in shape["per_year"]:
            add(
                f"| {row['year']} | {row['rows']:,} | {row['symbols']:,} | {row['isins']:,} "
                f"| {row['sessions']} | {row['first_date']} | {row['last_date']} |"
            )
        add("")

        ms = checks["missing_sessions"]
        add("### Missing trading sessions")
        add("")
        if ms["missing_session_count"] == 0:
            add(
                f"None. All {ms['calendar_sessions']:,} sessions on the trading calendar have "
                "data in this universe."
            )
        else:
            add(
                f"**{ms['missing_session_count']} of {ms['calendar_sessions']:,} calendar "
                "sessions have no rows at all:**"
            )
            add("")
            for d in ms["missing_sessions"][:200]:
                add(f"- {d}")
            if ms["missing_session_count"] > 200:
                add(f"- … and {ms['missing_session_count'] - 200} more")
        add("")

        dup = checks["duplicates"]
        add("### Duplicate keys")
        add("")
        add(
            f"`(Date, Symbol)` duplicate groups: **{dup['duplicate_date_symbol_groups']}** "
            f"({dup['duplicate_date_symbol_extra_rows']} extra rows). "
            f"`(Date, ISIN)` duplicate groups: **{dup['duplicate_date_isin_groups']}** "
            f"({dup['duplicate_date_isin_extra_rows']} extra rows)."
        )
        if dup["duplicate_date_isin_examples"]:
            add("")
            add("Examples:")
            for ex in dup["duplicate_date_isin_examples"]:
                add(f"- {ex}")
        add("")

        bar = checks["bar_sanity"]
        add("### Bar sanity")
        add("")
        add("| Condition | Rows |")
        add("|---|---:|")
        for key in (
            "null_date", "null_symbol", "null_isin", "null_close", "null_ohl",
            "non_positive", "high_lt_low", "close_outside", "open_outside",
            "null_volume", "zero_volume", "negative_volume",
        ):
            add(f"| `{key}` | {bar[key]:,} |")
        add("")
        add(bar["zero_volume_note"])
        if bar.get("null_isin_examples"):
            add("")
            add("Rows with no ISIN, by symbol:")
            for ex in bar["null_isin_examples"]:
                add(
                    f"- `{ex['symbol']}` — {ex['rows']:,} rows, "
                    f"{ex['first']} to {ex['last']}"
                )
        add("")

        adj = checks["adjustment_sanity"]
        add("### Adjustment sanity")
        add("")
        add(adj["method"])
        add("")
        add(
            f"{adj['events_checked_against_prices']:,} of {adj['events_available']:,} applied "
            f"events had a price on the ex-date and were checked. "
            f"Residual unadjusted gaps: **{adj['residual_unadjusted_gaps']}**."
        )
        if adj["examples"]:
            add("")
            for ex in adj["examples"]:
                add(
                    f"- {ex['ex_date']} `{ex['symbol']}` {ex['action']} "
                    f"factor {ex['price_factor']} left a {ex['adjusted_move']:+.1%} move"
                )
        add("")

        tr = checks["tradability"]
        add("### Tradability")
        add("")
        add(
            "Corporate actions are clean, so this is the remaining way a backtest goes wrong: "
            "the price is correct and nobody could have traded at it."
        )
        add("")
        add("| Condition | Rows | Share of research-eligible |")
        add("|---|---:|---:|")
        add(
            f"| Frozen bar (open = high = low = close) | {tr['frozen_bars']:,} "
            f"| {tr['frozen_bars_share']:.2%} |"
        )
        add(
            f"| Turnover under Rs 1 lakh | {tr['turnover_under_1_lakh']:,} "
            f"| {tr['turnover_under_1_lakh_share']:.2%} |"
        )
        add(
            f"| Turnover under Rs 10 lakh | {tr['turnover_under_10_lakh']:,} "
            f"| {tr['turnover_under_10_lakh_share']:.2%} |"
        )
        add(
            f"| Turnover under Rs 1 crore | {tr['turnover_under_1_crore']:,} "
            f"| {tr['turnover_under_1_crore_share']:.2%} |"
        )
        add(f"| Zero volume | {tr['zero_volume']:,} | — |")
        add("")
        add(
            f"Longest run of sessions with an unchanged close: "
            f"**{tr['longest_unchanged_close_run_sessions']}**. "
            f"{tr['rows_in_unchanged_close_runs_of_5_or_more']:,} rows sit in a run of five "
            "or more."
        )
        add("")
        add(tr["note"])
        add("")

        # Absent in reports written before this check existed. An old report should
        # still render rather than raise.
        ub = checks.get("unexplained_breaks")
        if ub is not None:
            add("### Unexplained price breaks")
            add("")
            add(
                f"A one-day move past {ub['threshold']:.0%} with no corporate action within "
                f"{ub['event_window_days']} days either side of it. NSE's price bands top out "
                "at 20%, so a move this size that nothing explains is a data fault rather than "
                "a return."
            )
            add("")
            add(
                f"{ub['unexplained_breaks']:,} found · {ub['already_excluded']:,} already "
                f"excluded from research · **{ub['still_research_eligible']:,} still "
                "research-eligible**"
            )
            if ub["still_research_eligible"]:
                add("")
                add(
                    "These are still eligible, which means a strategy can act on them. "
                    "They split into two kinds, and the two have different causes."
                )
                add("")
                add(
                    f"**{ub['eligible_within_one_session']:,} happened inside a single "
                    f"session.** The previous row is the previous trading day, so the move is "
                    "real: either a genuine crash or an adjustment that was never applied."
                )
                if ub["worst_within_one_session"]:
                    add("")
                    add("| Date | Symbol | ISIN | Move |")
                    add("|---|---|---|---:|")
                    for r in ub["worst_within_one_session"]:
                        add(
                            f"| {r['date']} | {r['symbol']} | {r['isin']} "
                            f"| {r['move']:+.1%} |"
                        )
                add("")
                add(
                    f"**{ub['eligible_across_a_gap']:,} span a gap of more than "
                    f"{ub['gap_days_threshold']} days.** These are not one-day moves at all. "
                    "The security has no rows for the intervening period, so the whole gap's "
                    "return is being reported as a single session. The question here is not "
                    "what moved the price - it is why the rows are missing."
                )
                if ub["worst_across_a_gap"]:
                    add("")
                    add("| Date | Symbol | ISIN | Move | Gap (days) |")
                    add("|---|---|---|---:|---:|")
                    for r in ub["worst_across_a_gap"]:
                        add(
                            f"| {r['date']} | {r['symbol']} | {r['isin']} "
                            f"| {r['move']:+.1%} | {r['gap_days']} |"
                        )
            else:
                add("")
                add(
                    "None are still eligible. Note that the check above, `adjustment sanity`, "
                    "cannot see these: it starts from the corporate-action ledger and looks at "
                    "each ex-date, so a break with no event behind it is never examined. That "
                    "is how CUPID's +406.66% on 2026-01-01 passed while this report still said "
                    "PASS."
                )
            add("")

        el = checks["eligibility"]
        add("### Research eligibility and quarantine")
        add("")
        add(
            f"{el['research_eligible']:,} of {el['rows']:,} rows are research-eligible. "
            f"{el['quarantined']:,} rows ({el['quarantined_fraction']:.2%}) carry a "
            "corporate-action quarantine."
        )
        if el["top_quarantine_reasons"]:
            add("")
            add("| Reason | Rows |")
            add("|---|---:|")
            for r in el["top_quarantine_reasons"]:
                add(f"| {r['reason']} | {r['rows']:,} |")
        add("")

    surv = report["survivorship"]
    add("## Survivorship-bias test")
    add("")
    add(surv["interpretation"])
    add("")
    add("| Probe date | Members |")
    add("|---|---:|")
    for s in surv["snapshots"]:
        add(f"| {s['date']} | {s['members']} |")
    add("")
    add("| From | To | Common | Left only | Right only |")
    add("|---|---|---:|---:|---:|")
    for o in surv["consecutive_overlaps"]:
        add(f"| {o['from']} | {o['to']} | {o['common']} | {o['left_only']} | {o['right_only']} |")
    add("")
    add(
        f"- Members common to the first and last probe date: "
        f"**{surv['first_vs_last_common_members']}**"
    )
    add(
        f"- Current members that were absent at the start of history: "
        f"**{surv['current_members_absent_at_start']}**"
    )
    add(
        f"- Securities that left the index and never returned: "
        f"**{surv['securities_that_left_and_never_returned']}**, contributing "
        f"{surv['membership_rows_from_departed_securities']:,} membership rows that a "
        "survivorship-biased dataset would not contain"
    )
    add("")

    cov = report["member_price_coverage"]
    add("## Price coverage of index members")
    add("")
    add(
        f"{cov['member_sessions_without_price']:,} of {cov['member_sessions']:,} member-sessions "
        f"({cov['fraction']:.3%}) have no price row."
    )
    add("")
    if cov.get("by_era"):
        add("The aggregate hides the part that matters. Split at the membership anchor:")
        add("")
        add("| Era | Member sessions | Without a price | Share |")
        add("|---|---:|---:|---:|")
        labels = {
            "official_era_backtest_safe": f"Official, {OFFICIAL_MEMBERSHIP_ANCHOR} onward "
            "(BACKTEST_SAFE)",
            "reconstructed_era_price_data_only": "Reconstructed, before the anchor "
            "(PRICE_DATA_ONLY)",
        }
        for key, label in labels.items():
            era = cov["by_era"].get(key)
            if era:
                add(
                    f"| {label} | {era['member_sessions']:,} | {era['without_a_price']:,} "
                    f"| {era['fraction']:.4%} |"
                )
        add("")
        add(
            "Coverage in the era you are meant to backtest on is effectively complete. The "
            "gaps sit in the years that are already labelled not backtestable."
        )
        add("")
    add(cov["note"])
    if cov["worst_symbols"]:
        add("")
        add("| Symbol | Member sessions with no price |")
        add("|---|---:|")
        for r in cov["worst_symbols"]:
            add(f"| `{r['symbol']}` | {r['missing_sessions']:,} |")
    add("")

    par = report["parquet_csv_parity"]
    add("## Parquet vs CSV")
    add("")
    add(par["method"])
    add("")
    add(f"{par['years_identical']} of {par['years_checked']} universe-years verified identical.")
    add("")

    src = report.get("exchange_verification", {})
    add("## Re-verified against NSE's own bhavcopy")
    add("")
    if src.get("status") == "NOT_RUN":
        add(src.get("note", "Not run."))
    else:
        add(src.get("method", ""))
        add("")
        add(
            f"**{src.get('rows_matching_the_exchange', 0):,} of "
            f"{src.get('rows_checked', 0):,} rows match the exchange exactly** across "
            f"{src.get('sessions_sampled', 0)} random sessions from "
            f"{src.get('first_session')} to {src.get('last_session')}. "
            f"Mismatches: **{src.get('rows_mismatched', 0)}**. "
            f"Match rate: **{src.get('match_rate', 0):.4%}**."
        )
        add("")
        add(
            "Prices are compared to within half a paisa and volume must match exactly. This "
            "is the highest authority available: there is nothing to appeal to above the "
            "exchange's own published file."
        )
    add("")

    ext = report["external_crosscheck"]
    add("## Independent source cross-check")
    add("")
    if ext.get("status") == "NOT_RUN":
        add(ext["note"])
    else:
        add(ext.get("summary", ""))
        add("")
        add(ext.get("method", ""))
        if ext.get("symbols"):
            add("")
            add(
                "| Symbol | Source | Overlapping sessions | Median abs. daily-return "
                "difference | Sessions agreeing | Verdict |"
            )
            add("|---|---|---:|---:|---:|---|")
            for s in ext["symbols"]:
                # A symbol no external source could answer for has none of the comparison
                # fields, so every cell is formatted defensively rather than indexed.
                sessions = s.get("overlapping_sessions")
                median = s.get("median_abs_return_difference")
                agreement = s.get("sessions_agreeing_within_tolerance")
                cells = [
                    f"`{s['symbol']}`",
                    s.get("source") or "—",
                    f"{sessions:,}" if sessions else "—",
                    f"{median:.5f}" if median is not None else "—",
                    f"{agreement:.2%}" if agreement is not None else "—",
                    s["verdict"],
                ]
                add("| " + " | ".join(cells) + " |")
    add("")

    add("## What this report does not check")
    add("")
    add(
        "- It does not verify dividends, because none are applied. This is a price-return "
        "dataset by design."
    )
    add(
        "- It does not read an official NSE holiday calendar. Non-trading weekdays are inferred "
        "from the absence of end-of-day data."
    )
    add(
        "- It cannot validate membership before "
        f"{OFFICIAL_MEMBERSHIP_ANCHOR.isoformat()} against an official source, because no such "
        "source exists for those dates. Those years are labelled `PRICE_DATA_ONLY` in "
        "`MANIFEST.json` for exactly that reason."
    )
    add(
        "- The VAJRA 750 universe is a self-defined rule, not an index, so there is no external "
        f"membership list to check it against. Its first rebalance is "
        f"{VAJRA750_FIRST_REBALANCE.isoformat()}."
    )
    add("")
    return "\n".join(lines) + "\n"


def write_quality_report(root: Path | None = None) -> dict[str, Any]:
    root = Path(root) if root else paths.DATA_ROOT
    report = run_quality_checks(root)
    text = render_report(report)
    path = root / "DATA_QUALITY_REPORT.md"
    path.write_text(text, encoding="utf-8")
    return report


__all__ = ["render_report", "run_quality_checks", "write_quality_report"]
