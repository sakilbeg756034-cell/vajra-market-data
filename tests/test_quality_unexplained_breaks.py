"""The check that would have caught CUPID.

`adjustment_sanity` starts from the corporate-action ledger and looks at each ex-date, so a
break with no event behind it is never examined. CUPID's +406.66% on 2026-01-01 had nothing
in the ledger within days of it, stayed research-eligible, and the report still said PASS.

These tests hold the new check to the two things that matter: it fires when an unexplained
break is still eligible, and it stays quiet once that break has been excluded.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from vajra_regime.quality import check_unexplained_breaks


def _write(tmp_path: Path, *, eligible: bool, with_event: bool) -> Path:
    """A tiny published tree: one security, one impossible move, one CA ledger."""
    root = tmp_path / "published"
    (root / "nifty750" / "parquet").mkdir(parents=True, exist_ok=True)
    (root / "corporate_actions").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # 100 -> 105 -> 525: the third bar is a five-fold move, which NSE's 20% bands make
    # impossible without a corporate action.
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (DATE '2026-01-05', 'AAA', 'INE000A01001', 100.0, NULL::DOUBLE, TRUE),
                (DATE '2026-01-06', 'AAA', 'INE000A01001', 105.0, 0.05, TRUE),
                (DATE '2026-01-07', 'AAA', 'INE000A01001', 525.0, 4.0, {str(eligible).upper()})
            ) AS t(Date, Symbol, ISIN, Close, Return1D, IsResearchEligible)
        ) TO '{(root / "nifty750" / "parquet" / "nifty750_2026.parquet").as_posix()}'
          (FORMAT PARQUET)
        """
    )

    ex_date = "DATE '2026-01-07'" if with_event else "DATE '2020-06-15'"
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                ('AAA', 'INE000A01001', {ex_date}, 'Bonus 4:1')
            ) AS t(Symbol, ISIN, ExDate, Subject)
        ) TO '{(root / "corporate_actions"
                / "official_nse_corporate_actions_all.parquet").as_posix()}'
          (FORMAT PARQUET)
        """
    )
    con.close()
    return root


def test_fires_when_an_unexplained_break_is_still_eligible(tmp_path: Path) -> None:
    root = _write(tmp_path, eligible=True, with_event=False)
    with duckdb.connect() as con:
        result = check_unexplained_breaks(con, root, "nifty750")

    assert result["pass"] is False
    assert result["unexplained_breaks"] == 1
    assert result["still_research_eligible"] == 1
    assert result["worst"][0]["symbol"] == "AAA"
    assert result["return_column"] == "Return1D"


def test_quiet_once_the_break_has_been_excluded(tmp_path: Path) -> None:
    root = _write(tmp_path, eligible=False, with_event=False)
    with duckdb.connect() as con:
        result = check_unexplained_breaks(con, root, "nifty750")

    # Still found - the break has not gone anywhere - but no longer actionable.
    assert result["pass"] is True
    assert result["unexplained_breaks"] == 1
    assert result["already_excluded"] == 1
    assert result["still_research_eligible"] == 0


def test_a_break_the_ledger_explains_is_not_reported(tmp_path: Path) -> None:
    """A move with a corporate action on the same date is somebody else's check.

    `adjustment_sanity` owns that case and can measure it properly against the ratio. This
    one exists only for breaks nothing explains, so it must not double-report.
    """
    root = _write(tmp_path, eligible=True, with_event=True)
    with duckdb.connect() as con:
        result = check_unexplained_breaks(con, root, "nifty750")

    assert result["pass"] is True
    assert result["unexplained_breaks"] == 0
