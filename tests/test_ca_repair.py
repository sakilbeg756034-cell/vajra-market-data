"""Tests for the corporate-action repair pass.

The bug this module was written for: BHARTIARTL's 2009-07-24 1:2 face-value split was never
applied to the pre-split history, leaving a -48.9% one-day "return" in a large cap that was
still flagged research-eligible. These tests rebuild that situation in miniature.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from vajra_regime import ca_repair, paths


def _write(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("f", frame)
    con.execute(f"COPY (SELECT * FROM f) TO '{str(path).replace(chr(92), '/')}' (FORMAT PARQUET)")


SESSIONS = [date(2024, 3, d) for d in (11, 12, 13, 14, 15)]
EX_DATE = date(2024, 3, 14)


def _unadjusted_series() -> pd.DataFrame:
    """Closes of 200, 202, 204 then 103, 104 - a 1:2 split that was never applied."""
    closes = [200.0, 202.0, 204.0, 103.0, 104.0]
    returns = [None] + [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    return pd.DataFrame(
        {
            "Date": SESSIONS,
            "Symbol": ["TESTCO"] * 5,
            "ISIN": ["INE999Z01001"] * 5,
            "Open": closes,
            "High": [c * 1.01 for c in closes],
            "Low": [c * 0.99 for c in closes],
            "Close": closes,
            "PointInTimePriceEligibilityClose": closes,
            "Volume": [1000, 1100, 1200, 2600, 2700],
            "PriceAdjustmentFactor": [1.0] * 5,
            "VolumeAdjustmentFactor": [1.0] * 5,
            "AdjustedReturn1D": returns,
            "DiscontinuityClassification": ["NONE"] * 5,
            "IsResearchEligible": [True] * 5,
            "CorporateActionQuarantineReason": [""] * 5,
        }
    )


def _reconciliation(action_type: str = "SPLIT", price_factor: float | None = 0.5) -> pd.DataFrame:
    """The official archive as the engine reads it: raw NSE text, no pre-parsed ratio.

    The detector parses the subject itself, so a fixture that handed it a ready-made factor
    would not exercise the path that actually runs.
    """
    subject = {
        "SPLIT": "Face Value Split From Rs.10/- To Rs.5/-",
        "DEMERGER": "Demerger",
    }.get(action_type, "Face Value Split From Rs.10/- To Rs.5/-")
    return pd.DataFrame(
        {
            "EventId": ["EV1"],
            "ExDate": [EX_DATE],
            "Symbol": ["TESTCO"],
            "ISIN": ["INE999Z01001"],
            "Subject": [subject],
            "Series": ["EQ"],
        }
    )


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    pit = tmp_path / "store" / "NIFTY500 Point In Time"
    monkeypatch.setattr(paths, "NIFTY500_PIT", pit)
    monkeypatch.setattr(paths, "CLEAN_PARQUET_BY_YEAR", tmp_path / "store" / "clean")
    monkeypatch.setattr(paths, "LOGS_ROOT", tmp_path / "logs")
    (tmp_path / "store" / "clean").mkdir(parents=True, exist_ok=True)
    return pit


def _year_path(pit: Path) -> Path:
    return pit / "08 Parquet" / "certified_adjusted" / "year=2024" / "nifty500_adjusted_daily.parquet"


def test_unapplied_split_is_detected(store: Path) -> None:
    _write(_year_path(store), _unadjusted_series())
    _write(store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
           _reconciliation())
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    found = ca_repair.detect(con, ca_repair.NIFTY500)
    assert len(found["mechanical"]) == 1
    event = found["mechanical"][0]
    assert event["symbol"] == "TESTCO"
    assert event["price_factor"] == 0.5
    assert event["observed_move"] == pytest.approx(103.0 / 204.0 - 1.0, abs=1e-6)
    assert event["move_after_repair"] == pytest.approx(103.0 / 102.0 - 1.0, abs=1e-6)


def test_repair_rescales_history_and_fixes_the_boundary_return(store: Path) -> None:
    _write(_year_path(store), _unadjusted_series())
    _write(store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
           _reconciliation())

    result = ca_repair.repair()
    entry = result["universes"]["nifty500"]
    assert entry["action"] == "REPAIRED"
    assert entry["verified"] is True

    con = duckdb.connect()
    frame = con.execute(
        f"SELECT * FROM read_parquet('{str(_year_path(store)).replace(chr(92), '/')}') ORDER BY Date"
    ).fetchdf()

    # Pre-split closes halved, post-split untouched.
    assert list(frame["Close"])[:3] == pytest.approx([100.0, 101.0, 102.0])
    assert list(frame["Close"])[3:] == pytest.approx([103.0, 104.0])
    # Open/High/Low rescaled with them.
    assert frame["Open"].iloc[0] == pytest.approx(100.0)
    assert frame["High"].iloc[0] == pytest.approx(202.0 * 0.5 * (1.0), abs=1.0)
    # Volume doubled on the pre-split side only.
    assert list(frame["Volume"])[:3] == [2000, 2200, 2400]
    assert list(frame["Volume"])[3:] == [2600, 2700]
    # The factor columns record what was applied.
    assert list(frame["PriceAdjustmentFactor"])[:3] == pytest.approx([0.5, 0.5, 0.5])
    assert list(frame["PriceAdjustmentFactor"])[3:] == pytest.approx([1.0, 1.0])
    # Returns inside the pre-split period are unchanged; only the boundary moves.
    assert frame["AdjustedReturn1D"].iloc[1] == pytest.approx(202.0 / 200.0 - 1.0)
    assert frame["AdjustedReturn1D"].iloc[3] == pytest.approx(103.0 / 102.0 - 1.0)
    assert frame["DiscontinuityClassification"].iloc[3] == ca_repair.REPAIR_NOTE


def test_repair_is_idempotent(store: Path) -> None:
    _write(_year_path(store), _unadjusted_series())
    _write(store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
           _reconciliation())

    ca_repair.repair()
    con = duckdb.connect()
    path = str(_year_path(store)).replace("\\", "/")
    after_first = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY Date").fetchdf()

    second = ca_repair.repair()
    assert second["universes"]["nifty500"]["action"] == "NO_CHANGE"
    after_second = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY Date").fetchdf()
    pd.testing.assert_frame_equal(after_first, after_second)


def test_repair_never_changes_the_row_count(store: Path) -> None:
    _write(_year_path(store), _unadjusted_series())
    _write(store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
           _reconciliation())
    ca_repair.repair()
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{str(_year_path(store)).replace(chr(92), '/')}')"
    ).fetchone()[0]
    assert rows == 5


def test_non_mechanical_break_is_excluded_not_rescaled(store: Path) -> None:
    """A demerger drop is a real print but not a real return. Prices must be left alone and
    the row marked ineligible, because rescaling would need the value of what shareholders
    received, which is not in this dataset."""
    frame = _unadjusted_series()
    _write(_year_path(store), frame)
    _write(
        store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
        _reconciliation(action_type="DEMERGER", price_factor=None),
    )

    result = ca_repair.repair()
    entry = result["universes"]["nifty500"]
    assert entry["mechanical_count"] == 0
    assert entry["non_mechanical_count"] == 1

    con = duckdb.connect()
    out = con.execute(
        f"SELECT * FROM read_parquet('{str(_year_path(store)).replace(chr(92), '/')}') ORDER BY Date"
    ).fetchdf()
    # Prices untouched.
    assert list(out["Close"]) == pytest.approx([200.0, 202.0, 204.0, 103.0, 104.0])
    # Only the boundary row is excluded.
    assert list(out["IsResearchEligible"]) == [True, True, True, False, True]
    assert out["CorporateActionQuarantineReason"].iloc[3] == ca_repair.EXCLUSION_REASON


def test_an_already_adjusted_series_is_left_alone(store: Path) -> None:
    closes = [100.0, 101.0, 102.0, 103.0, 104.0]
    returns = [None] + [closes[i] / closes[i - 1] - 1.0 for i in range(1, 5)]
    frame = _unadjusted_series()
    for column in ("Open", "High", "Low", "Close", "PointInTimePriceEligibilityClose"):
        frame[column] = closes
    frame["AdjustedReturn1D"] = returns
    _write(_year_path(store), frame)
    _write(store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
           _reconciliation())

    result = ca_repair.repair()
    assert result["universes"]["nifty500"]["action"] == "NO_CHANGE"


def test_factor_expression_compounds_two_events_in_the_right_order() -> None:
    events = [
        {"isin": "X", "ex_date": "2020-01-10", "price_factor": 0.5, "volume_factor": 2.0},
        {"isin": "X", "ex_date": "2022-06-01", "price_factor": 0.2, "volume_factor": 5.0},
    ]
    expression = ca_repair._factor_expression(events, price=True)
    # Before the first event both factors apply; between them only the second.
    assert "0.1" in expression
    assert "0.2" in expression
    assert expression.strip().startswith("CASE")


def test_factor_expression_is_neutral_when_there_is_nothing_to_do() -> None:
    assert ca_repair._factor_expression([], price=True) == "1.0"


def test_a_duplicated_ledger_entry_is_applied_only_once(store: Path) -> None:
    """NSE republishes revised corporate actions, so the same split can appear twice for one
    (ISIN, ex-date). Compounding it would scale the history by the factor squared - which is
    exactly what happened the first time drill 4 ran twice against the same store."""
    _write(_year_path(store), _unadjusted_series())
    doubled = pd.concat([_reconciliation(), _reconciliation()], ignore_index=True)
    _write(
        store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
        doubled,
    )

    result = ca_repair.repair()
    assert result["universes"]["nifty500"]["mechanical_count"] == 1

    con = duckdb.connect()
    frame = con.execute(
        f"SELECT * FROM read_parquet('{str(_year_path(store)).replace(chr(92), '/')}') ORDER BY Date"
    ).fetchdf()
    # Halved once (0.5), not twice (0.25).
    assert list(frame["Close"])[:3] == pytest.approx([100.0, 101.0, 102.0])


def test_an_event_filed_under_the_old_isin_is_still_found(store: Path) -> None:
    """A face-value change issues a NEW ISIN, so the event is filed under the old one while
    the price rows already carry the new one. Matching on ISIN alone missed nine real events,
    including HDFC's 2010 split, which left a phantom -79.4% crash in a top-ten constituent."""
    _write(_year_path(store), _unadjusted_series())
    archive = _reconciliation()
    archive["ISIN"] = ["INE999Z01009"]  # the pre-split ISIN, different from the price rows
    _write(
        store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
        archive,
    )

    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    found = ca_repair.detect(con, ca_repair.NIFTY500)
    assert len(found["mechanical"]) == 1
    assert found["mechanical"][0]["symbol"] == "TESTCO"
    # And it reports the price rows' identity, not the stale one from the event.
    assert found["mechanical"][0]["isin"] == "INE999Z01001"


def test_a_ratio_that_does_not_explain_the_move_is_not_applied(store: Path) -> None:
    """Rescaling on a ratio that leaves the boundary at -45% would be guessing. The row is
    excluded instead, with the prices left exactly as the exchange reported them."""
    frame = _unadjusted_series()
    # A -89% print against a 1:5 split. It is close enough to -80% to be recognised as that
    # event, but applying 0.2 would still leave the boundary at roughly -45%. This is AHCL's
    # 2026-04-24 case, reproduced.
    frame.loc[3, ["Open", "High", "Low", "Close", "PointInTimePriceEligibilityClose"]] = 22.5
    frame.loc[3, "AdjustedReturn1D"] = 22.5 / 204.0 - 1.0
    _write(_year_path(store), frame)
    archive = _reconciliation()
    archive["Subject"] = ["Face Value Split From Rs.10/- To Rs.2/-"]
    _write(
        store / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet",
        archive,
    )

    result = ca_repair.repair()
    entry = result["universes"]["nifty500"]
    assert entry["mechanical_count"] == 0
    assert len(entry["unrepairable_residual"]) == 1
    assert "RATIO_DOES_NOT_EXPLAIN" in entry["unrepairable_residual"][0]["handling"]

    con = duckdb.connect()
    out = con.execute(
        f"SELECT * FROM read_parquet('{str(_year_path(store)).replace(chr(92), '/')}') ORDER BY Date"
    ).fetchdf()
    assert list(out["Close"])[:3] == pytest.approx([200.0, 202.0, 204.0])  # untouched
    assert bool(out["IsResearchEligible"].iloc[3]) is False
