"""Failure drills: rejected builds must not replace published signals."""
from datetime import date, timedelta

import pandas as pd
import pytest

from vajra_regime.cloud import daily
from vajra_regime.cloud.state import StatePaths


def seed(tmp_path, day):
    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir()
    pd.DataFrame({"Date": [day], "Symbol": ["AAA"], "ISIN": ["OLD"]}).to_parquet(paths.prices)
    return paths


def table(day):
    frame = pd.DataFrame({
        "RANK": list(range(1, 501)), "SYMBOL": [f"S{i}" for i in range(500)],
        "ISIN": [f"I{i}" for i in range(500)], "ELIGIBLE": ["HAAN"] * 500,
        "SERIES": ["EQ"] * 500,
    })
    frame.attrs["asof"] = day
    return frame


def test_failed_gate_preserves_published_bytes(tmp_path, monkeypatch):
    day = date(2026, 9, 8)
    seed(tmp_path, day)
    out = tmp_path / "out"
    out.mkdir()
    names = ["latest_signals.csv", "universe_current.csv", "status.json"]
    for name in names:
        (out / name).write_bytes(b"previous validated output")
    bad = table(day).iloc[:3].copy()
    monkeypatch.setattr(daily, "refresh_corporate_actions", lambda *a: 0)
    monkeypatch.setattr(daily, "refresh_reference", lambda *a: 0)
    monkeypatch.setattr(daily.signal, "rank_table", lambda *a: bad)
    with pytest.raises(SystemExit, match="CLOUD SIGNAL"):
        daily.run(tmp_path, day, tmp_path / "scratch")
    for name in names:
        assert (out / name).read_bytes() == b"previous validated output"


@pytest.mark.parametrize("offset", [-1, 46])
def test_out_of_bounds_gap_stops_before_fetch(tmp_path, monkeypatch, offset):
    day = date(2026, 9, 8)
    paths = seed(tmp_path, day - timedelta(days=offset))
    previous = paths.prices.read_bytes()
    def unexpected(*args):
        pytest.fail("invalid catch-up must not access network")
    monkeypatch.setattr(daily, "_bhavcopy_for", unexpected)
    with pytest.raises(RuntimeError):
        daily.run(tmp_path, day, tmp_path / "scratch")
    assert paths.prices.read_bytes() == previous


def test_known_isin_transition_keeps_corporate_action(tmp_path):
    paths = seed(tmp_path, date(2026, 9, 8))
    pd.DataFrame({"Symbol": ["AAA", "AAA", "REUSED", "REUSED"],
                  "ISIN": ["OLD", "NEW", "OTHER1", "OTHER2"]}).to_parquet(paths.prices)
    pd.DataFrame({"SourceISIN": ["OLD", "NEW"],
                  "CanonicalISIN": ["OLD", "OLD"]}).to_parquet(paths.isin_lineage)
    events = pd.DataFrame({"Symbol": ["AAA", "REUSED"]})
    result = daily._attach_isin(paths, events)
    assert result[["Symbol", "ISIN"]].to_dict("records") == [{"Symbol": "AAA", "ISIN": "OLD"}]


@pytest.mark.parametrize("defect", ["duplicate", "extra", "series", "ineligible", "future"])
def test_gate_checks_actual_trade_rows(defect):
    day = date(2026, 9, 8)
    frame = table(day)
    if defect == "duplicate":
        frame.loc[1, "RANK"] = 1
    elif defect == "extra":
        frame.loc[20, "RANK"] = 20
    elif defect == "series":
        frame.loc[0, "SERIES"] = "BE"
    elif defect == "ineligible":
        frame.loc[0, "ELIGIBLE"] = "NAHI"
    status = {"eligible": 500, "top_symbols": frame.SYMBOL.head(20).tolist()}
    with pytest.raises(SystemExit):
        daily._gate(status, frame, day + timedelta(days=defect == "future"), day)


def test_valid_gate_accepts_twenty_unique_names():
    day = date(2026, 9, 8)
    daily._gate({"eligible": 500}, table(day), day, day)
