"""Cloud par naye ISIN badlav ka naksha -- laptop ke bina (14-Sep-2026)."""
from datetime import date, timedelta

import pandas as pd
import pytest

from vajra_regime.cloud import daily, lineage_update, signal
from vajra_regime.cloud.state import PRICE_COLUMNS, StatePaths

OLD = "INE123A01018"
NEW = "INE123A01026"
NEWER = "INE123A01034"
OTHER = "INE999Z01011"
D0 = date(2026, 9, 1)


def _rows(symbol, isin, start, days, close=100.0):
    return [{"Date": start + timedelta(days=n), "ISIN": isin, "Symbol": symbol,
             "Series": "EQ", "Open": close, "High": close, "Low": close,
             "Close": close, "Volume": 1000, "TurnoverINR": close * 1000,
             "Traded": True, "IsFrozenBar": False, "AdjustedThrough": pd.NaT,
             "EngineQuarantined": False} for n in range(days)]


def _store(tmp_path, rows, lineage):
    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows)[list(PRICE_COLUMNS)].to_parquet(paths.prices, index=False)
    pd.DataFrame(lineage, columns=["SourceISIN", "CanonicalISIN"]).to_parquet(
        paths.isin_lineage, index=False)
    return paths


def _naksha(paths):
    frame = pd.read_parquet(paths.isin_lineage)
    return dict(zip(frame["SourceISIN"], frame["CanonicalISIN"]))


def _split_store(tmp_path, **kw):
    rows = (_rows("AAA", OLD, D0, 5, kw.get("old_close", 100.0))
            + _rows("AAA", NEW, D0 + timedelta(days=6), 3, kw.get("new_close", 100.0)))
    return _store(tmp_path, rows, lineage=[(OLD, OLD)])


def test_face_value_isin_change_joins_same_company(tmp_path):
    paths = _split_store(tmp_path)
    report = lineage_update.extend(paths)
    assert _naksha(paths)[NEW] == OLD
    assert [x["NewISIN"] for x in report["added"]] == [NEW]
    assert report["unresolved"] == []


def test_chain_of_changes_stays_on_first_company(tmp_path):
    rows = (_rows("AAA", OLD, D0, 3) + _rows("AAA", NEW, D0 + timedelta(days=4), 3)
            + _rows("AAA", NEWER, D0 + timedelta(days=8), 3))
    paths = _store(tmp_path, rows, lineage=[(OLD, OLD)])
    lineage_update.extend(paths)
    naksha = _naksha(paths)
    assert naksha[NEW] == OLD
    assert naksha[NEWER] == OLD


def test_different_issuer_is_not_joined_but_reported(tmp_path):
    rows = _rows("AAA", OLD, D0, 3) + _rows("AAA", OTHER, D0 + timedelta(days=4), 3)
    paths = _store(tmp_path, rows, lineage=[(OLD, OLD)])
    report = lineage_update.extend(paths)
    assert OTHER not in _naksha(paths)
    assert report["added"] == []
    assert [x["NewISIN"] for x in report["unresolved"]] == [OTHER]


@pytest.mark.parametrize("start", [D0 + timedelta(days=2), D0 + timedelta(days=30)],
                         ids=["overlap", "gap-over-20-days"])
def test_overlap_or_long_gap_is_not_joined(tmp_path, start):
    rows = _rows("AAA", OLD, D0, 5) + _rows("AAA", NEW, start, 3)
    paths = _store(tmp_path, rows, lineage=[(OLD, OLD)])
    report = lineage_update.extend(paths)
    assert NEW not in _naksha(paths)
    assert report["added"] == []


def test_laptop_decision_wins_over_rule(tmp_path):
    rows = _rows("AAA", OLD, D0, 3) + _rows("AAA", NEW, D0 + timedelta(days=4), 3)
    paths = _store(tmp_path, rows, lineage=[(OLD, OLD), (NEW, NEW)])
    report = lineage_update.extend(paths)
    assert _naksha(paths)[NEW] == NEW
    assert report == {"added": [], "unresolved": []}


def test_second_run_leaves_file_untouched(tmp_path):
    paths = _split_store(tmp_path)
    lineage_update.extend(paths)
    before = paths.isin_lineage.read_bytes()
    assert lineage_update.extend(paths)["added"] == []
    assert paths.isin_lineage.read_bytes() == before


def test_joined_change_keeps_split_event_and_one_adjusted_series(tmp_path):
    # 1:5 split: purana bhaav 500, naya 100. Jodne ke baad ek hi company, aur
    # split ka event us company par lag kar series sapaat 100 deti hai.
    paths = _split_store(tmp_path, old_close=500.0, new_close=100.0)
    events = pd.DataFrame({"Symbol": ["AAA"]})
    assert daily._attach_isin(paths, events).empty      # pehle: event chhoot jaata

    lineage_update.extend(paths)
    assert daily._attach_isin(paths, events)["ISIN"].tolist() == [OLD]

    ex = D0 + timedelta(days=6)
    pd.DataFrame({"EventId": ["E1"], "ISIN": [OLD], "Symbol": ["AAA"], "ExDate": [ex],
                  "PriceFactor": [0.2], "VolumeFactor": [5.0], "ActionType": ["SPLIT"],
                  "ParseStatus": ["OK"]}).to_parquet(paths.events, index=False)
    frame = signal.adjusted_frame(paths)
    dates = pd.to_datetime(frame["Date"])
    assert set(frame["ISIN"]) == {OLD}
    assert set(frame.loc[dates >= pd.Timestamp(ex), "SourceISIN"]) == {NEW}
    assert frame["Close"].round(6).eq(100.0).all()


def test_daily_run_joins_lineage_before_corporate_actions(tmp_path, monkeypatch):
    day = D0 + timedelta(days=8)
    paths = _split_store(tmp_path)
    order = []
    real_extend = lineage_update.extend

    def spy_extend(p):
        order.append("lineage")
        return real_extend(p)

    def spy_ca(p, today):
        order.append("ca")
        assert _naksha(p)[NEW] == OLD
        return 0

    monkeypatch.setattr(daily.lineage_update, "extend", spy_extend)
    monkeypatch.setattr(daily, "_bhavcopy_for", lambda *a: None)
    monkeypatch.setattr(daily, "refresh_corporate_actions", spy_ca)
    monkeypatch.setattr(daily, "refresh_reference", lambda *a: 0)
    monkeypatch.setattr(daily.signal, "rank_table", lambda *a: pytest.fail("stop here"))
    with pytest.raises(pytest.fail.Exception):
        daily.run(tmp_path, day, tmp_path / "scratch")
    assert order == ["lineage", "ca"]
