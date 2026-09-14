"""Cloud daily run: NSE ki chhoti gadbad par retry, aur naya din na ho to turant rukna.

14-Sep-2026. Workflow ab 16:37 IST se har 15 minute chalta hai (NSE ~16:33 par
bhavcopy daalta hai). Isliye (1) bekaar koshish NSE ke API ko nahi chhooni
chahiye, aur (2) API ka ek jhatka poora signal nahi girana chahiye.
"""
import json
import urllib.error
from datetime import date, timedelta

import pandas as pd
import pytest

from vajra_regime.cloud import daily
from vajra_regime.cloud.state import StatePaths

D = date(2026, 9, 11)


def _flaky(monkeypatch, fail_times, exc=None):
    calls = {"n": 0}

    def fetch(opener, start, end, timeout_seconds=60):
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise exc or urllib.error.URLError("timed out")
        return b"", [{"symbol": "AAA"}]

    sleeps = []
    monkeypatch.setattr(daily.ca, "_nse_opener", lambda: object())
    monkeypatch.setattr(daily.ca, "_fetch_ca_json", fetch)
    monkeypatch.setattr(daily, "_sleep", sleeps.append)
    return calls, sleeps


def test_ca_feed_recovers_after_two_network_failures(monkeypatch):
    _calls, sleeps = _flaky(monkeypatch, 2)
    rows = daily._fetch_ca_rows(D - timedelta(days=120), D)
    assert rows and all(r == {"symbol": "AAA"} for r in rows)
    assert sleeps == [30, 90]


def test_ca_feed_stops_loudly_after_three_failures(monkeypatch):
    _calls, sleeps = _flaky(monkeypatch, 10_000)
    with pytest.raises(RuntimeError, match="3 koshish"):
        daily._fetch_ca_rows(D - timedelta(days=120), D)
    assert sleeps == [30, 90]


def test_programming_errors_are_not_retried(monkeypatch):
    _calls, sleeps = _flaky(monkeypatch, 1, exc=KeyError("bug"))
    with pytest.raises(KeyError):
        daily._fetch_ca_rows(D - timedelta(days=120), D)
    assert sleeps == []


def _published(tmp_path, asof):
    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True)
    pd.DataFrame({"Date": [asof], "Symbol": ["AAA"],
                  "ISIN": ["INE000A01011"]}).to_parquet(paths.prices)
    out = tmp_path / "out"
    out.mkdir()
    (out / "status.json").write_text(json.dumps({"as_of_session": asof.isoformat()}),
                                     encoding="utf-8")
    (out / "latest_signals.csv").write_bytes(b"published")
    return paths, out


def test_no_new_session_stops_before_any_nse_api_or_rebuild(tmp_path, monkeypatch):
    paths, out = _published(tmp_path, D)
    monkeypatch.setattr(daily, "_bhavcopy_for", lambda *a: None)     # aaj ki file abhi nahi aayi
    for name in ("refresh_corporate_actions", "refresh_reference"):
        monkeypatch.setattr(daily, name, lambda *a: pytest.fail("NSE API must not be called"))
    monkeypatch.setattr(daily.lineage_update, "extend", lambda *a: pytest.fail("no rebuild"))
    monkeypatch.setattr(daily.signal, "rank_table", lambda *a: pytest.fail("no rebuild"))
    before = paths.prices.read_bytes()

    status = daily.run(tmp_path, D + timedelta(days=3), tmp_path / "scratch")

    assert status["skipped"] is True
    assert status["as_of_session"] == D.isoformat()
    assert (out / "latest_signals.csv").read_bytes() == b"published"
    assert paths.prices.read_bytes() == before


def test_force_rebuilds_even_without_new_session(tmp_path, monkeypatch):
    _published(tmp_path, D)
    monkeypatch.setattr(daily, "_bhavcopy_for", lambda *a: None)
    monkeypatch.setattr(daily.lineage_update, "extend", lambda *a: {"added": [], "unresolved": []})
    monkeypatch.setattr(daily, "refresh_corporate_actions",
                        lambda *a: pytest.fail("rebuild reached"))
    with pytest.raises(pytest.fail.Exception, match="rebuild reached"):
        daily.run(tmp_path, D + timedelta(days=3), tmp_path / "scratch", force=True)


def test_unreadable_status_never_counts_as_published(tmp_path, monkeypatch):
    _paths, out = _published(tmp_path, D)
    (out / "status.json").write_bytes(b"previous validated output")
    monkeypatch.setattr(daily, "_bhavcopy_for", lambda *a: None)
    monkeypatch.setattr(daily.lineage_update, "extend", lambda *a: {"added": [], "unresolved": []})
    monkeypatch.setattr(daily, "refresh_corporate_actions",
                        lambda *a: pytest.fail("rebuild reached"))
    with pytest.raises(pytest.fail.Exception, match="rebuild reached"):
        daily.run(tmp_path, D + timedelta(days=3), tmp_path / "scratch")


def test_cli_force_flag_reaches_run(tmp_path, monkeypatch, capsys):
    seen = {}

    def fake_run(root, today, scratch, force=False):
        seen["force"] = force
        return {"ok": True}

    monkeypatch.setattr(daily, "run", fake_run)
    assert daily.main(["--root", str(tmp_path), "--today", "2026-09-14", "--force"]) == 0
    assert seen == {"force": True}


def _no_nse_no_rebuild(monkeypatch):
    monkeypatch.setattr(daily, "_bhavcopy_for", lambda *a: None)
    for name in ("refresh_corporate_actions", "refresh_reference"):
        monkeypatch.setattr(daily, name, lambda *a: pytest.fail("NSE API must not be called"))
    monkeypatch.setattr(daily.lineage_update, "extend", lambda *a: pytest.fail("no rebuild"))
    monkeypatch.setattr(daily.signal, "rank_table", lambda *a: pytest.fail("no rebuild"))


def test_skip_path_still_fails_loudly_when_data_is_stale(tmp_path, monkeypatch):
    # NSE hafton data na de: pehle har koshish chup-chaap "skipped" deti thi.
    paths, out = _published(tmp_path, D)
    _no_nse_no_rebuild(monkeypatch)
    before = paths.prices.read_bytes()
    with pytest.raises(SystemExit, match="data aage badha hi nahi"):
        daily.run(tmp_path, D + timedelta(days=daily.MAX_SIGNAL_AGE_DAYS + 1), tmp_path / "scratch")
    assert (out / "latest_signals.csv").read_bytes() == b"published"
    assert paths.prices.read_bytes() == before


def test_skip_path_at_the_age_limit_still_skips(tmp_path, monkeypatch):
    _published(tmp_path, D)
    _no_nse_no_rebuild(monkeypatch)
    status = daily.run(tmp_path, D + timedelta(days=daily.MAX_SIGNAL_AGE_DAYS), tmp_path / "scratch")
    assert status["skipped"] is True


def test_gate_and_skip_path_share_one_age_rule():
    assert daily.MAX_SIGNAL_AGE_DAYS == 10
    assert daily._stale_problem(D, D + timedelta(days=10)) is None
    assert "data aage badha hi nahi" in daily._stale_problem(D, D + timedelta(days=11))
    table = pd.DataFrame({
        "RANK": list(range(1, 501)), "SYMBOL": [f"S{i}" for i in range(500)],
        "ISIN": [f"I{i}" for i in range(500)], "ELIGIBLE": ["HAAN"] * 500,
        "SERIES": ["EQ"] * 500,
    })
    daily._gate({"eligible": 500}, table, D, D + timedelta(days=10))
    with pytest.raises(SystemExit, match="data aage badha hi nahi"):
        daily._gate({"eligible": 500}, table, D, D + timedelta(days=11))
