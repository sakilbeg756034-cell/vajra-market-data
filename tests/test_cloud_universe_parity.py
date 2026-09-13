from __future__ import annotations

import pandas as pd

from vajra_regime.cloud import signal


def _row(date: str, isin: str, median: float, series: str = "EQ", quarantined: bool = False):
    return {
        "Date": pd.Timestamp(date),
        "ISIN": isin,
        "MedianTurnover60": median,
        "TurnoverObservations60": 60,
        "RowsInStore": 300,
        "LastTradedDate": pd.Timestamp(date),
        "Series": series,
        "EngineQuarantined": quarantined,
    }


def test_monthly_membership_filters_before_filling_top_slots(monkeypatch):
    monkeypatch.setattr(signal, "UNIVERSE_SIZE", 1)
    rows = []
    for date in ("2026-01-30", "2026-02-02"):
        rows.extend([
            _row(date, "A-BE", 100.0, series="BE"),
            _row(date, "B-QUARANTINED", 90.0, quarantined=True),
            _row(date, "C-ELIGIBLE", 80.0),
        ])

    membership = signal.vajra750_membership(pd.DataFrame(rows), seed=None)

    assert not membership.loc[pd.Timestamp("2026-02-02"), "A-BE"]
    assert not membership.loc[pd.Timestamp("2026-02-02"), "B-QUARANTINED"]
    assert membership.loc[pd.Timestamp("2026-02-02"), "C-ELIGIBLE"]


def test_latest_security_snapshot_is_used_at_market_month_end(monkeypatch):
    monkeypatch.setattr(signal, "UNIVERSE_SIZE", 1)
    rows = [
        _row("2026-01-29", "A", 100.0),
        _row("2026-01-30", "B", 80.0),
        _row("2026-02-02", "A", 100.0),
        _row("2026-02-02", "B", 80.0),
    ]

    membership = signal.vajra750_membership(pd.DataFrame(rows), seed=None)

    assert membership.loc[pd.Timestamp("2026-02-02"), "A"]
    assert not membership.loc[pd.Timestamp("2026-02-02"), "B"]
