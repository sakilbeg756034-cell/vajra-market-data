"""ca_subject -- laptop aur cloud ka EKMATRA corporate-action tark (25-Sep-2026).

Har misaal NSE feed ki ASLI line hai.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from vajra_regime import ca_subject
from vajra_regime.cloud import daily, signal
from vajra_regime.cloud.state import StatePaths


@pytest.mark.parametrize("subject, factor", [
    ("Fv Splt Frm Rs 10 To Re 1", 0.1),                       # JSWSTEEL 2017 -- pehle chhoot-ta tha
    ("Fv Spl-Rs10tors5/Bon-2:1", 0.5 / 3),                    # ENGINERSIN 2010
    ("Face Value Split Rs.10/- To Re.1/- Per Share", 0.1),    # SBIN 2014 -- pehle quarantine
    ("Bonus 2:1/Dividend- Rs 1.60 Per Share", 1 / 3),         # purana cloud: sirf DIVIDEND
    ("Bonus 1:1 And Face Value Split From Rs.10 To Re.1", 0.05),  # purana cloud: sirf 0.5
    ("Consolidation Of Equity Shares From Re 1 Per Share To Rs 10 Per Share", 10.0),
])
def test_factor(subject, factor):
    f, _, _ = ca_subject.price_factor(subject)
    assert f == pytest.approx(factor)


@pytest.mark.parametrize("subject", [
    "Div-Fin Rs.5+Spl Rs.10   Purpose Revised",       # special dividend, split nahi
    "Demerger", "Rights 2:1 @ Premium Rs 2/-",
    "Capital Reduction Rs 10 To Rs 3.30 / Consolidation Rs 3.30 To Rs.10",
    "Bon 1 Dvr : 20 Eq Shares", "Bonus Preference Shares 21:1",
])
def test_no_factor(subject):
    assert ca_subject.price_factor(subject)[0] is None


def test_event_columns_dividend_ek_aur_review_none():
    cols = ca_subject.event_columns(["Dividend - Rs 2 Per Share", "Demerger", "Bonus 1:1"])
    assert cols["PriceFactor"] == [1.0, None, 0.5]
    assert cols["ParseStatus"] == ["PARSED", "REVIEW", "PARSED"]
    assert cols["ActionType"][1] in ca_subject.UNRATIOED_KINDS


@pytest.mark.parametrize("subject, op, pc, want", [
    ("Demerger", 650.0, 1485.0, 650.0 / 1485.0),              # STAR 2024-12-06
    ("Demerger", 99.0, 100.0, None),                          # 3% se kam gira -> nahi
    ("Scheme Of Arrangement", 660.0, 682.0, None),            # scheme: 25% se kam -> nahi
    ("Scheme Of Arrangement", 300.0, 682.0, 300.0 / 682.0),
    ("Scheme Of Amalgamation", 300.0, 682.0, None),           # merger -- kabhi nahi
    ("Demerger", 1.0, 100.0, None),                           # 98%+ -> data par shak
])
def test_demerger_price_factor(subject, op, pc, want):
    got = ca_subject.demerger_price_factor(subject, op, pc)
    assert (got is None and want is None) or got == pytest.approx(want)


def test_cloud_quarantine_naye_kism_jaanta_hai():
    for kind in ("REVIEW_NEEDED", "BONUS_GAIR_EQUITY", "CONSOLIDATION", "DEMERGER"):
        assert kind in signal.UNRATIOED_ACTION_TYPES


def test_cloud_demerger_factor_store_se(tmp_path: Path):
    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True)
    pd.DataFrame({
        "Date": [date(2024, 12, 5), date(2024, 12, 6), date(2024, 12, 5)],
        "ISIN": ["INE939A01011", "INE939A01011", "INEOTHER0001"],
        "Open": [1290.0, 650.0, 10.0], "Close": [1485.0, 682.0, 10.0],
    }).to_parquet(paths.prices)
    events = pd.DataFrame({
        "EventId": ["e1", "e2", "e3"],
        "ISIN": ["INE939A01011", "INE939A01011", "INEOTHER0001"],
        "Symbol": ["STAR", "STAR", "X"],
        "ExDate": [date(2024, 12, 6), date(2025, 1, 9), date(2024, 12, 6)],
        **ca_subject.event_columns(["Demerger", "Demerger", "Dividend Rs 2"]),
    })
    out = daily._demerger_factors(paths, events, ["Demerger", "Demerger", "Dividend Rs 2"])
    assert out.at[0, "PriceFactor"] == pytest.approx(650.0 / 1485.0)
    assert out.at[0, "ActionType"] == "DEMERGER_BHAAV"
    assert pd.isna(out.at[1, "PriceFactor"])          # ex-date ka bhaav abhi store me nahi
    assert out.at[2, "PriceFactor"] == 1.0            # dividend jaisa tha waisa
