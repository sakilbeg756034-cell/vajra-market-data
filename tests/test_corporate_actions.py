from __future__ import annotations

import math
from datetime import date

import pandas as pd

from vajra_regime.corporate_actions import (
    classify_adjustment,
    normalize_corporate_action_rows,
    reconcile_corporate_actions,
)


def test_bonus_factor_is_backward_price_adjustment() -> None:
    parsed = classify_adjustment("Bonus 2:1")
    assert parsed.action_type == "BONUS"
    assert parsed.parse_status == "PARSED"
    assert math.isclose(parsed.price_factor or 0, 1 / 3)
    assert math.isclose(parsed.volume_factor or 0, 3.0)


def test_split_factor_is_backward_price_adjustment() -> None:
    parsed = classify_adjustment(
        "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share"
    )
    assert parsed.action_type == "SPLIT"
    assert parsed.parse_status == "PARSED"
    assert math.isclose(parsed.price_factor or 0, 0.2)
    assert math.isclose(parsed.volume_factor or 0, 5.0)


def test_complex_actions_are_not_auto_adjusted() -> None:
    demerger = classify_adjustment("Demerger")
    rights = classify_adjustment("Rights 1:4 @ Premium Rs 25")
    dividend = classify_adjustment("Interim Dividend - Rs 11 Per Share")
    assert demerger.parse_status == "REVIEW"
    assert rights.parse_status == "REVIEW"
    assert dividend.parse_status == "NO_ADJUST"
    assert dividend.price_factor == 1.0


def test_normalize_flexible_nse_fields() -> None:
    frame = normalize_corporate_action_rows(
        [
            {
                "symbol": "ABC",
                "series": "EQ",
                "subject": "Bonus 1:1",
                "exDate": "15-Jul-2026",
                "recDate": "15-Jul-2026",
                "faceVal": "10",
                "comp": "ABC Limited",
            }
        ]
    )
    assert len(frame) == 1
    assert frame.iloc[0]["Symbol"] == "ABC"
    assert frame.iloc[0]["ExDate"] == pd.Timestamp(date(2026, 7, 15))


def test_reconciliation_marks_supported_bonus_auto_ready() -> None:
    actions = normalize_corporate_action_rows(
        [
            {
                "symbol": "ABC",
                "series": "EQ",
                "subject": "Bonus 1:1",
                "exDate": "15-Jul-2026",
                "recDate": "15-Jul-2026",
            }
        ]
    )
    symbol_history = pd.DataFrame(
        [
            {
                "ISIN": "INE000A01001",
                "Symbol": "ABC",
                "FirstDate": "2020-01-01",
                "LastDate": "2026-08-06",
                "LatestSeries": "EQ",
            }
        ]
    )
    prices = pd.DataFrame(
        [
            {"Date": "2026-07-14", "ISIN": "INE000A01001", "Symbol": "ABC", "Close": 100.0},
            {"Date": "2026-07-15", "ISIN": "INE000A01001", "Symbol": "ABC", "Close": 51.0},
        ]
    )
    result = reconcile_corporate_actions(actions, symbol_history, prices)
    assert result.iloc[0]["MatchStatus"] == "MATCHED_UNIQUE_ISIN"
    assert result.iloc[0]["Decision"] == "AUTO_READY_SPLIT_BONUS"
    assert math.isclose(result.iloc[0]["PostVsAdjustedPreGap"], 0.02)
