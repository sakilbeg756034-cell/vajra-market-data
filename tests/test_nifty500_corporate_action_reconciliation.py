from __future__ import annotations

import pytest

from vajra_regime.nifty500_migration.corporate_action_reconciliation import classify_official_action


@pytest.mark.parametrize(
    ("subject", "action_type", "factor"),
    [
        ("Bonus 1:1", "BONUS", 0.5),
        ("Dividend Rs 1.40 Per Share / Bonus 1:1", "BONUS", 0.5),
        ("Fv Split Rs.10/- To Re.1/", "SPLIT", 0.1),
        ("Agm/Div-Rs.2/Fv Rs10tors5", "SPLIT", 0.5),
        ("Face Valus Split (Sub-Division) - From Rs 10/- Per To Rs 2/- Per Share", "SPLIT", 0.2),
        ("Bonus 1:1/Fv Spl Rs.5tore.1", "BONUS_AND_SPLIT", 0.1),
        ("Face Value Consolidation From Re 1 To Rs 10", "CONSOLIDATION", 10.0),
    ],
)
def test_mechanical_action_parser(subject: str, action_type: str, factor: float) -> None:
    parsed = classify_official_action(subject)
    assert parsed.action_type == action_type
    assert parsed.price_factor == pytest.approx(factor)
    assert parsed.volume_factor == pytest.approx(1 / factor)
    assert parsed.parse_status == "PARSED"


def test_complex_actions_are_review_only() -> None:
    assert classify_official_action("Demerger").parse_status == "REVIEW"
    assert classify_official_action("Rights 1:5").action_type == "RIGHTS"
    assert classify_official_action("Scheme of Amalgamation Bonus 4:5").action_type == (
        "MERGER_OR_AMALGAMATION"
    )


def test_non_equity_bonus_is_not_silently_adjusted() -> None:
    parsed = classify_official_action("Scheme Of Arrangement - Bonus Debentures 6:1")
    assert parsed.action_type == "OTHER_INFORMATIONAL"
    assert parsed.price_factor == 1.0
