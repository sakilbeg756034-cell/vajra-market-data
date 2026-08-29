from __future__ import annotations

from vajra_regime.nifty500_migration.foundation_certification import _grade


def test_year_grade_retains_pre_anchor_uncertainty() -> None:
    assert _grade(2012, 0.99, 0.99) == "RECONSTRUCTED_MEDIUM_CONFIDENCE"
    assert _grade(2013, 0.99, 0.99) == "RECONSTRUCTED_MEDIUM_CONFIDENCE"


def test_year_grade_distinguishes_official_anchor_and_reconstruction_periods() -> None:
    assert _grade(2020, 0.999, 0.999) == "VERIFIED_MULTI_SOURCE"
    assert _grade(2025, 0.999, 0.999) == "RECONSTRUCTED_HIGH_CONFIDENCE"
    assert _grade(2025, 0.90, 0.999) == "INSUFFICIENT"
