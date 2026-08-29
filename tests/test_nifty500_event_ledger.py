from __future__ import annotations

from vajra_regime.nifty500_migration.event_ledger import MANUAL_EVENTS, SUPERSEDED_OR_NON_EVENTS, _split


def test_manual_events_are_balanced_and_unique() -> None:
    for event in MANUAL_EVENTS:
        exclusions = _split(event["exclusions"])
        inclusions = _split(event["inclusions"])
        expected_delta = event.get("documented_member_count_delta", 0)
        assert len(inclusions) - len(exclusions) == expected_delta
        assert set(exclusions).isdisjoint(inclusions)


def test_correction_and_nullification_not_applied_as_events() -> None:
    assert "ind_prs09092009.pdf" in SUPERSEDED_OR_NON_EVENTS
    assert "ind_prs12032020.pdf" in SUPERSEDED_OR_NON_EVENTS
    assert "ind_prs18022020.pdf" in SUPERSEDED_OR_NON_EVENTS
    assert "ind_prs13052020.pdf" in SUPERSEDED_OR_NON_EVENTS
