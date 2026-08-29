from __future__ import annotations

from datetime import date

from vajra_regime.nifty500_migration.timeline import (
    _identity_equivalent,
    _membership_intervals,
    _reverse_to_start,
)


def test_reverse_event_removes_inclusion_and_restores_exclusion() -> None:
    events = {
        date(2020, 1, 2): [
            {
                "source_file": "official.pdf",
                "exclusion_set": {"OLD"},
                "inclusion_set": {"NEW"},
            }
        ]
    }
    state, exceptions = _reverse_to_start(
        anchor_members={"KEEP", "NEW"},
        anchor_date=date(2020, 1, 3),
        start=date(2020, 1, 1),
        events=events,
    )
    assert state == {"KEEP", "OLD"}
    assert exceptions == []


def test_membership_intervals_are_half_open() -> None:
    states = [
        {
            "effective_date": "2020-01-01",
            "members": {"A"},
            "confidence_grade": "ONE",
            "evidence_source": "x",
        },
        {
            "effective_date": "2020-02-01",
            "members": {"B"},
            "confidence_grade": "TWO",
            "evidence_source": "y",
        },
    ]
    intervals = _membership_intervals(states, as_of=date(2020, 2, 2))
    assert intervals[0]["valid_to_exclusive"] == "2020-02-01"
    assert intervals[1]["valid_to_exclusive"] == "2020-02-03"


def test_symbol_aliases_can_be_economically_identical() -> None:
    identity = {"OLD": "INE000000001", "NEW": "INE000000001"}
    assert _identity_equivalent({"KEEP", "OLD"}, {"KEEP", "NEW"}, identity)
    assert not _identity_equivalent({"KEEP", "OLD"}, {"KEEP", "OTHER"}, identity)
