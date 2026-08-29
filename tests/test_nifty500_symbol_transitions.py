from __future__ import annotations

import inspect

from vajra_regime.nifty500_migration import symbol_transitions


def test_symbol_transition_builder_is_identity_only() -> None:
    source = inspect.getsource(symbol_transitions.build_symbol_transitions)
    assert "IDENTITY_ONLY_NO_MEMBERSHIP_COUNT_CHANGE" in source
