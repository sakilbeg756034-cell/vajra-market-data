from __future__ import annotations

import json
from datetime import date

from vajra_regime.nifty500_migration.corporate_action_archive import _chunks, _response_rows


def test_chunks_are_contiguous_and_bounded() -> None:
    chunks = _chunks(date(2020, 1, 1), date(2020, 7, 1), days=90)
    assert chunks[0][0] == date(2020, 1, 1)
    assert chunks[-1][1] == date(2020, 7, 1)
    assert all(
        left[1].toordinal() + 1 == right[0].toordinal()
        for left, right in zip(chunks, chunks[1:], strict=False)
    )


def test_response_rows_accepts_official_data_wrapper() -> None:
    payload = json.dumps({"data": [{"symbol": "ABC"}]}).encode()
    assert _response_rows(payload) == [{"symbol": "ABC"}]
