from __future__ import annotations

from vajra_regime.nifty500_migration.adjusted_ohlcv import _relevant_eod2_sources


def test_relevant_eod2_sources_match_case_insensitively(tmp_path) -> None:
    (tmp_path / "abc.csv").write_text("Date,Open,High,Low,Close,Volume\n", encoding="utf-8")
    (tmp_path / "xyz.csv").write_text("Date,Open,High,Low,Close,Volume\n", encoding="utf-8")
    identity_payload = {
        "sym2isin": {"ABC": "ISIN1"},
        "isin2hist": {
            "ISIN1": [
                {"symbol": "OLDABC", "from_date": "2009-01-01", "to_date": "2020-01-01"},
                {"symbol": "ABC", "from_date": "2020-01-02", "to_date": "2026-01-01"},
            ]
        },
    }
    paths, missing, metadata = _relevant_eod2_sources(
        {"OLDABC", "MISSING"},
        {"ISIN1"},
        identity_payload,
        eod2_root=tmp_path,
    )
    assert [path.name for path in paths] == ["abc.csv"]
    assert missing == {"MISSING"}
    assert metadata["ABC"]["identity_isins"] == {"ISIN1"}
    assert metadata["ABC"]["historical_symbols"] == {"ABC", "OLDABC"}
