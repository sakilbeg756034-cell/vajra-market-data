from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from vajra_regime import publish, publish_docs


def _frame(rows: int = 6, *, symbol: str = "AAA", isin: str = "INE000A01001") -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=rows)
    return pd.DataFrame(
        {
            "Date": [d.date() for d in dates],
            "Symbol": [symbol] * rows,
            "ISIN": [isin] * rows,
            "Open": [100.0 + i for i in range(rows)],
            "High": [101.0 + i for i in range(rows)],
            "Low": [99.0 + i for i in range(rows)],
            "Close": [100.5 + i for i in range(rows)],
            "Volume": [1000 + i for i in range(rows)],
            # The store writes "" here and the published files must carry NULL instead,
            # otherwise Parquet and CSV disagree on read-back.
            "CorporateActionQuarantineReason": [""] * rows,
            "year": [2024] * rows,
        }
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("f", frame)
    con.execute(f"COPY (SELECT * FROM f) TO '{str(path).replace(chr(92), '/')}' (FORMAT PARQUET)")


# --------------------------------------------------------------------------- year labels


@pytest.mark.parametrize(
    ("universe", "year", "expected"),
    [
        ("nifty500", 2009, "PRICE_DATA_ONLY"),
        ("nifty500", 2012, "PRICE_DATA_ONLY"),
        ("nifty500", 2013, "PARTIAL"),
        ("nifty500", 2014, "BACKTEST_SAFE"),
        ("nifty500", 2026, "BACKTEST_SAFE"),
        ("nifty750", 2009, "PRICE_DATA_ONLY"),
        ("nifty750", 2010, "PARTIAL"),
        ("nifty750", 2011, "BACKTEST_SAFE"),
    ],
)
def test_year_labels_follow_the_membership_anchor(universe: str, year: int, expected: str) -> None:
    label, basis = publish.year_label(universe, year)
    assert label == expected
    assert basis


def test_the_anchor_is_the_documented_date() -> None:
    """If this ever changes, every year label and the whole of START_HERE_AI.md changes with
    it, so it is pinned deliberately."""
    assert date(2013, 4, 18) == publish.OFFICIAL_MEMBERSHIP_ANCHOR
    assert date(2010, 8, 31) == publish.VAJRA750_FIRST_REBALANCE


# --------------------------------------------------------------------------- parity check


def test_parquet_csv_check_catches_a_real_difference(tmp_path: Path) -> None:
    """The first version of this check used per-column BIT_XOR of hashes. It passed while
    117,504 rows differed, because an even number of identical mismatches cancels under XOR.
    This test exists so that mistake cannot come back."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    source = tmp_path / "src.parquet"
    _write_parquet(source, _frame(4))
    parquet_path = tmp_path / "a.parquet"
    csv_path = tmp_path / "a.csv"

    select = publish._normalised_select(con, source)
    con.execute(f"COPY ({select}) TO '{str(parquet_path).replace(chr(92), '/')}' (FORMAT PARQUET)")
    con.execute(f"COPY ({select}) TO '{str(csv_path).replace(chr(92), '/')}' "
                "(HEADER, DELIMITER ',')")
    # Identical to begin with.
    assert publish._assert_parquet_csv_identical(con, parquet_path, csv_path)

    # Now corrupt one value in a way that leaves counts and column multisets plausible.
    text = csv_path.read_text(encoding="utf-8").splitlines()
    text[1] = text[1].replace("100.5", "999.5")
    csv_path.write_text("\n".join(text) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="PARQUET_CSV_MISMATCH"):
        publish._assert_parquet_csv_identical(con, parquet_path, csv_path)


def test_empty_strings_become_null_so_the_two_formats_agree(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    source = tmp_path / "src.parquet"
    _write_parquet(source, _frame(5))
    select = publish._normalised_select(con, source)
    assert "NULLIF" in select
    assert '"year"' not in select  # the partition column is dropped
    nulls = con.execute(
        f"SELECT COUNT(*) FROM ({select}) WHERE CorporateActionQuarantineReason IS NULL"
    ).fetchone()[0]
    assert nulls == 5


def test_write_year_round_trips_and_is_reused_when_unchanged(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    source = tmp_path / "store" / "src.parquet"
    _write_parquet(source, _frame(7))
    root = tmp_path / "published"
    spec = publish.YearSource("nifty500", 2024, source)

    first = publish._write_year(con, spec, root)
    assert first["rows"] == 7
    assert first["reused_unchanged"] is False
    assert (root / "nifty500" / "parquet" / "nifty500_2024.parquet").is_file()
    assert (root / "nifty500" / "csv" / "nifty500_2024.csv").is_file()
    assert first["label"] == "BACKTEST_SAFE"

    prior = {"nifty500:2024": first}
    second = publish._write_year(con, spec, root, prior)
    assert second["reused_unchanged"] is True
    assert second["parquet"]["sha256"] == first["parquet"]["sha256"]


def test_reparse_scan_accepts_an_ordinary_tree(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "b.txt").write_text("x", encoding="utf-8")
    publish._assert_no_reparse(tmp_path)


def test_orphan_partials_are_cleaned_up(tmp_path: Path) -> None:
    (tmp_path / ".x.parquet.abc.partial").write_text("junk", encoding="utf-8")
    removed = publish._cleanup_orphan_partials(tmp_path)
    assert removed == [".x.parquet.abc.partial"]
    assert not list(tmp_path.glob("*.partial"))


# --------------------------------------------------------------------------- verification


def _minimal_published(tmp_path: Path) -> tuple[Path, dict]:
    root = tmp_path / "VAJRA_DATA"
    (root / "nifty500" / "parquet").mkdir(parents=True)
    payload = root / "nifty500" / "parquet" / "nifty500_2024.parquet"
    _write_parquet(payload, _frame(3))
    import hashlib

    manifest = {
        "manifest_version": "TEST",
        "latest_session": "2024-01-03",
        "files": [
            {
                "path": "nifty500/parquet/nifty500_2024.parquet",
                "bytes": payload.stat().st_size,
                "sha256": hashlib.sha256(payload.read_bytes()).hexdigest(),
            }
        ],
    }
    (root / "MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root, manifest


def test_verify_published_passes_on_a_clean_folder(tmp_path: Path) -> None:
    root, _ = _minimal_published(tmp_path)
    result = publish.verify_published(root)
    assert result["pass"] is True
    assert result["files_checked"] == 1


def test_verify_published_detects_tampering(tmp_path: Path) -> None:
    root, _ = _minimal_published(tmp_path)
    target = root / "nifty500" / "parquet" / "nifty500_2024.parquet"
    target.write_bytes(target.read_bytes() + b"\x00")
    result = publish.verify_published(root)
    assert result["pass"] is False
    assert result["hash_mismatched"] == ["nifty500/parquet/nifty500_2024.parquet"]


def test_verify_published_flags_stray_files(tmp_path: Path) -> None:
    """The whole point of the published folder is that it contains nothing but the dataset."""
    root, _ = _minimal_published(tmp_path)
    (root / "leftover.txt").write_text("scratch", encoding="utf-8")
    result = publish.verify_published(root)
    assert result["pass"] is False
    assert result["unexpected_files"] == ["leftover.txt"]


# --------------------------------------------------------------------------- documentation


def _fake_manifest() -> dict:
    def years(universe: str, span: range) -> list[dict]:
        out = []
        for y in span:
            label, basis = publish.year_label(universe, y)
            out.append(
                {
                    "year": y,
                    "label": label,
                    "membership_basis": basis,
                    "rows": 1000,
                    "symbols": 500,
                    "sessions": 248,
                    "first_date": f"{y}-01-01",
                    "last_date": f"{y}-12-31",
                    "parquet": {"path": f"{universe}/parquet/{universe}_{y}.parquet",
                                "bytes": 1, "sha256": "x"},
                    "csv": {"path": f"{universe}/csv/{universe}_{y}.csv", "bytes": 1,
                            "sha256": "x"},
                    "parquet_csv_identical": True,
                }
            )
        return out

    universes = {}
    for name, span in (("nifty500", range(2009, 2027)), ("nifty750", range(2009, 2027))):
        rows = years(name, span)
        universes[name] = {
            "definition": "test",
            "years": rows,
            "rows": 1000 * len(rows),
            "distinct_symbols": 1207,
            "first_date": rows[0]["first_date"],
            "last_date": rows[-1]["last_date"],
            "backtest_safe_years": [r["year"] for r in rows if r["label"] == "BACKTEST_SAFE"],
            "price_data_only_years": [r["year"] for r in rows if r["label"] == "PRICE_DATA_ONLY"],
            "partial_years": [r["year"] for r in rows if r["label"] == "PARTIAL"],
        }
    return {
        "generated_at_utc": "2026-08-29T00:00:00+00:00",
        "latest_session": "2026-08-28",
        "official_membership_anchor": "2013-04-18",
        "file_count": 99,
        "total_bytes": 3_000_000_000,
        "universes": universes,
        "calendar": {"trading_sessions": 4371, "first_session": "2009-01-01",
                     "last_session": "2026-08-28"},
        "corporate_actions": {"events_quarantined": 123,
                              "events_reconciled_against_prices": 16814},
    }


def test_start_here_states_the_things_that_make_a_backtest_wrong() -> None:
    text = publish_docs.start_here(_fake_manifest(), None)
    # The four claims a reader must not miss.
    assert "PRICE_DATA_ONLY" in text
    assert "survivorship" in text.lower()
    assert "not a total return" in text.lower() or "not total return" in text.lower()
    assert "NIFTY 750" in text and "not an index" in text.lower()
    # And the anchor date must be spelled out, not implied.
    assert "2013-04-18" in text


def test_start_here_renders_without_leftover_placeholders() -> None:
    text = publish_docs.start_here(_fake_manifest(), None)
    assert "{" not in text.split("```")[0]  # no unfilled f-string braces in the prose
    assert "VERIFY_SNIPPET" not in text


def test_data_dictionary_covers_every_column_it_is_given() -> None:
    schemas = {
        "test table": [
            {"name": "Date", "type": "DATE", "units": "date", "meaning": "session"},
            {"name": "Close", "type": "DOUBLE", "units": "INR", "meaning": "adjusted close"},
        ]
    }
    text = publish_docs.data_dictionary(_fake_manifest(), schemas)
    assert "`Date`" in text
    assert "`Close`" in text
    assert "INR" in text
