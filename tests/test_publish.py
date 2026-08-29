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


# --------------------------------------------------------------------- quality report render


def test_quality_report_renders_with_an_external_crosscheck() -> None:
    """The renderer read a key the cross-check module never produced, so the daily run crashed
    the first time an external cross-check result existed on disk. Both shapes are covered
    here: a symbol that was compared, and one no source could answer for."""
    from vajra_regime import quality

    report = _quality_stub(
        external={
            "status": "RUN",
            "summary": "14 of 15 agree.",
            "method": "Daily returns, not price levels.",
            "symbols": [
                {
                    "symbol": "RELIANCE",
                    "source": "yfinance",
                    "overlapping_sessions": 4104,
                    "median_abs_return_difference": 0.000029,
                    "sessions_agreeing_within_tolerance": 0.9946,
                    "verdict": "AGREES",
                },
                {"symbol": "TATAMOTORS", "verdict": "NO_EXTERNAL_SOURCE_ANSWERED"},
            ],
        }
    )
    text = quality.render_report(report)
    assert "RELIANCE" in text
    assert "TATAMOTORS" in text
    assert "NO_EXTERNAL_SOURCE_ANSWERED" in text


def test_quality_report_renders_without_an_external_crosscheck() -> None:
    from vajra_regime import quality

    report = _quality_stub(external={"status": "NOT_RUN", "note": "not run"})
    assert "not run" in quality.render_report(report)


def _quality_stub(*, external: dict) -> dict:
    universe = {
        "shape": {
            "rows": 10,
            "distinct_symbols": 2,
            "distinct_isins": 2,
            "sessions": 5,
            "first_date": "2024-01-01",
            "last_date": "2024-01-05",
            "per_year": [
                {
                    "year": 2024,
                    "rows": 10,
                    "symbols": 2,
                    "isins": 2,
                    "sessions": 5,
                    "first_date": "2024-01-01",
                    "last_date": "2024-01-05",
                }
            ],
        },
        "missing_sessions": {"calendar_sessions": 5, "missing_session_count": 0,
                             "missing_sessions": [], "pass": True},
        "duplicates": {
            "duplicate_date_symbol_groups": 0, "duplicate_date_symbol_extra_rows": 0,
            "duplicate_date_symbol_examples": [], "duplicate_date_isin_groups": 0,
            "duplicate_date_isin_extra_rows": 0, "duplicate_date_isin_examples": [],
            "pass": True,
        },
        "bar_sanity": {
            k: 0 for k in (
                "rows", "null_date", "null_symbol", "null_isin", "null_close", "null_ohl",
                "non_positive", "high_lt_low", "close_outside", "open_outside",
                "null_volume", "zero_volume", "negative_volume", "fatal_total",
            )
        } | {"pass": True, "zero_volume_note": "note"},
        "adjustment_sanity": {
            "events_available": 1, "events_checked_against_prices": 1,
            "residual_unadjusted_gaps": 0, "examples": [], "pass": True, "method": "m",
        },
        "eligibility": {
            "rows": 10, "research_eligible": 10, "quarantined": 0,
            "quarantined_fraction": 0.0, "top_quarantine_reasons": [],
        },
    }
    return {
        "version": "TEST",
        "generated_at_utc": "2026-08-29T00:00:00+00:00",
        "published_root": "D:/VAJRA_DATA",
        "latest_session": "2024-01-05",
        "manifest_payload_sha256": "x",
        "universes": {"nifty500": universe, "nifty750": universe},
        "survivorship": {
            "pass": True, "snapshots": [{"date": "2024-01-01", "members": 500}],
            "consecutive_overlaps": [], "first_vs_last_common_members": 1,
            "current_members_absent_at_start": 1,
            "securities_that_left_and_never_returned": 1,
            "membership_rows_from_departed_securities": 1, "interpretation": "i",
        },
        "member_price_coverage": {
            "member_sessions": 10, "member_sessions_without_price": 0, "fraction": 0.0,
            "pass": True, "worst_symbols": [], "note": "n",
        },
        "parquet_csv_parity": {"years_checked": 1, "years_identical": 1, "pass": True,
                               "method": "m"},
        "external_crosscheck": external,
        "verdicts": {"survivorship": "PASS"},
        "overall": "PASS",
    }


# ------------------------------------------------------------------ resilience: deletions


def _year_setup(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, publish.YearSource, Path]:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    source = tmp_path / "store" / "src.parquet"
    _write_parquet(source, _frame(6))
    return con, publish.YearSource("nifty500", 2024, source), tmp_path / "published"


def test_a_deleted_parquet_is_rebuilt(tmp_path: Path) -> None:
    """Parquet is the dataset. Losing one costs the time to write it again and nothing else."""
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    parquet = root / "nifty500" / "parquet" / "nifty500_2024.parquet"
    parquet.unlink()

    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)
    assert parquet.is_file()
    assert second["reused_unchanged"] is False
    assert second["rows"] == 6


def test_a_deleted_csv_for_an_old_year_stays_deleted(tmp_path: Path) -> None:
    """The operator deletes CSVs when the disk fills. Recreating seventeen years of them on
    the next run would silently undo that, so a year whose Parquet survives is left alone."""
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    csv_path = root / "nifty500" / "csv" / "nifty500_2024.csv"
    csv_path.unlink()

    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)
    assert not csv_path.exists()
    assert second["csv_present"] is False
    assert "csv" not in second
    assert "CSV_REMOVED_BY_OPERATOR" in second["csv_absent_reason"]


def test_a_deleted_csv_for_the_current_year_is_rewritten(tmp_path: Path) -> None:
    """New data always gets a CSV, so a wipe stops the mirror for old years without stopping
    it for good - the engine resumes from that day forward."""
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2024)
    csv_path = root / "nifty500" / "csv" / "nifty500_2024.csv"
    csv_path.unlink()

    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2024)
    assert csv_path.is_file()
    assert second["csv_present"] is True


def test_losing_both_files_rebuilds_both(tmp_path: Path) -> None:
    """Both gone is a real gap, not a disk-space decision - which is what happens when the
    whole published folder is deleted."""
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    (root / "nifty500" / "parquet" / "nifty500_2024.parquet").unlink()
    (root / "nifty500" / "csv" / "nifty500_2024.csv").unlink()

    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)
    assert (root / "nifty500" / "parquet" / "nifty500_2024.parquet").is_file()
    assert (root / "nifty500" / "csv" / "nifty500_2024.csv").is_file()
    assert second["csv_present"] is True


def test_a_corrupted_parquet_is_rebuilt(tmp_path: Path) -> None:
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    parquet = root / "nifty500" / "parquet" / "nifty500_2024.parquet"
    parquet.write_bytes(b"not a parquet file")

    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)
    assert second["reused_unchanged"] is False
    assert second["parquet"]["sha256"] == first["parquet"]["sha256"]


def test_an_untouched_year_is_not_rewritten(tmp_path: Path) -> None:
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)
    assert second["reused_unchanged"] is True


def test_manifest_omits_a_year_with_no_csv(tmp_path: Path) -> None:
    """Listing a CSV that is absent on purpose would make every integrity check fail."""
    con, spec, root = _year_setup(tmp_path)
    first = publish._write_year(con, spec, root, active_year=2026)
    (root / "nifty500" / "csv" / "nifty500_2024.csv").unlink()
    second = publish._write_year(con, spec, root, {"nifty500:2024": first}, active_year=2026)

    universes = {
        "nifty500": {
            "definition": "d",
            "years": [second],
            "rows": second["rows"],
            "first_date": second["first_date"],
            "last_date": second["last_date"],
        },
    }
    manifest = publish.build_manifest(
        con,
        root,
        universes=universes,
        extra_records=[],
        calendar_summary={},
        corporate_action_summary={},
    )
    paths_listed = [f["path"] for f in manifest["files"]]
    assert "nifty500/parquet/nifty500_2024.parquet" in paths_listed
    assert "nifty500/csv/nifty500_2024.csv" not in paths_listed
    assert manifest["csv_policy"]["years_without_csv"]["nifty500"] == [2024]


# ------------------------------------------------------------------------ changelog


def test_changelog_uses_ist_and_keeps_one_line_per_day(tmp_path: Path) -> None:
    """A run at 03:18 IST is stamped 2026-08-29 by UTC, so the file appeared to run
    backwards - 2026-08-30 followed by 2026-08-29, which reads as corruption."""
    from datetime import date as _date

    publish._append_changelog(tmp_path, _date(2026, 8, 29), "latest session 2026-08-28 - NO CHANGE")
    publish._append_changelog(tmp_path, _date(2026, 8, 30), "latest session 2026-08-28 - NO CHANGE")
    publish._append_changelog(tmp_path, _date(2026, 8, 30), "latest session 2026-08-28 - NO CHANGE")

    lines = [
        row for row in (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
        if row.startswith("- ")
    ]
    assert len(lines) == 2, lines
    dates = [row.split(" - ")[0].removeprefix("- ") for row in lines]
    assert dates == sorted(dates)
    assert "IST" in (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8")


def test_a_day_that_changed_stays_marked_as_changed(tmp_path: Path) -> None:
    """Four quiet runs after one real update must not erase the fact that the day updated."""
    from datetime import date as _date

    publish._append_changelog(tmp_path, _date(2026, 8, 30), "99 files - UPDATED")
    publish._append_changelog(tmp_path, _date(2026, 8, 30), "99 files - NO CHANGE")
    publish._append_changelog(tmp_path, _date(2026, 8, 30), "99 files - NO CHANGE")

    lines = [
        row for row in (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
        if row.startswith("- ")
    ]
    assert len(lines) == 1
    assert "UPDATED" in lines[0]


def test_changelog_survives_a_new_day_after_an_updated_day(tmp_path: Path) -> None:
    from datetime import date as _date

    publish._append_changelog(tmp_path, _date(2026, 8, 30), "99 files - UPDATED")
    publish._append_changelog(tmp_path, _date(2026, 8, 31), "100 files - UPDATED")
    lines = [
        row for row in (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
        if row.startswith("- ")
    ]
    assert len(lines) == 2
    assert lines[0].startswith("- 2026-08-30")
    assert lines[1].startswith("- 2026-08-31")
