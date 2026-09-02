from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace

import duckdb

from vajra_regime.corporate_actions import RECONCILIATION_TABLE
from vajra_regime.nse_live import RAW_TABLE
from vajra_regime.rolling_master import LEGACY_TABLE, rebuild_rolling_clean_data


def _config(tmp_path: Path) -> SimpleNamespace:
    root = tmp_path / "Vajra Market System"
    master = root / "02 Master Historical Data"
    database = master / "05 Database" / "Vajra_Master_Market_Data.duckdb"
    database.parent.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(
        environment=SimpleNamespace(
            root=root,
            published_data_root=tmp_path / "Vajra Backtesting",
            master_data_root=master,
            duckdb_path=database,
            logs_dir=root / "08 Logs",
        ),
        data={"clean_table": "clean_daily"},
    )


def _seed_database(config: SimpleNamespace) -> None:
    with duckdb.connect(str(config.environment.duckdb_path)) as connection:
        connection.execute(
            """
            CREATE TABLE clean_daily AS
            SELECT * FROM (VALUES
                -- AAA's legacy rows arrive from EOD2 with the 2026-01-02 bonus ALREADY
                -- applied: 50.0, not the 100.0 it traded at. That is what the real feed
                -- does, and it is why these rows must not be adjusted a second time.
                (DATE '2025-12-30', 2025, 'AAA', 'INE000A01001', 50.0, 50.5, 49.5, 50.0, 2000::BIGINT,
                 10.0, 100.0, 500.0, 100000.0, NULL::DOUBLE, NULL::DOUBLE, NULL::BIGINT, 1, 100000.0, 1::BIGINT,
                 FALSE, FALSE, FALSE, TRUE),
                (DATE '2025-12-31', 2025, 'AAA', 'INE000A01001', 50.0, 50.5, 49.5, 50.0, 2000::BIGINT,
                 10.0, 100.0, 500.0, 100000.0, 50.0, 0.0, 1::BIGINT, 2, 100000.0, 2::BIGINT,
                 FALSE, FALSE, FALSE, TRUE),
                (DATE '2025-12-31', 2025, 'BBB', 'INE000B01001', 200.0, 202.0, 198.0, 200.0, 500::BIGINT,
                 5.0, 100.0, 250.0, 100000.0, NULL::DOUBLE, NULL::DOUBLE, NULL::BIGINT, 1, 100000.0, 1::BIGINT,
                 FALSE, FALSE, FALSE, TRUE)
            ) AS t(
                Date, Year, Symbol, ISIN, Open, High, Low, Close, Volume,
                TotalTrades, QuantityPerTrade, DeliveryQuantity, Turnover, PrevClose, Return1D, GapDays,
                HistoryCount, MedianTurnover60, TurnoverObservations60, LargeReturnAnomalyFlag,
                LongGapOver30DaysFlag, IsWarmupPeriod, IsBacktestPeriod
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE {RAW_TABLE} AS
            SELECT * FROM (VALUES
                (DATE '2026-01-01', 'AAA', 'INE000A01001', 'EQ', 100.0, 101.0, 99.0, 100.0, 1000::BIGINT, 100000.0,
                 'a.zip', 'a.csv', 'sha-a', TIMESTAMP '2026-01-02 00:00:00'),
                (DATE '2026-01-02', 'AAA', 'INE000A01001', 'EQ', 50.0, 52.0, 49.0, 51.0, 2200::BIGINT, 112200.0,
                 'b.zip', 'b.csv', 'sha-b', TIMESTAMP '2026-01-03 00:00:00'),
                (DATE '2026-01-01', 'BBB', 'INE000B01001', 'EQ', 200.0, 201.0, 198.0, 200.0, 500::BIGINT, 100000.0,
                 'a.zip', 'a.csv', 'sha-a', TIMESTAMP '2026-01-02 00:00:00'),
                (DATE '2026-01-02', 'BBB', 'INE000B01001', 'EQ', 150.0, 151.0, 149.0, 150.0, 700::BIGINT, 105000.0,
                 'b.zip', 'b.csv', 'sha-b', TIMESTAMP '2026-01-03 00:00:00')
            ) AS t(Date, Symbol, ISIN, Series, Open, High, Low, Close, Volume, Turnover,
                   SourceFile, SourceMember, SourceSha256, IngestedAtUTC)
            """
        )
        connection.execute(
            f"""
            CREATE TABLE {RECONCILIATION_TABLE} AS
            SELECT * FROM (VALUES
                ('bonus-a', 'AAA', 'INE000A01001', 'AAA Ltd', 'EQ', 'Bonus 1:1', 'BONUS', DATE '2026-01-02',
                 DATE '2026-01-02', '10', 0.5, 2.0, 'PARSED', 'MATCHED_UNIQUE_ISIN', DATE '2026-01-01',
                 100.0, DATE '2026-01-02', 51.0, 0.02, 'AUTO_READY_SPLIT_BONUS', 'Bonus 1:1'),
                ('rights-b', 'BBB', 'INE000B01001', 'BBB Ltd', 'EQ', 'Rights 1:4', 'RIGHTS', DATE '2026-01-02',
                 DATE '2026-01-02', '10', NULL, NULL, 'REVIEW', 'MATCHED_UNIQUE_ISIN', DATE '2026-01-01',
                 200.0, DATE '2026-01-02', 150.0, NULL, 'REVIEW_COMPLEX_OR_UNPARSED', 'Rights review')
            ) AS t(
                EventId, Symbol, ISIN, CompanyName, Series, Subject, ActionType, ExDate, RecordDate, FaceValue,
                PriceFactorForPreExHistory, VolumeFactorForPreExHistory, ParseStatus, MatchStatus, PreDate, PreClose,
                PostDate, PostClose, PostVsAdjustedPreGap, Decision, Note
            )
            """
        )


def test_rolling_master_adjusts_only_the_live_half_and_quarantines_rights(
    tmp_path: Path,
) -> None:
    """The factor belongs to the as-traded half only, and the seam must not move.

    Two sources meet at LIVE_START: EOD2's legacy feed, which is already split-
    and bonus-adjusted, and NSE's bhavcopy, which is as-traded. Applying the
    factor to both adjusts the legacy half twice, and because the live half is
    adjusted once the two stop agreeing exactly at the boundary.

    That is not hypothetical. On 2026-09-02 fourteen securities carried a break
    on 2026-01-01, each of size exactly the reciprocal of its own pending-2026
    bonus - ZFCVINDIA x5.94 against a 5:1, CUPID x5.07 against a 4:1, ECLERX
    x2.05 against a 1:1 - while RELIANCE and INFY, which had no 2026 action,
    were untouched. It stayed invisible for years because every row used to come
    from the one feed, and a uniform extra factor over a whole series changes no
    return and leaves no seam to see.

    So this asserts the property whose failure costs money - continuity across
    the boundary - rather than a transformation.
    """
    config = _config(tmp_path)
    _seed_database(config)

    summary = rebuild_rolling_clean_data(config)

    assert summary["validation"]["ok"] is True
    assert summary["verified_split_bonus_events_applied"] == 1
    assert summary["manual_corporate_action_edit_required"] is False

    with duckdb.connect(str(config.environment.duckdb_path), read_only=True) as connection:
        assert LEGACY_TABLE in {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        historical_close = connection.execute(
            "SELECT Close FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2025-12-31'"
        ).fetchone()[0]
        historical_volume = connection.execute(
            "SELECT Volume FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2025-12-31'"
        ).fetchone()[0]
        ex_return = connection.execute(
            "SELECT Return1D FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2026-01-02'"
        ).fetchone()[0]
        live_pre_ex_close = connection.execute(
            "SELECT Close FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2026-01-01'"
        ).fetchone()[0]
        seam_return = connection.execute(
            "SELECT Return1D FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2026-01-01'"
        ).fetchone()[0]
        rights_quarantine = connection.execute(
            "SELECT CorporateActionQuarantineFlag FROM clean_daily "
            "WHERE ISIN = 'INE000B01001' AND Date = DATE '2026-01-02'"
        ).fetchone()[0]
        duplicates = connection.execute(
            "SELECT COUNT(*) FROM (SELECT Date, ISIN, COUNT(*) n FROM clean_daily GROUP BY Date, ISIN HAVING n > 1)"
        ).fetchone()[0]

    # Legacy comes out exactly as the feed supplied it.
    assert math.isclose(historical_close, 50.0, rel_tol=0, abs_tol=1e-10)
    assert historical_volume == 2000
    # The live pre-ex row is as-traded and does need the factor: 100.0 -> 50.0.
    assert math.isclose(live_pre_ex_close, 50.0, rel_tol=0, abs_tol=1e-10)
    # And therefore the two halves meet without a step. This is the assertion
    # that would have caught the 2026-01-01 breaks.
    assert math.isclose(seam_return, 0.0, rel_tol=0, abs_tol=1e-10)
    assert math.isclose(ex_return, 0.02, rel_tol=0, abs_tol=1e-9)
    assert rights_quarantine is True
    assert duplicates == 0

    parquet_2026 = (
        config.environment.master_data_root / "01 Daily Clean Parquet By Year" / "EOD2_Clean_2026.parquet"
    )
    assert parquet_2026.exists()
    report_path = (
        config.environment.master_data_root
        / "03 Quality Reports"
        / "09_Rolling_Adjusted_Master_Summary.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["final_last_date"] == "2026-01-02"


def test_rerun_is_deterministic_and_does_not_compound_bonus(tmp_path: Path) -> None:
    """Rebuilding twice must not apply the factor twice to the live half."""
    config = _config(tmp_path)
    _seed_database(config)

    rebuild_rolling_clean_data(config)
    rebuild_rolling_clean_data(config)

    with duckdb.connect(str(config.environment.duckdb_path), read_only=True) as connection:
        legacy_close = connection.execute(
            "SELECT Close FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2025-12-31'"
        ).fetchone()[0]
        live_close = connection.execute(
            "SELECT Close FROM clean_daily WHERE ISIN = 'INE000A01001' AND Date = DATE '2026-01-01'"
        ).fetchone()[0]
        rows = connection.execute("SELECT COUNT(*) FROM clean_daily").fetchone()[0]

    # Untouched however many times the build runs.
    assert math.isclose(legacy_close, 50.0, rel_tol=0, abs_tol=1e-10)
    # Adjusted once, not 100 -> 50 -> 25.
    assert math.isclose(live_close, 50.0, rel_tol=0, abs_tol=1e-10)
    assert rows == 7
