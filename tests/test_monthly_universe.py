from __future__ import annotations

from types import SimpleNamespace

import duckdb

from vajra_regime.monthly_universe import continue_monthly_750_universe


def _config(tmp_path):
    root = tmp_path / "Vajra Market System"
    database = (
        root
        / "02 Master Historical Data"
        / "05 Database"
        / "master.duckdb"
    )
    database.parent.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(
        environment=SimpleNamespace(
            root=root,
            duckdb_path=database,
        ),
        data={
            "clean_table": "clean_daily",
            "universe_table": "monthly_vajra_750_universe",
            "expected_universe_size": 750,
        },
    )


def _build_fixture_database(config) -> None:
    with duckdb.connect(str(config.environment.duckdb_path)) as connection:
        connection.execute(
            """
            CREATE TABLE monthly_vajra_750_universe AS
            WITH months AS (
                SELECT
                    LAST_DAY(
                        DATE '2010-08-01' + i * INTERVAL '1 month'
                    )::DATE AS RebalanceDate,
                    DATE_TRUNC(
                        'month',
                        DATE '2010-08-01' + i * INTERVAL '1 month'
                    )::DATE AS MonthStart
                FROM range(185) t(i)
            ),
            members AS (
                SELECT i AS LiquidityRank FROM range(1, 751) t(i)
            )
            SELECT
                m.RebalanceDate,
                m.MonthStart,
                m.RebalanceDate AS SecurityLastDate,
                0::INTEGER AS StaleCalendarDays,
                'H' || LPAD(
                    CAST(r.LiquidityRank AS VARCHAR), 4, '0'
                ) AS Symbol,
                'INE' || LPAD(
                    CAST(r.LiquidityRank AS VARCHAR), 9, '0'
                ) AS ISIN,
                100.0::DOUBLE AS Close,
                100000.0::DOUBLE AS Volume,
                10000000.0::DOUBLE AS Turnover,
                300::BIGINT AS HistoryCount,
                (1000000000.0 - r.LiquidityRank)::DOUBLE
                    AS MedianTurnover60,
                60::BIGINT AS TurnoverObservations60,
                r.LiquidityRank::BIGINT AS LiquidityRank,
                FALSE AS LargeReturnAnomalyFlag,
                FALSE AS LongGapOver30DaysFlag
            FROM months m
            CROSS JOIN members r
            ORDER BY m.RebalanceDate, r.LiquidityRank
            """
        )
        connection.execute(
            """
            CREATE TABLE monthly_750_coverage AS
            SELECT
                RebalanceDate,
                900::BIGINT AS EligibleCandidates,
                750::BIGINT AS SelectedMembers,
                'FULL_750'::VARCHAR AS UniverseStatus
            FROM (
                SELECT DISTINCT RebalanceDate
                FROM monthly_vajra_750_universe
            )
            ORDER BY RebalanceDate
            """
        )
        connection.execute(
            """
            CREATE TABLE clean_daily AS
            WITH month_dates(MonthStart, TradingDate) AS (
                VALUES
                    (DATE '2026-01-01', DATE '2026-01-30'),
                    (DATE '2026-02-01', DATE '2026-02-27'),
                    (DATE '2026-03-01', DATE '2026-03-31'),
                    (DATE '2026-04-01', DATE '2026-04-30'),
                    (DATE '2026-05-01', DATE '2026-05-29'),
                    (DATE '2026-06-01', DATE '2026-06-30'),
                    (DATE '2026-07-01', DATE '2026-07-31'),
                    (DATE '2026-08-01', DATE '2026-08-06')
            ),
            candidates AS (
                SELECT i AS CandidateRank FROM range(1, 761) t(i)
            )
            SELECT
                d.TradingDate AS Date,
                'L' || LPAD(
                    CAST(c.CandidateRank AS VARCHAR), 4, '0'
                ) AS Symbol,
                'INE' || LPAD(
                    CAST(100000 + c.CandidateRank AS VARCHAR), 9, '0'
                ) AS ISIN,
                100.0::DOUBLE AS Close,
                100000.0::DOUBLE AS Volume,
                (2000000000.0 - c.CandidateRank * 1000)::DOUBLE
                    AS Turnover,
                400::BIGINT AS HistoryCount,
                (2000000000.0 - c.CandidateRank * 1000)::DOUBLE
                    AS MedianTurnover60,
                60::BIGINT AS TurnoverObservations60,
                FALSE AS LargeReturnAnomalyFlag,
                FALSE AS LongGapOver30DaysFlag,
                'EQ' AS Series,
                CASE
                    WHEN c.CandidateRank = 1 THEN FALSE
                    ELSE TRUE
                END AS IsResearchEligible
            FROM month_dates d
            CROSS JOIN candidates c
            """
        )


def test_continuation_preserves_history_builds_only_completed_months(
    tmp_path,
):
    config = _config(tmp_path)
    _build_fixture_database(config)

    summary = continue_monthly_750_universe(config)

    assert summary["status"] == "SUCCESS"
    assert summary["historical_months_preserved"] == 185
    assert summary["historical_rows_preserved"] == 138750
    assert summary["live_completed_months"] == 7
    assert summary["live_selected_rows"] == 5250
    assert summary["live_last_rebalance"] == "2026-07-31"
    assert summary["partial_live_months"] == 0
    assert summary["current_partial_month_rows"] == 0
    assert summary["duplicate_rebalance_isin_groups"] == 0
    assert summary["duplicate_rebalance_rank_groups"] == 0
    assert summary["quarantine_excluded_candidate_rows"] == 7

    database = str(config.environment.duckdb_path)
    with duckdb.connect(database, read_only=True) as connection:
        live_counts = connection.execute(
            """
            SELECT RebalanceDate, COUNT(*) AS Members
            FROM monthly_vajra_750_universe
            WHERE RebalanceDate >= DATE '2026-01-01'
            GROUP BY RebalanceDate
            ORDER BY RebalanceDate
            """
        ).fetchall()
        assert len(live_counts) == 7
        assert all(int(row[1]) == 750 for row in live_counts)

        august_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM monthly_vajra_750_universe
            WHERE DATE_TRUNC('month', RebalanceDate)::DATE
                = DATE '2026-08-01'
            """
        ).fetchone()[0]
        assert int(august_rows) == 0

        quarantined = connection.execute(
            """
            SELECT COUNT(*)
            FROM monthly_vajra_750_universe
            WHERE RebalanceDate = DATE '2026-07-31'
              AND ISIN = 'INE000100001'
            """
        ).fetchone()[0]
        assert int(quarantined) == 0

    universe_dir = (
        tmp_path
        / "Vajra Market System"
        / "02 Master Historical Data"
        / "02 Monthly 750 Universe"
    )
    backup_dir = (
        tmp_path
        / "Vajra Market System"
        / "09 Backups"
        / "Monthly 750 Universe Legacy 2010-2025"
    )
    assert universe_dir.exists()
    assert backup_dir.exists()


def test_a_name_in_the_surveillance_segment_cannot_enter_the_universe(tmp_path):
    """BE ke din stock ka DATA rakhte hain, par usse KHAREEDTE nahi.

    Ye do alag baatein hain aur pehle dono ek saath toot gayi thin. Intake sirf
    EQ leta tha, isliye surveillance ke daur me stock data se hi gayab ho jaata
    tha -- aur wapas aane par uska "1-din ka return" poore gap ka nikalta tha
    (SUZLON par +58.6%, jo asal me 98 din ka move tha).

    Ab data aata hai. Par BE me har sauda delivery me settle karna padta hai aur
    intraday mana hai -- us din us naam ko portfolio me lena asli duniya me hota
    hi nahi. Isliye rok yahan hai, LiquidityRank se PEHLE, taaki wo naam top-750
    ki ek jagah bhi na ghere. Uski jagah agla eligible naam aana chahiye.
    """
    config = _config(tmp_path)
    _build_fixture_database(config)

    database = str(config.environment.duckdb_path)
    with duckdb.connect(database) as connection:
        connection.execute(
            """
            UPDATE clean_daily SET Series = 'BE'
            WHERE Symbol = 'L0002' AND Date = DATE '2026-07-31'
            """
        )

    summary = continue_monthly_750_universe(config)
    assert summary["status"] == "SUCCESS"

    with duckdb.connect(database, read_only=True) as connection:
        present = connection.execute(
            """
            SELECT COUNT(*) FROM monthly_vajra_750_universe
            WHERE RebalanceDate = DATE '2026-07-31' AND Symbol = 'L0002'
            """
        ).fetchone()[0]
        # usi naam ka pichhla mahina, jab wo EQ me tha
        previous = connection.execute(
            """
            SELECT COUNT(*) FROM monthly_vajra_750_universe
            WHERE RebalanceDate = DATE '2026-06-30' AND Symbol = 'L0002'
            """
        ).fetchone()[0]
        members = connection.execute(
            """
            SELECT COUNT(*) FROM monthly_vajra_750_universe
            WHERE RebalanceDate = DATE '2026-07-31'
            """
        ).fetchone()[0]

    assert int(present) == 0, "BE ke din wo naam universe me nahi hona chahiye"
    assert int(previous) == 1, "EQ wale mahine me wahi naam maujood tha"
    assert int(members) == 750, "uski jagah agla eligible naam aana chahiye"
