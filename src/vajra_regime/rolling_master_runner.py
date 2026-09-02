"""Rebuild the adjusted rolling master on its own, outside the daily pipeline.

Useful when the pipeline decides it has nothing to catch up but the master still
needs rebuilding - after a change to how prices are adjusted, for instance. The
production runner skips the rebuild when `catchup_outcome` is
`ALREADY_CURRENT_...`, so a code change alone will not trigger one.
"""
from __future__ import annotations

import json

from vajra_regime.config import load_config
from vajra_regime.rolling_master import rebuild_rolling_clean_data


def main() -> None:
    # The legacy 2009-2025 snapshot used to be created and checked here, by a
    # `legacy_recovery` module that the 2026-08-29 reset removed. Nothing
    # replaced it because nothing needed to: rebuild_rolling_clean_data already
    # creates the snapshot when it is missing and refuses to run when it is
    # present but does not end at HISTORICAL_END. The import stayed behind and
    # this runner raised ModuleNotFoundError on every invocation until
    # 2026-09-02.
    config = load_config()
    summary = rebuild_rolling_clean_data(config)
    print(json.dumps(summary, indent=2, default=str))
    print("")
    print("VAJRA ADJUSTED ROLLING MASTER COMPLETED")
    print(f"Rows: {summary['final_rows']}")
    print(f"First date: {summary['final_first_date']}")
    print(f"Last date: {summary['final_last_date']}")
    print(f"Verified split/bonus applied: {summary['verified_split_bonus_events_applied']}")
    print(f"Quarantine rows: {summary['validation']['quarantine_rows']}")
    print(f"Duplicate Date+ISIN groups: {summary['validation']['duplicate_date_isin_groups']}")
    print(f"Summary: {summary['summary_path']}")


if __name__ == "__main__":
    main()
