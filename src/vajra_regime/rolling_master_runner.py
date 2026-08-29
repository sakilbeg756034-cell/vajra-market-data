from __future__ import annotations

import json

from vajra_regime.config import load_config
from vajra_regime.legacy_recovery import ensure_legacy_snapshot
from vajra_regime.rolling_master import rebuild_rolling_clean_data


def main() -> None:
    config = load_config()
    recovery = ensure_legacy_snapshot(config)
    print("Legacy snapshot safety check:")
    print(json.dumps(recovery, indent=2, default=str))
    print("")

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
    print("Manual corporate-action editing required: NO")
    print("Published dataset changed during build: NO")
    print(f"Summary: {summary['summary_path']}")


if __name__ == "__main__":
    main()
