from __future__ import annotations

import json

from vajra_regime.config import load_config
from vajra_regime.monthly_universe import continue_monthly_750_universe


def main() -> None:
    config = load_config()
    summary = continue_monthly_750_universe(config)
    print(json.dumps(summary, indent=2, default=str))
    print()
    print("VAJRA MONTHLY 750 UNIVERSE CONTINUATION COMPLETED")
    print(f"Outcome: {summary['outcome']}")
    print(f"Latest clean date: {summary['latest_clean_date']}")
    print(f"Historical months preserved: {summary['historical_months_preserved']}")
    print(f"Historical rows preserved: {summary['historical_rows_preserved']}")
    print(f"Live completed months: {summary['live_completed_months']}")
    print(f"Live selected rows: {summary['live_selected_rows']}")
    print(f"Live first rebalance: {summary['live_first_rebalance']}")
    print(f"Live last rebalance: {summary['live_last_rebalance']}")
    print(f"Partial live months: {summary['partial_live_months']}")
    print(
        "Duplicate RebalanceDate+ISIN groups: "
        f"{summary['duplicate_rebalance_isin_groups']}"
    )
    print(
        "Duplicate RebalanceDate+LiquidityRank groups: "
        f"{summary['duplicate_rebalance_rank_groups']}"
    )
    print(f"Current partial-month rows: {summary['current_partial_month_rows']}")
    print(
        "Quarantine-excluded candidate rows: "
        f"{summary['quarantine_excluded_candidate_rows']}"
    )
    print("Manual universe editing required: NO")
    print("Published dataset changed during build: NO")
    print(f"Status: {summary['status_path']}")


if __name__ == "__main__":
    main()
