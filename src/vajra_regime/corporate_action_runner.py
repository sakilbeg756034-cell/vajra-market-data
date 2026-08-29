from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from vajra_regime.config import load_config
from vajra_regime.corporate_actions import run_corporate_action_audit


def _yesterday_ist() -> date:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date() - timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit NSE corporate actions before adjusted append.")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else _yesterday_ist()
    if end < start:
        raise ValueError("End date cannot be before start date.")

    config = load_config()
    summary = run_corporate_action_audit(config, start_date=start, end_date=end)
    print(json.dumps(summary, indent=2, default=str))
    print("")
    print("VAJRA CORPORATE-ACTION AUDIT COMPLETED")
    print(f"Events: {summary['events']}")
    print(f"Matched unique ISIN: {summary['matched_unique_isin']}")
    print(f"Auto-ready split/bonus: {summary['auto_ready_split_bonus']}")
    print(f"Review required: {summary['review_required']}")
    print(f"Informational/no-adjustment: {summary['informational_no_adjustment']}")
    print("clean_daily modified: NO")
    print("Historical Parquet modified: NO")
    print(f"Summary: {summary['summary_path']}")


if __name__ == "__main__":
    main()
