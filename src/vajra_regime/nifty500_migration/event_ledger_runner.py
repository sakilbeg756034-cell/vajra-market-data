from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from vajra_regime.nifty500_migration.constants import DATA_ROOT
from vajra_regime.nifty500_migration.event_ledger import build_official_event_ledger


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the reconciled official Nifty500 membership event ledger")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 8, 13))
    args = parser.parse_args()
    print(json.dumps(build_official_event_ledger(data_root=args.data_root, as_of=args.as_of), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
