from __future__ import annotations

import argparse
import json
from pathlib import Path

from vajra_regime.nifty500_migration.constants import DATA_ROOT
from vajra_regime.nifty500_migration.review_event_resolver import resolve_review_events


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve legacy official Nifty500 press tables in PDF layout mode")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    args = parser.parse_args()
    print(json.dumps(resolve_review_events(data_root=args.data_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
