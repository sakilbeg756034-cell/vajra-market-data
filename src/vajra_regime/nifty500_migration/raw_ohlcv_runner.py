from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from vajra_regime.nifty500_migration.constants import DATA_ROOT
from vajra_regime.nifty500_migration.raw_ohlcv import build_official_raw_ohlcv


def main() -> int:
    parser = argparse.ArgumentParser(description="Build official raw PIT Nifty500 OHLCV parquet")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2009, 1, 1))
    parser.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 8, 13))
    args = parser.parse_args()
    print(json.dumps(build_official_raw_ohlcv(data_root=args.data_root, start=args.start, as_of=args.as_of), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
