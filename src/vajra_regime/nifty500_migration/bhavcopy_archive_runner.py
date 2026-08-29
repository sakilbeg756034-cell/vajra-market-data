from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from vajra_regime.nifty500_migration.bhavcopy_archive import archive_official_bhavcopies
from vajra_regime.nifty500_migration.constants import DATA_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive official NSE historical equity bhavcopies")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2009, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2025, 12, 31))
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    print(
        json.dumps(
            archive_official_bhavcopies(
                data_root=args.data_root,
                start=args.start,
                end=args.end,
                workers=args.workers,
            ),
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
