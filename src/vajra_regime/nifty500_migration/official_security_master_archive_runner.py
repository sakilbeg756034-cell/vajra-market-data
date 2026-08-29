from __future__ import annotations

import argparse
import json
from pathlib import Path

from vajra_regime.nifty500_migration.constants import DATA_ROOT
from vajra_regime.nifty500_migration.official_security_master_archive import (
    archive_official_2008_security_master,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive and parse the official 2008 NSE security master")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    args = parser.parse_args()
    print(json.dumps(archive_official_2008_security_master(data_root=args.data_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
