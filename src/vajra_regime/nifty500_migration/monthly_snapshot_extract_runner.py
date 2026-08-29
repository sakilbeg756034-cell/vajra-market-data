from __future__ import annotations

import json

from vajra_regime.nifty500_migration.monthly_snapshot_extract import extract_official_monthly_snapshots


def main() -> None:
    print(json.dumps(extract_official_monthly_snapshots(), indent=2), flush=True)


if __name__ == "__main__":
    main()
