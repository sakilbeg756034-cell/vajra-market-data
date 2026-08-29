from __future__ import annotations

import json

from vajra_regime.nifty500_migration.incremental_catchup import run_incremental_catchup


def main() -> int:
    print(json.dumps(run_incremental_catchup(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

