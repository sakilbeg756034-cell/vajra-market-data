from __future__ import annotations

import json

from vajra_regime.nifty500_migration.production_pipeline import (
    run_nifty500_production_pipeline,
)


def main() -> int:
    print(json.dumps(run_nifty500_production_pipeline(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

