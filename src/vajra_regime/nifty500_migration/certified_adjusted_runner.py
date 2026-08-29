from __future__ import annotations

import json

from vajra_regime.nifty500_migration.certified_adjusted import build_certified_adjusted


def main() -> int:
    print(json.dumps(build_certified_adjusted(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
