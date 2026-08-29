from __future__ import annotations

import json

from vajra_regime.nifty500_migration.foundation_certification import build_foundation_certification


def main() -> int:
    print(json.dumps(build_foundation_certification(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
