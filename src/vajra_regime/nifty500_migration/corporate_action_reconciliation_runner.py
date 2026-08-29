from __future__ import annotations

import json

from vajra_regime.nifty500_migration.corporate_action_reconciliation import (
    build_corporate_action_reconciliation,
)


def main() -> int:
    print(json.dumps(build_corporate_action_reconciliation(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
