from __future__ import annotations

import json

from vajra_regime.nifty500_migration.corporate_action_archive import (
    archive_official_corporate_actions,
)


def main() -> int:
    print(json.dumps(archive_official_corporate_actions(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
