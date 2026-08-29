from __future__ import annotations

import json

from vajra_regime.nifty500_migration.secondary_archive import archive_secondary_constituent_evidence


def main() -> None:
    print(json.dumps(archive_secondary_constituent_evidence(), indent=2), flush=True)


if __name__ == "__main__":
    main()
