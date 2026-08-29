from __future__ import annotations

import json

from vajra_regime.nifty500_migration.source_archive import archive_official_sources


if __name__ == "__main__":
    print(json.dumps(archive_official_sources(), indent=2, default=str))

