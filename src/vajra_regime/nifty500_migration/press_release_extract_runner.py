from __future__ import annotations

import json

from vajra_regime.nifty500_migration.press_release_extract import extract_press_release_evidence


if __name__ == "__main__":
    print(json.dumps(extract_press_release_evidence(), indent=2, default=str))

