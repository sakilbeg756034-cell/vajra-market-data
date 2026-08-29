from __future__ import annotations

import argparse
import json
from pathlib import Path

from vajra_regime.nifty500_migration.constants import DATA_ROOT
from vajra_regime.nifty500_migration.historical_name_map import extract_official_press_layout_name_map


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract official company-name/symbol mappings from press PDFs")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    args = parser.parse_args()
    print(json.dumps(extract_official_press_layout_name_map(data_root=args.data_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
