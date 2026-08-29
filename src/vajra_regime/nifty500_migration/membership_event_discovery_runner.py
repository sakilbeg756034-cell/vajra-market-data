from __future__ import annotations

import json

from vajra_regime.nifty500_migration.membership_event_discovery import discover_official_membership_events


def main() -> None:
    print(json.dumps(discover_official_membership_events(), indent=2), flush=True)


if __name__ == "__main__":
    main()
