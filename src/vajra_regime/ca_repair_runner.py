from __future__ import annotations

import json
import sys

from vajra_regime.ca_repair import repair


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    result = repair(dry_run=dry_run)
    for name, entry in result["universes"].items():
        print(f"=== {name}: {entry['action']}")
        print(f"    mechanical unapplied : {entry['mechanical_count']}")
        for e in entry["unapplied_mechanical_events"]:
            print(
                f"      {e['ex_date']} {e['symbol']:<12} {e['action_type']:<8} "
                f"factor={e['price_factor']} move={e['observed_move']:+.4f} "
                f"-> {e['move_after_repair']:+.4f}"
            )
        print(f"    non-mechanical breaks: {entry['non_mechanical_count']}")
        for e in entry["non_mechanical_price_breaks"]:
            print(f"      {e['ex_date']} {e['symbol']:<12} {e['observed_move']:+.4f}  {e['subject'][:60]}")
        print(f"    files rewritten      : {len(entry['files_rewritten'])}")
        print(f"    verified             : {entry['verified']}")
    print()
    print(json.dumps({"status": result["status"], "seconds": result["duration_seconds"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
