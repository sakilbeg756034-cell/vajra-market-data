from __future__ import annotations

import json

from vajra_regime.quality import write_quality_report


def main() -> int:
    report = write_quality_report()
    print(json.dumps(report["verdicts"], indent=2))
    print("OVERALL:", report["overall"])
    return 0 if report["overall"] != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
