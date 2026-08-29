from __future__ import annotations

import json

from vajra_regime.publish import publish_dataset


def main() -> int:
    status = publish_dataset()
    print(json.dumps(status, indent=2, default=str))
    return 0 if status["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
