"""NSE ke apne bhavcopy archive se poora itihaas laao (survivorship-free).

Ye script sirf DOWNLOAD karti hai -- parse ya rebuild nahi. Do wajah se alag
rakha gaya hai:

  1. Download me sabse zyada waqt lagta hai aur wo network par nirbhar hai.
     Beech me ruk jaye to dobara chalane par jo file aa chuki hai wo dobara
     nahi maangi jaati -- kaam wahin se aage badhta hai.

  2. Parse aur rebuild disk par chalta hai aur tez hai. Use alag rakhne se
     har baar network chhedne ki zaroorat nahi padti.

Chalane ka tareeka:
    python scripts/fetch_historical_bhavcopy.py 2011-01-01 2026-09-02
"""
from __future__ import annotations

import io
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vajra_regime.nse_historical import fetch_range  # noqa: E402

ROOT = Path(r"D:\VAJRA_ENGINE\store\03 Incoming NSE EOD\02 Historical Bhavcopy ZIP")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    if end > date.today():
        end = date.today()

    print(f"NSE historical bhavcopy : {start} -> {end}", flush=True)
    print(f"jagah                   : {ROOT}", flush=True)
    t0 = time.time()
    counts = fetch_range(start, end, ROOT, pause=0.35, progress_every=100)
    mins = (time.time() - t0) / 60.0

    print(flush=True)
    print(f"KHATAM  {mins:.1f} minute me", flush=True)
    print(f"  naye download : {counts['DOWNLOADED']}", flush=True)
    print(f"  pehle se the  : {counts['EXISTS']}", flush=True)
    print(f"  chhutti (404) : {counts['HOLIDAY']}", flush=True)
    on_disk = sum(1 for _ in ROOT.rglob("*.zip"))
    print(f"  disk par kul  : {on_disk} zip", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
