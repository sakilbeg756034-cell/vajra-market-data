"""Engine ka roz ka run.

6 SEPTEMBER 2026 KA BADLAV -- DHYAAN SE PADHIYE
==============================================
Ye pipeline pehle `D:\\VAJRA_DATA` me dataset PUBLISH karti thi. Ab NAHI karti.

Kyun: is pipeline ka price source `eod2_data-main` tha, jo sirf AAJ LISTED
symbols ki file rakhta hai. Nateeja -- uske banaye dataset me 17 saal me ek
bhi company delist, merge ya insolvent nahi hoti thi. Uspar chalaya gaya har
backtest jhootha (bahut ooncha) CAGR deta tha.

Ab `D:\\VAJRA_DATA` NSE ke apne bhavcopy archive se banta hai. Use banane
wali ek hi jagah hai:

    D:\\VAJRA_ENGINE\\code\\scripts\\build_vajra_data.py

Is pipeline ka kaam ab sirf ITNA hai (aur ye zaroori kaam hai):

    * NSE se naya bhavcopy utaarna       -> store/03 Incoming NSE EOD
    * master historical store sambhalna  -> store/02 Master Historical Data
    * NIFTY 500 point-in-time membership -> store (isi se publisher padhta hai)
    * corporate action reconciliation

Yaani ye ab **kachcha maal** taiyaar karti hai, dataset nahi.

`--publish` de kar purana bartav wapas laaya ja sakta hai, par aisa mat
kijiye: wo `D:\\VAJRA_DATA` ko survivorship-biased data se bhar dega aur
koi error nahi aayega.
"""

from __future__ import annotations

import argparse
import json

from vajra_regime.nifty500_migration.production_pipeline import (
    run_nifty500_production_pipeline,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--publish",
        action="store_true",
        help="PURANA, BIASED dataset D:\\VAJRA_DATA me likho. Istemal mat kijiye.",
    )
    args = parser.parse_args()

    if args.publish:
        print("!! CHETAVNI: --publish diya gaya hai.")
        print("!! Ye D:\\VAJRA_DATA ko SURVIVORSHIP-BIASED data se bhar dega.")
        print("!! Sahi dataset banane ke liye: scripts\\build_vajra_data.py")

    result = run_nifty500_production_pipeline(publish=args.publish)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
