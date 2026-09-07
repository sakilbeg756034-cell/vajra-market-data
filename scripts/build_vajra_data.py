"""build_vajra_data.py -- EK COMMAND. Poora dataset dobara ban jaata hai.

    D:\\VAJRA_ENGINE\\venv\\Scripts\\python.exe D:\\VAJRA_ENGINE\\code\\scripts\\build_vajra_data.py

Lagbhag 15-25 minute. **Network ko chhoo-ta tak nahi** -- bhavcopy pehle se
utri hui hoti hai (engine ka roz ka run wo kaam kar chuka hota hai).

KYA BANTA HAI
=============
    D:\\VAJRA_DATA      <- EKMATRA dataset. Survivorship-free. parquet + CSV.

KADAM (isi kram me, kyunki har kadam pichhle par khada hai)
==========================================================
    1. build_raw_panel.py       NSE bhavcopy zip  ->  kachcha panel
    2. build_identity.py        ISIN badle to bhi ek hi company
    3. build_adjusted_panel.py  split / bonus ka adjustment
    4. publish_vajra_data.py    D:\\VAJRA_DATA  (parquet + CSV + docs)

Ye chaar script `D:\\VAJRA_RESEARCH\\work\\scripts\\` me hain.

AGAR KOI KADAM FAIL HO
======================
Yahin ruk jaata hai aur **purana dataset waise ka waisa** rehta hai. Aadha-
adhoora naya dataset likhne se purana sahi dataset behtar hai.

Kadam 4 khud bhi teen jaanch karta hai aur koi bhi fail ho to publish nahi
karta:
    * survivorship gate  -- 2025 se pehle band hui company 100 se zyada honi
                            chahiye (warna data phir se biased hai)
    * parquet == CSV     -- har file me row ginti barabar
    * staging            -- sab kuch pehle alag jagah banta hai, phir hi
                            asli jagah aata hai

YE FILE KYUN BANI
=================
6 September 2026 tak DO dataset folder the -- `D:\\VAJRA_DATA` (biased) aur
`D:\\VAJRA_DATA_NEW` (sahi). Dono roz taaza hote the. Naam se pata nahi
chalta tha kaunsa sahi hai, aur galat wale par chalaya gaya backtest KUCH
TODTA NAHI tha -- bas jawab galat aata tha.

Ab ek hi dataset hai, aur use banane ki ek hi jagah -- ye file.
"""
from __future__ import annotations

import io
import subprocess
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SCRIPTS = Path(r"D:\VAJRA_RESEARCH\work\scripts")
PYTHON = Path(r"D:\VAJRA_ENGINE\venv\Scripts\python.exe")
TARGET = Path(r"D:\VAJRA_DATA")

STEPS = [
    ("kachcha panel (NSE bhavcopy se)", "build_raw_panel.py"),
    ("sthir company pehchaan", "build_identity.py"),
    ("corporate action adjustment", "build_adjusted_panel.py"),
    ("D:\\VAJRA_DATA publish karo (parquet + CSV)", "publish_vajra_data.py"),
]


def main() -> int:
    print("=" * 88)
    print("VAJRA_DATA -- POORA DATASET DOBARA BANAO".center(88))
    print("=" * 88)
    print(f"\ntarget : {TARGET}")
    print(f"kadam  : {len(STEPS)}\n", flush=True)

    if not PYTHON.exists():
        print(f"!! Python nahi mila: {PYTHON}")
        return 1
    missing = [f for _, f in STEPS if not (SCRIPTS / f).exists()]
    if missing:
        print(f"!! ye script nahi mili: {missing}")
        print(f"   dekhi gayi jagah: {SCRIPTS}")
        return 1

    t0 = time.time()
    for i, (label, fname) in enumerate(STEPS, 1):
        print("-" * 88)
        print(f"KADAM {i}/{len(STEPS)} : {label}")
        print("-" * 88, flush=True)
        started = time.time()
        proc = subprocess.run(
            [str(PYTHON), str(SCRIPTS / fname)],
            cwd=str(SCRIPTS), capture_output=True, text=True,
            encoding="utf-8", errors="replace")
        if proc.returncode != 0:
            print(proc.stdout[-6000:] if proc.stdout else "")
            print(proc.stderr[-4000:] if proc.stderr else "")
            print(f"\n!! KADAM {i} FAIL ({fname}), exit {proc.returncode}")
            print("   PURANA dataset waise ka waisa hai -- kuch bigda nahi.")
            print("   Upar ka error padho, theek karo, dobara chalao.")
            return 1
        tail = [ln for ln in (proc.stdout or "").rstrip().split("\n") if ln.strip()]
        for ln in tail[-8:]:
            print("   " + ln)
        print(f"   [{time.time() - started:.0f}s]\n", flush=True)

    print("=" * 88)
    print(f"HO GAYA -- {(time.time() - t0) / 60.0:.1f} minute me")
    print("=" * 88)

    man = TARGET / "MANIFEST.json"
    if man.exists():
        import json
        d = json.loads(man.read_text(encoding="utf-8"))
        print(f"\n   dataset    : {d.get('dataset')}  ({d.get('version')})")
        print(f"   daur       : {d.get('coverage_start')} -> {d.get('coverage_end')}")
        print(f"   company    : {d.get('companies'):,}")
        print(f"   2025 se pehle band : {d.get('companies_delisted_before_2025'):,}")
        print(f"   row        : nifty750 {d.get('rows_nifty750'):,}   "
              f"nifty500 {d.get('rows_nifty500'):,}")
        print(f"   format     : {', '.join(d.get('formats', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
