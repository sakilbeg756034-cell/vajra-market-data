"""build_vajra_data.py -- EK COMMAND. Poora dataset dobara ban jaata hai.

    D:\\VAJRA_ENGINE\\venv\\Scripts\\python.exe D:\\VAJRA_ENGINE\\code\\scripts\\build_vajra_data.py

Lagbhag 15-25 minute. Bhavcopy pehle se utri honi chahiye (poora daily runner
wo kaam karta hai). Ye builder network se corporate-action feed refresh karta
hai; missing bhavcopy download nahi karta.

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

import hashlib
import io
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SCRIPTS = Path(r"D:\VAJRA_RESEARCH\work\scripts")
PYTHON = Path(r"D:\VAJRA_ENGINE\venv\Scripts\python.exe")
TARGET = Path(r"D:\VAJRA_DATA")

# ---- "KUCH NAYA NAHI TO DOBARA MAT BANAO" -- 24 September 2026 -------------
#
# Pehle har run -- har login, Shanivaar-Ravivaar bhi -- poora dataset 15-20
# minute me dobara banta tha, chahe ek bhi nayi file na aayi ho (19-Sep-2026,
# Shanivaar: 5 poore run). Ab KADAM 1 (corporate action) ke baad do
# fingerprint milaaye jaate hain:
#
#   INPUT  -- dataset jin cheezon se banta hai: har NSE bhavcopy zip (naam +
#             size + mtime), NSE corporate action ka CONTENT, NIFTY 500
#             membership ka CONTENT, aur build ka CODE.
#   OUTPUT -- D:\VAJRA_DATA ki HAR file (naam + size + mtime).
#
# Dono pichhle safal build jaise hon tabhi baaki kadam chhoote jaate hain.
# Ek bhi cheez alag -- naya din, naya split/bonus, code badla, ya D:\VAJRA_DATA
# me koi CSV/parquet delete ya badli -- to POORA build pehle jaisa. Yaani galti
# hamesha "faltu dobara bana diya" ki disha me hoti hai, "purana data chup-chaap
# chhod diya" ki disha me kabhi nahi. `--force` hamesha poora banata hai.
#
# CA aur membership ka CONTENT isliye, mtime nahi: dono file har run me dobara
# likhi jaati hain (mtime roz badalta hai) chahe andar kuch na badla ho.
STAMP = Path(r"D:\VAJRA_ENGINE\store\_vajra_data_build_stamp.json")
RESULT = Path(r"D:\VAJRA_ENGINE\logs\latest_dataset_build.json")
BHAV_ROOTS = [
    Path(r"D:\VAJRA_ENGINE\store\02 Master Historical Data\NIFTY500 Point In Time"
         r"\01 Raw Source Archives\Official NSE Equity Bhavcopy"),
    Path(r"D:\VAJRA_ENGINE\store\03 Incoming NSE EOD\01 Official UDiFF ZIP"),
]
CA_FILES = [Path(r"D:\VAJRA_RESEARCH\work\ca_history\nse_corporate_actions.parquet"),
            Path(r"D:\VAJRA_RESEARCH\work\ca_history\nse_corporate_actions_2010_2026.parquet")]
MEMBERSHIP = Path(r"D:\VAJRA_ENGINE\store\02 Master Historical Data\NIFTY500 Point In Time"
                  r"\07 Point In Time Panels\nifty500_daily_membership_certified.parquet")
PRE2011_LAYER = Path(r"D:\VAJRA_ENGINE\store\02 Master Historical Data\PRE2011 Frozen Layer\current")
CODE_FILES = sorted(SCRIPTS.glob("*.py")) + [
    Path(r"D:\VAJRA_ENGINE\code\src\vajra_regime\paths.py"), Path(__file__).resolve()]


def _frame_hash(path: Path) -> str:
    import pandas as pd
    df = pd.read_parquet(path).astype("string")
    df = df.sort_values(list(df.columns), na_position="first").reset_index(drop=True)
    h = pd.util.hash_pandas_object(df, index=False).values.tobytes()
    return hashlib.sha256(h + ",".join(df.columns).encode()).hexdigest()


def input_fingerprint() -> dict:
    h = hashlib.sha256()
    n_zip = 0
    for root in BHAV_ROOTS:
        for p in sorted(root.rglob("*.zip")) if root.exists() else []:
            st = p.stat()
            h.update(f"{p}|{st.st_size}|{st.st_mtime_ns}\n".encode())
            n_zip += 1
    ca = next((p for p in CA_FILES if p.exists()), None)
    code = hashlib.sha256()
    for p in CODE_FILES:
        code.update(p.name.encode() + b"\0" + p.read_bytes())
    pre_man = PRE2011_LAYER / "MANIFEST.json"
    return {
        "pre2011_layer": hashlib.sha256(pre_man.read_bytes()).hexdigest() if pre_man.exists() else None,
        "bhavcopy_zips": h.hexdigest(), "n_zips": n_zip,
        "corporate_actions": _frame_hash(ca) if ca else None,
        "membership": _frame_hash(MEMBERSHIP) if MEMBERSHIP.exists() else None,
        "code": code.hexdigest(),
    }


def output_fingerprint() -> dict:
    h = hashlib.sha256()
    n = 0
    for p in sorted(TARGET.rglob("*")) if TARGET.exists() else []:
        if p.is_file():
            st = p.stat()
            h.update(f"{p.relative_to(TARGET)}|{st.st_size}|{st.st_mtime_ns}\n".encode())
            n += 1
    return {"files": h.hexdigest(), "n_files": n}


def _write_json(path: Path, body: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def why_rebuild(now_in: dict, now_out: dict) -> list[str]:
    """Khaali list = kuch nahi badla. Warna har badli cheez ki wajah."""
    if not STAMP.exists():
        return ["pichhle build ka record nahi mila"]
    try:
        old = json.loads(STAMP.read_text(encoding="utf-8"))
    except Exception:
        return ["pichhle build ka record padha nahi gaya"]
    reasons = []
    labels = {"bhavcopy_zips": "NSE bhavcopy zip (naya din ya badli file)",
              "corporate_actions": "corporate action (naya split/bonus/koi event)",
              "membership": "NIFTY 500 membership", "code": "build ka code",
              "pre2011_layer": "PRE2011 frozen layer"}
    for k, label in labels.items():
        if old.get("input", {}).get(k) != now_in.get(k):
            reasons.append(f"{label} badla")
    if old.get("output") != now_out:
        reasons.append(f"D:\\VAJRA_DATA ki file badli ya gayab "
                       f"(ab {now_out['n_files']} file, pehle {old.get('output', {}).get('n_files')})")
    return reasons

# (label, script, kya FAIL hone par ruk jaana hai)
STEPS = [
    # KADAM 0 -- 7 September 2026 ko joda gaya.
    #
    # Ye NSE se naye corporate action laata hai. Pehle ye chain me tha hi
    # nahi, aur us script me END ki tareekh HARDCODED thi (2026-09-03).
    # Nateeja: us din ke baad ka koi split/bonus adjustment me aata hi nahi
    # tha -- aur dataset bina kisi error ke publish ho jaata tha. Ek chhoota
    # hua split us naam ke bhaav me jhootha -50% giraav banata hai, aur
    # momentum use turant 'sabse bura' samajh kar bech deta hai.
    #
    # roko=False: NSE ka feed kabhi-kabhi jawab nahi deta. Ek din ka fail
    # poore dataset ko nahi rokna chahiye -- purana CA data abhi bhi theek
    # hai. Par agar wo 7 din se zyada purana ho jaye to aakhri kadam ka
    # `ca_freshness_gate` publish rok deta hai. Der maaf hai; purana data
    # chup-chaap chalte rehna maaf nahi.
    ("NSE se naye corporate action laao", "fetch_ca_history.py", False),
    ("kachcha panel (NSE bhavcopy se)", "build_raw_panel.py", True),
    ("sthir company pehchaan", "build_identity.py", True),
    ("corporate action adjustment", "build_adjusted_panel.py", True),
    ("D:\\VAJRA_DATA publish karo (parquet + CSV)", "publish_vajra_data.py", True),
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
    missing = [f for _, f, _ in STEPS if not (SCRIPTS / f).exists()]
    if missing:
        print(f"!! ye script nahi mili: {missing}")
        print(f"   dekhi gayi jagah: {SCRIPTS}")
        return 1

    force = "--force" in sys.argv[1:]
    t0 = time.time()
    for i, (label, fname, roko) in enumerate(STEPS, 1):
        if i == 2:
            # KADAM 1 (corporate action) ho chuka -- ab faisla: dobara banana hai?
            now_in, now_out = input_fingerprint(), output_fingerprint()
            reasons = ["--force"] if force else why_rebuild(now_in, now_out)
            if not reasons:
                print("=" * 88)
                print("KUCH NAYA NAHI -- D:\\VAJRA_DATA pehle jaisa sahi hai, dobara nahi banaya")
                print("   (koi naya NSE din nahi, koi naya corporate action nahi, code wahi,")
                print(f"    aur D:\\VAJRA_DATA ki saari {now_out['n_files']} file jaisi ki taisi)")
                print("=" * 88, flush=True)
                _write_json(RESULT, {"action": "SKIPPED_NOTHING_NEW",
                                     "time_local": datetime.now().isoformat(timespec="seconds")})
                return 0
            print("DOBARA BAN RAHA HAI, kyunki:")
            for r in reasons:
                print(f"   - {r}")
            print(flush=True)
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
            if roko:
                print("   PURANA dataset waise ka waisa hai -- kuch bigda nahi.")
                print("   Upar ka error padho, theek karo, dobara chalao.")
                return 1
            print("   Ye kadam ROKNE wala nahi hai -- aage badh rahe hain.")
            print("   Agar iski wajah se data purana reh gaya, to aakhri kadam")
            print("   ka gate publish khud rok dega. Der maaf hai; chup-chaap")
            print("   purana data chalte rehna maaf nahi.", flush=True)
            continue
        tail = [ln for ln in (proc.stdout or "").rstrip().split("\n") if ln.strip()]
        for ln in tail[-8:]:
            print("   " + ln)
        print(f"   [{time.time() - started:.0f}s]\n", flush=True)

    print("=" * 88)
    print(f"HO GAYA -- {(time.time() - t0) / 60.0:.1f} minute me")
    print("=" * 88)

    # Safal build ka record. INPUT wahi jo build SHURU hone se pehle naapa tha --
    # build ke dauran koi nayi zip aayi ho to wo is dataset me nahi hai, aur
    # agla run fingerprint alag dekh kar use zaroor banayega. (Build ke BAAD
    # naapna ulti disha me galat hota: nayi zip chhoot jaati.) OUTPUT abhi
    # publish hua wala.
    _write_json(STAMP, {"built_local": datetime.now().isoformat(timespec="seconds"),
                        "input": now_in, "output": output_fingerprint()})
    _write_json(RESULT, {"action": "REBUILT", "reasons": reasons,
                         "time_local": datetime.now().isoformat(timespec="seconds")})

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
