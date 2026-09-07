"""verify_system.py -- poore system ki sehat, ek command me.

KYUN
====
Is system ke kai hisse hain: laptop ka engine, dataset, cloud ka signal,
Google Sheet ka script, aur locked strategy. Har hissa doosre par khada hai.

Agar inme se kahin kuch alag ho jaye -- jaise sheet 12 stock maange aur engine
20 bheje -- to KUCH TOOT-TA NAHI. Koi error nahi aata. Bas jawab galat aata
hai, aur pata mahino baad chalta hai.

Ye file wahi khatra pakadti hai. Kuch badalne ke BAAD aur asli paisa lagane se
PEHLE ise chalao.

CHALANE KA TAREEKA
==================
    D:\\VAJRA_ENGINE\\venv\\Scripts\\python.exe D:\\VAJRA_ENGINE\\code\\scripts\\verify_system.py

Har jaanch PASS / FAIL / CHETAVNI deti hai. Ek bhi FAIL ho to asli paisa mat
lagao jab tak wo theek na ho jaye.

6 SEPTEMBER 2026 SE KYA BADLA
=============================
* Ab **ek hi dataset** hai: `D:\\VAJRA_DATA`. `VAJRA_DATA_NEW` khatam.
* Backtest ke liye **koi environment variable nahi** chahiye.
* Ummeed kiye gaye CAGR/MaxDD ab is file me LIKHE NAHI hain -- wo
  `LOCKED_STRATEGY_V2.json` se padhe jaate hain. Pehle wo yahan hardcoded the
  aur chup-chaap purane pad gaye the.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ENGINE = Path(r"D:\VAJRA_ENGINE")
RESEARCH = Path(r"D:\VAJRA_RESEARCH")
GATE = Path(r"D:\VAJRA SYSTEM GATE")
SHEET_DIR = Path(r"D:\google sheet maintenance file")
DATA = Path(r"D:\VAJRA_DATA")
PYTHON = ENGINE / "venv" / "Scripts" / "python.exe"
LOCK = RESEARCH / "work/results/LOCKED_STRATEGY_V2.json"

results: list[tuple[str, str, str]] = []


def ok(name, detail=""):
    results.append(("PASS", name, detail))
    print(f"   [PASS] {name}" + (f"  -- {detail}" if detail else ""), flush=True)


def bad(name, detail=""):
    results.append(("FAIL", name, detail))
    print(f"   [FAIL] {name}" + (f"  -- {detail}" if detail else ""), flush=True)


def warn(name, detail=""):
    results.append(("CHETAVNI", name, detail))
    print(f"   [CHETAVNI] {name}" + (f"  -- {detail}" if detail else ""), flush=True)


def head(t):
    print("\n" + "-" * 92)
    print(t)
    print("-" * 92, flush=True)


# ==================================================================== 1
def check_files():
    head("1. WO FILE JO HONI HI CHAHIYE")
    must = [
        (GATE / "VAJRA_SYSTEM.md", "poore system ka naksha"),
        (GATE / "VAJRA_MOMENTUM_STRATEGY.pdf", "strategy ka PDF"),
        (GATE / "KABHI_DELETE_MAT_KARNA.md", "delete na karne wali list"),
        (LOCK, "locked strategy"),
        (SHEET_DIR / "VajraMomentumScanner.gs", "sheet ka script"),
        (ENGINE / "code/scripts/build_vajra_data.py", "dataset banane ka ek-maatra command"),
        (ENGINE / "code/src/vajra_regime/cloud/signal.py", "cloud signal"),
        (DATA / "MANIFEST.json", "dataset ka manifest"),
        (DATA / "START_HERE_AI.md", "dataset ka START HERE"),
        (RESEARCH / "work/scripts/publish_vajra_data.py", "dataset publisher"),
        (RESEARCH / "work/scripts/verify_vajra_data.py", "dataset verifier"),
    ]
    for p, what in must:
        (ok if p.exists() else bad)(what, str(p))

    store = (ENGINE / "store/02 Master Historical Data"
             / "NIFTY500 Point In Time/01 Raw Source Archives"
             / "Official NSE Equity Bhavcopy")
    if store.exists():
        n = sum(1 for _ in store.rglob("*.zip"))
        (ok if n > 4000 else warn)("NSE bhavcopy archive", f"{n:,} zip file")
    else:
        bad("NSE bhavcopy archive", "folder hi nahi mila -- ye sabse keemti hissa hai")


# ==================================================================== 2
def check_one_dataset():
    head("2. EK HI DATASET -- aur wo sach me survivorship-free hai?")
    strays = [p for p in Path("D:/").glob("VAJRA_DATA*")
              if p.is_dir() and p != DATA]
    (ok if not strays else bad)(
        "D:\\ me doosra data folder nahi hai",
        "mila: " + ", ".join(p.name for p in strays) if strays else "sirf VAJRA_DATA")

    import duckdb
    con = duckdb.connect()
    g = (DATA / "nifty750/parquet/nifty750_*.parquet").as_posix()
    try:
        dead, total = con.execute(
            f"SELECT SUM(CASE WHEN mx < DATE '2025-01-01' THEN 1 ELSE 0 END), COUNT(*) "
            f"FROM (SELECT ISIN, MAX(Date) mx FROM read_parquet('{g}') GROUP BY 1)"
        ).fetchone()
        last = con.execute(f"SELECT MAX(Date) FROM read_parquet('{g}')").fetchone()[0]
    except Exception as exc:                                          # noqa: BLE001
        bad("dataset padha nahi ja saka", str(exc)[:90])
        con.close()
        return
    (ok if dead > 100 else bad)(
        "survivorship-free hai", f"{total:,} company, 2025 se pehle band {dead:,}")

    for uni in ("nifty750", "nifty500"):
        npq = len(list((DATA / uni / "parquet").glob("*.parquet")))
        ncsv = len(list((DATA / uni / "csv").glob("*.csv")))
        (ok if npq == ncsv and npq > 0 else bad)(
            f"{uni}: parquet aur CSV dono", f"{npq} parquet, {ncsv} csv")

    try:
        man = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))
        same = str(man["latest_session"])[:10] == str(last)[:10]
        (ok if same else bad)("MANIFEST data se milta hai",
                              f"{man['latest_session']} vs {last}")
        age = (date.today() - date.fromisoformat(str(man["latest_session"])[:10])).days
        if age <= 4:
            ok("dataset taaza hai", f"aakhri din {last} ({age} din)")
        elif age <= 10:
            warn("dataset thoda baasi", f"aakhri din {last} ({age} din)")
        else:
            bad("dataset BAASI hai", f"aakhri din {last} ({age} din) -- "
                                     f"scripts\\build_vajra_data.py chalao")
    except Exception as exc:                                          # noqa: BLE001
        bad("manifest padha nahi gaya", str(exc)[:90])
    con.close()


# ==================================================================== 3
def check_consistency():
    head("3. SABSE ZAROORI -- engine, sheet aur locked strategy ek jaisi hain?")
    try:
        lock = json.loads(LOCK.read_text(encoding="utf-8"))["final_spec"]
    except Exception as exc:                                          # noqa: BLE001
        bad("lock file padhi nahi gayi", str(exc)[:90])
        return

    sig = (ENGINE / "code/src/vajra_regime/cloud/signal.py").read_text(encoding="utf-8")
    gs = (SHEET_DIR / "VajraMomentumScanner.gs").read_text(encoding="utf-8")

    def grab(text, pat):
        m = re.search(pat, text, re.M)
        return m.group(1) if m else None

    checks = [
        ("kitne stock (n)", lock["n"],
         grab(sig, r"^N_HOLDINGS\s*=\s*(\d+)"), grab(gs, r"N_HOLDINGS:\s*(\d+)")),
        ("exit rank", int(lock["n"] * lock["buffer"]),
         grab(sig, r"^EXIT_RANK\s*=\s*(\d+)"), grab(gs, r"EXIT_RANK:\s*(\d+)")),
        ("vol filter", lock.get("max_vol_pct"),
         grab(sig, r"^MAX_VOL_PERCENTILE\s*=\s*([\d.]+)"), None),
        ("max weight", lock["max_weight"],
         grab(sig, r"^MAX_WEIGHT\s*=\s*([\d.]+)"), None),
    ]
    for name, want, in_engine, in_sheet in checks:
        parts, bad_any = [f"lock={want}"], False
        if in_engine is not None:
            same = float(in_engine) == float(want)
            parts.append(f"engine={in_engine}" + ("" if same else " <-- ALAG"))
            bad_any |= not same
        if in_sheet is not None:
            same = float(in_sheet) == float(want)
            parts.append(f"sheet={in_sheet}" + ("" if same else " <-- ALAG"))
            bad_any |= not same
        (bad if bad_any else ok)(name, "  ".join(parts))

    for col in ("WEIGHT_PCT", "VOL_RANK_PCT"):
        in_e, in_s = col in sig, col in gs
        (ok if (in_e and in_s) else bad)(
            f"CSV column {col}",
            f"engine={'haan' if in_e else 'NAHI'}  sheet={'haan' if in_s else 'NAHI'}")

    # ---- DOCUMENT ka number lock file se milta hai? -------------------------
    #
    # 6 September 2026 ki poori samasya YAHI thi: lock file me kuch aur likha
    # tha, document me kuch aur, aur sheet ke README me teesra. Koi error nahi
    # aata tha -- bas jo padhta wo galat number le kar chala jaata.
    #
    # Ye jaanch us drift ko turant pakad leti hai.
    try:
        ev = json.loads(LOCK.read_text(encoding="utf-8"))["evidence"]
        want = f'{ev["oos_2016_cost_only_pct"]:.2f}'
        docs = {
            "VAJRA_SYSTEM.md": GATE / "VAJRA_SYSTEM.md",
            "sheet ka README": SHEET_DIR / "VajraMomentumScanner.gs",
        }
        for label, path in docs.items():
            text = path.read_text(encoding="utf-8", errors="replace")
            (ok if want in text else bad)(
                f"{label} me aaj ka CAGR likha hai",
                f"{want}% dhoondha gaya -- " + ("mila" if want in text else "NAHI MILA"))
    except Exception as exc:                                          # noqa: BLE001
        warn("document ka number jaancha nahi gaya", str(exc)[:90])

    # Vol filter ki PARIBHASHA dono jagah ek hi honi chahiye.
    # 6-Sep-2026: research poore panel par rank karta tha, live eligible me.
    # Koi error nahi aata tha -- bas 2.3 pp CAGR ka farq.
    fb = (RESEARCH / "work/scripts/engine/fastbt.py").read_text(encoding="utf-8")
    lb = (RESEARCH / "work/scripts/lab.py").read_text(encoding="utf-8")
    in_fastbt = "cfg.max_vol_pct" in fb and "np.where(elig, volp" in fb
    live_elig = 'where(eligible)' in sig and "rank(pct=True)" in sig
    old_way = re.search(r"^\s*f = _and\(f, vol\.rank\(", lb, re.M) is not None
    (ok if (in_fastbt and live_elig and not old_way) else bad)(
        "vol percentile ki paribhasha research aur live me ek hai",
        f"fastbt={'haan' if in_fastbt else 'NAHI'}  "
        f"signal.py={'haan' if live_elig else 'NAHI'}  "
        f"lab.py me purana tareeka={'HAAN (galat)' if old_way else 'nahi'}")


# ==================================================================== 4
def check_live():
    head("4. LIVE SIGNAL -- cloud par jo file hai wahi")
    csv = SHEET_DIR / "vajra-signals/out/latest_signals.csv"
    status = SHEET_DIR / "vajra-signals/out/status.json"
    if not csv.exists():
        bad("live CSV nahi mili", str(csv))
        return
    header = csv.read_text(encoding="utf-8").split("\n", 1)[0]
    for col in ("WEIGHT_PCT", "VOL_RANK_PCT", "RANK", "SCORE", "ELIGIBLE"):
        (ok if col in header else bad)(f"CSV me {col}")

    try:
        st = json.loads(status.read_text(encoding="utf-8"))
        ok("status.json", f"n_holdings={st['n_holdings']}  "
                          f"exit_rank={st['exit_rank']}  "
                          f"eligible={st['eligible']}/{st['universe_rows']}")
        last = date.fromisoformat(st["as_of_session"])
        age = (date.today() - last).days
        (ok if age <= 4 else warn)(
            "live signal taaza", f"{last} ({age} din)"
            + ("" if age <= 4 else " -- GitHub Actions dekho"))
        if st["eligible"] >= st["universe_rows"]:
            bad("vol filter live me lag hi nahi raha",
                f"eligible {st['eligible']} == universe {st['universe_rows']}")
        else:
            ok("vol filter live me lag raha hai",
               f"{st['universe_rows'] - st['eligible']} naam bahar")
    except Exception as exc:                                          # noqa: BLE001
        bad("status.json padha nahi gaya", str(exc)[:90])


# ==================================================================== 5
def check_engine_tests():
    head("5. ENGINE KE APNE TEST")
    try:
        p = subprocess.run([str(PYTHON), "-m", "pytest", "tests/", "-q",
                            "--no-header"],
                           cwd=str(ENGINE / "code"), capture_output=True,
                           text=True, encoding="utf-8", errors="replace",
                           timeout=1800)
        tail = [x for x in (p.stdout or "").strip().split("\n") if x.strip()]
        (ok if p.returncode == 0 else bad)("pytest", (tail[-1] if tail else "")[:90])
    except Exception as exc:                                          # noqa: BLE001
        warn("pytest chala nahi", str(exc)[:90])


# ==================================================================== 6
def check_schedule():
    head("6. ROZ KA TASK -- laptop par")
    try:
        p = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "$t=Get-ScheduledTask -TaskName 'VAJRA Data Engine' "
             "-TaskPath '\\VAJRA\\' -ErrorAction Stop; "
             "$i=Get-ScheduledTaskInfo -TaskName 'VAJRA Data Engine' "
             "-TaskPath '\\VAJRA\\'; "
             "Write-Output ($t.State.ToString()+'|'+$i.LastTaskResult+'|'"
             "+$i.LastRunTime.ToString('yyyy-MM-dd HH:mm'))"],
            capture_output=True, text=True, timeout=120)
        out = (p.stdout or "").strip()
        if "|" in out:
            state, code, when = out.split("|")
            if state.lower() == "ready" and code.strip() == "0":
                ok("scheduled task", f"{state}, aakhri run {when}, natija OK")
            elif state.lower() == "disabled":
                bad("scheduled task BAND hai", f"{state} -- data taaza nahi hoga")
            else:
                warn("scheduled task", f"{state}, natija code {code}, {when}")
        else:
            bad("scheduled task nahi mila", (p.stderr or "")[:80])
    except Exception as exc:                                          # noqa: BLE001
        warn("task jaancha nahi ja saka", str(exc)[:90])

    st = ENGINE / "logs/latest_engine_run.json"
    if st.exists():
        try:
            d = json.loads(st.read_text(encoding="utf-8"))
            (ok if d.get("status") == "SUCCESS" else warn)(
                "aakhri engine run", f"{d.get('status')}  {d.get('generated_at_local')}")
        except Exception as exc:                                      # noqa: BLE001
            warn("run status padha nahi gaya", str(exc)[:90])

    ps_path = ENGINE / "code/scripts/windows/run_vajra_data_engine.ps1"
    ps = ps_path.read_text(encoding="utf-8")
    (ok if "build_vajra_data.py" in ps else bad)(
        "roz ka script naya dataset banata hai")
    (ok if "refresh_survivorship_free_data" not in ps else bad)(
        "purani refresh script hataayi ja chuki hai")
    lone = sum(1 for i, c in enumerate(ps)
               if c == "\r" and (i + 1 >= len(ps) or ps[i + 1] != "\n"))
    (ok if lone == 0 else bad)("daily script me tooti hui line nahi",
                               f"akela \\r = {lone}")

    # Engine ab biased dataset publish nahi karta
    runner = (ENGINE / "code/src/vajra_regime/nifty500_migration"
              / "production_pipeline_runner.py").read_text(encoding="utf-8")
    (ok if "publish=args.publish" in runner else bad)(
        "engine default me publish NAHI karta",
        "warna wo D:\\VAJRA_DATA ko biased data se bhar dega")


# ==================================================================== 7
REPRO = r"""
import io, json, sys
from pathlib import Path
sys.path.insert(0, r'D:\VAJRA_RESEARCH\work\scripts')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import config as C
C.unlock_holdout('verify_system.py ki routine jaanch')
import lab
from engine import fastbt as F, metrics as M
s = json.loads(Path(r'D:\VAJRA_RESEARCH\work\results\LOCKED_STRATEGY_V2.json')
               .read_text(encoding='utf-8'))['final_spec']
m, mask, P, vol = lab.data('nifty750')
sc = lab.build_score('nifty750', s)
f = lab.build_filter('nifty750', lab.filter_spec(s))
cfg = lab.make_cfg(s, 'verify')
assert cfg.max_vol_pct is not None, 'max_vol_pct cfg me nahi pohoncha'
nav = F.run(P, sc, cfg, vol=vol, extra_filter=f,
            apply_costs=True, apply_tax=False).nav
print(f'{M.cagr(nav)*100:.2f}|{M.max_dd(nav)*100:.1f}|'
      f'{m["Close"].shape[0]}|{m["Close"].shape[1]}')
"""


def check_strategy_reproduces():
    head("7. LOCKED STRATEGY WAHI JAWAB DETI HAI?")
    try:
        ev = json.loads(LOCK.read_text(encoding="utf-8"))["evidence"]
        want_cagr = float(ev["full_period_cost_only_pct"])
        want_dd = float(ev["maxdd_full_cost_only_pct"])
    except Exception as exc:                                          # noqa: BLE001
        bad("lock file me ummeed ka number nahi mila", str(exc)[:90])
        return

    try:
        p = subprocess.run([str(PYTHON), "-c", REPRO], capture_output=True,
                           text=True, encoding="utf-8", errors="replace",
                           env=dict(os.environ), timeout=2400)
        line = [x for x in (p.stdout or "").strip().split("\n") if "|" in x]
        if not line:
            bad("backtest chala nahi", (p.stderr or "")[-250:])
            return
        cagr, dd, sess, names = line[-1].split("|")
        cagr, dd = float(cagr), float(dd)
        ok("panel", f"{sess} session x {names} naam")
        if abs(cagr - want_cagr) < 0.05 and abs(dd - want_dd) < 0.2:
            ok("locked strategy wahi jawab deti hai",
               f"CAGR {cagr}% ({want_cagr} chahiye), MaxDD {dd}% ({want_dd} chahiye)")
        else:
            bad("NUMBER BADAL GAYA",
                f"CAGR {cagr}% ({want_cagr} chahiye), MaxDD {dd}% ({want_dd} chahiye)")
    except Exception as exc:                                          # noqa: BLE001
        bad("backtest fail", str(exc)[:150])


# ==================================================================== main
def main() -> int:
    print("=" * 92)
    print("VAJRA -- POORE SYSTEM KI JAANCH".center(92))
    print("=" * 92)
    print(f"\n{datetime.now():%Y-%m-%d %H:%M}\n")

    check_files()
    check_one_dataset()
    check_consistency()
    check_live()
    check_schedule()
    check_engine_tests()
    check_strategy_reproduces()

    n_ok = sum(1 for s, _, _ in results if s == "PASS")
    n_bad = sum(1 for s, _, _ in results if s == "FAIL")
    n_warn = sum(1 for s, _, _ in results if s == "CHETAVNI")

    print("\n" + "=" * 92)
    print(f"NATEEJA :  {n_ok} PASS   {n_warn} CHETAVNI   {n_bad} FAIL")
    print("=" * 92)

    if n_bad:
        print("\nJO FAIL HUA:")
        for s, name, det in results:
            if s == "FAIL":
                print(f"   - {name}" + (f"  ({det})" if det else ""))
        print("\n>>> Ek bhi FAIL hai. Asli paisa mat lagao jab tak ye theek na ho.")
    if n_warn:
        print("\nCHETAVNI (turant khatra nahi, par dekh lena):")
        for s, name, det in results:
            if s == "CHETAVNI":
                print(f"   - {name}" + (f"  ({det})" if det else ""))
    if not n_bad and not n_warn:
        print("\n>>> Sab theek hai. System chalne ke laayak hai.")
    return 1 if n_bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
