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


def info(name, detail=""):
    """Sirf batane ke liye -- na PASS, na FAIL, na CHETAVNI.

    Kuch number aise hote hain jinka roz badalna SAHI hai (jaise "aaj tak ka
    CAGR", jo har naye session par khisakta hai). Unhe PASS/FAIL me daalna do
    me se ek galti karta hai: ya to jhoothi FAIL roz aati hai, ya gate itna
    dheela kar diya jaata hai ki wo kuch pakadta hi nahi. Isliye ye teesri
    kism hai -- dikhta hai, ginti me nahi aata.
    """
    print(f"   [JAANKARI] {name}" + (f"  -- {detail}" if detail else ""), flush=True)


def head(t):
    print("\n" + "-" * 92)
    print(t)
    print("-" * 92, flush=True)


# ==================================================================== 1
def check_files():
    head("1. WO FILE JO HONI HI CHAHIYE")
    must = [
        (GATE / "VAJRA_SYSTEM.md", "poore system ka naksha"),
        # 7-Sep-2026: yahan PDF thi. Uske number chaar peedhi purane ho
        # chuke the (29.59% / 26.82%) aur ye jaanch phir bhi PASS deti
        # thi -- kyunki ye sirf "file hai ya nahi" dekhti hai, andar kya
        # likha hai wo nahi. PDF archive me chali gayi; ab uski jagah
        # Markdown hai, jise check_stale_numbers.py bhi chhaanta hai.
        (GATE / "VAJRA_STRATEGY_SAMJHAO.md", "strategy aasan bhasha me"),
        (GATE / "START_HERE_AI.md", "poore system ka darwaza"),
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
    core = (ENGINE / "code/src/vajra_regime/cloud/core.py").read_text(encoding="utf-8")
    gs = (SHEET_DIR / "VajraMomentumScanner.gs").read_text(encoding="utf-8")

    def grab(text, pat):
        m = re.search(pat, text, re.M)
        return m.group(1) if m else None

    # 8 September 2026: is list me pehle sirf CHAAR number the (n, exit rank,
    # vol filter, max weight). Baaki har lever bina milaan ke pada tha --
    # sabse zaroori `skip`, jo 7-Sep ko hi joda gaya tha aur jiske do jagah
    # alag hone par koi error nahi aata, bas rank chup-chaap alag ho jaate.
    # `min_adtv`, `max_frozen`, `max_stale` bhi ab yahin milte hain.
    checks = [
        ("kitne stock (n)", lock["n"],
         grab(sig, r"^N_HOLDINGS\s*=\s*(\d+)"), grab(gs, r"N_HOLDINGS:\s*(\d+)")),
        ("exit rank", int(lock["n"] * lock["buffer"]),
         grab(sig, r"^EXIT_RANK\s*=\s*(\d+)"), grab(gs, r"EXIT_RANK:\s*(\d+)")),
        ("vol filter", lock.get("max_vol_pct"),
         grab(sig, r"^MAX_VOL_PERCENTILE\s*=\s*([\d.]+)"), None),
        ("max weight", lock["max_weight"],
         grab(sig, r"^MAX_WEIGHT\s*=\s*([\d.]+)"), None),
        ("skip (kitne session chhode)", lock.get("skip", 0),
         grab(core, r"^SKIP_SESSIONS\s*=\s*(\d+)"), None),
        ("ADTV ka farsh", lock["min_adtv"],
         grab(sig, r"^MIN_ADTV_INR\s*=\s*([\d_.]+)"), None),
        ("frozen-bar ki hadd", lock["max_frozen"],
         grab(sig, r"^MAX_FROZEN_RATE\s*=\s*([\d.]+)"), None),
        ("stale-bhaav ki hadd", lock["max_stale"],
         grab(sig, r"^MAX_STALE_SESSIONS\s*=\s*(\d+)"), None),
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

    # AGREE ke do lookback -- lock me list hai, code me do alag constant.
    want_windows = sorted(lock.get("windows", []))
    got_windows = sorted(
        int(x) for x in (grab(core, r"^SHORT_LOOKBACK\s*=\s*(\d+)"),
                         grab(core, r"^LONG_LOOKBACK\s*=\s*(\d+)")) if x
    )
    (ok if got_windows == want_windows else bad)(
        "AGREE ke lookback", f"lock={want_windows}  engine={got_windows}")

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

    # BE/BZ ka filter live me SACH ME zinda hai?
    #
    # 8 September 2026: `universe_metrics` ka SELECT `Series` gira deta tha,
    # aur `rank_table` me ek chup-chaap fallback tha jo column na milne par
    # sab kuch EQ maan leta tha. Nateeja: surveillance filter live me MARA
    # HUA tha -- 734 me se 734 naam "EQ", jabki us din 248 BE aur 27 BZ the.
    # HFCL BE me jaa chuka tha aur signal me RANK 3 par khada tha.
    #
    # Ye jaanch code ki SHAKAL dekhti hai, output ki nahi -- kyunki kisi din
    # sach me saare naam EQ ho sakte hain, aur tab output wali jaanch jhoothi
    # shikayat karti.
    # Column ka kram badal sakta hai (8-Sep ko SourceISIN juda tha), isliye
    # jaanch `universe_metrics` ke SELECT me "Series" DHOONDHTI hai, poori line
    # se milaati nahi -- warna ye jaanch har chhote badlav par jhooth bolti.
    sel = re.search(r"SELECT\s+Date,\s*ISIN(.*?)FROM adj", sig, re.S)
    metrics_has_series = bool(sel) and "Series" in sel.group(1)
    no_silent_eq = 'pd.Series("EQ", index=at_asof.index' not in sig
    (ok if (metrics_has_series and no_silent_eq) else bad)(
        "BE/BZ filter live me zinda hai",
        f"universe_metrics me Series={'haan' if metrics_has_series else 'NAHI'}  "
        f"chup-chaap EQ maan lena hataya={'haan' if no_silent_eq else 'NAHI'}")

    # ISIN badalne par series toot to nahi rahi? (8-Sep-2026 ka sudhaar)
    has_lineage = "isin_lineage" in sig and "CanonicalISIN" in sig
    st_py = (ENGINE / "code/src/vajra_regime/cloud/state.py").read_text(encoding="utf-8")
    (ok if (has_lineage and "isin_lineage" in st_py) else bad)(
        "ISIN badalne par series judi rehti hai",
        f"signal.py me naksha={'haan' if has_lineage else 'NAHI'}  "
        f"state.py me file={'haan' if 'isin_lineage' in st_py else 'NAHI'}")


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

    # CLOUD KA STATUS SEEDHE CLOUD SE PADHO -- local clone se NAHI.
    #
    # 8 September 2026 ko ye kami pakdi gayi. Is hisse ka title tha "cloud par
    # jo file hai wahi", par ye `D:\google sheet maintenance file\vajra-signals`
    # ki LOCAL COPY padhta tha. Wo copy tabhi taaza hoti hai jab koi haath se
    # `git pull` kare. Yaani ye jaanch cloud ki sehat naapti hi nahi thi -- wo
    # sirf ye bataati thi ki aapne aakhri baar kab pull kiya tha.
    #
    # Us din ye "live signal taaza -- 2026-09-04 (4 din)" chhaap raha tha,
    # jabki cloud par 2026-09-07 ka signal pada tha. Agar cloud sach me mar
    # jaata, to ye jaanch usi tarah chup rehti.
    #
    # Repo public hai, isliye koi token nahi chahiye. Internet na ho to ye
    # CHETAVNI deti hai aur local copy par lautti hai -- par tab saaf likha
    # jaata hai ki number local copy ka hai, cloud ka nahi.
    cloud_st, source = None, "local copy"
    try:
        import urllib.request                                     # noqa: PLC0415
        url = ("https://raw.githubusercontent.com/sakilbeg756034-cell/"
               "vajra-signals/main/out/status.json")
        with urllib.request.urlopen(url, timeout=25) as resp:      # noqa: S310
            cloud_st = json.loads(resp.read().decode("utf-8"))
        source = "cloud"
    except Exception as exc:                                          # noqa: BLE001
        warn("cloud se status.json nahi mila",
             f"{str(exc)[:70]} -- neeche ke number LOCAL COPY ke hain")

    try:
        local_st = json.loads(status.read_text(encoding="utf-8"))
        st = cloud_st if cloud_st is not None else local_st
        ok("status.json", f"n_holdings={st['n_holdings']}  "
                          f"exit_rank={st['exit_rank']}  "
                          f"eligible={st['eligible']}/{st['universe_rows']}")
        last = date.fromisoformat(st["as_of_session"])
        age = (date.today() - last).days
        (ok if age <= 4 else bad)(
            f"live signal taaza ({source})", f"{last} ({age} din)"
            + ("" if age <= 4 else " -- GitHub Actions dekho"))
        if cloud_st is not None:
            lag = (date.fromisoformat(cloud_st["as_of_session"])
                   - date.fromisoformat(local_st["as_of_session"])).days
            (ok if lag == 0 else warn)(
                "local clone cloud ke barabar hai",
                f"local {local_st['as_of_session']}  cloud "
                f"{cloud_st['as_of_session']}"
                + ("" if lag == 0 else "  -- `git pull` chalao, warna "
                                       "reconcile.py purani file se milaayega"))
        if st["eligible"] >= st["universe_rows"]:
            bad("vol filter live me lag hi nahi raha",
                f"eligible {st['eligible']} == universe {st['universe_rows']}")
        else:
            ok("vol filter live me lag raha hai",
               f"{st['universe_rows'] - st['eligible']} naam bahar")
    except Exception as exc:                                          # noqa: BLE001
        bad("status.json padha nahi gaya", str(exc)[:90])

    check_published_csv()


# ==================================================================== 4b
def check_published_csv():
    """PUBLISH KI GAYI file ke ANDAR ka number bhi jaancho -- sirf column nahi.

    8 September 2026 ke audit me ye kami pakdi gayi, aur wo mehngi thi.

    Us waqt do gate the aur DONO is file se chook rahe the:
      * upar wala hissa sirf ye dekhta tha ki CSV me column maujood hain aur
        tareekh taaza hai -- andar ka number sahi hai ya nahi, wo nahi.
      * `reconcile.py` cloud ka signal laptop par DOBARA banata hai; publish
        ki gayi file wo padhta hi nahi.

    Nateeja us din khud dikh gaya. 07:22 baje BE/BZ ka fix push hua, par
    aakhri cloud run us se pehle (2026-09-07 19:24 UTC) chala tha. Poore din
    sheet me HFCL RANK 3 par, WEIGHT 6.20%, `SERIES = EQ`, `ELIGIBLE = HAAN`
    khada raha -- jabki cloud ke apne store me wo 3-Sep se BE hai. Dono gate
    PASS de rahe the.

    Isliye ye jaanch WAHI file padhti hai jo sheet padhti hai, aur use cloud
    ke apne store se milaati hai. Jo file operator dekhta hai, gate usi file
    ko dekhe -- uske jaise kisi doosre hisaab ko nahi.
    """
    import pandas as pd                                           # noqa: PLC0415

    # N aur exit-rank LOCK FILE se -- yahan haath se likhne ka matlab hota ek
    # aur jagah jo chup-chaap khisak jaati.
    try:
        spec = json.loads(LOCK.read_text(encoding="utf-8"))["final_spec"]
        n_holdings = int(spec["n"])
        exit_rank = int(spec["n"] * spec["buffer"])
    except Exception as exc:                                          # noqa: BLE001
        bad("lock file se n/exit_rank nahi mila", str(exc)[:90])
        return

    repo = SHEET_DIR / "vajra-signals"
    csv_path = repo / "out/latest_signals.csv"

    # Cloud se hi utaaro -- local clone tabhi taaza hoti hai jab koi `git pull`
    # kare, aur wahi chuppi upar wale hisse ko 8-Sep ko dhokha de chuki hai.
    src = "local copy"
    try:
        import io                                                 # noqa: PLC0415
        import urllib.request                                     # noqa: PLC0415
        url = ("https://raw.githubusercontent.com/sakilbeg756034-cell/"
               "vajra-signals/main/out/latest_signals.csv")
        with urllib.request.urlopen(url, timeout=30) as resp:      # noqa: S310
            live = pd.read_csv(io.StringIO(resp.read().decode("utf-8")))
        src = "cloud"
    except Exception as exc:                                          # noqa: BLE001
        warn("cloud se latest_signals.csv nahi mili",
             f"{str(exc)[:60]} -- local copy par jaanch ho rahi hai")
        if not csv_path.exists():
            bad("publish ki gayi CSV kahin nahi mili", str(csv_path))
            return
        live = pd.read_csv(csv_path)

    ranked = live[live["RANK"].notna()]

    # --- 1. RANK apne aap me theek hai? -------------------------------------
    dupes = int(ranked["RANK"].duplicated().sum())
    (ok if dupes == 0 else bad)(
        f"live CSV: RANK me duplicate nahi ({src})",
        f"{len(ranked)} ranked naam, {ranked['RANK'].nunique()} alag rank"
        + ("" if dupes == 0 else f" -- {dupes} duplicate, SCORE tie par rank "
                                 "round ho raha hai"))

    n_top = int((ranked["RANK"] <= n_holdings).sum())
    want_top = min(n_holdings, len(ranked))
    (ok if n_top == want_top else bad)(
        f"live CSV: top-{n_holdings} me theek {want_top} naam",
        f"{n_top} mile"
        + ("" if n_top == want_top else " -- tie par ek EXTRA naam khareeda "
                                        "jaata hai, aur weight bhi usme bantta hai"))

    # --- 2. WEIGHT ----------------------------------------------------------
    w = live["WEIGHT_PCT"].dropna()
    w_sum, w_max = float(w.sum()), (float(w.max()) if len(w) else 0.0)
    (ok if abs(w_sum - 100.0) <= 0.5 else bad)(
        "live CSV: weight ka jod 100%", f"{w_sum:.2f}% ({len(w)} naam par)")
    (ok if w_max <= 15.05 else bad)(
        "live CSV: kisi naam me 15% se zyada nahi", f"sabse bada {w_max:.2f}%")

    # --- 3. Sabse zaroori: SERIES cloud ke apne STORE se milta hai? ---------
    #
    # Yahi wo jaanch hai jo HFCL ko pakadti. CSV apna SERIES khud likhti hai,
    # aur wo galat ho sakta hai; store me NSE ka apna bhavcopy pada hai.
    store = repo / "state/prices.parquet"
    if not store.exists():
        warn("cloud store nahi mila, SERIES ka milaan nahi hua", str(store))
        return
    px = pd.read_parquet(store, columns=["Date", "Symbol", "Series"])
    asof = px["Date"].max()
    truth = (px[px["Date"] == asof].drop_duplicates("Symbol")
             .set_index("Symbol")["Series"])
    joined = live.assign(TRUE_SERIES=live["SYMBOL"].map(truth))
    checked = joined[joined["TRUE_SERIES"].notna()]
    mismatch = checked[checked["SERIES"] != checked["TRUE_SERIES"]]
    traded = mismatch[mismatch["RANK"].notna()
                      & (mismatch["RANK"] <= exit_rank)]
    (ok if len(traded) == 0 else bad)(
        "live CSV: SERIES cloud ke store se milta hai (rank 1-%d)" % exit_rank,
        f"{len(checked)} naam milaye, {len(mismatch)} par farq, "
        f"{len(traded)} trade ke dayre me"
        + ("" if len(traded) == 0 else " -- " + ", ".join(
            f"{r.SYMBOL} rank {int(r.RANK)}: CSV {r.SERIES} par store "
            f"{r.TRUE_SERIES}" for r in traded.head(5).itertuples())))

    be_bz = checked[checked["RANK"].notna()
                    & checked["TRUE_SERIES"].isin(["BE", "BZ"])]
    (ok if len(be_bz) == 0 else bad)(
        "live CSV: koi BE/BZ naam rank par nahi",
        "0 mila" if len(be_bz) == 0 else ", ".join(
            f"{r.SYMBOL} (rank {int(r.RANK)}, {r.TRUE_SERIES})"
            for r in be_bz.head(5).itertuples()))


# ==================================================================== 5
def check_engine_tests():
    head("5. ENGINE KE APNE TEST")
    try:
        # `-o addopts=""` ZAROORI hai.
        #
        # pyproject.toml me `addopts = "-q --disable-warnings --maxfail=1"` hai.
        # Use aise hi chhodne par do nuksaan hote the:
        #   * `--maxfail=1` pehli failure par ruk jaata tha -- yaani "kitne
        #     test toote" ka jawab kabhi nahi milta tha
        #   * addopts ka `-q` aur yahan ka `-q` mil kar DOUBLE-QUIET ban jaate
        #     the, aur us halat me pytest aakhri summary line ("156 passed")
        #     chhaapta hi nahi. Nateeja: is jaanch ka saboot sirf dots tha.
        # START_HERE_AI.md hissa 5 khud kehta hai ki addopts hataana zaroori
        # hai; ye file usi apne niyam ko nahi maan rahi thi.
        p = subprocess.run([str(PYTHON), "-m", "pytest", "tests/",
                            "-o", "addopts=", "-q", "--no-header"],
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
            code = code.strip()
            if state.lower() == "disabled":
                bad("scheduled task BAND hai", f"{state} -- data taaza nahi hoga")
            elif code == "267009":          # Windows: "task abhi chal raha hai"
                ok("scheduled task", f"{state}, abhi chal raha hai ({when})")
            elif code == "0":
                ok("scheduled task", f"{state}, aakhri run {when}, natija OK")
            else:
                # PEHLE YE SIRF CHETAVNI THI -- aur wo galat tha.
                # 8 September 2026: do raat se roz ka run fail ho raha tha
                # (NSE ki list me DUMMYHEG aa gaya tha) aur ye script phir bhi
                # "0 FAIL" chhaap rahi thi. Jis script ka kaam hi ye saabit
                # karna hai ki system theek hai, wo tootay hue pipeline par
                # chup nahi reh sakti.
                bad("roz ka task FAIL hua",
                    f"{state}, natija code {code}, {when} -- "
                    f"logs\\daily\\ ka aakhri log padho")
        else:
            bad("scheduled task nahi mila", (p.stderr or "")[:80])
    except Exception as exc:                                          # noqa: BLE001
        warn("task jaancha nahi ja saka", str(exc)[:90])

    st = ENGINE / "logs/latest_engine_run.json"
    if st.exists():
        try:
            d = json.loads(st.read_text(encoding="utf-8"))
            s = d.get("status")
            when = d.get("generated_at_local")
            # RUNNING ka matlab do me se ek hai:
            #   * run ABHI chal raha hai (waqt taaza hai) -- theek hai
            #   * run beech me MAR gaya (waqt purana hai) -- ye dekhna zaroori
            # 7-Sep-2026 ko yahi hua tha: 18:10 ka run poora kaam kar chuka tha
            # (dataset publish bhi ho gaya), par process maara gaya aur status
            # file me 13:46 wale purane run ka SUCCESS pada raha. Ab shuru me
            # hi RUNNING likha jaata hai, isliye aisa run chhupta nahi.
            #
            # FAILED ab CHETAVNI nahi, FAIL hai. Wajah upar scheduled task
            # wale note me likhi hai: tootay hue pipeline par "0 FAIL"
            # chhaapna hi sabse bada khatra hai.
            age_min = None
            try:
                age_min = (datetime.now() - datetime.fromisoformat(str(when))
                           ).total_seconds() / 60.0
            except Exception:                                         # noqa: BLE001
                pass
            if s == "SUCCESS":
                ok("aakhri engine run", f"{s}  {when}")
            elif s == "FAILED":
                bad("aakhri engine run FAIL hua",
                    f"{when} -- {str(d.get('message'))[:70]}")
            elif s == "RUNNING" and age_min is not None and age_min <= 60:
                # 60 minute se kam purana RUNNING = sach me chal raha hai.
                # Poora run ~13 minute leta hai, isliye 60 udaar hadd hai.
                ok("aakhri engine run", f"{s} (abhi chal raha hai)  {when}")
            elif s == "RUNNING":
                # Purana RUNNING = run beech me MAR gaya. 7-Sep-2026 ko yahi
                # hua tha: 18:10 ka run poora kaam kar chuka tha, par process
                # maara gaya aur status file me purana SUCCESS pada raha.
                bad("engine run beech me MAR gaya",
                    f"{s} likha hai par {when} ka hai -- itna purana RUNNING "
                    f"ka matlab process khatam ho gaya tha")
            else:
                warn("aakhri engine run", f"{s}  {when}")
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
# NAV ko USI DIN par kaato jis din tak ka data lock file ne dekha tha.
#
# Lock ek DAAWA hai: "is config par, is dataset par (2026-09-04 tak), CAGR
# 33.36% aata hai." Us daawe ko dobara jaanchne ke liye WAHI window chahiye.
# Bina kaate, dataset ke har naye session par number apne aap khisak jaata
# hai aur ye jaanch ROZ jhoothi FAIL deti -- 8-Sep-2026 ko theek yahi hua
# (33.54% aaya, jabki 04-Sep par kaatne se bilkul 33.36% mila).
# Aur roz jhoothi shikayat karne wale gate ko log dekhna chhod dete hain.
import pandas as pd
lock_last = pd.Timestamp(json.loads(
    Path(r'D:\VAJRA_RESEARCH\work\results\LOCKED_STRATEGY_V2.json')
    .read_text(encoding='utf-8'))['dataset_last_session'])
frozen = nav[nav.index <= lock_last]
print(f'{M.cagr(frozen)*100:.2f}|{M.max_dd(frozen)*100:.1f}|'
      f'{m["Close"].shape[0]}|{m["Close"].shape[1]}|'
      f'{frozen.index[-1].date()}|{M.cagr(nav)*100:.2f}|{nav.index[-1].date()}')
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
        cagr, dd, sess, names, cut_on, live_cagr, live_on = line[-1].split("|")
        cagr, dd = float(cagr), float(dd)
        ok("panel", f"{sess} session x {names} naam")

        lock_last = str(json.loads(LOCK.read_text(encoding="utf-8"))
                        ["dataset_last_session"])[:10]
        if cut_on != lock_last:
            # Lock 2026-09-04 tak ka daawa karta hai par panel usse pehle hi
            # khatam ho gaya -- yaani daawa dobara jaanchna mumkin hi nahi.
            warn("lock ka daawa jaancha nahi ja saka",
                 f"lock {lock_last} tak ka hai, panel sirf {cut_on} tak jaata hai")
        elif abs(cagr - want_cagr) < 0.05 and abs(dd - want_dd) < 0.2:
            ok("locked strategy wahi jawab deti hai",
               f"{cut_on} tak: CAGR {cagr}% ({want_cagr} chahiye), "
               f"MaxDD {dd}% ({want_dd} chahiye)")
        else:
            bad("NUMBER BADAL GAYA",
                f"{cut_on} tak: CAGR {cagr}% ({want_cagr} chahiye), "
                f"MaxDD {dd}% ({want_dd} chahiye)")

        # Aaj tak ka number sirf JAANKARI hai -- ispar koi gate nahi.
        # Dataset roz aage badhta hai, isliye ye roz thoda badlega. Ye
        # normal hai; jo cheez badalni NAHI chahiye wo upar wali line hai.
        info("aaj tak ka number (gate nahi)",
             f"{live_on} tak: CAGR {live_cagr}%  "
             f"(lock ke din {cut_on} par {cagr}% tha)")
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
