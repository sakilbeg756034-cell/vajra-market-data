"""Roz ka cloud run: NSE se naya data lao, store aage badhao, ranking likho.

Ye GitHub Actions par chalta hai, laptop ke bina. Design ki sabse badi majboori
yahi thi: laptop mahine bhar band reh sakta hai, isliye ye layer kisi laptop-run
ka INTEZAAR nahi kar sakti aur na hi uspar bharosa kar sakti hai. Jo bhi jaanch
zaroori hai wo yahin, isi run me honi chahiye -- warna ek toota hua signal mahine
bhar chup-chaap sahi dikhta rahega.

Isi soch se har run ke aakhir me gates chalte hain aur fail hone par run RED hota
hai. Ek RED run ka matlab hai purani `out/` file waisi ki waisi padi rahegi --
galat nayi file likhne se purani sahi file behtar hai, aur GitHub khud mail bhej
dega.

Naya ganit yahan kuch nahi hai. Bhavcopy parser, corporate action parser aur
adjustment classifier -- teenon engine ke wahi tested function hain.
"""
from __future__ import annotations

import argparse
import http.client
import io
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from vajra_regime import corporate_actions as ca
from vajra_regime import nse_live
from vajra_regime.cloud import lineage_update, signal
from vajra_regime.cloud.state import StatePaths, append_sessions, read_meta, write_meta

# Ek run me itne se zyada session peeche nahi jaayenge. Laptop mahine bhar band
# rahe to bhi cloud roz chalta hai, isliye normally 1-3 din hi bharne hote hain;
# ye chhat sirf tab lagti hai jab Actions khud kai din band raha ho.
MAX_CATCHUP_SESSIONS = 45

# Corporate action calendar itne din peeche se refresh hota hai. NSE purane
# event der se bhi jodta/badalta hai, isliye sirf "aaj" dekhna kaafi nahi.
CA_LOOKBACK_DAYS = 120

# NAAM AUR SECTOR -- DO ALAG SOURCE, KYUNKI EK SE DONO NAHI MILTE
# ----------------------------------------------------------------
# Bhavcopy me na company ka poora naam hota hai na industry. Sector isliye
# zaroori hai ki 12 stock ek hi sector me hon to wo ek chhupa hua joker hai,
# aur bina sector column ke wo dikhta hi nahi.
#
# Pehle dono niftyindices ki Total Market list se aate the. Wo list NSE ke
# apne 750 se banti hai, jabki hamari VAJRA 750 turnover se -- isliye 731 me
# se 117 naam usme the hi NAHI, aur top-12 me se paanch cell khaali dikhte the
# (SBC, BLISSGVS, RPEL, RPTECH, HAPPYFORGE).
#
# NAAM ke liye ab NSE ki poori listed-equity list use hoti hai: usme 2,568
# naam hain aur hamare 731 me se 731 mil jaate hain. Nifty500, Microcap250,
# Smallcap250 -- teenon jaanche gaye, teenon Total Market ke ANDAR hi hain,
# isliye unse sector ka daayra nahi badhta.
#
# SECTOR sirf niftyindices se aata hai, aur wahi ~84% par rukta hai. NSE ka
# per-symbol metadata API usse bhar sakta tha par wo automated request ko
# block karta hai (403). Isliye baaki sector khaali rehte hain -- galat sector
# bhar dena khaali chhodne se bura hai, kyunki sector concentration ISI column
# se dekha jaata hai.
NAME_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
REFERENCE_URL = ("https://niftyindices.com/IndexConstituent/"
                 "ind_niftytotalmarket_list.csv")

MIN_ELIGIBLE_NAMES = 300
MIN_UNIVERSE_ROWS = 500

# Signal itne calendar din se purana ho to run RED. Ye hadd DO jagah lagti hai
# -- `_gate` me aur "naya din nahi, ruko" wale raste par -- isliye ek hi naam.
MAX_SIGNAL_AGE_DAYS = 10


def _bhavcopy_for(day: date, scratch: Path) -> pd.DataFrame | None:
    """Ek din ki as-traded rows (EQ + surveillance BE/BZ). Chhutti par None.

    404 ka matlab chhutti ya abhi publish nahi hua -- wo galti nahi hai. Baaki har
    HTTP error upar uthta hai, kyunki "data nahi mila" ko chup-chaap "aaj koi
    trade nahi hua" maan lena wahi chuppi hai jise ye pipeline rokne ki koshish
    karta hai.
    """
    url = nse_live.official_bhavcopy_url(day)
    destination = scratch / f"bhavcopy_{day:%Y%m%d}.zip"
    status, _ = nse_live._atomic_download(url, destination)
    if status == "NOT_PUBLISHED":
        return None

    frame, _report = nse_live.normalize_udiff_bhavcopy(destination, day)
    out = pd.DataFrame({
        "Date": pd.to_datetime(frame["Date"]).dt.date,
        "ISIN": frame["ISIN"].astype(str),
        "Symbol": frame["Symbol"].astype(str),
        # BE/BZ rows yahan aati hain taaki price series me hole na bane. Wo
        # tradeable nahi hain -- rok signal.py ke eligible mask me lagti hai.
        "Series": frame["Series"].astype(str),
        "Open": frame["Open"].astype(float),
        "High": frame["High"].astype(float),
        "Low": frame["Low"].astype(float),
        "Close": frame["Close"].astype(float),
        "Volume": frame["Volume"].astype("int64"),
        "TurnoverINR": frame["Turnover"].astype(float),
    })
    out["Traded"] = out["Volume"] > 0
    out["IsFrozenBar"] = (
        (out["Open"] == out["High"])
        & (out["High"] == out["Low"])
        & (out["Low"] == out["Close"])
    )
    # As-traded. Ispar abhi tak koi corporate action laga hi nahi, isliye
    # AdjustedThrough khaali -- sab kuch baad me point-in-time lagega.
    out["AdjustedThrough"] = pd.NaT
    # Live rows par engine ka koi faisla nahi hai -- inpar cloud ka apna
    # (thoda sakht) niyam lagta hai. Sakht hona surakshit disha hai: wo naam
    # chhodta hai, jodta nahi.
    out["EngineQuarantined"] = False
    return out


# NSE ka corporate action API kabhi-kabhi ek request par atak jaata hai --
# timeout, 5xx, ya JSON ki jagah HTML. Pehle ek hi jhatke se us koshish ka
# POORA signal ruk jaata tha. Ab teen koshish, beech me thoda intezaar. Teeno
# fail hon to run pehle ki tarah RED hota hai -- purane event par chup-chaap
# signal nahi banta. Code ki galti (KeyError wagairah) dobara nahi aazmayi jaati.
CA_FETCH_ATTEMPTS = 3
CA_RETRY_WAIT_SECONDS = (30, 90)
_RETRYABLE = (urllib.error.URLError, TimeoutError, ConnectionError,
              http.client.HTTPException, ValueError)
_sleep = time.sleep


def _fetch_ca_rows(start: date, today: date) -> list[dict]:
    """NSE CA calendar ki saari rows -- network ki chhoti gadbad par dobara koshish."""
    last: Exception | None = None
    for attempt in range(1, CA_FETCH_ATTEMPTS + 1):
        try:
            # Har koshish naye cookie/session ke saath, poori list shuru se.
            opener = ca._nse_opener()
            rows: list[dict] = []
            for chunk_start, chunk_end in ca._chunk_dates(start, today):
                _payload, chunk = ca._fetch_ca_json(opener, chunk_start, chunk_end)
                rows.extend(chunk)
            return rows
        except _RETRYABLE as exc:
            last = exc
            if attempt < CA_FETCH_ATTEMPTS:
                wait = CA_RETRY_WAIT_SECONDS[attempt - 1]
                print(f"NSE corporate action feed: koshish {attempt}/{CA_FETCH_ATTEMPTS} "
                      f"fail ({exc}); {wait}s baad dobara")
                _sleep(wait)
    raise RuntimeError(
        f"NSE corporate action feed {CA_FETCH_ATTEMPTS} koshish ke baad bhi nahi mila: {last}"
    ) from last


def refresh_corporate_actions(paths: StatePaths, today: date) -> int:
    """CA calendar dobara laao aur har event ka price factor nikalo.

    Factor `classify_adjustment` se aata hai -- wahi function jo engine use karta
    hai. Jo event samajh na aaye (demerger, merger) uska factor None rehta hai
    aur signal.py use chhod deta hai: bina anupaat ke andaaza lagana hi wo galti
    hai jisse CUPID wala +406% bana tha.
    """
    rows = _fetch_ca_rows(today - timedelta(days=CA_LOOKBACK_DAYS), today)

    normalized = ca.normalize_corporate_action_rows(rows)
    if normalized.empty:
        return 0

    # NSE ka corporate action feed SYMBOL par aata hai, ISIN par nahi. Engine
    # ISIN apne security master se nikalta hai; cloud ke paas wo nahi, isliye
    # apne hi store se nikala jaata hai.
    #
    # Jahan ek symbol ek se zyada ISIN par laga hai wahan event CHHOD diya jaata
    # hai, kisi ek par thopa nahi jaata. Poore dataset me 146 symbol aise hain,
    # aur galat security par bonus factor lagana usse kaheen bura hai ki event
    # chhoot jaye: chhoote hue event ka jhatka unexplained-break wale niyam me
    # pakda jaata hai aur wo naam quarantine ho jaata hai.
    normalized = _attach_isin(paths, normalized)
    if normalized.empty:
        return 0

    parsed = [ca.classify_adjustment(str(s)) for s in normalized["Subject"]]
    fresh = pd.DataFrame({
        "EventId": normalized["EventId"].astype(str),
        "ISIN": normalized["ISIN"].astype(str),
        "Symbol": normalized["Symbol"].astype(str),
        "ExDate": pd.to_datetime(normalized["ExDate"]).dt.date,
        "PriceFactor": [p.price_factor for p in parsed],
        "VolumeFactor": [p.volume_factor for p in parsed],
        "ActionType": [p.action_type for p in parsed],
        "ParseStatus": [p.parse_status for p in parsed],
    })

    if paths.events.exists():
        old = pd.read_parquet(paths.events)
        # Naya jawab jeetta hai: NSE kabhi-kabhi purana event sudharta hai.
        fresh = pd.concat([old[~old["EventId"].isin(fresh["EventId"])], fresh])

    # Purani file aur naya batch alag dtype le kar aa sakte hain (date vs
    # datetime vs object). Concat ke baad column object ban jaata hai aur
    # sort_values type error deta hai -- isliye dono ko ek hi type par laate hain.
    fresh["ExDate"] = pd.to_datetime(fresh["ExDate"], errors="coerce").dt.date
    fresh = fresh[fresh["ExDate"].notna()]
    fresh = fresh.sort_values(["ExDate", "ISIN"]).reset_index(drop=True)
    paths.events.parent.mkdir(parents=True, exist_ok=True)
    fresh.to_parquet(paths.events, index=False)
    return int(len(fresh))


def _attach_isin(paths: StatePaths, events: pd.DataFrame) -> pd.DataFrame:
    """Symbol se ISIN nikalo -- sirf tab jab jawab ek hi ho."""
    prices = pd.read_parquet(paths.prices, columns=["Symbol", "ISIN"])
    # A known ISIN transition is one company, not an ambiguous symbol.
    # Only the explicit lineage map may resolve it; never infer from names.
    lineage = signal.isin_lineage(paths)
    if not lineage.empty:
        lookup = lineage.set_index("SourceISIN")["CanonicalISIN"]
        prices["ISIN"] = prices["ISIN"].map(lookup).fillna(prices["ISIN"])
    prices["Symbol"] = prices["Symbol"].astype(str).str.strip().str.upper()
    pairs = prices.drop_duplicates()
    counts = pairs.groupby("Symbol")["ISIN"].nunique()
    unique = counts[counts == 1].index
    lookup = (pairs[pairs["Symbol"].isin(unique)]
              .drop_duplicates("Symbol").set_index("Symbol")["ISIN"])

    out = events.copy()
    out["ISIN"] = out["Symbol"].astype(str).str.strip().str.upper().map(lookup)
    return out[out["ISIN"].notna()].copy()


def _csv_from(url: str) -> pd.DataFrame:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": nse_live.USER_AGENT,
            "Accept": "text/csv,*/*",
            "Referer": "https://www.nseindia.com/",
        },
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = response.read()
    frame = pd.read_csv(io.BytesIO(payload), encoding="utf-8-sig")
    frame.columns = [str(c).strip() for c in frame.columns]
    return frame


def refresh_reference(paths: StatePaths) -> int:
    """Company naam aur industry laao. Fail ho to purani list chalti rahe.

    Naam aur sector alag-alag source se aate hain aur alag-alag door tak
    pahunchte hain. Dono ek saath fail nahi hote, isliye dono ki apni koshish
    hoti hai: sector na mile to naam phir bhi bhar jaate hain.
    """
    names = pd.DataFrame(columns=["ISIN", "NAME"])
    try:
        raw = _csv_from(NAME_URL)
        names = pd.DataFrame({
            "ISIN": raw["ISIN NUMBER"].astype(str).str.strip(),
            "NAME": raw["NAME OF COMPANY"].astype(str).str.strip(),
        })
    except Exception as exc:                                    # noqa: BLE001
        print(f"NSE equity list nahi mili ({exc}); naam ke liye index list par bharosa")

    sectors = pd.DataFrame(columns=["ISIN", "NAME", "SECTOR"])
    try:
        raw = _csv_from(REFERENCE_URL)
        sectors = pd.DataFrame({
            "ISIN": raw["ISIN Code"].astype(str).str.strip(),
            "NAME": raw["Company Name"].astype(str).str.strip(),
            "SECTOR": raw["Industry"].astype(str).str.strip(),
        })
    except Exception as exc:                                    # noqa: BLE001
        print(f"index constituent list nahi mili ({exc}); sector khaali rahenge")

    if names.empty and sectors.empty:
        # Naam aur sector sundarta hain, faisla nahi. Inke liye poora run
        # girana galat hoga -- purani list se kaam chal jaata hai.
        print("koi reference list nahi mili; purani chalti rahegi")
        return 0

    # Poori equity list ke naam pehle rakhe jaate hain, kyunki wo har listed
    # naam tak pahunchti hai. Index list uske baad aati hai aur sirf wahan
    # bharti hai jahan pehli ne kuch diya hi nahi -- aur sector to sirf wahin
    # se aata hai.
    stacked = pd.concat(
        [names.assign(SECTOR=pd.NA), sectors], ignore_index=True
    )
    stacked["ISIN"] = stacked["ISIN"].astype(str).str.strip()
    stacked = stacked[stacked["ISIN"].str.startswith("INE")]
    merged = (
        stacked.groupby("ISIN", as_index=False)
        .agg({"NAME": "first", "SECTOR": "first"})
    )

    paths.reference.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(paths.reference, index=False)
    return int(len(merged))


def _seed_history(paths: StatePaths) -> pd.Series | None:
    if not paths.history_counts.exists():
        return None
    seed = pd.read_parquet(paths.history_counts)
    return seed.set_index("ISIN")["HistoryCount"].astype(float)


def _already_published(root: Path, session: date) -> bool:
    """out/status.json isi session ka hai? Padh na paaye to NAHI -- andaaza nahi."""
    try:
        status = json.loads((root / "out" / "status.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(status, dict) and status.get("as_of_session") == session.isoformat()


def run(root: Path, today: date, scratch: Path, force: bool = False) -> dict:
    paths = StatePaths(root)
    if not paths.prices.exists():
        raise FileNotFoundError(
            "state/prices.parquet nahi mila. Pehle laptop par bootstrap chalao: "
            "python -m vajra_regime.cloud.bootstrap --out <repo>"
        )

    meta = read_meta(paths)
    have = set(pd.read_parquet(paths.prices, columns=["Date"])["Date"].unique())
    have = {pd.Timestamp(d).date() for d in have}
    last_stored = max(have)

    # Never discard the oldest missing dates and then report a fresh signal.
    # Recovery beyond this bounded run needs an explicit store rebuild.
    gap_days = (today - last_stored).days
    if gap_days < 0:
        raise RuntimeError("Cloud store contains a future session; build stopped")
    if gap_days > MAX_CATCHUP_SESSIONS:
        raise RuntimeError(
            f"Cloud catch-up gap is {gap_days} calendar days, limit "
            f"{MAX_CATCHUP_SESSIONS}; rebuild state without skipping sessions"
        )

    wanted = [
        last_stored + timedelta(days=n)
        for n in range(1, (today - last_stored).days + 1)
    ]

    scratch.mkdir(parents=True, exist_ok=True)
    added_rows, added_days, holidays = 0, [], []
    for day in wanted:
        frame = _bhavcopy_for(day, scratch)
        if frame is None:
            holidays.append(day.isoformat())
            continue
        added_rows += append_sessions(paths, frame)
        added_days.append(day.isoformat())

    # NAYA DIN NAHI AAYA -- TURANT RUKO (14-Sep-2026).
    #
    # Workflow ab 16:37 IST se har 15 minute chalta hai, kyunki NSE bhavcopy
    # ~16:33 IST par daalta hai. Zyadatar koshishon me naya din hota hi nahi.
    # Tab corporate action API aur reference list dobara maangna NSE par faltu
    # bojh hai -- aur baar-baar maangne par NSE GitHub ko block kar sakta hai,
    # jisse poora live signal band ho jaata. Jo session pehle hi publish ho
    # chuka hai uska signal dobara banane se kuch naya nahi milta.
    #
    # Keemat: kisi purane din ka corporate action NSE der se jode, to wo agle
    # naye session ke run me lagta hai, usi shaam nahi. Haath se chalaya run
    # (`--force`) hamesha poora hisaab dobara karta hai.
    if not added_days and not force and _already_published(root, last_stored):
        # Rukne se pehle PURANA-DATA wali chetavni. Bina iske NSE ka data hafton
        # na aaye to har koshish chup-chaap "skipped" deti: run kabhi RED nahi
        # hota aur GitHub email nahi aata (14-Sep-2026 ko pakda). `_gate` wala
        # hi niyam aur wahi sandesh -- do jagah do niyam nahi.
        stale = _stale_problem(last_stored, today)
        if stale:
            raise SystemExit("CLOUD SIGNAL BUILD ROKA GAYA:\n  - " + stale)
        print(f"koi naya session nahi -- {last_stored} pehle se publish hai; run yahin ruka")
        return {"skipped": True, "as_of_session": last_stored.isoformat(),
                "holidays_or_unpublished": holidays}

    # Naye ISIN badlav corporate action se PEHLE jodne hain: `_attach_isin`
    # isi naksha se symbol ko company par laata hai. Ulta kram hone par
    # face-value split ka apna event "ek symbol, do ISIN" maan kar chhoot jaata.
    lineage_report = lineage_update.extend(paths)
    events = refresh_corporate_actions(paths, today)
    reference = refresh_reference(paths)

    table = signal.rank_table(paths, _seed_history(paths))
    asof = table.attrs["asof"]
    asof_date = pd.Timestamp(asof).date()

    eligible = int((table["ELIGIBLE"] == "HAAN").sum())
    top = table[table["RANK"].notna()].head(signal.N_HOLDINGS)
    status = {
        "as_of_session": asof_date.isoformat(),
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        # Jo file padhega use pata hona chahiye ki ye kahan bani. Hamesha
        # "github-actions" likh dena ek chhota jhooth hai jo debug ke waqt
        # mehnga padta hai.
        "built_by": "github-actions" if os.environ.get("GITHUB_ACTIONS") else "laptop",
        "universe_rows": int(len(table)),
        "eligible": eligible,
        "n_holdings": signal.N_HOLDINGS,
        "exit_rank": signal.EXIT_RANK,
        "top_symbols": list(top["SYMBOL"]),
        "sessions_added_this_run": added_days,
        "holidays_or_unpublished": holidays,
        "corporate_action_events_known": events,
        "reference_names_known": reference,
        # Naye ISIN badlav: jo joda gaya, aur jo haal ka naya ISIN kisi purane
        # tukde se nahi juda. Doosri list khaali na ho to sheet chetavni deti hai.
        "isin_lineage_added": lineage_report["added"],
        "isin_lineage_unresolved": lineage_report["unresolved"],
    }
    # Validate BEFORE touching the last published outputs, including local runs.
    _gate(status, table, asof_date, today)
    out = root / "out"
    out.mkdir(parents=True, exist_ok=True)
    # Serialize all files first. Each replacement is atomic on the same volume;
    # GitHub publishes the resulting directory together in its output commit.
    with tempfile.TemporaryDirectory(dir=root, prefix=".signal-stage-") as stage:
        staging = Path(stage)
        table.to_csv(staging / "latest_signals.csv", index=False)
        table[["SYMBOL", "ISIN"]].to_csv(staging / "universe_current.csv", index=False)
        (staging / "status.json").write_text(
            json.dumps(status, indent=2), encoding="utf-8"
        )
        for name in ("latest_signals.csv", "universe_current.csv", "status.json"):
            os.replace(staging / name, out / name)

    meta.update({
        "last_run_utc": status["generated_at_utc"],
        "last_session": asof_date.isoformat(),
        "rows_appended_last_run": added_rows,
    })
    write_meta(paths, meta)

    return status


def _stale_problem(asof: date, today: date) -> str | None:
    """Signal hadd se purana hai? Hai to wajah, warna None."""
    if (today - asof).days > MAX_SIGNAL_AGE_DAYS:
        return f"signal {asof} ka hai par aaj {today} hai -- data aage badha hi nahi"
    return None


def _gate(status: dict, table: pd.DataFrame, asof: date, today: date) -> None:
    """Galat file likhne se behtar hai koi nayi file na likhna.

    Har jaanch ek aisi khaamosh kharaabi pakadti hai jo dekhne me theek lagti hai:
    khaali ranking, aadha universe, ya sabse khatarnaak -- ek signal jo hafton
    purana ho par roz "aaj ka" dikhta rahe.
    """
    problems = []
    if len(table) < MIN_UNIVERSE_ROWS:
        problems.append(
            f"universe me sirf {len(table)} naam (kam se kam {MIN_UNIVERSE_ROWS} chahiye)"
        )
    if status["eligible"] < MIN_ELIGIBLE_NAMES:
        problems.append(
            f"sirf {status['eligible']} eligible naam "
            f"(kam se kam {MIN_ELIGIBLE_NAMES} chahiye)"
        )
    ranks = pd.to_numeric(table["RANK"], errors="coerce")
    ranked = ranks.dropna()
    top = table[ranks.between(1, signal.N_HOLDINGS)]
    if ranked.duplicated().any() or not ranked.eq(ranked.round()).all() or (ranked < 1).any():
        problems.append("RANK must contain unique positive integers")
    if len(top) != signal.N_HOLDINGS:
        problems.append(
            f"top-{signal.N_HOLDINGS} me {len(top)} naam mile"
        )
    if not table.loc[ranks.notna(), "ELIGIBLE"].eq("HAAN").all():
        problems.append("ineligible name has a trade rank")
    if "SERIES" not in table or not table.loc[ranks.notna(), "SERIES"].eq("EQ").all():
        problems.append("ranked names must have verified EQ series")
    if asof > today:
        problems.append("signal date is in the future")
    stale = _stale_problem(asof, today)
    if stale:
        problems.append(stale)
    if problems:
        raise SystemExit(
            "CLOUD SIGNAL BUILD ROKA GAYA:\n  - " + "\n  - ".join(problems)
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True,
                        help="vajra-signals repo ka checkout")
    parser.add_argument("--today", type=date.fromisoformat, default=None)
    parser.add_argument("--scratch", type=Path, default=Path("_scratch"))
    parser.add_argument("--force", action="store_true",
                        help="naya session na ho tab bhi poora signal dobara banao")
    args = parser.parse_args(argv)

    status = run(args.root, args.today or datetime.now(UTC).date(), args.scratch,
                 force=args.force)
    print(json.dumps(status, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
