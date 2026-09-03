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
import io
import json
import os
import sys
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from vajra_regime import corporate_actions as ca
from vajra_regime import nse_live
from vajra_regime.cloud import signal
from vajra_regime.cloud.state import StatePaths, append_sessions, read_meta, write_meta

# Ek run me itne se zyada session peeche nahi jaayenge. Laptop mahine bhar band
# rahe to bhi cloud roz chalta hai, isliye normally 1-3 din hi bharne hote hain;
# ye chhat sirf tab lagti hai jab Actions khud kai din band raha ho.
MAX_CATCHUP_SESSIONS = 45

# Corporate action calendar itne din peeche se refresh hota hai. NSE purane
# event der se bhi jodta/badalta hai, isliye sirf "aaj" dekhna kaafi nahi.
CA_LOOKBACK_DAYS = 120

# NSE ki apni Total Market list. Isme har naam ka poora company naam aur
# industry hai -- dono hamare bhavcopy me nahi aate. Sector isliye zaroori hai
# ki 12 stock ek hi sector me hon to ye ek chhupa hua joker hai, aur bina
# sector column ke wo dikhta hi nahi.
#
# Ye list NSE ke apne 750 se banti hai aur hamari VAJRA 750 turnover se, isliye
# ~84% naam hi milte hain. Baaki khaali rehte hain -- galat naam bhar dene se
# khaali chhod dena behtar hai.
REFERENCE_URL = ("https://niftyindices.com/IndexConstituent/"
                 "ind_niftytotalmarket_list.csv")

MIN_ELIGIBLE_NAMES = 300
MIN_UNIVERSE_ROWS = 500


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


def refresh_corporate_actions(paths: StatePaths, today: date) -> int:
    """CA calendar dobara laao aur har event ka price factor nikalo.

    Factor `classify_adjustment` se aata hai -- wahi function jo engine use karta
    hai. Jo event samajh na aaye (demerger, merger) uska factor None rehta hai
    aur signal.py use chhod deta hai: bina anupaat ke andaaza lagana hi wo galti
    hai jisse CUPID wala +406% bana tha.
    """
    opener = ca._nse_opener()
    start = today - timedelta(days=CA_LOOKBACK_DAYS)
    rows: list[dict] = []
    for chunk_start, chunk_end in ca._chunk_dates(start, today):
        _payload, chunk = ca._fetch_ca_json(opener, chunk_start, chunk_end)
        rows.extend(chunk)

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
    pairs = prices.drop_duplicates()
    counts = pairs.groupby("Symbol")["ISIN"].nunique()
    unique = counts[counts == 1].index
    lookup = (pairs[pairs["Symbol"].isin(unique)]
              .drop_duplicates("Symbol").set_index("Symbol")["ISIN"])

    out = events.copy()
    out["ISIN"] = out["Symbol"].astype(str).str.upper().map(lookup)
    return out[out["ISIN"].notna()].copy()


def refresh_reference(paths: StatePaths) -> int:
    """Company naam aur industry NSE se laao. Fail ho to purani list chalti rahe."""
    try:
        request = urllib.request.Request(
            REFERENCE_URL,
            headers={"User-Agent": nse_live.USER_AGENT, "Accept": "text/csv,*/*"},
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = response.read()
        frame = pd.read_csv(io.BytesIO(payload), encoding="utf-8-sig")
    except Exception as exc:                                    # noqa: BLE001
        # Naam aur sector sundarta hain, faisla nahi. Inke liye poora run
        # girana galat hoga -- purani list se kaam chal jaata hai.
        print(f"reference list nahi mili ({exc}); purani chalti rahegi")
        return 0

    out = pd.DataFrame({
        "ISIN": frame["ISIN Code"].astype(str).str.strip(),
        "NAME": frame["Company Name"].astype(str).str.strip(),
        "SECTOR": frame["Industry"].astype(str).str.strip(),
    })
    out = out[out["ISIN"].str.startswith("INE")].drop_duplicates("ISIN")
    paths.reference.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(paths.reference, index=False)
    return int(len(out))


def _seed_history(paths: StatePaths) -> pd.Series | None:
    if not paths.history_counts.exists():
        return None
    seed = pd.read_parquet(paths.history_counts)
    return seed.set_index("ISIN")["HistoryCount"].astype(float)


def run(root: Path, today: date, scratch: Path) -> dict:
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

    wanted = [
        last_stored + timedelta(days=n)
        for n in range(1, (today - last_stored).days + 1)
    ][-MAX_CATCHUP_SESSIONS:]

    scratch.mkdir(parents=True, exist_ok=True)
    added_rows, added_days, holidays = 0, [], []
    for day in wanted:
        frame = _bhavcopy_for(day, scratch)
        if frame is None:
            holidays.append(day.isoformat())
            continue
        added_rows += append_sessions(paths, frame)
        added_days.append(day.isoformat())

    events = refresh_corporate_actions(paths, today)
    reference = refresh_reference(paths)

    table = signal.rank_table(paths, _seed_history(paths))
    asof = table.attrs["asof"]
    asof_date = pd.Timestamp(asof).date()

    out = root / "out"
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "latest_signals.csv", index=False)
    table[["SYMBOL", "ISIN"]].to_csv(out / "universe_current.csv", index=False)

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
    }
    (out / "status.json").write_text(
        json.dumps(status, indent=2), encoding="utf-8"
    )

    meta.update({
        "last_run_utc": status["generated_at_utc"],
        "last_session": asof_date.isoformat(),
        "rows_appended_last_run": added_rows,
    })
    write_meta(paths, meta)

    _gate(status, table, asof_date, today)
    return status


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
    if len(status["top_symbols"]) < signal.N_HOLDINGS:
        problems.append(
            f"top-{signal.N_HOLDINGS} me sirf {len(status['top_symbols'])} naam mile"
        )
    if (today - asof).days > 10:
        problems.append(
            f"signal {asof} ka hai par aaj {today} hai -- data aage badha hi nahi"
        )
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
    args = parser.parse_args(argv)

    status = run(args.root, args.today or datetime.now(UTC).date(), args.scratch)
    print(json.dumps(status, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
