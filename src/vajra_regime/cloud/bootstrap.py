"""Cloud store ko pehli baar bharo, VAJRA_DATA se. Ek hi baar chalta hai.

Cloud ko 252 session ka itihaas chahiye tabhi wo R12 nikaal sakta hai. Wo itihaas
NSE se roz-roz kheenchna 17 saal ka kaam hai, isliye ek baar laptop se de diya
jaata hai. Uske baad cloud khud aage badhta rahega.

SABSE ZAROORI BAAT: jo bhaav yahan se jaate hain wo PEHLE SE ADJUSTED hain --
engine ne unpar corporate action pehle hi laga rakhe hain. Cloud roz jo naya data
laata hai wo AS-TRADED hota hai. Do alag tarah ki rows ek hi file me.

Theek yahi mel `rolling_master.py` me bug ban chuka tha: dono par factor lag raha
tha, aur legacy rows par wo doosri baar lag raha tha. Saalon dikha nahi, kyunki
jab saari rows ek hi feed se aati hain to poori series ek saath scale hoti hai
aur return badalta hi nahi -- bug tabhi bahar aaya jab 2026 me doosri feed judi.

Isliye har bootstrap row par `AdjustedThrough` likha jaata hai: bootstrap ka
aakhri session. Cloud padhte waqt sirf usse AAGE ke ex-date wale factor lagata hai.

Pehla draft yahan EventId ki list likhta tha -- "jo engine ne laga diya use chhod
do". Wo galat tha aur reconciliation me pakda gaya. Engine ki legacy rows EOD2 se
aati hain aur EOD2 apna adjustment KHUD kar chuka hota hai; wo engine ke applied
ledger me hai hi nahi. HIRECT ka 2026-03-27 wala 1:1 bonus isi wajah se dobara
lag raha tha aur R12 52% ki jagah 204% dikh raha tha. Sawal "kisne lagaya" nahi,
"kab tak laga hua hai" hai.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from vajra_regime import corporate_actions as ca
from vajra_regime.cloud.state import RETAIN_SESSIONS, StatePaths, write_meta

DEFAULT_PUBLISHED = Path(r"D:\VAJRA_DATA")


def _prices(published: Path, sessions: int) -> pd.DataFrame:
    glob = (published / "nifty750" / "parquet" / "nifty750_*.parquet").as_posix()
    with duckdb.connect() as con:
        return con.execute(
            f"""
            WITH keep AS (
                SELECT DISTINCT Date FROM read_parquet('{glob}')
                ORDER BY Date DESC LIMIT {sessions}
            )
            SELECT Date, ISIN, Symbol, Open, High, Low, Close,
                   CAST(Volume AS BIGINT)          AS Volume,
                   TurnoverINR,
                   Volume > 0                      AS Traded,
                   IsFrozenBar,
                   (SELECT max(Date) FROM keep)    AS AdjustedThrough,
                   -- Engine ka apna quarantine faisla saath aata hai.
                   --
                   -- Cloud ise khud se nahi nikaal sakta: engine har RIGHTS ko
                   -- price-bridge aur security master se milata hai aur zyadatar
                   -- hal kar leta hai (aaj 143 me se sirf 20 quarantine hue).
                   -- Cloud ne wahi niyam khud lagaya to 110 naam zyada kaat diye.
                   -- Isliye faisla udhaar liya jaata hai, dobara nikala nahi.
                   (CorporateActionQuarantineFlag
                    OR NOT IsResearchEligible)     AS EngineQuarantined
            FROM read_parquet('{glob}')
            WHERE Date IN (SELECT Date FROM keep)
            ORDER BY Date, ISIN
            """
        ).df()


def _event_calendar(published: Path) -> pd.DataFrame:
    """Poora NSE corporate action calendar, factor ke saath.

    Factor `classify_adjustment` se aata hai -- wahi function jo engine chalata
    hai. Jo event samajh na aaye (demerger, merger) uska factor None rehta hai
    aur signal.py use chhod deta hai.
    """
    path = (published / "corporate_actions"
            / "official_nse_corporate_actions_all.parquet").as_posix()
    with duckdb.connect() as con:
        events = con.execute(
            f"""
            SELECT DISTINCT EventId, ISIN, Symbol,
                   CAST(ExDate AS DATE) AS ExDate, Subject
            FROM read_parquet('{path}')
            WHERE ExDate IS NOT NULL AND ISIN IS NOT NULL
            """
        ).df()
    parsed = [ca.classify_adjustment(str(s)) for s in events["Subject"]]
    return pd.DataFrame({
        "EventId": events["EventId"].astype(str),
        "ISIN": events["ISIN"].astype(str),
        "Symbol": events["Symbol"].astype(str),
        "ExDate": events["ExDate"],
        "PriceFactor": [p.price_factor for p in parsed],
        "VolumeFactor": [p.volume_factor for p in parsed],
        "ActionType": [p.action_type for p in parsed],
        "ParseStatus": [p.parse_status for p in parsed],
    }).sort_values(["ExDate", "ISIN"]).reset_index(drop=True)


def _history_seed(published: Path, first_stored: pd.Timestamp) -> pd.DataFrame:
    """Store shuru hone se PEHLE har naam ne kitne session dekhe the.

    Iske bina cloud ki ginti zero se shuru hoti aur har naam 252-session ki shart
    par fail karta -- universe khaali ho jaata aur wajah dhoondhne me ghante
    lagte, kyunki koi error nahi aati, bas list chhoti ho jaati.
    """
    glob = (published / "nifty750" / "parquet" / "nifty750_*.parquet").as_posix()
    with duckdb.connect() as con:
        return con.execute(
            f"""
            SELECT ISIN, max(HistoryCount) AS HistoryCount
            FROM read_parquet('{glob}')
            WHERE Date < DATE '{first_stored.date().isoformat()}'
            GROUP BY ISIN
            """
        ).df()


def run(published: Path, out: Path, sessions: int) -> dict:
    paths = StatePaths(out)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)

    prices = _prices(published, sessions)
    if prices.empty:
        raise SystemExit(f"{published} me nifty750 parquet nahi mila.")
    first = pd.Timestamp(prices["Date"].min())

    events = _event_calendar(published)
    seed = _history_seed(published, first)

    prices.to_parquet(paths.prices, index=False, compression="zstd")
    events.to_parquet(paths.events, index=False)
    seed.to_parquet(paths.history_counts, index=False)

    meta = {
        "bootstrapped_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "bootstrapped_from": str(published),
        "first_session": str(first.date()),
        "last_session": str(pd.Timestamp(prices["Date"].max()).date()),
        "sessions": int(prices["Date"].nunique()),
        "rows": int(len(prices)),
        "securities": int(prices["ISIN"].nunique()),
        "event_calendar_rows": int(len(events)),
        "adjusted_through": str(pd.Timestamp(prices["Date"].max()).date()),
        "prices_are": "ADJUSTED through adjusted_through; live rows appended later are AS-TRADED",
    }
    write_meta(paths, meta)
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--published", type=Path, default=DEFAULT_PUBLISHED)
    parser.add_argument("--out", type=Path, required=True,
                        help="vajra-signals repo ka checkout")
    parser.add_argument("--sessions", type=int, default=RETAIN_SESSIONS)
    args = parser.parse_args(argv)

    print(json.dumps(run(args.published, args.out, args.sessions), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
