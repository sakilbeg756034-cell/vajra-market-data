"""Store se aaj ka ranking banao.

Do kaam hain, aur dono me look-ahead ghusne ki jagah hai:

1. BHAAV KO POINT-IN-TIME ADJUST KARNA. Kisi bhi din t ke bhaav par sirf wo
   corporate action lagta hai jiska ex-date t ke BAAD hai -- aur us row ke apne
   `AdjustedThrough` ke bhi baad, kyunki bootstrap rows me kuch pehle se laga hua
   hai. Wajah aur ek pakdi gayi galti state.py me likhi hai.

2. UNIVERSE. VAJRA 750 = har mahine ke aakhri session par 60-session median
   turnover se top 750, jo agle mahine bhar laagu rehta hai. Niyam wahi hai jo
   engine ke monthly_universe.py me hai: HistoryCount >= 252,
   TurnoverObservations60 >= 40, MedianTurnover60 > 0; rank MedianTurnover60 DESC.
"""
from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from vajra_regime.cloud import core
from vajra_regime.cloud.state import StatePaths

UNIVERSE_SIZE = 750
MIN_HISTORY_SESSIONS = 252
MIN_TURNOVER_OBSERVATIONS = 40
STALE_CALENDAR_DAYS = 7

# Wahi thresholds jo engine ke ca_repair.py me hain -- do jagah do number
# rakhne ka matlab hota do alag jawab.
UNEXPLAINED_MOVE_THRESHOLD = 0.50
UNEXPLAINED_EVENT_WINDOW_DAYS = 5
LOOKBACK_BLACKOUT = 252

# Jin corporate action ka koi anupaat hota hi nahi. Inhe "adjust" karna andaaza
# lagana hai, isliye engine inhe 252 session ke liye bahar rakhta hai -- aur
# cloud ko bhi wahi karna chahiye, warna cloud aisa naam khareedne ko keh dega
# jo backtest ke universe me kabhi tha hi nahi. VEDL (demerger) reconciliation
# me theek isi wajah se cloud me rank 44 par aa gaya tha.
UNRATIOED_ACTION_TYPES = ("RIGHTS", "MERGER", "DEMERGER", "SPLIT", "BONUS")
LONG_GAP_DAYS = 30
LONG_GAP_RETURN = 0.20

# Strategy gates -- STRATEGY-RULEBOOK me locked.
MIN_ADTV_INR = 2_500_000.0
MAX_STALE_SESSIONS = 21
MAX_FROZEN_RATE = 0.20
N_HOLDINGS = 12
EXIT_RANK = 36

OUTPUT_COLUMNS = [
    "RANK", "SYMBOL", "ISIN", "CLOSE", "SCORE", "R12_PCT", "R6_PCT",
    "VOLATILITY_PCT", "VAM", "AGREE", "ADTV_CR", "STALE_SESSIONS",
    "FROZEN_RATE", "ELIGIBLE",
]


def adjusted_frame(paths: StatePaths) -> pd.DataFrame:
    """Har row ka point-in-time adjusted close."""
    prices = paths.prices.as_posix()
    events = paths.events.as_posix()
    with duckdb.connect() as con:
        return con.execute(
            f"""
            WITH ev AS (
                SELECT ISIN, CAST(ExDate AS DATE) AS ExDate, PriceFactor
                FROM read_parquet('{events}')
                WHERE PriceFactor IS NOT NULL AND PriceFactor <> 1.0
                  AND ExDate IS NOT NULL
            ),
            p AS (SELECT * FROM read_parquet('{prices}')),
            f AS (
                SELECT p.Date AS d, p.ISIN AS i,
                       coalesce(exp(sum(ln(ev.PriceFactor))), 1.0) AS Factor
                FROM p
                LEFT JOIN ev
                       ON ev.ISIN = p.ISIN
                      AND ev.ExDate > p.Date
                      -- Jo is row me PEHLE SE laga hua hai use dobara mat lagao.
                      AND (p.AdjustedThrough IS NULL
                           OR ev.ExDate > p.AdjustedThrough)
                GROUP BY p.Date, p.ISIN
            )
            SELECT p.Date, p.ISIN, p.Symbol,
                   p.Close * f.Factor            AS Close,
                   p.Volume,
                   -- Engine turnover ko Close*Volume se banata hai, NSE ke
                   -- report kiye gaye traded value se nahi. Ye chunaav yahan
                   -- dohrana zaroori hai warna ADTV aur universe dono khisak
                   -- jaate hain. Split par price factor aur volume factor ek
                   -- doosre ke ulta hote hain, isliye ye gunanfal adjustment se
                   -- badalta hi nahi -- as-traded se ginna bhi wahi jawab deta.
                   p.Close * p.Volume            AS TurnoverINR,
                   p.Traded, p.IsFrozenBar,
                   p.EngineQuarantined, p.AdjustedThrough
            FROM p JOIN f ON f.d = p.Date AND f.i = p.ISIN
            ORDER BY p.Date, p.ISIN
            """
        ).df()


def _pivot(frame: pd.DataFrame, column: str, fill=None) -> pd.DataFrame:
    out = frame.pivot(index="Date", columns="ISIN", values=column).sort_index()
    return out if fill is None else out.fillna(fill)


def matrices(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    traded = _pivot(frame, "Traded", False).astype(bool)
    # Bina trade wale din ka bhaav aage carry hota hai -- warna har chhutti return
    # me ek jhootha jhatka ban jaati.
    close = _pivot(frame, "Close").ffill()
    return {
        "Close": close,
        "TurnoverINR": _pivot(frame, "TurnoverINR", 0.0),
        "Traded": traded,
        "IsFrozenBar": _pivot(frame, "IsFrozenBar", False).astype(bool),
    }


def universe_metrics(paths: StatePaths) -> pd.DataFrame:
    """MedianTurnover60, TurnoverObservations60 aur HistoryCount -- engine wale
    hi window par.

    Pandas ka `rolling(60)` calendar rows par chalta hai. Engine ka window
    `ROWS BETWEEN 59 PRECEDING AND CURRENT ROW` har security ki APNI rows par
    chalta hai. Jo naam roz trade nahi karta, dono ke liye "60" ka matlab alag
    ho jaata hai -- aur wahi 32 naam ka fark tha jo pehle reconciliation me mila.

    Isliye ye hisaab yahin SQL me hota hai, taaki semantics udhaar li jaayein,
    dobara likhi na jaayein.
    """
    frame = adjusted_frame(paths)
    with duckdb.connect() as con:
        con.register("adj", frame)
        return con.execute(
            """
            SELECT Date, ISIN, Symbol, Close, TurnoverINR, Traded, IsFrozenBar,
                   EngineQuarantined, AdjustedThrough,
                   ROW_NUMBER() OVER (
                       PARTITION BY ISIN ORDER BY Date
                   ) AS RowsInStore,
                   MEDIAN(TurnoverINR) OVER (
                       PARTITION BY ISIN ORDER BY Date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS MedianTurnover60,
                   COUNT(TurnoverINR) OVER (
                       PARTITION BY ISIN ORDER BY Date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS TurnoverObservations60,
                   MAX(CASE WHEN Traded THEN Date END) OVER (
                       PARTITION BY ISIN ORDER BY Date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS LastTradedDate
            FROM adj ORDER BY Date, ISIN
            """
        ).df()


def vajra750_membership(metrics: pd.DataFrame,
                        seed: pd.Series | None) -> pd.DataFrame:
    """Mahine ke aakhri session par top-750, agle mahine bhar laagu.

    Membership rebalance ke AGLE session se lagti hai (`idx > rd`), us din se
    nahi. Us ek din ka fark hi look-ahead hai: rebalance ke close se chuni gayi
    list us din ke apne signal me use nahi ki ja sakti.
    """
    median60 = metrics.pivot(index="Date", columns="ISIN",
                             values="MedianTurnover60").sort_index()
    observations = metrics.pivot(index="Date", columns="ISIN",
                                 values="TurnoverObservations60").sort_index()
    rows_in_store = metrics.pivot(index="Date", columns="ISIN",
                                  values="RowsInStore").sort_index()
    last_traded = metrics.pivot(index="Date", columns="ISIN",
                                values="LastTradedDate").sort_index()

    # Store sirf ~500 session rakhta hai, isliye ginti yahin se shuru karna galat
    # hoga -- 15 saal purana naam naya dikhta aur 252-session ki shart par fail
    # kar jaata. Bootstrap ke waqt ki asli ginti seed hoti hai.
    history = rows_in_store
    if seed is not None:
        history = history.add(seed.reindex(history.columns).fillna(0.0), axis=1)

    idx = median60.index
    month_end = pd.Series(idx, index=idx).groupby([idx.year, idx.month]).max()
    member = pd.DataFrame(False, index=idx, columns=median60.columns)

    rebalances = list(month_end)
    for i, rd in enumerate(rebalances):
        # Jo naam rebalance se 7 din se zyada pehle aakhri baar traded hua, wo
        # universe me nahi aata. Uska bhaav thehra hua hai, aur thehre bhaav par
        # nikala gaya momentum ek jhootha sapaat trend dikhata hai.
        stale = (rd - pd.to_datetime(last_traded.loc[rd])).dt.days
        eligible = (
            (history.loc[rd] >= MIN_HISTORY_SESSIONS)
            & (observations.loc[rd] >= MIN_TURNOVER_OBSERVATIONS)
            & (median60.loc[rd] > 0)
            & (stale <= STALE_CALENDAR_DAYS)
        )
        ranked = median60.loc[rd].where(eligible).sort_values(ascending=False)
        chosen = ranked.head(UNIVERSE_SIZE).index
        stop = rebalances[i + 1] if i + 1 < len(rebalances) else idx[-1]
        member.loc[(idx > rd) & (idx <= stop), chosen] = True
    return member


def quarantine(paths: StatePaths, frame: pd.DataFrame, close: pd.DataFrame,
               traded: pd.DataFrame) -> pd.DataFrame:
    """Jin naamon ka bhaav bharosemand nahi, unhe 252 session ke liye bahar rakho.

    Teen wajah, teenon ek hi baat kehti hain -- is series par momentum ka matlab
    nahi banta:

    1. AISA CORPORATE ACTION JISKA ANUPAAT NAHI HOTA (demerger, merger, rights).
       Inhe adjust karna andaaza hai. Engine inhe quarantine karta hai; cloud ne
       nahi kiya tha aur VEDL demerger ke beech rank 44 par aa gaya tha.
    2. LAMBA GAP (30 din se zyada) BADE JHATKE KE SAATH -- relisting ya suspension.
    3. AISA JHATKA JISE KOI EVENT NAHI SAMJHATA. NSE ke band 20% par rukte hain;
       usse bada move bina kisi event ke data ki kharabi hai. Yahi CUPID wala
       +406% tha.

    Kisi bhi haal me bhaav SUDHARA nahi jaata. Bina anupaat ke sudharna khud ek
    andaaza hai; jo samajh na aaye use chhod dena imaandar hai. Window 252 session
    ka isliye hai ki R12 utna hi peeche dekhta hai -- chhota rakhne par toota hua
    bhaav lookback ke andar hi baitha rehta.
    """
    # Bootstrap window me engine ka faisla hi chalta hai. Cloud ke apne niyam
    # sirf uske BAAD ke hisse par lagte hain -- warna cloud engine ke hal kiye
    # hue events ko dobara kaatne lagta hai (reconciliation me 110 naam extra
    # kat gaye the).
    boundary = pd.to_datetime(frame["AdjustedThrough"]).max()
    events_path = paths.events.as_posix()
    with duckdb.connect() as con:
        available = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{events_path}')"
        ).fetchall()}
        action_filter = (
            "AND ActionType IN " + str(UNRATIOED_ACTION_TYPES)
            if "ActionType" in available else ""
        )
        unratioed = con.execute(
            f"""
            SELECT DISTINCT ISIN, CAST(ExDate AS DATE) AS ExDate
            FROM read_parquet('{events_path}')
            WHERE ExDate IS NOT NULL AND PriceFactor IS NULL {action_filter}
            """
        ).df()
        known = con.execute(
            f"SELECT DISTINCT ISIN, CAST(ExDate AS DATE) AS ExDate "
            f"FROM read_parquet('{events_path}') WHERE ExDate IS NOT NULL"
        ).df()

    flagged = (frame.pivot(index="Date", columns="ISIN", values="EngineQuarantined")
               .reindex(index=close.index, columns=close.columns)
               .fillna(False).astype(bool))

    # 1. Anupaat-heen corporate action -- sirf bootstrap ke baad wale.
    for isin, ex in zip(unratioed["ISIN"], unratioed["ExDate"], strict=False):
        ex = pd.Timestamp(ex)
        if isin in flagged.columns and (pd.isna(boundary) or ex > boundary):
            flagged.loc[flagged.index >= ex, isin] = True

    ret = close.pct_change()
    # 2. Lamba gap + bada jhatka. Gap us naam ke apne traded din se naapa jaata
    #    hai, calendar se nahi -- warna har lambi chhutti gap ban jaati.
    traded_dates = close.index.to_series()
    for isin in close.columns:
        days = traded_dates[traded[isin].to_numpy()]
        if len(days) < 2:
            continue
        gaps = days.diff().dt.days
        breaks = days[(gaps > LONG_GAP_DAYS).to_numpy()]
        for d in breaks:
            if pd.notna(boundary) and d <= boundary:
                continue          # is hisse par engine ka faisla pehle se hai
            if abs(ret.at[d, isin]) > LONG_GAP_RETURN:
                flagged.loc[flagged.index >= d, isin] = True

    # 3. Jhatka jise koi event nahi samjhata.
    suspicious = ret.abs() > UNEXPLAINED_MOVE_THRESHOLD
    if suspicious.to_numpy().any():
        by_isin: dict[str, set] = {}
        for isin, ex in zip(known["ISIN"], known["ExDate"], strict=False):
            by_isin.setdefault(isin, set()).add(pd.Timestamp(ex))
        window = pd.Timedelta(days=UNEXPLAINED_EVENT_WINDOW_DAYS)
        for isin in close.columns[suspicious.any(axis=0).to_numpy()]:
            for d in close.index[suspicious[isin].fillna(False).to_numpy()]:
                if pd.notna(boundary) and d <= boundary:
                    continue      # is hisse par engine ka faisla pehle se hai
                if not any(abs(e - d) <= window for e in by_isin.get(isin, ())):
                    flagged.loc[d, isin] = True

    return flagged.rolling(LOOKBACK_BLACKOUT, min_periods=1).max().astype(bool)


def rank_table(paths: StatePaths,
               seed_history: pd.Series | None = None) -> pd.DataFrame:
    frame = universe_metrics(paths)
    m = matrices(frame)
    asof = m["Close"].index[-1]
    member = vajra750_membership(frame, seed_history)

    sc = core.score(m["Close"])
    r12 = core.total_return(m["Close"], core.LONG_LOOKBACK)
    r6 = core.total_return(m["Close"], core.SHORT_LOOKBACK)
    vol = core.realised_vol(m["Close"])
    liquidity = core.adtv(m["TurnoverINR"])
    frozen = core.frozen_rate(m["IsFrozenBar"], m["Traded"])
    stale = core.stale_reference_gap(m["Traded"])

    symbols = (frame[frame["Date"] == asof]
               .drop_duplicates("ISIN").set_index("ISIN")["Symbol"])

    barred = quarantine(paths, frame, m["Close"], m["Traded"])
    live = member.loc[asof] & m["Traded"].loc[asof] & ~barred.loc[asof]
    universe = live[live].index

    df = pd.DataFrame({
        "SYMBOL": symbols.reindex(universe),
        "ISIN": universe,
        "CLOSE": m["Close"].loc[asof].reindex(universe).round(2),
        "SCORE": sc.loc[asof].reindex(universe).round(4),
        "R12_PCT": (r12.loc[asof].reindex(universe) * 100).round(1),
        "R6_PCT": (r6.loc[asof].reindex(universe) * 100).round(1),
        "VOLATILITY_PCT": (vol.loc[asof].reindex(universe) * 100).round(1),
        "ADTV_CR": (liquidity.loc[asof].reindex(universe) / 1e7).round(2),
        "STALE_SESSIONS": stale.loc[asof].reindex(universe),
        "FROZEN_RATE": frozen.loc[asof].reindex(universe).round(3),
    })
    df["VAM"] = (df["R12_PCT"] / df["VOLATILITY_PCT"]).round(4)
    df["AGREE"] = (((df["R6_PCT"] > 0).astype(float)
                    + (df["R12_PCT"] > 0).astype(float)) / 2)

    eligible = (
        (df["ADTV_CR"] >= MIN_ADTV_INR / 1e7)
        & (df["STALE_SESSIONS"] <= MAX_STALE_SESSIONS)
        & (df["FROZEN_RATE"].fillna(0) <= MAX_FROZEN_RATE)
        & df["SCORE"].notna()
    )
    df["RANK"] = df["SCORE"].where(eligible).rank(ascending=False)
    df["ELIGIBLE"] = np.where(eligible, "HAAN", "NAHI")
    df = df.sort_values("RANK", na_position="last")
    df["RANK"] = df["RANK"].astype("Float64").round().astype("Int64")
    df["STALE_SESSIONS"] = (
        df["STALE_SESSIONS"].astype("Float64").round().astype("Int64")
    )
    df.attrs["asof"] = asof
    return df[OUTPUT_COLUMNS]
