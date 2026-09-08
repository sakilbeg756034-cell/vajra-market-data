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
# 252 HI RAHEGA -- 257 nahi.
#
# Ye UNIVERSE ka niyam hai (VAJRA 750 me kaun aayega), signal ka nahi.
# Engine ka monthly_universe.py aur research ka mask dono 252 par hain.
# Ise 257 karne par cloud ka universe research se ALAG ho jaata -- aur
# wo farq kahin error nahi deta, bas dono jagah alag naam aane lagte.
# (7-Sep-2026 ko skip=5 jodte waqt ye galti ho kar pakdi gayi thi.)
#
# skip=5 ke liye jo 257 session chahiye wo apne aap sambhal jaata hai:
# jis naam ke paas 257 session nahi hain uska SCORE NaN aata hai, aur
# neeche `df["SCORE"].notna()` use ELIGIBLE se bahar kar deta hai.
# Shart wahin lagni chahiye jahan uska asar hai.
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

# Strategy gates -- `LOCKED_STRATEGY_V2.json` me locked.
#
# YE VALUE YAHAN HARDCODED HONI HI CHAHIYE: ye file GitHub Actions par chalti
# hai, jahan lock file maujood hi nahi hoti. Isliye inhe lock file se milaane
# ka kaam `verify_system.py` (hissa 3) karta hai -- wo teenon jagah (lock,
# engine, sheet) ka number aapas me milaata hai.
#
# Kyun 20 naam: kam naamo wale portfolio ka poora munafa mutthi bhar stocks par
# tik jaata hai -- naapa gaya, 411 me se sirf 20 naam par 96.5%. Wo hunar nahi,
# kismat hai. 20 naam par ye bhaar 56% par aa jaata hai, aur CAGR bhi behtar.
MIN_ADTV_INR = 2_500_000.0
MAX_STALE_SESSIONS = 21
MAX_FROZEN_RATE = 0.20
N_HOLDINGS = 20
EXIT_RANK = 50                      # = N_HOLDINGS x buffer 2.5

# Ek naam me kitna zyada se zyada paisa. 20 naam par barabar baantne se har ek
# 5% hota hai, isliye 15% ki hadd tabhi lagti hai jab kisi ka SCORE baaki sab se
# bahut ooncha ho. Backtest me yahi hadd thi.
MAX_WEIGHT = 0.15

# Sabse volatile naam nahi kharide jaate.
#
# 252-din ki volatility ke hisaab se, us din ke ELIGIBLE naamo me jo sabse upar
# ke 10% hain, wo nahi liye jaate. Naapa gaya (2016 se aage, cost ke baad):
#
#     bina filter        30.57%  Sharpe 1.076  MaxDD -42.9%
#     is filter ke saath 31.39%  Sharpe 1.165  MaxDD -44.3%
#
# Dhyan do: MaxDD is filter ke saath THODA KHARAB hota hai. Filter
# isliye hai ki risk-adjusted return (Sharpe) saaf behtar hai.
#
# (7-Sep-2026 par skip=5 ke saath dobara naapa gaya. Us se pehle ye
#  25.7% aur 26.8% the -- wo skip=0 ke number the.)
#
# EK BAAT JO JAAN-BOOJH KAR AISE HAI: percentile SIRF us din ke ELIGIBLE naamo
# me nikalta hai, poore panel me nahi.
#
# 6 September 2026 tak research ka code POORE PANEL par rank karta tha, jabki
# ye file eligible me. Do alag strategy chal rahi thin aur koi error nahi aata
# tha -- bas backtest 2.8 pp zyada dikhata tha. Ab research bhi yahi karta hai
# (`fastbt.run` me `cfg.max_vol_pct`).
#
# Ye file SAHI thi. Galat research ka code tha.
MAX_VOL_PERCENTILE = 0.90

# 3 mahine ka return aur all-time high. Ye faisle me nahi aate -- SCORE me inka
# koi hissa nahi -- par scanner me inke bina naam pehchana nahi jaata: "kitna
# upar chala gaya" aur "top se kitna neeche hai" wahi do sawal hain jo har koi
# pehle poochta hai.
SHORT_LOOKBACK = 63

OUTPUT_COLUMNS = [
    "RANK", "SYMBOL", "NAME", "SECTOR", "ISIN", "SERIES", "CLOSE", "SCORE",
    "WEIGHT_PCT",
    "R12_PCT", "R12_SKIP_PCT", "R6_PCT", "R3_PCT", "VOLATILITY_PCT",
    "VOL_RANK_PCT",
    "VAM", "AGREE",
    "ATH", "ATH_DATE", "FROM_ATH_PCT",
    "ADTV_CR", "STALE_SESSIONS", "FROZEN_RATE", "ELIGIBLE",
]


def isin_lineage(paths: StatePaths) -> pd.DataFrame:
    """SourceISIN -> CanonicalISIN. File na ho to khaali (purana behaviour).

    Ye jaan-boojh kar ek CHHOTI file hai aur store se ALAG hai: store me har
    row NSE ke apne ISIN ke saath padi rehti hai (wahi sach hai jo NSE ne us
    din kaha), aur naksha sirf PADHTE waqt lagta hai. Isse store kabhi jhootha
    nahi hota, aur naksha badalne par purana data dobara likhna nahi padta.
    """
    if not paths.isin_lineage.exists():
        return pd.DataFrame(columns=["SourceISIN", "CanonicalISIN"])
    frame = pd.read_parquet(paths.isin_lineage)
    return frame.dropna().drop_duplicates("SourceISIN")


def adjusted_frame(paths: StatePaths) -> pd.DataFrame:
    """Har row ka point-in-time adjusted close, company ke sthir ISIN par."""
    prices = paths.prices.as_posix()
    events = paths.events.as_posix()
    lineage = isin_lineage(paths)
    with duckdb.connect() as con:
        # ISIN KO COMPANY KE STHIR ID PAR LE AAO -- 8 September 2026.
        #
        # NSE face value badalne par naya ISIN de deta hai. Cloud ke liye wo
        # bilkul naya security ban jaata tha, isliye ek hi company ki series
        # do tukdo me pad jaati thi. Naapa gaya: 500-session store me 31 symbol
        # aise the. Nateeja do tarah ka nuksaan:
        #   * naya tukda 252-session wali shart par fail -> naam ~1 saal ke
        #     liye sheet se GAYAB (TDPOWERSYS, jo laptop ke backtest me rank 11
        #     par tha -- yaani kharidne wala naam)
        #   * jo bacha rehta uska R12 aadhi series par banta (V2RETAIL ka SCORE
        #     0.403 se alag tha, jo sabse bada farq tha)
        #
        # Naksha DONO taraf lagta hai -- bhaav par bhi aur corporate action par
        # bhi -- kyunki dono company ke hain, ISIN ke nahi. Adjustment phir bhi
        # `AdjustedThrough` se bandha rehta hai, isliye bootstrap rows par kuch
        # dobara nahi lagta.
        con.register("lineage", lineage)
        # CAST zaroori hai: khaali event file me ISIN ka type DOUBLE aa jaata
        # hai (koi row hi nahi hoti), aur DuckDB VARCHAR ke saath COALESCE
        # karne se mana kar deta hai. Ye asli store me kabhi nahi hota, par
        # ek naye/khaali store par script fatne ke bajay chalni chahiye.
        canon_p = ("COALESCE(lp.CanonicalISIN, CAST(p.ISIN AS VARCHAR))"
                   if len(lineage) else "CAST(p.ISIN AS VARCHAR)")
        canon_e = ("COALESCE(le.CanonicalISIN, CAST(ev.ISIN AS VARCHAR))"
                   if len(lineage) else "CAST(ev.ISIN AS VARCHAR)")
        join_p = ("LEFT JOIN lineage lp ON lp.SourceISIN = CAST(p.ISIN AS VARCHAR)"
                  if len(lineage) else "")
        join_e = ("LEFT JOIN lineage le ON le.SourceISIN = CAST(ev.ISIN AS VARCHAR)"
                  if len(lineage) else "")
        # Series column naya hai. Jo state file usse pehle bani thi usme wo nahi
        # hoga -- aur wahan 'EQ' likhna sach hai, kyunki tab intake BE/BZ leta
        # hi nahi tha. Isse purani state bina dobara bootstrap kiye chalti rehti
        # hai; naye din apni asli series ke saath aate hain.
        stored = {
            row[0] for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{prices}')"
            ).fetchall()
        }
        series_expr = "p.Series" if "Series" in stored else "'EQ' AS Series"
        return con.execute(
            f"""
            WITH ev AS (
                SELECT {canon_e} AS ISIN, CAST(ev.ExDate AS DATE) AS ExDate,
                       ev.PriceFactor
                FROM read_parquet('{events}') ev
                {join_e}
                WHERE ev.PriceFactor IS NOT NULL AND ev.PriceFactor <> 1.0
                  AND ev.ExDate IS NOT NULL
            ),
            praw AS (
                SELECT p.* REPLACE ({canon_p} AS ISIN),
                       p.ISIN AS SourceISIN
                FROM read_parquet('{prices}') p
                {join_p}
            ),
            -- Do source-ISIN ek hi din par ek hi company ke ho jaayein to ek
            -- hi row rakhni hai (warna pivot phat jaata hai). Aisa hona nahi
            -- chahiye -- naapa gaya: 0 baar -- par chup-chaap tootne se behtar
            -- hai ki niyam likha ho: jo sach me trade hua aur jiska turnover
            -- zyada tha, wahi rakha jaata hai.
            p AS (
                SELECT * FROM praw
                QUALIFY row_number() OVER (
                    PARTITION BY Date, ISIN
                    ORDER BY Traded DESC, TurnoverINR DESC NULLS LAST, SourceISIN
                ) = 1
            ),
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
                   {series_expr},
                   p.Traded, p.IsFrozenBar,
                   p.EngineQuarantined, p.AdjustedThrough,
                   -- NSE us din is company ko kis ISIN se bulata tha. Sheet
                   -- aur reconcile me YAHI dikhna chahiye -- aaj ka asli ISIN,
                   -- company ka andar wala sthir id nahi.
                   p.SourceISIN
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
            -- `Series` YAHAN SE KABHI MAT HATANA.
            --
            -- 8 September 2026 ko ye kami pakdi gayi. `adjusted_frame` Series
            -- nikalta tha, par YE SELECT use gira deta tha. Aage `rank_table`
            -- me ek "purani state file" wala rasta tha jo column na milne par
            -- chup-chaap sab kuch EQ maan leta tha -- aur wahi har roz chal
            -- raha tha. Nateeja: BE/BZ ka filter LIVE ME MARA HUA THA.
            --
            -- Naapa gaya (2026-09-07 ka published signal): 734 me se 734 naam
            -- "EQ" likhe the, jabki usi din ke bhavcopy me 248 BE aur 27 BZ
            -- the. HFCL 3 September ko BE me gaya tha aur signal me RANK 3,
            -- WEIGHT 6.20% par khada tha -- jabki backtest ka universe
            -- (`IsEQ`) aise naam ko kabhi nahi leta. Live aur backtest do
            -- alag strategy chala rahe the, aur kahin koi error nahi aata tha.
            SELECT Date, ISIN, SourceISIN, Symbol, Series, Close, TurnoverINR,
                   Traded, IsFrozenBar, EngineQuarantined, AdjustedThrough,
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

    # Event NSE ke ISIN par aate hain, par neeche `flagged` ke column company ke
    # STHIR id par hain. Bina mel ke jis naam ka ISIN badla ho uska quarantine
    # chup-chaap LAGTA HI NAHI -- aur ye udaar disha hai (cloud aisa naam
    # kharidne ko keh deta jise engine ne rok rakha hai). Isliye dono taraf
    # ek hi id par laaya jaata hai.
    if len(unratioed):
        unratioed = unratioed.assign(ISIN=_canon_series(paths, unratioed["ISIN"]))
    if len(known):
        known = known.assign(ISIN=_canon_series(paths, known["ISIN"]))

    # ENGINE KA FAISLA US DIN KA HAI -- SAAL BHAR KA NAHI.
    #
    # 8-Sep-2026 ko pakdi gayi. Neeche `flagged` par 252-session ka blackout
    # lagta hai (aakhri line). Wo blackout SAHI hai un jhatkon ke liye jinhe
    # koi corporate action nahi samjhata -- aisa break poore saal R12 ko
    # zeher kar deta hai. Par wo `EngineQuarantined` par lagana GALAT tha.
    #
    # `EngineQuarantined` = CorporateActionQuarantineFlag OR NOT
    # IsResearchEligible -- yaani "engine ne is din ko REVIEW me rakha".
    # Naapa gaya (VAJRA_DATA, 2025-09 se): aise 45 alag reason the aur unme
    # se EK BHI unexplained break nahi tha -- sab "REVIEW_NEEDED: Rights ...",
    # "Demerger", "BONUS_GAIR_EQUITY" -- aur har ek theek 3 DIN ka. Engine
    # unhe suljha kar bhaav theek kar deta hai; uske baad series saaf hai,
    # zeher kuch nahi bacha.
    #
    # Us 3-din wale flag par saal bhar ka blackout lagane se kya hua:
    #   aaj bandh naam            : 57
    #   inme se laptop ke universe me: 15 -- ADANIENT, HINDUNILVR, VEDL,
    #                               RATNAVEER (laptop par RANK 50, yaani
    #                               trade ke dayre me) aur 11 aur
    #   HINDUNILVR aakhri baar flag hua tha 273 DIN pehle
    #
    # Laptop yahi kaam theek karta hai: `IsResearchEligible` US DIN ka lagta
    # hai, aur 252-din ka blackout SIRF `UnexplainedBreak` par
    # (`data.unexplained_blackout`). Ab cloud bhi wahi karta hai --
    # engine ka faisla us din ka, aur blackout sirf un teen niyamo par jo
    # cloud KHUD pakadta hai (jo apni paribhasha se hi "samajh nahi aaya"
    # wale hain).
    engine_din_ka = (frame.pivot(index="Date", columns="ISIN", values="EngineQuarantined")
                     .reindex(index=close.index, columns=close.columns)
                     .fillna(False).astype(bool))
    flagged = pd.DataFrame(False, index=close.index, columns=close.columns)

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

    # Blackout SIRF cloud ke apne teen niyamo par. Engine ka faisla us din ka.
    blackout = flagged.rolling(LOOKBACK_BLACKOUT, min_periods=1).max().astype(bool)
    return (engine_din_ka | blackout).astype(bool)


def _canon_series(paths: StatePaths, isins: pd.Series) -> pd.Series:
    """Kisi bhi ISIN ki list ko company ke sthir id par le aao."""
    lin = isin_lineage(paths)
    if not len(lin):
        return isins
    naksha = dict(zip(lin["SourceISIN"].astype(str), lin["CanonicalISIN"].astype(str)))
    return isins.astype(str).map(lambda v: naksha.get(v, v))


def _reference(paths: StatePaths, universe) -> tuple[pd.Series, pd.Series]:
    """ISIN se company naam aur sector. Na mile to khaali -- andaaza nahi.

    `reference_names.parquet` roz ke run se banti hai aur usme NSE ka apna ISIN
    hota hai, jabki `universe` company ke STHIR id par hai. Isliye lookup se
    pehle dono ko ek hi id par laaya jaata hai -- warna jis naam ka ISIN badla
    ho uska company-naam aur sector chup-chaap khaali ho jaata.
    """
    empty = pd.Series("", index=universe)
    if not paths.reference.exists():
        return empty, empty.copy()
    ref = pd.read_parquet(paths.reference)
    ref = ref.assign(ISIN=_canon_series(paths, ref["ISIN"]))
    ref = ref.drop_duplicates("ISIN").set_index("ISIN")
    return (ref["NAME"].reindex(universe).fillna(""),
            ref["SECTOR"].reindex(universe).fillna(""))


def _all_time_high(paths: StatePaths, frame: pd.DataFrame,
                   close: pd.DataFrame, universe):
    """Asli all-time high -- store ke 500 session se nahi.

    Do hisse jodte hain:
      1. Bootstrap se pehle ka ATH, jo laptop par poore 17-saal ke data se nikla
      2. Store ke andar ka high

    Pehle hisse par bootstrap ke BAAD wala corporate action factor lagana padta
    hai. Bina uske ek 1:1 bonus ke baad ATH dogna dikhta rahega aur "top se
    kitna neeche" hamesha jhootha bada dikhega -- theek wahi galti jo is project
    me do baar ho chuki hai, bas doosre roop me.
    """
    store_high = close.max().reindex(universe)
    store_when = close.idxmax().reindex(universe)

    if not paths.ath_seed.exists():
        return store_high, store_when.dt.strftime("%Y-%m-%d").fillna("")

    boundary = pd.to_datetime(frame["AdjustedThrough"]).max()
    with duckdb.connect() as con:
        factors = con.execute(
            f"""
            SELECT ISIN, coalesce(exp(sum(ln(PriceFactor))), 1.0) AS Factor
            FROM read_parquet('{paths.events.as_posix()}')
            WHERE PriceFactor IS NOT NULL AND PriceFactor <> 1.0
              AND ExDate IS NOT NULL
              AND CAST(ExDate AS DATE) > DATE '{boundary.date().isoformat()}'
            GROUP BY ISIN
            """
        ).df().set_index("ISIN")["Factor"] if pd.notna(boundary) else pd.Series(dtype=float)

    # Dono taraf company ke sthir id par -- ATH seed VAJRA_DATA se aati hai aur
    # event calendar NSE ke ISIN par, jabki `universe` sthir id par hai. Bina
    # is mel ke jis naam ka ISIN badla ho uska ATH chup-chaap khaali ho jaata.
    seed = pd.read_parquet(paths.ath_seed)
    seed = seed.assign(ISIN=_canon_series(paths, seed["ISIN"]))
    seed = seed.drop_duplicates("ISIN").set_index("ISIN")
    if len(factors):
        factors = factors.groupby(_canon_series(paths, pd.Series(factors.index))
                                  .values).prod()
    scale = factors.reindex(universe).fillna(1.0)
    seed_high = seed["AthClose"].reindex(universe) * scale
    seed_when = pd.to_datetime(seed["AthDate"].reindex(universe))

    use_seed = seed_high.fillna(-1.0) >= store_high.fillna(-1.0)
    ath = seed_high.where(use_seed, store_high)
    when = seed_when.where(use_seed, store_when)
    return ath, when.dt.strftime("%Y-%m-%d").fillna("")


def rank_table(paths: StatePaths,
               seed_history: pd.Series | None = None) -> pd.DataFrame:
    frame = universe_metrics(paths)
    m = matrices(frame)
    asof = m["Close"].index[-1]
    member = vajra750_membership(frame, seed_history)

    sc = core.score(m["Close"])
    # DO alag R12 -- inhe mila dena hi wo galti hai jise 7-Sep-2026 ke skip=5
    # ke saath rokna hai:
    #   r12       -> AGREE ke liye (aaj tak). Sheet me "R12 %" yahi hai.
    #   r12_mag   -> VAM ke liye (paanch session pehle tak). Sheet me
    #                "R12 SKIP %". VAM = r12_mag / VOL, r12 / VOL NAHI.
    r12 = core.total_return(m["Close"], core.LONG_LOOKBACK)
    r12_mag = core.total_return(m["Close"], core.LONG_LOOKBACK, core.SKIP_SESSIONS)
    r6 = core.total_return(m["Close"], core.SHORT_LOOKBACK)
    r3 = core.total_return(m["Close"], SHORT_LOOKBACK)
    vol = core.realised_vol(m["Close"])
    liquidity = core.adtv(m["TurnoverINR"])
    frozen = core.frozen_rate(m["IsFrozenBar"], m["Traded"])
    # STALE ab us bhaav ko naapta hai jo VAM SACH ME use karta hai, yaani
    # t-257 wala -- t-252 wala nahi. Warna ye gate us bhaav ki jaanch karta
    # jispar score tika hi nahi hai.
    stale = core.stale_reference_gap(
        m["Traded"], core.LONG_LOOKBACK + core.SKIP_SESSIONS)

    at_asof = frame[frame["Date"] == asof].drop_duplicates("ISIN").set_index("ISIN")
    symbols = at_asof["Symbol"]
    # SERIES YAHAN HONA HI CHAHIYE -- aur na milne par CHILLANA hai.
    #
    # Pehle yahan ek chup-chaap fallback tha: column na mile to sab "EQ" maan
    # lo. Wo fallback purani state file ke liye tha, par asal me wo HAR DIN
    # chal raha tha, kyunki `universe_metrics` ka SELECT Series ko gira deta
    # tha. BE/BZ ka poora filter is ek line ki wajah se mara hua tha.
    #
    # Purani state file ka intezaam pehle se `adjusted_frame` me hai: wahan
    # column na ho to `'EQ' AS Series` likha jaata hai. Yaani yahan tak Series
    # hamesha pahunchti hai. Agar nahi pahunchi to kuch aur toota hai --
    # aur tab chup rehna hi sabse bada khatra hai.
    if "Series" not in at_asof.columns:
        raise RuntimeError(
            "Series column signal tak nahi pahuncha. BE/BZ ka filter bina "
            "iske lag hi nahi sakta, aur chup-chaap 'sab EQ' maan lena wahi "
            "bug hai jo 8-Sep-2026 ko pakdi gayi thi. "
            "`universe_metrics` ka SELECT dekho."
        )
    series_at_asof = at_asof["Series"].astype("string").str.upper()

    barred = quarantine(paths, frame, m["Close"], m["Traded"])
    live = member.loc[asof] & m["Traded"].loc[asof] & ~barred.loc[asof]
    universe = live[live].index

    names, sectors = _reference(paths, universe)
    px = m["Close"].loc[asof].reindex(universe)
    ath, ath_date = _all_time_high(paths, frame, m["Close"], universe)

    df = pd.DataFrame({
        "SYMBOL": symbols.reindex(universe),
        "NAME": names,
        "SECTOR": sectors,
        # ISIN me NSE ka AAJ ka ISIN jaata hai, company ka andar wala sthir id
        # nahi. Andar hum sthir id par jodte hain (taaki ISIN badalne par series
        # na toote), par bahar wahi dikhna chahiye jo NSE aaj bolta hai --
        # sheet me bhi aur reconcile ke join me bhi.
        "ISIN": (at_asof["SourceISIN"].reindex(universe)
                 if "SourceISIN" in at_asof.columns else pd.Series(universe, index=universe)),
        "SERIES": series_at_asof.reindex(universe).fillna("EQ"),
        "CLOSE": m["Close"].loc[asof].reindex(universe).round(2),
        "SCORE": sc.loc[asof].reindex(universe).round(4),
        "R12_PCT": (r12.loc[asof].reindex(universe) * 100).round(4),
        "R12_SKIP_PCT": (r12_mag.loc[asof].reindex(universe) * 100).round(4),
        "R6_PCT": (r6.loc[asof].reindex(universe) * 100).round(4),
        "R3_PCT": (r3.loc[asof].reindex(universe) * 100).round(4),
        "ATH": ath.round(2),
        "ATH_DATE": ath_date,
        "FROM_ATH_PCT": ((px / ath.replace(0.0, np.nan) - 1.0) * 100).round(2),
        "VOLATILITY_PCT": (vol.loc[asof].reindex(universe) * 100).round(4),
        "ADTV_CR": (liquidity.loc[asof].reindex(universe) / 1e7).round(2),
        "STALE_SESSIONS": stale.loc[asof].reindex(universe),
        "FROZEN_RATE": frozen.loc[asof].reindex(universe).round(3),
    })
    # VAM aur AGREE POORE number se, chhape hue number se nahi.
    #
    # Pehle ye dono R12_PCT / R6_PCT se bante the -- jo pehle hi ek dashamlav par
    # round ho chuke the. Nateeja: AADHARHFC ka asli R6 thoda positive tha par
    # "0.0%" par round ho gaya, isliye AGREE 0.5 ki jagah 0.0 chhapa, jabki SCORE
    # (jo poore number se banta hai) 0.5 hi maan raha tha. File ke andar hi
    # SCORE = VAM x AGREE ka hisaab nahi milta tha.
    #
    # Sheet me ye saaf dikh jaata: teen column saamne hote aur unka gunanfal
    # chauthe se mel nahi khaata. Aisi cheez bharosa todti hai, chahe rank par
    # koi asar na ho.
    vam_raw = (r12_mag / vol.replace(0.0, np.nan)).loc[asof].reindex(universe)
    agree_raw = (((r6 > 0).astype(float) + (r12 > 0).astype(float)) / 2)
    agree_raw = agree_raw.where(r6.notna() & r12.notna()).loc[asof].reindex(universe)
    df["VAM"] = vam_raw.round(4)
    df["AGREE"] = agree_raw

    eligible = (
        (df["ADTV_CR"] >= MIN_ADTV_INR / 1e7)
        & (df["STALE_SESSIONS"] <= MAX_STALE_SESSIONS)
        & (df["FROZEN_RATE"].fillna(0) <= MAX_FROZEN_RATE)
        & df["SCORE"].notna()
        # Surveillance segment kharida nahi jaata.
        #
        # BE (trade-for-trade) aur BZ (non-compliant) me har sauda delivery me
        # settle karna padta hai aur intraday mana hai. Aisa naam rank par aana
        # jhootha bharosa deta -- sheet me wo top-12 me dikhta, par usse waise
        # kharidna hota hi nahi jaise baaki naam.
        #
        # Un dino ka DATA phir bhi aata hai, warna price series me hole ban
        # jaata hai aur R12/R6 jhuthe ho jaate hain. Data alag sawaal hai,
        # trade alag.
        & df["SERIES"].fillna("EQ").eq("EQ")
    )

    # Ab sabse volatile 10% naam nikaalo -- ELIGIBLE ke ANDAR se.
    #
    # Kram mayne rakhta hai: percentile UN naamo me nikalta hai jo baaki har
    # shart paar kar chuke hain. Agar poore universe par nikaalte to cutoff wo
    # naam tay karte jinhe waise bhi kharida nahi ja sakta.
    #
    # `rank(pct=True)` NaN chhod deta hai, isliye pehle non-eligible ko NaN
    # kiya jaata hai. Ye ek line chup-chaap galat ho sakti thi.
    vol_in_eligible = df["VOLATILITY_PCT"].where(eligible)
    vol_rank = vol_in_eligible.rank(pct=True)
    low_vol_enough = vol_rank.le(MAX_VOL_PERCENTILE)
    # Jiska VOL hai hi nahi, wo waise bhi eligible nahi ho sakta.
    df["VOL_RANK_PCT"] = vol_rank.round(4)
    eligible = eligible & low_vol_enough.fillna(False)

    # RANK: BINA ROUND KIYE SCORE par, aur tie par bhi hamesha alag rank.
    #
    # 8-Sep-2026 ko pakdi gayi. Pehle yahan `df["SCORE"]` tha -- jo upar
    # `.round(4)` ho chuka hai -- aur `.rank(ascending=False)` ka default
    # method `average` hai. Do naam ka rounded SCORE barabar hote hi dono ko
    # 178.5 jaisa aadha rank milta, aur agli line ka `.round()` (banker's
    # rounding) dono ko 178 bana deta. Naapa gaya (7-Sep ki live file):
    # 660 ranked naam par 500 hi alag rank -- yaani 160 duplicate.
    #
    # Neeche se dekhne me ye bekaar lagta hai (sabse upar wala duplicate 178
    # par tha). Par `top` ki shart `RANK <= N_HOLDINGS` hai. Rank 20 par tie
    # hote hi `top` me **21 naam** aa jaate aur weight 21 me bant jaata --
    # yaani live 21 naam rakhta jabki backtest 20. Us din rank 1-60 me do
    # lagatar SCORE ka sabse chhota farq 0.0006 tha, yaani tie se sirf 6
    # rounding-kadam door.
    #
    # `fastbt.py` (jisne locked number banaye) `np.argsort(-s, kind="stable")`
    # se hamesha THEEK N naam leta hai. Ab live bhi wahi karta hai:
    # poore number par rank, aur `method="first"` -- tie par pehle wala aage.
    score_raw = sc.loc[asof].reindex(universe)
    df["RANK"] = score_raw.where(eligible).rank(ascending=False, method="first")
    df["ELIGIBLE"] = np.where(eligible, "HAAN", "NAHI")
    df = df.sort_values("RANK", na_position="last")
    df["RANK"] = df["RANK"].astype("Float64").round().astype("Int64")

    # AUR AB ISE SAABIT KARO -- maano mat.
    #
    # Upar wali bug ka sabse bura roop ye tha ki wo kahin AWAAZ nahi karti
    # thi: file theek dikhti, bas usme 21 naam par weight hota. Isliye ab
    # ginti KHUD jaanchi jaati hai. Galat file likhne se behtar hai koi file
    # na likhna.
    n_ranked = int(df["RANK"].notna().sum())
    n_top = int((df["RANK"].notna() & (df["RANK"] <= N_HOLDINGS)).sum())
    if n_top != min(N_HOLDINGS, n_ranked):
        raise RuntimeError(
            f"top-{N_HOLDINGS} me {n_top} naam aa gaye "
            f"(={min(N_HOLDINGS, n_ranked)} hone chahiye the). "
            f"RANK me tie hai -- weight galat naamo me bant jaata."
        )
    if int(df["RANK"].dropna().duplicated().sum()):
        raise RuntimeError("RANK me duplicate hai -- ranking ka tie-break toota.")

    # WEIGHT -- V2 me paisa barabar nahi, SCORE ke hisaab se bantata hai.
    #
    # Naapa gaya (OOS 2016-2026, cost ke baad): barabar baantne par 26.8%,
    # score se baantne par 27.8%, aur Sharpe 0.97 se 1.03.
    #
    # Sirf top-N par lagta hai (jo sach me kharide jaate hain), aur MAX_WEIGHT
    # ki hadd ke baad bacha hua hissa doosron me usi anupaat me baant diya
    # jaata hai -- warna weight ka jod 100% se kam reh jaata aur portfolio me
    # chup-chaap cash pada rehta.
    df["WEIGHT_PCT"] = pd.NA
    top = df["RANK"].notna() & (df["RANK"] <= N_HOLDINGS)
    if top.any():
        s = df.loc[top, "SCORE"].astype(float).clip(lower=0.0)
        w = (s / s.sum()) if s.sum() > 0 else pd.Series(
            1.0 / len(s), index=s.index)
        for _ in range(10):                       # cap lagakar dobara baanto
            over = w > MAX_WEIGHT
            if not over.any():
                break
            spare = float((w[over] - MAX_WEIGHT).sum())
            w[over] = MAX_WEIGHT
            room = ~over
            if not room.any() or w[room].sum() <= 0:
                break
            w[room] = w[room] + spare * (w[room] / w[room].sum())
        df.loc[top, "WEIGHT_PCT"] = (w * 100).round(2)
    df["STALE_SESSIONS"] = (
        df["STALE_SESSIONS"].astype("Float64").round().astype("Int64")
    )
    df.attrs["asof"] = asof
    return df[OUTPUT_COLUMNS]
