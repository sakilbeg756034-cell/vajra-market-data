"""Signal ka ganit. EK JAGAH, aur sirf ek jagah.

Ye file cloud chalata hai aur laptop ka reconciliation script isi se milata hai.
Isi liye ye apna alag module hai: agar score do jagah likha hota to wo dhire-dhire
alag ho jaata aur kisi ko pata bhi na chalta. Is project me theek yahi ho chuka
hai -- 2026-09-02 ko backtest ek purani data copy padh raha tha, sab "chal raha
tha", aur jawab galat tha.

Formula (STRATEGY-RULEBOOK me locked hai -- naye trial-log entry ke bina mat badlo):

    R12_MAG  = close[t-5] / close[t-257] - 1     <- SIRF ye skip karta hai
    R12_SIGN = close      / close[252]   - 1
    R6_SIGN  = close      / close[126]   - 1
    VOL      = stdev(daily log return, 252) * sqrt(252)
    VAM      = R12_MAG / VOL
    AGREE    = ((R6_SIGN > 0) + (R12_SIGN > 0)) / 2
    SCORE    = VAM * AGREE

SKIP = 5  (7 September 2026 ko joda gaya)
--------------------------------------------------------------------------
Kya badla: VAM ke andar wala 12-mahine ka return ab AAJ tak nahi, balki
PAANCH SESSION PEHLE tak naapa jaata hai -- yaani pichhla ek hafta chhod
diya jaata hai.

Kyun: ek hafte ka return "short-term reversal" se bhara hota hai -- jo stock
abhi-abhi tez bhaaga hai wo agle kuch din aksar thoda peeche aata hai. Us
hafte ko score me ginne se hum aksar us naam ko top par le aate the jo abhi
palatne wala tha.

Kyun 5 (aur 21 nahi): skip 2 se 7 tak ka ek CHAUDA pathaar naapa gaya (skip
0..30 me se 13 skip base se behtar the). 5 session = ek hafta, aur
Jegadeesh-Titman (1993) ke ASLI paper me skip ek HAFTA hi tha -- ek mahine
wala convention baad me Kenneth French ki UMD library se aaya. Yaani ye
number data se nahi nikala gaya; iski wajah pehle se maujood thi.

AGREE ko skip NAHI karte: AGREE sirf ye poochhta hai ki trend abhi bhi upar
hai ya nahi ("aaj ka bhaav 12/6 mahine pehle se ooper hai kya"). Uske liye
sabse taaza bhaav hi sahi hai. Skip sirf MAGNITUDE (kitna bhaaga) par lagta
hai, DISHA (kis taraf) par nahi.

VOL ko bhi skip NAHI karte: wo aaj tak ki volatility hai. Ye academic
convention hai aur isme aage ka koi data nahi aata.

LIVE PAR ISKA ASAR: VAM ab AAJ ke bhaav par nirbhar nahi karta. Din ke andar
bhaav hilne se SCORE ka magnitude hissa nahi badalta -- sirf AGREE (0, 0.5
ya 1) palat sakta hai. Isliye sheet ka LIVE SCORE pehle se zyada sthir hai.

Naapa gaya: research ka lab.build_score aur ye formula 5,892,875 cell par
BILKUL milte hain (sabse bada farq 0.0).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SESSIONS_PER_YEAR = 252
LONG_LOOKBACK = 252
SHORT_LOOKBACK = 126

# Pichhle kitne session chhodne hain (sirf VAM ke magnitude hisse me).
SKIP_SESSIONS = 5


def total_return(close: pd.DataFrame, lookback: int, skip: int = 0) -> pd.DataFrame:
    """Return [t-lookback-skip, t-skip] ke beech ka. skip=0 par purana behaviour."""
    if skip == 0:
        return close / close.shift(lookback) - 1.0
    return close.shift(skip) / close.shift(lookback + skip) - 1.0


def realised_vol(close: pd.DataFrame, window: int = SESSIONS_PER_YEAR) -> pd.DataFrame:
    log_ret = np.log(close / close.shift(1))
    return log_ret.rolling(window, min_periods=window).std() * np.sqrt(SESSIONS_PER_YEAR)


def score(close: pd.DataFrame) -> pd.DataFrame:
    """VAM * AGREE.

    AGREE do lookback ke beech ka jhagda pakadta hai: agar 12-mahine ka return
    positive hai par 6-mahine ka negative, to trend palat raha hai aur score
    aadha ho jaata hai. Dono negative hon to score zero -- aisa naam kabhi top
    par nahi aayega.

    DHYAN DO: yahan DO alag R12 hain, aur unhe mila dena hi wo galti hogi jo
    live aur research ko chup-chaap alag kar degi --
        r12_mag  : skip ke SAATH  -> sirf VAM me (kitna bhaaga)
        r12_sign : skip ke BINA   -> sirf AGREE me (kis taraf ja raha hai)
    """
    r12_mag = total_return(close, LONG_LOOKBACK, SKIP_SESSIONS)
    r12_sign = total_return(close, LONG_LOOKBACK)
    r6_sign = total_return(close, SHORT_LOOKBACK)
    vol = realised_vol(close)
    vam = r12_mag / vol.replace(0.0, np.nan)
    agree = ((r6_sign > 0).astype(float) + (r12_sign > 0).astype(float)) / 2.0
    agree = agree.where(r6_sign.notna() & r12_sign.notna())
    return vam * agree


def adtv(turnover_inr: pd.DataFrame, window: int = 63) -> pd.DataFrame:
    return turnover_inr.rolling(window, min_periods=1).mean()


def frozen_rate(frozen: pd.DataFrame, traded: pd.DataFrame,
                window: int = 126) -> pd.DataFrame:
    """Circuit par band bars ka hissa.

    Trade hue din ke hisaab se dekha jaata hai, calendar ke hisaab se nahi: jo din
    stock trade hi nahi hua wo hisaab me nahi aata, warna kam trade hone wala naam
    apne aap saaf dikhne lagta.
    """
    f = (frozen & traded).rolling(window, min_periods=1).sum()
    t = traded.rolling(window, min_periods=1).sum()
    return f / t.replace(0, np.nan)


def stale_reference_gap(traded: pd.DataFrame,
                        lookback: int = LONG_LOOKBACK) -> pd.DataFrame:
    """R12 ka jo purana bhaav use ho raha hai, wo kitne session baasi hai.

    R12 aaj ke close ko 252 session pehle ke close se todta hai. Agar us din stock
    trade hi nahi hua tha, to wahan pichhla bhaav carry-forward hua hoga -- aur tab
    R12 ek asli move nahi, ek thehri hui line ka bhram hai.
    """
    idx = np.arange(len(traded.index))[:, None]
    seen = np.maximum.accumulate(np.where(traded.to_numpy(), idx, -1), axis=0)
    last_traded = pd.DataFrame(seen, index=traded.index, columns=traded.columns)
    reference = pd.DataFrame(
        np.repeat(idx - lookback, traded.shape[1], axis=1),
        index=traded.index, columns=traded.columns,
    )
    return reference - last_traded.shift(lookback)
