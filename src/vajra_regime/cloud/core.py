"""Signal ka ganit. EK JAGAH, aur sirf ek jagah.

Ye file cloud chalata hai aur laptop ka reconciliation script isi se milata hai.
Isi liye ye apna alag module hai: agar score do jagah likha hota to wo dhire-dhire
alag ho jaata aur kisi ko pata bhi na chalta. Is project me theek yahi ho chuka
hai -- 2026-09-02 ko backtest ek purani data copy padh raha tha, sab "chal raha
tha", aur jawab galat tha.

Formula (STRATEGY-RULEBOOK me locked hai -- naye trial-log entry ke bina mat badlo):

    R12   = close / close[252] - 1
    R6    = close / close[126] - 1
    VOL   = stdev(daily log return, 252) * sqrt(252)
    VAM   = R12 / VOL
    AGREE = ((R6 > 0) + (R12 > 0)) / 2
    SCORE = VAM * AGREE
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SESSIONS_PER_YEAR = 252
LONG_LOOKBACK = 252
SHORT_LOOKBACK = 126


def total_return(close: pd.DataFrame, lookback: int) -> pd.DataFrame:
    return close / close.shift(lookback) - 1.0


def realised_vol(close: pd.DataFrame, window: int = SESSIONS_PER_YEAR) -> pd.DataFrame:
    log_ret = np.log(close / close.shift(1))
    return log_ret.rolling(window, min_periods=window).std() * np.sqrt(SESSIONS_PER_YEAR)


def score(close: pd.DataFrame) -> pd.DataFrame:
    """VAM * AGREE.

    AGREE do lookback ke beech ka jhagda pakadta hai: agar 12-mahine ka return
    positive hai par 6-mahine ka negative, to trend palat raha hai aur score
    aadha ho jaata hai. Dono negative hon to score zero -- aisa naam kabhi top
    par nahi aayega.
    """
    r12 = total_return(close, LONG_LOOKBACK)
    r6 = total_return(close, SHORT_LOOKBACK)
    vol = realised_vol(close)
    vam = r12 / vol.replace(0.0, np.nan)
    agree = ((r6 > 0).astype(float) + (r12 > 0).astype(float)) / 2.0
    agree = agree.where(r6.notna() & r12.notna())
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
