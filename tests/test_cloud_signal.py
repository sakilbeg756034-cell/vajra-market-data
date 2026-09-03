"""Cloud layer ka wo niyam jo do baar galat ho chuka hai.

Pehli baar engine me: `rolling_master.py` dono feed par corporate action factor
laga raha tha, jabki legacy feed pehle se adjusted thi.

Doosri baar cloud me, isi session me: bootstrap rows par factor dobara lag raha
tha kyunki filter EventId se milata tha. Engine ki legacy rows EOD2 se aati hain
aur EOD2 apna adjustment khud kar chuka hota hai -- wo engine ke applied-ledger
me hai hi nahi. HIRECT ka 1:1 bonus isi wajah se dobara lag gaya aur R12 52% ki
jagah 204% dikhne laga.

Isliye ye test us ek sawal par tika hai: kya `AdjustedThrough` sach me rok raha
hai? Sirf output dekhne se ye kabhi nahi pakda jaata -- galat jawab bhi bilkul
saaf dikhta hai.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from vajra_regime.cloud.signal import adjusted_frame
from vajra_regime.cloud.state import StatePaths


def _store(tmp_path: Path, *, adjusted_through: object) -> StatePaths:
    """Ek naam, ek 1:1 bonus, aur do row: bonus se pehle aur baad me."""
    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "Date": [pd.Timestamp("2026-03-25").date(), pd.Timestamp("2026-03-30").date()],
        "ISIN": ["INE000A01001"] * 2,
        "Symbol": ["AAA"] * 2,
        "Open": [800.0, 400.0], "High": [800.0, 400.0],
        "Low": [800.0, 400.0], "Close": [800.0, 400.0],
        "Volume": [1000, 1000], "TurnoverINR": [800_000.0, 400_000.0],
        "Traded": [True, True], "IsFrozenBar": [False, False],
        "AdjustedThrough": [adjusted_through, adjusted_through],
        "EngineQuarantined": [False, False],
    }).to_parquet(paths.prices, index=False)

    pd.DataFrame({
        "EventId": ["e1"], "ISIN": ["INE000A01001"], "Symbol": ["AAA"],
        "ExDate": [pd.Timestamp("2026-03-27").date()],
        "PriceFactor": [0.5], "VolumeFactor": [2.0],
        "ActionType": ["BONUS"], "ParseStatus": ["OK"],
    }).to_parquet(paths.events, index=False)
    return paths


def test_bootstrap_rows_do_not_take_the_factor_twice(tmp_path: Path) -> None:
    # Ye row 2026-09-02 tak adjusted hai, aur bonus ka ex-date usse pehle hai --
    # matlab bonus in bhaavon me pehle se sama chuka hai.
    paths = _store(tmp_path, adjusted_through=pd.Timestamp("2026-09-02").date())
    frame = adjusted_frame(paths).set_index("Date")["Close"]

    assert float(frame.iloc[0]) == pytest.approx(800.0)
    assert float(frame.iloc[1]) == pytest.approx(400.0)


def test_as_traded_rows_do_take_the_factor(tmp_path: Path) -> None:
    """Live rows par kuch laga hi nahi hai, isliye bonus lagna CHAHIYE.

    Bina iske series bonus ki tareekh par aadhi ho jaati aur R12 ek 50% ka
    jhootha girna dikhata.
    """
    paths = _store(tmp_path, adjusted_through=None)
    frame = adjusted_frame(paths).set_index("Date")["Close"]

    # Ex-date se pehle wali row aadhi hoti hai, baad wali waisi hi rehti hai --
    # dono ab ek hi paimane par hain.
    assert float(frame.iloc[0]) == pytest.approx(400.0)
    assert float(frame.iloc[1]) == pytest.approx(400.0)


def test_the_two_cases_do_not_agree_by_accident(tmp_path: Path) -> None:
    """Dono raaste alag jawab dete hain -- warna upar ke test kuch sabit nahi karte.

    Agar `AdjustedThrough` ka koi asar hi na hota to dono case ek jaisa nikalte
    aur upar ke dono test bina kisi wajah ke pass hote rehte.
    """
    already = adjusted_frame(
        _store(tmp_path / "a", adjusted_through=pd.Timestamp("2026-09-02").date())
    )["Close"].tolist()
    raw = adjusted_frame(_store(tmp_path / "b", adjusted_through=None))["Close"].tolist()
    assert already != raw
