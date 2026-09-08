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


def test_a_state_file_written_before_series_existed_still_loads(tmp_path: Path) -> None:
    """Cloud ki state file GitHub par pehle se maujood hai -- bina Series ke.

    Jab Series column joda gaya to sabse bada khatra "galat jawab" nahi tha, wo
    "koi jawab nahi" tha: agar padhne wala SQL seedha `p.Series` maangta, to
    purani file par roz ka cloud run phat jaata aur sheet update hona hi band ho
    jaati -- laptop band hone par mujhe pata bhi na chalta.

    Purani file me har row EQ hi hai, kyunki tab intake BE/BZ leta hi nahi tha.
    Isliye wahan 'EQ' maan lena andaza nahi, sach hai.
    """
    paths = _store(tmp_path, adjusted_through=None)
    stored = pd.read_parquet(paths.prices)
    assert "Series" not in stored.columns, "fixture jaan-boojh kar purani shakl me hai"

    frame = adjusted_frame(paths)

    assert "Series" in frame.columns
    assert set(frame["Series"]) == {"EQ"}


def test_new_sessions_append_onto_a_pre_series_state_file(tmp_path: Path) -> None:
    """Purani state par naye din judne chahiye, phategi nahi.

    `append_sessions` purani file aur nayi rows ko UNION karta hai. Column ki
    ginti alag ho to wo wahin ruk jaata. Yahi rasta roz chalta hai, isliye ye
    tootna sabse mehnga hota.
    """
    from vajra_regime.cloud.state import append_sessions

    paths = _store(tmp_path, adjusted_through=None)

    incoming = pd.DataFrame({
        "Date": [pd.Timestamp("2026-03-31").date()],
        "ISIN": ["INE000B01001"], "Symbol": ["BBB"], "Series": ["BE"],
        "Open": [50.0], "High": [50.0], "Low": [50.0], "Close": [50.0],
        "Volume": [10], "TurnoverINR": [500.0],
        "Traded": [True], "IsFrozenBar": [True],
        "AdjustedThrough": [pd.NaT], "EngineQuarantined": [False],
    })

    written = append_sessions(paths, incoming)
    assert written == 1

    merged = pd.read_parquet(paths.prices)
    assert set(merged["Series"]) == {"EQ", "BE"}
    # purani rows ko EQ mila, nayi row apni asli series ke saath aayi
    assert merged.loc[merged["Symbol"] == "AAA", "Series"].eq("EQ").all()
    assert merged.loc[merged["Symbol"] == "BBB", "Series"].eq("BE").all()


def test_series_survives_the_universe_metrics_query(tmp_path: Path) -> None:
    """Series `universe_metrics` se hokar signal tak pahunchni CHAHIYE.

    8 September 2026 ko pakdi gayi bug. `adjusted_frame` Series nikalta tha,
    par `universe_metrics` ka SELECT use GIRA deta tha -- aur `rank_table` me
    ek "purani state file" wala rasta tha jo column na milne par chup-chaap
    sab kuch EQ maan leta tha.

    Nateeja: BE/BZ ka filter LIVE ME MARA HUA THA. Naapa gaya (2026-09-07 ka
    published signal): 734 me se 734 naam "EQ" likhe the, jabki usi din ke
    bhavcopy me 248 BE aur 27 BZ the. HFCL 3 September ko BE me jaa chuka tha
    aur signal me RANK 3, WEIGHT 6.20% par khada tha -- jabki backtest ka
    universe (`IsEQ`) aisa naam kabhi nahi leta.

    Ye test theek us jagah khada hai jahan column gira tha.
    """
    from vajra_regime.cloud.signal import universe_metrics

    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "Date": [pd.Timestamp("2026-03-25").date()] * 2,
        "ISIN": ["INE000A01001", "INE000B01001"],
        "Symbol": ["AAA", "BBB"],
        "Series": ["EQ", "BE"],
        "Open": [800.0, 50.0], "High": [800.0, 50.0],
        "Low": [800.0, 50.0], "Close": [800.0, 50.0],
        "Volume": [1000, 10], "TurnoverINR": [800_000.0, 500.0],
        "Traded": [True, True], "IsFrozenBar": [False, False],
        "AdjustedThrough": [None, None],
        "EngineQuarantined": [False, False],
    }).to_parquet(paths.prices, index=False)
    pd.DataFrame({
        "EventId": [], "ISIN": [], "Symbol": [], "ExDate": [],
        "PriceFactor": [], "VolumeFactor": [], "ActionType": [], "ParseStatus": [],
    }).to_parquet(paths.events, index=False)

    metrics = universe_metrics(paths)
    assert "Series" in metrics.columns, (
        "universe_metrics ne Series gira di -- BE/BZ ka filter mar jayega"
    )
    by_symbol = metrics.set_index("Symbol")["Series"]
    assert by_symbol["AAA"] == "EQ"
    assert by_symbol["BBB"] == "BE"


def test_isin_badalne_par_series_nahi_tootti(tmp_path: Path) -> None:
    """NSE ISIN badle to bhi ek hi company ki series EK rehni chahiye.

    8 September 2026 ko pakdi gayi. NSE face value badalne par naya ISIN de
    deta hai. Cloud ke liye wo BILKUL naya security ban jaata tha, isliye ek
    hi company ki price series do tukdo me pad jaati thi.

    Naapa gaya (500-session store me): 31 symbol aise the. Do tarah ka nuksaan:
      * naya tukda 252-session wali shart par fail -> naam ~1 saal ke liye
        sheet se GAYAB. TDPOWERSYS ka ISIN 24-Aug ko badla tha; laptop ke
        backtest me wo rank 11 par tha -- yaani kharidne wala naam -- aur
        cloud ki file me tha hi nahi.
      * jo bacha rehta uska R12 aadhi series par banta. V2RETAIL ka SCORE
        0.403 se alag tha, jo reconcile ka sabse bada farq tha.

    Sudhaar ke baad SCORE ka farq 725 me se 725 naam par THEEK 0.0 ho gaya.

    Ye test wahi ek sawaal poochta hai: do ISIN, ek company -- kya series
    judti hai, aur kya bahar NSE ka AAJ ka ISIN dikhta hai?
    """
    from vajra_regime.cloud.signal import adjusted_frame

    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "Date": [pd.Timestamp("2026-03-25").date(), pd.Timestamp("2026-03-26").date()],
        "ISIN": ["INE000A01001", "INE000A01019"],   # NSE ne beech me ISIN badla
        "Symbol": ["AAA", "AAA"],
        "Series": ["EQ", "EQ"],
        "Open": [100.0, 101.0], "High": [100.0, 101.0],
        "Low": [100.0, 101.0], "Close": [100.0, 101.0],
        "Volume": [10, 10], "TurnoverINR": [1000.0, 1010.0],
        "Traded": [True, True], "IsFrozenBar": [False, False],
        "AdjustedThrough": [None, None],
        "EngineQuarantined": [False, False],
    }).to_parquet(paths.prices, index=False)
    pd.DataFrame({
        "EventId": [], "ISIN": [], "Symbol": [], "ExDate": [],
        "PriceFactor": [], "VolumeFactor": [], "ActionType": [], "ParseStatus": [],
    }).to_parquet(paths.events, index=False)

    # naksha ke BINA: do alag security (purana behaviour, jaan-boojh kar bacha
    # hua taaki purani state file bina lineage ke bhi chalti rahe)
    bina = adjusted_frame(paths)
    assert bina["ISIN"].nunique() == 2

    # naksha ke SAATH: ek hi company
    pd.DataFrame({
        "SourceISIN": ["INE000A01001", "INE000A01019"],
        "CanonicalISIN": ["INE000A01001", "INE000A01001"],
    }).to_parquet(paths.isin_lineage, index=False)

    saath = adjusted_frame(paths)
    assert saath["ISIN"].nunique() == 1, "ISIN badalne par series abhi bhi toot rahi hai"
    assert set(saath["ISIN"]) == {"INE000A01001"}
    # store me NSE ka apna ISIN jyon ka tyon rehta hai -- sheet me wahi dikhna hai
    assert list(saath.sort_values("Date")["SourceISIN"]) == ["INE000A01001", "INE000A01019"]
    # ek din par ek hi row -- warna pivot phat jaata hai
    assert not saath.duplicated(subset=["Date", "ISIN"]).any()
