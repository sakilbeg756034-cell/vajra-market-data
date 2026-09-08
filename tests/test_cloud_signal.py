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


def test_engine_quarantine_saal_bhar_nahi_chalta(tmp_path: Path) -> None:
    """Engine ka quarantine US DIN ka hai -- saal bhar ka nahi.

    8 September 2026 ko pakdi gayi. `quarantine()` ke aakhir me 252-session ka
    blackout lagta hai. Wo blackout un jhatkon ke liye SAHI hai jinhe koi
    corporate action nahi samjhata -- aisa break poore saal R12 ko zeher kar
    deta hai. Par wo `EngineQuarantined` par bhi lag raha tha, aur wo GALAT
    tha.

    `EngineQuarantined` ka matlab hai "engine ne is din ko review me rakha".
    Naapa gaya (VAJRA_DATA, 2025-09 se): aise 45 alag reason the aur unme se
    EK BHI unexplained break nahi tha -- sab "REVIEW_NEEDED: Rights ...",
    "Demerger", "BONUS_GAIR_EQUITY" -- aur har ek theek 3 DIN ka.

    Nateeja: aaj 57 naam bandh the, jinme se 15 laptop ke universe me the --
    ADANIENT, HINDUNILVR, VEDL, aur RATNAVEER jo laptop par RANK 50 par tha,
    yaani trade ke dayre me. HINDUNILVR aakhri baar 273 DIN pehle flag hua tha.

    Sudhaar ke baad reconcile me rank ka farq 3 se 1 par aa gaya.
    """
    from vajra_regime.cloud.signal import matrices, quarantine, universe_metrics

    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    din = pd.bdate_range("2026-01-01", periods=40)
    rows = []
    for i, d in enumerate(din):
        rows.append({
            "Date": d.date(), "ISIN": "INE000A01001", "Symbol": "AAA", "Series": "EQ",
            "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0 + i,
            "Volume": 10, "TurnoverINR": 1000.0, "Traded": True, "IsFrozenBar": False,
            "AdjustedThrough": din[-1].date(),
            # engine ne sirf 3 din review me rakha tha, phir suljha diya
            "EngineQuarantined": i in (5, 6, 7),
        })
    pd.DataFrame(rows).to_parquet(paths.prices, index=False)
    pd.DataFrame({
        "EventId": [], "ISIN": [], "Symbol": [], "ExDate": [],
        "PriceFactor": [], "VolumeFactor": [], "ActionType": [], "ParseStatus": [],
    }).to_parquet(paths.events, index=False)

    frame = universe_metrics(paths)
    m = matrices(frame)
    barred = quarantine(paths, frame, m["Close"], m["Traded"])

    flagged_din = [m["Close"].index[i] for i in (5, 6, 7)]
    assert barred.loc[flagged_din].to_numpy().all(), "un teen dino par to bandh hona hi chahiye"
    baad = m["Close"].index[8:]
    assert not barred.loc[baad].to_numpy().any(), (
        "engine ne suljha diya tha; uske BAAD naam bandh nahi rehna chahiye. "
        "252-session ka blackout sirf un niyamo par lagta hai jo cloud khud pakadta hai."
    )


def test_rank_tie_par_top_me_ek_extra_naam_nahi_aata(tmp_path: Path, monkeypatch) -> None:
    """RANK tie hone par bhi `top` me theek N naam aane chahiye.

    8 September 2026 ke audit me pakdi gayi. `rank_table` pehle ROUND HO CHUKE
    SCORE par `.rank(ascending=False)` lagata tha, jiska default method
    `average` hai, aur phir rank ko bhi round karta tha. Do naam ka rounded
    SCORE barabar hote hi dono ko 2.5 jaisa aadha rank milta aur `.round()`
    (banker's rounding) dono ko 2 bana deta.

    Naapa gaya (7-Sep-2026 ki asli live file): 660 ranked naam par sirf 500
    alag rank -- yaani 160 duplicate. Us din sabse upar wala duplicate rank
    178 par tha, isliye kisi ko dikha nahi.

    Par `top` ki shart `RANK <= N_HOLDINGS` hai. Rank 20 par tie hote hi
    `top` me 21 naam aa jaate aur weight 21 me bant jaata -- yaani live 21
    naam rakhta jabki backtest 20. `fastbt.py` (jisne locked number banaye)
    `np.argsort(-s, kind="stable")` se hamesha THEEK N leta hai.

    Yahan B aur C bilkul ek jaisi series hain, isliye unka SCORE bit-par-bit
    barabar hai -- sabse sakht tie. N_HOLDINGS 2 par rakha gaya hai taaki
    tie theek hadd par pade.
    """
    from vajra_regime.cloud import signal as sig

    monkeypatch.setattr(sig, "N_HOLDINGS", 2)

    paths = StatePaths(tmp_path)
    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    din = pd.bdate_range("2025-01-01", periods=280)

    def bhaav(name: str, i: int) -> float:
        if name == "AAA":                     # seedhi chadhai -> vol kam, score sabse upar
            return 100.0 * (1.002 ** i) * (1.001 if i % 2 else 0.999)
        # BBB aur CCC bilkul ek jaise -- jhatkedaar, kam badhat
        return 100.0 * (1.0005 ** i) * (1.02 if i % 2 else 0.98)

    rows = []
    for isin, name in (("INE000A01001", "AAA"), ("INE000A01019", "BBB"),
                       ("INE000A01027", "CCC")):
        for i, d in enumerate(din):
            px = bhaav(name, i)
            rows.append({
                "Date": d.date(), "ISIN": isin, "Symbol": name, "Series": "EQ",
                "Open": px, "High": px, "Low": px, "Close": px,
                # ADTV `Close x Volume` se banta hai, TurnoverINR se nahi.
                # 100 x 10 lakh = 10 Cr -- 0.25 Cr ke farsh se aaram se upar.
                "Volume": 1_000_000, "TurnoverINR": 50_000_000.0,
                "Traded": True, "IsFrozenBar": False,
                "AdjustedThrough": din[-1].date(), "EngineQuarantined": False,
            })
    pd.DataFrame(rows).to_parquet(paths.prices, index=False)
    pd.DataFrame({
        "EventId": [], "ISIN": [], "Symbol": [], "ExDate": [],
        "PriceFactor": [], "VolumeFactor": [], "ActionType": [], "ParseStatus": [],
    }).to_parquet(paths.events, index=False)

    df = sig.rank_table(paths)

    ranked = df[df["RANK"].notna()]
    score = dict(zip(ranked["SYMBOL"], ranked["SCORE"], strict=False))
    assert score["BBB"] == score["CCC"], "test ka apna aadhaar: dono ka SCORE barabar ho"

    assert not ranked["RANK"].duplicated().any(), (
        "tie par bhi har naam ka apna rank hona chahiye -- warna round hokar "
        "do naam ek hi rank par aa jaate hain"
    )
    top = ranked[ranked["RANK"] <= 2]
    assert len(top) == 2, (
        f"top-2 me {len(top)} naam aa gaye. Tie par ek EXTRA naam khareed liya "
        f"jaata -- aur weight bhi usi me bant jaata."
    )
    assert list(ranked.sort_values("RANK")["SYMBOL"])[0] == "AAA"
    # weight sirf top-2 me, aur poora 100%
    w = df["WEIGHT_PCT"].dropna()
    assert len(w) == 2
    assert float(w.sum()) == pytest.approx(100.0, abs=0.05)
