"""Naam aur sector kahan se aate hain -- aur kyun do alag jagah se.

Pehle dono ek hi list se aate the: niftyindices ki Total Market (750). Wo list
NSE ke apne 750 se banti hai, jabki hamari VAJRA 750 turnover se. Nateeja: 731
me se 117 naam us list me the hi nahi, aur sheet ke top-12 me se PAANCH cell
khaali dikhte the.

Khaali cell "thoda kam data" nahi tha -- wo bharose ka sawaal ban gaya, kyunki
dekhne wale ko lagta hai sheet adhoori hai.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from vajra_regime.cloud import daily
from vajra_regime.cloud.state import StatePaths


def _equity_list() -> pd.DataFrame:
    return pd.DataFrame({
        "SYMBOL": ["AAA", "BBB", "CCC"],
        "NAME OF COMPANY": ["Aaa Ltd", "Bbb Ltd", "Ccc Ltd"],
        "ISIN NUMBER": ["INE000A01001", "INE000B01001", "INE000C01001"],
    })


def _index_list() -> pd.DataFrame:
    # Sirf AAA is index me hai -- yahi asli haal hai: index list chhoti hoti hai.
    return pd.DataFrame({
        "Company Name": ["Aaa Limited"],
        "Industry": ["Capital Goods"],
        "ISIN Code": ["INE000A01001"],
    })


def _serve(equity, index):
    def _fake(url: str) -> pd.DataFrame:
        return equity() if "EQUITY_L" in url else index()
    return patch.object(daily, "_csv_from", _fake)


def test_every_listed_name_gets_a_company_name(tmp_path: Path) -> None:
    """Naam poori equity list se aata hai, index list se nahi.

    Index list me sirf AAA hai. Phir bhi teenon ko naam milna chahiye -- warna
    wahi 117 khaali cell wapas aa jaate hain.
    """
    paths = StatePaths(tmp_path)
    with _serve(_equity_list, _index_list):
        rows = daily.refresh_reference(paths)

    ref = pd.read_parquet(paths.reference).set_index("ISIN")
    assert rows == 3
    assert ref.loc["INE000A01001", "NAME"] == "Aaa Ltd"
    assert ref.loc["INE000B01001", "NAME"] == "Bbb Ltd"
    assert ref.loc["INE000C01001", "NAME"] == "Ccc Ltd"


def test_sector_only_where_the_index_list_actually_says_so(tmp_path: Path) -> None:
    """Sector sirf wahan bharta hai jahan wo sach me maloom hai.

    Sector concentration ISI column se dekhi jaati hai -- 12 me se 6 naam ek
    sector me hon to wo chhupa hua joker hai. Aisi jagah andaaza bhar dena
    khaali chhodne se bura hai, kyunki khaali cell sawaal poochhta hai aur
    galat sector chup rehta hai.
    """
    paths = StatePaths(tmp_path)
    with _serve(_equity_list, _index_list):
        daily.refresh_reference(paths)

    ref = pd.read_parquet(paths.reference).set_index("ISIN")
    assert ref.loc["INE000A01001", "SECTOR"] == "Capital Goods"
    assert pd.isna(ref.loc["INE000B01001", "SECTOR"])
    assert pd.isna(ref.loc["INE000C01001", "SECTOR"])


def test_names_still_arrive_when_the_index_list_is_down(tmp_path: Path) -> None:
    """Do source ka poora matlab yahi hai: ek gire to doosra chalta rahe.

    Pehle dono ek hi request par tike the, isliye wo ek request fail hone par
    naam AUR sector dono gayab ho jaate the.
    """
    def _boom() -> pd.DataFrame:
        raise RuntimeError("niftyindices down")

    paths = StatePaths(tmp_path)
    with _serve(_equity_list, _boom):
        rows = daily.refresh_reference(paths)

    ref = pd.read_parquet(paths.reference)
    assert rows == 3
    assert ref["NAME"].notna().all()
    assert ref["SECTOR"].isna().all()


def test_nothing_is_written_when_both_sources_fail(tmp_path: Path) -> None:
    """Dono gir jayein to purani list rehne deni chahiye, khaali nahi karni.

    Naam aur sector sundarta hain, faisla nahi -- inke liye poora run girana
    bhi galat hai aur maujooda list mita dena bhi.
    """
    def _boom() -> pd.DataFrame:
        raise RuntimeError("down")

    paths = StatePaths(tmp_path)
    with _serve(_boom, _boom):
        rows = daily.refresh_reference(paths)

    assert rows == 0
    assert not paths.reference.exists()
