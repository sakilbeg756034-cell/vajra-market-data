from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import duckdb

from vajra_regime.nse_live import (
    RAW_TABLE,
    append_raw_day_to_master,
    normalize_udiff_bhavcopy,
    official_bhavcopy_url,
)


def _write_sample_zip(path: Path) -> None:
    # CLASHBE jaan-boojh kar CLASHEQ se PEHLE aur zyada volume ke saath rakha hai:
    # agar chunav sirf volume ya file ki tarteeb par hota to BE jeet jaata.
    csv_text = """TradDt,TckrSymb,ISIN,SctySrs,OpnPric,HghPric,LwPric,ClsPric,TtlTradgVol,TtlTrfVal
2026-01-02,AAA,INE000A01001,EQ,100,110,95,105,1000,105000
2026-01-02,AAAOLD,INE000A01001,EQ,100,110,95,105,500,52500
2026-01-02,BBB,INE000B01001,BE,50,55,49,54,2000,108000
2026-01-02,BAD,INE000C01001,EQ,10,9,8,9,100,900
2026-01-02,ZZZ,INE000D01001,BZ,20,22,19,21,300,6300
2026-01-02,SMEONE,INE000E01001,SM,30,33,29,32,400,12800
2026-01-02,SMETWO,INE000F01001,ST,40,44,39,43,500,21500
2026-01-02,GSEC,IN0020230011,GS,99,99,99,99,600,59400
2026-01-02,CLASHBE,INE000G01001,BE,70,77,69,76,9999,759924
2026-01-02,CLASHEQ,INE000G01001,EQ,70,77,69,76,100,7600
"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BhavCopy_NSE_CM_0_0_0_20260102_F_0000.csv", csv_text)
    path.write_bytes(buffer.getvalue())


def test_official_bhavcopy_url() -> None:
    assert official_bhavcopy_url(date(2026, 1, 2)).endswith(
        "BhavCopy_NSE_CM_0_0_0_20260102_F_0000.csv.zip"
    )


def test_normalize_udiff_filters_and_deduplicates(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)

    frame, report = normalize_udiff_bhavcopy(
        zip_path,
        date(2026, 1, 2),
        minimum_kept_rows=1,
    )

    assert report["raw_rows"] == 10
    assert report["kept_rows"] == 4
    assert frame["Symbol"].tolist() == ["AAA", "BBB", "ZZZ", "CLASHEQ"]
    assert not frame.duplicated(["Date", "ISIN"]).any()


def test_raw_master_append_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "master.duckdb"
    with duckdb.connect(str(database)):
        pass

    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)
    frame, report = normalize_udiff_bhavcopy(
        zip_path,
        date(2026, 1, 2),
        minimum_kept_rows=1,
    )
    manifest = {
        "Date": "2026-01-02",
        "Status": "VALIDATED",
        "SourceURL": official_bhavcopy_url(date(2026, 1, 2)),
        "ZipPath": str(zip_path),
        "ParquetPath": str(tmp_path / "day.parquet"),
        "SourceSha256": frame["SourceSha256"].iloc[0],
        "RawRows": report["raw_rows"],
        "KeptRows": report["kept_rows"],
        "Message": "test",
        "RecordedAtUTC": "2026-01-03T00:00:00+00:00",
    }

    inserted = len(frame)
    assert append_raw_day_to_master(database, frame, manifest) == inserted
    assert append_raw_day_to_master(database, frame, manifest) == 0

    with duckdb.connect(str(database), read_only=True) as connection:
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE}"
        ).fetchone()[0]
        date_count = connection.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE} WHERE Date = '2026-01-02'"
        ).fetchone()[0]
    assert row_count == inserted
    assert date_count == inserted


def test_surveillance_series_survive_but_sme_and_govt_do_not(tmp_path: Path) -> None:
    """BE/BZ ka data chahiye; SME aur government securities ka nahi.

    NSE jab stock ko surveillance me daalta hai to series EQ se BE ho jaati hai.
    Stock roz trade hota rehta hai. Pehle intake sirf EQ leta tha, isliye aisa
    stock us poore daur ke liye data se GAYAB ho jaata tha -- bina kisi error ke.

    Nateeja: wapas EQ me aane wale din ka "1-din ka return" asal me poore gap ka
    nikalta tha. SUZLON 2024-01-15 par +58.6% dikha, jabki wo 98 din ka move tha.

    SME (SM/ST) aur govt securities (GS) phir bhi bahar hi rehne chahiye -- wo
    is strategy ka universe hai hi nahi, aur unhe lene se 750 ki ginti hi badal
    jaati.
    """
    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)

    frame, _ = normalize_udiff_bhavcopy(zip_path, date(2026, 1, 2), minimum_kept_rows=1)

    assert set(frame["Series"]) == {"EQ", "BE", "BZ"}
    assert "BBB" in set(frame["Symbol"])          # BE bacha
    assert "ZZZ" in set(frame["Symbol"])          # BZ bacha
    assert not {"SMEONE", "SMETWO", "GSEC"} & set(frame["Symbol"])


def test_eq_beats_be_for_one_isin_even_when_be_has_more_volume(tmp_path: Path) -> None:
    """Ek security ek din me do series me dikhe to EQ hi asli tradeable row hai.

    Purana dedup sabse zyada volume wali row rakhta tha. Ab jab BE rows bhi aati
    hain, wo niyam ulta pad sakta tha: BE me volume zyada ho to BE row jeet
    jaati, aur ek tradeable naam surveillance row ke roop me darj ho jaata.

    Isliye series ka darja volume se UPAR rakha gaya hai. Sample me CLASHBE
    jaan-boojh kar pehle aur 100 guna zyada volume ke saath hai -- phir bhi
    CLASHEQ jeetna chahiye.
    """
    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)

    frame, _ = normalize_udiff_bhavcopy(zip_path, date(2026, 1, 2), minimum_kept_rows=1)

    clash = frame.loc[frame["ISIN"] == "INE000G01001"]
    assert len(clash) == 1
    assert clash["Symbol"].iloc[0] == "CLASHEQ"
    assert clash["Series"].iloc[0] == "EQ"
