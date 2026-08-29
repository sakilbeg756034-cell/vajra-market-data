from __future__ import annotations

import zipfile
from datetime import date

from vajra_regime.nifty500_migration.raw_ohlcv import _identity_history, _resolve_isin, parse_official_bhavcopy


def test_parse_legacy_and_udiff_bhavcopy(tmp_path) -> None:
    legacy = tmp_path / "legacy.zip"
    with zipfile.ZipFile(legacy, "w") as bundle:
        bundle.writestr(
            "cm01JAN2009bhav.csv",
            "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,\n"
            "ABC,EQ,10,12,9,11,11,10,100,1100,1-JAN-2009,\n",
        )
    udiff = tmp_path / "udiff.zip"
    with zipfile.ZipFile(udiff, "w") as bundle:
        bundle.writestr(
            "BhavCopy_NSE_CM_0_0_0_20250102_F_0000.csv",
            "TradDt,ISIN,TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd\n"
            "2025-01-02,INE000A01001,XYZ,EQ,20,22,19,21,20,200,4200,15\n",
        )
    assert list(parse_official_bhavcopy(legacy, date(2009, 1, 1)))[0]["Symbol"] == "ABC"
    assert list(parse_official_bhavcopy(udiff, date(2025, 1, 2)))[0]["ISIN"] == "INE000A01001"


def test_identity_resolution_is_effective_dated() -> None:
    histories, fallback = _identity_history(
        {
            "isin2hist": {
                "INE000A01001": [
                    {"symbol": "OLD", "from_date": "2010-01-01", "to_date": "2014-12-31"},
                    {"symbol": "NEW", "from_date": "2015-01-01", "to_date": "2026-12-31"},
                ]
            },
            "sym2isin": {"OLD": "INE000A01001", "NEW": "INE000A01001"},
        }
    )
    assert _resolve_isin("OLD", date(2014, 6, 1), histories=histories, fallback=fallback) == "INE000A01001"
    assert _resolve_isin("NEW", date(2016, 6, 1), histories=histories, fallback=fallback) == "INE000A01001"
