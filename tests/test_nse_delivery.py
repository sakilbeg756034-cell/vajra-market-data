"""Delivery data ka wo hissa jo chup-chaap galat ho sakta hai.

Sabse bada khatra yahan "download fail hua" nahi hai -- wo dikh jaata hai. Khatra
ye hai ki file AA jaye par adhoori ho, ya '-' ko 0 samajh liya jaye. Dono halat
me column bhar jaata hai, saaf dikhta hai, aur galat hota hai.

Isi liye ye test uparwale rasta (network) nahi dekhte -- wo parsing dekhte hain
jahan chuppi paida hoti hai.
"""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from vajra_regime import nse_delivery


def _csv(rows: str, header: str | None = None) -> bytes:
    head = header or (
        "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
        "LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, "
        "NO_OF_TRADES, DELIV_QTY, DELIV_PER"
    )
    return (head + "\n" + rows).encode()


def _rows(n: int, deliv: str = "1000") -> str:
    return "\n".join(
        f"SYM{i:04d}, EQ, 02-Sep-2026, 100, 100, 101, 99, 100, 100, 100.5, "
        f"5000, 5.0, 250, {deliv}, 20.00"
        for i in range(n)
    )


class _Response:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> bool:
        return False


def _serve(payload: bytes):
    return patch.object(nse_delivery.urllib.request, "urlopen",
                        lambda *a, **k: _Response(payload))


def test_a_dash_stays_unknown_and_never_becomes_zero() -> None:
    """NSE '-' likhta hai jahan delivery laagu nahi hoti.

    '-' ko 0 maan lena sabse mehngi galti hoti: 0 ka matlab "kuch delivery nahi
    hui" hai, jo ek asli aur bura signal hai. "pata nahi" ko "bura" me badal
    dena poore backtest ko chup-chaap jhutha kar deta.
    """
    with _serve(_csv(_rows(600, deliv="-"))):
        frame = nse_delivery.fetch_delivery(date(2026, 9, 2))

    assert frame is not None
    assert frame["DeliveryQuantity"].isna().all()
    assert not (frame["DeliveryQuantity"] == 0).any()


def test_a_partial_file_is_refused_rather_than_stored() -> None:
    """Aadha delivery data poore delivery data jaisa hi dikhta hai.

    Isliye kam rows wali file lene se inkaar karna hi ek matra bachav hai --
    warna us din ke aadhe naam khaali reh jaate aur kisi ko pata nahi chalta.
    """
    with _serve(_csv(_rows(10))), pytest.raises(ValueError, match="minimum"):
        nse_delivery.fetch_delivery(date(2026, 9, 2))


def test_surveillance_series_kept_but_govt_securities_dropped() -> None:
    """Delivery ka series-set price master ke BARABAR hona chahiye.

    Price master ab EQ ke saath BE/BZ bhi rakhta hai, taaki surveillance ke daur
    me stock apni hi price history se gayab na ho. Agar delivery sirf EQ laati
    rahe to wo rows to aa jaatin par unki delivery khaali reh jaati -- yaani
    wahi chuppi wapas, bas doosre column me. (Aisa hua bhi tha: 2026 ki delivery
    coverage 100% se girkar 90.9% ho gayi thi, theek utni jitni BE/BZ rows thin.)

    Government securities phir bhi bahar hi rehni chahiye.
    """
    def _block(prefix: str, series: str, count: int) -> str:
        return "\n".join(
            f"{prefix}{i:04d}, {series}, 02-Sep-2026, 100, 100, 101, 99, 100, "
            "100, 100.5, 5000, 5.0, 250, 1000, 20.00" for i in range(count)
        )

    mixed = "\n".join([
        _rows(600),
        _block("GS", "GS", 50),
        _block("BEX", "BE", 30),
        _block("BZX", "BZ", 10),
    ])
    with _serve(_csv(mixed)):
        frame = nse_delivery.fetch_delivery(date(2026, 9, 2))

    assert set(frame["Series"]) == {"EQ", "BE", "BZ"}
    assert not frame["Symbol"].str.startswith("GS").any()
    assert int((frame["Series"] == "BE").sum()) == 30
    assert int((frame["Series"] == "BZ").sum()) == 10


def test_rows_from_another_date_are_dropped() -> None:
    """File me kisi aur din ki row aa jaye to wo us din ki nahi hai.

    Ye ho sakta hai agar NSE galat file de de. Bina is jaanch ke wo rows chup-chaap
    galat tareekh par chip jaatin.
    """
    wrong = _rows(600) + "\n" + "\n".join(
        f"OLD{i:04d}, EQ, 01-Sep-2026, 100, 100, 101, 99, 100, 100, 100.5, "
        "5000, 5.0, 250, 1000, 20.00" for i in range(20)
    )
    with _serve(_csv(wrong)):
        frame = nse_delivery.fetch_delivery(date(2026, 9, 2))

    assert (pd.Series(frame["Date"]) == date(2026, 9, 2)).all()
    assert not frame["Symbol"].str.startswith("OLD").any()


def test_nse_serving_the_previous_day_is_treated_as_a_holiday() -> None:
    """Ye asli me hua, 2026-01-15 par.

    15 January chhutti thi. NSE ne us tareekh ka file maangne par HTTP 200
    lautaya aur andar POORA 14-January ka data tha. Bina is jaanch ke 14 ka
    delivery data 15 par chip jaata -- ek din shift, koi error nahi, aur mahinon
    baad kisi ko samajh na aata ki number kyun nahi mil rahe.

    File apni tareekh khud batati hai; wo maangi hui se alag ho to us din ka data
    hai hi nahi -- chhutti, galti nahi.
    """
    previous_day = "\n".join(
        f"SYM{i:04d}, EQ, 14-Jan-2026, 100, 100, 101, 99, 100, 100, 100.5, "
        "5000, 5.0, 250, 1000, 20.00"
        for i in range(2000)
    )
    with _serve(_csv(previous_day)):
        assert nse_delivery.fetch_delivery(date(2026, 1, 15)) is None


def test_a_missing_column_is_an_error_not_a_silent_blank() -> None:
    """NSE format badal de to run rukna chahiye, khaali column nahi bharna."""
    header = "SYMBOL, SERIES, DATE1, TTL_TRD_QNTY, NO_OF_TRADES"
    body = "\n".join(f"SYM{i}, EQ, 02-Sep-2026, 5000, 250" for i in range(600))
    with _serve(_csv(body, header=header)), pytest.raises(ValueError, match="missing columns"):
        nse_delivery.fetch_delivery(date(2026, 9, 2))


def test_a_holiday_returns_none_rather_than_raising() -> None:
    """404 ka matlab chhutti hai, galti nahi -- pipeline ko chalte rehna chahiye."""
    def _raise(*_a, **_k):
        raise nse_delivery.urllib.error.HTTPError(
            "url", 404, "Not Found", hdrs=None, fp=io.BytesIO(b"")
        )

    with patch.object(nse_delivery.urllib.request, "urlopen", _raise):
        assert nse_delivery.fetch_delivery(date(2026, 8, 15)) is None


def test_other_http_errors_are_raised() -> None:
    """403 ko chhutti maan lena poore din ka data chup-chaap gira deta."""
    def _raise(*_a, **_k):
        raise nse_delivery.urllib.error.HTTPError(
            "url", 403, "Forbidden", hdrs=None, fp=io.BytesIO(b"")
        )

    with patch.object(nse_delivery.urllib.request, "urlopen", _raise), \
            pytest.raises(RuntimeError, match="403"):
        nse_delivery.fetch_delivery(date(2026, 9, 2))
