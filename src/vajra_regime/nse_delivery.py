"""NSE ka delivery data -- `sec_bhavdata_full`, jo UDiFF bhavcopy me hota hi nahi.

KYA MASLA THA
-------------
Engine ka live feed UDiFF bhavcopy hai, aur usme DELIVERY, TRADE-COUNT jaisi
cheezein hoti hi nahi. Isliye `rolling_master.py` live rows par teenon column
`NULL` rakhta tha:

    NULL::DOUBLE AS TotalTrades,
    NULL::DOUBLE AS QuantityPerTrade,
    NULL::DOUBLE AS DeliveryQuantity,

Nateeja: 2020-2025 me delivery data 99-100% rows par tha, aur 2026 me ZERO.
Koi error nahi aayi, koi warning nahi -- bas column khaali ho gaya. Wahi chuppi
jo is project me baar-baar mehngi padi hai.

KYUN ZAROORI HAI
----------------
Delivery batati hai ki din ka kitna volume ASLI kharidari thi aur kitna intraday
shor. Do stock ek jaisa volume dikha sakte hain, par ek me 80% delivery ho aur
doosre me 8% -- wo do bilkul alag cheezein hain. Bina is column ke wo fark
dikhta hi nahi.

SYMBOL PAR JOIN -- aur kyun ye yahan surakshit hai
--------------------------------------------------
Is project ka niyam hai: "kabhi Symbol par join mat karo, hamesha ISIN par" --
kyunki 146 symbol ek se zyada ISIN par lage hain aur company naam badalne par
symbol badal jaata hai.

`sec_bhavdata_full` me ISIN hai hi nahi, sirf SYMBOL. Par ye join EK HI DIN ke
andar hota hai, aur wahan symbol unique hai: poore 17 saal ke data me ek bhi
aisa (Date, Symbol) nahi mila jispar do ISIN hon (0 case, 2026-09-03 ko jaancha).
Symbol saalon me badalte hain, ek din ke andar nahi.

Isliye join `(Date, Symbol)` par hota hai aur sirf live rows ke liye -- legacy
rows (2009-2025) me delivery pehle se maujood hai aur wo chhui nahi jaati.
"""
from __future__ import annotations

import io
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from vajra_regime.config import AppConfig
from vajra_regime.nse_live import TRADEABLE_SERIES, USER_AGENT

DELIVERY_URL = (
    "https://nsearchives.nseindia.com/products/content/"
    "sec_bhavdata_full_{ddmmyyyy}.csv"
)
DELIVERY_TABLE = "nse_delivery_daily"

# Ek din me itni se kam EQ rows aayein to file adhoori hai -- use lena galat
# hoga, kyunki aadha delivery data poore delivery data jaisa hi dikhta hai.
MINIMUM_ROWS = 500


def delivery_url(trading_date: date) -> str:
    return DELIVERY_URL.format(ddmmyyyy=trading_date.strftime("%d%m%Y"))


def _ensure_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DELIVERY_TABLE} (
            Date DATE,
            Symbol VARCHAR,
            Series VARCHAR,
            TotalTrades DOUBLE,
            DeliveryQuantity DOUBLE,
            DeliveryPercent DOUBLE,
            AveragePrice DOUBLE,
            TradedQuantity DOUBLE,
            SourceUrl VARCHAR,
            IngestedAtUTC TIMESTAMP
        )
        """
    )


def _number(series: pd.Series) -> pd.Series:
    """NSE '-' aur khaali ko NaN banao, comma hatao.

    NSE un rows me '-' likhta hai jahan delivery laagu nahi hoti. Use 0 maan
    lena galat hoga: 0 ka matlab "kuch delivery nahi hui" hai, aur '-' ka matlab
    "pata nahi" hai. Dono ek nahi hain.
    """
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip()
        .replace({"-": None, "": None}),
        errors="coerce",
    )


def fetch_delivery(trading_date: date) -> pd.DataFrame | None:
    """Ek din ki EQ delivery rows. Chhutti par None.

    404 = chhutti ya abhi publish nahi hua; wo galti nahi hai. Baaki har HTTP
    error upar uthta hai -- "file nahi mili" ko chup-chaap "delivery zero thi"
    maan lena wahi chuppi hai jise rokna hai.
    """
    url = delivery_url(trading_date)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,*/*",
            "Referer": "https://www.nseindia.com/",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise RuntimeError(f"NSE delivery download HTTP {exc.code}: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"NSE delivery download failed: {url}: {exc}") from exc

    frame = pd.read_csv(io.BytesIO(payload))
    frame.columns = [str(c).strip().upper() for c in frame.columns]

    required = {"SYMBOL", "SERIES", "DATE1", "TTL_TRD_QNTY", "NO_OF_TRADES",
                "DELIV_QTY", "DELIV_PER", "AVG_PRICE"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            f"sec_bhavdata_full for {trading_date} is missing columns: {missing}"
        )

    # Price master ke saath ek hi series-set. Warna BE/BZ rows to aati hain par
    # unki delivery khaali reh jaati -- aur wahi chuppi wapas aa jaati jise
    # rokne ke liye ye file likhi gayi thi.
    frame["SERIES"] = frame["SERIES"].astype(str).str.strip().str.upper()
    frame = frame.loc[frame["SERIES"].isin(TRADEABLE_SERIES)].copy()

    # NSE CHHUTTI KE DIN PICHHLE DIN KA FILE DE DETA HAI -- HTTP 200 ke saath.
    #
    # 15 January 2026 chhutti thi. `sec_bhavdata_full_15012026.csv` maangne par
    # NSE ne 200 lautaya aur andar 14-Jan-2026 ka poora data tha. Bina is jaanch
    # ke 14 tareekh ka delivery data 15 tareekh par chip jaata -- ek din shift,
    # koi error nahi, aur 6 mahine baad kisi ko samajh na aata ki number kyun
    # nahi mil rahe.
    #
    # Isliye file ki APNI tareekh dekhi jaati hai. Wo maangi hui tareekh se alag
    # ho to iska matlab hai "us din ka data hai hi nahi" -- yaani chhutti, galti
    # nahi. Aur jab tareekh sahi ho par rows kam hon, tabhi file adhoori hai.
    parsed = pd.to_datetime(frame["DATE1"].astype(str).str.strip(),
                            format="%d-%b-%Y", errors="coerce")
    matching = frame.loc[parsed.eq(pd.Timestamp(trading_date))].copy()
    if matching.empty:
        return None

    frame = matching
    if len(frame) < MINIMUM_ROWS:
        raise ValueError(
            f"Only {len(frame)} EQ delivery rows for {trading_date}; "
            f"minimum is {MINIMUM_ROWS}. Refusing a partial file."
        )

    out = pd.DataFrame({
        "Date": trading_date,
        "Symbol": frame["SYMBOL"].astype(str).str.strip().str.upper(),
        "Series": frame["SERIES"],
        "TotalTrades": _number(frame["NO_OF_TRADES"]),
        "DeliveryQuantity": _number(frame["DELIV_QTY"]),
        "DeliveryPercent": _number(frame["DELIV_PER"]),
        "AveragePrice": _number(frame["AVG_PRICE"]),
        "TradedQuantity": _number(frame["TTL_TRD_QNTY"]),
        "SourceUrl": url,
        "IngestedAtUTC": datetime.now(UTC),
    })
    return out.drop_duplicates(["Date", "Symbol"], keep="first").reset_index(drop=True)


def catch_up_delivery(
    config: AppConfig,
    *,
    start_date: date,
    end_date: date,
    database_path: str | Path | None = None,
    refetch_existing: bool = False,
) -> dict[str, object]:
    """`start_date` se `end_date` tak jo din missing hain wahi laao.

    Jo din pehle se table me hai use dobara download nahi kiya jaata -- backfill
    beech me ruk jaye to dobara chalane par wo wahin se uthata hai.

    `refetch_existing` un dino ko bhi dobara laata hai. Iski zaroorat tab padi
    jab series-set badla: purane din EQ-only aaye the, aur unme BE/BZ ki
    delivery baad me jodni thi. Insert (Date, Symbol) par anti-join karta hai,
    isliye dobara chalana surakshit hai -- purani rows dohrai nahi jaatin.
    """
    path = Path(database_path or config.environment.duckdb_path)
    fetched, holidays, existing = [], [], []

    with duckdb.connect(str(path)) as connection:
        _ensure_table(connection)
        have = {
            row[0] for row in connection.execute(
                f"SELECT DISTINCT Date FROM {DELIVERY_TABLE}"
            ).fetchall()
        }

        day = start_date
        while day <= end_date:
            if day.weekday() >= 5:            # NSE shanivaar-ravivaar band
                day += timedelta(days=1)
                continue
            if day in have and not refetch_existing:
                existing.append(day.isoformat())
                day += timedelta(days=1)
                continue

            frame = fetch_delivery(day)
            if frame is None:
                holidays.append(day.isoformat())
            else:
                connection.register("incoming_delivery", frame)
                connection.execute(
                    f"""
                    INSERT INTO {DELIVERY_TABLE}
                    SELECT i.* FROM incoming_delivery i
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {DELIVERY_TABLE} e
                        WHERE e.Date = i.Date AND e.Symbol = i.Symbol
                    )
                    """
                )
                connection.unregister("incoming_delivery")
                fetched.append(day.isoformat())
            day += timedelta(days=1)

        total, days = connection.execute(
            f"SELECT count(*), count(DISTINCT Date) FROM {DELIVERY_TABLE}"
        ).fetchone()

    return {
        "fetched_days": len(fetched),
        "already_had": len(existing),
        "holidays_or_unpublished": len(holidays),
        "table_rows": int(total),
        "table_days": int(days),
        "first_fetched": fetched[0] if fetched else None,
        "last_fetched": fetched[-1] if fetched else None,
    }
