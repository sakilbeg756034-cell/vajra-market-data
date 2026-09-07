"""NSE ka apna purana bhavcopy archive -- wahi jagah jahan MARE HUE naam bhi hain.

KYA MASLA THA
-------------
Is project ka poora price history ek third-party source (`eod2_data-main`) se
aaya tha. Wo har symbol ki alag CSV file rakhta hai -- aur SIRF UN symbols ki
jo AAJ listed hain. Company delist ho jaye, merge ho jaye, ya insolvency me
chali jaye, to uski file rukh jaati hai.

Nateeja: 17 saal me hamare data me ek bhi naam "mara" nahi. Ek bhi delisting,
suspension, ya insolvency nahi. Bharat me har saal dozens hoti hain.

Naapa gaya, 30 January 2015 par:

    NSE ki apni bhavcopy (EQ)  : 1,416 naam
    hamare data me us din      : 1,087 naam
    GAYAB                      :   483  (34.1%)

Gayab naamo me ABGSHIP (ABG Shipyard, Rs 22,000 crore fraud), ALBK (Allahabad
Bank), ABIRLANUVO, 8KMILES (Rs 1,000 se Rs 10), ADHUNIK, ADVANTA, 3IINFOTECH --
sab apne waqt me bade aur liquid naam, aur kai to girne se PEHLE momentum naam
the. Yaani theek wahi jo ye strategy khareedti hai.

Survivorship bias hamesha return BADHA kar dikhata hai, aur momentum uspar
sabse zyada exposed hai.

KYUN YE SOURCE THEEK HAI
------------------------
NSE ka daily bhavcopy us din ka RECORD hai, kisi list ka snapshot nahi. Jo us
din trade hua wo usme hai -- chahe wo company aaj maujood ho ya na ho. Isliye
ye survivorship-free hai *by construction*, kisi ke bharose se nahi.

DO FORMAT, EK SEEMA
-------------------
    2011 se           cm{DD}{MON}{YYYY}bhav.csv.zip  -- ISIN aur TOTALTRADES ke saath
    2009-2010         wahi file, par ISIN column NAHI

Hamara backtest 2011 se shuru hota hai (2009-10 sirf warm-up hai, jahan R12 ko
252 session chahiye). ISIN 2011 se maujood hai, isliye us daur ka rebuild
saaf-suthra ho sakta hai. 2009-10 ko chhua nahi jaata.

NSE KE SAATH TAMEEZ SE
----------------------
~3,700 file maangni hain. Beech me thoda rukna zaroori hai -- na sirf isliye ki
block na ho, balki isliye bhi ki ye kisi aur ka server hai. Har file ek baar
disk par aa jaye to dobara nahi maangi jaati.
"""
from __future__ import annotations

import io
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from vajra_regime.nse_live import USER_AGENT

HISTORICAL_URL = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/"
    "{year}/{mon}/cm{day:02d}{mon}{year}bhav.csv.zip"
)

# ISIN is se pehle bhavcopy me hai hi nahi -- jaancha gaya: 2010-JUN me nahi,
# 2011-JUN me hai. Bina ISIN ke row ko surakshit tareeke se jodna mumkin nahi,
# isliye us daur ko chhua hi nahi jaata.
FIRST_YEAR_WITH_ISIN = 2011

MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

# Ye wahi teen series hain jo live intake me hain -- EQ aur surveillance BE/BZ.
# SME (SM/ST) aur government securities (GS/GB/SG) yahan bhi nahi.
KEEP_SERIES = ("EQ", "BE", "BZ")

REQUIRED = ("SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE",
            "TOTTRDQTY", "TOTTRDVAL", "ISIN")


def historical_url(day: date) -> str:
    return HISTORICAL_URL.format(year=day.year, mon=MONTHS[day.month - 1], day=day.day)


def archive_path(root: Path, day: date) -> Path:
    return root / str(day.year) / f"cm{day.day:02d}{MONTHS[day.month - 1]}{day.year}bhav.csv.zip"


def download_day(day: date, root: Path, *, timeout: int = 45,
                 retries: int = 2) -> str:
    """Ek din ka zip disk par laao. Pehle se hai to chhoo kar nahi dekhte.

    Wapas aata hai: "EXISTS" | "DOWNLOADED" | "HOLIDAY"

    404 ka matlab chhutti hai, galti nahi -- NSE us din ka file banata hi nahi.
    Baaki har error upar uthta hai, kyunki "file nahi mili" ko chup-chaap "us
    din kuch nahi hua" maan lena wahi chuppi hai jise ye poora project rokta
    aaya hai.
    """
    path = archive_path(root, day)
    if path.exists() and path.stat().st_size > 0:
        return "EXISTS"

    url = historical_url(day)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/zip,application/octet-stream,*/*",
            "Referer": "https://www.nseindia.com/",
        },
    )
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read()
            break
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return "HOLIDAY"
            last = exc
        except Exception as exc:                                # noqa: BLE001
            last = exc
        if attempt < retries:
            time.sleep(2.0 * (attempt + 1))
    else:
        raise RuntimeError(f"NSE historical download failed: {url}: {last}")

    if len(payload) < 200:
        raise RuntimeError(f"NSE historical file suspiciously small "
                           f"({len(payload)} bytes): {url}")

    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".zip.part")
    partial.write_bytes(payload)
    partial.replace(path)
    return "DOWNLOADED"


def parse_day(path: Path, day: date) -> pd.DataFrame:
    """Ek din ka zip -> saaf rows. Har jaanch upar uthti hai, chup nahi rehti."""
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"{path.name}: {len(names)} CSV mile, 1 chahiye tha")
        frame = pd.read_csv(io.BytesIO(archive.read(names[0])))

    frame.columns = [str(c).strip().upper() for c in frame.columns]
    missing = sorted(set(REQUIRED).difference(frame.columns))
    if missing:
        raise ValueError(f"{path.name}: ye column nahi mile: {missing}")

    frame["SERIES"] = frame["SERIES"].astype(str).str.strip().str.upper()
    frame = frame.loc[frame["SERIES"].isin(KEEP_SERIES)].copy()

    out = pd.DataFrame({
        "Date": day,
        "Symbol": frame["SYMBOL"].astype(str).str.strip().str.upper(),
        "ISIN": frame["ISIN"].astype(str).str.strip().str.upper(),
        "Series": frame["SERIES"],
        "Open": pd.to_numeric(frame["OPEN"], errors="coerce"),
        "High": pd.to_numeric(frame["HIGH"], errors="coerce"),
        "Low": pd.to_numeric(frame["LOW"], errors="coerce"),
        "Close": pd.to_numeric(frame["CLOSE"], errors="coerce"),
        "Volume": pd.to_numeric(frame["TOTTRDQTY"], errors="coerce"),
        "Turnover": pd.to_numeric(frame["TOTTRDVAL"], errors="coerce"),
        "TotalTrades": (pd.to_numeric(frame["TOTALTRADES"], errors="coerce")
                        if "TOTALTRADES" in frame.columns else pd.NA),
    })

    # Wahi bhaav-jaanch jo live intake par lagti hai. Alag niyam rakhne ka
    # matlab hota do adhe data-set jinme alag cheezein "valid" hain.
    out = out.loc[
        out["ISIN"].str.startswith("INE", na=False)
        & out["Open"].gt(0) & out["High"].gt(0)
        & out["Low"].gt(0) & out["Close"].gt(0)
        & out["Volume"].notna() & out["Volume"].ge(0)
        & out["High"].ge(out[["Open", "Close", "Low"]].max(axis=1))
        & out["Low"].le(out[["Open", "Close", "High"]].min(axis=1))
    ].copy()

    # Ek ISIN ek din me ek hi baar. Do series me mile to EQ jeetta hai --
    # tradeable wahi hai.
    rank = {s: i for i, s in enumerate(KEEP_SERIES)}
    out = (out.assign(_r=out["Series"].map(rank))
              .sort_values(["ISIN", "_r", "Volume"], ascending=[True, True, False])
              .drop_duplicates("ISIN", keep="first")
              .drop(columns="_r")
              .reset_index(drop=True))
    return out


def _weekdays(start: date, end: date):
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def fetch_range(start: date, end: date, root: Path, *,
                pause: float = 0.35, progress_every: int = 100) -> dict[str, object]:
    """`start` se `end` tak ke saare din laao. Jo pehle se hai wo chhoda jaata hai."""
    if start.year < FIRST_YEAR_WITH_ISIN:
        raise ValueError(
            f"{start.year} me bhavcopy me ISIN hai hi nahi. "
            f"Ye module {FIRST_YEAR_WITH_ISIN} se hi chalta hai."
        )
    counts = {"DOWNLOADED": 0, "EXISTS": 0, "HOLIDAY": 0}
    for done, day in enumerate(_weekdays(start, end), start=1):
        status = download_day(day, root)
        counts[status] += 1
        if status == "DOWNLOADED":
            time.sleep(pause)
        if progress_every and done % progress_every == 0:
            print(f"   {done} din  |  naye {counts['DOWNLOADED']}  "
                  f"pehle se {counts['EXISTS']}  chhutti {counts['HOLIDAY']}",
                  flush=True)
    return counts


__all__ = ["FIRST_YEAR_WITH_ISIN", "archive_path", "download_day",
           "fetch_range", "historical_url", "parse_day"]
