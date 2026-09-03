"""Cloud ka chhota store: as-traded bhaav + alag se corporate action factor.

YAHAN EK GALTI DOBARA NAHI HONI CHAHIYE
--------------------------------------
2026-09-02 tak engine `rolling_master.py` me DONO feed par corporate action
factor laga raha tha -- jabki unme se ek (EOD2 legacy) pehle se adjusted thi.
Saalon tak ye nahi dikha, kyunki jab saari rows ek hi feed se aati hain to poori
series ek saath scale ho jaati hai aur return badalta hi nahi. Bug tabhi bahar
aaya jab 2026 se doosri, as-traded feed judi.

Is store me theek wahi do feed milti hain: bootstrap rows (VAJRA_DATA se, PEHLE
SE adjusted) aur live rows (NSE bhavcopy se, as-traded).

Isliye har row apne saath `AdjustedThrough` leke chalti hai: wo tareekh jahan tak
us row ka bhaav pehle se adjusted hai. Bootstrap rows par ye bootstrap ka aakhri
session hota hai, live rows par NULL (kuch laga hi nahi). Padhte waqt sirf usse
AAGE ke ex-date wale factor lagte hain.

Pehla draft is kaam ke liye EventId milata tha -- "jo engine ne pehle se laga
diya hai use chhod do". Wo galat tha aur test me pakda gaya: engine ki legacy
rows EOD2 se aati hain, aur EOD2 apne adjustment KHUD kar chuka hota hai. Wo
engine ke applied-ledger me hain hi nahi. HIRECT ka 2026-03-27 wala 1:1 bonus
isi wajah se dobara lag raha tha aur uska R12 52% ki jagah 204% dikh raha tha.
Sawal "kisne lagaya" nahi hai, sawal "kab tak laga hua hai" hai.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

PRICE_COLUMNS = (
    "Date", "ISIN", "Symbol", "Open", "High", "Low", "Close",
    "Volume", "TurnoverINR", "Traded", "IsFrozenBar", "AdjustedThrough", "EngineQuarantined",
)
EVENT_COLUMNS = ("EventId", "ISIN", "Symbol", "ExDate", "PriceFactor", "VolumeFactor")

# Signal ko 252 session chahiye, universe ko 60-session median turnover, aur
# stale-gate ko 252 session peeche dekhna hota hai. 500 rakhna is sab ke liye
# kaafi hai aur file ~40 MB par rukti hai.
RETAIN_SESSIONS = 500


@dataclass(frozen=True)
class StatePaths:
    root: Path

    @property
    def prices(self) -> Path:
        return self.root / "state" / "prices.parquet"

    @property
    def events(self) -> Path:
        return self.root / "state" / "ca_events.parquet"

    @property
    def meta(self) -> Path:
        return self.root / "state" / "meta.json"

    @property
    def history_counts(self) -> Path:
        return self.root / "state" / "history_counts.parquet"


def read_meta(paths: StatePaths) -> dict:
    if not paths.meta.exists():
        return {}
    return json.loads(paths.meta.read_text(encoding="utf-8"))


def write_meta(paths: StatePaths, meta: dict) -> None:
    paths.meta.parent.mkdir(parents=True, exist_ok=True)
    paths.meta.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")


def stored_sessions(paths: StatePaths) -> list[date]:
    if not paths.prices.exists():
        return []
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT DISTINCT Date FROM read_parquet('{paths.prices.as_posix()}') "
            "ORDER BY Date"
        ).fetchall()
    return [r[0] for r in rows]


def append_sessions(paths: StatePaths, frame: pd.DataFrame) -> int:
    """Naye as-traded din jodo. Jo din pehle se hai use dobara nahi likha jaata.

    Wapas aane wali ginti ASAL me likhi gayi rows ki hai, bheji gayi rows ki
    nahi -- taaki "kuch nahi juda" chupchaap "sab theek hai" na dikhe.
    """
    missing = [c for c in PRICE_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"append_sessions is missing columns: {missing}")
    frame = frame.loc[:, list(PRICE_COLUMNS)]

    paths.prices.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("incoming", frame)
        if paths.prices.exists():
            src = f"read_parquet('{paths.prices.as_posix()}')"
            con.execute(
                f"CREATE TABLE merged AS SELECT * FROM {src} "
                "UNION ALL SELECT i.* FROM incoming i "
                f"WHERE NOT EXISTS (SELECT 1 FROM {src} p "
                "               WHERE p.Date = i.Date AND p.ISIN = i.ISIN)"
            )
        else:
            con.execute("CREATE TABLE merged AS SELECT * FROM incoming")

        before = 0
        if paths.prices.exists():
            before = con.execute(
                f"SELECT count(*) FROM read_parquet('{paths.prices.as_posix()}')"
            ).fetchone()[0]
        after = con.execute("SELECT count(*) FROM merged").fetchone()[0]

        # Purani session hata do, warna file har saal badhti rahegi.
        con.execute(
            "CREATE TABLE trimmed AS SELECT * FROM merged WHERE Date >= ("
            f"  SELECT min(d) FROM (SELECT DISTINCT Date AS d FROM merged "
            f"  ORDER BY d DESC LIMIT {RETAIN_SESSIONS}))"
        )
        tmp = paths.prices.with_suffix(".parquet.tmp")
        con.execute(
            f"COPY (SELECT * FROM trimmed ORDER BY Date, ISIN) "
            f"TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    tmp.replace(paths.prices)
    return int(after - before)
