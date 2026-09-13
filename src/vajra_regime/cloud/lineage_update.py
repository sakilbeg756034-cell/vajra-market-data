"""Naye ISIN badlav cloud par hi naksha me jodo -- laptop ke bina.

KYUN
----
`state/isin_lineage.parquet` bootstrap ke din laptop se banta hai. Uske baad
NSE jab bhi kisi company ki face value badalta hai, naya ISIN aata hai jo
naksha me nahi hota. Cloud use bilkul naya security samajhta hai: purani
series ek ISIN par, nayi doosre par. Naya tukda 252-session wali shart par fail
hota hai aur naam ~1 saal ke liye sheet se chup-chaap GAYAB ho jaata hai. Saath
hi us symbol ka corporate action "ek symbol, do ISIN" maan kar chhod diya jaata
hai -- yaani split ka apna event bhi. Ye wahi TDPOWERSYS wala bug hai
(8-Sep-2026). Naksha ne use theek kiya tha, par sirf bootstrap ke din tak ke
badlav ke liye.

Naapa gaya (14-Sep-2026, laptop dataset): 750 ke universe me har saal 50-70
aise badlav hote hain (2024 me 70). Yaani lagbhag har hafte ek.

NIYAM -- jodna sirf tab jab saari baatein sach hon
-------------------------------------------------
Ek hi Symbol ke do lagatar ISIN-tukde A (purana) aur B (naya):
  1. ISIN ke pehle 9 akshar same: `IN` + company type + 4-akshar issuer code
     + 2-akshar security type. Yaani WAHI issuer, WAHI equity; sirf serial
     badla.
  2. Overlap nahi -- A ka aakhri din B ke pehle din se pehle.
  3. Beech ka gap 20 calendar din se zyada nahi.
  4. B pehle se naksha me nahi hai. Laptop ne jo faisla kiya, wahi jeetta hai.

Laptop ke apne naksha ke khilaf jaancha gaya (14-Sep-2026):
  * 2011-2026, 750 universe: 518 lagatar tukdo me 511 sahi jode, 0 galat
    jode, 2 chhoote (dono me stock 26 aur 245 din suspend tha).
  * cloud ke asli 500-session store par replay (naksha pichhli tareekh par
    "jama" kar): 2026-01 ke baad ke 31 badlav me 29 sahi, 0 doosri company
    se jode; chhoote 2 naam ka purana ISIN store me tha hi nahi.

Galat jodne ka nuksaan bhi seemit hai: do series ke beech ka bhaav-jhatka agar
koi corporate action nahi samjhata, to `signal.quarantine` ka "unexplained
move" niyam naam ko bahar kar deta hai. Jodna kabhi toote bhaav ko chup-chaap
sahi nahi dikha sakta.

Jo naya ISIN niyam me nahi baithta wo JODA NAHI jaata -- par chup bhi nahi
rehta: `status.json` ke `isin_lineage_unresolved` me aata hai.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import duckdb
import pandas as pd

from vajra_regime.cloud.state import StatePaths

ISSUER_PREFIX_CHARS = 9
MAX_GAP_CALENDAR_DAYS = 20
# Sirf haal ke unresolved badlav report hote hain. Warna ek purana, jaan-boojh
# kar na joda gaya tukda store ke 500 session tak roz chetavni deta rehta.
UNRESOLVED_REPORT_DAYS = 30


def same_issuer_equity(old: str, new: str) -> bool:
    """Dono ISIN ek hi issuer ke ek hi security type ke hain?"""
    return (len(old) == 12 and len(new) == 12 and old.startswith("IN")
            and old[:ISSUER_PREFIX_CHARS] == new[:ISSUER_PREFIX_CHARS])


def _spans(paths: StatePaths) -> list[tuple[str, str, date, date]]:
    """Har (Symbol, NSE ISIN) ka pehla aur aakhri din store me.

    Tareekh ISO text ban kar aati hai aur `date` me badli jaati hai: store me
    Date kabhi date, kabhi timestamp type ki hoti hai, aur dono ko pandas ke
    raaste milana hi wo jagah hai jahan type ki chup-chaap galti hoti hai.
    """
    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT upper(trim(CAST(Symbol AS VARCHAR))),
                   CAST(ISIN AS VARCHAR),
                   strftime(CAST(min(Date) AS DATE), '%Y-%m-%d'),
                   strftime(CAST(max(Date) AS DATE), '%Y-%m-%d')
            FROM read_parquet('{paths.prices.as_posix()}')
            WHERE ISIN IS NOT NULL AND Symbol IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 3, 2
            """
        ).fetchall()
    return [(s, i, date.fromisoformat(f), date.fromisoformat(l)) for s, i, f, l in rows]


def _existing(paths: StatePaths) -> pd.DataFrame:
    if not paths.isin_lineage.exists():
        return pd.DataFrame({"SourceISIN": pd.Series(dtype=str),
                             "CanonicalISIN": pd.Series(dtype=str)})
    frame = pd.read_parquet(paths.isin_lineage, columns=["SourceISIN", "CanonicalISIN"])
    return frame.dropna().astype(str).drop_duplicates("SourceISIN")


def extend(paths: StatePaths) -> dict:
    """Store me aaye naye ISIN badlav naksha me jodo.

    Wapas aata hai: `added` (jo joda gaya) aur `unresolved` (haal ka naya ISIN
    jo kisi purane tukde se nahi juda). Kuch naya na mile to file chhui nahi
    jaati -- doosra run bit-for-bit wahi file chhodta hai.
    """
    report: dict = {"added": [], "unresolved": []}
    if not paths.prices.exists():
        return report

    existing = _existing(paths)
    canon = dict(zip(existing["SourceISIN"], existing["CanonicalISIN"]))
    spans = _spans(paths)
    if not spans:
        return report
    report_from = max(last for *_rest, last in spans) - timedelta(days=UNRESOLVED_REPORT_DAYS)

    by_symbol: dict[str, list[tuple[str, date, date]]] = {}
    for symbol, isin, first, last in spans:
        by_symbol.setdefault(symbol, []).append((isin, first, last))

    for symbol in sorted(by_symbol):
        chain = by_symbol[symbol]
        for (a, _a_first, a_last), (b, b_first, _b_last) in zip(chain, chain[1:]):
            if canon.get(a, a) == canon.get(b, b) or b in canon:
                # Pehle se ek hi company, ya B ka faisla pehle ho chuka hai
                # (laptop ka naksha, ya isi run me kisi aur symbol se).
                continue
            gap = (b_first - a_last).days
            item = {"Symbol": symbol, "OldISIN": a, "NewISIN": b,
                    "OldLast": a_last.isoformat(), "NewFirst": b_first.isoformat(),
                    "GapDays": gap}
            if same_issuer_equity(a, b) and 0 < gap <= MAX_GAP_CALENDAR_DAYS:
                # A->B->C: C bhi seedha A ki company par, kyunki canon[a] pehle
                # hi pichhle kadam ka jawab de chuka hota hai.
                canon[b] = canon.get(a, a)
                item["CanonicalISIN"] = canon[b]
                report["added"].append(item)
            elif b_first >= report_from:
                report["unresolved"].append(item)

    if report["added"]:
        added = pd.DataFrame(
            [{"SourceISIN": x["NewISIN"], "CanonicalISIN": x["CanonicalISIN"]}
             for x in report["added"]]
        )
        out = (pd.concat([existing, added], ignore_index=True)
               .drop_duplicates("SourceISIN", keep="first")
               .sort_values("SourceISIN").reset_index(drop=True))
        paths.isin_lineage.parent.mkdir(parents=True, exist_ok=True)
        tmp = paths.isin_lineage.with_suffix(".parquet.tmp")
        out.to_parquet(tmp, index=False)
        os.replace(tmp, paths.isin_lineage)
    return report
