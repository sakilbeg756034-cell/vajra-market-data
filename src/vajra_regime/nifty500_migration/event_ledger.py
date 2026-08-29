from __future__ import annotations

# ruff: noqa: E501 -- official evidence descriptions and comma-delimited event records remain auditable as one line.

import csv
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION, INVALID_SYMBOL_TOKENS


MANUAL_EVENTS = [
    {
        "source_file": "ind_prs10022009.pdf",
        "effective_date": "2009-03-27",
        "exclusions": "DICIND,FOSECOIND,INEOS,MAHSCOOTER,NEPC,SALORA,SAMTEL,SANDESH,SUMMITSEC,SWARAJMAZ,THIRU,VINDHYATEL,VISEXPORTS,WHEELS,WWIL",
        "inclusions": "PIRHEALTH,PURVA,ADVANTA,FCH,VISHAL,IRB,KOUTONS,CHETTINAD,AREVAT&D,UTVSOF,JAICORPLTD,DYNAMATECH,NFL,KSOILS,BALLARPUR",
        "curation_reason": "Official PDF contains company names in a split cross-page table; historical ticker aliases cross-checked against archived snapshots and NSE evidence.",
        "confidence": "RECONSTRUCTED_HIGH_CONFIDENCE",
    },
    {
        "source_file": "ind_prs06052009.pdf",
        "effective_date": "2009-05-11",
        "exclusions": "SHREE",
        "inclusions": "REIAGRO",
        "curation_reason": "Official company-name event; symbols reconciled using official NSE archive references.",
        "confidence": "VERIFIED_MULTI_SOURCE",
    },
    {
        "source_file": "ind_prs19062009.pdf",
        "effective_date": "2009-06-23",
        "exclusions": "AZTECSOFT",
        "inclusions": "CENTURYPLY",
        "curation_reason": "Official company-name event; symbols reconciled using official press alias history.",
        "confidence": "VERIFIED_MULTI_SOURCE",
    },
    {
        "source_file": "ind_prs04092009.pdf",
        "effective_date": "2009-10-22",
        "exclusions": "AGRODUTCH,UCALFUEL,MRO-TEK,VALUEIND,CRESTANI,SESHAPAPER,UTTAM,LUMAXIND,HONDAPOWER,CHETTINAD",
        "inclusions": "UBL,IFCI,PTC,ISPATIND,KEC,GTOFFSHORE,GSPL,YESBANK,EKC,REDINGTON",
        "curation_reason": "September 9 official correction changed effective date from October 20 to October 22; official names mapped to contemporaneous tickers.",
        "confidence": "RECONSTRUCTED_HIGH_CONFIDENCE",
    },
    {
        "source_file": "ind_prs16122009.pdf",
        "effective_date": "2009-12-22",
        "exclusions": "ZANDU",
        "inclusions": "3IINFOTECH",
        "curation_reason": "Official company-name event with contemporaneous symbol cross-check.",
        "confidence": "VERIFIED_MULTI_SOURCE",
    },
    {
        "source_file": "ind_prs19022010.pdf",
        "effective_date": "2010-02-24",
        "exclusions": "ASIANHOT",
        "inclusions": "PENINLAND",
        "curation_reason": "Official company-name event; pre-restructuring Asian Hotels ticker retained.",
        "confidence": "RECONSTRUCTED_HIGH_CONFIDENCE",
    },
    {
        "source_file": "ind_prs24022010.pdf",
        "effective_date": "2010-04-08",
        "exclusions": "MUKTAARTS,DONEAR,SIRPUR,CONSOLID,SMARTLINK",
        "inclusions": "GAMMNINFRA,KGL,JINDALSWHL,BINANICEM,BANCOINDIA",
        "curation_reason": "Official periodic-review table split across PDF pages; contemporaneous symbol aliases reconciled.",
        "confidence": "RECONSTRUCTED_HIGH_CONFIDENCE",
    },
    {
        "source_file": "ind_prs05042010.pdf",
        "effective_date": "2010-04-06",
        "exclusions": "MICRO",
        "inclusions": "NHPC",
        "curation_reason": "Official company-name event with historical ticker cross-check.",
        "confidence": "VERIFIED_MULTI_SOURCE",
    },
    {
        "source_file": "ind_prs12042010.pdf",
        "effective_date": "2010-04-15",
        "exclusions": "ZEENEWS,KIRLOSOIL",
        "inclusions": "ADANIPOWER,OIL",
        "curation_reason": "Exact official S&P CNX 500 demerger/suspension table; legacy PDF layout was not captured by the generic parser.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs20052010.pdf",
        "effective_date": "2010-05-26",
        "exclusions": "GRASIM",
        "inclusions": "JSWENERGY",
        "curation_reason": "Exact official S&P CNX 500 Grasim demerger replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs10022011.pdf",
        "effective_date": "2011-03-25",
        "exclusions": "AGCNET,AJANTPHARM,ASIANELEC,BPL,DWARKESH,GOKEX,HMT,PEARLPOLY,KOHINOOR,MAHINDUGIN,MIRZAINT,MUNJALSHOW,OMAXAUTO,PVP,RAMCOSYS,SAREGAMA,VISHAL",
        "inclusions": "ADSL,BAJAJELEC,BGRENERGY,COREPROTEC,COX&KINGS,DELTACORP,EMAMILTD,JKTYRE,KEMROCK,OPTOCIRCUI,ORISSAMINE,PANTALOONR,PIPAVAV,SADBHAV,WHIRLPOOL,ZEEL,ZYDUSWELL",
        "curation_reason": "Official 17-for-17 periodic-review company-name table; symbol aliases reconciled from official/archived histories.",
        "confidence": "RECONSTRUCTED_HIGH_CONFIDENCE",
    },
    {
        "source_file": "ind_prs01122011.pdf",
        "effective_date": "2011-12-07",
        "exclusions": "IBREALEST",
        "inclusions": "JAIBALAJI",
        "curation_reason": "Effective date is printed in official press release but OCR date parser missed it.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs27042011.pdf",
        "effective_date": "2011-05-03",
        "exclusions": "TRIVENI",
        "inclusions": "PRESTIGE",
        "curation_reason": "Exact official S&P CNX 500 demerger replacement; split-page OCR missed the table.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs16052011.pdf",
        "effective_date": "2011-05-18",
        "exclusions": "BINANICEM",
        "inclusions": "IL&FSENGG",
        "curation_reason": "Exact official S&P CNX 500 voluntary-delisting replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs14032012.pdf",
        "effective_date": "2012-04-27",
        "exclusions": "ADSL,BALAJITELE,DHAMPURSUG,INOXLEISUR,LAKSHMIEFL,NFL,NDTV,NELCO,PANACEABIO,PAPERPROD,STCINDIA,SURYAROSNI,TATAMETALI,WOCKPHARMA",
        "inclusions": "ALSTOMT&D,ARSHIYA,EROSMEDIA,ICRA,ICSA,KWALITY,LOVABLE,MUTHOOTFIN,PRAKASH,RAMKY,TDPOWERSYS,TECHNO,TREEHOUSE,WABCOINDIA",
        "curation_reason": "Official symbol table is complete; OCR split the month from April 27, 2012.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs08062012.pdf",
        "effective_date": "2012-06-18",
        "exclusions": "CHEMPLAST",
        "inclusions": "MANINFRA",
        "curation_reason": "Exact official S&P CNX 500 voluntary-delisting replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs05032013.pdf",
        "effective_date": "2013-03-07",
        "exclusions": "ORIENTPPR",
        "inclusions": "SPARC",
        "curation_reason": "Exact official CNX 500 demerger replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs06062013.pdf",
        "effective_date": "2013-06-11",
        "exclusions": "FUTUREVENT,JSWISPAT",
        "inclusions": "INFRATEL,DBCORP",
        "curation_reason": "Official symbol table and effective date are explicit but OCR duplicates tokens.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs14062013.pdf",
        "effective_date": "2013-06-21",
        "exclusions": "KSOILS",
        "inclusions": "3IINFOTECH",
        "curation_reason": "Exact official CNX 500 table; CRISIL in the PDF masthead was incorrectly captured as a constituent symbol by OCR.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs11072013.pdf",
        "effective_date": "2013-07-17",
        "exclusions": "CENTURYPLY,JINDALPOLY",
        "inclusions": "20MICRONS,INFINITE",
        "curation_reason": "Exact official CNX 500 corporate-restructuring table and effective date.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs02082013.pdf",
        "effective_date": "2013-08-07",
        "exclusions": "SUJANATOW",
        "inclusions": "WIPRO",
        "curation_reason": "Exact official CNX 500 suspension-replacement table and effective date.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs25042014.pdf",
        "effective_date": "2014-05-12",
        "exclusions": "WYETH",
        "inclusions": "MARICO",
        "curation_reason": "Exact official CNX 500 scheme-of-arrangement replacement; layout OCR split the symbol table.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs18062014.pdf",
        "effective_date": "2014-06-30",
        "exclusions": "MAHINDUGIN",
        "inclusions": "RAIN",
        "curation_reason": "Exact official CNX 500 scheme-of-arrangement replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs18072014.pdf",
        "effective_date": "2014-07-28",
        "exclusions": "MANINDS",
        "inclusions": "CASTROLIND",
        "curation_reason": "Exact official CNX 500 demerger replacement.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs20022015.pdf",
        "effective_date": "2015-03-27",
        "exclusions": "DCW,EDUCOMP,ELDERPHARM,HUBTOWN,IL&FSENGG,IMFA,LOVABLE,PFOCUS,RAJTV,REIAGROLTD,TI,VENKEYS",
        "inclusions": "FCEL,GULFOILLUB,IFBIND,JKCEMENT,KPRMILL,KNRCON,LGBBROSLTD,MARKSANS,NDTV,PENIND,MCDOWELL-N,VINATIORGA",
        "curation_reason": "Exact official CNX 500 section; OCR had appended LIX15 and LIX15 Midcap rows and split the IL&FSENGG symbol.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs24082015.pdf",
        "effective_date": "2015-09-28",
        "exclusions": "CROMPGREAV,MAX",
        "inclusions": "ADLABS,EVEREADY",
        "curation_reason": "Exact official CNX 500 section; HAVELLS belongs to the following LIX15 Midcap section.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs12082015.pdf",
        "effective_date": "2015-09-28",
        "exclusions": "ANSALAPI,BGRENERGY,GAMMONIND,GUJNRECOKE,HINDOILEXP,HOTELEELA,IFBIND,KESORAMIND,MANGCHEFER,MONNETISPA,ORCHIDCHEM,RAMCOIND,RASOYPR,RTNPOWER",
        "inclusions": "ADANIPOWER,BALKRISIND,CAMLINFINE,FLFL,IL&FSENGG,INOXWIND,KRBL,METALFORGE,ORIENTCEM,RICOAUTO,SITICABLE,SRIPIPES,SYMPHONY,VRLLOG",
        "curation_reason": "Exact official 14-for-14 CNX 500 section; OCR had appended LIX15 and LIX15 Midcap rows.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs22022016_2.pdf",
        "effective_date": "2016-04-01",
        "exclusions": "ADLABS,ATFL,ASAHIINDIA,AUTOAXLES,CAMLINFINE,CARBORUNIV,CENTENKA,DHANBANK,ENIL,ESABINDIA,FMGOETZE,FLEXITUFF,GABRIEL,GAMMNINFRA,GEOJITBNPP,GEOMETRIC,GHCL,GITANJALI,GRAPHITE,GTLINFRA,GUJALKALI,GIPCL,GNFC,GULFOILLUB,HEG,HERITGFOOD,IL&FSENGG,IBVENTURES,ITDCEM,IVRCLINFRA,JYOTISTRUC,KCP,KKCL,KNRCON,KSBPUMPS,LGBBROSLTD,LINDEINDIA,MAHSCOOTER,MAHSEAMLES,MBLINFRA,MERCATOR,METALFORGE,MTEDUCARE,MUNJALSHOW,NBVENTURES,NAVNETEDUL,NDTV,NIITLTD,NOCIL,NOIDATOLL,OPTOCIRCUI,BINDALAGRO,PATELENG,PENINLAND,PENIND,PRAKASH,RELIGARE,RICOAUTO,SEINV,SHANTIGEAR,SHRENUJ,SONASTEER,SRIPIPES,SBT,SUPREMEINF,SUPPETRO,SWARAJENG,TATAINVEST,TDPOWERSYS,TVTODAY,USHAMART,UTTAMSTL,VESUVIUS,VIVIDHA,WHEELS",
        "inclusions": "8KMILES,AARTIDRUGS,ADANIENT,ADANITRANS,ABFRL,AEGISCHEM,AHLUCONT,ASHIANA,AVANTIFEED,BGRENERGY,BLISSGVS,BRFL,CAPLIPOINT,CCL,CERA,DALMIABHA,DHANUKA,DISHTV,GLOBOFFS,GRANULES,GREENPLY,GRINDWELL,GUJGASLTD,HEIDELBERG,HMVL,HITACHIHOM,HMT,HOTELEELA,IDFC,IFBIND,IGARASHI,ICIL,INDOCO,INTELLECT,IPAPPM,JETAIRWAYS,JINDALPOLY,JMTAUTOLTD,KESORAMIND,KIRLOSENG,KWALITY,MTNL,MAHINDCIE,MANAPPURAM,MANPASAND,MINDACORP,NAVKARCORP,PCJEWELLER,POLARIS,RAMCOSYS,RKFORGE,RTNPOWER,RELAXO,SADBHIN,SHARDACROP,SHILPAMED,SJVN,SMLISUZU,SNOWMAN,SOLARINDS,SOMANYCERA,STCINDIA,STYABS,SYNGENE,TAKE,TATAMTRDVR,TTML,TEXRAIL,TIDEWATER,TIMKEN,TCI,TRITURBINE,TVSSRICHAK,VGUARD,WONDERLA,ZEELEARN",
        "curation_reason": "Exact official 75-for-76 Nifty 500 table. The documented +1 is Tata Motors DVR, which made the index contain 501 securities.",
        "confidence": "VERIFIED_OFFICIAL",
        "documented_member_count_delta": 1,
    },
    {
        "source_file": "ind_prs12082016.pdf",
        "effective_date": "2016-09-30",
        "exclusions": "AARTIDRUGS,ABGSHIP,ASHIANA,CASTEXTECH,CLNINDIA,ELECTCAST,ELGIEQUIP,ESSDEE,FINANTECH,GLOBOFFS,HMT,HOTELEELA,IFBIND,INEOSSTYRO,KIRLOSENG,KSK,MINDACORP,PARSVNATH,PURVA,RATNAMANI,SADBHIN,TTML,TREEHOUSE,TBZ,VAIBHAVGBL",
        "inclusions": "ABIRLANUVO,ALKEM,APLAPOLLO,CARBORUNIV,COFFEEDAY,CROMPGREAV,LALPATHLAB,EQUITAS,IDFCBANK,INFIBEAM,INDIGO,ITDCEM,KSBPUMPS,LINDEINDIA,MFSL,NH,NAVINFLUOR,NILKAMAL,OCL,RELIGARE,SEQUENT,SFCL,MYSOREBANK,SBT,SUPRAJIT",
        "curation_reason": "Exact official 25-for-25 Nifty 500 table; page-split OCR omitted INEOSSTYRO, IDFCBANK and KSBPUMPS.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs16022017.pdf",
        "effective_date": "2017-03-31",
        "exclusions": "ALOKTEXT,BRFL,BRIGADE,DYNAMATECH,FLFL,INGERRAND,IPAPPM,JPPOWER,JPINFRATEC,JSWHL,KSBPUMPS,LAOPALA,LITL,LINDEINDIA,MAHLIFE,MAYURUNIQ,SEQUENT,SHOPERSTOP,SIMPLEXINF,SINTEX,SITINET,SBBJ,MYSOREBANK,SBT,SUPRAJIT",
        "inclusions": "DBL,ENDURANCE,FMGOETZE,FRETAIL,GHCL,GNFC,GUJALKALI,ICICIPRULI,KNRCON,LTI,LTTS,MAXINDIA,MGL,MINDACORP,MINDAIND,NAVNETEDUL,NBVENTURES,PARAGMILK,QUESS,RATNAMANI,SHILPI,STRTECH,SUDARSCHEM,TATAINVEST,THYROCARE",
        "curation_reason": "Exact official 25-for-25 Nifty 500 table; page-split OCR omitted KSBPUMPS.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs08012018.pdf",
        "effective_date": "2018-02-05",
        "exclusions": "CESC,FORTIS,POLARIS,STAR",
        "inclusions": "COCHINSHIP,HATSUN,RELCAPITAL,TIFIN",
        "curation_reason": "Exact official 4-for-4 Nifty 500 table; page-split OCR omitted TIFIN.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs10062020.pdf",
        "effective_date": "2020-06-26",
        "exclusions": "CHALET,DHFL,FMGOETZE,FLFL,GAYAPROJ,IBULISL,ITDCEM,JISLJALEQS,KENNAMET,KIRLOSENG,LAKSHVILAS,MAGMA,NETWORK18,PARAGMILK,PCJEWELLER,RELCAPITAL,RELINFRA,RPOWER,RESPONIND,SHK,SHILPAMED,SUNCLAYLTD,TECHNOE,TRITURBINE,WABAG",
        "inclusions": "ABB,ALKYLAMINE,BHARATRAS,CENTURYTEX,CSBBANK,DHANUKA,ESABINDIA,GRSE,GMMPFAUDLR,FLUOROCHEM,HATHWAY,IIFLWAM,IRCTC,INDOCO,INGERRAND,KSB,LAOPALA,POLYMED,SCHNEIDER,SEQUENT,SCI,SUMICHEM,TATACOMM,UJJIVANSFB,VESUVIUS",
        "curation_reason": "Exact official 25-for-25 deferred-review table; page-split OCR omitted IIFLWAM.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs22042021.pdf",
        "effective_date": "2021-04-29",
        "exclusions": "GHCL,TATASTLBSL",
        "inclusions": "BURGERKING,INFIBEAM",
        "curation_reason": "Exact official 2-for-2 amalgamation table; page-split OCR omitted TATASTLBSL.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs23082024_1.pdf",
        "effective_date": "2024-08-30",
        "exclusions": "TATAMTRDVR",
        "inclusions": "",
        "curation_reason": "Official cancellation of Tata Motors DVR. No replacement was required because DVR had been an additional 501st security.",
        "confidence": "VERIFIED_OFFICIAL",
        "documented_member_count_delta": -1,
    },
    {
        "source_file": "ind_prs25092024.pdf",
        "effective_date": "2024-09-30",
        "exclusions": "PRSMJOHNSN",
        "inclusions": "IDEA",
        "curation_reason": "Official revocation of IDEA exclusion. Applied with the August 23 periodic event, it substitutes PRSMJOHNSN as the exclusion while preserving IDEA.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs01082018.pdf",
        "effective_date": "2018-08-08",
        "exclusions": "TECHNO",
        "inclusions": "BDL",
        "curation_reason": "Official symbol table and effective date are explicit; date appears after the index section.",
        "confidence": "VERIFIED_OFFICIAL",
    },
    {
        "source_file": "ind_prs09012020.pdf",
        "effective_date": "2020-01-16",
        "exclusions": "SUVEN",
        "inclusions": "TATASTLBSL",
        "curation_reason": "Official symbol table and effective date are explicit; NIFTY 500 heading omits the word Index.",
        "confidence": "VERIFIED_OFFICIAL",
    },
]

SUPERSEDED_OR_NON_EVENTS = {
    "ind_prs09092009.pdf",  # Correction notice incorporated into the September 4 event date.
    "ind_prs12032020.pdf",  # March 27 proposal subsequently declared null and void.
    "ind_prs18022020.pdf",  # March 27 periodic review subsequently declared null and void.
    "ind_prs19032020.pdf",  # Consequential March 27 proposal subsequently declared null and void.
    "ind_prs13052020.pdf",  # Nullification/methodology notice, not a constituent event.
}


def _split(value: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(token.strip().upper() for token in value.split(",") if token.strip()))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def build_official_event_ledger(*, data_root: Path = DATA_ROOT, as_of: date = date(2026, 8, 13)) -> dict[str, Any]:
    history = data_root / "02 Constituent History"
    parsed_path = history / "official_nifty500_membership_events_parsed_v1.csv"
    layout_path = history / "official_nifty500_membership_events_layout_resolved_v1.csv"
    manifest_path = data_root / "10 Provenance" / "nifty500_relevant_press_release_manifest.csv"
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        source_manifest = {row["file_name"]: row for row in csv.DictReader(handle)}

    manually_replaced = {row["source_file"] for row in MANUAL_EVENTS}
    candidate_rows: list[dict[str, Any]] = []
    for path, method in ((parsed_path, "PARSED_OFFICIAL_TEXT"), (layout_path, "RESOLVED_OFFICIAL_LAYOUT")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if row["source_file"] in manually_replaced | SUPERSEDED_OR_NON_EVENTS:
                    continue
                if row["effective_date"] > as_of.isoformat():
                    continue
                exclusions = _split(row["exclusions"])
                inclusions = _split(row["inclusions"])
                if not exclusions or not inclusions or set(exclusions + inclusions) & set(INVALID_SYMBOL_TOKENS):
                    continue
                candidate_rows.append(
                    {
                        **row,
                        "exclusions": ",".join(exclusions),
                        "inclusions": ",".join(inclusions),
                        "source_method": method,
                        "curation_reason": "Machine-extracted from exact official Nifty500 section.",
                        "confidence": row.get("confidence") or "VERIFIED_OFFICIAL",
                    }
                )

    for event in MANUAL_EVENTS:
        source = source_manifest[event["source_file"]]
        candidate_rows.append(
            {
                **event,
                "announcement_date": source["announcement_date_from_filename"],
                "source_sha256": source["sha256"],
                "section_number": 1,
                "source_method": "CURATED_OFFICIAL_PRESS_EVENT",
            }
        )

    deduplicated: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in candidate_rows:
        exclusions = _split(row["exclusions"])
        inclusions = _split(row["inclusions"])
        key = (row["effective_date"], ",".join(exclusions), ",".join(inclusions))
        normalized = {
            "announcement_date": row.get("announcement_date", ""),
            "effective_date": row["effective_date"],
            "source_file": row["source_file"],
            "source_sha256": row["source_sha256"],
            "exclusions": key[1],
            "inclusions": key[2],
            "exclusion_count": len(exclusions),
            "inclusion_count": len(inclusions),
            "balanced_count": len(exclusions) == len(inclusions),
            "source_method": row["source_method"],
            "confidence": row["confidence"],
            "curation_reason": row["curation_reason"],
        }
        existing = deduplicated.get(key)
        if existing is None or normalized["source_method"].startswith("CURATED"):
            deduplicated[key] = normalized
    rows = sorted(deduplicated.values(), key=lambda row: (row["effective_date"], row["source_file"]))
    output_path = history / "nifty500_official_membership_event_ledger_v1.csv"
    _write_csv(output_path, rows)
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "as_of": as_of.isoformat(),
        "event_count": len(rows),
        "earliest_effective_date": min(row["effective_date"] for row in rows),
        "latest_effective_date": max(row["effective_date"] for row in rows),
        "balanced_event_count": sum(bool(row["balanced_count"]) for row in rows),
        "unbalanced_official_event_count": sum(not bool(row["balanced_count"]) for row in rows),
        "manual_curated_event_count": sum(row["source_method"].startswith("CURATED") for row in rows),
        "superseded_or_non_event_sources_excluded": sorted(SUPERSEDED_OR_NON_EVENTS),
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "official_membership_event_ledger_status.json", status)
    return status
