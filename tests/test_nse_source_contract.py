"""NSE source contract + non-equity member handling (05-Oct-2026, 29-Sep REIT incident).

Scenarios: new non-equity series RR, unknown series XX, header rename (legacy aliases), extra harmless column,
removed required column, wrong internal date, previous-day stale file, zero-byte / truncated / HTML / corrupt
file, duplicate symbol, bad prices, constituent additions/removals/series/ISIN changes, and the catch-up counting
missing bars only among equity-eligible members while official non-equity members stay recorded.
"""
from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from vajra_regime.nifty500_migration import incremental_catchup as IC
from vajra_regime.nifty500_migration import source_contract as SC

UDIFF = ["TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb", "SctySrs",
         "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp", "FinInstrmNm", "OpnPric", "HghPric", "LwPric",
         "ClsPric", "LastPric", "PrvsClsgPric", "UndrlygPric", "SttlmPric", "OpnIntrst", "ChngInOpnIntrst",
         "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty", "Rmks", "Rsvd1", "Rsvd2",
         "Rsvd3", "Rsvd4"]


def _rows(session: str, n_eq: int = 600, extra: list[dict] | None = None) -> list[dict]:
    out = []
    for i in range(n_eq):
        out.append({"TradDt": session, "TckrSymb": f"STK{i:04d}", "SctySrs": "EQ", "ISIN": f"INE{i:06d}01010",
                    "OpnPric": "100", "HghPric": "110", "LwPric": "95", "ClsPric": "105", "PrvsClsgPric": "100",
                    "TtlTradgVol": "1000", "TtlTrfVal": "105000", "TtlNbOfTxsExctd": "10"})
    return out + (extra or [])


def _zip(tmp: Path, name: str, rows: list[dict], header: list[str] = UDIFF, raw: bytes | None = None) -> Path:
    path = tmp / name
    buf = io.StringIO()
    if raw is None:
        frame = pd.DataFrame(rows)
        for col in header:
            if col not in frame:
                frame[col] = ""
        frame[header].to_csv(buf, index=False)
        payload = buf.getvalue().encode()
    else:
        payload = raw
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr(name.replace(".zip", ""), payload)
    return path


REIT = {"TradDt": "2026-09-29", "TckrSymb": "EMBASSY", "SctySrs": "RR", "ISIN": "INE041025011",
        "OpnPric": "420", "HghPric": "430", "LwPric": "418", "ClsPric": "428.7", "TtlTradgVol": "1000"}


# ------------------------------------------------------------------------------------------ bhavcopy canary
def test_good_file_passes_and_counts_series(tmp_path: Path) -> None:
    p = _zip(tmp_path, "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip", _rows("2026-09-29", extra=[REIT]))
    r = SC.check_bhavcopy(p, date(2026, 9, 29))
    assert r["status"] == "PASS" and r["series_counts"]["RR"] == 1 and r["equity_rows"] == 600


def test_wrong_internal_date_and_previous_day_file_rejected(tmp_path: Path) -> None:
    p = _zip(tmp_path, "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip", _rows("2026-09-28"))
    with pytest.raises(SC.SourceContractError, match="SOURCE_WRONG_DATE"):
        SC.check_bhavcopy(p, date(2026, 9, 29))


@pytest.mark.parametrize("kind,match", [("zero", "SOURCE_EMPTY"), ("html", "SOURCE_HTML"),
                                        ("corrupt", "SOURCE_CORRUPT"), ("truncated", "SOURCE_TRUNCATED")])
def test_broken_files_rejected(tmp_path: Path, kind: str, match: str) -> None:
    name = "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip"
    if kind == "zero":
        p = tmp_path / name; p.write_bytes(b"")
    elif kind == "html":
        p = _zip(tmp_path, name, [], raw=b"<html><body>Resource not found</body></html>")
    elif kind == "corrupt":
        p = tmp_path / name; p.write_bytes(b"PK\x03\x04garbage")
    else:
        p = _zip(tmp_path, name, _rows("2026-09-29", n_eq=40))
    with pytest.raises(SC.SourceContractError, match=match):
        SC.check_bhavcopy(p, date(2026, 9, 29))


def test_removed_required_column_fails_extra_column_and_reorder_adapt(tmp_path: Path) -> None:
    rows = _rows("2026-09-29")
    p = _zip(tmp_path, "a.csv.zip", rows, header=[c for c in UDIFF if c != "ClsPric"])
    with pytest.raises(SC.SourceContractError, match="SCHEMA_REQUIRED_COLUMN_MISSING"):
        SC.check_bhavcopy(p, date(2026, 9, 29))
    for r in rows:
        r["NewHarmlessCol"] = "x"
    p2 = _zip(tmp_path, "b.csv.zip", rows, header=list(reversed(UDIFF)) + ["NewHarmlessCol"])
    r = SC.check_bhavcopy(p2, date(2026, 9, 29), known_headers=UDIFF)
    assert r["status"] == "PASS" and r["schema_drift"]["added"] == ["NewHarmlessCol"]


def test_header_rename_to_legacy_names_still_parses(tmp_path: Path) -> None:
    legacy = ["SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL",
              "TIMESTAMP", "TOTALTRADES", "ISIN"]
    rows = [{"SYMBOL": f"S{i}", "SERIES": "EQ", "OPEN": "1", "HIGH": "2", "LOW": "1", "CLOSE": "1.5",
             "TOTTRDQTY": "5", "TIMESTAMP": "29-SEP-2026", "ISIN": f"INE{i:06d}01010"} for i in range(600)]
    p = _zip(tmp_path, "legacy.csv.zip", rows, header=legacy)
    assert SC.check_bhavcopy(p, date(2026, 9, 29))["date_column"] == "TIMESTAMP"


def test_duplicate_symbol_and_bad_prices_rejected(tmp_path: Path) -> None:
    rows = _rows("2026-09-29"); rows.append(dict(rows[0]))
    with pytest.raises(SC.SourceContractError, match="SOURCE_DUPLICATE_SYMBOL"):
        SC.check_bhavcopy(_zip(tmp_path, "d.csv.zip", rows), date(2026, 9, 29))
    rows = _rows("2026-09-29")
    for r in rows[:20]:
        r["ClsPric"] = "0"
    with pytest.raises(SC.SourceContractError, match="SOURCE_BAD_PRICES"):
        SC.check_bhavcopy(_zip(tmp_path, "e.csv.zip", rows), date(2026, 9, 29))


# ------------------------------------------------------------------------------------------ members
def _members(extra: list[dict] | None = None) -> list[dict]:
    base = [{"Company Name": f"C{i}", "Industry": "X", "Symbol": f"STK{i:04d}", "Series": "EQ",
             "ISIN Code": f"INE{i:06d}01010"} for i in range(5)]
    return base + (extra or [])


def test_classification_rr_known_non_equity_xx_unknown() -> None:
    c = SC.classify_official_members(_members([
        {"Company Name": "EMBASSY OFFICE PARKS REIT", "Symbol": "EMBASSY", "Series": "RR", "ISIN Code": "INE041025011"},
        {"Company Name": "Some InvIT", "Symbol": "INVX", "Series": "IV", "ISIN Code": "INE0XX025011"},
        {"Company Name": "Mystery", "Symbol": "MYST", "Series": "XX", "ISIN Code": "INE0YY025011"}]))
    assert c["official"] == 8 and len(c["equity"]) == 5
    assert {m["Symbol"]: (m["AssetType"], m["VajraEquityEligible"], m["ExclusionReason"]) for m in c["non_equity"]} == {
        "EMBASSY": ("REIT", False, "NON_EQUITY_SERIES_REIT"), "INVX": ("INVIT", False, "NON_EQUITY_SERIES_INVIT")}
    assert [m["Symbol"] for m in c["unknown"]] == ["MYST"]


def test_membership_diff_reports_additions_removals_series_isin_and_category() -> None:
    prior = _members()
    today = [dict(m) for m in prior[1:]]                       # STK0000 removed
    today[0]["ISIN Code"] = "INE999999999"                     # STK0001 ISIN change (symbol rename/ISIN)
    today[1]["Series"] = "BE"                                  # STK0002 series change EQ -> BE (equity)
    today.append({"Symbol": "EMBASSY", "Series": "RR", "ISIN Code": "INE041025011"})
    today.append({"Symbol": "MYST", "Series": "XX", "ISIN Code": "INE0YY025011"})
    d = SC.membership_diff(prior, today)
    assert d["added"] == ["EMBASSY", "MYST"] and d["removed"] == ["STK0000"]
    assert d["isin_changed"][0]["Symbol"] == "STK0001" and d["series_changed"][0]["to"] == "BE"
    assert {c["Symbol"]: c["class"] for c in d["asset_category_changes"]} == {"EMBASSY": "REIT", "MYST": "UNKNOWN"}
    assert d["review_needed"] is True


# ------------------------------------------------------------------------------------------ catch-up semantics
def _data_root(tmp: Path, members: list[dict]) -> tuple[Path, Path]:
    root = tmp / "pit"
    raw = root / "08 Parquet" / "raw" / "year=2026"
    raw.mkdir(parents=True)
    cols = ["Date", "Symbol", "MembershipSymbol", "ISIN", "ExchangeISIN", "Series", "Open", "High", "Low",
            "Close", "PrevClose", "Volume", "Turnover", "TotalTrades", "SourceFormat", "SourceArchive",
            "SourceSha256", "SourceMember", "MembershipConfidence", "MembershipEvidence", "FoundationVersion",
            "IdentityStatus"]
    seed = pd.DataFrame([{c: None for c in cols} | {"Date": date(2026, 9, 28), "Symbol": "STK0000",
                         "MembershipSymbol": "STK0000", "Open": 1.0, "High": 2.0, "Low": 1.0, "Close": 1.5,
                         "Volume": 1}])
    seed.to_parquet(raw / "nifty500_raw_daily.parquet", index=False)
    (root / "10 Provenance").mkdir(parents=True)
    (root / "11 Logs").mkdir(parents=True)
    snap = tmp / "ind_nifty500list.csv"
    pd.DataFrame(members).to_csv(snap, index=False)
    return root, snap


REIT_MEMBER = {"Company Name": "EMBASSY OFFICE PARKS REIT", "Industry": "Realty", "Symbol": "EMBASSY",
               "Series": "RR", "ISIN Code": "INE041025011"}


def test_official_non_equity_member_is_not_a_missing_equity_bar_and_is_recorded(tmp_path: Path) -> None:
    members = [{"Company Name": f"C{i}", "Industry": "X", "Symbol": f"STK{i:04d}", "Series": "EQ",
                "ISIN Code": f"INE{i:06d}01010"} for i in range(600)] + [REIT_MEMBER]
    root, snap = _data_root(tmp_path, members)
    z = _zip(tmp_path, "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip", _rows("2026-09-29", extra=[REIT]))
    out = IC._append_missing_raw_sessions(root, sessions=[date(2026, 9, 29)], source_paths={date(2026, 9, 29): z},
                                          snapshot=snap)
    assert out["new_missing_rows"] == []                                   # no false failure
    cov = out["coverage"][0]
    assert (cov["official_members"], cov["equity_eligible"], cov["non_equity_excluded"],
            cov["missing_equity_bars"], cov["equity_bars_present"]) == (601, 600, 1, 0, 600)   # no equity stock lost
    ne = out["non_equity_member_rows"][0]
    assert (ne["Symbol"], ne["OfficialMember"], ne["VajraEquityEligible"], ne["BhavcopySeries"]) == (
        "EMBASSY", True, False, "RR")
    raw = pd.read_parquet(root / "08 Parquet" / "raw" / "year=2026" / "nifty500_raw_daily.parquet")
    assert "EMBASSY" not in set(raw["MembershipSymbol"])                  # never an equity bar
    rec = IC._record_non_equity_members(root, out["non_equity_member_rows"])
    rec2 = IC._record_non_equity_members(root, out["non_equity_member_rows"])   # idempotent
    m = pd.read_csv(root / "09 Validation" / "nifty500_official_raw_missing_member_rows.csv", dtype=str)
    assert len(m) == 1 and m.iloc[0]["Reason"].startswith("OFFICIAL_NON_EQUITY_MEMBER_RR_REIT")
    assert m.iloc[0]["MembershipConfidence"] == "VERIFIED_OFFICIAL_CURRENT"   # official membership kept
    reg = pd.read_csv(root / "03 Security Master" / "nifty500_non_equity_member_registry.csv", dtype=str)
    assert reg.iloc[0]["VajraEquityEligible"] == "False" and reg.iloc[0]["OfficialMember"] == "True"
    assert rec["rows"] == rec2["rows"] == 1


def test_truly_missing_equity_member_still_fails_and_unknown_series_fails_closed(tmp_path: Path) -> None:
    members = [{"Company Name": f"C{i}", "Industry": "X", "Symbol": f"STK{i:04d}", "Series": "EQ",
                "ISIN Code": f"INE{i:06d}01010"} for i in range(601)]          # STK0600 has no bar
    root, snap = _data_root(tmp_path, members)
    z = _zip(tmp_path, "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip", _rows("2026-09-29"))
    out = IC._append_missing_raw_sessions(root, sessions=[date(2026, 9, 29)], source_paths={date(2026, 9, 29): z},
                                          snapshot=snap)
    assert [m["Symbol"] for m in out["new_missing_rows"]] == ["STK0600"]
    root2, snap2 = _data_root(tmp_path / "u", members[:600] + [{"Company Name": "M", "Industry": "X",
                              "Symbol": "MYST", "Series": "XX", "ISIN Code": "INE0YY025011"}])
    with pytest.raises(SC.SourceContractError, match="UNKNOWN_CONSTITUENT_SERIES"):
        IC._append_missing_raw_sessions(root2, sessions=[date(2026, 9, 29)],
                                        source_paths={date(2026, 9, 29): z}, snapshot=snap2)


def test_canary_rejects_wrong_date_file_inside_catchup(tmp_path: Path) -> None:
    members = [{"Company Name": "C", "Industry": "X", "Symbol": f"STK{i:04d}", "Series": "EQ",
                "ISIN Code": f"INE{i:06d}01010"} for i in range(600)]
    root, snap = _data_root(tmp_path, members)
    z = _zip(tmp_path, "BhavCopy_NSE_CM_0_0_0_20260929_F_0000.csv.zip", _rows("2026-09-26"))
    with pytest.raises(SC.SourceContractError, match="SOURCE_WRONG_DATE"):
        IC._append_missing_raw_sessions(root, sessions=[date(2026, 9, 29)], source_paths={date(2026, 9, 29): z},
                                        snapshot=snap)
