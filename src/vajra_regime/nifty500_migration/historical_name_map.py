from __future__ import annotations

import csv
import os
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from pypdf import PdfReader

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, INVALID_SYMBOL_TOKENS


LEADING_NUMBER = re.compile(r"^\s*\d+[.)]?\s+")
NON_ALNUM = re.compile(r"[^A-Z0-9]+")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9&.-]*")


def normalize_company_name(value: str) -> str:
    text = value.upper().replace("&", " AND ")
    text = re.sub(r"\bLIMITED\b", " LTD ", text)
    text = re.sub(r"\bCORPORATION\b", " CORP ", text)
    text = re.sub(r"\bCOMPANY\b", " CO ", text)
    text = re.sub(r"\bINCORPORATED\b", " INC ", text)
    return NON_ALNUM.sub("", text)


def company_name_aliases(value: str) -> set[str]:
    normalized = normalize_company_name(value)
    aliases = {normalized}
    aliases.add(re.sub(r"\bLIMITED\b", "LTD", NON_ALNUM.sub(" ", value.upper())).replace(" ", ""))
    aliases.add(re.sub(r"(?:PVT)?LTD$", "", normalized))
    aliases.add(re.sub(r"(?:PVT)?LIMITED$", "", normalized))
    aliases.add(normalized.replace("CORPORATION", "CORP"))
    aliases.add(normalized.replace("CORP", "CORPORATION"))
    return {alias for alias in aliases if len(alias) >= 4}


def _add(
    rows: list[dict[str, str]],
    *,
    company_name: str,
    symbol: str,
    source: str,
    source_grade: str,
) -> None:
    normalized = normalize_company_name(company_name)
    cleaned_symbol = symbol.strip().upper()
    if len(normalized) < 4 or not cleaned_symbol or cleaned_symbol in INVALID_SYMBOL_TOKENS:
        return
    for alias in company_name_aliases(company_name):
        rows.append(
            {
                "normalized_company_name": alias,
                "company_name": " ".join(company_name.split()),
                "symbol": cleaned_symbol,
                "source": source,
                "source_grade": source_grade,
            }
        )


def extract_name_symbol_from_layout_line(line: str, known_symbols: set[str]) -> tuple[str, str] | None:
    matches = list(TOKEN.finditer(line))
    blocked = {"CO", "CORP", "INDIA", "LIMITED", "LTD", "NAME", "NO", "SR"}
    candidates = [
        match
        for match in matches
        if match.group() == match.group().upper()
        and any(character.isalpha() for character in match.group())
        and match.group().strip(".").upper() not in blocked
        and match.group().strip(".").upper() not in INVALID_SYMBOL_TOKENS
        and (
            match.group().upper() in known_symbols
            or (bool(re.match(r"^\s*\d{1,3}\s+", line)) and len(match.group().strip(".")) >= 3)
        )
    ]
    if not candidates:
        return None
    already_known = [match for match in candidates if match.group().upper() in known_symbols]
    symbol_match = already_known[-1] if already_known else candidates[-1]
    symbol = symbol_match.group().upper()
    company = LEADING_NUMBER.sub("", line[: symbol_match.start()]).strip(" -:;,.")
    if len(company.split()) < 2 or normalize_company_name(company) in {
        "COMPANYNAME",
        "SECURITYNAME",
        "SYMBOLCOMPANYNAME",
    }:
        return None
    return " ".join(company.split()), symbol


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def extract_official_press_layout_name_map(*, data_root: Path = DATA_ROOT) -> dict[str, Any]:
    from vajra_regime.nifty500_migration.membership_event_discovery import _known_symbols

    manifest_path = data_root / "10 Provenance" / "nifty500_relevant_press_release_manifest.csv"
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        manifest = list(csv.DictReader(handle))
    known_symbols = _known_symbols(include_layout=False)
    output_rows: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    for index, item in enumerate(manifest, start=1):
        pdf_path = Path(item["path"])
        try:
            reader = PdfReader(pdf_path, strict=False)
            for page in reader.pages:
                for line in (page.extract_text(extraction_mode="layout") or "").splitlines():
                    extracted = extract_name_symbol_from_layout_line(line, known_symbols)
                    if extracted is None:
                        continue
                    company_name, symbol = extracted
                    output_rows.append(
                        {
                            "normalized_company_name": normalize_company_name(company_name),
                            "company_name": company_name,
                            "symbol": symbol,
                            "source_file": pdf_path.name,
                            "source_sha256": item["sha256"],
                            "source_grade": "A_OFFICIAL_NSE_INDICES_PRESS_RELEASE_LAYOUT",
                        }
                    )
        except Exception as exc:
            failures.append({"source_file": pdf_path.name, "error": f"{type(exc).__name__}: {exc}"})
        if index % 50 == 0:
            print(f"Official layout name map: {index}/{len(manifest)} documents", flush=True)
    deduplicated = sorted(
        {
            (row["normalized_company_name"], row["symbol"], row["source_file"]): row
            for row in output_rows
            if len(row["normalized_company_name"]) >= 4
        }.values(),
        key=lambda row: (row["normalized_company_name"], row["symbol"], row["source_file"]),
    )
    output_path = data_root / "03 Security Master" / "official_press_release_name_symbol_rows.csv"
    _write_csv(output_path, deduplicated)
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE_WITH_DOCUMENT_FAILURES" if failures else "COMPLETE",
        "generated_at_utc": generated,
        "documents_inspected": len(manifest),
        "name_symbol_rows": len(deduplicated),
        "unique_normalized_names": len({row["normalized_company_name"] for row in deduplicated}),
        "unique_symbols": len({row["symbol"] for row in deduplicated}),
        "document_failures": failures,
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "official_press_layout_name_map_status.json", status)
    return status


def build_official_name_symbol_rows(*, data_root: Path = DATA_ROOT) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    security_2008 = data_root / "03 Security Master" / "nse_official_security_master_2008.csv"
    if security_2008.exists():
        with security_2008.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                _add(
                    rows,
                    company_name=row["company_name"],
                    symbol=row["symbol"],
                    source=Path(row.get("source", "nse_official_security_master_2008")).name,
                    source_grade="A_OFFICIAL_NSE_2008_REFERENCE",
                )

    current = data_root / "01 Raw Source Archives" / "Official Current Constituents" / "ind_nifty500list.csv"
    with current.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            _add(
                rows,
                company_name=row["Company Name"],
                symbol=row["Symbol"],
                source=current.name,
                source_grade="A_OFFICIAL_CURRENT_NIFTY500",
            )

    monthly = (
        data_root
        / "02 Constituent History"
        / "Official Monthly Snapshots"
        / "nifty500_official_monthly_members.csv"
    )
    with monthly.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("company_name"):
                _add(
                    rows,
                    company_name=row["company_name"],
                    symbol=row["symbol"],
                    source=row["source_archive"],
                    source_grade="A_OFFICIAL_MONTHLY_NIFTY500",
                )

    layout_map = data_root / "03 Security Master" / "official_press_release_name_symbol_rows.csv"
    if layout_map.exists():
        with layout_map.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                _add(
                    rows,
                    company_name=row["company_name"],
                    symbol=row["symbol"],
                    source=row["source_file"],
                    source_grade=row["source_grade"],
                )

    known_symbols = {row["symbol"] for row in rows}
    text_dir = data_root / "02 Constituent History" / "Official Press Release Text"
    for path in sorted(text_dir.glob("*.txt")):
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            cleaned = " ".join(raw_line.replace("\xa0", " ").split())
            if " " not in cleaned:
                continue
            token = cleaned.rsplit(" ", 1)[-1].strip("()[],:;").upper()
            if token not in known_symbols:
                continue
            company = LEADING_NUMBER.sub("", cleaned[: cleaned.rfind(" ")]).strip(" -:;,.")
            if len(company.split()) < 2 or "COMPANY NAME" in company.upper():
                continue
            _add(
                rows,
                company_name=company,
                symbol=token,
                source=path.name,
                source_grade="A_OFFICIAL_PRESS_RELEASE_NAME_SYMBOL_ROW",
            )
    rows.sort(key=lambda row: (row["normalized_company_name"], row["symbol"], row["source"]))
    return rows


def unique_name_symbol_map(rows: Iterable[dict[str, str]]) -> tuple[dict[str, str], dict[str, set[str]]]:
    candidates: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        candidates[row["normalized_company_name"]].add(row["symbol"])
    resolved = {name: next(iter(symbols)) for name, symbols in candidates.items() if len(symbols) == 1}
    conflicts = {name: symbols for name, symbols in candidates.items() if len(symbols) > 1}
    return resolved, conflicts
