from __future__ import annotations

import html
import re
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.source_archive import _download, _write_csv


NSE_2008_SECURITY_MASTER_URL = "https://nsearchives.nseindia.com/content/circulars/cmpt11274.htm"


class _TableRows(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        name = tag.casefold()
        if name == "tr":
            self._row = []
        elif name in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name in {"td", "th"} and self._cell is not None and self._row is not None:
            value = " ".join(html.unescape("".join(self._cell)).replace("\xa0", " ").split())
            self._row.append(value)
            self._cell = None
        elif name == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell = None


def parse_nse_security_master_html(payload: str) -> list[dict[str, str]]:
    """Extract official NSE serial/symbol/company-name rows from the 2008 circular."""

    parser = _TableRows()
    parser.feed(payload)
    records: dict[str, dict[str, str]] = {}
    symbol_pattern = re.compile(r"^[A-Z0-9][A-Z0-9&.-]{0,29}$")
    for row in parser.rows:
        cells = [cell.strip() for cell in row if cell.strip()]
        if len(cells) < 3 or not cells[0].isdigit():
            continue
        symbol = cells[1].upper()
        company_name = cells[2]
        if not symbol_pattern.fullmatch(symbol) or len(company_name) < 3:
            continue
        records.setdefault(
            symbol,
            {
                "serial_number": cells[0],
                "symbol": symbol,
                "company_name": company_name,
                "source": "NSE_CMPT11274_2008_SECURITY_MASTER",
                "source_url": NSE_2008_SECURITY_MASTER_URL,
                "confidence": "VERIFIED_OFFICIAL",
            },
        )
    return sorted(records.values(), key=lambda item: item["symbol"])


def archive_official_2008_security_master(*, data_root: Path = DATA_ROOT) -> dict[str, Any]:
    raw_dir = data_root / "01 Raw Source Archives" / "Official Historical Security Masters"
    security_dir = data_root / "03 Security Master"
    provenance_dir = data_root / "10 Provenance"
    logs_dir = data_root / "11 Logs"
    checkpoint_dir = data_root / "12 Checkpoints"
    for directory in (raw_dir, security_dir, provenance_dir, logs_dir, checkpoint_dir):
        directory.mkdir(parents=True, exist_ok=True)

    archive_path = raw_dir / "nse_cmpt11274_2008_security_master.html"
    record = _download(NSE_2008_SECURITY_MASTER_URL, archive_path, expected_type="text/html")
    if record["status"] == "FAILED":
        raise RuntimeError(f"Official 2008 NSE security master unavailable: {record['error']}")
    payload = archive_path.read_text(encoding="cp1252", errors="replace")
    rows = parse_nse_security_master_html(payload)
    if len(rows) < 400:
        raise RuntimeError(f"Official security-master parse unexpectedly small: {len(rows)}")
    for row in rows:
        row["source_sha256"] = record["sha256"]

    output_path = security_dir / "nse_official_security_master_2008.csv"
    _write_csv(output_path, rows)
    provenance_path = provenance_dir / "official_security_master_source_manifest.csv"
    _write_csv(
        provenance_path,
        [
            {
                **record,
                "source_tier": "A_AUTHORITATIVE_NSE",
        "coverage": "NSE circular symbol/company-name reference table published in 2008",
                "use_policy": "HISTORICAL_NAME_TO_SYMBOL_RECONCILIATION_ONLY",
                "known_limitations": "Not a Nifty500 constituent list and not an ISIN history source",
                "validation_result": f"PARSED_{len(rows)}_UNIQUE_SYMBOLS",
            }
        ],
    )
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "source_url": NSE_2008_SECURITY_MASTER_URL,
        "archive_path": str(archive_path),
        "archive_sha256": record["sha256"],
        "unique_symbols": len(rows),
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
        "provenance_path": str(provenance_path),
        "provenance_sha256": sha256_file(provenance_path),
        "use_policy": "OFFICIAL_NAME_TO_SYMBOL_RECONCILIATION_ONLY",
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "official_security_master_archive_status.json", status)
    checkpoint = {
        "phase": 4,
        "name": "OFFICIAL_HISTORICAL_SECURITY_MASTER_ARCHIVE",
        "status": status["status"],
        "recorded_at_utc": generated,
        "source_sha256": status["archive_sha256"],
        "output_sha256": status["output_sha256"],
        "unique_symbols": len(rows),
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(checkpoint_dir / "phase_04_official_security_master.json", checkpoint)
    return status
