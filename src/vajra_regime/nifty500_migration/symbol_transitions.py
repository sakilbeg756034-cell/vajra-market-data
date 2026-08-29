from __future__ import annotations

import csv
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.raw_ohlcv import EOD2_MAP


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def build_symbol_transitions(
    *, data_root: Path = DATA_ROOT, start: date = date(2009, 1, 1), as_of: date = date(2026, 8, 13)
) -> dict[str, Any]:
    payload = json.loads(EOD2_MAP.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for isin, history in payload["isin2hist"].items():
        entries = sorted(history, key=lambda row: (row["from_date"], row["symbol"]))
        for previous, current in zip(entries, entries[1:], strict=False):
            effective = date.fromisoformat(current["from_date"])
            old_symbol = previous["symbol"].strip().upper()
            new_symbol = current["symbol"].strip().upper()
            if old_symbol == new_symbol or not (start <= effective <= as_of):
                continue
            rows.append(
                {
                    "effective_date": effective.isoformat(),
                    "old_symbol": old_symbol,
                    "new_symbol": new_symbol,
                    "isin": isin,
                    "prior_from_date": previous["from_date"],
                    "prior_to_date": previous["to_date"],
                    "new_from_date": current["from_date"],
                    "new_to_date": current["to_date"],
                    "source": "EOD2_NSE_DERIVED_ISIN_SYMBOL_HISTORY",
                    "source_path": str(EOD2_MAP),
                    "source_sha256": sha256_file(EOD2_MAP),
                    "confidence": "VERIFIED_MULTI_SOURCE_IDENTITY_TRANSITION",
                    "membership_effect": "IDENTITY_ONLY_NO_MEMBERSHIP_COUNT_CHANGE",
                }
            )
    rows.sort(key=lambda row: (row["effective_date"], row["old_symbol"], row["new_symbol"]))
    output_path = data_root / "03 Security Master" / "nifty500_effective_symbol_transitions.csv"
    _write_csv(output_path, rows)
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "source_path": str(EOD2_MAP),
        "source_sha256": sha256_file(EOD2_MAP),
        "transition_count": len(rows),
        "earliest_transition": min((row["effective_date"] for row in rows), default=""),
        "latest_transition": max((row["effective_date"] for row in rows), default=""),
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "symbol_transition_build_status.json", status)
    return status
