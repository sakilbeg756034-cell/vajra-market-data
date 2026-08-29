from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import CHECKPOINT_ROOT, DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.source_archive import _download, _write_csv


NIFTYHISTORY_HOME = "https://niftyhistory.in/"


def _download_url(end_date: date) -> str:
    query = urlencode(
        {
            "index_type": "Nifty 500",
            "start_date": "2008-01-01",
            "end_date": end_date.isoformat(),
        }
    )
    return f"{NIFTYHISTORY_HOME}download?{query}"


def _profile_snapshot_csv(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {"effective_date", "index_type", "inclusions", "exclusions", "symbols"}
    if not rows or not required.issubset(rows[0]):
        raise RuntimeError("Secondary constituent ledger has an unexpected schema")
    member_counts = [len([symbol for symbol in row["symbols"].split(",") if symbol.strip()]) for row in rows]
    duplicate_counts = [
        count - len(set(symbol.strip().upper() for symbol in row["symbols"].split(",") if symbol.strip()))
        for row, count in zip(rows, member_counts, strict=True)
    ]
    return {
        "snapshot_rows": len(rows),
        "earliest_effective_date_as_published": min(row["effective_date"] for row in rows),
        "latest_effective_date_as_published": max(row["effective_date"] for row in rows),
        "member_count_min": min(member_counts),
        "member_count_max": max(member_counts),
        "snapshots_with_duplicate_symbols": sum(value > 0 for value in duplicate_counts),
    }


def archive_secondary_constituent_evidence(*, as_of: date = date(2026, 8, 13)) -> dict[str, object]:
    raw_dir = DATA_ROOT / "01 Raw Source Archives" / "Secondary Niftyhistory Crosscheck"
    provenance_dir = DATA_ROOT / "10 Provenance"
    logs_dir = DATA_ROOT / "11 Logs"
    for directory in (raw_dir, provenance_dir, logs_dir, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)

    homepage = _download(NIFTYHISTORY_HOME, raw_dir / "homepage.html", expected_type="text/html")
    ledger_path = raw_dir / f"nifty500_2008-01-01_to_{as_of.isoformat()}.csv"
    ledger = _download(_download_url(as_of), ledger_path, expected_type="text/csv")
    if homepage["status"] == "FAILED" or ledger["status"] == "FAILED":
        raise RuntimeError("Secondary constituent cross-check could not be archived")
    profile = _profile_snapshot_csv(ledger_path)

    records = [
        {
            **homepage,
            "title": "Niftyhistory public download landing page",
            "source_tier": "B_SECONDARY_UNVERIFIED",
            "allowed_use": "CROSSCHECK_ONLY",
        },
        {
            **ledger,
            "title": "Niftyhistory public Nifty500 snapshot ledger",
            "source_tier": "B_SECONDARY_UNVERIFIED",
            "allowed_use": "CROSSCHECK_ONLY",
        },
    ]
    manifest_path = provenance_dir / "secondary_constituent_source_manifest.csv"
    _write_csv(manifest_path, records)
    generated = datetime.now(UTC).isoformat()
    status: dict[str, object] = {
        "status": "ARCHIVED_CROSSCHECK_ONLY",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "source": NIFTYHISTORY_HOME,
        "usage_lock": "NEVER_AUTHORITATIVE_WITHOUT_OFFICIAL_EVENT_RECONCILIATION",
        "known_prevalidation_concern": (
            "Published ledger requires date and event-level reconciliation; apparent day/month swaps and "
            "truncated change lists were observed during discovery."
        ),
        "profile": profile,
        "ledger_path": str(ledger_path),
        "ledger_sha256": sha256_file(ledger_path),
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "payment_or_subscription_used": False,
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "secondary_constituent_archive_status.json", status)
    checkpoint = {
        "phase": 3,
        "name": "SECONDARY_CONSTITUENT_EVIDENCE_ARCHIVE",
        "status": status["status"],
        "recorded_at_utc": generated,
        "ledger_sha256": status["ledger_sha256"],
        "profile": profile,
        "usage_lock": status["usage_lock"],
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_03_secondary_constituent_archive.json", checkpoint)
    return status
