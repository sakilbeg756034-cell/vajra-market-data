from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from vajra_regime import paths
from vajra_regime.checkpoint import (
    atomic_json,
    canonical_hash,
    sha256_file,
    write_phase_checkpoint,
)
from vajra_regime.nifty500_migration.certified_adjusted import (
    _build_relisting_intervals,
    _build_year,
)
from vajra_regime.nifty500_migration.constants import CHECKPOINT_ROOT, DATA_ROOT, FOUNDATION_VERSION
from vajra_regime.nifty500_migration.corporate_action_archive import (
    archive_official_corporate_actions,
)
from vajra_regime.nifty500_migration.corporate_action_reconciliation import (
    build_corporate_action_reconciliation,
)
from vajra_regime.nifty500_migration.foundation_certification import (
    build_foundation_certification,
)
from vajra_regime.nifty500_migration.raw_ohlcv import parse_official_bhavcopy
from vajra_regime.nifty500_migration.source_archive import CURRENT_CONSTITUENTS, _download
from vajra_regime.nifty500_migration.timeline import build_point_in_time_membership


# Year-agnostic: see the note in raw_ohlcv.py.
LIVE_UDIFF_ROOT = paths.LIVE_UDIFF_ROOT
LIVE_UDIFF_GLOB = "*/BhavCopy_NSE_CM_0_0_0_*_F_0000.csv.zip"
LIVE_VALIDATION_ROOT = paths.LIVE_VALIDATION_ROOT
MASTER_DB = paths.MASTER_DB


def _session_from_zip(path: Path) -> date:
    return datetime.strptime(path.name.split("_")[6], "%Y%m%d").date()


def discover_local_catchup_sessions(last_clean: date, *, today: date) -> dict[str, Any]:
    available = {
        _session_from_zip(path): path
        for path in LIVE_UDIFF_ROOT.glob(LIVE_UDIFF_GLOB)
        if last_clean < _session_from_zip(path) < today
    }
    if not available:
        return {
            "latest_available": last_clean,
            "expected_sessions": [],
            "available_sessions": [],
            "missing_source_sessions": [],
            "source_paths": {},
        }
    latest = max(available)
    with duckdb.connect(str(MASTER_DB), read_only=True) as connection:
        expected = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT Date FROM clean_daily WHERE Date > ? AND Date <= ? ORDER BY Date",
                [last_clean, latest],
            ).fetchall()
        ]
    missing = [session for session in expected if session not in available]
    return {
        "latest_available": latest,
        "expected_sessions": expected,
        "available_sessions": sorted(available),
        "missing_source_sessions": missing,
        "source_paths": available,
    }


def _refresh_current_snapshot(data_root: Path, *, as_of: date) -> dict[str, Any]:
    current_dir = data_root / "01 Raw Source Archives" / "Official Current Constituents"
    current_dir.mkdir(parents=True, exist_ok=True)
    dated = current_dir / f"{as_of.isoformat()}_ind_nifty500list.csv"
    active = current_dir / "ind_nifty500list.csv"
    record = _download(CURRENT_CONSTITUENTS, dated)
    if record["status"] == "FAILED":
        if not active.exists():
            raise RuntimeError(f"Official current Nifty500 snapshot unavailable: {record['error']}")
        dated = active
        record = {
            **record,
            "status": "NETWORK_FAILED_REUSED_HASH_VALID_PRIOR_OFFICIAL_SNAPSHOT",
            "path": str(active),
            "sha256": sha256_file(active),
        }
    frame = pd.read_csv(dated)
    required = {"Company Name", "Industry", "Symbol", "Series", "ISIN Code"}
    if not required.issubset(frame.columns):
        raise RuntimeError(f"Official constituent schema changed: {sorted(frame.columns)}")
    if len(frame) != 500 or frame["Symbol"].nunique() != 500 or frame["ISIN Code"].nunique() != 500:
        raise RuntimeError("Official current Nifty500 snapshot failed exact 500-member identity gate")
    prior_hash = sha256_file(active) if active.exists() else None
    downloaded_hash = sha256_file(dated)
    if not active.exists() or prior_hash != downloaded_hash:
        if active.exists():
            preserved = current_dir / "Snapshots" / f"pre_{as_of.isoformat()}_{prior_hash[:12]}.csv"
            preserved.parent.mkdir(parents=True, exist_ok=True)
            if not preserved.exists():
                shutil.copy2(active, preserved)
        temporary = active.with_name(f".{active.name}.{uuid4().hex}.partial")
        shutil.copy2(dated, temporary)
        os.replace(temporary, active)
    result = {
        "status": record["status"],
        "source_url": CURRENT_CONSTITUENTS,
        "as_of": as_of.isoformat(),
        "members": 500,
        "dated_snapshot_path": str(dated),
        "dated_snapshot_sha256": downloaded_hash,
        "active_snapshot_path": str(active),
        "active_snapshot_sha256": sha256_file(active),
        "prior_active_sha256": prior_hash,
        "membership_changed_since_prior_snapshot": bool(prior_hash and prior_hash != downloaded_hash),
    }
    result["payload_sha256"] = canonical_hash(result)
    atomic_json(data_root / "10 Provenance" / f"current_constituent_snapshot_{as_of}.json", result)
    return result


def _number(value: str, *, integer: bool = False) -> float | int | None:
    if value in {"", "-"}:
        return None
    parsed = float(value)
    return int(parsed) if integer else parsed


def _append_missing_raw_sessions(
    data_root: Path,
    *,
    sessions: list[date],
    source_paths: dict[date, Path],
    snapshot: Path,
) -> dict[str, Any]:
    raw_path = data_root / "08 Parquet" / "raw" / "year=2026" / "nifty500_raw_daily.parquet"
    existing = pd.read_parquet(raw_path)
    existing["Date"] = pd.to_datetime(existing["Date"]).dt.date
    members = pd.read_csv(snapshot, dtype=str).fillna("")
    member_map = {
        str(row["Symbol"]).strip().upper(): row for row in members.to_dict(orient="records")
    }
    rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, str]] = []
    source_manifest: list[dict[str, Any]] = []
    for session in sessions:
        path = source_paths[session]
        source_hash = sha256_file(path)
        parsed = {row["Symbol"]: row for row in parse_official_bhavcopy(path, session)}
        source_manifest.append(
            {
                "session": session.isoformat(),
                "path": str(path),
                "sha256": source_hash,
                "bytes": path.stat().st_size,
            }
        )
        for symbol, member in member_map.items():
            exchange = parsed.get(symbol)
            if exchange is None:
                missing_rows.append(
                    {
                        "Date": session.isoformat(),
                        "Symbol": symbol,
                        "ExpectedISIN": member["ISIN Code"],
                        "Reason": "OFFICIAL_CURRENT_MEMBER_ABSENT_FROM_OFFICIAL_BHAVCOPY",
                        "SourceArchive": path.name,
                    }
                )
                continue
            expected_isin = member["ISIN Code"]
            exchange_isin = exchange["ISIN"]
            rows.append(
                {
                    "Date": session,
                    "Symbol": exchange["Symbol"],
                    "MembershipSymbol": symbol,
                    "ISIN": expected_isin or exchange_isin,
                    "ExchangeISIN": exchange_isin or None,
                    "Series": exchange["Series"],
                    "Open": _number(exchange["Open"]),
                    "High": _number(exchange["High"]),
                    "Low": _number(exchange["Low"]),
                    "Close": _number(exchange["Close"]),
                    "PrevClose": _number(exchange["PrevClose"]),
                    "Volume": _number(exchange["Volume"], integer=True),
                    "Turnover": _number(exchange["Turnover"]),
                    "TotalTrades": _number(exchange["TotalTrades"], integer=True),
                    "SourceFormat": exchange["SourceFormat"],
                    "SourceArchive": path.name,
                    "SourceSha256": source_hash,
                    "SourceMember": exchange["SourceMember"],
                    "MembershipConfidence": "VERIFIED_OFFICIAL_CURRENT",
                    "MembershipEvidence": snapshot.name,
                    "FoundationVersion": FOUNDATION_VERSION,
                    "IdentityStatus": (
                        "OFFICIAL_BHAVCOPY_ISIN_SYMBOL_MATCH"
                        if expected_isin == exchange_isin
                        else "OFFICIAL_MEMBERSHIP_VS_BHAVCOPY_ISIN_MISMATCH"
                    ),
                }
            )
    new = pd.DataFrame(rows, columns=existing.columns)
    combined = pd.concat(
        [existing.loc[~existing["Date"].isin(sessions)], new], ignore_index=True
    ).sort_values(["Date", "MembershipSymbol"])
    invalid = (
        (combined["Open"] <= 0)
        | (combined["High"] < combined[["Open", "Close"]].max(axis=1))
        | (combined["Low"] > combined[["Open", "Close"]].min(axis=1))
        | (combined["Volume"] < 0)
    )
    duplicate_count = int(combined.duplicated(["Date", "MembershipSymbol"]).sum())
    if duplicate_count or bool(invalid.any()):
        raise RuntimeError(
            f"Incremental raw validation failed: duplicates={duplicate_count}, invalid={int(invalid.sum())}"
        )
    temporary = raw_path.with_name(f".{raw_path.name}.{uuid4().hex}.partial")
    combined.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, raw_path)
    manifest_path = data_root / "10 Provenance" / "nifty500_incremental_bhavcopy_manifest.csv"
    prior_manifest = pd.read_csv(manifest_path) if manifest_path.exists() else pd.DataFrame()
    manifest = pd.concat([prior_manifest, pd.DataFrame(source_manifest)], ignore_index=True)
    if not manifest.empty:
        manifest = manifest.drop_duplicates("session", keep="last").sort_values("session")
    temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.{uuid4().hex}.partial")
    manifest.to_csv(temporary_manifest, index=False)
    os.replace(temporary_manifest, manifest_path)
    return {
        "appended_sessions": [session.isoformat() for session in sessions],
        "appended_rows": len(rows),
        "new_missing_rows": missing_rows,
        "raw_2026_path": str(raw_path),
        "raw_2026_sha256": sha256_file(raw_path),
        "raw_2026_rows": len(combined),
        "raw_2026_sessions": int(combined["Date"].nunique()),
        "latest_date": str(max(combined["Date"])),
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
    }


def _update_raw_status(data_root: Path, incremental: dict[str, Any]) -> dict[str, Any]:
    status_path = data_root / "11 Logs" / "official_raw_ohlcv_build_status.json"
    prior = json.loads(status_path.read_text(encoding="utf-8"))
    paths = sorted((data_root / "08 Parquet" / "raw").glob("year=*/nifty500_raw_daily.parquet"))
    with duckdb.connect() as connection:
        total = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                   SUM(ISIN IS NULL), MIN(Date), MAX(Date)
            FROM read_parquet(?)
            """,
            [[str(path) for path in paths]],
        ).fetchone()
        current = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT Symbol), COUNT(DISTINCT ISIN),
                   SUM(ISIN IS NULL),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol),
                   SUM(Open <= 0), SUM(High < GREATEST(Open, Close) OR Low > LEAST(Open, Close)),
                   SUM(Volume < 0)
            FROM read_parquet(?)
            """,
            [incremental["raw_2026_path"]],
        ).fetchone()
    yearly = list(prior["yearly_status"])
    old_2026 = next(row for row in yearly if int(row["year"]) == 2026)
    updated_2026 = {
        **old_2026,
        "status": "COMPLETE_INCREMENTAL_HASH_VERIFIED",
        "input_fingerprint_sha256": canonical_hash(incremental),
        "output_sha256": incremental["raw_2026_sha256"],
        "rows": current[0],
        "sessions": current[1],
        "symbols": current[2],
        "isins": current[3],
        "unresolved_isin_rows": current[4],
        "duplicate_date_symbol_rows": current[5],
        "invalid_price_rows": current[6],
        "invalid_high_low_rows": current[7],
        "negative_volume_rows": current[8],
        "recorded_at_utc": datetime.now(UTC).isoformat(),
    }
    updated_2026["checkpoint_fingerprint_sha256"] = canonical_hash(updated_2026)
    yearly = [updated_2026 if int(row["year"]) == 2026 else row for row in yearly]
    status = {
        **prior,
        "status": "COMPLETE_INCREMENTAL_HASH_VERIFIED",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_sessions": total[1],
        "rows": total[0],
        "sessions": total[1],
        "symbols": total[2],
        "isins": total[3],
        "unresolved_isin_rows": total[4],
        "earliest_date": str(total[5]),
        "latest_date": str(total[6]),
        "yearly_status": yearly,
        "incremental_manifest_sha256": incremental["manifest_sha256"],
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(status_path, status)
    atomic_json(
        data_root / "12 Checkpoints" / "raw_ohlcv_years" / "raw_ohlcv_2026.json",
        updated_2026,
    )
    return status


def _refresh_adjusted_2026(data_root: Path) -> dict[str, Any]:
    raw_paths = sorted((data_root / "08 Parquet" / "raw").glob("year=*/nifty500_raw_daily.parquet"))
    raw_2026 = next(path for path in raw_paths if path.parent.name == "year=2026")
    raw_2025 = next(path for path in raw_paths if path.parent.name == "year=2025")
    eod2 = data_root / "08 Parquet" / "secondary_adjusted_cache" / "eod2_relevant_adjusted_daily.parquet"
    reconciliation = data_root / "04 Corporate Actions" / "nifty500_corporate_action_reconciliation.parquet"
    old_outputs = sorted(
        (data_root / "08 Parquet" / "certified_adjusted").glob(
            "year=*/nifty500_adjusted_daily.parquet"
        )
    )
    immutable_pre2026 = {
        path.parent.name: sha256_file(path)
        for path in old_outputs
        if path.parent.name != "year=2026"
    }
    relisting, relisting_status = _build_relisting_intervals(data_root, raw_paths)
    output = (
        data_root
        / "08 Parquet"
        / "certified_adjusted"
        / "year=2026"
        / "nifty500_adjusted_daily.parquet"
    )
    _build_year(
        raw_path=raw_2026,
        prior_raw_path=raw_2025,
        eod2_path=eod2,
        reconciliation_path=reconciliation,
        relisting_path=relisting,
        output_path=output,
        year=2026,
    )
    outputs = sorted(
        (data_root / "08 Parquet" / "certified_adjusted").glob(
            "year=*/nifty500_adjusted_daily.parquet"
        )
    )
    unchanged = {
        key: immutable_pre2026[key]
        == sha256_file(next(path for path in outputs if path.parent.name == key))
        for key in immutable_pre2026
    }
    with duckdb.connect() as connection:
        metrics = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol),
                   SUM(Open <= 0 OR High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) OR Volume < 0),
                   SUM(CorporateActionQuarantineFlag), SUM(UnresolvedAdjustmentDiscontinuityFlag),
                   SUM(ExtremeOfficialMarketMoveFlag), SUM(IsResearchEligible)
            FROM read_parquet(?)
            """,
            [str(output)],
        ).fetchone()
        total = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT Date), COUNT(DISTINCT ISIN), MIN(Date), MAX(Date),
                   SUM(CorporateActionQuarantineFlag), SUM(UnresolvedAdjustmentDiscontinuityFlag),
                   SUM(ExtremeOfficialMarketMoveFlag), SUM(IsResearchEligible),
                   SUM(Open <= 0 OR High < GREATEST(Open, Close) OR Low > LEAST(Open, Close) OR Volume < 0),
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || MembershipSymbol)
            FROM read_parquet(?)
            """,
            [[str(path) for path in outputs]],
        ).fetchone()
        source_counts = dict(
            connection.execute(
                "SELECT AdjustmentSource, COUNT(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                [[str(path) for path in outputs]],
            ).fetchall()
        )
    if not all(unchanged.values()):
        raise RuntimeError("A pre-2026 adjusted partition changed during incremental catch-up")
    if total[6] or total[9] or total[10]:
        raise RuntimeError("Incremental adjusted certification hard gate failed")
    checkpoint = {
        "year": 2026,
        "status": "COMPLETE_INCREMENTAL",
        "input_fingerprint_sha256": canonical_hash(
            {
                "raw_sha256": sha256_file(raw_2026),
                "eod2_sha256": sha256_file(eod2),
                "reconciliation_sha256": sha256_file(reconciliation),
                "relisting_sha256": relisting_status["sha256"],
            }
        ),
        "output_path": str(output),
        "output_sha256": sha256_file(output),
        "rows": metrics[0],
        "sessions": metrics[1],
        "isins": metrics[2],
        "duplicate_date_membership_symbol_rows": metrics[3],
        "invalid_bar_rows": metrics[4],
        "quarantine_rows": metrics[5],
        "unresolved_adjustment_discontinuity_rows": metrics[6],
        "extreme_official_market_move_rows": metrics[7],
        "research_eligible_rows": metrics[8],
        "recorded_at_utc": datetime.now(UTC).isoformat(),
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(
        data_root / "12 Checkpoints" / "certified_adjusted_years" / "certified_adjusted_2026.json",
        checkpoint,
    )
    prior_status_path = data_root / "11 Logs" / "certified_adjusted_build_status.json"
    prior = json.loads(prior_status_path.read_text(encoding="utf-8"))
    yearly = [checkpoint if int(row["year"]) == 2026 else row for row in prior["yearly_status"]]
    status = {
        **prior,
        "status": "CERTIFIED_PASS_WITH_DOCUMENTED_QUARANTINE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": total[0],
        "sessions": total[1],
        "isins": total[2],
        "earliest_date": str(total[3]),
        "latest_date": str(total[4]),
        "quarantine_rows": total[5],
        "unresolved_adjustment_discontinuity_rows": total[6],
        "extreme_official_market_move_rows": total[7],
        "research_eligible_rows": total[8],
        "invalid_bar_rows": total[9],
        "duplicate_date_membership_symbol_rows": total[10],
        "adjustment_source_counts": source_counts,
        "relisting_audit": relisting_status,
        "corporate_action_reconciliation_sha256": sha256_file(reconciliation),
        "yearly_status": yearly,
        "pre2026_partitions_unchanged": all(unchanged.values()),
        "pre2026_partition_hash_checks": unchanged,
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(prior_status_path, status)
    atomic_json(
        data_root / "12 Checkpoints" / "phase_07_certified_adjusted.json",
        {**status, "checkpoint_status": "COMPLETE_INCREMENTAL_HASH_VERIFIED"},
    )
    return status


def run_incremental_catchup(
    *, data_root: Path = DATA_ROOT, today: date | None = None
) -> dict[str, Any]:
    today = today or date.today()
    adjusted_status_path = data_root / "11 Logs" / "certified_adjusted_build_status.json"
    prior_adjusted = json.loads(adjusted_status_path.read_text(encoding="utf-8"))
    last_clean = date.fromisoformat(prior_adjusted["latest_date"])
    discovery = discover_local_catchup_sessions(last_clean, today=today)
    if discovery["missing_source_sessions"]:
        raise RuntimeError(
            "DATA_STALE / INCOMPLETE: official source gaps "
            + ",".join(str(value) for value in discovery["missing_source_sessions"])
        )
    if not discovery["expected_sessions"]:
        return {
            "status": "ALREADY_CURRENT_TO_LATEST_LOCAL_COMPLETED_NSE_SESSION",
            "last_clean": last_clean.isoformat(),
            "latest_available": str(discovery["latest_available"]),
        }
    as_of = discovery["latest_available"]
    snapshot_status = _refresh_current_snapshot(data_root, as_of=as_of)
    timeline_status = build_point_in_time_membership(data_root=data_root, as_of=as_of)
    if timeline_status["latest_date"] != as_of.isoformat():
        raise RuntimeError("PIT timeline did not reach the latest completed NSE session")
    incremental = _append_missing_raw_sessions(
        data_root,
        sessions=discovery["expected_sessions"],
        source_paths=discovery["source_paths"],
        snapshot=Path(snapshot_status["active_snapshot_path"]),
    )
    if incremental["new_missing_rows"]:
        raise RuntimeError(
            f"DATA_STALE / INCOMPLETE: {len(incremental['new_missing_rows'])} current-member bars missing"
        )
    raw_status = _update_raw_status(data_root, incremental)
    ca_archive = archive_official_corporate_actions(data_root=data_root, as_of=as_of)
    ca_reconciliation = build_corporate_action_reconciliation(data_root=data_root)
    adjusted_status = _refresh_adjusted_2026(data_root)
    foundation = build_foundation_certification(data_root=data_root, as_of=as_of)
    status: dict[str, Any] = {
        "status": "CATCHUP_COMPLETE_CERTIFIED",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "prior_last_clean": last_clean.isoformat(),
        "latest_completed_session": as_of.isoformat(),
        "sessions_caught_up": [str(value) for value in discovery["expected_sessions"]],
        "session_count": len(discovery["expected_sessions"]),
        "missing_source_sessions": [],
        "current_snapshot": snapshot_status,
        "timeline_status": timeline_status["status"],
        "raw_status": raw_status["status"],
        "raw_latest_date": raw_status["latest_date"],
        "corporate_action_chunks": ca_archive["chunks"],
        "corporate_action_events": ca_archive["events"],
        "corporate_action_review_events": ca_reconciliation["review_quarantine_events"],
        "adjusted_status": adjusted_status["status"],
        "adjusted_latest_date": adjusted_status["latest_date"],
        "foundation_status": foundation["status"],
        "foundation_latest_date": foundation["latest_date"],
        "pre2026_adjusted_partitions_unchanged": adjusted_status[
            "pre2026_partitions_unchanged"
        ],
        "input_zip_hashes": {
            str(session): sha256_file(discovery["source_paths"][session])
            for session in discovery["expected_sessions"]
        },
        "output_hashes": {
            "raw_2026": incremental["raw_2026_sha256"],
            "adjusted_2026": next(
                row["output_sha256"]
                for row in adjusted_status["yearly_status"]
                if int(row["year"]) == 2026
            ),
            "certified_membership": foundation["certified_daily_membership_sha256"],
        },
    }
    status["status_payload_sha256"] = canonical_hash(status)
    status_path = data_root / "11 Logs" / "incremental_catchup_status.json"
    atomic_json(status_path, status)
    write_phase_checkpoint(
        CHECKPOINT_ROOT / "phase_10_incremental_catchup.json",
        phase=10,
        name="Laptop-off incremental Nifty500 catch-up",
        status="COMPLETE",
        inputs={
            "prior_last_clean": last_clean.isoformat(),
            "source_zip_hashes": status["input_zip_hashes"],
        },
        outputs=status["output_hashes"],
        evidence={
            "sessions_caught_up": status["sessions_caught_up"],
            "pre2026_adjusted_partitions_unchanged": status[
                "pre2026_adjusted_partitions_unchanged"
            ],
            "foundation_status": foundation["status"],
            "status_sha256": sha256_file(status_path),
        },
    )
    return status
