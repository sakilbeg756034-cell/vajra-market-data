from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime import paths
from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


MASTER_DB = paths.MASTER_DB


def _split(value: str) -> set[str]:
    return {token.strip().upper() for token in str(value).split(",") if token.strip()}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _load_events(data_root: Path, *, start: date, as_of: date) -> dict[date, list[dict[str, Any]]]:
    path = data_root / "02 Constituent History" / "nifty500_official_membership_event_ledger_v1.csv"
    grouped: dict[date, list[dict[str, Any]]] = defaultdict(list)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            effective = date.fromisoformat(row["effective_date"])
            if start <= effective <= as_of:
                row["exclusion_set"] = _split(row["exclusions"])
                row["inclusion_set"] = _split(row["inclusions"])
                row["balanced"] = row["balanced_count"].casefold() == "true"
                grouped[effective].append(row)
    transition_path = data_root / "03 Security Master" / "nifty500_effective_symbol_transitions.csv"
    if transition_path.exists():
        with transition_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                effective = date.fromisoformat(row["effective_date"])
                if not (start <= effective <= as_of):
                    continue
                grouped[effective].insert(
                    0,
                    {
                        "effective_date": row["effective_date"],
                        "source_file": Path(row["source_path"]).name,
                        "source_sha256": row["source_sha256"],
                        "exclusion_set": {row["old_symbol"]},
                        "inclusion_set": {row["new_symbol"]},
                        "balanced": True,
                        "confidence": row["confidence"],
                        "source_method": "EFFECTIVE_DATED_IDENTITY_TRANSITION",
                        "conditional_identity_transition": True,
                    },
                )
    return grouped


def _load_official_anchors(data_root: Path, *, as_of: date) -> dict[date, dict[str, Any]]:
    path = (
        data_root
        / "02 Constituent History"
        / "Official Monthly Snapshots"
        / "nifty500_official_monthly_members.csv"
    )
    grouped: dict[date, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            snapshot = date.fromisoformat(row["snapshot_date"])
            grouped.setdefault(
                snapshot,
                {
                    "members": set(),
                    "source": row["source_archive"],
                    "source_sha256": row["source_archive_sha256"],
                    "grade": "VERIFIED_OFFICIAL",
                },
            )["members"].add(row["symbol"].strip().upper())

    current_path = data_root / "01 Raw Source Archives" / "Official Current Constituents" / "ind_nifty500list.csv"
    current_members: set[str] = set()
    with current_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            current_members.add(row["Symbol"].strip().upper())
    grouped[as_of] = {
        "members": current_members,
        "source": current_path.name,
        "source_sha256": sha256_file(current_path),
        "grade": "VERIFIED_OFFICIAL_CURRENT",
    }
    return grouped


def _load_alias_identity_map(data_root: Path) -> dict[str, str]:
    path = data_root / "03 Security Master" / "nifty500_effective_symbol_transitions.csv"
    mapping: dict[str, str] = {}
    if not path.exists():
        return mapping
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            isin = row["isin"].strip().upper()
            if not isin:
                continue
            mapping[row["old_symbol"].strip().upper()] = isin
            mapping[row["new_symbol"].strip().upper()] = isin
    return mapping


def _identity_equivalent(left: set[str], right: set[str], alias_identity: dict[str, str]) -> bool:
    def identities(symbols: set[str]) -> set[str]:
        return {alias_identity.get(symbol, f"UNRESOLVED_SYMBOL:{symbol}") for symbol in symbols}

    return identities(left) == identities(right)


def _reverse_to_start(
    *,
    anchor_members: set[str],
    anchor_date: date,
    start: date,
    events: dict[date, list[dict[str, Any]]],
) -> tuple[set[str], list[dict[str, Any]]]:
    state = set(anchor_members)
    exceptions: list[dict[str, Any]] = []
    for effective in sorted((value for value in events if start <= value <= anchor_date), reverse=True):
        for event in reversed(events[effective]):
            if event.get("conditional_identity_transition"):
                if not (event["inclusion_set"] & state):
                    continue
                state.difference_update(event["inclusion_set"])
                state.update(event["exclusion_set"])
                continue
            missing_inclusions = event["inclusion_set"] - state
            already_present_exclusions = event["exclusion_set"] & state
            if missing_inclusions or already_present_exclusions:
                exceptions.append(
                    {
                        "operation": "REVERSE",
                        "effective_date": effective.isoformat(),
                        "source_file": event["source_file"],
                        "missing_expected_inclusions": ",".join(sorted(missing_inclusions)),
                        "already_present_exclusions": ",".join(sorted(already_present_exclusions)),
                    }
                )
            state.difference_update(event["inclusion_set"])
            state.update(event["exclusion_set"])
    return state, exceptions


def _states_and_reconciliation(
    *,
    start: date,
    as_of: date,
    events: dict[date, list[dict[str, Any]]],
    anchors: dict[date, dict[str, Any]],
    alias_identity: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    first_anchor_date = min(anchors)
    state, exceptions = _reverse_to_start(
        anchor_members=anchors[first_anchor_date]["members"],
        anchor_date=first_anchor_date,
        start=start,
        events=events,
    )
    confidence = "RECONSTRUCTED_MEDIUM_CONFIDENCE"
    evidence_source = f"REVERSED_FROM_{first_anchor_date.isoformat()}"
    state_rows: list[dict[str, Any]] = []
    reconciliation: list[dict[str, Any]] = []
    change_dates = sorted({start, *events, *anchors})
    for change_date in change_dates:
        if change_date < start or change_date > as_of:
            continue
        applied_sources: list[str] = []
        for event in events.get(change_date, []):
            if event.get("conditional_identity_transition"):
                if not (event["exclusion_set"] & state):
                    continue
                state.difference_update(event["exclusion_set"])
                state.update(event["inclusion_set"])
                applied_sources.append(event["source_file"])
                evidence_source = "+".join(applied_sources)
                continue
            missing_exclusions = event["exclusion_set"] - state
            preexisting_inclusions = event["inclusion_set"] & state
            if missing_exclusions or preexisting_inclusions:
                exceptions.append(
                    {
                        "operation": "FORWARD",
                        "effective_date": change_date.isoformat(),
                        "source_file": event["source_file"],
                        "missing_expected_exclusions": ",".join(sorted(missing_exclusions)),
                        "preexisting_inclusions": ",".join(sorted(preexisting_inclusions)),
                    }
                )
            state.difference_update(event["exclusion_set"])
            state.update(event["inclusion_set"])
            applied_sources.append(event["source_file"])
            confidence = (
                "RECONSTRUCTED_HIGH_CONFIDENCE" if event["balanced"] else "RECONSTRUCTED_MEDIUM_CONFIDENCE"
            )
            evidence_source = "+".join(applied_sources)
        if change_date in anchors:
            official = anchors[change_date]["members"]
            missing_from_prediction = official - state
            extra_in_prediction = state - official
            identity_equivalent = _identity_equivalent(official, state, alias_identity)
            reconciliation.append(
                {
                    "anchor_date": change_date.isoformat(),
                    "source": anchors[change_date]["source"],
                    "source_sha256": anchors[change_date]["source_sha256"],
                    "official_count": len(official),
                    "predicted_count_before_reset": len(state),
                    "missing_from_prediction_count": len(missing_from_prediction),
                    "extra_in_prediction_count": len(extra_in_prediction),
                    "missing_from_prediction": ",".join(sorted(missing_from_prediction)),
                    "extra_in_prediction": ",".join(sorted(extra_in_prediction)),
                    "exact_match_before_reset": not missing_from_prediction and not extra_in_prediction,
                    "identity_equivalent_before_reset": identity_equivalent,
                }
            )
            # An official snapshot can publish tomorrow's symbol at today's close. When the economic
            # security sets are identical, preserve the effective-dated tradable alias and let the explicit
            # identity transition change it on the documented date instead of creating a one-session gap.
            if not identity_equivalent:
                state = set(official)
            confidence = anchors[change_date]["grade"]
            evidence_source = anchors[change_date]["source"]
        state_rows.append(
            {
                "effective_date": change_date.isoformat(),
                "member_count": len(state),
                "confidence_grade": confidence,
                "evidence_source": evidence_source,
                "members": set(state),
            }
        )
    return state_rows, reconciliation, exceptions


def _membership_intervals(states: list[dict[str, Any]], *, as_of: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, state in enumerate(states):
        valid_to = states[index + 1]["effective_date"] if index + 1 < len(states) else date.resolution + as_of
        valid_to_value = valid_to if isinstance(valid_to, str) else valid_to.isoformat()
        for symbol in sorted(state["members"]):
            rows.append(
                {
                    "valid_from": state["effective_date"],
                    "valid_to_exclusive": valid_to_value,
                    "symbol": symbol,
                    "confidence_grade": state["confidence_grade"],
                    "evidence_source": state["evidence_source"],
                    "foundation_version": FOUNDATION_VERSION,
                }
            )
    return rows


def _write_daily_parquet(*, intervals_path: Path, output_path: Path, start: date, as_of: date) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid4().hex}.partial")
    intervals_sql = str(intervals_path).replace("'", "''")
    temporary_sql = str(temporary).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(f"ATTACH '{MASTER_DB.as_posix()}' AS master (READ_ONLY)")
        connection.execute(
            f"""
            COPY (
                WITH interval_rows AS (
                    SELECT CAST(valid_from AS DATE) AS valid_from,
                           CAST(valid_to_exclusive AS DATE) AS valid_to_exclusive,
                           symbol, confidence_grade, evidence_source, foundation_version
                    FROM read_csv_auto('{intervals_sql}', header=true, all_varchar=true)
                ), sessions AS (
                    SELECT DISTINCT Date
                    FROM master.clean_daily
                    WHERE Date BETWEEN DATE '{start.isoformat()}' AND DATE '{as_of.isoformat()}'
                ), candidates AS (
                    SELECT s.Date,
                           i.symbol AS Symbol,
                           h.ISIN,
                           i.confidence_grade AS MembershipConfidence,
                           i.evidence_source AS MembershipEvidence,
                           i.foundation_version AS FoundationVersion,
                           CASE WHEN h.ISIN IS NULL THEN 'UNRESOLVED_SYMBOL_TO_ISIN'
                                ELSE 'RESOLVED_FROM_CERTIFIED_MASTER_HISTORY' END AS IdentityStatus,
                           ROW_NUMBER() OVER (
                               PARTITION BY s.Date, i.symbol
                               ORDER BY CASE WHEN h.ISIN IS NULL THEN 1 ELSE 0 END,
                                        h.Observations DESC NULLS LAST,
                                        h.ISIN
                           ) AS identity_choice
                    FROM sessions s
                    JOIN interval_rows i
                      ON s.Date >= i.valid_from AND s.Date < i.valid_to_exclusive
                    LEFT JOIN master.vajra_security_symbol_history h
                      ON h.Symbol = i.symbol AND s.Date BETWEEN h.FirstDate AND h.LastDate
                )
                SELECT Date, Symbol, ISIN, MembershipConfidence, MembershipEvidence,
                       FoundationVersion, IdentityStatus
                FROM candidates
                WHERE identity_choice = 1
                ORDER BY Date, Symbol
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """,
        )
    os.replace(temporary, output_path)


def build_point_in_time_membership(
    *,
    data_root: Path = DATA_ROOT,
    start: date = date(2009, 1, 1),
    as_of: date = date(2026, 8, 13),
) -> dict[str, Any]:
    output_dir = data_root / "02 Constituent History"
    panel_dir = data_root / "07 Point In Time Panels"
    parquet_dir = data_root / "08 Parquet"
    validation_dir = data_root / "09 Validation"
    logs_dir = data_root / "11 Logs"
    for directory in (output_dir, panel_dir, parquet_dir, validation_dir, logs_dir):
        directory.mkdir(parents=True, exist_ok=True)
    events = _load_events(data_root, start=start, as_of=as_of)
    anchors = _load_official_anchors(data_root, as_of=as_of)
    alias_identity = _load_alias_identity_map(data_root)
    states, reconciliation, exceptions = _states_and_reconciliation(
        start=start,
        as_of=as_of,
        events=events,
        anchors=anchors,
        alias_identity=alias_identity,
    )
    intervals = _membership_intervals(states, as_of=as_of)
    intervals_path = output_dir / "nifty500_membership_intervals.csv"
    reconciliation_path = validation_dir / "nifty500_official_anchor_reconciliation.csv"
    exceptions_path = validation_dir / "nifty500_membership_event_application_exceptions.csv"
    state_manifest_path = output_dir / "nifty500_membership_state_manifest.csv"
    _write_csv(intervals_path, intervals)
    _write_csv(reconciliation_path, reconciliation)
    _write_csv(exceptions_path, exceptions)
    _write_csv(
        state_manifest_path,
        [{key: value for key, value in row.items() if key != "members"} for row in states],
    )
    daily_path = parquet_dir / "nifty500_daily_membership.parquet"
    _write_daily_parquet(
        intervals_path=intervals_path,
        output_path=daily_path,
        start=start,
        as_of=as_of,
    )
    with duckdb.connect() as connection:
        summary = connection.execute(
            """
            SELECT COUNT(*) AS row_count,
                   COUNT(DISTINCT Date) AS session_count,
                   COUNT(DISTINCT Symbol) AS symbol_count,
                   COUNT(DISTINCT ISIN) AS isin_count,
                   SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END) AS unresolved_isin_rows,
                   COUNT(*) - COUNT(DISTINCT CAST(Date AS VARCHAR) || '|' || Symbol) AS duplicate_date_symbol_rows,
                   MIN(Date) AS earliest_date,
                   MAX(Date) AS latest_date,
                   MIN(daily_count) AS min_daily_members,
                   MAX(daily_count) AS max_daily_members
            FROM (
                SELECT *, COUNT(*) OVER (PARTITION BY Date) daily_count
                FROM read_parquet(?)
            )
            """,
            [str(daily_path)],
        ).fetchone()
    exact_anchors = sum(str(row["exact_match_before_reset"]).casefold() == "true" for row in reconciliation)
    identity_exact_anchors = sum(
        str(row["identity_equivalent_before_reset"]).casefold() == "true" for row in reconciliation
    )
    generated = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "status": "COMPLETE_WITH_RECONCILIATION_EXCEPTIONS" if exceptions else "COMPLETE",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "start": start.isoformat(),
        "as_of": as_of.isoformat(),
        "official_anchor_count": len(anchors),
        "official_anchors_exact_before_reset": exact_anchors,
        "official_anchors_identity_equivalent_before_reset": identity_exact_anchors,
        "official_anchors_requiring_economic_reset": len(reconciliation) - identity_exact_anchors,
        "event_application_exception_count": len(exceptions),
        "state_change_count": len(states),
        "interval_rows": len(intervals),
        "daily_rows": summary[0],
        "sessions": summary[1],
        "unique_symbols": summary[2],
        "unique_isins": summary[3],
        "unresolved_isin_rows": summary[4],
        "duplicate_date_symbol_rows": summary[5],
        "earliest_date": str(summary[6]),
        "latest_date": str(summary[7]),
        "min_daily_members": summary[8],
        "max_daily_members": summary[9],
        "intervals_path": str(intervals_path),
        "intervals_sha256": sha256_file(intervals_path),
        "daily_membership_path": str(daily_path),
        "daily_membership_sha256": sha256_file(daily_path),
        "reconciliation_path": str(reconciliation_path),
        "reconciliation_sha256": sha256_file(reconciliation_path),
        "exceptions_path": str(exceptions_path),
        "exceptions_sha256": sha256_file(exceptions_path),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(logs_dir / "point_in_time_membership_build_status.json", status)
    return status
