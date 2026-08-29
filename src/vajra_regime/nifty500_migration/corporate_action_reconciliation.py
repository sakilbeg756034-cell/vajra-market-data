from __future__ import annotations

import csv
import os
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import DATA_ROOT, FOUNDATION_VERSION


@dataclass(frozen=True)
class ParsedAction:
    action_type: str
    price_factor: float | None
    volume_factor: float | None
    parse_status: str
    note: str


def _ratio(text: str, prefix: str) -> tuple[float, float] | None:
    match = re.search(rf"{prefix}.*?(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", text)
    if not match:
        return None
    numerator, denominator = float(match.group(1)), float(match.group(2))
    if numerator <= 0 or denominator <= 0:
        return None
    return numerator, denominator


def _split_values(text: str) -> tuple[float, float] | None:
    markers = [text.find(token) for token in ("FACE VALUE", "FV ", "FV-", "SPLIT", "SPLT", "SUB-DIV")]
    positions = [position for position in markers if position >= 0]
    if not positions:
        return None
    tail = text[min(positions) :]
    match = re.search(
        r"(?:RS|RE)?\s*\.?\s*(\d+(?:\.\d+)?)\s*(?:/-)?[^0-9]{0,30}?"
        r"(?:TO|TOR)\s*(?:RS|RE)?\s*\.?\s*(\d+(?:\.\d+)?)",
        tail,
    )
    if not match:
        return None
    old_face, new_face = float(match.group(1)), float(match.group(2))
    if old_face <= 0 or new_face <= 0 or old_face == new_face:
        return None
    return old_face, new_face


def classify_official_action(subject: str) -> ParsedAction:
    text = re.sub(r"\s+", " ", subject.upper().replace("â€“", "-").replace("â€”", "-")).strip()
    if "DEMERG" in text or "DE-MERG" in text:
        return ParsedAction("DEMERGER", None, None, "REVIEW", "Event-specific demerger adjustment prohibited")
    if "AMALGAM" in text or re.search(r"\bMERGER\b", text):
        return ParsedAction("MERGER_OR_AMALGAMATION", None, None, "REVIEW", "Complex identity/economic event")
    if re.search(r"\bRIGHTS?\b", text):
        return ParsedAction("RIGHTS", None, None, "REVIEW", "Rights issue is not mechanically adjusted")

    excluded_bonus = any(token in text for token in ("DEBENTURE", "NCRPS", "PREFERENCE", " DVR"))
    bonus = None if excluded_bonus else _ratio(text, r"(?:BONUS|BON\b)[^0-9]*")
    split = _split_values(text)
    if bonus or split:
        price_factor = 1.0
        notes: list[str] = []
        action_parts: list[str] = []
        if bonus:
            new_shares, old_shares = bonus
            bonus_factor = old_shares / (old_shares + new_shares)
            price_factor *= bonus_factor
            action_parts.append("BONUS")
            notes.append(f"bonus {new_shares:g}:{old_shares:g}")
        if split:
            old_face, new_face = split
            split_factor = new_face / old_face
            price_factor *= split_factor
            action_parts.append("SPLIT" if split_factor < 1 else "CONSOLIDATION")
            notes.append(f"face value {old_face:g}->{new_face:g}")
        if not 0 < price_factor < 100:
            return ParsedAction("MECHANICAL_UNPARSED", None, None, "REVIEW", "Invalid combined factor")
        return ParsedAction(
            "_AND_".join(action_parts),
            price_factor,
            1.0 / price_factor,
            "PARSED",
            "; ".join(notes),
        )

    has_mechanical_words = any(
        token in text for token in ("BONUS", "BON-", "FACE VALUE", "FV ", "FV-", "SPLIT", "SPLT", "SUB-DIV")
    )
    if has_mechanical_words and not excluded_bonus:
        return ParsedAction("MECHANICAL_UNPARSED", None, None, "REVIEW", "Ratio/face-value terms not safely parsed")
    if "DIVIDEND" in text or re.search(r"\bDIV\b", text):
        return ParsedAction("DIVIDEND", 1.0, 1.0, "NO_ADJUST", "Price-return series; dividend informational")
    if "BUYBACK" in text or "BUY BACK" in text:
        return ParsedAction("BUYBACK", 1.0, 1.0, "NO_ADJUST", "No mechanical OHLCV rescaling")
    return ParsedAction("OTHER_INFORMATIONAL", 1.0, 1.0, "NO_ADJUST", "No mechanical OHLCV rescaling")


def _atomic_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def build_corporate_action_reconciliation(*, data_root: Path = DATA_ROOT) -> dict[str, Any]:
    ca_path = data_root / "04 Corporate Actions" / "nifty500_official_corporate_actions_all_equities.parquet"
    raw_paths = sorted((data_root / "08 Parquet" / "raw").glob("year=*/nifty500_raw_daily.parquet"))
    if not ca_path.exists() or not raw_paths:
        raise RuntimeError("Official corporate actions and PIT raw OHLCV are required")

    with duckdb.connect() as connection:
        source_rows = connection.execute(
            """
            WITH raw_bounds AS (
                SELECT ISIN, MIN(Date) AS FirstPITDate, MAX(Date) AS LastPITDate
                FROM read_parquet(?) WHERE ISIN IS NOT NULL GROUP BY ISIN
            ), relevant AS (
                SELECT c.*, b.FirstPITDate, b.LastPITDate
                FROM read_parquet(?) c JOIN raw_bounds b USING (ISIN)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY c.ISIN, c.ExDate, UPPER(TRIM(c.Subject))
                    ORDER BY c.EventId
                ) = 1
            )
            SELECT r.EventId, r.Symbol, r.ISIN, r.Series, r.CompanyName, r.Subject,
                   r.FaceValue, r.ExDate, r.RecordDate, r.SourceArchive, r.SourceSha256,
                   r.FirstPITDate, r.LastPITDate,
                   pre.Date AS PreDate, pre.Close AS PreClose,
                   post.Date AS PostDate, post.Close AS PostClose
            FROM relevant r
            LEFT JOIN LATERAL (
                SELECT Date, Close FROM read_parquet(?) p
                WHERE p.ISIN = r.ISIN AND p.Date < r.ExDate
                  AND p.Date >= r.ExDate - INTERVAL 15 DAY
                ORDER BY Date DESC LIMIT 1
            ) pre ON true
            LEFT JOIN LATERAL (
                SELECT Date, Close FROM read_parquet(?) p
                WHERE p.ISIN = r.ISIN AND p.Date >= r.ExDate
                  AND p.Date <= r.ExDate + INTERVAL 15 DAY
                ORDER BY Date LIMIT 1
            ) post ON true
            ORDER BY r.ExDate, r.ISIN, r.EventId
            """,
            [
                [str(path) for path in raw_paths],
                str(ca_path),
                [str(path) for path in raw_paths],
                [str(path) for path in raw_paths],
            ],
        ).fetchall()
        columns = [column[0] for column in connection.description]

    prepared: list[dict[str, Any]] = []
    for values in source_rows:
        row = dict(zip(columns, values, strict=True))
        parsed = classify_official_action(str(row["Subject"]))
        prepared.append({**row, **asdict(parsed)})

    compound_factors: dict[tuple[str, object], float] = {}
    for row in prepared:
        if row["parse_status"] != "PARSED" or not row["price_factor"]:
            continue
        key = (str(row["ISIN"]), row["ExDate"])
        compound_factors[key] = compound_factors.get(key, 1.0) * float(row["price_factor"])

    output_rows: list[dict[str, Any]] = []
    for row in prepared:
        parsed = ParsedAction(
            action_type=str(row["action_type"]),
            price_factor=row["price_factor"],
            volume_factor=row["volume_factor"],
            parse_status=str(row["parse_status"]),
            note=str(row["note"]),
        )
        in_observation = row["FirstPITDate"] <= row["ExDate"] <= row["LastPITDate"]
        factor_for_bridge = compound_factors.get((str(row["ISIN"]), row["ExDate"]), parsed.price_factor)
        bridge_gap: float | None = None
        if factor_for_bridge and row["PreClose"] and row["PostClose"]:
            bridge_gap = float(row["PostClose"]) / (float(row["PreClose"]) * factor_for_bridge) - 1.0
        if not in_observation:
            decision = "OUTSIDE_PIT_OBSERVATION"
        elif parsed.parse_status == "NO_ADJUST":
            decision = "INFORMATIONAL_NO_PRICE_ADJUSTMENT"
        elif parsed.parse_status != "PARSED":
            decision = "REVIEW_COMPLEX_OR_UNPARSED"
        elif bridge_gap is None:
            near_entry = row["PreDate"] is None and (row["ExDate"] - row["FirstPITDate"]).days <= 15
            near_exit = row["PostDate"] is None and (row["LastPITDate"] - row["ExDate"]).days <= 15
            decision = (
                "NO_PIT_CROSS_EVENT_BRIDGE_REQUIRED"
                if near_entry or near_exit
                else "REVIEW_MISSING_PRICE_BRIDGE"
            )
        elif abs(bridge_gap) <= 0.35:
            decision = (
                "AUTO_READY_VERIFIED_MECHANICAL_COMPOUND"
                if factor_for_bridge != parsed.price_factor
                else "AUTO_READY_VERIFIED_MECHANICAL"
            )
        else:
            decision = "REVIEW_PRICE_BRIDGE"
        output_rows.append(
            {
                **{
                    key: str(value) if value is not None else ""
                    for key, value in row.items()
                    if key not in asdict(parsed)
                },
                **asdict(parsed),
                "CompoundPriceFactor": factor_for_bridge,
                "InPITObservation": in_observation,
                "PostVsAdjustedPreGap": bridge_gap,
                "Decision": decision,
                "QuarantinePolicy": (
                    "252_VALID_OBSERVATIONS_FROM_EX_DATE"
                    if decision.startswith("REVIEW_")
                    else "NONE"
                ),
                "FoundationVersion": FOUNDATION_VERSION,
            }
        )

    action_root = data_root / "04 Corporate Actions"
    csv_path = action_root / "nifty500_corporate_action_reconciliation.csv"
    fieldnames = list(output_rows[0])
    _atomic_csv(csv_path, output_rows, fieldnames)
    parquet_path = action_root / "nifty500_corporate_action_reconciliation.parquet"
    temporary = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.partial")
    csv_sql = str(csv_path).replace("'", "''")
    temporary_sql = str(temporary).replace("'", "''")
    raw_path_literals = ["'" + str(path).replace("'", "''") + "'" for path in raw_paths]
    raw_list_sql = "[" + ",".join(raw_path_literals) + "]"
    with duckdb.connect() as connection:
        connection.execute(
            f"CREATE TEMP VIEW pit_raw AS SELECT * FROM read_parquet({raw_list_sql})",
        )
        connection.execute(
            f"""
            COPY (
                WITH typed AS (
                SELECT EventId, Symbol, NULLIF(ISIN, '') AS ISIN, Series, CompanyName, Subject,
                       FaceValue, CAST(ExDate AS DATE) AS ExDate,
                       TRY_CAST(NULLIF(RecordDate, '') AS DATE) AS RecordDate,
                       SourceArchive, SourceSha256, CAST(FirstPITDate AS DATE) AS FirstPITDate,
                       CAST(LastPITDate AS DATE) AS LastPITDate,
                       TRY_CAST(NULLIF(PreDate, '') AS DATE) AS PreDate,
                       TRY_CAST(NULLIF(PreClose, '') AS DOUBLE) AS PreClose,
                       TRY_CAST(NULLIF(PostDate, '') AS DATE) AS PostDate,
                       TRY_CAST(NULLIF(PostClose, '') AS DOUBLE) AS PostClose,
                       action_type AS ActionType,
                       TRY_CAST(NULLIF(price_factor, '') AS DOUBLE) AS PriceFactor,
                       TRY_CAST(NULLIF(volume_factor, '') AS DOUBLE) AS VolumeFactor,
                       TRY_CAST(NULLIF(CompoundPriceFactor, '') AS DOUBLE) AS CompoundPriceFactor,
                       parse_status AS ParseStatus, note AS Note,
                       CAST(InPITObservation AS BOOLEAN) AS InPITObservation,
                       TRY_CAST(NULLIF(PostVsAdjustedPreGap, '') AS DOUBLE) AS PostVsAdjustedPreGap,
                       Decision, QuarantinePolicy, FoundationVersion
                FROM read_csv_auto('{csv_sql}', header=true, all_varchar=true)
                ), with_quarantine_end AS (
                    SELECT typed.*,
                           q.Date AS QuarantineEndDate
                    FROM typed
                    LEFT JOIN LATERAL (
                        SELECT Date FROM pit_raw p
                        WHERE typed.Decision LIKE 'REVIEW_%'
                          AND p.ISIN = typed.ISIN AND p.Date >= typed.ExDate
                        ORDER BY Date LIMIT 1 OFFSET 251
                    ) q ON true
                )
                SELECT * FROM with_quarantine_end
                ORDER BY ExDate, ISIN, EventId
            ) TO '{temporary_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        summary_rows = connection.execute(
            """
            SELECT Decision, COUNT(*) FROM read_parquet(?) GROUP BY Decision ORDER BY Decision
            """,
            [str(temporary)],
        ).fetchall()
        type_rows = connection.execute(
            """
            SELECT ActionType, COUNT(*) FROM read_parquet(?) WHERE InPITObservation
            GROUP BY ActionType ORDER BY ActionType
            """,
            [str(temporary)],
        ).fetchall()
    os.replace(temporary, parquet_path)
    decision_counts = dict(summary_rows)
    status: dict[str, Any] = {
        "status": "COMPLETE_WITH_EXPLICIT_REVIEW_QUARANTINE",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "foundation_version": FOUNDATION_VERSION,
        "official_ca_sha256": sha256_file(ca_path),
        "raw_year_sha256": {path.parent.name: sha256_file(path) for path in raw_paths},
        "events_for_pit_identities": len(output_rows),
        "decision_counts": decision_counts,
        "in_observation_action_type_counts": dict(type_rows),
        "auto_ready_verified_mechanical": sum(
            count for decision, count in summary_rows if decision.startswith("AUTO_READY_")
        ),
        "review_quarantine_events": sum(count for decision, count in summary_rows if decision.startswith("REVIEW_")),
        "reconciliation_path": str(parquet_path),
        "reconciliation_sha256": sha256_file(parquet_path),
        "parser_code_sha256": sha256_file(Path(__file__)),
        "policy": (
            "Official NSE ISIN + mechanical ratio + raw pre/post bridge <=35%; "
            "rights/merger/demerger/unparsed events are never guessed and receive 252-observation quarantine"
        ),
    }
    status["status_payload_sha256"] = canonical_hash(status)
    atomic_json(data_root / "11 Logs" / "corporate_action_reconciliation_status.json", status)
    atomic_json(
        data_root / "12 Checkpoints" / "phase_06_corporate_action_reconciliation.json",
        {**status, "checkpoint_status": "COMPLETE_HASH_VERIFIED"},
    )
    return status
