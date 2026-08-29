from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def write_phase_checkpoint(
    path: Path,
    *,
    phase: int,
    name: str,
    status: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    checkpoint = {
        "phase": phase,
        "name": name,
        "status": status,
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "inputs": inputs,
        "outputs": outputs,
        "evidence": evidence,
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(path, checkpoint)
    return checkpoint
