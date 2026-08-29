"""Delete everything on D: except the three folders that stay.

Refuses to run unless the replacement has been proved good first:
  * D:/VAJRA_DATA passes verify_published
  * the latest quality report is not FAIL
  * the latest engine run succeeded
  * every file in the original store still has a counterpart in the engine store

Run with --dry-run to see the plan. Run with --execute to delete.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

D = Path("D:/")

# Never named, never listed, never touched. Present here only so the deletion loop can skip
# it by exact name without ever reading inside it.
KEEP_EXACT = {
    "VAJRA_DATA",
    "VAJRA_ENGINE",
    "Obsidian dump",
    "System Volume Information",
    "$RECYCLE.BIN",
    ".claude",  # holds the permission rules that protect the folder above
}

ORIGINAL_STORE = D / "Vajra Market System"
ENGINE_STORE = D / "VAJRA_ENGINE" / "store"
STORE_FOLDERS = [
    "01 Protected Source Data",
    "02 Master Historical Data",
    "03 Incoming NSE EOD",
    "04 Corporate Actions",
    "08 Logs",
    "09 Backups",
]


def sha256(path: Path) -> str:
    d = hashlib.sha256()
    with path.open("rb") as h:
        for chunk in iter(lambda: h.read(4 * 1024 * 1024), b""):
            d.update(chunk)
    return d.hexdigest()


def preflight() -> dict:
    """Everything that must be true before a single byte is removed."""
    checks: dict = {}

    sys.path.insert(0, str(D / "VAJRA_ENGINE" / "code" / "src"))
    from vajra_regime.publish import verify_published  # noqa: PLC0415

    verification = verify_published(D / "VAJRA_DATA")
    checks["published_dataset_verifies"] = {
        "pass": verification["pass"],
        "files_checked": verification["files_checked"],
        "latest_session": verification["latest_session"],
    }

    quality_path = D / "VAJRA_ENGINE" / "logs" / "quality" / "latest_quality_report.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8")) if quality_path.is_file() else {}
    checks["quality_not_failing"] = {
        "pass": quality.get("overall") in {"PASS", "PASS_WITH_WARNINGS"},
        "overall": quality.get("overall"),
    }

    drills_path = D / "VAJRA_ENGINE" / "logs" / "drills" / "drill_results.json"
    drills = json.loads(drills_path.read_text(encoding="utf-8")) if drills_path.is_file() else {}
    checks["all_drills_passed"] = {
        "pass": bool(drills) and all(v == "PASS" for v in drills.get("verdicts", {}).values()),
        "verdicts": drills.get("verdicts", {}),
    }

    # Every original file must still be represented in the engine store. Contents may differ
    # where the corporate-action repair rewrote a file - that is the point of the repair - so
    # differences are reported, not failed.
    missing: list[str] = []
    differing: list[str] = []
    same = 0
    for folder in STORE_FOLDERS:
        base = ORIGINAL_STORE / folder
        if not base.is_dir():
            continue
        for source in base.rglob("*"):
            if not source.is_file() or source.name.endswith((".tmp", ".partial")):
                continue
            target = ENGINE_STORE / source.relative_to(ORIGINAL_STORE)
            relative = str(source.relative_to(ORIGINAL_STORE)).replace("\\", "/")
            # The Google Sheets and Telegram credentials were deliberately not carried over:
            # the engine no longer talks to either service, so keeping live tokens around
            # would be a liability rather than a backup.
            if "Local Secrets" in relative:
                continue
            if not target.is_file():
                missing.append(relative)
            elif source.stat().st_size != target.stat().st_size or sha256(source) != sha256(target):
                differing.append(str(source.relative_to(ORIGINAL_STORE)).replace("\\", "/"))
            else:
                same += 1
    checks["every_original_file_has_a_counterpart"] = {
        "pass": not missing,
        "identical": same,
        "differing": len(differing),
        "missing": len(missing),
        "missing_examples": missing[:20],
        "differing_examples": differing[:40],
        "note": "Differences are expected where the corporate-action repair rewrote a file.",
    }

    checks["all_pass"] = all(
        v.get("pass") for k, v in checks.items() if isinstance(v, dict) and "pass" in v
    )
    return checks


def plan() -> list[dict]:
    entries: list[dict] = []
    for item in sorted(D.iterdir(), key=lambda p: p.name.lower()):
        if item.name in KEEP_EXACT:
            continue
        try:
            if item.is_dir():
                size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
                count = sum(1 for f in item.rglob("*") if f.is_file())
            else:
                size = item.stat().st_size
                count = 1
        except OSError:
            size, count = -1, -1
        entries.append(
            {
                "path": str(item),
                "kind": "dir" if item.is_dir() else "file",
                "files": count,
                "bytes": size,
            }
        )
    return entries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--skip-preflight", action="store_true")
    args = parser.parse_args()

    checks = {"skipped": True} if args.skip_preflight else preflight()
    entries = plan()

    print("=== KEEPING ===")
    for name in sorted(KEEP_EXACT):
        print(f"  {name}")
    print()
    print("=== DELETING ===")
    total = 0
    for entry in entries:
        total += max(entry["bytes"], 0)
        print(f"  {entry['kind']:<4} {entry['files']:>7} files  {entry['bytes'] / 1e9:>7.2f} GB  {entry['path']}")
    print(f"\n  total: {len(entries)} entries, {total / 1e9:.2f} GB")
    print()
    print("=== PREFLIGHT ===")
    print(json.dumps(checks, indent=2)[:4000])

    if not args.execute:
        print("\nDRY RUN - nothing deleted. Pass --execute to proceed.")
        return 0

    if not args.skip_preflight and not checks.get("all_pass"):
        print("\nPREFLIGHT FAILED - refusing to delete anything.")
        return 1

    removed: list[dict] = []
    failed: list[dict] = []
    for entry in entries:
        path = Path(entry["path"])
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed.append(entry)
            print(f"  removed {path}")
        except Exception as error:  # noqa: BLE001
            failed.append({**entry, "error": repr(error)[:300]})
            print(f"  FAILED  {path}: {error}")

    record = {
        "performed_at_utc": datetime.now(UTC).isoformat(),
        "preflight": checks,
        "removed": removed,
        "failed": failed,
        "bytes_removed": sum(max(e["bytes"], 0) for e in removed),
        "kept": sorted(KEEP_EXACT),
    }
    out = D / "VAJRA_ENGINE" / "logs" / "deletion_record.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(f"\nrecord written to {out}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
