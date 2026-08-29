from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

from vajra_regime.config import AppConfig


LEGACY_DATABASE_NAME = "EOD2_Aug2010_Dec2025_Clean.duckdb"
PROTECTED_TREE_FINGERPRINT_VERSION = "protected_tree_fingerprint_v1"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_protected_tree(
    root: Path,
    *,
    include_content_hashes: bool = False,
) -> dict[str, object]:
    """Return a deterministic, read-only fingerprint for a protected directory tree.

    Daily automation uses the fast metadata mode (relative path, type, size and
    nanosecond mtime). A slower content-hash mode is available for periodic baseline
    and disaster-recovery audits. Symlinks are fingerprinted but never followed.
    """
    root = Path(root)
    if not root.exists():
        # First ever run: an empty published folder is a valid starting fingerprint.
        root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir():
        raise NotADirectoryError(root)

    digest = hashlib.sha256()
    file_count = 0
    directory_count = 0
    total_size_bytes = 0
    latest_mtime_ns = 0

    for directory, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        dirnames.sort(key=str.casefold)
        filenames.sort(key=str.casefold)
        directory_path = Path(directory)
        directory_stat = directory_path.stat()
        relative_directory = directory_path.relative_to(root).as_posix() or "."
        digest.update(
            f"D\0{relative_directory}\0{directory_stat.st_mtime_ns}\n".encode(
                "utf-8", errors="surrogatepass"
            )
        )
        directory_count += 1
        latest_mtime_ns = max(latest_mtime_ns, directory_stat.st_mtime_ns)

        for filename in filenames:
            path = directory_path / filename
            stat = path.lstat()
            relative_path = path.relative_to(root).as_posix()
            content_marker = ""
            if path.is_symlink():
                content_marker = f"SYMLINK:{os.readlink(path)}"
            elif include_content_hashes:
                content_marker = _file_sha256(path)
            record = (
                f"F\0{relative_path}\0{stat.st_mode}\0{stat.st_size}\0"
                f"{stat.st_mtime_ns}\0{content_marker}\n"
            )
            digest.update(record.encode("utf-8", errors="surrogatepass"))
            file_count += 1
            total_size_bytes += int(stat.st_size)
            latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)

    latest_mtime_utc = (
        datetime.fromtimestamp(latest_mtime_ns / 1_000_000_000, tz=UTC).isoformat()
        if latest_mtime_ns
        else None
    )
    return {
        "version": PROTECTED_TREE_FINGERPRINT_VERSION,
        "mode": "CONTENT_SHA256" if include_content_hashes else "FAST_METADATA",
        "root": str(root.resolve()),
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fingerprint_sha256": digest.hexdigest(),
        "file_count": file_count,
        "directory_count": directory_count,
        "total_size_bytes": total_size_bytes,
        "latest_mtime_utc": latest_mtime_utc,
        "content_hashes_included": include_content_hashes,
    }


def compare_protected_tree_fingerprints(
    before: dict[str, object],
    after: dict[str, object],
) -> dict[str, object]:
    unchanged = (
        before.get("version") == after.get("version")
        and before.get("mode") == after.get("mode")
        and before.get("root") == after.get("root")
        and before.get("fingerprint_sha256") == after.get("fingerprint_sha256")
        and before.get("file_count") == after.get("file_count")
        and before.get("directory_count") == after.get("directory_count")
        and before.get("total_size_bytes") == after.get("total_size_bytes")
    )
    return {
        "unchanged": unchanged,
        "modified": not unchanged,
        "before_fingerprint_sha256": before.get("fingerprint_sha256"),
        "after_fingerprint_sha256": after.get("fingerprint_sha256"),
    }


@contextmanager
def guard_protected_tree(
    root: Path,
    *,
    include_content_hashes: bool = False,
) -> Iterator[dict[str, object]]:
    """Fail closed if the protected tree changes while the guarded work runs."""
    evidence: dict[str, object] = {
        "before": fingerprint_protected_tree(
            root, include_content_hashes=include_content_hashes
        )
    }
    try:
        yield evidence
    finally:
        after = fingerprint_protected_tree(root, include_content_hashes=include_content_hashes)
        evidence["after"] = after
        evidence.update(compare_protected_tree_fingerprints(evidence["before"], after))
        if evidence["modified"]:
            raise RuntimeError(
                "Protected tree fingerprint changed during the run: " + str(root)
            )


def _same_file(left: Path, right: Path) -> bool:
    if not left.exists() or not right.exists():
        return False
    try:
        return os.path.samefile(left, right)
    except OSError:
        left_stat = left.stat()
        right_stat = right.stat()
        return (
            left_stat.st_dev == right_stat.st_dev
            and left_stat.st_ino != 0
            and left_stat.st_ino == right_stat.st_ino
        )


def ensure_mutable_master(config: AppConfig) -> dict[str, object]:
    """Ensure the rolling DuckDB is an independent mutable file, not a hard link.

    Phase 0 initially used a zero-space hard link for the master database. That is safe for
    read-only research but unsafe once 2026 live tables begin changing. This function breaks
    only that alias by copying the current master to a temporary file and atomically replacing
    the master path. The verified legacy copy is never modified or deleted.
    """
    master = Path(config.environment.duckdb_path)
    database_dir = master.parent
    legacy_copy = database_dir / LEGACY_DATABASE_NAME
    log_dir = Path(config.environment.logs_dir) / "Database Safety"
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / "mutable_master_status.json"

    if not master.exists():
        raise FileNotFoundError(f"Rolling master DuckDB not found: {master}")

    was_linked = _same_file(master, legacy_copy)
    report: dict[str, object] = {
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "master": str(master),
        "legacy_copy": str(legacy_copy),
        "legacy_copy_exists": legacy_copy.exists(),
        "was_hardlinked_to_legacy_copy": was_linked,
        "detached": False,
        "master_size_bytes": master.stat().st_size,
        "legacy_preserved": True,
    }

    if not was_linked:
        report["status"] = "MASTER_ALREADY_INDEPENDENT"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    size = master.stat().st_size
    free = shutil.disk_usage(database_dir).free
    safety_reserve = 2 * 1024**3
    if free < size + safety_reserve:
        raise OSError(
            "Not enough free space to detach the rolling DuckDB safely. "
            f"Need roughly {(size + safety_reserve) / 1024**3:.2f} GB free."
        )

    temporary = master.with_name(master.name + ".detaching.tmp")
    if temporary.exists():
        temporary.unlink()
    shutil.copy2(master, temporary)
    if temporary.stat().st_size != size:
        temporary.unlink(missing_ok=True)
        raise OSError("Temporary rolling-master copy size did not match the source database.")

    os.replace(temporary, master)
    if _same_file(master, legacy_copy):
        raise OSError("Rolling-master hard link could not be detached safely.")

    report["detached"] = True
    report["status"] = "MASTER_DETACHED_AND_MUTABLE"
    report["master_size_bytes"] = master.stat().st_size
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
