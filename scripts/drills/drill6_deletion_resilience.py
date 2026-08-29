"""Drill 6 - the operator deletes things.

The requirement, in his words: if a Parquet or a CSV is deleted by accident - or the whole
folder in a moment of frustration - the system must not stop. It must recover as much correct
history as it can, by itself, with no manual step. And CSVs specifically: he deletes those when
the laptop runs out of disk, so those must NOT be silently recreated for old years.

Six scenarios, run against the real published dataset, because a resilience claim tested only
on a toy copy is not a resilience claim.

Everything here is recoverable from the engine's working store, which is why it is safe to do
this to production.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

DATA = Path(r"D:\VAJRA_DATA")
CODE = Path(r"D:\VAJRA_ENGINE\code")
PY = Path(r"D:\VAJRA_ENGINE\venv\Scripts\python.exe")


def sha(path: Path) -> str | None:
    if not path.is_file():
        return None
    d = hashlib.sha256()
    with path.open("rb") as h:
        for chunk in iter(lambda: h.read(4 * 1024 * 1024), b""):
            d.update(chunk)
    return d.hexdigest()


def publish() -> int:
    result = subprocess.run(
        [str(PY), "-m", "vajra_regime.publish_runner"],
        cwd=str(CODE),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:])
    return result.returncode


def ensure_csv_present(universe: str, year: int) -> bool:
    """Bring a deliberately-absent CSV back the documented way: delete its Parquet too.

    START_HERE_AI.md tells the reader this is how to recover a CSV they deleted. Using it here
    means the drill also proves that instruction is true.
    """
    csv_path = DATA / universe / "csv" / f"{universe}_{year}.csv"
    if csv_path.is_file():
        return True
    (DATA / universe / "parquet" / f"{universe}_{year}.parquet").unlink(missing_ok=True)
    publish()
    return csv_path.is_file()


def main() -> int:
    scenarios: dict[str, dict] = {}

    # This drill is re-runnable, so start from a state where every CSV exists.
    recovery_worked = all(
        ensure_csv_present(u, y)
        for u, y in (("nifty500", 2015), ("nifty750", 2012), ("nifty750", 2014))
    )
    scenarios["0_documented_csv_recovery_works"] = {
        "method": "delete the year's Parquet as well; the next run rebuilds both",
        "pass": recovery_worked,
    }

    # ---------------------------------------------------------------- 1. Parquet deleted
    target = DATA / "nifty500" / "parquet" / "nifty500_2017.parquet"
    before = sha(target)
    target.unlink()
    code = publish()
    scenarios["1_parquet_deleted_is_rebuilt"] = {
        "file": target.name,
        "publish_exit_code": code,
        "restored": target.is_file(),
        "byte_identical_to_before": sha(target) == before,
        "pass": code == 0 and target.is_file() and sha(target) == before,
    }

    # ------------------------------------------------- 2. CSV for an old year deleted
    csv_old = DATA / "nifty500" / "csv" / "nifty500_2015.csv"
    csv_old2 = DATA / "nifty750" / "csv" / "nifty750_2012.csv"
    csv_old.unlink()
    csv_old2.unlink()
    code = publish()
    manifest = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))
    without = manifest["csv_policy"]["years_without_csv"]
    listed = [f["path"] for f in manifest["files"]]
    scenarios["2_old_csv_deleted_stays_deleted"] = {
        "files": [csv_old.name, csv_old2.name],
        "publish_exit_code": code,
        "still_absent": not csv_old.exists() and not csv_old2.exists(),
        "recorded_in_manifest": {"nifty500": without["nifty500"], "nifty750": without["nifty750"]},
        "not_listed_as_a_dataset_file": (
            "nifty500/csv/nifty500_2015.csv" not in listed
            and "nifty750/csv/nifty750_2012.csv" not in listed
        ),
        "parquet_still_there": (DATA / "nifty500" / "parquet" / "nifty500_2015.parquet").is_file(),
        "pass": (
            code == 0
            and not csv_old.exists()
            and not csv_old2.exists()
            and 2015 in without["nifty500"]
            and 2012 in without["nifty750"]
        ),
    }

    # --------------------------------------- 3. both files for a year deleted -> rebuilt
    pq = DATA / "nifty750" / "parquet" / "nifty750_2014.parquet"
    cs = DATA / "nifty750" / "csv" / "nifty750_2014.csv"
    pq_before, cs_before = sha(pq), sha(cs)
    pq.unlink()
    cs.unlink()
    code = publish()
    scenarios["3_both_deleted_are_both_rebuilt"] = {
        "publish_exit_code": code,
        "parquet_restored": pq.is_file() and sha(pq) == pq_before,
        "csv_restored": cs.is_file() and sha(cs) == cs_before,
        "pass": code == 0 and sha(pq) == pq_before and sha(cs) == cs_before,
    }

    # ------------------------------------------------------- 4. manifest and docs deleted
    for name in ("MANIFEST.json", "START_HERE_AI.md", "DATA_DICTIONARY.md", "CHANGELOG.md"):
        (DATA / name).unlink(missing_ok=True)
    code = publish()
    scenarios["4_manifest_and_docs_deleted_are_rebuilt"] = {
        "publish_exit_code": code,
        "restored": {
            name: (DATA / name).is_file()
            for name in ("MANIFEST.json", "START_HERE_AI.md", "DATA_DICTIONARY.md", "CHANGELOG.md")
        },
        "pass": code == 0
        and all(
            (DATA / n).is_file()
            for n in ("MANIFEST.json", "START_HERE_AI.md", "DATA_DICTIONARY.md", "CHANGELOG.md")
        ),
    }

    # ------------------------------------------------------------ 5. a file is corrupted
    corrupt = DATA / "nifty500" / "parquet" / "nifty500_2020.parquet"
    good = sha(corrupt)
    corrupt.write_bytes(b"this is not a parquet file")
    code = publish()
    scenarios["5_corrupted_file_is_replaced"] = {
        "publish_exit_code": code,
        "restored_to_the_correct_bytes": sha(corrupt) == good,
        "pass": code == 0 and sha(corrupt) == good,
    }

    # ------------------------------------------- 6. an entire universe folder deleted
    import shutil

    folder = DATA / "nifty750"
    file_count_before = sum(1 for p in folder.rglob("*") if p.is_file())
    shutil.rmtree(folder)
    code = publish()
    file_count_after = sum(1 for p in folder.rglob("*") if p.is_file()) if folder.exists() else 0
    manifest = json.loads((DATA / "MANIFEST.json").read_text(encoding="utf-8"))
    scenarios["6_whole_universe_folder_deleted_is_rebuilt"] = {
        "publish_exit_code": code,
        "files_before": file_count_before,
        "files_after": file_count_after,
        "rows_after": manifest["universes"]["nifty750"]["rows"],
        # More files than before is the correct outcome, not a mismatch: an earlier scenario
        # deleted a CSV that was then honoured as absent. With the whole folder gone there is
        # nothing left to honour, so every year gets its CSV back.
        "csvs_restored_because_nothing_was_left_to_honour": file_count_after >= file_count_before,
        "pass": code == 0 and file_count_after >= file_count_before,
    }

    # ------------------------------------------------------------------ final integrity
    sys.path.insert(0, str(CODE / "src"))
    from vajra_regime.publish import verify_published  # noqa: PLC0415

    verification = verify_published(DATA)

    result = {
        "drill": "6_DELETION_RESILIENCE",
        "performed_at_utc": datetime.now(UTC).isoformat(),
        "scenarios": scenarios,
        "final_verification": {
            "pass": verification["pass"],
            "files_checked": verification["files_checked"],
            "missing": verification["missing"],
            "hash_mismatched": verification["hash_mismatched"],
            "unexpected_files": verification["unexpected_files"],
        },
    }
    result["verdict"] = (
        "PASS"
        if all(s["pass"] for s in scenarios.values()) and verification["pass"]
        else "FAIL"
    )
    out = Path(r"D:\VAJRA_ENGINE\logs\drills\drill6_deletion_resilience.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    for name, entry in scenarios.items():
        print(f"{entry['pass'] and 'PASS' or 'FAIL'}  {name}")
    print(f"final verification: {verification['pass']}")
    print(f"VERDICT: {result['verdict']}")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    os.chdir(CODE)
    sys.exit(main())
