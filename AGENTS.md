# Notes for anyone (or anything) working on this repository

## What this is

One job: build a point-in-time NSE equity OHLCV dataset and publish it to `D:\VAJRA_DATA\`.

On 2026-08-29 this repository was cut down from a system that also did strategy, backtesting,
screening, regime detection, forecasting, Google Sheets sync and Telegram alerts. 127 modules
were removed. **Do not add any of that back here.** If it is not about producing correct OHLCV,
membership, corporate actions or the calendar, it belongs somewhere else. `tests/
test_publish_isolation.py` fails if the removed modules reappear.

## Things that will bite you

**Paths.** Nothing hardcodes a location. `src/vajra_regime/paths.py` is the only place that
knows where anything lives, driven by `VAJRA_ENGINE_ROOT`, `VAJRA_STORE_ROOT` and
`VAJRA_DATA_ROOT`. If you write `Path("D:/...")` anywhere else, you break the ability to run
against a throwaway copy, which is how the gap-recovery drill avoids touching production.

**The store's numbered folders.** `02 Master Historical Data`, `08 Logs` and friends look ugly
and are deliberately unchanged. Every checkpoint, manifest and status file written since
2026-08 records those exact strings.

**`--maxfail=1`.** `pyproject.toml` sets it in `addopts`. Run `pytest --maxfail=0` or you will
only ever see the first failure.

**The VAJRA 750 supporting master is rebuilt from scratch every run.** All eighteen
`EOD2_Clean_*.parquet` files are rewritten daily by `rebuild_rolling_clean_data`. Any
correction applied to them must be re-applied on every run. That is why `ca_repair` is
idempotent rather than one-shot. The NIFTY 500 `certified_adjusted` partitions behave
differently: only the current year is rewritten.

**DuckDB.** `rows` is a reserved word in 1.5 - quote column aliases. `CREATE VIEW ...
read_parquet(?)` cannot be prepared; build the file list inline.

**Empty strings vs NULL.** The store writes `''` in `CorporateActionQuarantineReason`. The
publisher converts it to `NULL`, because a CSV round-trip turns `''` into `NULL` anyway and the
two formats have to agree exactly.

## Two mistakes already made here; do not repeat them

**A checksum that could not fail.** The Parquet-vs-CSV equality check originally compared
per-column `BIT_XOR(hash(...))`. It passed while 117,504 rows differed, because an even number
of identical mismatches cancels under XOR. It is now a full `EXCEPT ALL` in both directions,
and `tests/test_publish.py` has a test whose only purpose is to keep it that way.

**A year written into a glob.** The bhavcopy reader globbed a path with `2026` in it. On
2027-01-01 it would have stopped seeing new files, silently, with no error. When you write a
path that contains a date component, ask what happens when the date rolls over.

## Trusting your own status files

The quality checker deliberately re-derives everything from the published Parquet rather than
reading the build pipeline's `CERTIFIED_PASS` files. That is how the BHARTIARTL split bug was
found: the pipeline's own ledger said the split had been verified and applied, and the prices
said otherwise. If you add a check, make it read the data, not the claim about the data.

## Order of operations in a run

Fetch -> rebuild -> catch up -> 750 universe -> repair corporate actions -> health gate ->
publish -> verify.

Steps before `publish` run inside a fingerprint guard on `D:\VAJRA_DATA\`. If anything writes
there during the build, the run aborts. That guard is what makes a mid-run failure safe.

## Style

Match what is there. Long, auditable SQL is preferred over clever SQL. Every threshold gets a
comment saying why it is that number. Status files carry a `sha256` of their own payload.
Writes are atomic: temp file, then `os.replace`, never in-place.
