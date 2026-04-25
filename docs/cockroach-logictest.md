# CockroachDB Logictest Subsets

CockroachDB's `pkg/sql/logictest/testdata/logic_test/*` corpus is valuable,
but it is not the same format as plain `.slt` files. This repo now ships a
lossy converter plus a runner wrapper so we can reuse the PG-ish subset of a
Cockroach logic test file against pgrust.

Files:

- `scripts/convert_cockroach_logic_test.py`
- `scripts/run_cockroach_logic_test.sh`
- `scripts/run_cockroach_logic_suite.py`
- `scripts/run_slt_record_replay.py`
- `scripts/cockroach-logictest-presets/smoke.list`

## What the converter does

It keeps the records that map cleanly onto generic `sqllogictest`:

- `statement ok`
- `statement error`
- `query <types>`
- `query error`
- `rowsort` / `valuesort`

It intentionally skips records that are hard-wired to Cockroach features or
custom harness directives, such as:

- `skipif` / `onlyif`
- `subtest`
- index hints like `@foo`, `@[42]`, `@{FORCE_INDEX=...}`
- obvious Cockroach-only SQL forms like `:::` casts

This makes the conversion useful for harvesting compatibility signal, not for
reproducing Cockroach's exact test semantics.

## Usage

Run a named Cockroach logictest file:

```bash
scripts/run_cockroach_logic_test.sh --test float
```

Run an explicit file path:

```bash
scripts/run_cockroach_logic_test.sh \
  --file ../cockroach/pkg/sql/logictest/testdata/logic_test/float
```

Keep the converted temporary `.slt` file around for inspection:

```bash
scripts/run_cockroach_logic_test.sh --test float --keep-converted
```

Use real PostgreSQL as the oracle before comparing pgrust:

```bash
scripts/run_cockroach_logic_test.sh --test float --postgres-oracle
```

In this mode the flow is:

1. Convert the Cockroach logictest file into a generic `.slt` subset.
2. Drop Cockroach records that expected errors before materialization, because
   `sqllogictest --override` rewrites result output but does not flip an
   expected-error record to expected-success if PostgreSQL accepts it.
3. Run the converted file against a temporary PostgreSQL cluster with
   `--override` so the expected rows come from PostgreSQL.
4. Run pgrust against that PostgreSQL-derived `.slt`.

This keeps Cockroach in the role we want: source of useful candidate SQL, not
the behavioral authority.

## Batch smoke suite

Run the default smoke manifest:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir ../cockroach \
  --sqllogictest-dir ../sqllogictest-rs
```

The suite runner uses `--postgres-oracle` by default. To debug the raw
Cockroach expectations instead, pass `--no-postgres-oracle`.

The default manifest is:

```text
scripts/cockroach-logictest-presets/smoke.list
```

It currently includes:

- `select`
- `float`
- `insert`
- `delete`
- `aggregate`
- `array`
- `join`
- `cast`
- `case`
- `distinct`

Use `--limit N` for quick harness checks:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir ../cockroach \
  --sqllogictest-dir ../sqllogictest-rs \
  --skip-build \
  --limit 2
```

Measure records independently with replay mode:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir ../cockroach \
  --sqllogictest-dir ../sqllogictest-rs \
  --skip-build \
  --record-replay \
  --record-replay-limit 20 \
  --record-replay-jobs 8
```

Measure only PostgreSQL-materialized success records:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir ../cockroach \
  --sqllogictest-dir ../sqllogictest-rs \
  --skip-build \
  --record-replay \
  --record-replay-expected-success-only \
  --record-replay-jobs 8
```

Each test gets its own pgrust data directory, converted `.slt` file, runner
log, and sqllogictest results directory under the suite results directory.
Ports are derived from `--base-port` and `--postgres-base-port` plus the test
index so each file can run in a clean server process.

The suite runner writes a machine-readable summary:

```text
<results-dir>/summary.json
```

The summary includes:

- `manifest`
- `results_dir`
- `postgres_oracle`
- per-test `status`, `exit_code`, `converted`, `skipped`, `log_path`, and
  `first_failure`
- per-test `failure_class`, which buckets the first blocker into categories
  like result mismatch, unsupported feature, PostgreSQL expected-error
  mismatch, unexpected pgrust error, or harness/server failure
- aggregate totals for passed, failed, converted, and skipped records
- per-test `record_progress`, which reports how many records passed before the
  first failing record in that `.slt` file
- aggregate `record_prefix_pass_percent`, which is a prefix-progress metric,
  not a fully independent per-record pass rate

`sqllogictest` stops executing a file at the first failing record, so
`record_progress` deliberately answers "how far did pgrust get before the first
mismatch?" A true independent record pass rate would need a heavier replay
harness because later records often depend on earlier schema and data setup.

`--record-replay` enables that heavier mode. For each target record it starts a
fresh pgrust database, replays earlier successful state-building records as
unchecked setup, then asserts only the target record against the
PostgreSQL-materialized expectation. Its summary reports:

- `pass`: target records that matched PostgreSQL
- `fail`: target records that reached the target assertion but did not match
- `setup_failed`: target records whose prerequisite setup could not be replayed
  on pgrust
- `unknown`: harness failures that were not attributable to setup or target
  assertion lines
- `pass_percent_of_total` and `pass_percent_of_asserted`

This is intentionally opt-in because it starts pgrust once per measured record.
Use `--record-replay-limit` for quick samples before running a whole manifest.
Use `--record-replay-jobs N` to run independent record checks in parallel with
the same fresh-server-per-record isolation semantics.
Use `--record-replay-expected-success-only` when you want a success-path
compatibility view that ignores PostgreSQL-expected error records. This avoids
counting exact error-message differences as SQL success-path failures.

Current `select` 20-record sample on this branch:

- serial replay: `45.939s`, `19 / 20` passing
- `--jobs 4`: `15.335s`, `19 / 20` passing
- `--jobs 8`: `9.219s`, `19 / 20` passing

The serial, 4-job, and 8-job runs matched record-for-record on index, SQL, and
status for that sample.

## Current status

The first useful target is `float`:

- it contains enough ordinary SQL to be meaningful
- it also contains enough Cockroach-only syntax that the converter gets
  exercised immediately

Current observed signals on this branch without `--postgres-oracle`:

- `float`: converts `31` records and skips `10`; the first real mismatch is
  that pgrust currently allows `-0::float` alongside `0` in a unique
  constraint where Cockroach expects a duplicate-key error.
- `select`: converts `174` records and skips `34`; the first real mismatch is
  that pgrust currently accepts `WHERE 'hello'` instead of rejecting it as a
  boolean type error.

Current observed signals with `--postgres-oracle`:

- `float`: converts `26` success records and skips `15`; PostgreSQL rewrites
  Cockroach-specific expectations such as `f::string` into PostgreSQL errors.
  The first pgrust mismatch is `SELECT * FROM i WHERE f = 0`, where PostgreSQL
  returns both `-0` and `0` and pgrust currently returns only `0`.
- `select`: converts `143` success records and skips `65`; PostgreSQL
  materialization succeeds. The first pgrust mismatch is
  `SELECT * FROM abc WHERE a = NULL`, where pgrust currently errors with an
  `integer = text` operator lookup failure instead of matching PostgreSQL's
  result.

The current Postgres-oracle smoke manifest result on this branch is:

- total files: `10`
- passed files: `0`
- failed files: `10`
- converted records: `1875`
- skipped records: `520`
- prefix-passed records before first failures: `109 / 1875` (`5.8%`)
- failure classes:
  - `postgres_expected_error_mismatch`: `4`
  - `result_mismatch`: `2`
  - `unexpected_pgrust_error`: `1`
  - `unsupported_feature`: `3`
- likely converter artifacts among first failures: `1`
- independent replay with `--record-replay-jobs 8`:
  - pass total: `599 / 1875` (`31.9%`)
  - pass among asserted target records: `599 / 851` (`70.4%`)
  - target assertion failures: `252`
  - setup failures: `1024`
  - unknown harness outcomes: `0`
  - sum of per-file replay elapsed time: `1144.836s`

Current per-file prefix progress:

- `select`: `10 / 143` (`7.0%`)
- `float`: `7 / 26` (`26.9%`)
- `insert`: `42 / 109` (`38.5%`)
- `delete`: `29 / 98` (`29.6%`)
- `aggregate`: `1 / 590` (`0.2%`)
- `array`: `7 / 402` (`1.7%`)
- `join`: `1 / 175` (`0.6%`)
- `cast`: `4 / 279` (`1.4%`)
- `case`: `3 / 19` (`15.8%`)
- `distinct`: `5 / 34` (`14.7%`)

Current per-file independent replay progress:

- `select`: `81 / 143` (`56.6%`), `50` fail, `12` setup-failed
- `float`: `7 / 26` (`26.9%`), `2` fail, `17` setup-failed
- `insert`: `42 / 109` (`38.5%`), `9` fail, `58` setup-failed
- `delete`: `29 / 98` (`29.6%`), `5` fail, `64` setup-failed
- `aggregate`: `93 / 590` (`15.8%`), `11` fail, `486` setup-failed
- `array`: `92 / 402` (`22.9%`), `44` fail, `266` setup-failed
- `join`: `127 / 175` (`72.6%`), `29` fail, `19` setup-failed
- `cast`: `105 / 279` (`37.6%`), `83` fail, `91` setup-failed
- `case`: `10 / 19` (`52.6%`), `7` fail, `2` setup-failed
- `distinct`: `13 / 34` (`38.2%`), `12` fail, `9` setup-failed

That baseline is expected to fail today. The value is that the failures are now
stable, PostgreSQL-anchored, and summarized in one artifact so we can expand
coverage without first fixing pgrust behavior.

Prefer `--postgres-oracle` when using Cockroach tests for PostgreSQL drop-in
compatibility work. The raw mode is still useful for debugging the converter,
but Cockroach is not the behavioral authority for pgrust.

This is not a full Cockroach corpus runner yet. It is an extraction path for
turning one large real-world compatibility corpus into something pgrust can
measure incrementally.
