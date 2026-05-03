# Third-Party Testing Roadmap

Last updated: 2026-05-02

This document is the continuation note for pgrust third-party compatibility
testing. It records what landed, what the current numbers mean, and what to do
next when resuming this work.

## Goal

Use external test suites to measure how close pgrust is to being a PostgreSQL
drop-in replacement.

Important constraint: third-party suites are sources of useful SQL and protocol
coverage, not behavioral authorities. When possible, expected behavior should
come from real PostgreSQL.

## What Landed

PR #124 added the initial third-party sqllogictest harness:

- `scripts/run_sqllogictest.sh`: generic sqllogictest runner against pgrust over
  pgwire.
- `scripts/convert_cockroach_logic_test.py`: lossy CockroachDB logictest to
  generic sqllogictest converter.
- `scripts/run_cockroach_logic_test.sh`: one-file Cockroach wrapper with
  optional PostgreSQL-oracle materialization.
- `scripts/run_cockroach_logic_suite.py`: manifest runner with JSON summaries,
  failure classes, prefix metrics, and record replay integration.
- `scripts/run_slt_record_replay.py`: independent per-record replay harness.
- `scripts/cockroach-logictest-presets/smoke.list`: initial 10-file smoke
  manifest.
- `docs/sqllogictest.md` and `docs/cockroach-logictest.md`: operational docs.

The merged PR was:

```text
PR #124, merge commit 2d22a930b
```

## Current Baseline

The current 10-file Cockroach smoke manifest is PostgreSQL-oracle materialized.
Cockroach contributes candidate SQL; PostgreSQL produces expected output.

Latest recorded full smoke result:

```text
file pass rate: 0 / 10
converted records: 1875
skipped records: 520
prefix progress before first file failure: 109 / 1875 = 5.8%
independent replay pass total: 599 / 1875 = 31.9%
independent replay pass among asserted targets: 599 / 851 = 70.4%
target assertion failures: 252
setup failures: 1024
unknown harness outcomes: 0
```

Interpretation:

- `0 / 10` file pass rate is expected and too coarse to guide work.
- `prefix progress` is how far sqllogictest gets before the first failure in
  each file.
- `independent replay` is the better compatibility signal: each target record
  gets a fresh pgrust server/data dir, earlier successful state-building records
  are replayed as setup, then only the target record is asserted.
- `setup_failed` means the target record was not actually measured because its
  prerequisite setup could not be replayed on pgrust.
- `pass among asserted targets` excludes setup failures and shows how often
  pgrust matched PostgreSQL when the replay reached the target assertion.

## Main Commands

Run the normal Postgres-oracle smoke suite:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir /tmp/cockroach \
  --sqllogictest-dir /tmp/sqllogictest-rs \
  --skip-build
```

Run full independent replay with parallel fresh-server isolation:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir /tmp/cockroach \
  --sqllogictest-dir /tmp/sqllogictest-rs \
  --skip-build \
  --record-replay \
  --record-replay-jobs 8
```

Run the success-path replay view:

```bash
scripts/run_cockroach_logic_suite.py \
  --cockroach-dir /tmp/cockroach \
  --sqllogictest-dir /tmp/sqllogictest-rs \
  --skip-build \
  --record-replay \
  --record-replay-jobs 8 \
  --record-replay-expected-success-only
```

Use `--limit N` and `--record-replay-limit N` for quick checks before running a
full manifest.

## Next Best Work

1. Run full success-path replay and document the baseline.

   Command:

   ```bash
   scripts/run_cockroach_logic_suite.py \
     --cockroach-dir /tmp/cockroach \
     --sqllogictest-dir /tmp/sqllogictest-rs \
     --skip-build \
     --record-replay \
     --record-replay-jobs 8 \
     --record-replay-expected-success-only
   ```

   Update `docs/cockroach-logictest.md` with the result. This gives the cleanest
   current KPI for PostgreSQL-success compatibility.

2. Add a replay-summary analyzer.

   The analyzer should read a suite `summary.json` plus nested record replay
   summaries and emit:

   - top target assertion failures by SQL prefix and error class
   - top setup failures by failed setup SQL prefix
   - per-file pass/fail/setup-failed rates
   - likely converter artifacts
   - suggested next blocker categories

3. Decide how to treat `setup_failed`.

   For product-level compatibility, setup failures are still real blockers. For
   harness planning, they should be separated from target assertion failures
   because they often mean one early missing feature hides many later records.

4. Improve harness speed without weakening the gold-standard metric.

   Current replay starts one pgrust server per measured record. `--jobs 8`
   parallelism helped, but full replay is still slow. Possible next modes:

   - group targets by identical setup prefix
   - experiment with one server plus one database per target
   - compare fast modes against fresh-server replay on small manifests before
     trusting them

5. Split manifests by category.

   The initial `smoke.list` is broad. Add category manifests such as:

   - `select.list`
   - `dml.list`
   - `types.list`
   - `joins.list`
   - `aggregates.list`
   - `arrays.list`

   This makes it easier to run focused baselines and track progress by area.

6. Start the next third-party harness after sqllogictest is stable.

   Highest-value next candidate is SQLancer, using real PostgreSQL as the
   differential reference where possible. After that, consider SQLAlchemy and
   pgjdbc for application/driver compatibility.

## Decision Points

- Should the headline KPI be all converted records, asserted targets only, or
  expected-success-only targets?
- Should exact PostgreSQL error-message compatibility be tracked as a separate
  metric from successful SQL behavior?
- When a replay summary shows one pgrust feature blocks hundreds of later
  records, should the next branch fix pgrust behavior or keep expanding the
  harness first?
- Should faster replay modes be allowed in CI, while fresh-server replay remains
  the nightly/manual gold standard?

## Pickup Checklist

When resuming this thread:

1. Start from current `perf-optimization`.
2. Confirm external checkouts exist:

   ```bash
   test -d /tmp/cockroach
   test -d /tmp/sqllogictest-rs
   ```

3. Run a quick smoke:

   ```bash
   scripts/run_cockroach_logic_suite.py \
     --cockroach-dir /tmp/cockroach \
     --sqllogictest-dir /tmp/sqllogictest-rs \
     --skip-build \
     --limit 1 \
     --record-replay \
     --record-replay-limit 20 \
     --record-replay-jobs 8
   ```

4. Run the next full baseline you care about.
5. Copy the resulting `summary.json` path into the commit or PR notes.
6. Update this roadmap or `docs/cockroach-logictest.md` with any new baseline.
