# Benchmark Harness

This repo already had several one-off benchmarks, but no single local entrypoint
for collecting stable artifacts over time. `scripts/run_bench.py` is the first
pass at that local harness.

## What it runs

- `select-wire`
  Compares pgrust and PostgreSQL over the wire using the existing
  `bench/bench_select_wire.sh` script.
- `pgbench`
  Uses the real upstream `pgbench` client with custom script files under
  `bench/sql/` against both engines.
- `pgbench-like`
  Runs the existing in-repo `pgbench_like.rs` binary as a pgrust-only internal
  benchmark.
- `sysbench`
  Runs upstream sysbench OLTP Lua workloads through the PostgreSQL driver
  against pgrust and PostgreSQL. This suite is opt-in because it requires the
  external `sysbench` binary.

## Why both `pgbench` and `pgbench_like`

- Real `pgbench` is the comparable signal.
  It is the upstream PostgreSQL benchmark client, so the same tool can drive
  both pgrust and PostgreSQL side by side.
- `pgbench_like.rs` is still useful as an internal signal.
  It exercises pgrust directly and can keep giving performance signal even when
  wire-protocol or SQL compatibility gaps prevent the real `pgbench` path from
  succeeding.

The intent is to keep both for now and let the harness record where the real
`pgbench` path works, where it fails, and how the internal pgrust-only signal
changes over time.

## Usage

Run all current suites and write JSON results:

```bash
python scripts/run_bench.py
```

Run just the real `pgbench`-backed suite:

```bash
python scripts/run_bench.py --suite pgbench
```

Run only against PostgreSQL:

```bash
python scripts/run_bench.py --engines postgres
```

Run one workload shape:

```bash
python scripts/run_bench.py --suite pgbench --pgbench-workload point-select
```

Run a sysbench OLTP workload:

```bash
python scripts/run_bench.py --suite sysbench --sysbench-workload point-select
```

Run a tiny sysbench smoke:

```bash
python scripts/run_bench.py --suite sysbench --sysbench-workload point-select --sysbench-table-size 20 --sysbench-events 1 --clients 1
```

Pass an upstream sysbench Lua option to both engines:

```bash
python scripts/run_bench.py --suite sysbench --sysbench-workload read-only --sysbench-option=--distinct_ranges=0
```

Run with no warmup:

```bash
python scripts/run_bench.py --suite pgbench --pgbench-warmup-transactions 0
```

Choose a stable results directory:

```bash
python scripts/run_bench.py --results-dir /tmp/pgrust-bench-run
```

The runner writes:

- `summary.json` with machine-readable metrics
- `environment` in `summary.json` with tool versions, platform info, and
  benchmark binary metadata
- `fairness` in `summary.json` with the comparison assumptions and caveats
- `comparisons` in `summary.json` with pgrust/PostgreSQL throughput and
  latency ratios when both engines run the same workload
- `artifacts/*.stdout.txt` and `artifacts/*.stderr.txt` with raw command output

It also prints a compact report table after each run. To render a report from
an existing summary without rerunning benchmarks:

```bash
python scripts/run_bench.py --report-json /tmp/pgrust-bench-run/summary.json
```

Use `--no-report` when you only want JSON and artifacts.

Record a run into local history:

```bash
python scripts/run_bench.py --suite pgbench --history-dir .bench-history --history-label local
```

The runner writes timestamped summaries under `.bench-history/runs/` and updates
`.bench-history/index.json`. The `.bench-history/` directory is ignored by git;
it is meant for local iteration, not checked-in benchmark data.

Render recent local history:

```bash
python scripts/run_bench.py --report-history .bench-history
```

Generate a standalone local dashboard:

```bash
python scripts/run_bench.py --report-history .bench-history --history-dashboard .bench-history/dashboard.html
```

Check the latest run for local regressions:

```bash
python scripts/run_bench.py --check-history-regressions .bench-history --regression-threshold-percent 5
```

The history report shows recent runs plus the latest pgrust/PostgreSQL ratios
and the delta from the most recent earlier recorded run with the same workload.
This is intentionally lightweight tracking; dashboard publishing and regression
alerting are separate later phases.

The regression check uses the same ratio history. A throughput-ratio drop larger
than the threshold is a regression, and a latency-ratio increase larger than the
threshold is a regression. It exits non-zero only when a regression is detected,
so it can be used manually now and wired into automation later.

By default it also builds benchmark binaries into the bounded pgrust target
pool under `/tmp`, for example `/tmp/pgrust-target-pool/pgrust/<slot>`,
instead of the repo's single shared `/tmp/pgrust-target`. That reduces cargo
lock contention with other agents working in parallel while still reusing
artifacts within each pool slot. Use `PGRUST_TARGET_POOL_SIZE`,
`PGRUST_TARGET_POOL_DIR`, or `PGRUST_TARGET_SLOT` to tune slot count, location,
or explicit assignment.

By default, if a PostgreSQL comparison is requested, the runner creates a
temporary local PostgreSQL cluster under the results directory and stops it at
the end of the run. Use `--external-postgres` when you want to point at an
already-running PostgreSQL server.

For `pgbench` workloads, the runner resets the benchmark tables before each
workload by default. This keeps write-heavy workloads from changing the input
state seen by later workloads. Use `--reuse-pgbench-data` when you want faster
iteration and accept shared state across workloads.

## pgbench workloads

- `scan-count`
  Repeated `count(*)` over the benchmark table.
- `point-select`
  Single-row lookup by id.
- `range-select`
  Small range predicate over id.
- `activity-count`
  Count rows in an activity bucket, a common dashboard/filter shape.
- `feed-page`
  Keyset-style page over increasing ids with `ORDER BY id LIMIT 20`.
- `top-touched`
  Top-N ordered read over an activity counter.
- `event-join`
  Bounded parent/event join over `scanbench` and `scanbench_events`.
- `insert-only`
  Append-only event inserts.
- `read-write`
  Short transaction with lookup, update, and event insert.
- `mixed-oltp`
  Weighted mix of point reads, updates, and inserts.

These are intentionally simple, common shapes. They are meant to establish a
fair local comparison loop before adding heavier benchmark families.

The fixture data seeds `scanbench.touched` across 10 buckets and inserts one
seed event per row in `scanbench_events`. Workloads that need secondary access
paths create matching indexes for both pgrust and PostgreSQL during the
per-workload setup.

## sysbench workloads

- `point-select`
  Upstream `oltp_point_select` single-row OLTP reads.
- `read-only`
  Upstream `oltp_read_only` transaction mix.
- `read-write`
  Upstream `oltp_read_write` transaction mix.
- `write-only`
  Upstream `oltp_write_only` transaction mix.
- `insert`
  Upstream `oltp_insert` append workload.
- `update-index`
  Upstream `oltp_update_index` indexed update workload.
- `update-non-index`
  Upstream `oltp_update_non_index` non-indexed update workload.

The sysbench suite disables sysbench prepared statements for both engines with
`--db-ps-mode=disable` so the comparison exercises the same simple PostgreSQL
wire path pgrust is most likely to support today. It also uses
`--auto_inc=off` to avoid PostgreSQL `SERIAL` setup differences.

Use `--sysbench-option=--name=value` for upstream Lua knobs when pgrust does
not yet support a default sysbench query shape. For example, upstream
`read-only` and `read-write` include a `SELECT DISTINCT ... ORDER BY` query
that pgrust currently reports as unsupported; `--sysbench-option=--distinct_ranges=0`
keeps the rest of the transaction mix comparable across both engines.

Install sysbench locally before running this suite. On macOS with Homebrew:

```bash
brew install sysbench
```

## Manual only

The benchmark harness is intentionally not wired into GitHub Actions or merge
queue checks. Run it manually when you want local performance data.

## Current scope

This is intentionally local-only for now:

- no CI integration
- no dashboard publishing
- no regression alerting
- no checked-in benchmark history

Those come later once the local harness and result format settle down.
