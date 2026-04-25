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

The history report shows recent runs plus the latest pgrust/PostgreSQL ratios
and the delta from the most recent earlier recorded run with the same workload.
This is intentionally lightweight tracking; dashboard publishing and regression
alerting are separate later phases.

By default it also builds benchmark binaries into a worktree-local
`.bench-target/` directory instead of the repo's shared `/tmp/pgrust-target`.
That avoids cargo lock contention with other agents working in parallel.

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
- `insert-only`
  Append-only event inserts.
- `read-write`
  Short transaction with lookup, update, and event insert.
- `mixed-oltp`
  Weighted mix of point reads, updates, and inserts.

These are intentionally simple, common shapes. They are meant to establish a
fair local comparison loop before adding heavier benchmark families.

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
