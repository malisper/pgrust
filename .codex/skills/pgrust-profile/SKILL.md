---
name: pgrust-profile
description: Build profiling repros for pgrust when the user asks for a profile, hotspot analysis, dtrace command, benchmark harness, or flamegraph-style stack sampling. Use this to create or update a small benchmark binary or script, then give the user a concrete dtrace command that runs the benchmark under profiling and writes the output file for later analysis.
---

# Pgrust Profiling

Use this skill when the user asks for:
- a profile
- hotspot analysis
- a `dtrace` command
- a benchmark harness for profiling
- a reproducible command to run under `dtrace`

## Default approach

Prefer an in-process Rust benchmark binary under `src/bin/` that:
- opens `Database` directly
- creates or reuses the needed table(s)
- seeds deterministic data
- runs the target query or workload `N` times
- prints simple timing/counter output

Use the existing benchmark binaries when they already match the request:
- [src/bin/inproc_query_bench.rs](../../src/bin/inproc_query_bench.rs)
- [src/bin/full_scan_bench.rs](../../src/bin/full_scan_bench.rs)
- [src/bin/bench_insert.rs](../../src/bin/bench_insert.rs)

Only use a TCP/server benchmark if the user explicitly wants wire-protocol costs included.

## Output expectations

After creating or updating the benchmark, give the user:

1. the build command
2. a single ready-to-run `dtrace -c` command
3. the expected output file path

Use this command shape by default:

```bash
cargo build --release --bin <bench_bin>

sudo rm -f /tmp/pgrust_dtrace.out && \
sudo dtrace -x ustackframes=100 \
  -n 'profile-997 /execname == "<bench_bin>"/ { @[ustack()] = count(); }' \
  -c "./target/release/<bench_bin> <bench args>" \
  -o /tmp/pgrust_dtrace.out && \
bench/analyze_profile.sh /tmp/pgrust_dtrace.out
```

Notes:
- Use `execname == "<bench_bin>"` for `dtrace -c` benchmarks.
- Keep the benchmark command self-contained so `dtrace` can launch it directly.
- Default output file is `/tmp/pgrust_dtrace.out` unless the user asks otherwise.

## Implementation rules

- Keep the benchmark narrow and deterministic.
- Prefer direct `db.execute(...)` loops over shelling out to `psql`.
- Put reusable benchmarking logic in Rust, not in large shell scripts.
- Add flags for the obvious tuning knobs:
  - `--dir`
  - `--rows`
  - `--iterations`
  - `--query` or focused workload flags like `--count`
- If the request is about a specific query shape, make that query configurable.

## Validation

For a new benchmark binary:
- run `cargo check --bin <bench_bin>`

If the user wants a full profile flow, also provide:
- `cargo build --release --bin <bench_bin>`

## Reporting

When analyzing a produced profile:
- report the profile file path
- summarize top hotspots
- show the hottest relevant call path
- separate read-side and write-side catalog costs when applicable
