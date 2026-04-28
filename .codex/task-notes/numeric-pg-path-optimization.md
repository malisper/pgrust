Goal:
- Optimize the slow numeric regression query using PostgreSQL-shaped executor mechanisms only.

Key decisions:
- No generate_series-specific aggregate fusion or parent-node streaming shortcut.
- FunctionScan simple path now materializes one-column SRF values and reuses a scan slot, while ordinality and multi-column SRFs stay on the materialized-row fallback.
- generate_series uses value-per-call internal state for integer and numeric variants.
- Plain aggregates without grouping/HAVING/distinct/order/filter/direct args bypass group lookup/vector machinery.
- Numeric SUM uses a mutable finite accumulator and normalizes at finalization.

Files touched:
- Cargo.toml
- src/bin/numeric_query_bench.rs
- src/backend/executor/agg.rs
- src/backend/executor/nodes.rs
- src/backend/executor/srf.rs
- src/backend/executor/startup.rs
- src/include/nodes/execnodes.rs

Tests run:
- scripts/cargo_isolated.sh check --features tools --bin numeric_query_bench
- scripts/run_regression.sh --test numeric --timeout 300 --results-dir /tmp/diffs/pgrust-numeric-final
- scripts/cargo_isolated.sh test --lib --quiet generate_series
- scripts/cargo_isolated.sh test --lib --quiet aggregate
- scripts/run_regression.sh --test aggregates --timeout 300 --results-dir /tmp/diffs/pgrust-aggregates-after
- scripts/cargo_isolated.sh run --release --features tools --bin numeric_query_bench -- --dir /tmp/pgrust_numeric_bench_release_after3 --rows 100000 --iterations 30 --variant all
- scripts/cargo_isolated.sh run --release --features tools --bin numeric_query_bench -- --dir /tmp/pgrust_numeric_bench_release_1m_after --rows 1000000 --iterations 10 --variant all

Remaining:
- Upstream aggregates regression still fails on known broader unsupported cases; first mismatch is outer-level aggregate/subquery handling, not this plain aggregate path.
