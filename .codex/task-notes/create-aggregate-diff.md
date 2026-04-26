Goal:
Fix PostgreSQL parity for create_aggregate regression diffs.

Key decisions:
- Preserve PostgreSQL behavior; do not update expected output.
- Model aggregate signatures with ordinary args plus optional ORDER BY args.
- Use existing function resolution for support functions, then reject matches
  that would require run-time coercion.
- Store aggregate metadata in pg_proc, pg_aggregate, and dependency rows.
- Forward parser-thread notices so quoted aggregate option warnings reach
  clients.
- Collapse duplicate pg_namespace rows during scans so psql catalog joins do not
  multiply aggregate describe output.

Files touched:
- Parser AST, grammar, and parser tests.
- Aggregate creation/drop/catalog code.
- pg_proc and pg_aggregate builtin/catalog metadata.
- Executor display/runtime helpers and protocol notice/error handling.

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet create_aggregate
- CARGO_TARGET_DIR=/tmp/pgrust-target-create-aggregate-fresh2 scripts/run_regression.sh --schedule /tmp/pgrust-schedules/create_aggregate.schedule --test create_aggregate --results-dir /tmp/pgrust_regress_create_aggregate_fresh6 --timeout 240 --ignore-deps

Remaining:
- Full upstream-schedule validation is still blocked by unrelated create_index
  base setup failures in this workspace.
