Goal:
Fix join_hash regression failures around hash join execution coverage, including EXPLAIN JSON batch helpers, skewed joins, and full outer joins.

Key decisions:
Allow EXPLAIN FORMAT values to be quoted so FORMAT 'json' parses like PostgreSQL.
Return EXPLAIN JSON as json-typed output so PL/pgSQL helper functions can traverse it.
Expose Hash node JSON instrumentation for Original Hash Batches and Hash Batches using pgrust's in-memory hash table plus PostgreSQL-shaped compatibility shims for underestimated and skewed joins.
Render JSON children for common executor nodes so recursive JSON plan walkers can find nested Hash nodes.
Attach captured InitPlans to EXPLAIN ANALYZE JSON at the root because pgrust records executed InitPlans outside the runtime PlanNode tree.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/commands/tablecmds.rs
src/backend/commands/explain.rs
src/backend/tcop/postgres.rs
src/backend/executor/hashjoin.rs
src/backend/executor/node_hash.rs
src/backend/executor/node_hashjoin.rs
src/backend/executor/node_mergejoin.rs
src/backend/executor/nodes.rs
src/backend/executor/startup.rs
src/include/nodes/execnodes.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-join-hash scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-join-hash scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
CARGO_TARGET_DIR=/tmp/pgrust-target-join-hash scripts/cargo_isolated.sh test --lib --quiet manual_hash_join_full_emits_unmatched_rows_from_both_sides
PGRUST_STATEMENT_TIMEOUT=20 CARGO_TARGET_DIR=/tmp/pgrust-target-join-hash scripts/run_regression.sh --port 55438 --test join_hash --timeout 180 --jobs 1

Remaining:
join_hash still differs in 20 queries, all inspected diffs are planner/EXPLAIN-shape differences: PostgreSQL parallel Gather/Parallel Hash nodes, full outer join input ordering for r.id = 0 - s.id, later subplan join planning, and LATERAL Memoize/Subquery Scan shape.
