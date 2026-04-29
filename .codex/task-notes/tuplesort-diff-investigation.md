Goal:
Fix all independent causes behind the tuplesort regression diff in /tmp/diffs.

Key decisions:
- Numeric `to_char` now treats `FM` as a fill-mode token anywhere in the numeric picture, including trailing `000000000000FM`.
- LIMIT/OFFSET parsing folds simple integer constant expressions while preserving `LIMIT NULL`.
- Set-operation type resolution coerces raw NULL literals to peer column types, with all-NULL columns still resolving to text.
- EXPLAIN can wrap DECLARE CURSOR and explains only the cursor query.
- Materialized cursor scroll positions now distinguish "on last row" from "after end".
- CLUSTER ... USING now rewrites heap storage in PostgreSQL tuplesort peer order, rebuilds indexes, updates `indisclustered`, and tracks temp relfilenode replacement for rollback.
- Planner GUCs for nestloop/hashjoin/mergejoin/material are wired through path generation.
- INNER/LEFT/RIGHT JOIN USING bind merged columns to the visible side var; FULL JOIN still uses COALESCE.
- Mark/restore explain shape now supports descending merge pathkeys, sorted DISTINCT aggregate incremental sort, Merge Cond rendering, and aggregate display cleanup.
- Added `percentile_disc` and fixed hypothetical aggregate direct-arg coercion needed by tuplesort.
- Added logical TID `Value::Tid` support for ctid formatting/comparison.

Files touched:
- SQL grammar/AST/parser, analyzer/coercion/scope, planner path/explain, executor aggregate/cursor/value formatting, catalog/storage/session plumbing, and database CLUSTER command support.
- Main new file: `src/pgrust/database/commands/cluster.rs`.

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check --lib`
- `scripts/cargo_isolated.sh test --lib --quiet cluster_temp_table`
- `scripts/cargo_isolated.sh test --lib --quiet merge_join`
- `scripts/cargo_isolated.sh test --lib --quiet planner_join_gucs_and_using_binding_affect_explain_shape`
- `scripts/cargo_isolated.sh test --lib --quiet materialized_scroll_distinguishes_last_row_from_after_end`
- `scripts/cargo_isolated.sh test --lib --quiet percentile_disc_ordered_set_aggregate_returns_discrete_value`
- `scripts/cargo_isolated.sh test --lib --quiet hypothetical_set_aggregates_support_multicolumn_ordering_and_empty_percent_rank`
- `scripts/cargo_isolated.sh test --lib --quiet to_char`
- `scripts/run_regression.sh --test tuplesort --results-dir /tmp/diffs/tuplesort-after19 --timeout 180 --port 62483`

Remaining:
- tuplesort passes: 108/108 queries matched in `/tmp/diffs/tuplesort-after19`.
