Goal:
- Fix btree_index regression diffs around row-comparison index quals and oidvector array EXPLAIN text.

Key decisions:
- Allow row-comparison btree quals to match row fields against non-consecutive index columns by carrying matched index positions in synthetic row scan keys.
- Render synthetic row scan-key display expressions before normal attribute lookup.
- Preserve ScalarArrayOp display expressions for EXPLAIN so catalog-aware types like oidvector[] and oid[] survive.
- Quote array literal elements containing whitespace/braces for PostgreSQL-style array EXPLAIN output.

Files touched:
- crates/pgrust_optimizer/src/path/costsize.rs
- src/backend/executor/nodes.rs
- crates/pgrust_commands/src/explain_expr.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test btree_index --port 55453 --results-dir /tmp/pgrust-btree-index-regress

Remaining:
- None for the reported btree_index diffs.
