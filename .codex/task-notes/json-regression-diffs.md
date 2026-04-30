Goal:
Fix remaining PostgreSQL JSON/jsonb regression gaps that are local to parser,
catalog, executor, and value semantics.

Key decisions:
Keep plain json raw text preservation separate from jsonb semantic comparison.
Wire missing builtins through pg_proc plus analyzer/executor paths instead of
special-casing only parser output. Compare jsonb aggregate/group/hash/btree keys
with decoded jsonb semantics so numeric display-scale differences do not split
equal values.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/access/hash/support.rs
src/backend/access/nbtree/nbtcompare.rs
src/backend/executor/agg.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_json.rs
src/backend/executor/expr_string.rs
src/backend/executor/jsonb.rs
src/backend/executor/nodes.rs
src/backend/executor/tests.rs
src/backend/parser/analyze/agg.rs
src/backend/parser/analyze/agg_output.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/expr/json.rs
src/backend/parser/analyze/expr/ops.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/gram.rs
src/backend/rewrite/views.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh check
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet jsonb_delete_path_operator_and_concat_unknown_literals_work
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet hash_value_matches_semantic_jsonb_numeric_equality
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/run_regression.sh --skip-build --test json --results-dir /tmp/pgrust-json-rebuilt --timeout 180 --port 55524
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/run_regression.sh --test jsonb --results-dir /tmp/pgrust-jsonb-final2 --timeout 240 --port 55534

Remaining:
json: 467/470 matched; only extra LINE/caret diagnostics remain for two
json_populate_record timestamp errors and json_object_agg_unique duplicate-key
error.

jsonb: 1054/1084 matched. Remaining failures are concentrated in GIN/jsonb_path_ops
catalog and bitmap-index planner support, JSONPATH EXPLAIN deparse formatting,
GIN-index containment for numeric 25 vs 25.0, jsonb_pretty psql divider widths,
some exact JSONB path/subscript error detail/caret formatting, scalar jsonb
subscript update edge cases, repeat(text,int) in the subscript section, and
SQL/JSON/jsonpath planner gaps.
