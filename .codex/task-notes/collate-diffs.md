Goal:
Fix high-impact collate regression diffs caused by dropped expression/column collation metadata.
Key decisions:
- Added analyzer-local derived collation state to distinguish default, implicit, explicit, and conflict.
- Wired expression consumers (ORDER BY, comparisons, LIKE/SIMILAR, scalar-array and row-valued subquery comparisons, ordered-set aggregate ordering, string_agg args) to derive from bound expressions instead of raw type defaults.
- Re-enabled set-operation output collation resolution where PostgreSQL needs it, while treating default collation as weaker than non-default implicit collation.
- Preserved VALUES-derived collations in range-table descriptors so recursive CTE collation mismatch diagnostics see the anchor term's real collation.
- Added CTAS validation for unresolved collatable set-operation output columns.
- Tightened CREATE/DROP COLLATION behavior for builtin provider options, libc-style LC_COLLATE/LC_CTYPE creation, copying "default", and table-column dependencies.
- Included schema-owned collations in DROP SCHEMA CASCADE notices and cleanup.
- Fixed psql describe-column collation display to return non-default column collation names.
Files touched:
- crates/pgrust_analyze/src/agg_output.rs
- crates/pgrust_analyze/src/collation.rs
- crates/pgrust_analyze/src/expr.rs
- crates/pgrust_analyze/src/expr/ops.rs
- crates/pgrust_analyze/src/expr/subquery.rs
- crates/pgrust_analyze/src/agg_output_special.rs
- crates/pgrust_analyze/src/lib.rs
- crates/pgrust_analyze/src/query.rs
- crates/pgrust_analyze/src/scope.rs
- crates/pgrust_commands/src/collation.rs
- src/backend/tcop/postgres.rs
- src/pgrust/database/commands/collation.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database/commands/drop.rs
- src/backend/parser/tests.rs
Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_invalid_collation_usage
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --port 55433 --test collate --timeout 120 --results-dir /tmp/pgrust-collate-results
Remaining:
- collate regression still fails: 121/144 queries matched, 205 diff lines.
- Remaining diffs are explain/view/index rendering, error LINE/HINT/caret formatting, and intentional message wording differences for implicit mismatch vs "could not determine".
