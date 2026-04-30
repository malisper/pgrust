Goal:
Make `scripts/run_regression.sh --test updatable_views --timeout 120 --port <free-port>` pass without updating expected output.

Key decisions:
- Added `ALTER VIEW ... ALTER COLUMN ... SET/DROP DEFAULT` and `ALTER VIEW RESET (...)` parsing through existing ALTER TABLE paths.
- Added `pg_relation_is_updatable(regclass,bool)` and `pg_column_is_updatable(regclass,smallint,bool)` using the information-schema updatability logic.
- Added missing `cos(float8)` builtin needed by this regression.
- Stored and read view `check_option` reloptions; reset now stores an empty reloptions array so stale stored SQL suffixes do not re-enable checks.
- Added PostgreSQL-style rule classification for unconditional/conditional DO INSTEAD and DO ALSO metadata behavior; MERGE now rejects relations with DML rules.
- Applied view defaults during auto-view INSERT/MERGE INSERT mapping and tracked raw DEFAULT positions.
- Relaxed duplicate assignment rejection for distinct subscripts/fields while preserving duplicate whole-column rejection.
- Improved generated-column DEFAULT errors, ON CONFLICT arbiter binding through renamed view columns, nested view metadata recursion, and partitioned base relkind metadata.
- Fixed MERGE RETURNING old/new absent pseudo rows for auto-updatable views; view pseudo-row expressions now become NULL when the base pseudo row is absent.
- Rewrote local Vars inside scalar subqueries when building view pseudo-row output expressions, so `old` view expressions use old base values.
- Propagated scalar-subquery output names recursively so `(SELECT (SELECT new))` names like PostgreSQL.
- Nested unsupported-view errors now carry the failing inner view name, so nested read-only view DML reports the inner view like PostgreSQL.
- Fixed parser handling for `-array_col[subscript]` without changing grammar precedence globally; this removes the unary-minus-on-array failure in WCO array assignment.
- Rule-updatable view metadata now propagates unconditional INSERT/UPDATE/DELETE rule column capabilities through nested views, while `information_schema.columns.is_updatable` only uses true update capability.
- EXPLAIN auto-view rewrite errors now print the semantic detail instead of leaking `ViewDmlRewriteError` debug output.
- Reverted unsafe DO ALSO runtime and stored-query fallback experiments after they caused timeouts/worse diffs.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_agg_support.rs
src/backend/parser/analyze/agg_output_special.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/parser/analyze/query.rs
src/backend/parser/analyze/system_views.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/rewrite/mod.rs
src/backend/rewrite/view_dml.rs
src/backend/rewrite/views.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/pgrust/database/commands/alter_column_options.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/rules.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
TMPDIR="/Volumes/OSCOO PSSD/tmp" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/rust/seoul-v2-target" cargo check --lib --quiet
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65443 --results-dir /tmp/pgrust_regress_updatable_views_seoul8  # 974/1139, 1918 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65444 --results-dir /tmp/pgrust_regress_updatable_views_seoul9  # 975/1139, 1892 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65446 --results-dir /tmp/pgrust_regress_updatable_views_seoul10 # 976/1139, 1881 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65448 --results-dir /tmp/pgrust_regress_updatable_views_seoul11 # 977/1139, 1872 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65452 --results-dir /tmp/pgrust_regress_updatable_views_seoul13 # 977/1139, 1871 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65456 --results-dir /tmp/pgrust_regress_updatable_views_seoul15 # 977/1139, 1871 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65458 --results-dir /tmp/pgrust_regress_updatable_views_seoul16 # 979/1139, 1854 lines
scripts/cargo_isolated.sh test --lib --quiet parse_array_subscript_expressions_and_targets # passed with fresh target after one corrupted dyld artifact
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65462 --results-dir /tmp/pgrust_regress_updatable_views_seoul18 # 981/1139, 1845 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65464 --results-dir /tmp/pgrust_regress_updatable_views_seoul19 # 986/1139, 1810 lines
scripts/run_regression.sh --test updatable_views --timeout 120 --port 65466 --results-dir /tmp/pgrust_regress_updatable_views_seoul20 # 988/1139, 1780 lines

Remaining:
- Still failing overall: latest `/tmp/pgrust_regress_updatable_views_seoul20`, 988/1139 queries matched, 1780 diff lines.
- Early display/catalog mismatch: DROP SEQUENCE CASCADE misses ro_view19 notice.
- MERGE ordering/visibility: NOT MATCHED BY SOURCE ordering differs for one DELETE row; EXPLAIN output still differs for rewritten view DML.
- DO ALSO rule side effects are still missing.
- Nested rule/trigger views with LIMIT/OFFSET still error instead of routing through rules/triggers.
- Check-option enforcement is incomplete for MERGE, nested views, partition routing, INSTEAD-rule cases, and some whole-row checks.
- ON CONFLICT view permission checks and whole-row expression views still differ.
