Goal:
Summarize distinct failure reasons and implement the first fix slices for /tmp/diffs/partition_prune.diff.

Key decisions:
Treated hunk counts as the main frequency unit, with explicit emitted error counts called out separately where one root cause cascades.
Follow-up investigation found four main partition-pruning causes:
- pgrust strips casts/collations and compares generic Values instead of requiring a PostgreSQL partition opfamily-compatible operator/support proc. This over-prunes cases like a = 1::numeric.
- range IS NOT NULL handling returns true only for default range partitions, pruning normal range partitions incorrectly.
- nested/default partition pruning does not combine the query qual with inherited parent partition constraints, so OR/default cases keep or drop the wrong subpartitions.
- there is no PostgreSQL-style runtime partition-prune plan state/Subplans Removed behavior; Param/InitPlan/prepared-statement cases mostly fall back to normal plan shape or fail earlier.
Implementation slices completed:
- Added PREPARE parameter type lists, EXECUTE args, EXPLAIN EXECUTE parsing, and prepared SELECT/UPDATE execution via session-level parameter substitution.
- Fixed range IS NOT NULL to keep non-default range partitions.
- Made partition key cast matching conservative: integer-family casts still prune, numeric casts do not.
- Added regex scalar-array support for ~ ANY and !~ ALL.
- Added non-ANALYZE EXPLAIN DELETE support.
- Added Append/MergeAppend partition-prune metadata through path, plan, setrefs, subselect finalization, executor startup, and executor EXPLAIN state.
- Added executor startup pruning over stored child bounds using runtime-evaluable clauses; static planner-pruned children are no longer reported as `Subplans Removed`.
- Tightened nested/default pruning with relation-own ancestor bound checks, single-key default-domain intersection, conservative multi-key default proof, and ancestor-aware list child pruning.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs
src/backend/optimizer/partition_prune.rs
src/backend/parser/analyze/expr/subquery.rs
src/backend/parser/analyze/collation.rs
src/backend/executor/exec_expr/subquery.rs
src/backend/executor/nodes.rs
src/backend/rewrite/views.rs
src/backend/optimizer/sublink_pullup.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/matview.rs
src/include/nodes/plannodes.rs
src/include/nodes/pathnodes.rs
src/include/nodes/execnodes.rs
src/backend/executor/startup.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/plan/subselect.rs
src/backend/optimizer/setrefs.rs
src/backend/optimizer/partitionwise.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parse_prepare_and_execute_statements
scripts/cargo_isolated.sh test --lib --quiet sql_prepare_execute_parameters_and_explain_execute_work
scripts/cargo_isolated.sh test --lib --quiet regex_scalar_array_quantifiers_work
scripts/cargo_isolated.sh test --lib --quiet optimizer::partition_prune::tests
scripts/cargo_isolated.sh test --lib --quiet execute_prepared_select_uses_external_params
scripts/cargo_isolated.sh test --lib --quiet plpgsql_dynamic_explain_execute_uses_session_prepared_statement
scripts/cargo_isolated.sh test --lib --quiet partition_bounds_accept_array_hash_enum_and_composite_keys
scripts/cargo_isolated.sh test --lib --quiet streaming_select_installs_prepared_context_for_plpgsql_dynamic_execute
scripts/run_regression.sh --test partition_prune --timeout 60 --port 65452
PGRUST_STATEMENT_TIMEOUT=30 PGRUST_REGRESS_BASE_SETUP_TIMEOUT=600 scripts/run_regression.sh --test partition_prune --timeout 180 --port 65452

Remaining:
Committed implementation as c1d59343b.
Current uncommitted slice is at 621/750 queries matched, 129 mismatched queries, 68 diff hunks, 2886 diff lines. Latest diff copied to /tmp/diffs/partition_prune.diff.
Prepared external params now work through normal `EXPLAIN EXECUTE` and through PL/pgSQL dynamic SQL in the server streaming SELECT path. The previous 4 `unsupported statement` failures from `explain_parallel_append('execute ab_q4/ab_q5 ...')` are gone; they are now ordinary runtime pruning/plan shape mismatches.
Array hash partition support, enum/record bound serialization, composite text casts, and partition-prune constant cast folding have focused coverage, but the full regression still shows enum/record query comparisons rendered as text and not pruned, so binding/type preservation remains the next issue there.
Main remaining categories:
- Runtime Append/MergeAppend pruning/explain state remains the largest blocker: 38 `Subplans Removed` mentions and 117 `never executed` mentions in the diff. The metadata/executor path exists and prepared external params are preserved, but non-ANALYZE `EXPLAIN EXECUTE` still does not initialize runtime pruning state, InitPlan params are duplicated per child in some paths, and nested-loop/parallel-shaped plans still do not preserve PostgreSQL's visible pruned child state.
- Static nested/default pruning is reduced but not gone. Remaining notable cases are PostgreSQL-conservative OR/range behavior around `rlp` and multi-key `mc3p`; pgrust is sometimes keeping too many child ranges/defaults.
- PL/pgSQL CONTINUE is fixed. Remaining PL/pgSQL-related hunks are plan-shape/runtime-pruning output, not syntax errors.
- EXPLAIN ANALYZE UPDATE is partly wired, but partitioned UPDATE/view rewrite paths still differ.
- MERGE source grammar/data-modifying CTE gaps remain: 6 join syntax errors, 1 merge syntax error, 1 trailing semicolon parse error, plus 1 subquery column-count error.
- Array/enum/record partition key blockers are partially fixed: hash array partition creation/routing is available, enum/record bound storage works, but enum/record query comparisons still bind/render as text and therefore do not prune to `Result`/single partition.
- View update/check-option path: 3 cannot-update-view errors where PostgreSQL rewrites to base partitioned table updates.
- Scalar-array NULL/empty pruning, custom operator syntax, ATTACH PARTITION dropped-column wording, and formatting-only plan differences remain as smaller isolated categories.
