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
- Added non-ANALYZE `EXPLAIN EXECUTE` startup pruning for external prepared params without executing scans, and kept one-child Append nodes visible when they need to print `Subplans Removed`.
- Passed the executor catalog into runtime partition-prune hash proofs so custom hash partition opclass support functions choose the same child as PostgreSQL.
- Fixed enum/composite list partition pruning by evaluating constant casts to the partition key type, and taught non-verbose EXPLAIN to render dynamic enum/composite type names for those constant casts without changing normal filter formatting.
- Rewrote array-key `IN`/`NOT IN` list binding for array left operands as OR/AND comparisons against typed array constants instead of treating the RHS array literal as a scalar-array element list.
- Matched PostgreSQL's `ATTACH PARTITION` child-only-column error branch before the generic column-count mismatch, including detail text and SQLSTATE.

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
src/backend/executor/mod.rs
src/backend/commands/explain.rs
src/backend/parser/analyze/coerce.rs
src/backend/commands/partition.rs

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
scripts/cargo_isolated.sh test --lib --quiet runtime_hash_pruning_uses_custom_opclass_support_proc
scripts/cargo_isolated.sh test --lib --quiet partitioned_key_coverage_checks_fire_for_root_partition_of_and_attach_partition
scripts/run_regression.sh --test partition_prune --timeout 60 --port 65452
PGRUST_STATEMENT_TIMEOUT=30 PGRUST_REGRESS_BASE_SETUP_TIMEOUT=600 scripts/run_regression.sh --test partition_prune --timeout 180 --port 65452
PGRUST_STATEMENT_TIMEOUT=30 PGRUST_REGRESS_BASE_SETUP_TIMEOUT=600 scripts/run_regression.sh --test partition_prune --timeout 300 --port 65452

Remaining:
Committed implementation as c1d59343b.
Committed implementation as e8145e080.
Latest slice is at 630/750 queries matched, 120 mismatched queries, 126 diff hunks, 2716 diff lines. Latest diff copied to /tmp/diffs/partition_prune.diff.
Prepared external params now work through normal `EXPLAIN EXECUTE` and through PL/pgSQL dynamic SQL in the server streaming SELECT path. The previous 4 `unsupported statement` failures from `explain_parallel_append('execute ab_q4/ab_q5 ...')` are gone; they are now ordinary runtime pruning/plan shape mismatches.
Non-ANALYZE `EXPLAIN EXECUTE hp_q1('xxx')` now prunes to `hp2` and prints `Subplans Removed: 3`, matching PostgreSQL for the custom hash opclass case.
Array hash partition support, enum/record bound serialization, composite text casts, and partition-prune constant cast folding have focused coverage. The full regression no longer has `pp_enumpart`, `pp_recpart`, or `pph_arrpart` hunks.
Main remaining categories:
- Runtime Append/MergeAppend pruning/explain state remains the largest blocker: 37 `Subplans Removed` mentions and 117 `never executed` mentions in the diff. The non-ANALYZE prepared external-param path is fixed, but EXPLAIN ANALYZE still needs PostgreSQL-style visible pruned child state for InitPlan/nested-loop/parallel-shaped plans.
- Static nested/default pruning is reduced but not gone. Remaining notable cases are PostgreSQL-conservative OR/range behavior around `rlp` and multi-key `mc3p`; pgrust is sometimes keeping too many child ranges/defaults.
- PL/pgSQL CONTINUE is fixed. Remaining PL/pgSQL-related hunks are plan-shape/runtime-pruning output, not syntax errors.
- EXPLAIN ANALYZE UPDATE is partly wired, but partitioned UPDATE/view rewrite paths still differ.
- MERGE source grammar/data-modifying CTE gaps remain: 6 join syntax errors, 1 merge syntax error, 1 trailing semicolon parse error, plus 1 subquery column-count error.
- Array/enum/record partition key blockers are mostly fixed for this file: hash array partition creation/routing is available, enum/record bound storage works, enum/record query comparisons now prune/render correctly, and array-key `IN` list comparisons render/prune correctly for `pph_arrpart`.
- View update/check-option path: 3 cannot-update-view errors where PostgreSQL rewrites to base partitioned table updates.
- Scalar-array NULL/empty pruning, custom operator syntax, view update rewrites, MERGE joined-source syntax, and formatting-only plan differences remain as smaller isolated categories.
Latest category counts in `/tmp/diffs/partition_prune.diff`: `Subplans Removed` 37, `never executed` 117, `ERROR:` 16, `syntax error` 9, `rlp` 35, `mc2p` 8, `mc3p` 125, `pph_arrpart` 0, `pp_arrpart` 13, `pp_enumpart` 0, `pp_recpart` 0, `to_char` 4, `MERGE` 3, `UPDATE` 8, `ATTACH PARTITION` 0, `ANY` 55, `ALL` 4.
