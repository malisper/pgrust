Goal:
Diagnose why the `rules` regression test errored.

Key decisions:
Ran `scripts/run_regression.sh --test rules --timeout 60`. The test never reached
`rules`; isolated base staging for tests after `create_index` failed while running
the `create_index` dependency for the `post_create_index` base.

Files touched:
None outside this note.

Tests run:
`scripts/run_regression.sh --test rules --timeout 60`
Result: errored during base setup. Output:
`/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.q8dmO4/output/base_post_create_index_create_index.out`

`scripts/run_regression.sh --test create_index --skip-build --timeout 60`
Result: timed out after 466/687 query blocks. Output:
`/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.SdtVxX/output/create_index.out`

`RUST_BACKTRACE=full scripts/run_regression.sh --test rules --skip-build --timeout 60`
Result: base setup completed on rerun; `rules` ran and failed normally with
535/626 query blocks matched. Output:
`/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.be0cus/output/rules.out`

Manual standalone repros:
- The minimal `concur_heap` sequence around create_index.sql lines 493-540 did
  not crash and produced the expected invalid-then-valid `concur_index3`.
- A full manual bootstrapped `create_index.sql` run with `RUST_BACKTRACE=full`
  also completed. This points away from a deterministic REINDEX bad-state bug
  at that statement.

Full regression run:
`RUSTC_WRAPPER= RUST_BACKTRACE=full scripts/run_regression.sh --port 57033`
Raw log:
`/tmp/pgrust-full-regression-nosccache-20260429-150257.log`
Results:
`/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.iRSMq0`
Summary: completed; 231 planned/total, 69 passed, 160 failed, 0 errored,
2 timed out (`gin`, `vacuum_parallel`). Query match rate was 86.04%
(43,636 / 50,713).

The full run did not reproduce the original connection-loss at
`REINDEX TABLE concur_heap;`. The `post_create_index` base setup completed, and
`create_index` later failed by diff only. The full log has no `server closed the
connection unexpectedly`, `connection to server was lost`, or panic signature.

Focused rules rerun:
`RUST_BACKTRACE=full scripts/run_regression.sh --test rules --skip-build --timeout 300 --port 57033`
Raw log:
`/tmp/pgrust-rules-regression-20260429-154702.log`
Results:
`/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.EjJGKo`
Copied artifacts:
`/tmp/diffs/rules-regression/rules.diff`
`/tmp/diffs/rules-regression/rules.out`
Summary: base setup completed; `rules` failed by diff only, 535/626 query
blocks matched, 2533 diff lines, 0 errors/timeouts.

Current rules failure breakdown:
- First mismatch is numeric display scale after `update rtest_empmass set salary
  = salary + '1000.00'`: pgrust prints `6000`, `7000`, `5000` where Postgres
  prints `6000.00`, `7000.00`, `5000.00`.
- PostgreSQL numeric add uses `Max(var1->dscale, var2->dscale)`. pgrust
  `NumericValue::add` uses normalized storage `scale` to set `dscale`, so values
  like `4000.00` (stored as scale 0, dscale 2) lose display scale through
  arithmetic.
- `CREATE VIEW shoe_ready` fails because pgrust lacks `int4smaller(int4,int4)`.
  PostgreSQL has it in `pg_proc.dat`; pgrust has `int4larger` but no
  `int4smaller`.
- `insert into shoelace_ok select * from shoelace_arrive` hits
  `special executor Var referenced without a bound tuple` on `INNER_VAR`/`NEW`.
  pgrust binds rule OLD/NEW to the executor `OUTER_VAR`/`INNER_VAR` slots, while
  PostgreSQL has separate rule varnos `PRS2_OLD_VARNO=1` and
  `PRS2_NEW_VARNO=2`. Nested rule-action execution can clobber those executor
  slots.
- The `vview` update rule later hits an unbound Exec param for the same broad
  family: rule actions with `INSERT ... SELECT` and rewritten `NEW`/`OLD`
  references are not being carried through planning/execution like PostgreSQL.
- Large later diffs are catalog/ruleutils and unsupported-DDL noise:
  `pg_views`/`pg_rules` deparse output differs heavily; `ALTER RULE`, rule
  enable/disable, some rule action statement forms, `CREATE VIEW AS VALUES`, and
  `EXPLAIN INSERT` are unsupported or partial.

PostgreSQL comparison:
- PostgreSQL `ReindexTable()` calls `reindex_relation()` with
  `REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS`.
- `reindex_relation()` processes the toast table before the main table, skips
  invalid toast indexes, calls `reindex_index()` for each index, and does a
  `CommandCounterIncrement()` after each rebuild.
- pgrust `reindex_table_indexes_in_transaction()` currently only loops
  `catalog.index_relations_for_heap(relation.relation_oid)` for the main
  relation. It does not process toast indexes there. The create_index output
  confirms this: `pg_toast_TABLE_index` relfilenode stays unchanged where
  PostgreSQL expects it to change.
- pgrust `REINDEX SCHEMA`/database-style paths also rebuild matching tables in
  one transaction, while PostgreSQL's `ReindexMultipleTables()` intentionally
  reindexes each table in a separate transaction.

Remaining:
Implemented the selected root-cause pass:
- `NumericValue::add` now preserves result display scale as
  `max(left.dscale, right.dscale)` while retaining storage-scale alignment.
- Added `int4smaller(int4,int4)` with pg_proc OID 769, analyzer lookup, runtime
  execution, and view deparse support.
- Added `RULE_OLD_VAR`/`RULE_NEW_VAR` and rule tuple bindings separate from
  executor `OUTER_VAR`/`INNER_VAR`; rule pseudo Vars now survive setrefs and are
  evaluated from stable rule old/new tuples.

Focused validation after implementation:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet numeric_add_preserves_display_scale`
  passed: 2 tests.
- `scripts/cargo_isolated.sh test --lib --quiet int4smaller`
  passed: 2 tests.
- `scripts/cargo_isolated.sh test --lib --quiet insert_select_rule_action_keeps_new_binding_through_nested_update_rule`
  passed.
- `scripts/cargo_isolated.sh test --lib --quiet update_rule_insert_select_uses_old_new_without_exec_params`
  passed.

Final focused rules rerun:
`RUST_BACKTRACE=full scripts/run_regression.sh --test rules --timeout 300 --port 57033 --results-dir /tmp/pgrust-rules-root-fixes-2`
Raw log:
`/tmp/pgrust-rules-root-fixes-2.log`
Results:
`/tmp/pgrust-rules-root-fixes-2`
Summary: `rules` still fails by diff only, 548/626 query blocks matched, 2383
diff lines, 0 errored/timeouts.

Acceptance checks passed in the final `rules` artifacts:
- No `function int4smaller(integer, integer) does not exist`.
- No `function function(integer, integer) does not exist`.
- No `special executor Var referenced without a bound tuple`.
- No `executor param reached expression evaluation without a binding`.
- `rtest_emplog` numeric output now keeps `.00`.
- `shoelace_ok` and `vview` sequences complete and produce expected rows.

Remaining `rules` diffs are follow-up areas:
- `INSERT ... ON CONFLICT` with insert/update rules is not rejected, which causes
  later shoelace duplicate rows.
- Large `pg_views`/`pg_rules` ruleutils/deparse output differs.
- Unsupported/partial syntax and DDL forms remain: `ALTER RULE`, rule
  enable/disable, some rule action statement forms, `CREATE VIEW AS VALUES`,
  `EXPLAIN INSERT`, and selected catalog helper functions.

Goal:
Finish `rules` regression parity after the root-cause pass.

Key decisions:
- Implemented only behavior and deparse paths exercised by `rules`; kept broad
  SQL compatibility beyond this regression out of scope.
- Used PostgreSQL's `rules.out` as the expected reference and fixed the
  remaining rule DDL/action, ruleutils, helper function, and SQL-visible error
  position gaps in the relevant parser/analyzer/catalog/rewrite/executor paths.
- Final four mismatches were one missing `FOR UPDATE OF old` cursor and three
  ruleutils/function-body indentation diffs.

Files touched:
- Parser/analyzer and node metadata for rule action SQL positions and supported
  rule action shapes.
- Catalog/DDL paths for rule replacement, rename, enable/disable, pg_rewrite,
  pg_rules/pg_views, and helper-function metadata.
- Rewrite/view deparse paths for rule actions, values views, pg_get_viewdef,
  pg_get_ruledef, and SQL-standard function bodies used by `rules`.
- Executor/function paths for helper functions and SQL-visible deparse output.
- `src/backend/tcop/postgres.rs` only for normal error-position inference for
  `FOR UPDATE OF <target>` missing-relation errors.

Tests run:
- `cargo fmt`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh check`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet create_rule_rejects_unqualified_action_reference -- --nocapture`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet pg_get_ruledef -- --nocapture`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet pg_rules_exposes_user_rules_but_not_return_rules -- --nocapture`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet pg_get_viewdef_renders_values_rows_and_set_operation_inputs -- --nocapture`
- `RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=14 env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_missing_for_update_target -- --nocapture`
- `CARGO_TARGET_DIR=/tmp/pgrust-target RUSTC_WRAPPER=/usr/bin/env cargo build --bin pgrust_server`
- `CARGO_TARGET_DIR=/tmp/pgrust-target RUSTC_WRAPPER=/usr/bin/env RUST_BACKTRACE=full PGRUST_REGRESS_BASE_SETUP_TIMEOUT=600 scripts/run_regression.sh --test rules --skip-build --timeout 300 --port 57033`

Final validation:
- `rules` passes: 626/626 queries matched, 0 diffs.
- Final results directory:
  `/Volumes/OSCOO PSSD/pgrust/tmp//pgrust_regress_results.damascus-v4.DxW6SN`
- Rebased onto `origin/perf-optimization` before PR creation.

Remaining:
- No remaining `rules` regression diffs.
