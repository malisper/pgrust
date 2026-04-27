Goal:
Fix PostgreSQL guc regression failures around transaction/session GUC behavior and close out feasible remaining gaps.

Key decisions:
Implemented transaction-scoped GUC state. Active transactions now track start and commit GUC states; SET LOCAL mutates only the effective state, normal SET/RESET also updates the commit state, and savepoints snapshot effective plus commit GUC state. Added RELEASE SAVEPOINT, custom dotted GUCs, LOAD plpgsql prefix reservation, DISCARD TEMP/ALL session reset, holdable cursor declaration outside explicit transactions, vacuum_cost_delay formatting/range validation, finite seq_page_cost validation, SQL DateStyle timestamp output, and wire-level shims for SQL PREPARE, pg_prepared_statements, pg_listening_channels(), and current_schemas(false).

Added function-level SET/proconfig support for CREATE/ALTER FUNCTION, function-local GUC application/restoration for SQL and PL/pgSQL functions, CREATE FUNCTION SET validation controlled by check_function_bodies, pg_stat_activity.query_id reporting behind compute_query_id/track_activities, pg_settings_get_flags(text), and pg_show_all_settings().

Files touched:
.codex/task-notes/guc-regression.md
src/backend/executor/driver.rs
src/backend/executor/exec_expr.rs
src/backend/executor/function_guc.rs
src/backend/executor/mod.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/srf.rs
src/backend/libpq/pqformat.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/system_view_registry.rs
src/backend/utils/misc/guc.rs
src/backend/utils/time/timestamp.rs
src/bin/query_repl.rs
src/include/catalog/builtin_ranges.rs
src/include/catalog/pg_proc.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/pgrust/cluster.rs
src/pgrust/database.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/routine.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
src/pl/plpgsql/ast.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/gram.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet guc
scripts/cargo_isolated.sh test --lib --quiet parse_sql_prepare_statement_extracts_name_and_query
scripts/cargo_isolated.sh test --lib --quiet sql_is_discard_all_requires_all_target
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-cape-town-guc CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server
bash -lc 'CARGO_TARGET_DIR=/tmp/pgrust-target-cape-town-guc scripts/run_regression.sh --skip-build --test guc --schedule <(printf "test: guc\n") --timeout 180 --port 59440 --results-dir /tmp/pgrust-guc-cape-town-after-fix-customschedule3'
CARGO_TARGET_DIR=/tmp/pgrust-target-cape-town-final scripts/run_regression.sh --test guc --schedule <(printf "test: guc\n") --timeout 180 --port 59450 --results-dir /tmp/pgrust-guc-cape-town-final4

Remaining:
Custom-schedule guc regression now passes: 229/229 queries matched in /tmp/pgrust-guc-cape-town-final4. Full stock --test guc may still trip unrelated create_index base staging failures unless run with a custom schedule.
