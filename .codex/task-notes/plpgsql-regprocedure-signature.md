Goal:
Fix the PL/pgSQL regression hunk where PG_ROUTINE_OID output rendered a regprocedure as `current_function` instead of `current_function(text)`.

Key decisions:
Keep regproc names and regprocedure signatures separate in protocol formatting so bare regproc output is not changed. Prefer current-catalog procedure signatures when formatting regprocedure OIDs.

Files touched:
src/backend/libpq/pqformat.rs
src/backend/tcop/postgres.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet typed_data_row_renders_regprocedure_with_proc_name
scripts/cargo_isolated.sh test --lib --quiet plpgsql_get_diagnostics_pg_routine_oid_reports_current_function
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-regprocedure-signature

Remaining:
Latest regression baseline is 2184/2271 matched with 1040 diff lines. Remaining large clusters are PG_CONTEXT stack text, composite/record return coercion, WHERE CURRENT OF, transition tables, and several error formatting differences.
