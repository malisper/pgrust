Goal:
Implement the create_procedure regression fix plan and make the regression pass.

Key decisions:
- Added shared routine parsing/execution paths for ALTER FUNCTION/PROCEDURE/ROUTINE and DROP ROUTINE.
- Stored procedure defaults in pg_proc.proargdefaults and used them during CALL candidate matching.
- Fixed EXECUTE privilege checks and GRANT/REVOKE grammar for table INSERT plus function/procedure/routine EXECUTE.
- Added LEAST alongside existing GREATEST support.
- Fixed catalog visibility for deleted bootstrap tuples by making xmin=0 rows honor committed xmax.
- Deparsed the SQL-standard INSERT body shape needed by create_procedure pg_get_functiondef.

Files touched:
Parser/analyzer, pg_proc catalog metadata, routine create/drop/alter/privilege/session execution, SQL function display, MVCC visibility, and psql error-position formatting.

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet parse_drop_and_alter_procedure_statements
- scripts/cargo_isolated.sh test --lib --quiet procedure_catalog_display_helpers_read_pg_proc_metadata
- scripts/cargo_isolated.sh test --lib --quiet sql_standard_procedure_body_displays_and_executes
- scripts/cargo_isolated.sh test --lib --quiet hint_bits_exhaustive_permutations
- CARGO_TARGET_DIR=/tmp/pgrust-target-dubai-createproc scripts/run_regression.sh --test create_procedure --timeout 300 --jobs 1 --port 55469 --skip-build

Remaining:
create_procedure now passes. Some plan items are intentionally shallow compatibility support where the row shape is not modeled yet, especially proconfig/support/dependency-on-extension persistence.
