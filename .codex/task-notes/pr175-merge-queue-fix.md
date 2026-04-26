Goal:
- Fix PR #175 merge-queue failures from cargo-test-run shard 2.

Key decisions:
- Treat pg_proc proargdefaults as an expected descriptor column.
- Parenthesize substituted SQL procedure argument literals so casts and array subscripts bind correctly.
- Ignore OUT-only argument specs when matching DROP FUNCTION signatures.

Files touched:
- src/include/catalog/pg_proc.rs
- src/pgrust/session.rs
- src/pgrust/database/commands/drop.rs

Tests run:
- cargo fmt
- cargo test --lib --quiet pg_proc_desc_matches_expected_columns
- cargo test --lib --quiet call_procedure_resolves_defaults_named_args_variadic_and_multi_drop
- cargo test --lib --quiet drop_function_ignores_argument_names_and_out_only_modes
- cargo test --lib --quiet create_procedure
- cargo check
- cargo test --lib --quiet call_procedure

Remaining:
- Push branch and let GitHub merge queue rerun.
