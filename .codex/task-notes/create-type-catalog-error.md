Goal:
Investigate and fix extra `ERROR: catalog error` lines after `DROP TYPE ... CASCADE` in the `create_type` regression.

Key decisions:
The base-type cascade path now advances command ids between dependent function drops and the final type drop, matching the existing composite-type cascade behavior. This prevents the final catalog delete from seeing dependency rows that were already deleted by earlier dependent object drops in the same transaction.

Files touched:
- `src/pgrust/database/commands/typecmds.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `scripts/cargo_isolated.sh test --lib --quiet drop_base_type_cascade_drops_support_functions_without_catalog_error`
- `scripts/run_regression.sh --test create_type --timeout 120`

Remaining:
None.
