Goal:
Diagnose why the upstream PostgreSQL `identity` regression output differs.

Key decisions:
- First real mismatch is `CREATE TABLE itest3 (... identity (start with 7 increment by 5) ...)`, which pgrust rejects because `column_identity` does not parse identity sequence options.
- Most later `itest3` failures cascade from that table not being created.
- Identity-specific `ALTER TABLE ... ADD/DROP/SET GENERATED/SET INCREMENT/RESTART` forms are not represented in the parser/executor dispatch, so they fall through to unsupported ALTER TABLE form errors.
- `INSERT ... OVERRIDING {SYSTEM|USER} VALUE` is not parsed; `GENERATED ALWAYS AS IDENTITY` assignments are not rejected when overriding is absent, so explicit inserts/updates diverge.
- `information_schema.columns` is a very small synthetic view and lacks identity/default/nullability columns used by the regression. `information_schema.sequences` and `pg_catalog.pg_sequence` are absent.
- `pg_get_serial_sequence` returns an unqualified relcache name rather than PostgreSQL's schema-qualified `public.<seq>`.
- Extra later diffs are also caused by unsupported unlogged tables/sequences, typed tables, date partition keys, and some MERGE identity/overriding cases.

Files touched:
- `src/backend/parser/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/analyze/create_table.rs`
- `src/backend/parser/analyze/modify.rs`
- `src/backend/catalog/store/heap.rs`
- `src/pgrust/database/commands/alter_column_identity.rs`
- `src/pgrust/database_tests.rs`
- session/executor routing files

Tests run:
- `scripts/cargo_isolated.sh check` (passes; existing `query_repl` unreachable-pattern warning remains)
- `scripts/cargo_isolated.sh test --lib --quiet parse_identity_options_alter_and_overriding`
- `scripts/cargo_isolated.sh test --lib --quiet identity_sequence_options_drive_owned_sequence`
- `scripts/cargo_isolated.sh test --lib --quiet alter_identity_and_overriding_enforce_generated_always`
- `scripts/run_regression.sh --test identity --timeout 60` did not reach identity; unrelated `create_index` base dependency setup failed.
- `scripts/run_regression.sh --test identity --timeout 60 --ignore-deps` also stopped at the same `create_index` base setup failure.

Remaining:
- Remaining identity regression diffs should now be dominated by info schema/pg_sequence, unlogged/typed/partition support, and MERGE `OVERRIDING`.
