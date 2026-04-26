Goal:
Make the `alter_generic` regression pass by implementing the missing generic-object DDL on real catalog rows.

Key decisions:
- Added AST/parser/command support for aggregate routine ALTER, conversion, language, foreign server, operator owner/schema, operator family/class ADD/DROP/rename/owner/schema, statistics owner/schema, and text search objects.
- Added physical catalog support for `pg_foreign_server` and mutation/dependency helpers for conversion, language, opfamily/opclass, text search, and related cascade notices.
- Kept the existing statistics tcop shortcut but narrowed it with a real-row response for the `pg_statistic_ext` owner/namespace query used by this regression.

Files touched:
- Parser/AST/routing: `src/backend/parser/gram.rs`, `src/backend/parser/tests.rs`, `src/include/nodes/parsenodes.rs`, executor/session/database routing files.
- Catalog/storage/cache: `src/include/catalog/*`, `src/backend/catalog/*`, `src/backend/utils/cache/*`.
- Commands: `conversion.rs`, `foreign_data_wrapper.rs`, `language.rs`, `opclass.rs`, `operator.rs`, `routine.rs`, `create_statistics.rs`, `text_search.rs`, `drop.rs`.

Tests run:
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet parse_text_search_generic_statements`
- `scripts/cargo_isolated.sh test --lib --quiet parse_operator_family_and_class_alter_statements`
- `scripts/cargo_isolated.sh test --lib --quiet parse_drop_and_alter_procedure_statements`
- `scripts/cargo_isolated.sh test --lib --quiet parse_alter_aggregate_rename_statement`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-pool/tunis-v2/alter-generic scripts/run_regression.sh --test alter_generic --timeout 180 --jobs 1 --port 59760` passed 332/332 after rebasing on `origin/perf-optimization`.

Remaining:
- The only observed warning is a pre-existing unreachable-pattern warning in `query_repl.rs`.
