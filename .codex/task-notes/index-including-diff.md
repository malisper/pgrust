Goal:
Implement PostgreSQL-style INCLUDE support for index_including.

Key decisions:
Preserve the key/non-key split: indkey/index storage include key + payload
columns, while indnkeyatts, indclass, indcollation, indoption, uniqueness, scan
bounds, and duplicate details use only key columns. Btree stores INCLUDE payloads
with generic value encoding and compares only the key prefix. Constraint and
deparse paths render INCLUDE while pg_constraint.conkey remains key-only.

Files touched:
Parser/AST/analyzer DDL paths, btree build/insert/scan paths, catalog
constraint/index metadata, pg_get_* deparse, index AM support checks, row-prefix
btree planner display/costing, focused parser/database tests.

Tests run:
cargo fmt
cargo check
cargo test --lib --quiet unique_include_constraint_uses_only_key_columns_for_enforcement_and_catalogs
cargo test --lib --quiet alter_table_add_unique_using_index_include_derives_key_conkey
cargo test --lib --quiet parse_create_table_primary_key_and_unique_constraints
cargo test --lib --quiet parse_alter_table_constraint_statements
cargo test --lib --quiet explain_bootstrap_
cargo test --lib --quiet create_unique_index_rejects_duplicate_live_keys
cargo test --lib --quiet index_matrix_residual_filter_still_returns_correct_rows
scripts/run_regression.sh --test index_including --ignore-deps
scripts/run_regression.sh --test index_including
git diff --check

Remaining:
index_including passes 135/135 with --ignore-deps. The non-ignored run still
stops before executing index_including because the create_index dependency setup
fails independently.
