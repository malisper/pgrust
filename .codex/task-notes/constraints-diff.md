Goal:
Implement the semantic fixes behind the constraints regression failures from /tmp/pgrust-diffs-2026-04-30T0340Z/constraints.diff, leaving formatting-only/error-text-only mismatches out of scope.

Key decisions:
Column CHECK names now use the single referenced user column when possible and fall back to table_check for multi-column checks.
EXCLUDE predicates are parsed, stored in pg_index.indpred, bound into exclusion constraints, and honored during insert/update/deferred validation.
Expression exclusion keys are enforced and validated; scalar GiST equality uses the btree opclass compatibility shim for metadata while heap-scan enforcement uses scalar equality.
Partition attach FK clones use parent names with PostgreSQL-style numeric suffixes on collision.
Partitioned unique index creation can reuse matching local child constraint indexes without changing child constraint inheritance.
Deferred unique recheck ignores same-transaction deletes/updates that supersede the conflicting tuple before commit.
Primary-key-created NOT NULL catalog metadata is preserved on PK drop and inherited/partition state is kept consistent.
regclass[] text input resolves relation names for prepared parameters through catalog-aware casts.
ALTER TABLE ADD COLUMN with volatile defaults keeps the default expression without trying to derive a literal missing value.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/access/index/unique.rs
src/backend/catalog/store/heap.rs
src/backend/commands/tablecmds.rs
src/backend/executor/expr_casts.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/include/catalog/pg_proc.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
src/pgrust/database/commands/partitioned_indexes.rs
src/pgrust/database/commands/partitioned_keys.rs
src/pgrust/database/ddl.rs
src/pgrust/database/foreign_keys.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
cargo check -q with external TMPDIR/CARGO_TARGET_DIR and sccache disabled
cargo test --lib --quiet lower_create_table_names_column_checks_from_referenced_columns
cargo test --lib --quiet parse_create_table_exclusion_constraint_captures_predicate_sql
cargo test --lib --quiet scalar_gist_exclusion_constraint_honors_predicate
cargo test --lib --quiet deferrable_exclusion_constraint_checks_at_commit
cargo test --lib --quiet alter_table_add_exclusion_constraint_accepts_expression_key
cargo test --lib --quiet attach_partition_fk_clone_uses_available_parent_name
cargo test --lib --quiet create_unique_index_on_partitioned_table_reuses_child_constraint_index
cargo test --lib --quiet deferred_unique_recheck_ignores_same_transaction_delete
cargo test --lib --quiet drop_primary_key_preserves_created_not_null_constraint
cargo test --lib --quiet alter_table_only_drop_not_null_preserves_child_constraint
cargo test --lib --quiet partition_child_not_valid_or_no_inherit_not_null_blocks_primary_key
cargo test --lib --quiet regclass_array_literal_resolves_relation_names
cargo test --lib --quiet alter_table_add_column_reads_old_rows_with_null_or_default
scripts/run_regression.sh --test constraints --jobs 1 --timeout 240 --port 65468 using external TMPDIR/CARGO_TARGET_DIR: completed, 547/565 matched, no timeout.

Remaining:
constraints.diff remaining mismatches are out-of-scope display/error text differences: COPY relation-name casing, UNIQUE ENFORCED/ALTER CONSTRAINT wording and carets, exclusion expression detail parentheses, NOT NULL duplicate/NO INHERIT wording, and COMMENT ON CONSTRAINT domain/ownership wording/caret.
