Goal:
Fix C5 DDL/catalog mutation foundations for alter_table and constraints, focused on constraint/index/default/comment metadata rather than domain, tablespace, generated-column, planner, or executor-enforcement work.

Key decisions:
- Followed PostgreSQL dependency shape for index-backed constraints: table drops must traverse the table -> constraint -> owned index edge so constraint-owned indexes do not survive DROP TABLE.
- Kept MVCC catalog tests checking visible rows instead of raw physical heap rows, since catalog deletes leave dead tuples until cleanup.
- Fixed catalog scope for FK constraint/depend rows in non-default databases by using the store database oid instead of hard-coded database oid 1.
- Removed pg_description rows when dropping a relation constraint directly.
- Matched PostgreSQL-facing validation text for duplicate ALTER TYPE unique rebuilds, duplicate primary-key additions, and non-FK ALTER CONSTRAINT ENFORCED/NOT ENFORCED.

Files touched:
- src/backend/catalog/store/heap.rs
- src/backend/catalog/store.rs
- src/backend/parser/analyze/constraints.rs
- src/pgrust/database/commands/alter_column_type.rs
- src/pgrust/database/commands/constraint.rs

Tests run:
- scripts/cargo_isolated.sh test --lib --quiet catalog_store_drop
- scripts/cargo_isolated.sh test --lib --quiet catalog_store_foreign_key_constraint_uses_database_scope
- scripts/run_regression.sh --test alter_table --port 55444 --results-dir /tmp/pgrust-c5-alter-table
- scripts/run_regression.sh --test constraints --port 55448 --results-dir /tmp/pgrust-c5-constraints
- cargo fmt
- scripts/cargo_isolated.sh check

Remaining:
- alter_table still fails: 1564/1683 matched, 119 mismatches, 1061 diff lines. Remaining buckets include check-expression deparse shape, ADD COLUMN default/rewrite ordering, dropped-column object naming, inheritance diagnostics, domain/drop-column behavior, system-column/type resolution, FK alter-type validation, and filenode mapping output.
- constraints still fails: 549/565 matched, 16 mismatches, 128 diff lines. Remaining buckets are mostly COPY case-folding, parser placement diagnostics for UNIQUE ENFORCED/NOT ENFORCED, exclusion-expression display text, ON CONFLICT unsupported-prefix text, not-null wording, duplicate constraint-name formatting, and COMMENT ON CONSTRAINT domain/ownership messages.
