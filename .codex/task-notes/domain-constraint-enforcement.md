Goal:
Investigate why domain constraints/checks/nullability from /tmp/diffs/domain.diff are not enforced consistently.

Key decisions:
Root cause is architectural split: pgrust did not have a shared CoerceToDomain-style expression/runtime path. Domain enforcement was duplicated in cast-only code and table insert code, with different semantics. Cast-only enforcement was string-pattern based; table insertion parsed/evaluated CHECK expressions but only after insert materialization and not after update whole-row writes in every path. PL/pgSQL casts used no catalog, so domain lookup was skipped.

Added shared executor domain enforcement that evaluates arbitrary CHECK expressions with catalog context, recurses through nested domains, preserves PostgreSQL-style outer-domain error attribution, and handles array-of-domain values. Domain validation now runs from casts, pg_input_error_info, INSERT/COPY, UPDATE final row writes, add-column validation, and PL/pgSQL assignment/return/local initialization paths.

Files touched:
src/backend/executor/domain.rs
src/backend/executor/mod.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/optimizer/constfold.rs
src/backend/parser/analyze/expr.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/ddl.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet domain_constraints_are_enforced_for_casts_and_input_info
scripts/cargo_isolated.sh test --lib --quiet copy_uses_domain_input_semantics
scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_values_are_checked_against_domain_constraints
scripts/cargo_isolated.sh test --lib --quiet domain
scripts/run_regression.sh --test domain --jobs 1 --timeout 60 --port 6543 --results-dir /tmp/diffs/domain-fixed-3

Remaining:
The `domain` regression file still fails overall: 354/507 queries matched, 153 mismatched, 1075 diff lines in /tmp/diffs/domain-fixed-3/diff/domain.diff. Remaining diffs are mostly unrelated unsupported features or formatting/context differences, plus composite-domain sections blocked by missing btree opclass support for domains over composites and PL/pgSQL parser limitations around the array-level helper function body.

Update:
Follow-up patch for the composite-domain buckets adds domain-aware default opclass lookup, record btree comparison, SQL-function composite-domain result coercion/expansion, composite-domain array literal casting/checks, domain defaults after column default drops, psql display names for dynamic domain/composite types, and ALTER DOMAIN validation/rejection for domain chains versus unsupported derived composite/range columns.

Additional files touched:
src/backend/access/nbtree/nbtcompare.rs
src/backend/parser/analyze/modify.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/lsyscache.rs
src/include/catalog/pg_opclass.rs
src/pgrust/database/commands/domain.rs

Additional tests run:
scripts/cargo_isolated.sh test --lib --quiet composite_domain
scripts/cargo_isolated.sh test --lib --quiet composite_array_domains
scripts/cargo_isolated.sh test --lib --quiet sql_functions_recheck_composite_domain_results
scripts/cargo_isolated.sh test --lib --quiet domain_defaults_apply_after_column_default_drop
scripts/cargo_isolated.sh test --lib --quiet alter_domain_
scripts/cargo_isolated.sh test --lib --quiet domain
scripts/run_regression.sh --test domain --jobs 1 --timeout 120 --port 6543 --results-dir /tmp/diffs/domain-fixed-8

Current remaining:
The latest `domain` regression run is /tmp/diffs/domain-fixed-8: 400/507 queries matched, 107 mismatched, 855 diff lines. Original bucket counts are down from Composite-domain opclass cascades 33 -> 5 and Composite-domain navigation/check propagation 31 -> 13. Remaining items in those buckets are mostly unordered SELECT output, EXPLAIN/rule deparse formatting, a sequence value difference after failed inserts, composite-array table order, record text input for derived composite/domain arrays, and DROP DOMAIN cascade notice coverage.

Update:
Follow-up patch for the CREATE/ALTER DOMAIN DDL validation and PostgreSQL-exact error text bucket adds parser validation for unsupported domain constraint forms, duplicate DEFAULT, conflicting NULL/NOT NULL, CREATE DOMAIN enforceability/deferrability/NO INHERIT messages, DDL-time validation for pseudo-type bases, noncollatable COLLATE, and invalid default constant casts. It also adds domain-specific error-position mapping for the regression caret output.

Additional files touched:
src/backend/parser/gram.rs
src/backend/tcop/postgres.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/create.rs
src/pgrust/database_tests.rs

Additional tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet create_domain_rejects_invalid_ddl_forms
scripts/run_regression.sh --test domain --jobs 1 --timeout 120 --results-dir /tmp/diffs/domain-ddl-errors

Current remaining:
The latest `domain` regression run is /tmp/diffs/domain-ddl-errors: 417/507 queries matched, 90 mismatched, 759 diff lines. The 17-query CREATE/ALTER DOMAIN DDL validation/error-text bucket is resolved; the targeted error strings no longer appear as mismatches in /tmp/diffs/domain-ddl-errors/diff/domain.diff.
