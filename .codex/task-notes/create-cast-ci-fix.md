Goal:
Fix the CI failures from the CREATE CAST PR after explicit cast validation started rejecting range/multirange text I/O casts.

Key decisions:
Restrict the catalog-backed explicit-cast hard rejection to user-defined base type OIDs, not user-defined range, multirange, array, or composite rows.
Narrow that check further to text-backed base types so enum text input casts keep working.
Do not lower unsupported built-in internal cast functions into user-defined function calls; fall back to the existing cast path for those.

Files touched:
src/backend/parser/analyze/expr.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet create_default_range_opclasses_for_btree_hash_and_spgist
scripts/cargo_isolated.sh test --lib --quiet user_defined_multiranges_can_back_table_columns
scripts/cargo_isolated.sh test --lib --quiet user_defined_ranges_support_default_and_manual_multirange_names
scripts/cargo_isolated.sh test --lib --quiet abs_builtin_supports_smallint_filters
scripts/cargo_isolated.sh test --lib --quiet function_style_type_casts_lower_to_regular_casts
scripts/cargo_isolated.sh test --lib --quiet create_enum_type_exposes_catalog_rows_and_can_back_table_columns
scripts/cargo_isolated.sh test --lib --quiet generate_series_rejects_non_finite_numeric_bounds
scripts/cargo_isolated.sh test --lib --quiet integer_arithmetic_overflow_raises_error
scripts/cargo_isolated.sh test --lib --quiet bytea_hash_and_encoding_functions_work
scripts/cargo_isolated.sh test --lib --quiet numeric_scalar_helpers_follow_postgres_basics
scripts/cargo_isolated.sh test --lib --quiet cte_self_join_aliases_keep_distinct_columns
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-regress-shanghai-createcast bash scripts/run_regression.sh --test create_cast --jobs 1 --timeout 300 --port 57555 --results-dir /tmp/pgrust-create-cast-shanghai-v4

Remaining:
Push the second CI fix to the existing PR branch and watch CI.
