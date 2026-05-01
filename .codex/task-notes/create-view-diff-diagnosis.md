Goal:
Make `scripts/run_regression.sh --test create_view --results-dir
/tmp/diffs/create_view` match PostgreSQL expected output without editing
expected files.

Key decisions:
PostgreSQL `../postgres` remains the compatibility reference. Implemented
narrow compatibility fixes first rather than changing expected output:
scalar-expression FROM items are now parsed/lowered as single-row projection
sources, semantic temp-schema errors no longer print cursor positions, whole-row
composite `VALUES` comparisons treat a single composite column as one value,
restricted non-system view access is checked in session, direct executor, and
streaming SELECT paths, and view deparse no longer falls back to Rust debug
strings for the `create_view` expressions hit so far.

Files touched:
`crates/pgrust_sql_grammar/src/gram.pest`,
`src/backend/parser/gram.rs`, `src/include/nodes/parsenodes.rs`,
`src/include/nodes/primnodes.rs`, `src/backend/parser/analyze/*`,
`src/backend/rewrite/*`, `src/backend/executor/*`,
`src/backend/tcop/postgres.rs`, `src/pgrust/database/commands/*`,
`src/pgrust/session.rs`, `src/pgrust/database_tests.rs`.

Tests run:
`cargo fmt`
`CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-nagoya-v2-create-view-check' scripts/cargo_isolated.sh check`
Targeted tests:
`scalar_expression_from_items_use_relation_alias_for_single_column`
`whole_row_composite_in_values_compares_as_single_value`
`restrict_nonsystem_relation_kind_blocks_view_select`
Regression:
`CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-nagoya-v2-create-view-check' scripts/run_regression.sh --test create_view --results-dir /tmp/diffs/create_view --timeout 120 --port 6552`
Latest regression result: FAIL, `230/311` queries matched, `1646` diff lines.

Remaining:
Major remaining clusters are still deparse parity: relation namespace/alias
assignment after renames, join alias and `JOIN USING`/natural join merged-column
printing, comma FROM-list vs cross-join rendering, PostgreSQL operator/function
special-form deparse for `tt201v`, exact `SYSTEM_USER` provenance, function RTE
named composite behavior after dropped/type-changed columns, temporary/dependent
view dependency cascade counts, and several formatting/cast differences in
viewdefs and EXPLAIN.
