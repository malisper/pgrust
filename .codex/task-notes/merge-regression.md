Goal:
Diagnose and reduce failures in the `merge` regression diff from the pasted attachment.

Key decisions:
Fixed the MERGE planner hidden-column projection. Source visible columns start after the hidden target ctid, and the source-present marker follows the source visible columns.

Files touched:
src/backend/parser/analyze/modify.rs
src/backend/parser/tests.rs

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet plan_merge_uses_join_shape_for_explain`
`scripts/run_regression.sh --test merge` did not reach SQL execution because Cargo failed building a dependency artifact.
`CARGO_TARGET_DIR=/tmp/pgrust-target-pool/sparta/6 scripts/run_regression.sh --skip-build --timeout 300 --test merge` completed: merge failed with 457/641 queries matched and 2116 diff lines.
`PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh check` passed with existing unreachable-pattern warnings.
`PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet parse_merge` passed.
`PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --quiet ctid` passed.
`PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --quiet merge_checks_target_and_source_privileges` passed.
`CARGO_TARGET_DIR=/tmp/pgrust-target-pool/sparta/6 scripts/run_regression.sh --skip-build --timeout 300 --test merge` completed after rebuilding pgrust_server: merge failed with 486/641 queries matched and 2020 diff lines.

Remaining:
The fresh diff no longer has `merge source marker has unexpected value`, `ctid is not available`, or the earlier missing MERGE permission-check mismatches. ctid outer-join null handling and basic MERGE target/source privilege checks are fixed with focused tests. MERGE RETURNING now parses, but PostgreSQL-compatible result-row semantics still need binder/executor support for `merge_action()`, `old`, and `new`. Remaining regression gaps include full MERGE RETURNING output, some PostgreSQL-exact error text, transaction abort behavior after duplicate-key MERGE INSERT, and EXPLAIN formatting/plan shape differences.
