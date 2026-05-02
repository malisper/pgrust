Goal:
Compare regression-history runs 2026-05-02T1633Z and 2026-05-02T0339Z, explain mismatch count movement, and fix the typmod-array DDL regression.
Key decisions:
Fetched origin/regression-history. Confirmed query mismatches decreased from 3217 to 3088; failed test files increased. Top new regressions are char/varchar typmod array DDL failures caused by type ACL usage checks added in 455c0b8b53. Fixed LazyCatalogLookup array OID resolution to retry element type lookup without typmod, preserving ACL checks while resolving char(n)[]/varchar(n)[] to base array type OIDs.
Files touched:
src/backend/utils/cache/lsyscache.rs
src/backend/executor/tests.rs
src/pgrust/database_tests.rs
.codex/task-notes/regression-count-diff.md
Tests run:
Used git show/ls-tree and a local per-test query block counting script against run outputs.
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet type_usage_privilege_resolves_typmod_array_base_types
RUST_BACKTRACE=1 scripts/cargo_isolated.sh test --lib --quiet jsonb_build_object_embeds_row_to_json_subquery
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test arrays --timeout 120 --jobs 1
scripts/run_regression.sh --skip-build --test json --timeout 180 --jobs 1
scripts/run_regression.sh --skip-build --test jsonb --timeout 180 --jobs 1 (errored from an unrelated local dev-server crash at nested jsonb_build_object/row_to_json; diff copied to /tmp/diffs/jsonb-typmod-array-fix.diff)
RUST_BACKTRACE=1 scripts/run_regression.sh --skip-build --test jsonb --timeout 180 --jobs 1 (passed after rebuilding current pgrust_server)
Remaining:
Earlier jsonb standalone dev regression crash did not reproduce after rebuilding current pgrust_server. Added reduced in-process coverage for the crash query; full jsonb regression now passes locally.
