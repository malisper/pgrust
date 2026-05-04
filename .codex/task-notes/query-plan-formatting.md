Goal:
Fix query-plan formatting diffs seen in /tmp/diffs, especially verbose Output
qualification, negative integer literal casts, and debug Var fallback text.

Key decisions:
Keep fixes in EXPLAIN rendering/deparsing. Do not hide planner shape or
unsupported-feature differences as formatting.

Files touched:
crates/pgrust_commands/src/explain_expr.rs
crates/pgrust_commands/src/explain_verbose.rs
src/backend/commands/explain.rs

Tests run:
cargo fmt --check
scripts/cargo_isolated.sh test --lib --quiet explain -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet explain_expr_matches_postgres_filter_formatting -- --nocapture
scripts/run_regression.sh --test fast_default --timeout 90 --jobs 1 --port 55433

Remaining:
fast_default still has non-formatting diffs around generated-column support,
DELETE RETURNING plan shape, and table rewrite notices. A gist regression run
errored early due existing GiST option support gaps.
