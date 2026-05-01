Goal:
Fix remaining PostgreSQL compatibility failures in the create_view regression without editing expected files.

Key decisions:
Kept stored analyzed Query/view behavior and patched PostgreSQL-visible behavior in deparse, EXPLAIN, composite function row handling, deferred dropped-column errors, and schema cascade notice ordering. Added narrow :HACK: comments where compatibility shims are intentionally temporary.

Files touched:
src/backend/commands/explain.rs
src/backend/rewrite/rules.rs
src/backend/rewrite/views.rs
src/backend/optimizer/bestpath.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/setrefs.rs
src/include/nodes/datum.rs
src/backend/executor/*
src/backend/tcop/postgres.rs
src/pl/plpgsql/*
src/pgrust/database/commands/drop.rs

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=41 scripts/cargo_isolated.sh check
scripts/run_regression.sh --test create_view --results-dir /tmp/diffs/create_view --timeout 120 --port 17557

Remaining:
create_view still reports FAIL due only to formatting-only psql alignment/trailing-space diffs in pg_get_viewdef/pg_get_ruledef output. Latest run: 282/311 queries matched, 267 diff lines.
