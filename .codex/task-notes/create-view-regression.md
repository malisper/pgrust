Goal:
Implement the first slice of the `create_view` regression plan and keep current diffs in `/tmp/diffs/create_view`.

Key decisions:
Branch renamed to `malisper/create_view`. Added an in-process stored analyzed `Query` registry keyed by `_RETURN` rewrite OID, with legacy SQL fallback when no stored query exists. Dependent view rewrites now keep stored queries across relation rename/schema changes instead of forcing deparse/reparse. Used a unique `CARGO_TARGET_DIR=/tmp/pgrust-target-create-view-munich` when shared Cargo target locks blocked validation.
CI follow-up: stored view function targets now validate dropped composite attributes during view expansion, while SQL functions declared to return named composites project appended table columns away when the caller has an older analyzed row shape.

Files touched:
`src/backend/rewrite/{mod.rs,views.rs,rules.rs}`, `src/backend/catalog/store/heap.rs`, `src/backend/executor/{driver.rs,sqlfunc.rs}`, `src/backend/parser/analyze/coerce.rs`, `src/pgrust/database/commands/{create.rs,execute.rs,rename.rs}`, `src/pgrust/database/ddl.rs`.

Tests run:
`cargo fmt`
`CARGO_TARGET_DIR=/tmp/pgrust-target-create-view-munich cargo check`
`CARGO_TARGET_DIR=/tmp/pgrust-target-create-view-munich scripts/run_regression.sh --test create_view --results-dir /tmp/diffs/create_view --timeout 120 --port 6551`
`CARGO_TARGET_DIR=/tmp/pgrust-target-create-view-munich scripts/cargo_isolated.sh test --lib --quiet rows_from_view`
`CARGO_TARGET_DIR=/tmp/pgrust-target-create-view-munich scripts/cargo_isolated.sh check`

Remaining:
Latest `create_view` run fails at 219/311 matched, 92 mismatched, 1816 diff lines. Current working diff is saved to `/tmp/diffs/create_view/working.diff`. Remaining groups are mostly deparser/analyzer/runtime gaps: join alias/USING deparse, CTE and whole-row rendering, special SQL function forms in FROM (`tt20v`, `tt201v`), row-valued `ANY`/`ALL` over `VALUES`, function rowtype runtime behavior after dropped columns, restricted view SELECT path still not matching PostgreSQL, and cascade count differences for temp/dependent views.
