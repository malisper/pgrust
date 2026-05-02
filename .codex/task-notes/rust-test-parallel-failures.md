Goal:
Run Rust tests and diagnose failures after catalog/node crate split.

Key decisions:
Use `scripts/cargo_isolated.sh` per repo guidance. Full workspace test first
failed when `/tmp` was full. After deleting disposable target dirs, root lib
parallel tests reproduced FD exhaustion. Fixing that exposed a separate
stored-view cache collision.

Files touched:
- `src/backend/access/transam/checkpoint.rs`
- `src/pgrust/cluster.rs`
- `src/backend/rewrite/views.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/utils/cache/lsyscache.rs`
- view-cache call sites under catalog/executor/database code

Tests run:
- `scripts/cargo_isolated.sh test` initially failed after `/tmp` filled.
- `ulimit -n 65536 && scripts/cargo_isolated.sh test --lib --quiet` captured to `/tmp/pgrust-root-lib-parallel.log`: `3767 passed; 594 failed; 1 ignored`.
- Failed test names extracted to `/tmp/pgrust-root-lib-failed-tests.txt`.
- Sample failed tests passed individually with `--test-threads=1`.
- `cargo fmt --all -- --check` passed.
- `scripts/cargo_isolated.sh check` passed after the FD/cache-scope fixes.

Remaining:
Long-term follow-up: move stored view query cache ownership into the database
catalog state instead of keeping a process-global cache. See
`deferred/database-scoped-view-query-cache.md`.
