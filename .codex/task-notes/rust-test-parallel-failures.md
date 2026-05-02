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
- `src/backend/catalog/store.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `scripts/cargo_isolated.sh test` initially failed after `/tmp` filled.
- `ulimit -n 65536 && scripts/cargo_isolated.sh test --lib --quiet` captured to `/tmp/pgrust-root-lib-parallel.log`: `3767 passed; 594 failed; 1 ignored`.
- Failed test names extracted to `/tmp/pgrust-root-lib-failed-tests.txt`.
- Sample failed tests passed individually with `--test-threads=1`.
- `cargo fmt --all -- --check` passed.
- `scripts/cargo_isolated.sh check` passed after the FD/cache-scope fixes.
- `scripts/cargo_isolated.sh test --lib --quiet stored_view_query_cache_is_owned_by_database_instance` passed.
- `ulimit -n 65536 && scripts/cargo_isolated.sh test --lib --quiet` passed:
  `4362 passed; 0 failed; 1 ignored`.

Remaining:
Stored view query cache ownership moved into per-database catalog state.
