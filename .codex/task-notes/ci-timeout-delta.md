Goal:
Investigate the delta behind CI nextest timeouts for `timestamp_generate_series_accepts_interval_steps` and `bounded_random_results_work_in_typed_comparisons`.

Key decisions:
- Compared current branch against `origin/perf-optimization`; test bodies were unchanged.
- Fixed extracted timestamp generate_series state so capped infinity sets an explicit `finished` flag instead of using an impossible sentinel past `TIMESTAMP_NOEND`.
- Planned root test-driver SELECT statements against one `VisibleCatalog` per statement to avoid repeated broad `CatCache::from_catalog` rebuilds during analyzer lookups.

Files touched:
- `crates/pgrust_executor/src/generate_series.rs`
- `src/backend/executor/driver.rs`
- `.codex/task-notes/ci-timeout-delta.md`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_executor --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet timestamp_generate_series_accepts_interval_steps`
- `scripts/cargo_isolated.sh test --lib --quiet bounded_random_results_work_in_typed_comparisons`
- `scripts/cargo_isolated.sh test --lib --quiet select_sql_with_table_alias`

Remaining:
- `scripts/run_regression.sh` is still an unrelated pre-existing dirty change.
- The broader analyzer `CatalogLookup for Catalog` still rebuilds broad caches per lookup; this patch avoids that path in the executor SQL test driver.
