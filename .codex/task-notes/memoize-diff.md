Goal:
Implement memoize-owned regression fixes for the PostgreSQL memoize regression.

Key decisions:
Added planner GUC support for enable_hashjoin, enable_mergejoin, and enable_memoize.
Changed Query.limit_offset to Option<usize> so explicit OFFSET 0 blocks EXISTS pull-up.
Added Plan::Memoize plus executor MemoizeState with hit/miss/eviction/overflow/memory instrumentation.
Wrapped parameterized nested-loop inner plans in Memoize when the inner depends on immediate nest params.
Generalized parameterized inner path creation across subquery/projection/limit/filter wrappers and added a fallback parameterized filter for non-seqscan inner paths.
PL/pgSQL dynamic EXECUTE now passes current planner GUCs through config-aware planning, including dynamic EXPLAIN.
Runtime index arguments now detect casts/functions under boolean clauses, which fixed expression-key Memoize planning.
EXPLAIN actual rows now reports per-loop row counts.
Follow-up pass fixed btree float comparison for -0.0/+0.0, added Memoize cache key labels and runtime index labels, added Heap Fetches reporting for index-only scans, charged Memoize cache memory for key/row/entry overhead, and added a broad unordered btree range-scan cost penalty.

Files touched:
src/include/nodes/pathnodes.rs
src/include/nodes/parsenodes.rs
src/include/nodes/plannodes.rs
src/include/nodes/execnodes.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs
src/backend/parser/analyze/*
src/backend/rewrite/*
src/backend/optimizer/*
src/backend/executor/*
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/relation_refs.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet planner_memoizes_expression_key_nested_loop
scripts/run_regression.sh --test memoize --timeout 60 --port 55445

Remaining:
PR health follow-up fixed the cargo-test failures seen after the memoize work:
system-column outer refs now become immediate nested-loop params, memoized child dependency collection ignores params bound inside the child, disabled seq scans lose to usable index/bitmap alternatives, cross/unqualified nested-loop outers no longer drive runtime index probes, anchored regex btree scans are exempt from the broad range penalty, and direct Database::execute SET/RESET/SHOW now carries planner enable_* GUCs per client.

Tests run in the follow-up:
cargo fmt
git diff --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet unique_array_column_supports_duplicates_and_index_quals -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet create_gin_array_index_uses_bitmap_scan_and_rechecks -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet spgist_index_supports_null_and_bitmap_scans -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet explain_bootstrap_anchored_regex_uses_proname_index_range -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet inherited_scan_tableoid_tracks_physical_child_relation -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet create_index_catalog_paths_and_alter_table_set_parallel_workers -- --nocapture

Remaining known memoize regression residual: the parallel Gather/partial aggregate mismatch is still out of scope.
