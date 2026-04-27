Goal:
Close non-parallel select_distinct regression diffs while leaving parallel DISTINCT paths out of scope.

Key decisions:
- Removed DISTINCT-to-GROUP-BY lowering so DISTINCT is planned as DISTINCT.
- Added enable_sort GUC plumbing and plan-cache invalidation alongside enable_hashagg.
- Added IncrementalSort path/plan/executor support and EXPLAIN Sort Key/Presorted Key output.
- DISTINCT planning now generates sorted Unique paths, incremental-sort paths, hash aggregate paths when allowed, and Limit 1 paths for single-valued DISTINCT keys.
- DISTINCT pathkey exploration now considers permutations and prefixes so indexes can provide useful ordering.
- Added a narrow no-heap-column index-only enumeration path for DISTINCT constant/function targets.
- Added narrow select_distinct dependency setup for tenk1_hundred because full create_index setup fails later on unsupported syntax in this branch.
- Committed the initial planner/executor slice as 2504ba6ef.
- Follow-up display fixes qualify verbose aggregate/scan output, preserve ordered Subquery Scan display, normalize expression sort-key parentheses, reorder simple equality filter conjuncts last, and match PostgreSQL child indentation for Unique/Aggregate/SubqueryScan.

Files touched:
- src/backend/optimizer/plan/planner.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/path/mod.rs
- src/backend/optimizer/setrefs.rs
- src/backend/optimizer/pathnodes.rs
- src/backend/commands/explain.rs
- src/backend/executor/nodes.rs
- src/backend/executor/startup.rs
- src/include/nodes/{pathnodes,plannodes,execnodes}.rs
- src/pgrust/session.rs
- small exhaustive-match updates in tests and relation ref walkers

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet disabled_hashagg
- scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1
- CARGO_TARGET_DIR=/tmp/pgrust-target-nairobi-v2-regress scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
- Latest isolated select_distinct rerun fails at 100/105, 90 diff lines.
- Remaining diffs are accepted out-of-scope parallel DISTINCT plans and result ordering from absent Gather/Gather Merge/Parallel Scan support.
