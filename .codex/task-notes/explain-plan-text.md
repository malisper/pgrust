Goal:
Reduce first-wave regression hunks caused by EXPLAIN/display formatting without changing planner or executor semantics.

Key decisions:
Classified the target cluster as mixed. Only display-only paths were changed: subquery child sort-key deparsing, distance-operator sort-key parentheses, and same-column index qual display order. Plan shape differences, wrong rows, executor errors, missing index usage, grouping semantics, CTE/view deparse, memoize/lateral failures, and SRF errors were left alone.

Files touched:
src/backend/commands/explain.rs
src/backend/executor/nodes.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet explain_
scripts/cargo_isolated.sh check
scripts/run_regression.sh --skip-build --port 5435 --test select_distinct_on --jobs 1 --timeout 120 --results-dir /tmp/pgrust-regress-select-distinct-on-explain-live
scripts/run_regression.sh --skip-build --port 5436 --test create_index --jobs 1 --timeout 240 --results-dir /tmp/pgrust-regress-create-index-explain-live
scripts/run_regression.sh --skip-build --port 5437 --test aggregates --jobs 1 --timeout 240 --results-dir /tmp/pgrust-regress-aggregates-explain-live

Remaining:
Target diffs still contain semantic planner/executor work: memoize/lateral exec params, incremental sort, grouping sets/grouping(), CTE/view deparse and CTE planning, tsearch function folding, SRF behavior/errors, array index NULL row behavior, and bitmap/index path selection.
