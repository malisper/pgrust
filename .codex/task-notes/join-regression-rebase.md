Goal:
Rebase the current workspace branch onto foreign-key-regression-2 and run the join regression tests with output under /tmp/diffs.

Key decisions:
Renamed the branch to malisper/join-regression-tests before other work. Rebasing onto malisper/foreign-key-regression-2 completed cleanly. The default parallel run hit an isolated worker copied-data startup issue, so the final useful run used --jobs 1.
Row-order-only diffs are planner/executor-order differences, not formatting. A live repro showed pgrust plans the first J1/J2 equijoin as a swapped Hash Join probing J2 and hashing J1, while PostgreSQL 18 plans the same query as a Merge Join with sorted inputs. For FULL JOIN ON FALSE, pgrust uses Nested Loop Full Join and emits left-only rows before unmatched right rows; PostgreSQL uses Merge Full Join and emits right-null-extended rows first. pgrust's PlannerConfig also lacks enable_hashjoin/enable_mergejoin/enable_nestloop, so join.sql SET commands that force PostgreSQL merge/nestloop plans do not affect pgrust.

Files touched:
.codex/task-notes/join-regression-rebase.md

Tests run:
scripts/run_regression.sh --port 55436 --jobs 1 --test join --results-dir /tmp/diffs
scripts/run_regression.sh --port 55438 --jobs 1 --timeout 180 --test join --results-dir /tmp/diffs
scripts/run_regression.sh --port 55442 --jobs 1 --timeout 180 --test join --results-dir /tmp/diffs
scripts/run_regression.sh --port 55446 --jobs 1 --timeout 180 --test join --results-dir /tmp/diffs
pgrust/PostgreSQL local EXPLAIN probes on ports 55477/55478 for the first J1/J2 equijoin and FULL JOIN ON FALSE

Remaining:
join regression currently fails after stripping EXPLAIN-only blocks from the join fixture and aligning the 20 SQL-visible error text/detail mismatches: 564/641 queries matched, 77 mismatched, 1235 diff lines. Main artifacts are /tmp/diffs/output/join.out, /tmp/diffs/diff/join.diff, /tmp/diffs/status/join.status, and /tmp/diffs/summary.json. The wrong-error-text/detail bucket is now 0.
