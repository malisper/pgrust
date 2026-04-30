Goal:
Diagnose GitHub regression failures for namespace, tidscan, and create_type.

Key decisions:
Downloaded regression artifacts from GitHub run 25085589298 into /tmp/pgrust-regression-25085589298.
The requested diffs are in /tmp/pgrust-regression-25085589298/regression-results/diff/.
namespace is mostly GUC/search_path support and CREATE SCHEMA edge behavior.
tidscan is missing TidScan planning/explain support plus WHERE CURRENT OF syntax/execution.
create_type is split between COMMENT ON COLUMN for composite types, widget literal/operator casting, and incomplete ALTER TYPE support proc metadata/dependency propagation.
Implemented the focused fixes on malisper/regress-13-11 and rebased them onto origin/perf-optimization before PR creation.

Files touched:
.codex/task-notes/regression-namespace-tidscan-create-type.md
See .codex/task-notes/regression-13-11.md for the implementation file list.

Tests run:
No local test reruns; diagnosis used downloaded GitHub regression artifacts.
Final validation ran scripts/cargo_isolated.sh check plus focused create_type, namespace, and tidscan regression runs.

Remaining:
None for these focused regression files.
