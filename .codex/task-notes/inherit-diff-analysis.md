Goal:
Classify failure reasons in /tmp/diffs/inherit.diff, explain the EXPLAIN plan
drift, and implement the first ordered partition Append fix.

Key decisions:
Counted by unified diff hunk as the main unit because one root cause often changes many output lines.
Also counted added ERROR lines where useful.
Explain-plan investigation:
The biggest plan differences are from missing/partial partition order planning:
pgrust often has no ordered Append path, so it either uses a generic MergeAppend
or falls back to Sort -> Append. Expression pathkeys on partitioned children also
often fail to produce ordered child index paths, causing seq-scan Append plans.
Other clusters are join costing/path selection differences, min/max rewrite over
inherited/partitioned inputs, a few bad partition-pruning decisions, and EXPLAIN
renderer fallback to Rust Debug for unresolved Vars/Funcs.

Files touched:
.codex/task-notes/inherit-diff-analysis.md
src/include/nodes/pathnodes.rs
src/backend/optimizer/pathnodes.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/partitionwise.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/tests.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet optimizer::tests
scripts/cargo_isolated.sh test --lib --quiet executor::tests::explain
scripts/run_regression.sh --test inherit --results-dir /tmp/diffs/inherit_after_ordered_append_2 --timeout 120 --skip-build

Remaining:
inherit still fails. One completed rerun before the final cost-selection
narrowing produced 660/884 matched queries and 2815 diff lines in
/tmp/diffs/inherit_after_ordered_append_2. Later reruns hit intermittent
bootstrap server crashes during ANALYZE before reaching inherit, leaving
orphaned pgrust_server processes that were killed. Remaining visible work:
clean EXPLAIN display for unresolved Var/Func sort and join expressions, then
continue min/max, join costing, and partition-pruning follow-ups.
