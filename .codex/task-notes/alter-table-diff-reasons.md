Goal:
Classify reasons for alter_table regression diffs.
Key decisions:
Reran alter_table because /tmp/diffs had been overwritten by a later text run.
Counted causes by unified diff hunk, not by changed line or query.
Inheritance propagation root cause:
pgrust stores inheritance metadata, but ALTER handlers mostly operate on one relation or use shallow recursion. Constraint rename ignores coninhcount/conislocal and does not recurse through children. Drop column explicitly rejects inheritance tree members. Add column recurses, but does not implement PostgreSQL's inherited-column merge/error rules. PostgreSQL centralizes this in tablecmds.c ATPrepCmd/ATSimpleRecursion plus attinhcount/coninhcount checks.
Files touched:
.codex/task-notes/alter-table-diff-reasons.md
Tests run:
scripts/run_regression.sh --test alter_table --results-dir /tmp/diffs_alter_table_reason_count --port 55443
Remaining:
Diff artifacts are in /tmp/diffs_alter_table_reason_count.
