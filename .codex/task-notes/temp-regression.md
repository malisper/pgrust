Goal:
Diagnose /tmp/diffs temp-related regression failure.

Key decisions:
Only temp-related artifact found was /tmp/diffs/gin-temp-delete-wal.diff, which is a gin regression diff.
First mismatch is missing gin_clean_pending_list(unknown), before the temp-table section.
Temp-table section then plans disabled sequential scans instead of bitmap GIN scans.
ALTER INDEX SET fastupdate currently routes through btree reloption normalization and rejects GIN reloptions.

Files touched:
.codex/task-notes/temp-regression.md

Tests run:
None; read-only diagnosis.

Remaining:
Implement gin_clean_pending_list, route ALTER INDEX SET through access-method-specific reloption handling, and inspect temp GIN index path selection if failures remain.
