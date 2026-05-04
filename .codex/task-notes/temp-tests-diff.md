Goal:
Diagnose and fix the ON COMMIT/FK portion of the temp regression diff from .context/attachments/pasted_text_2026-05-04_16-20-02.txt.

Key decisions:
PostgreSQL really raises "unsupported ON COMMIT and foreign key combination" at COMMIT when an ON COMMIT DELETE ROWS temp table is referenced by a temp table that is not also being truncated. pgrust now checks FK dependencies before applying commit-time temp truncates.

Explicit pg_temp CREATE TYPE now resolves to the session temp namespace, records a temp effect, and marks composite type catalog rows as temp persistence. The remaining pasted diff still has separate causes: temp-schema CREATE VIEW rejection aborts the next 2PC block, and PL/pgSQL dynamic EXECUTE of DECLARE CURSOR routes through the read-only executor instead of the session portal path.

Files touched:
.codex/task-notes/temp-tests-diff.md
src/backend/catalog/store/heap.rs
src/pgrust/database/catalog_access.rs
src/pgrust/database/commands/typecmds.rs
src/pgrust/database/temp.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet temp_on_commit_delete_rejects_foreign_key_with_different_action
scripts/cargo_isolated.sh test --lib --quiet pg_temp_function_drop_and_operator_create_reject_prepare
scripts/cargo_isolated.sh check
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh check
scripts/run_regression.sh --test temp --jobs 1 --timeout 180 --results-dir /tmp/diffs/temp-oncommit-fix --port 55437
scripts/run_regression.sh --test temp --jobs 1 --timeout 180 --results-dir /tmp/diffs/temp-oncommit-fix2 --port 55438
scripts/run_regression.sh --test temp --jobs 1 --timeout 180 --results-dir /tmp/diffs/temp-temp-type-fix --port 55439

Remaining:
Implement temp CREATE VIEW support or adjust fixture; teach PL/pgSQL dynamic EXECUTE/session path to handle DECLARE/FETCH portal commands if temp buffer pin tests should pass.
