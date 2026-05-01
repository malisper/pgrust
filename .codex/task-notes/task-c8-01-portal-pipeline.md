Goal:
Stabilize the extended-query protocol, pipeline error recovery, and portal/cursor lifecycle slices for TASK-C8-01.

Key decisions:
- Added connection-level skip-until-Sync state for extended-protocol errors, matching PostgreSQL's `ignore_till_sync` behavior.
- Made extended Bind/Describe/Execute/Close errors enter skip-until-Sync and corrected unnamed prepared statement and missing portal error text.
- Dropped the unnamed prepared statement on simple Query messages, matching PostgreSQL simple-query lifecycle.
- Fixed portal fetch semantics for exact `max_rows` materialized fetches so they suspend until a later fetch proves end-of-portal.
- Preserved cursor source text semicolons in `pg_cursors`, allowed forward-only `FETCH ABSOLUTE n` only when moving forward, and aligned binary/FOR UPDATE cursor scrollability flags.

Files touched:
- `src/backend/tcop/postgres.rs`
- `src/pgrust/portal.rs`
- `src/pgrust/session.rs`

Tests run:
- `scripts/cargo_isolated.sh check` passed; existing unreachable-pattern warnings remain.
- `scripts/run_regression.sh --test psql_pipeline --port 51588 --results-dir /tmp/pgrust-task-c8-01-psql-pipeline-patched` still failed: 74/124 matched, 795 diff lines.
- `scripts/run_regression.sh --test portals --port 54519 --results-dir /tmp/pgrust-task-c8-01-portals-final2` improved to 326/349 matched, 227 diff lines.

Remaining:
- `portals` still has SQL cursor/function and WHERE CURRENT OF semantics gaps, including SQL functions declaring cursors, positioned update/delete diagnostics, and index-only current-row behavior.
- `psql_pipeline` still has broader psql pipeline/simple-query and implicit transaction behavior gaps, including constant SELECT pipeline handling and transaction-block semantics.
