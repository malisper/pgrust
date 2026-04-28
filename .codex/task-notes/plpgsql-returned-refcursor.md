Goal:
Make PL/pgSQL refcursors returned from functions visible to later PL/pgSQL and
SQL FETCH operations.

Key decisions:
Add `ExecutorContext.pending_portals` as a narrow handoff from PL/pgSQL runtime
to the owning session. Export open PL/pgSQL cursors as materialized visible
portals after successful function execution. PL/pgSQL FETCH/MOVE can read these
pending portals before the function call returns to the session, which fixes
nested returned-refcursor usage. Session drains pending portals only for
successful statements inside active transactions, so autocommit cursors do not
leak past statement end.

Files touched:
src/backend/executor/mod.rs
src/pl/plpgsql/exec.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs
ExecutorContext construction sites updated mechanically with
`pending_portals: Vec::new()`.

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-returned-refcursor

Remaining:
Regression is at 2158/2271 matched with 1332 diff lines. The named cursor
parameter diagnostics now mostly differ only by LINE/caret/context formatting;
larger open semantic clusters remain in records/composite returns,
diagnostics/context, SELECT INTO, transition tables, and polymorphic planner
output.
