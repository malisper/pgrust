## Context

`pgrust` now has an in-memory stats subsystem with SQL-visible support for:

- builtin stats functions
- `pg_stat_user_tables`
- `pg_statio_user_tables`
- `pg_stat_user_functions`
- `pg_stat_io`
- top-level transaction-aware relation and function stats
- session-local `stats_fetch_consistency` handling

That is enough for the current milestone, but the implementation is still a
pragmatic PostgreSQL-shaped approximation rather than a close copy of upstream
`pgstat` internals.

## Goal

Bring the stats subsystem closer to PostgreSQL's architecture and semantics
without rewriting the current feature surface from scratch.

## Important Follow-ups

### PG-Shaped Flush / Report Path

The current implementation still relies on explicit session-local pending state
and direct flush helpers. PostgreSQL's shape is closer to backend-local pending
entries plus a `pgstat_report_stat()`-style path that commits or periodically
flushes into shared stats storage.

Likely approach:
- introduce a clearer report/flush boundary in `pgstat`
- make commit-time reporting the normal path instead of ad hoc merging
- leave room for later idle-time/background flush behavior

### Transaction / Subtransaction Stats Layer

Top-level transaction behavior is implemented, but savepoint and subtransaction
semantics are still missing. PostgreSQL keeps this logic in a dedicated
transactional stats layer.

Likely approach:
- keep create/drop and xact-local deltas in `pgstat_xact`
- add nested xact frames later instead of mixing subtransaction logic into the
  generic session stats state
- make rollback-to-savepoint semantics a stats-layer concern when savepoints are
  implemented

### Relcache-Driven Relation Stats

Relation stats are currently counted from executor and command hooks without a
PostgreSQL-style relcache-attached pending stats entry.

Likely approach:
- attach relation stats state to relation open / relcache paths
- route scan and DML accounting through that relcache-owned stats state
- reduce the amount of session-global relation bookkeeping

### Function Stats Lifecycle Fidelity

Function call timing and counts exist, but the lifecycle is still simplified.
In particular, committed function-drop cleanup is not yet wired all the way
through the command path.

Likely approach:
- move more of the function stats lifecycle behind `pgstat_function`
- finish committed drop handling
- tighten invalidation behavior around replaced or dropped functions

### Structural `pg_stat_io`

`pg_stat_io` currently uses PG-shaped SQL rows, but the internal storage is
still more string-driven and only a subset of activity is counted.

Likely approach:
- key IO stats by enums or fixed identifiers for backend type, object, and
  context
- expand real counting coverage beyond the current client-backend relation path
- keep unsupported rows zero-filled, but make the supported rows map more
  directly to upstream `pgstat_io.c`

### Persistence and Restart Behavior

Stats are still in-memory only and reset on reopen. PostgreSQL preserves stats
across clean shutdown/startup.

Likely approach:
- add clean-shutdown serialization for the cumulative stats store
- restore the store on startup
- keep pending session-local state transient

### View / Regression Fidelity

The current stats views are operational, but some maintenance fields are still
placeholder zero or `NULL` values and broader PostgreSQL validation remains.

Likely approach:
- compare the stats views more directly against upstream
  `system_views.sql`
- expand regression coverage for nullability, row shape, and edge cases
- fill in remaining maintenance/analyze/vacuum-style fields as the underlying
  counters exist

## Why Deferred

The current subsystem is good enough to merge because the main user-visible
stats surface is present and the snapshot-mode correctness regression is fixed.
The remaining work is primarily about bringing the internal architecture and the
last layer of PostgreSQL fidelity closer to upstream, not about fixing a known
blocker in the current stats behavior.
