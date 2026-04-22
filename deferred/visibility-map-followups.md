# Visibility Map Follow-ups

`pgrust` now has a PostgreSQL-shaped visibility map fork, `PD_ALL_VISIBLE`
page state, lazy heap vacuum passes, callback-based index vacuum cleanup, and
durable `pg_class` vacuum metadata (`relallvisible`, `relallfrozen`,
`relfrozenxid`).

The remaining gaps from the original visibility-map plan are still deferred:

## Live `XLOG_HEAP2_VISIBLE` Use

The repo now has `RM_HEAP2_ID`, `XLOG_HEAP2_VISIBLE`, and recovery support for
heap-visible records, but the live visibility-map set path still relies on the
existing page-image-heavy plumbing instead of a fully PostgreSQL-shaped
`HEAP2_VISIBLE` write path.

Why this is deferred:
- the storage, vacuum, and catalog behavior needed to land first so the VM fork
  was functionally useful
- finishing the WAL shape cleanly is easier once the heap/vacuum behavior is
  stable and test coverage is in place

## Crash-Safe VM Clears During DML

Heap insert/update/delete paths clear `PD_ALL_VISIBLE` and VM bits, but they do
not yet emit the dedicated crash-safe multi-block WAL record that bundles the
heap page change with the VM clear.

Why this is deferred:
- it needs a purpose-built WAL record shape and replay path instead of the
  current simpler page-image fallback behavior
- the correctness-sensitive part was making heap mutation clear the VM state at
  all; optimizing and tightening the WAL contract is follow-on work

## Full Storage Lifecycle Audit

The touched truncate/rewrite/reset paths now clear `_vm` alongside the main
fork, but the audit is not exhaustive across every heap/toast storage rewrite
site yet.

Why this is deferred:
- the high-value paths for the visibility-map milestone are covered already
- a complete audit wants a separate sweep over catalog/storage helpers so the
  remaining cases are handled systematically instead of opportunistically

## Broader Verification and Vacuum Stress

Targeted vacuum/visibility metadata coverage is in place, but the broader
verification loop still needs another pass:

- the long-running btree vacuum tests should be rechecked to make sure there is
  no performance or locking regression in the new vacuum flow
- the full `cargo test --lib --quiet` and SQL-visible regression surface should
  be rerun to completion after the WAL/storage follow-ups land

Why this is deferred:
- the current milestone established functional behavior and targeted coverage
  first
- the remaining work is better done after the WAL/storage pieces above stop
  changing the maintenance path
