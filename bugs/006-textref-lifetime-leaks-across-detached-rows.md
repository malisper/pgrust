# Bug: `TextRef` values escaped pinned tuple lifetimes through detached row caches

**Files:** `src/backend/executor/nodes.rs`, `src/backend/executor/exec_expr.rs`, `src/backend/executor/srf.rs`

## Summary

Several executor paths cloned `Value` arrays out of a pinned heap-backed
`TupleSlot` and stored them in detached structures like virtual slots,
join/sort row caches, and correlated-subquery outer-row stacks. Those cloned
values could still contain `Value::TextRef`, which is only a raw pointer and
length. Once the original slot's buffer pin went away, later reads could
interpret reused memory as text and return garbage bytes.

## Symptoms

The clearest visible failure was
`pgrust::database::tests::create_index_and_alter_table_set_are_noops`, where
catalog joins sometimes returned corrupted `pg_proc.proname` strings such as:

- `"\u{8}\u{18}\0\r\u{6}\0"` instead of `booleq`
- `"\0\0\0\0\0\0"` instead of `int4eq`
- `"<@\0\u{b}\0\0"` instead of `boolle`

This looked like a `name` storage bug at first, but the corruption pattern
depended on plan shape and which rows had been cached, not on any one catalog
table layout.

## Root cause

`Value::TextRef` in
[src/include/nodes/datum.rs](/src/include/nodes/datum.rs:295)
does not own the underlying bytes or the buffer pin that protects them.

That is safe only while the value stays inside the original heap-backed slot.
The bug was that some executor paths crossed that boundary without first
materializing borrowed text into owned `Value::Text`.

Affected paths:

- join row caches in
  [src/backend/executor/nodes.rs](/src/backend/executor/nodes.rs)
- sort buffered rows in the same file
- `ProjectSet` cached input rows in the same file
- correlated-subquery `outer_rows` in
  [src/backend/executor/exec_expr.rs](/src/backend/executor/exec_expr.rs)
- scalar subquery result extraction in the same file
- select-list SRF scalar output extraction in
  [src/backend/executor/srf.rs](/src/backend/executor/srf.rs)

## Fix

Materialize cloned row values before storing them in any detached executor
state:

- call `Value::materialize_all(...)` before caching rows in join/sort state
- materialize `outer_rows` used for correlated subqueries
- materialize `ProjectSet` cached input rows
- materialize scalar subquery and scalar SRF outputs before they escape the
  source slot

## Why this is the right boundary

The buffer pin protects the bytes only while the row remains attached to the
heap-backed `TupleSlot`. A virtual row or cached `Vec<Value>` does not retain
that pin. The safe rule is:

- borrowed `TextRef` is allowed inside the original pinned slot
- any row/value copied into longer-lived executor state must be owned first

## Verification

- `cargo test --lib pgrust::database::tests::create_index_and_alter_table_set_are_noops -- --exact --nocapture`
- `RUST_MIN_STACK=33554432 cargo test --lib --quiet`

Result after the fix:

- `913 passed, 0 failed`
