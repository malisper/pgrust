# Remove the per-page row vector in `UPDATE` / `DELETE`

## Context

`src/executor/commands.rs` now materializes each visible tuple on a page into a
temporary `Vec<(tid, values)>` before evaluating the `UPDATE` / `DELETE`
predicate.

That extra allocation was introduced to make correlated subquery evaluation
borrow-safe: expression evaluation now needs mutable access to
`ExecutorContext`, but the old scan loop still held an immutable borrow of the
buffer page while evaluating predicates and assignment expressions.

The current shape is correct, but it adds avoidable work on hot write paths:

- one `Vec` allocation per scanned page
- one cloned value vector per visible tuple
- extra materialization even when the predicate is cheap and no subquery is
  present

## Goal

Keep correlated-subquery-capable expression evaluation without forcing
`UPDATE` / `DELETE` to buffer the whole page into an intermediate vector.

## Likely approaches

1. Split scan and evaluation more carefully so page borrows end before the
   expression path needs mutable `ExecutorContext`, without collecting every row
   first.
2. Restore a fast path for subquery-free predicates and assignments that can
   still evaluate directly against the current slot while the page is borrowed.
3. Rework page iteration so tuple identity is captured cheaply and tuple values
   are only materialized on demand.

## Why deferred

The current implementation fixed correctness first for correlated subqueries.
It is a good follow-up optimization, but not a blocker for functionality.
