# Heap Access Method — Deferred Features

This note records what is intentionally missing from the current heap access
method implementation.

The current code is enough to exercise the dependency chain:

- tuple/page physical format
- buffer manager
- storage manager
- heap insert/fetch at a behavioral-model level

It is not a realistic implementation of PostgreSQL heapam yet.

## Current insert-page selection is intentionally simple

The current `heap_insert()` algorithm in
`src/access/heap/am.rs` uses a very small policy:

1. ensure the relation exists
2. ask `smgr.nblocks()` for the number of blocks
3. if there are no blocks, create block 0
4. otherwise, try only the last existing block
5. if the tuple does not fit, append one new empty block
6. retry and insert there

That means the implementation is effectively append-only with one probe:

- it does not search older pages for free space
- it does not use a free space map
- it does not consider fillfactor
- it does not prune before deciding a page is full
- it does not coordinate page selection across concurrent inserters

This is good enough for the current behavioral model, but it is much simpler
than PostgreSQL.

## What PostgreSQL really does that we do not

At a high level, PostgreSQL heap insertion is tied into several subsystems
that are not modeled here yet:

- free space search / FSM-assisted page selection
- relation extension coordination
- pruning and reuse of space on existing pages
- visibility / MVCC-aware tuple state transitions
- WAL integration
- fillfactor and page-full heuristics
- concurrency and locking behavior around page choice

## Other missing heap features

The current heap layer also does not implement:

- typed attribute packing/unpacking
- `TupleDesc`-driven layout
- varlena / TOAST behavior
- update/delete/HOT chains
- visibility checks and snapshots
- speculative insertion
- vacuum/prune semantics
- line-pointer reuse policies
- multi-page scan/access APIs

## Recommendation

Do not treat the current page-selection logic as the long-term design.

When the project reaches the point where heap behavior matters beyond simple
end-to-end inserts, the first upgrade should be replacing the current
"try last page, else append" policy with a more PostgreSQL-like free-space
selection path.
