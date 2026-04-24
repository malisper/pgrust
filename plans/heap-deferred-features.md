# Heap Access Method — Deferred Features

This note records what is intentionally missing from the current heap access
method implementation.

The current code is enough to exercise the dependency chain:

- tuple/page physical format
- buffer manager
- storage manager
- heap insert/fetch at a behavioral-model level

It is not a realistic implementation of PostgreSQL heapam yet.

## MVCC is only a first slice

The current code now has a minimal MVCC model:

- tuple versions carry `xmin` / `xmax`
- updates create a new tuple version and chain the old version's `ctid`
- deletes mark `xmax`
- scans and fetches can be filtered through a snapshot

That is enough to model version visibility for simple insert/update/delete
flows, but it is still much simpler than PostgreSQL.

What is still intentionally missing from MVCC:

- no lock or conflict protocol around concurrent updates/deletes
- no command-id visibility rules within a transaction
- no rollback undo of physical tuple changes after abort
- no vacuum or pruning to reclaim dead tuple space
- no HOT updates or redirect/dead line-pointer states
- transaction status is now persisted to a small local status file, but it is
  still much simpler than PostgreSQL's real `pg_xact` / CLOG machinery
- each `begin`, `commit`, and `abort` currently rewrites the whole durable
  status file instead of updating a compact paged/segmented status store
- snapshots are still built by materializing `committed` and `in_progress`
  sets in memory at snapshot creation time
- there is no PostgreSQL-style xid horizon / snapshot-min-snapshot-max logic
  yet; visibility still depends on those fully materialized in-memory sets
- xid wraparound is not handled yet; xid comparisons currently rely on plain
  `u32` ordering with no epoch/wraparound-safe comparison logic
- there is still no crash-recovery pass that resolves leftover in-progress
  transactions after restart
- there is no separate PostgreSQL-style `ProcArray` / active-transaction
  registry yet; snapshot building still leans on the transaction-status map
- there is still no serious multithreaded/concurrent transaction model around
  snapshot building or xid ownership
- no WAL/recovery integration for version transitions
- a very small in-memory transaction manager and snapshot model

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
- it does not consider fillfactor, even if `CREATE TABLE ... WITH
  (fillfactor=...)` is accepted by the SQL layer
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
  Today this is still only a parser-level compatibility knob; the heap access
  method does not reserve free space based on the configured fillfactor yet.
- concurrency and locking behavior around page choice

## Heap API shape is also simplified

The current Rust heap API is intentionally smaller than PostgreSQL's heapam
surface.

Today we have direct helpers like:

- `heap_insert(...)`
- `heap_fetch(...)`
- `heap_scan_begin(...)`
- `heap_scan_next(...)`

That is fine for now, but it is not the likely long-term API.

PostgreSQL has additional concepts here that we will probably need in some
form even if we do not copy its exact C API:

- a scan descriptor/state object
- scan direction
- a slot-like abstraction for decoded rows
- a snapshot / visibility boundary
- clearer separation between physical tuple bytes and logical row values

The goal should be to keep the PostgreSQL semantic boundaries while choosing
a more Rust-native API shape, rather than copying heapam function signatures
literally.

## Other missing heap features

The current heap layer still does not implement:

- `TupleDesc`-driven layout
- wide-table support beyond the current tuple-header attribute-count limit
  The heap tuple header currently stores the physical attribute count in
  `infomask2 & HEAP_NATTS_MASK`, which only has 11 bits in
  [src/include/access/htup.rs](/src/include/access/htup.rs).
  That means the current heap format can only represent up to 2047 columns in
  one tuple, and the null bitmap grows in lockstep with that attribute count.
  This matches PostgreSQL's basic header field width, but it means wide-table
  behavior near that limit is still effectively deferred and should be treated
  as a hard implementation ceiling for now.
- varlena / TOAST behavior
- speculative insertion
- vacuum/prune semantics
- line-pointer reuse policies
- multi-page scan/access APIs
- WAL/recovery semantics for heap/MVCC are still incomplete

## SQL / Executor Front Door Is Still Very Small

The project now has a minimal parser, executor, and REPL, but that SQL layer
is still intentionally tiny compared to PostgreSQL.

What is implemented today:

- single-table `SELECT`
- `INSERT`, `UPDATE`, and `DELETE`
- simple `WHERE` expressions
- `SELECT *` expansion for a single table
- a demo REPL with one hard-coded relation schema

What is still intentionally missing:

- no PostgreSQL-style `pg_catalog` relations yet; catalog metadata is still a
  much simpler persisted file representation rather than heap-backed system
  catalogs
- no `CREATE TABLE`, `ALTER TABLE`, or `DROP TABLE`
- no real catalog storage or schema persistence
- no parser/planner support for joins
- no `ORDER BY`
- no `LIMIT` / `OFFSET`
- no aggregates
- no grouping / `GROUP BY` / `HAVING`
- no subqueries
- no `INSERT ... SELECT`
- no multi-row `INSERT ... VALUES (...), (...)`
- no `RETURNING`
- no aliases
- no qualified names like `table.col`
- no `table.*`
- no mixed target expansion like `SELECT *, id`
- no planner cost model or plan selection
- no DML plan nodes yet; write statements are still executed through direct
  statement dispatch instead of a PostgreSQL-like planned modify-table path
- no tuple-slot / junk-column machinery for a more PostgreSQL-like modify
  executor
- no statement-level locking or concurrency behavior beyond the current heap
  MVCC model
- the REPL is only a convenience binary with one seeded `people` table; it is
  not a general SQL shell

## Recommendation

Do not treat the current page-selection logic as the long-term design.

When the project reaches the point where heap behavior matters beyond simple
end-to-end inserts, the first upgrade should be replacing the current
"try last page, else append" policy with a more PostgreSQL-like free-space
selection path.
