# PostgreSQL Shared Buffer Manager Architecture

This document explains how PostgreSQL's shared buffer manager works today, what parts this Rust rewrite covers, and what is explicitly deferred.

## Covered Source Areas

- [`bufmgr.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/storage/buffer/bufmgr.c)
- [`freelist.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/storage/buffer/freelist.c)
- [`buf_table.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/storage/buffer/buf_table.c)
- [`buf_init.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/storage/buffer/buf_init.c)
- [`buf_internals.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/buf_internals.h)
- [`smgr.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/storage/smgr/smgr.c)
- [`smgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/smgr.h)

## Purpose

The shared buffer manager is PostgreSQL's page cache for relation forks. It maps a `(tablespace, database, relation, fork, block)` identity to one shared buffer frame and mediates:

- cache lookup
- page allocation
- eviction
- pin ownership
- content access synchronization
- dirty tracking
- read and write I/O state

The storage manager (`smgr`) sits below the buffer manager. It presents a relation/fork/block interface that ultimately dispatches to the filesystem-backed implementation in `md.c`.

## Main Objects

### BufferTag

Defined in [`buf_internals.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/buf_internals.h), `BufferTag` is the stable identity for a page:

- `spcOid`
- `dbOid`
- `relNumber`
- `forkNum`
- `blockNum`

This is the key for the shared buffer hash table.

### BufferDesc

Also defined in [`buf_internals.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/buf_internals.h), `BufferDesc` is the per-buffer descriptor. The important fields are:

- `tag`
- `buf_id`
- `state`
- `wait_backend_pgprocno`
- `freeNext`
- `io_wref`
- `content_lock`

The `state` packs three kinds of information:

- refcount
- usage count
- flags

Important flags:

- `BM_VALID`
- `BM_DIRTY`
- `BM_TAG_VALID`
- `BM_IO_IN_PROGRESS`
- `BM_IO_ERROR`
- `BM_JUST_DIRTIED`
- `BM_CHECKPOINT_NEEDED`
- `BM_PERMANENT`

### Mapping Table

`buf_table.c` manages the hash table from `BufferTag` to `buf_id`. It does not take locks itself. Locking is owned by callers in `bufmgr.c`.

Key functions:

- `BufTableLookup()`
- `BufTableInsert()`
- `BufTableDelete()`

### Replacement Strategy State

`freelist.c` owns the replacement strategy:

- freelist of unused buffers
- clock-sweep victim selection
- optional ring strategies for bulk access

The v1 Rust rewrite covers:

- freelist behavior
- default clock-sweep victim selection

The v1 Rust rewrite does not cover:

- bulk-access ring strategies

### SMgrRelation

Defined in [`smgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/smgr.h), `SMgrRelation` is the low-level relation file handle object used by `smgr`.

The important distinction is:

- `bufmgr` is the cache and synchronization layer
- `smgr` is the abstract physical I/O layer

## Core Read Path

For a shared-buffer read miss, the C implementation does this:

1. Build a `BufferTag`.
2. Hash it and look in the mapping table.
3. If present:
   - pin the existing buffer
   - if `BM_VALID` is set, this is a hit
   - if not valid, another backend may be reading it or a prior read failed
4. If absent:
   - select a victim buffer
   - install the new tag in the mapping table
   - set `BM_TAG_VALID`
   - begin I/O with `StartBufferIO()`
   - read from `smgr`
   - finish with `TerminateBufferIO(..., BM_VALID, ...)`

Important invariant:

- There must only be one canonical buffer mapping for a given tag.

## Core Write Path

Writing dirty shared buffers in the steady-state path goes through `FlushBuffer()`:

1. `StartBufferIO(..., forInput=false, ...)`
2. inspect page LSN
3. flush WAL first for permanent buffers
4. write page via `smgrwrite()`
5. `TerminateBufferIO(..., clear_dirty=true, ...)`

Important covered semantic for the Rust model:

- successful flush clears dirty state
- failed flush does not clear dirty state

The Rust model does not implement WAL ordering in v1. That is documented as deferred.

## Pins, Locks, and Ownership

PostgreSQL distinguishes:

- pins: protect a buffer from reuse
- content locks: protect page content access
- mapping locks: protect the tag-to-buffer mapping

The Rust model preserves pin semantics and canonical-mapping semantics, but does not model PostgreSQL's exact lock implementation. It uses deterministic API operations instead of LWLocks/spinlocks/condition variables.

## Replacement Behavior

Default replacement in PostgreSQL uses:

- freelist if an unused buffer is available
- clock sweep otherwise

A buffer can only be reused if it is not pinned and is otherwise eligible. Usage counts are decremented during clock sweep until a reusable victim is found.

The Rust v1 model preserves:

- freelist preference
- usage-count-based clock sweep
- no reuse of pinned buffers

## Failure Behavior

Important covered cases:

- read miss that completes successfully transitions to valid
- read miss that fails leaves the page not valid
- a second client requesting a page while a read is in progress attaches to the same canonical buffer
- flush failure retains dirty state for retry

## Deferred PostgreSQL Functionality

These are intentionally not implemented in the Rust v1 model:

- `localbuf.c`
- `StartReadBuffers()` / `WaitReadBuffers()`
- AIO ownership and callback plumbing
- checkpoint-specific dirty tracking (`BM_CHECKPOINT_NEEDED`)
- bgwriter scheduling and writeback pacing
- WAL flush-before-data semantics
- recovery and redo buffer reads
- real `smgr` -> `md.c` filesystem details
- exact shared-memory layout and lock protocol

## Rewrite Boundary

The Rust rewrite in `pgrust/` should be treated as a **behavioral specification model**, not as an in-tree PostgreSQL replacement. Its job in v1 is to lock down the semantics of the covered shared-buffer behavior and make them testable.

