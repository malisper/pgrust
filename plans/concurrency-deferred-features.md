# Concurrency — Deferred Features

This note records what is intentionally missing from the current multi-threading
and row-level locking implementation, and where the implementation simplifies or
diverges from PostgreSQL.

## What is implemented

- per-frame `Mutex` on buffer descriptors (metadata)
- per-frame `RwLock<()>` content lock (shared for reads, exclusive for writes)
- `RwLock` on the buffer tag lookup table
- `Mutex` on the free list / clock sweep strategy
- `Mutex` on the storage backend (I/O serialized)
- `Condvar` per buffer for I/O completion waits
- `AtomicU64` for buffer pool hit/read/written stats
- `Arc<BufferPool>` + `Arc<RwLock<TransactionManager>>` + `Arc<RwLock<DurableCatalog>>`
  in the `Database` handle for cross-thread sharing
- row-level locking via xmax as a claim marker + content lock for atomicity
- transaction wait mechanism (`TransactionWaiter` with condvar)
- EvalPlanQual-style retry: follow ctid chain, re-evaluate WHERE, recompute SET
- per-table lock manager with AccessShare / RowExclusive / AccessExclusive modes
- catalog lock held only briefly for name resolution, not during execution

---

## Simplifications (correct but less capable than PostgreSQL)

### Page copy instead of pointer-into-buffer

PostgreSQL reads tuples via a pointer into the still-pinned shared buffer
(zero-copy). `heap_scan_next` and `heap_fetch` copy the entire 8KB page under
the shared content lock, then work from the copy.

This costs an extra memcpy per page access but avoids the complexity of keeping
tuple pointers valid while the buffer is pinned. The lock is held for only the
duration of the copy.

### Global storage mutex instead of per-relation extension lock

PostgreSQL uses a dedicated `RelationExtensionLock` per relation for extending
files. Extending table A does not block extending table B.

The current implementation wraps `nblocks()` + `extend()` inside a single global
`Mutex<S>` on the storage backend. All storage operations (including extensions
of different relations) are serialized.

**To improve:** Add a per-relation extension lock (e.g., a `HashMap<RelFileLocator,
Mutex<()>>`) so extensions of different relations can proceed in parallel.

### No Free Space Map (FSM)

PostgreSQL uses the FSM to find pages with available space for inserts. The
current implementation always tries the last page and extends the relation if
it is full. This wastes space on tables with fragmented free space.

**To add:** An FSM implementation and integration with `heap_insert_version`'s
page selection logic.

### `try_read` loop for transaction status under content lock

PostgreSQL checks transaction status (via CLOG/pg_xact) under the buffer content
lock without deadlock risk because CLOG has its own partition LWLocks that do not
conflict with buffer content locks.

The current implementation has a single `RwLock<TransactionManager>` that serves
both purposes. Because `parking_lot::RwLock` is write-preferring, a blocking
`read()` call while holding the content lock can deadlock (a pending txns writer
blocks the reader, while the writer waits for another thread that holds the
content lock). The workaround is a `try_read` loop with `yield_now` (10 attempts)
that avoids the deadlock by failing gracefully instead of blocking.

**To improve:** Separate the transaction status store from the transaction
lifecycle lock. Use a concurrent data structure (e.g., `DashMap`) or per-xid
atomics for status lookups, eliminating the need for `try_read`.

### Transaction wait uses condvar + timeout instead of per-xid locks

PostgreSQL uses `XactLockTableWait`, which works through the lock manager.
Every running transaction holds an `ExclusiveLock` on its own xid. A waiter
requests a `ShareLock` on the target xid, which blocks in the lock manager's
wait queue until the target commits or aborts and releases its lock. There is
no race between checking and waiting — the lock request is registered
atomically.

The current implementation uses a `TransactionWaiter` with a `Condvar`. The
waiter checks the transaction status, then sleeps on the condvar with a 10ms
timeout. If `notify_all` fires between the status check and the sleep, the
notification is missed and the thread sleeps for the full 10ms before
re-checking. This adds up to 10ms of unnecessary latency per missed
notification.

**To improve:** Implement per-transaction-id locks matching PostgreSQL's
`XactLockTableWait`. Each transaction holds an exclusive lock on its xid at
start. Waiters request a shared lock on the target xid, which blocks
atomically with no polling or timeouts. On commit/abort, the exclusive lock
is released and all waiters wake immediately.

### No tuple-level lock manager

PostgreSQL uses `heap_acquire_tuplock` to establish priority among waiters for
the same row. This prevents starvation: once you acquire the tuple lock, you are
next in line even if another transaction commits in the meantime.

The current implementation wakes all waiters simultaneously via `notify_all`.
They race to re-acquire the row, with no fairness guarantee. Under very high
contention on a single row, some threads could be starved.

**To add:** A per-tuple or per-page wait queue that grants access in order.

### B-tree writers are serialized per index

PostgreSQL btree inserts and splits rely on page pins, page content locks,
lock-coupled descent, and the `BTP_INCOMPLETE_SPLIT` protocol described in
`src/backend/access/nbtree/README`. Multiple backends can insert into the same
index concurrently as long as they coordinate at the page level.

The current implementation is coarser: `btinsert()` takes a per-index mutex
before entering the write path, so only one thread at a time can modify a given
index relation. Scans still run concurrently, but concurrent writers to the same
index do not.

This is safe, but it is less capable than PostgreSQL and can hide bugs in the
page-level writer protocol by preventing true write/write interleavings.

**To improve:** Remove the per-index writer mutex and finish the PostgreSQL-style
writer path:
- lock-coupled descent through internal pages
- page-local exclusive locking during insert/split
- incomplete-split recovery during descent/ascent
- parent insertion/root split without relation-wide writer serialization

### Single-threaded transaction status persistence

`TransactionManager::persist` rewrites the entire status file on every
`begin`/`commit`/`abort`. PostgreSQL uses CLOG, a paged/segmented structure
where each transaction status is 2 bits, and only the affected page is written.

**To improve:** Replace the flat file with a paged structure similar to
PostgreSQL's CLOG.

---

## Divergences (structurally different from PostgreSQL)

### heap_update splits the lock across three acquisitions

PostgreSQL's `heap_update`, when the new tuple fits on the same page as the
old one, holds the buffer content lock continuously for the entire operation
(check xmax, insert new version, update old version's ctid). When the new
tuple goes on a different page, PostgreSQL releases the lock but uses a
WAL-logged "lock only" marking on the old tuple to signal that it is being
updated (not deleted).

The current implementation always does the update in three separate lock
acquisitions:

1. Lock old page, check xmax, set `xmax = our xid` (claim). Unlock.
2. Insert new version on whatever page has space (separate lock).
3. Lock old page again, set `ctid = new_tid`. Unlock.

Between steps 1 and 3, the old tuple has `xmax` set but `ctid` still points
to itself, which looks like a delete rather than an update. This is safe
because during the gap our transaction is in-progress:

- Concurrent readers see `xmax = InProgress` and treat the tuple as still
  visible (the xmax is not yet committed, so the old version is live).
- Concurrent writers see `xmax = InProgress` and wait for our transaction
  to finish. By the time we commit, ctid is already set correctly.

No transaction can observe the "looks deleted" intermediate state and act on
it incorrectly. However, this differs from PostgreSQL's approach and would
need to change if WAL and crash recovery are added (a crash between steps 1
and 3 would leave a tuple that appears deleted but should appear updated).

---

## Still deferred (not implemented)

### Buffer pin hazard tracking

PostgreSQL's pin system uses atomic operations for the shared refcount with a
separate backend-private `PrivateRefCountEntry` array. The current implementation
uses per-frame `HashMap<ClientId, usize>` under the frame `Mutex`.

**To add:** `AtomicU32` for the shared pin count and thread-local pin tracking.

### Transaction isolation levels

Only READ COMMITTED behavior (snapshot per statement). No REPEATABLE READ or
SERIALIZABLE.

**To add:** Store isolation level on the transaction, conditionally reuse
transaction-start snapshot, predicate lock tracking for SERIALIZABLE.

### SELECT FOR UPDATE / FOR SHARE

Not implemented. PostgreSQL uses tuple-level lock bits (`xmax` + infomask flags).

**To add:** Lock mode flags on tuple header, lock-wait queue, executor
integration.

### Serializable snapshot isolation (SSI)

No predicate locks, no read-write dependency cycle detection.

**To add:** Predicate lock manager, conflict detection at commit time.

### Deadlock detection

No heavyweight lock manager, no deadlock detection. The `parking_lot`
`deadlock_detection` feature is available for debugging but is not integrated
into the runtime.

**To add:** Wait-for graph and cycle detection.

### Two-phase commit

`PREPARE TRANSACTION` / `COMMIT PREPARED` not implemented.

### Savepoints and subtransactions

`SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` not implemented.

### Background workers

No bgwriter, checkpointer, autovacuum, WAL writer, or stats collector.

### Local buffers for temp tables

No separate unshared buffer pool for temporary tables.

### Ring buffer strategy

No special buffer replacement strategy for sequential scans, VACUUM, or bulk
writes.

### Advisory locks

`pg_advisory_lock` and related functions not implemented.
