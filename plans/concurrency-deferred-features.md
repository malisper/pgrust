# Concurrency — Deferred Features

This note records what is intentionally missing from the current multi-threading
implementation.

The current code has:

- per-frame `Mutex` on buffer descriptors
- `RwLock` on the buffer tag lookup table
- `Mutex` on the free list / clock sweep strategy
- `Mutex` on the storage backend (I/O serialized)
- `Condvar` per buffer for I/O completion waits
- `AtomicU64` for buffer pool hit/read/written stats
- `Arc<BufferPool>` + `Arc<RwLock<TransactionManager>>` + `Arc<RwLock<DurableCatalog>>`
  in the `Database` handle for cross-thread sharing

That is enough for multiple threads to run concurrent queries against the same
database instance. It is not a realistic implementation of PostgreSQL's
concurrency model.

## Content locks (LWLock per buffer)

PostgreSQL has a separate content lock per buffer — an `LWLock` that supports
shared (read) and exclusive (write) access to page contents. This allows
multiple readers to access the same page simultaneously while writers get
exclusive access.

The current implementation uses a single `Mutex` per frame that protects both
metadata and page contents. This means any access (even a read) serializes
against all other accesses to the same frame.

**To add:** Replace the per-frame `Mutex` with a split design: a spinlock or
small `Mutex` for metadata (pin count, flags) and an `RwLock` for page content.
Readers take a shared content lock; writers take an exclusive content lock.

## Buffer pin hazard tracking

PostgreSQL's pin system uses atomic operations (`pg_atomic_fetch_add_u32`) for
the shared refcount in the buffer header, with a separate backend-private
`PrivateRefCountEntry` array to track which buffers each backend has pinned.
This avoids contending on the buffer header spinlock for pin/unpin in the common
case.

The current implementation uses per-frame `HashMap<ClientId, usize>` under the
frame `Mutex` for pin tracking. This works but is slower than PostgreSQL's
approach for high-contention buffers.

**To add:** Use an `AtomicU32` for the shared pin count and a thread-local
`HashMap` for per-thread pin tracking.

## Transaction isolation levels

The current implementation provides only a single MVCC snapshot model that
behaves like READ COMMITTED — each statement sees a fresh snapshot. PostgreSQL
supports:

- READ UNCOMMITTED (treated as READ COMMITTED)
- READ COMMITTED (snapshot per statement)
- REPEATABLE READ (snapshot per transaction)
- SERIALIZABLE (snapshot per transaction + predicate locks)

**To add:** Store the isolation level on the transaction and conditionally
reuse the transaction-start snapshot for REPEATABLE READ. SERIALIZABLE requires
predicate lock tracking (SIRead locks).

## Row-level locking

`SELECT FOR UPDATE`, `SELECT FOR SHARE`, `SELECT FOR NO KEY UPDATE`, and
`SELECT FOR KEY SHARE` are not implemented. PostgreSQL uses tuple-level lock
bits (`xmax` and infomask flags) to implement these without blocking readers.

**To add:** Extend the tuple header with lock mode flags. Add a lock-wait
queue per tuple or per buffer. Integrate with the executor to acquire row
locks during scan.

## Serializable snapshot isolation (SSI)

PostgreSQL implements SSI via predicate locks (SIRead locks) that track which
tuples and index ranges were read by serializable transactions. Conflicts are
detected by checking for dangerous read-write dependency cycles.

**To add:** A predicate lock manager, conflict detection at commit time, and
serialization failure error handling.

## Deadlock detection

PostgreSQL has a deadlock detector that runs periodically to check for circular
waits in the lock manager. The current implementation has no heavyweight lock
manager and no deadlock detection.

**To add:** Build a wait-for graph from lock waiters and check for cycles on a
timer or when a wait exceeds a timeout.

## Two-phase commit

`PREPARE TRANSACTION` and `COMMIT PREPARED` are not implemented. These allow
transactions to be durably prepared and committed later, even across server
restarts.

**To add:** Persist prepared transaction state to disk and add a recovery path
that resolves prepared transactions on startup.

## Savepoints and subtransactions

`SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, and `RELEASE SAVEPOINT` are not
implemented. PostgreSQL uses a subtransaction stack with sub-XIDs.

**To add:** A subtransaction ID allocator, snapshot nesting, and rollback
logic that reverts changes to the most recent savepoint.

## Background workers

PostgreSQL runs several background processes: bgwriter, checkpointer,
autovacuum launcher, WAL writer, stats collector, etc. None of these exist.

**To add:** Background threads for:
- flushing dirty buffers (bgwriter)
- periodic checkpoints
- dead tuple reclamation (autovacuum)
- WAL writing

## Local buffers for temp tables

PostgreSQL uses a separate unshared buffer pool for temporary tables so they
do not compete with the shared buffer pool or require locking.

**To add:** A per-session `LocalBufferPool` that bypasses the shared pool
for temp-table relations.

## Ring buffer strategy

PostgreSQL uses special buffer replacement strategies for operations that scan
large amounts of data (sequential scans, VACUUM, bulk writes). These strategies
use a small ring of buffers to avoid polluting the shared buffer cache.

**To add:** Strategy objects that override the default clock sweep when a
scan accesses more than a threshold fraction of the pool.

## Advisory locks

`pg_advisory_lock`, `pg_try_advisory_lock`, and related functions are not
implemented.

**To add:** A shared hash table mapping lock keys to wait queues.
