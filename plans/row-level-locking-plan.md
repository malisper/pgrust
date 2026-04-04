---
name: Row-level locking
overview: Add per-buffer content locks, transaction waiting, EPQ-style retry for concurrent updates, and fix the catalog lock bottleneck so DML/queries run concurrently.
todos:
  - id: content-lock
    content: Add RwLock<()> content lock to BufferFrame + lock_buffer_shared/lock_buffer_exclusive methods
    status: completed
  - id: txn-wait
    content: Add transaction wait mechanism (Condvar on Database) so threads can block until a concurrent transaction finishes
    status: completed
  - id: heap-delete-atomic
    content: Rewrite heap_delete to hold exclusive content lock across check-xmax + set-xmax, with wait-and-retry loop
    status: completed
  - id: heap-update-atomic
    content: Rewrite heap_update with atomic claim, ctid-chain following, and EPQ-style re-evaluation of the WHERE clause against the new version
    status: completed
  - id: catalog-lock-fix
    content: Replace catalog-wide RwLock with per-table locking. Catalog lock held only briefly for name resolution, not during execution. DDL takes AccessExclusive on the table.
    status: completed
  - id: concurrent-update-tests
    content: "Add tests: concurrent updates to same row (no lost updates) and concurrent updates to different rows (parallel)"
    status: in_progress
  - id: save-plan
    content: Copy the final plan to pgrust/plans/row-level-locking-plan.md
    status: pending
isProject: false
---

# Row-Level Locking

## Problem

Two concurrent threads can update the same row simultaneously because:

1. `Database::execute()` takes a catalog **write** lock for all statements, accidentally serializing everything
2. Even if that were fixed, `heap_delete`/`heap_update` have a TOCTOU race: they read xmax, check it's 0, then write xmax as separate buffer pool calls with no lock held across them

## Approach

Follow PostgreSQL's two-layer locking model:

1. **Buffer content lock** (short-lived): exclusive lock on a page during check-and-modify, preventing TOCTOU races
2. **Transaction wait** (long-lived): when xmax is set by an in-progress transaction, release the content lock, wait for that transaction to commit/abort, then retry

After the blocking transaction commits, follow the **ctid chain** to find the new version and **re-evaluate the WHERE clause** against it (PostgreSQL calls this EvalPlanQual). If the new version still matches, proceed with the update/delete against it. If it no longer matches, skip the row.

Also replace the catalog-wide lock with per-table locking that matches PostgreSQL's relation-level lock modes, and release the catalog lock before execution starts.

## Step 1: Add content lock to BufferFrame

**File: [src/storage/buffer/mod.rs](pgrust/src/storage/buffer/mod.rs)**

Add an `RwLock<()>` to each `BufferFrame` as a content lock, separate from the metadata `Mutex`:

```rust
struct BufferFrame {
    inner: Mutex<BufferFrameInner>,
    content_lock: RwLock<()>,      // NEW: protects page access
    io_complete: Condvar,
}
```

Add methods that return lock guards:

```rust
pub fn lock_buffer_shared(&self, buffer_id: BufferId)
    -> Result<std::sync::RwLockReadGuard<'_, ()>, Error>

pub fn lock_buffer_exclusive(&self, buffer_id: BufferId)
    -> Result<std::sync::RwLockWriteGuard<'_, ()>, Error>
```

Initialize `content_lock: RwLock::new(())` in `BufferPool::new`. The existing `read_page`/`write_page_image` methods continue to work without automatically acquiring the content lock (backwards compatible), but callers who need atomicity acquire it explicitly.

## Step 2: Transaction wait mechanism

**File: [src/database.rs](pgrust/src/database.rs)**

Add a `TransactionWaiter` to `Database` (outside the `RwLock<TransactionManager>` so waiters don't hold the read lock while sleeping):

```rust
pub struct TransactionWaiter {
    mu: Mutex<()>,
    cv: Condvar,
}

pub struct Database {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub catalog: Arc<RwLock<DurableCatalog>>,
    pub txn_waiter: Arc<TransactionWaiter>,
}
```

- `Database::execute()` signals `txn_waiter.cv.notify_all()` after every `commit()` and `abort()`
- `TransactionWaiter::wait_for(&self, txns: &RwLock<TransactionManager>, xid)` loops: acquire txns read lock, check status, if still InProgress release read lock and wait on condvar

## Step 3: Rewrite heap_delete with wait-and-retry

**File: [src/access/heap/am.rs](pgrust/src/access/heap/am.rs)**

Add a `txn_waiter: &TransactionWaiter` parameter. New algorithm:

```
loop {
    pin buffer
    lock_exclusive (content lock)
    read tuple from page
    check visibility

    if xmax == 0:
        set xmax = our xid
        write page back
        unlock, unpin
        return Ok

    if xmax set by in-progress txn:
        copy xwait = xmax
        unlock (content lock)
        unpin
        txn_waiter.wait_for(txns, xwait)
        continue  // retry from top

    if xmax set by committed txn:
        the row was already deleted or updated
        unlock, unpin
        if ctid == self: return TupleDeleted
        else: return TupleUpdated { new_ctid }

    if xmax set by aborted txn:
        treat as xmax == 0 (the abort undid the modification)
        set xmax = our xid
        write page back
        unlock, unpin
        return Ok
}
```

This matches PostgreSQL's `heap_delete` with its `l1:` retry label.

## Step 4: Rewrite heap_update with ctid following + EPQ

**File: [src/access/heap/am.rs](pgrust/src/access/heap/am.rs)**

This is the most complex change. The algorithm follows PostgreSQL's pattern:

```
target_tid = original tid from the scan

loop {
    pin buffer for target_tid
    lock_exclusive
    read tuple

    if xmax == 0:
        // We can update this version
        set xmax = our xid (claim the row)
        unlock, unpin
        insert new version
        re-lock old page, set ctid = new_tid
        unlock, unpin
        return Ok

    if xmax set by in-progress txn:
        copy xwait = xmax
        unlock, unpin
        txn_waiter.wait_for(txns, xwait)
        // xwait committed or aborted, retry
        continue

    if xmax set by aborted txn:
        // treat as xmax == 0
        set xmax = our xid
        unlock, unpin
        insert new version
        re-lock old page, set ctid = new_tid
        unlock, unpin
        return Ok

    if xmax set by committed txn:
        unlock, unpin

        if ctid == self:
            // Row was deleted by committed txn, nothing to update
            return TupleDeleted

        // Row was updated to a new version — follow the chain (EPQ)
        new_tid = ctid
        fetch the tuple at new_tid
        RE-EVALUATE the WHERE predicate against the new tuple
        if predicate no longer matches:
            // The new version doesn't satisfy our WHERE clause, skip
            return Ok (0 affected rows for this tuple)
        // The new version still matches — retry the update against it
        target_tid = new_tid
        continue
}
```

The key EPQ addition: when a committed concurrent update is detected, follow the ctid chain to the new version, re-check the WHERE clause, and retry the update against the new version if it still matches. This ensures `UPDATE t SET val = val + 1 WHERE id = 1` produces the correct result (`val = 2`) when two transactions race, rather than erroring.

The WHERE predicate needs to be passed into `heap_update` (or into a new higher-level function). The current `execute_update` in `commands.rs` has the predicate — it will need to pass it through or restructure so the retry loop happens at the executor level.

**Executor-level restructure** (`src/executor/commands.rs`):

The cleanest approach: move the retry loop into `execute_update`. When `heap_update_with_cid` returns `TupleUpdated { new_ctid }`, the executor:

1. Fetches the tuple at `new_ctid`
2. Re-evaluates the WHERE predicate
3. If it matches, computes the new column values and calls `heap_update_with_cid` again with the new tid
4. If it doesn't match, skips the row

Similarly for `execute_delete`: when `heap_delete` returns `TupleUpdated { new_ctid }`, fetch the new version, re-check WHERE, and retry the delete if it still matches.

## Step 5: Per-table locking + release catalog before execution

**Files: [src/catalog.rs](pgrust/src/catalog.rs), [src/database.rs**](pgrust/src/database.rs)

Replace the catalog-wide `RwLock` with two things:

### 5a: Table lock manager

Add a `TableLockManager` to `Database` that tracks per-table locks, matching PostgreSQL's relation-level lock modes:

```rust
pub enum TableLockMode {
    AccessShare,       // SELECT — compatible with everything except AccessExclusive
    RowExclusive,      // INSERT/UPDATE/DELETE — compatible with AccessShare and other RowExclusive
    AccessExclusive,   // DROP TABLE / ALTER TABLE — blocks everything
}

pub struct TableLockManager {
    locks: Mutex<HashMap<RelFileLocator, Vec<TableLockEntry>>>,
    cv: Condvar,
}

struct TableLockEntry {
    mode: TableLockMode,
    holder: ClientId,
}
```

Lock compatibility (matching PostgreSQL):

- `AccessShare` vs `AccessShare`: compatible (concurrent SELECTs)
- `AccessShare` vs `RowExclusive`: compatible (SELECT while INSERT/UPDATE/DELETE runs)
- `RowExclusive` vs `RowExclusive`: compatible (concurrent DML on same table)
- `AccessExclusive` vs anything: conflicts (DDL blocks and is blocked by everything)

`lock_table(&self, rel, mode, client_id)` blocks if the mode conflicts with any existing holder. `unlock_table(&self, rel, client_id)` releases and wakes waiters.

### 5b: Catalog access restructure

Keep the `RwLock<DurableCatalog>` but only hold it briefly during name resolution (parsing/binding), never during execution:

```rust
// SELECT path:
let (plan, rel) = {
    let catalog = self.catalog.read().unwrap();
    let stmt = parse_statement(sql)?;
    let plan = build_plan(&select_stmt, catalog.catalog())?;
    let rel = /* extract RelFileLocator from plan */;
    (plan, rel)
    // catalog lock dropped here
};
self.table_locks.lock_table(rel, AccessShare, client_id);
let result = execute_plan(plan, &mut ctx);
self.table_locks.unlock_table(rel, client_id);

// DML path:
let bound_stmt = {
    let catalog = self.catalog.read().unwrap();
    bind_update(&parsed_stmt, catalog.catalog())?
    // catalog lock dropped here
};
self.table_locks.lock_table(bound_stmt.rel, RowExclusive, client_id);
let result = execute_update(bound_stmt, &mut ctx, xid, cid);
self.table_locks.unlock_table(bound_stmt.rel, client_id);

// DDL path:
self.table_locks.lock_table(rel, AccessExclusive, client_id);
let mut catalog = self.catalog.write().unwrap();
catalog.catalog_mut().create_table(...);
catalog.persist()?;
drop(catalog);
self.table_locks.unlock_table(rel, client_id);
```

This means:

- Two `SELECT`s on the same table run fully concurrently (both hold `AccessShare`)
- An `UPDATE` and a `SELECT` on the same table run concurrently (`RowExclusive` + `AccessShare` are compatible)
- Two `UPDATE`s on the same table run concurrently (both hold `RowExclusive`); row-level locking via xmax handles per-row serialization
- `DROP TABLE` blocks until all other locks on that table are released
- Operations on different tables never block each other at all
- The catalog `RwLock` is held only for the microseconds needed to look up table names, not during execution

## Step 6: Tests

**File: [src/database.rs](pgrust/src/database.rs)**

- **No lost updates**: Create a row with `val=0`. Spawn N threads each doing `UPDATE t SET val = val + 1 WHERE id = 1` M times. Verify final `val = N * M`.
- **Concurrent different rows**: Spawn threads updating different rows. Verify they complete quickly (not serialized).
- **Concurrent update + delete**: One thread updates a row, another deletes it. Verify exactly one succeeds and the row count is consistent.
- **EPQ predicate re-check**: Two threads both try `UPDATE t SET val = 99 WHERE val = 0`. Only one should succeed (the second sees `val = 99` after EPQ and skips).

