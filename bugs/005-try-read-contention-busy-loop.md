# Bug 005: try_read contention causes infinite busy-loop on concurrent updates

## Symptom

Under high concurrency (16+ clients), `UPDATE` statements on the same row
intermittently hang or produce `TupleAlreadyModified` errors, causing
lost updates or transaction failures in the pgbench-like workload.

## Root cause

`try_claim_tuple` (am.rs) checks the status of the blocking transaction's
xmax after releasing the buffer lock. It used `txns.try_read()` in a loop
of 10 attempts, falling back to `None` if all attempts failed:

```rust
let xmax_status = {
    let mut status = None;
    for _ in 0..10 {
        if let Some(guard) = txns.try_read() {
            status = guard.status(xmax);
            break;
        }
        std::thread::yield_now();
    }
    status
};

match xmax_status {
    Some(TransactionStatus::InProgress) | None => {
        Ok((ClaimResult::WaitFor(xmax), target_tid))
    }
    ...
}
```

Under contention, all 10 `try_read()` attempts fail because other threads
are committing/beginning transactions (holding the write lock on `txns`).
`None` is treated the same as `InProgress`, causing `WaitFor(xmax)`.

The `wait_for` function checks if the transaction is still in progress —
but it already committed, so `wait_for` returns immediately. The retry
loop calls `try_claim_tuple` again, which again fails `try_read`, creating
an infinite busy-loop:

    try_claim → try_read fails → WaitFor → wait_for returns immediately → retry

## Why try_read was used

The original comment says: "avoid deadlock with parking_lot's write-preferring
RwLock: a pending txns writer would block a blocking read() call." This was
true when the buffer lock was held during the txns status check. But the code
was later refactored to drop the buffer lock (lines 784-785) BEFORE checking
status. After that refactor, `try_read` was no longer needed — `read()` is
safe because there's no lock ordering violation.

## Fix

Replace `try_read()` loop with blocking `txns.read()`:

```rust
let xmax_status = txns.read().status(xmax);
```

This always returns the correct status. The buffer lock is already dropped,
so there's no deadlock risk.

## How PostgreSQL handles this

PG's `TransactionIdIsInProgress()` uses `LWLockAcquire(ProcArrayLock, LW_SHARED)`
which always blocks until acquired — PG never falls back to "unknown." The lock
protects a simple array scan (microseconds), so contention is minimal.

## Reproduction

Test `poc_try_read_contention_lost_update` in database.rs. Uses
`FORCE_TRY_READ_FAIL` flag to deterministically simulate `try_read` failures.
Without the fix, the test hangs (60s timeout). With the fix, it completes
in < 1s.

## Lessons learned

1. Non-blocking lock attempts (`try_read`, `try_lock`) that fall back to a
   default value are dangerous — the default may be incorrect under contention.
2. When a workaround for a deadlock is no longer needed (because the lock
   ordering was fixed), remove it — stale workarounds become bugs.
3. PostgreSQL's approach of always blocking for status checks is simpler and
   correct. The "conservative" fallback to InProgress is not actually
   conservative — it causes incorrect behavior.
