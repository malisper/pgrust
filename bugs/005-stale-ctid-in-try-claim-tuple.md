# Bug: Stale ctid in try_claim_tuple causes lost updates

**Commit:** 0118093  
**Files:** `src/access/heap/am.rs`

## Summary

`try_claim_tuple` captured the tuple's `ctid` before dropping the buffer lock,
then used that stale value after checking xmax status. If the updater wrote ctid
and committed between those two points, the stale ctid (still pointing to self)
caused the function to return `Deleted` instead of `Updated`, silently losing
the concurrent update.

## Symptoms

Under heavy concurrent UPDATE contention on the same row, some updates are
silently lost. The final counter value is lower than expected (e.g., 19 instead
of 20 for 4 threads x 5 increments). No error is reported — the update just
vanishes.

## Root cause

`try_claim_tuple` reads the tuple under an exclusive buffer lock, capturing
both `xmax` and `ctid`. It then drops the lock and checks xmax status via the
transaction manager. In the `Committed` branch, it uses the captured `ctid` to
distinguish `Deleted` (ctid == self) from `Updated` (ctid != self).

The race:

1. **Thread A** claims the tuple: sets `xmax=A`, releases lock. `ctid` still
   points to self (A hasn't written the new ctid yet).
2. **Thread B** enters `try_claim_tuple`, acquires lock, reads `xmax=A` and
   `ctid=self` (stale). Drops lock.
3. **Thread A** inserts new version, re-acquires lock, writes `ctid=new_tid`,
   releases lock, commits.
4. **Thread B** checks `status(A)` → `Committed`. Uses stale `ctid=self` →
   `ctid == target_tid` → returns `ClaimResult::Deleted`.
5. Caller treats `Deleted` as `TupleAlreadyModified` → gives up on the row.
   The update that Thread B was attempting is lost.

The key insight: Thread B captured `ctid` at step 2, before Thread A wrote it
at step 3. By the time Thread B sees `Committed`, the page has the correct
`ctid`, but Thread B never re-reads it.

## Fix

In the `Committed` branch, re-read the tuple from the page to get the current
`ctid` instead of using the stale copy. This is safe because once `xmax` is
committed, the old tuple's `ctid` is immutable — no transaction will modify it
again.

## How PostgreSQL handles this

PostgreSQL's `heap_update` holds the buffer content lock across the entire
check-xmax, set-xmax, write-ctid sequence (the "L1" lock in heapam.c). The
lock is not dropped between reading xmax and writing ctid, so no other backend
can observe a half-written state. In pgrust, `try_claim_tuple` drops the lock
before checking status (to avoid holding the buffer lock during CLOG lookups),
which creates the window for this race.
