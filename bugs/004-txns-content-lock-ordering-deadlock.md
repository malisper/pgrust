# Bug: Lock ordering deadlock between txns RwLock and buffer content_lock

**Files:** `src/executor/commands.rs`, `src/access/heap/am.rs`

## Summary

A 4-thread deadlock caused by inconsistent lock ordering between the
write-preferring `txns` RwLock and the write-preferring buffer `content_lock`
RwLock. The SELECT scan path acquired content_lock then txns, while the
UPDATE/DELETE scan path acquired txns then content_lock.

## Symptoms

Concurrent SELECT + UPDATE workloads on the same table hang indefinitely.
Reproducible with 4+ reader threads and 2+ writer threads operating on a
single-row table.

## Root cause

Two code paths acquired `txns` and `content_lock` in opposite order:

- **SELECT** (`heap_scan_next_visible_raw`): holds `content_lock(shared)` on
  the buffer page, then calls `txns.read()` for CLOG visibility lookup.
- **UPDATE/DELETE scan** (`execute_update_with_waiter`): holds `txns.read()`
  across the `heap_scan_next_visible` call, which internally calls
  `pool.read_page()` → `content_lock(shared)`.

With parking_lot's write-preferring RwLock, pending exclusive waiters block new
shared requests. This enables a 4-thread deadlock cycle:

1. Thread R (SELECT reader): holds `content_lock(P, shared)`, waits for
   `txns.read()` — blocked by WW's pending write.
2. Thread WW (writer committing): pending `txns.write()` — blocked by R2 who
   holds `txns.read()`.
3. Thread R2 (UPDATE writer scanning): holds `txns.read()`, waits for
   `content_lock(P, shared)` — blocked by W's pending exclusive.
4. Thread W (UPDATE writer claiming tuple): pending `content_lock(P, exclusive)`
   — blocked by R who holds shared.

Cycle: R → WW → R2 → W → R.

## Fix

Changed `execute_update_with_waiter` and `execute_delete_with_waiter` to not
hold `txns.read()` across `heap_scan_next`. Instead, the scan and visibility
check are split:

1. `heap_scan_next()` — acquires/releases `content_lock` (no `txns` held)
2. `txns.read()` + `tuple_visible()` — acquires/releases `txns` (no
   `content_lock` held)

This ensures consistent lock ordering: every path either acquires
`content_lock` before `txns`, or never holds both simultaneously.

## How PostgreSQL handles this

PostgreSQL avoids this by not having a single global lock for transaction status.
The CLOG is an SLRU with per-page lightweight locks, completely independent of
buffer content locks. The lock ordering is always buffer content lock first, then
CLOG SLRU lock — no path ever reverses this order.
