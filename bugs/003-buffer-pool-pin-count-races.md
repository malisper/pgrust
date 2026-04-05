# Bug: Buffer pool pin count races causing double-unpin / eviction of pinned buffers

**Files:** `src/storage/buffer/mod.rs`, `src/storage/buffer/types.rs`

## Summary

Three race conditions in `BufferPool` could corrupt a buffer's `pin_count`,
causing a pinned buffer to be evicted while a scan was still using it. This
resulted in panics (pin_count underflow), reading wrong page data, and
`NoBufferAvailable` errors under concurrent load.

## Symptoms

Running `bench_select_wire.sh --rows 500000 --clients 32` against the pgrust
server would crash within seconds with:
- `pin_count already 0` panics in `decrement_pin`
- `NoBufferAvailable` errors (all 128 buffers appeared pinned due to underflow
  wrapping the 14-bit pin_count to 16383)
- Occasional wrong query results (scan reading evicted buffer with different
  page data)

## Root causes

### Race 1: `complete_read` / `fail_read` clobbering pin_count

`complete_read` and `fail_read` used `lock_header()` / `unlock_header()` to
modify IO flags. `lock_header` snapshots the full 32-bit state word (including
pin_count) under a spinlock bit, then `unlock_header` does a plain `store()` to
write it back. Meanwhile, `pin_and_bump_usage` uses lock-free CAS to increment
pin_count — it does NOT respect the BM_LOCKED spinlock bit.

If `pin_and_bump_usage` runs between `lock_header` and `unlock_header`, the
store in `unlock_header` overwrites the incremented pin_count with the stale
snapshot, silently destroying a pin.

**Example timeline:**
1. Thread B evicts buffer 107, calls `init_for_io()` (pin_count=1), returns
   `ReadIssued`, starts `complete_read(107)`
2. Thread B: `lock_header()` snapshots state with pin_count=1
3. Thread A (fast path): `pin_and_bump_usage()` → CAS succeeds, pin_count=2
4. Thread B: `unlock_header()` stores snapshot with pin_count=1 — Thread A's
   pin is destroyed
5. Thread B's caller unpins → pin_count=0; buffer is now evictable while Thread
   A is using it

### Race 2: `wait_for_io` same pattern

`wait_for_io` also used `lock_header()` / `unlock_header()` just to read the
IO_IN_PROGRESS flag, with the same pin_count clobbering risk.

### Race 3: Duplicate-check path pinning after dropping locks

In `request_page`'s slow path, when a duplicate tag is found (another thread
already inserted the same tag while we were finding a victim), the code dropped
both the lookup write lock and the strategy lock, *then* called
`pin_and_bump_usage` on the existing buffer. Between dropping the locks and
pinning, another thread could evict that buffer, causing the caller to pin a
buffer that now held a completely different page.

## Fix

Mirrored PostgreSQL's approach to buffer header state management:

1. **`pin_and_bump_usage` / `increment_pin` / `decrement_pin`**: Changed from
   plain `fetch_add`/`fetch_sub` to CAS loops that wait for `BM_LOCKED` to be
   clear before attempting the CAS.  This matches PostgreSQL's `PinBuffer` and
   `UnpinBufferNoOwner`, which both contain `WaitBufHdrUnlocked` calls before
   their CAS.  PostgreSQL comments: "Since buffer spinlock holder can update
   status using just write, it's not safe to use atomic decrement here; thus
   use a CAS loop."

2. **Duplicate-check path**: Moved `pin_and_bump_usage` to run *before* dropping
   the lookup write lock, so the buffer cannot be evicted between finding it in
   the lookup and pinning it.  Matches PostgreSQL's `BufferAlloc` which calls
   `PinBuffer` before `LWLockRelease(newPartitionLock)` with the comment: "Pin
   the existing buffer before releasing the partition lock, preventing it from
   being evicted."

## How PostgreSQL handles this

PostgreSQL packs pin_count, usage_count, and flags into a single `pg_atomic_uint32`
state word (same as us).  The header spinlock (`BM_LOCKED` bit) is used by
`LockBufHdr`/`UnlockBufHdr` to make multi-field changes atomically — the holder
reads the state, modifies several fields, then stores the result.

The key invariant: any code that modifies the state word via CAS (`PinBuffer`,
`UnpinBufferNoOwner`) must first wait for `BM_LOCKED` to be clear.  This ensures
the CAS never races with the spinlock holder's plain store in `UnlockBufHdr`.
Our original code violated this invariant — `pin_and_bump_usage` used CAS
without checking `BM_LOCKED`, so a concurrent `UnlockBufHdr` store could
silently overwrite the pin increment.
