# Full Table Scan Optimization Log

**Goal:** Get `select * from scanbench` (10k rows, 2 columns: int + text) under 5ms, then under 2.5ms.

**Benchmark:** `full_scan_bench --rows 10000 --iterations 100`

## Baseline: 8.25ms

Profiled with dtrace. Top self-time costs:
- malloc/free: ~40% (per-row allocations everywhere)
- mach_absolute_time: ~10% (per-row Instant::now in exec_next)
- memmove: ~8% (page + tuple data copies)
- parking_lot deadlock detection: ~5%
- MVCC tuple_visible: ~4%

## Round 1: 8.25ms → ~5.0ms (commit e741b07)

### 1. Remove per-row timing in exec_next
- `Instant::now()` + `elapsed()` was called for every row, only needed for EXPLAIN ANALYZE
- Split into `exec_next` (no timing) and `exec_next_inner(timed: bool)`
- Impact: ~10% of samples eliminated

### 2. Eliminate into_values() redundant copy
- `into_values()` called `values()?.to_vec()` — copying already-materialized data
- Changed to move values out of the slot instead of cloning

### 3. Share RelationDesc via Rc instead of cloning per row
- `exec_seq_scan` called `state.desc.clone()` for every row (clones Vec<ColumnDesc>)
- Changed to `Rc<RelationDesc>`, shared across all rows in a scan

### 4. Share column names via Rc<[String]> instead of cloning per row
- `from_heap_tuple` cloned all column name strings for every row
- Pre-computed `Rc<[String]>` in SeqScanState, shared via Rc::clone

### 5. Cache attribute_descs in SeqScanState
- `TupleSlot::values()` called `desc.attribute_descs()` per row, allocating a new Vec
- Pre-computed `Rc<[AttributeDesc]>` once at scan start

### 6. Zero-copy deform: return &[u8] slices
- `HeapTuple::deform()` returned `Vec<Option<Vec<u8>>>` — .to_vec() per column
- Changed to return `Vec<Option<&[u8]>>` borrowing from tuple data
- Also updated `decode_value` to accept `&[u8]` instead of `Vec<u8>`

### 7. Make parking_lot deadlock_detection a Cargo feature
- 31 samples on `acquire_resource`/`release_resource` per lock operation
- Moved `deadlock_detection` to opt-in feature, tests use `--features deadlock_detection`

## Round 2: ~5.0ms → 2.5ms (commit 3ea4cd7)

Re-profiled. New top costs:
- memmove (210 samples, ~20%): HeapTuple::parse copying tuple data from page + read_page copying 8KB page
- malloc/free (~339 samples, ~33%): per-row String allocation, Vec allocations
- SipHash (63 samples): buffer pool hash lookups

### 8. Zero-copy scan: heap_scan_next_visible_raw()
- Old path: pin buffer → copy 8KB page → copy tuple data → unpin → check visibility → deform
- New path: pin buffer → callback with raw tuple bytes → deform in-place → unpin
- Avoids HeapTuple::parse entirely (no data copy, no null_bitmap copy)
- Impact: 4.5ms (10% improvement)

### 9. Fused deform+decode: decode_tuple_from_bytes()
- Old: deform_raw() → Vec<Option<&[u8]>> → decode_value() per column
- New: single pass over raw bytes, producing Vec<Value> directly
- Eliminates intermediate Vec allocation
- Impact: 4.43ms (small improvement)

### 10. CompactString for short text values
- Value::Text(String) forced heap allocation for every text value
- Added CompactString: stores ≤22 bytes inline on the stack, falls back to String
- Benchmark strings ("row-1234") are 5-9 bytes, all inline
- Impact: minimal alone, but eliminates per-row malloc for text

### 11. Zero-copy page access: BufferPool::with_page()
- `read_page()` copied the entire 8KB page out of the buffer frame
- Added `with_page()` that passes `&Page` reference to a closure
- The scan now does all work (visibility check, deform, decode) inside the closure
- This was the **single biggest win**: 4.47ms → 2.5ms (~44% speedup)
- The 8KB memmove per page was the dominant remaining cost

## Round 3: 2.5ms → 2.16ms (commit 5d09ee6)

Re-profiled. malloc/free was ~23% of self-time. Tried two approaches:

### 12. mimalloc allocator (winner)
- Replaced system allocator with mimalloc in the benchmark binary
- mimalloc has thread-local free lists and is much faster for small allocations
- Zero code changes to the library — just `#[global_allocator]`
- Impact: 2.53ms → 2.16ms (15% improvement)

### Arena allocator (rejected)
- Tried collecting all row Values into a flat `Vec<Value>` and splitting at the end
- The final split back into `Vec<Vec<Value>>` (required by `StatementResult` API) undid the benefit
- Result was actually slower (2.27ms) than simple mimalloc (2.16ms)
- A proper arena would require changing `StatementResult` to use flat storage throughout

### Attempted: Flat QueryResult storage (reverted)
- Tried replacing `Vec<Vec<Value>>` with flat `Vec<Value>` + column count
- Required refactoring `StatementResult` and all consumers
- Net result was ~2.2ms — slightly slower than 2.16ms with mimalloc alone
- The per-row `Vec<Value>` allocation is already cheap with mimalloc
- Also tried bulk page pinning (processing all tuples per page in one pin) —
  didn't help because re-pinning the same buffer is already a fast-path hit
- Reverted in favor of keeping the simpler API

## Round 4: 2.16ms → 1.26ms

Re-profiled with 5000 iterations for better sample quality. Top costs:
- MVCC check_visibility: 13.5% — BTreeMap lookup per tuple
- exec_next_inner: 8.7% — per-row executor dispatch (including Projection cloning)
- SipHash: 5% — buffer pool + pins_by_client hash lookups

### 13. Eliminate identity projection for select *
- `select *` always wrapped SeqScan in Projection, which cloned every value per row
- Added optimization in `build_plan`: if targets are Column(0), Column(1), ..., Column(n-1)
  matching all columns, skip the Projection node entirely
- Impact: 2.15ms → 1.74ms (19% improvement)

### 14. FxHashMap for buffer pool lookup and pins_by_client
- Replaced `std::collections::HashMap` (SipHash) with `rustc_hash::FxHashMap`
  for both the buffer pool tag→id lookup and per-frame pins_by_client
- FxHash is much faster for small integer-like keys (no cryptographic overhead)
- Impact: 1.74ms → 1.26ms (28% improvement)

## Round 5: 1.26ms → 0.94ms

### 15. Inline hot path functions
- Added `#[inline]` / `#[inline(always)]` to MVCC check_visibility,
  tuple_bytes_visible, page_get_item, page_get_item_id,
  page_get_max_offset_number, and decode_tuple_from_bytes
- Impact: 1.26ms → 1.19ms (6% improvement)

### 16. Hint bits (matching PostgreSQL's approach)
- Added HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMAX_COMMITTED,
  HEAP_XMAX_INVALID hint bit constants in tuple infomask
- INSERT sets HEAP_XMAX_INVALID on new tuples (xmax=0 means not deleted)
- UPDATE/DELETE clears HEAP_XMAX_INVALID when setting a real xmax
- Visibility check sets hint bits lazily on first scan (without marking page dirty)
- Fast path: when hint bits are present, skip the `txns.status()` BTreeMap
  lookup (the most expensive part of MVCC). Still checks
  `transaction_active_in_snapshot()` for snapshot correctness.
- Impact: 1.19ms → 0.94ms (21% improvement)

#### Bugs found and fixed during hint bits implementation:
1. **Setting XMAX_INVALID for xmax=0 in visibility check.** Should only be set
   by INSERT. The visibility check was incorrectly setting it, which made tuples
   with in-progress xmin appear visible on later scans after xmin committed.
2. **Not clearing HEAP_XMAX_INVALID on UPDATE/DELETE.** When setting a real xmax,
   the old XMAX_INVALID flag remained, making the fast path think the tuple was
   not deleted. Fixed in heap_delete, heap_delete_with_waiter, heap_update_with_cid,
   and try_claim_tuple.
3. **Fast path skipping snapshot check.** The initial fast path returned visible
   based solely on hint bits, without checking if xmin/xmax were in the snapshot's
   in-progress set. This made tuples visible to snapshots that were taken before
   the inserting transaction committed. Fixed by always checking
   `transaction_active_in_snapshot()` in the fast path, matching PostgreSQL's
   `XidInMVCCSnapshot()` behavior.

## Round 6: 0.94ms → 0.65ms

Refactored buffer pool to match PostgreSQL's locking architecture.

### 17. Atomic BufferState replacing Mutex
- Replaced `Mutex<BufferFrameInner>` with `AtomicU32` for metadata
  (pin_count, usage_count, valid, dirty, io_in_progress, io_error)
- All hot-path metadata operations are now lock-free
- Combined pin + usage_count bump into single CAS (matching PG's PinBuffer)

### 18. RwLock<Page> content lock
- Page data protected by `RwLock<Page>` instead of Mutex
- Multiple concurrent readers can scan the same page simultaneously
- Write paths use `lock_buffer_exclusive` for atomic read-modify-write

### 19. Unsafe hint bit writes under shared lock
- Hint bits written via unsafe pointer through the shared content lock
- Matches PostgreSQL's SetHintBits — idempotent OR under shared lock
- Dirty flag set atomically via `BufferState::set_dirty()`
- Removed `with_page_set_hints` method entirely

### 20. Removed pins_by_client
- Removed per-frame `Mutex<FxHashMap<ClientId, usize>>` from hot path
- Pin/unpin is now just an atomic increment/decrement
- Matches PostgreSQL: shared descriptor only has atomic refcount,
  per-backend tracking is local (PrivateRefCount)

### 21. Keep buffer pinned across same-page tuples
- `heap_scan_next_visible_raw` now keeps the buffer pinned when
  returning a tuple — only unpins when advancing to the next block
- Content lock is still released per-tuple (doesn't block writers)
- Saves per-tuple pin/unpin overhead (~10k atomic ops per scan)

### 22. PinnedBuffer RAII guard
- Added RAII guard that auto-unpins on drop
- Prevents pin leaks when functions return early via `?`
- All write-path functions converted to use guards

### Attempted: Pre-allocating rows Vec (no improvement)
- Tried `Vec::with_capacity(10_000)` for the result rows
- No measurable difference — mimalloc's amortized growth is already fast

## Final Result

| Stage | avg_ms_per_scan | rows_per_sec |
|-------|----------------|-------------|
| Baseline | 8.25 | 1,211,721 |
| After Round 1 | ~5.0 | ~2,000,000 |
| After Round 2 | 2.5 | 3,957,228 |
| After Round 3 | 2.16 | 4,620,976 |
| After Round 4 | 1.26 | 8,160,353 |
| After Round 5 | 0.94 | ~10,600,000 |
| After Round 6 | **0.65** | **~15,400,000** |

**12.7x overall speedup.**

## Key Lessons

1. **Profile before optimizing.** dtrace self-time sampling immediately shows where time is spent.
2. **Copies dominate.** The two biggest wins were eliminating copies: 8KB page copy and per-tuple data copy.
3. **Allocation is death by a thousand cuts.** Each individual malloc is cheap, but 10k+ per scan adds up.
4. **Rc is cheap.** Replacing per-row clones with Rc::clone is nearly free.
5. **Zero-copy APIs matter.** `with_page(&Page)` vs `read_page() -> Page` is the difference between borrowing and copying.
6. **Allocator choice matters.** mimalloc gave 15% for free — no code changes needed beyond swapping the global allocator.
7. **A faster allocator can beat a smarter data structure.** Flat storage to avoid per-row Vec allocations was slower than just using mimalloc with the existing `Vec<Vec<Value>>`. The allocator handles small allocations so efficiently that the overhead of a more complex layout isn't worth it.
8. **Bulk page pinning didn't help.** Re-pinning the same buffer is already a fast-path hit. The overhead of a more complex bulk closure outweighed the savings from fewer pin/unpin calls.
9. **Eliminate unnecessary plan nodes.** `select *` going through Projection cloned every value for no reason. Detecting identity projections at plan time was a 19% win.
10. **Hint bits skip lookups, not snapshot checks.** Following PostgreSQL: hint bits avoid the `txns.status()` BTreeMap lookup but still check `transaction_active_in_snapshot()`. Skipping the snapshot check causes correctness bugs with concurrent transactions.
11. **Match the real database's approach.** Reading PostgreSQL's `HeapTupleSatisfiesMVCC` directly revealed that INSERT sets `HEAP_XMAX_INVALID` and UPDATE/DELETE clears it. Getting hint bits right requires matching this protocol exactly.
12. **Atomic state beats Mutex for metadata.** Packing pin_count + usage_count + flags into an AtomicU32 eliminates all locking on the hot path. Combined CAS for pin+usage matches PostgreSQL's approach.
13. **Remove shared per-client tracking.** PostgreSQL tracks pins per-backend locally, not in a shared map. Our `pins_by_client` Mutex was unnecessary contention on every pin/unpin.
14. **Keep pins across same-page tuples.** Unpinning and re-pinning the same buffer between tuples on the same page wastes atomic operations. Keep the pin, release only the content lock.
15. **RAII guards prevent resource leaks.** Manual unpin calls scattered across error paths are fragile. PinnedBuffer guards make pin safety automatic.
