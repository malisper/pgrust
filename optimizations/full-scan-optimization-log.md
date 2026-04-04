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

## Final Result

| Stage | avg_ms_per_scan | rows_per_sec |
|-------|----------------|-------------|
| Baseline | 8.25 | 1,211,721 |
| After Round 1 | ~5.0 | ~2,000,000 |
| After Round 2 | 2.5 | 3,957,228 |
| After Round 3 | **2.16** | **4,620,976** |

**3.8x overall speedup.**

## Key Lessons

1. **Profile before optimizing.** dtrace self-time sampling immediately shows where time is spent.
2. **Copies dominate.** The two biggest wins were eliminating copies: 8KB page copy and per-tuple data copy.
3. **Allocation is death by a thousand cuts.** Each individual malloc is cheap, but 10k+ per scan adds up.
4. **Rc is cheap.** Replacing per-row clones with Rc::clone is nearly free.
5. **Zero-copy APIs matter.** `with_page(&Page)` vs `read_page() -> Page` is the difference between borrowing and copying.
6. **Allocator choice matters.** mimalloc gave 15% for free — no code changes needed beyond swapping the global allocator.
7. **A faster allocator can beat a smarter data structure.** Flat storage to avoid per-row Vec allocations was slower than just using mimalloc with the existing `Vec<Vec<Value>>`. The allocator handles small allocations so efficiently that the overhead of a more complex layout isn't worth it.
8. **Bulk page pinning didn't help.** Re-pinning the same buffer is already a fast-path hit. The overhead of a more complex bulk closure outweighed the savings from fewer pin/unpin calls.
