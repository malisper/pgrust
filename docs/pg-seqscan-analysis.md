# PostgreSQL Sequential Scan Analysis

Analysis of how PostgreSQL executes a simple `SELECT * FROM table` vs pgrust,
focused on per-tuple overhead.

## Key PG optimizations pgrust is missing

### 1. Batched visibility at page level
PG calls `heap_prepare_pagescan()` once per page, collecting all visible tuple
offsets into an `rs_vistuples[]` array. The per-tuple inner loop just indexes
into that array with no visibility checks. pgrust checks visibility on every
single tuple.

### 2. Lazy deformation / zero-copy slot
PG stores a pointer to the on-page tuple in the slot (`tts_nvalid = 0`).
Attributes are only deformed when accessed. For `SELECT *` going to the wire,
raw tuple bytes can be sent without deforming. pgrust eagerly decodes every
column into `Value` objects per row.

### 3. Function pointer dispatch
PG uses `node->ExecProcNode(node)` -- a single function pointer call. pgrust
uses a `match` on an 8-arm enum per tuple.

### 4. No copies until necessary
PG's `ExecStoreBufferHeapTuple` stores a pointer to the buffer page -- zero
copies. pgrust copies tuple data into `Vec<Value>` per row, causing allocation
churn.

### 5. Specialized scan variants
PG has separate functions for no-qual, qual-only, projection-only, and
qual+projection (nodeSeqscan.c). Eliminates per-tuple branches.

### 6. Aggressive inlining
`ExecProcNode`, `ExecScanFetch`, `ExecScanExtended` are all
`pg_attribute_always_inline`. Per-tuple dispatch is ~50-70 instructions.

## PG execution flow

```
ExecutePlan loop (execMain.c:1703)
  -> ExecProcNode (function pointer, inline)
    -> ExecSeqScan (nodeSeqscan.c:110)
      -> ExecScanExtended (execScan.h:160, inline)
        -> ExecScanFetch (execScan.h:31, inline)
          -> SeqNext (nodeSeqscan.c:50)
            -> table_scan_getnextslot (tableam.h)
              -> heap_getnextslot (heapam.c:1387)
                -> heapgettup_pagemode (heapam.c:1009)
```

### heapgettup_pagemode (the hot path)
Two-level loop: outer over pages, inner over tuples.
- On new page: `heap_prepare_pagescan()` does visibility for all tuples at once
- Per tuple: index into `rs_vistuples[]`, `PageGetItemId`, `PageGetItem`, return
- ~15-20 instructions per tuple within a page

### heap_prepare_pagescan (batched visibility)
- Locks buffer once (shared)
- If page is all-visible, skips per-tuple checks entirely
- Otherwise calls `page_collect_tuples()` with constant-folded args
- Stores visible offsets in `scan->rs_vistuples[]`
- Unlocks buffer
- Cost: one lock/unlock per page, amortized across ~185 tuples

### TupleTableSlot (lazy deformation)
- `ExecStoreBufferHeapTuple` stores a pointer, not a copy
- `tts_nvalid = 0` means no attributes deformed yet
- `tts_values[]` / `tts_isnull[]` filled lazily on demand
- For simple SELECT * to wire protocol, may never deform at all

## Profiling results (pgrust, 10k rows x 10k iterations)

Top hotspots by source line:

| Pct   | Location         | Description                           |
|-------|------------------|---------------------------------------|
| 6.4%  | mod.rs:372       | match dispatch in exec_next_inner     |
| 4.7%  | mod.rs:271       | execute_plan top-level loop           |
| 2.9%  | nodes.rs:359     | TupleSlot::into_values match          |
| 2.4%  | page.rs:136      | page_get_max_offset_number per tuple  |
| 2.0%  | tuple_decoder    | decode_inner prologue                 |
| 1.9%  | mod.rs:364       | exec_next_inner function entry        |
| 1.9%  | nodes.rs:329     | TupleSlot::values match dispatch      |
| 1.6%  | nodes.rs:349     | returning materialized values         |
| 1.5%  | page.rs:207      | page_get_item reading tuple bytes     |

~18% is pure dispatch/indirection overhead.

## Implemented optimizations

### 1. Batch visibility per page (DONE)
Collect visible tuple offsets once per page into `vis_tuples[]` array (like
PG's `rs_vistuples`), then iterate without per-tuple visibility checks.
Also eliminates per-tuple `page_get_max_offset_number` and `page_get_item_id`
calls which were ~5% of profile samples.

Result: 0.585ms -> 0.454ms per 10k-row scan (22% faster).
EXPLAIN ANALYZE 1M rows: 64.35ms -> 59.11ms (ratio vs PG: 1.42x -> 1.27x).

## Remaining optimization opportunities

1. **Lazy/zero-copy slot** -- store pointer to on-page bytes in TupleSlot
   instead of eagerly decoding into Values. Only decode when values are accessed.
   Expected savings: ~8-12% from eliminating per-tuple decode + allocation.

2. **Function pointer dispatch** -- replace match enum with function pointer.
   Expected savings: ~3-5% from eliminating per-tuple branch prediction misses.
