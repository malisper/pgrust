# Audit: table-AM analyze-scan vtable slots + heap provider (K1)

Date: 2026-06-15
Scope: `types-tableam` (vtable slots), `backend-access-heap-heapam-handler-core`
(heap provider), `backend-access-table-tableam(-seams)` (dispatch + seams).

C sources audited against:
- `src/include/access/tableam.h` — the `scan_analyze_next_block` /
  `scan_analyze_next_tuple` vtable slots + `table_beginscan_analyze` /
  `table_scan_analyze_next_block` / `table_scan_analyze_next_tuple` inlines (PG18.3).
- `src/backend/access/heap/heapam_handler.c` — `heapam_scan_analyze_next_block`
  / `heapam_scan_analyze_next_tuple`.
- `src/backend/commands/analyze.c` — `acquire_sample_rows` (the consumer).

## Vtable slot signatures (types-tableam)

PG18.3 has exactly TWO analyze slots (no `relation_analyze` slot exists in 18.3;
`table_beginscan_analyze` is just `scan_begin` with `SO_TYPE_ANALYZE`). Both
slots follow the #289 mcx-vtable convention (`for<'mcx> fn` with leading `Mcx`):

- `scan_analyze_next_block(mcx, scan: &mut TableScanDescData, next_buffer) -> PgResult<bool>`
- `scan_analyze_next_tuple(mcx, scan, oldest_xmin: TransactionId, liverows: &mut f64, deadrows: &mut f64, slot: &mut SlotData) -> PgResult<bool>`

The C `scan_analyze_next_block(scan, ReadStream *stream)` second arg is modeled
as `next_buffer: &mut dyn FnMut() -> PgResult<Buffer>`: the heap callback's ONLY
use of `stream` is `read_stream_next_buffer(stream, NULL)`, and `ReadStream`
(`backend-storage-aio-read-stream`) sits far above the `types-tableam` layer.
The closure-across-layers crossing is the same technique the already-landed
`index_build_range_scan` callback uses; `acquire_sample_rows` (the owner, which
DOES depend on read-stream) builds the closure over its stream. No invented
handle (opacity-inherited-never-introduced).

The `liverows`/`deadrows` out-counts are `&mut f64` (C `double *`), the
`OldestXmin` is `TransactionId`, the HeapTuple-out is the slot store. Faithful.

## Heap provider tuple-classification logic (analyze_scan.rs)

`heapam_scan_analyze_next_block`: pull next pinned buffer via the closure; if
`!BufferIsValid` return false; else `LockBuffer(SHARE)`,
`rs_cblock = BufferGetBlockNumber`, `rs_cindex = FirstOffsetNumber`, return true.
Matches C line-for-line. Buffer comes pinned from the stream (the closure).

`heapam_scan_analyze_next_tuple`: `Assert(TTS_IS_BUFFERTUPLE)`; read
`maxoffset = PageGetMaxOffsetNumber`; inner loop `rs_cindex <= maxoffset`:
- non-normal line pointer: if `ItemIdIsDead` then `*deadrows += 1`, skip
  (advance rs_cindex) — agrees with heap_page_prune_and_freeze counting.
- normal: build the HeapTuple (`ItemPointerSet` t_self = (cblock, cindex),
  t_tableOid = relid, t_data = PageGetItem, t_len = ItemIdGetLength), run
  `HeapTupleSatisfiesVacuum(OldestXmin)`, classify:
  - LIVE → sample, `*liverows += 1`.
  - DEAD / RECENTLY_DEAD → `*deadrows += 1`, no sample.
  - INSERT_IN_PROGRESS → not counted UNLESS xmin is our own current xact
    (`TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin)`), then sample
    + `*liverows += 1`.
  - DELETE_IN_PROGRESS → if updater is our own current xact
    (`HeapTupleHeaderGetUpdateXid`) count `*deadrows += 1` no sample; else sample
    + `*liverows += 1`.
  On sample: `ExecStoreBufferHeapTuple(targtuple, slot, cbuf)`, advance rs_cindex,
  return true LEAVING THE BUFFER LOCKED.
- end of block: `UnlockReleaseBuffer(rs_cbuf)`, `rs_cbuf = InvalidBuffer`,
  `ExecClearTuple(slot)`, return false.

Verified branch-for-branch against the C `switch`. The C `default:`
`elog(ERROR, "unexpected ...")` arm is unreachable because the Rust
`HTSV_Result` enum is exhaustively matched (the four classified results); no dead
arm kept.

`HeapTupleSatisfiesVacuum` is called sharelocked (the buffer is held since
`scan_analyze_next_block`), matching C.

## Dispatch + seams

`backend-access-table-tableam`: `table_beginscan_analyze` dispatches
`scan_begin(rel, None, 0, [], None, SO_TYPE_ANALYZE)`; the two next_block/tuple
dispatchers resolve the vtable via `am(&scan.rs_rd)` and forward. Installed from
`init_seams`; the seams-init recurrence guard (`every_declared_seam_is_installed
_by_its_owner`) passes, confirming wiring into `init_all`.

## Stubs / divergences

None. No todo!/unimplemented!. The `next_buffer` closure modeling is a faithful
layering resolution of the C `ReadStream *`, documented in DESIGN_DEBT (K1 DONE).

## Gate

- `cargo check --workspace` — clean.
- `cargo test -p no-todo-guard` — pass (no new todos).
- `cargo test -p seams-init` — both recurrence guards pass.
- CONTRACT_RECONCILE_PENDING count unchanged (53) — capability add, no ledger churn.
