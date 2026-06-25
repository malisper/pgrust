# Audit: backend-access-transam-generic-xlog

C source: `src/backend/access/transam/generic_xlog.c` (544 lines, PG 18.3).
Port: `crates/backend-access-transam-generic-xlog/src/lib.rs`.
c2rust: `../pgrust/c2rust-runs/backend-access-transam-generic-xlog/src/generic_xlog.rs`.

Independent re-derivation from the C; the port's comments/self-review were not
trusted.

## Function inventory and verdicts

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `writeFragment` (89) | `write_fragment` | MATCH | Native-endian `OffsetNumber` (u16) offset+length headers then `length` data bytes; `deltaLen` advanced by header+data. C `Assert` → `debug_assert!`. `delta`/`delta_len` split out of the slot so the page image can be read immutably while the delta is appended — behavior identical. |
| `computeRegionDelta` (120) | `compute_region_delta` | MATCH | Verified branch-by-branch: invalid-start prefix folded into first fragment; `loopEnd = min(targetEnd, validEnd)`; unmatched-run inner loop then matched-run inner loop; `i - fragmentEnd > MATCH_THRESHOLD` flush; trailing invalid-end region; final-fragment write. `fragmentEnd`/`fragmentBegin` as `i32` (C `int`); `as usize` cast on the threshold compare is sound because `fragmentEnd` is a valid `i` when `fragmentBegin >= 0`. |
| `computeDelta` (227) | `compute_delta` | MATCH | Reads `pd_lower`/`pd_upper` from target and cur; `deltaLen = 0`; two `computeRegionDelta` calls — lower part `(0,targetLower,0,curLower)` and upper part `(targetUpper,BLCKSZ,curUpper,BLCKSZ)`. `#ifdef WAL_DEBUG` verify block is compiled out in the default build (correctly omitted). |
| `GenericXLogStart` (268) | `GenericXLogStart` | MATCH (SEAMED: `RelationNeedsWAL`) | `palloc_aligned` → fallible mcx `PgVec` allocations (`vec_with_capacity_in` + per-page image/delta), returns `PgResult`. `isLogged = RelationNeedsWAL(relation)` via `relation_needs_wal` seam (relcache owns `rd_createSubid`/`wal_level`). Each page `buffer = InvalidBuffer`, image owned (C `image = images[i].data`). |
| `GenericXLogRegisterBuffer` (298) | `GenericXLogRegisterBuffer` | MATCH (SEAMED: `BufferGetPage`) | Linear scan of `MAX_GENERIC_XLOG_PAGES` slots: first invalid slot → copy `BufferGetPage(buffer)` into the image (via `with_buffer_page` seam) and record buffer+flags; existing match → return its block_id keeping original flags; overflow → `elog(ERROR, "maximum number %d ...")` → `Err(PgError::error(...))` with the same message. Returns `block_id` (C returns the image pointer; the owned model exposes `page_image_mut(block_id)`). |
| `GenericXLogFinish` (336) | `GenericXLogFinish` | MATCH (SEAMED) | Logged branch: `XLogBeginInsert`; `START_CRIT_SECTION` (RAII `CritSection`); per in-use page compute delta unless `GENERIC_XLOG_FULL_IMAGE`, then `memcpy(page,image,pd_lower)` / `memset` hole / `memcpy` upper part (pd_lower/pd_upper read from the **image** as in C), `MarkBufferDirty`, register buffer with `REGBUF_FORCE_IMAGE|REGBUF_STANDARD` (full image) or `REGBUF_STANDARD` + `XLogRegisterBufData(delta)`; `XLogInsert(RM_GENERIC_ID,0)`; `PageSetLSN` per page; `END_CRIT_SECTION`. Unlogged branch: crit section, full-image `memcpy` (hole not zeroed), `MarkBufferDirty`, `lsn = InvalidXLogRecPtr`. `pfree(state)` = drop by value. |
| `GenericXLogAbort` (443) | `GenericXLogAbort` | MATCH | `pfree(state)` = consume-by-value drop; no buffer changes. |
| `applyPageRedo` (452) | `apply_page_redo` | MATCH | Walk delta: read offset(u16-ne), length(u16-ne), `memcpy(page+offset, ptr, length)`, advance. `deltaSize` is the slice length. |
| `generic_redo` (477) | `generic_redo` | MATCH (SEAMED: `XLogReadBufferForRedo`) | `lsn = record->EndRecPtr`; `Assert(XLogRecMaxBlockId < MAX_GENERIC_XLOG_PAGES)` → `debug_assert!`; loop `0..=maxBlockId`: no block ref → InvalidBuffer/continue; else `XLogReadBufferForRedo` (seam) → buffer; on `BLK_NEEDS_REDO` apply `XLogRecGetBlockData` delta (read off the real `DecodedXLogRecord`), zero the hole from the post-apply pd_lower/pd_upper, `PageSetLSN`, `MarkBufferDirty`. Final loop unlock+release valid buffers. `XLogRecMaxBlockId`/`XLogRecHasBlockRef`/`XLogRecGetBlockData` resolved on the in-crate decoded-record type (no seam needed). |
| `generic_mask` (538) | `generic_mask` | MATCH (SEAMED: bufmask) | `mask_page_lsn_and_checksum(page)` then `mask_unused_space(page)` (which `elog(ERROR)`s on invalid bounds → `Err`). `blkno` unused, as in C. |

## Constants verified against C headers

- `MAX_GENERIC_XLOG_PAGES = XLR_NORMAL_MAX_BLOCK_ID = 4` (generic_xlog.h / xloginsert.h). ✓
- `GENERIC_XLOG_FULL_IMAGE = 0x0001` (generic_xlog.h). ✓
- `FRAGMENT_HEADER_SIZE = 2*sizeof(OffsetNumber) = 4`, `MATCH_THRESHOLD = 4`,
  `MAX_DELTA_SIZE = BLCKSZ + 8 = 8200` (generic_xlog.c). ✓
- `REGBUF_FORCE_IMAGE = 0x01`, `REGBUF_STANDARD = 0x08` (xloginsert.h). ✓
- `RM_GENERIC_ID = 20` (rmgrlist.h entry index, 0-based). ✓
- `XLogRedoAction { BLK_NEEDS_REDO=0, BLK_DONE=1, BLK_RESTORED=2, BLK_NOTFOUND=3 }`
  (xlogutils.h). ✓
- `InvalidBuffer = 0` (buf.h); `InvalidXLogRecPtr = 0` (xlogdefs.h). ✓
- PageHeaderData offsets: `pd_lsn@0` (8), `pd_checksum@8` (2), `pd_flags@10` (2),
  `pd_lower@12` (2), `pd_upper@14` (2) (bufpage.h). The port's `PD_LSN_OFFSET=0`,
  `PD_LOWER_OFFSET=12`, `PD_UPPER_OFFSET=14` match. ✓

## Seam audit

Outward seams (each owner is unported; a direct dep would be premature/cyclic,
so each goes through the owner's `-seams` crate, panicking until the owner
lands — the sanctioned mirror-and-panic):

- `relation_needs_wal` — `backend-utils-cache-relcache-seams` (new). `RelationNeedsWAL`
  reads `rd_createSubid`/`rd_firstRelfilelocatorSubid` (not in trimmed
  `RelationData`) and the `wal_level` GUC, all relcache-owned. Thin getter,
  infallible. Justified.
- `with_buffer_page` / `mark_buffer_dirty` / `unlock_release_buffer` —
  `backend-storage-buffer-bufmgr-seams` (new). `with_buffer_page` is the
  callback shape required by AGENTS.md (no `&'static mut`): the owner holds the
  pin/lock across the callback so the in-crate logic reads + writes the live
  page. Marshal + delegate only. Justified.
- `xlog_begin_insert` / `xlog_register_buffer` / `xlog_register_buf_data` /
  `xlog_insert_record` — `backend-access-transam-xloginsert-seams` (extended).
  Granular, mirroring the C `XLogBeginInsert`/`XLogRegisterBuffer`/
  `XLogRegisterBufData`/`XLogInsert` 1:1 (consistent with the crate's existing
  granular `xlog_register_data`). The rdata chain stays on the owner's side.
- `xlog_read_buffer_for_redo` — `backend-access-transam-xlogutils-seams`
  (extended). Reads the buffer manager + record block info during redo; owned by
  xlogutils. Takes `&XLogReaderState`; returns `(XLogRedoAction, Buffer)`.
- `mask_page_lsn_and_checksum` / `mask_unused_space` —
  `backend-access-common-bufmask-seams` (new crate). `mask_unused_space`
  returns `PgResult` (the C `elog(ERROR)` on invalid bounds).

In-crate (NOT seamed, correctly): `XLogRecMaxBlockId`/`XLogRecHasBlockRef`/
`XLogRecGetBlockData` (read off the real `DecodedXLogRecord` accessors),
`PageSetLSN` and `pd_lower`/`pd_upper` reads (fixed PageHeaderData byte offsets),
`START`/`END_CRIT_SECTION` (RAII guard over `backend-utils-error::config`
`crit_section_count`). All delta compute/apply is in-crate.

Inward seams: `crates/backend-access-transam-generic-xlog-seams` declares the
two rmgr-table callbacks this unit owns (`generic_redo`, `generic_mask`). Both
are installed by `init_seams()` (`set()` calls only), and
`seams-init::init_all()` calls `backend_access_transam_generic_xlog::init_seams()`.
Verified present.

## Design conformance

- Allocating functions (`GenericXLogStart`, `GenericXLogRegisterBuffer`,
  `GenericXLogFinish`) take/use `Mcx` and return `PgResult`; OOM flows through
  the fallible mcx APIs. ✓
- No invented opacity: `Buffer` is the real `i32` (buf.h), `XLogRedoAction` a
  real enum, `RelationData` the real trimmed type — no `usize`/`&[u8]`
  stand-ins for typed pointers. Page bytes are genuine `char *`/`&[u8]`. ✓
- No shared statics / atomics; crit-section count is backend-private state
  reached through the existing elog config (already thread_local-backed). ✓
- No ambient-global getter seams (relation passed by reference; `is_logged`
  computed via the relcache seam, not a global). ✓
- No lock/pin held across `?` without a guard: the buffer pin is held by the
  owner inside `with_buffer_page`'s callback; `CritSection` is a `Drop` guard so
  an early `Err` return still runs `END_CRIT_SECTION`. ✓
- No registry side tables, no unledgered divergence markers. ✓

## Verdict: PASS

All 10 C functions MATCH (six with justified SEAMED outward calls). Constants
verified against headers. Seams are thin and installed; `init_seams()` is
`set()`-only and wired into `seams-init`. No design-conformance findings.
