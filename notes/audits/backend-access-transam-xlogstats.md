# Audit: backend-access-transam-xlogstats

- **Unit:** `backend-access-transam-xlogstats`
- **C sources:** `src/backend/access/transam/xlogstats.c` (98 lines, PostgreSQL 18.3);
  declarations in `src/include/access/xlogstats.h`; macros in
  `src/include/access/xlogreader.h`; constants in `src/include/access/rmgr.h`
  and `src/include/access/rmgrlist.h`.
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-access-transam-xlogstats/src/xlogstats.rs`
- **Port:** `crates/backend-access-transam-xlogstats/src/lib.rs`, with the WAL
  record vocabulary in `crates/types-wal/src/wal.rs`.
- **Auditor:** independent re-derivation from the C sources and headers.

## Function inventory

`xlogstats.c` defines exactly two functions; the c2rust rendering confirms the
same two (`XLogRecGetLen`, `XLogRecStoreStats`) and nothing else survived the
preprocessor. No statics, no inline helpers in the .c file.

| # | C function (xlogstats.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `XLogRecGetLen` (:21) | `lib.rs::xlog_rec_get_len` | MATCH | C loops `block_id = 0..=XLogRecMaxBlockId` and adds `bimg_len` when `XLogRecHasBlockRef && XLogRecHasBlockImage`. Per xlogreader.h:418-424, `HasBlockRef` is `max_block_id >= block_id && blocks[block_id].in_use` and `HasBlockImage` is `blocks[block_id].has_image`; the `max_block_id >= block_id` half is vacuous inside the loop (loop bound is `<= max_block_id`), which c2rust's rendering makes explicit. The port iterates the `DecodedXLogRecord::blocks()` slice (documented contract: holds entries `0..=max_block_id`, in-use or not) and folds `DecodedBkpBlock::fpi_len()`, which returns `bimg_len as u32` iff `in_use && has_image`, else 0 — the identical predicate per block. Accumulation uses `wrapping_add` (C unsigned `+=`). `rec_len = xl_tot_len - fpi_len` via `wrapping_sub`, matching C's `XLogRecGetTotalLen(record) - *fpi_len` on `uint32`. Out-param pair returned as a tuple — behaviorally identical. The `XLogReaderState*` → `&DecodedXLogRecord` signature change is faithful: every C read goes through `(decoder)->record->...`, exactly the data the port receives. |
| 2 | `XLogRecStoreStats` (:52) | `lib.rs::xlog_rec_store_stats` | MATCH | `Assert(stats != NULL && record != NULL)` is structurally guaranteed by `&mut`/`&` references. `stats->count++` → `wrapping_add(1)`. `rmid = XLogRecGetRmid` → `header().rmid()` (`xl_rmid`). Calls `xlog_rec_get_len`, then folds count/rec_len/fpi_len into `rmgr_stats[rmid]` via `XLogRecStats::add_record` — count `+1`, `rec_len`/`fpi_len` widened `u32 → u64` then `wrapping_add`, identical to C's three `+=` on `uint64`. `recid = XLogRecGetInfo >> 4` (u8 shift; C does the shift on the int-promoted value and stores into `uint8` — identical result, top 4 bits). `if (rmid == RM_XACT_ID) recid &= 0x07` reproduced with `RM_XACT_ID == 1` (verified: rmgrlist.h entry order XLOG=0, Transaction=1; c2rust agrees). Final fold into `record_stats[rmid][recid]` via the same `add_record`. Index safety: `rmid: u8 ≤ 255 = RM_MAX_ID`, `recid = info >> 4 ≤ 15 < 16 = MAX_XLINFO_TYPES`, so the direct indexing can never panic where C would have been in bounds. |

## Types audit (`types-wal`, against the headers)

- `MAX_XLINFO_TYPES = 16` — verified xlogstats.h:19. Correct.
- `RM_MAX_ID = u8::MAX (255)` — verified rmgr.h:33 (`#define RM_MAX_ID UINT8_MAX`),
  not `RM_MAX_BUILTIN_ID`; tables sized `RM_MAX_ID + 1 = 256` rows ×
  `MAX_XLINFO_TYPES = 16` columns, matching
  `rmgr_stats[RM_MAX_ID + 1]` / `record_stats[RM_MAX_ID + 1][MAX_XLINFO_TYPES]`
  in xlogstats.h. Custom rmgr ids 128..=255 index in bounds. Correct.
- `RM_XACT_ID = 1` — verified against rmgrlist.h ordering and the c2rust
  constant (`RM_XACT_ID: RmgrIds = 1`). Correct.
- `XLogRecord` field set and widths (`xl_tot_len: u32`, `xl_xid: u32`,
  `xl_prev: u64`, `xl_info: u8`, `xl_rmid: u8`, `xl_crc: u32`) match
  xlogrecord.h / the c2rust struct. Accessors `total_len`/`info`/`rmid` are the
  exact macro bodies (`XLogRecGetTotalLen`/`XLogRecGetInfo`/`XLogRecGetRmid`).
- `DecodedBkpBlock` trimmed to `in_use`/`has_image`/`bimg_len: u16` — the only
  fields this unit reads; widths match xlogreader.h. The trim is documented and
  owned by the future xlogreader port.
- `XLogStats.startptr`/`endptr` are `#ifdef FRONTEND` upstream; the port keeps
  them unconditionally as documented owned fields with accessors. The two
  ported functions never touch them, so backend behavior is unaffected; the
  frontend consumer (`bin-pg-waldump-xlogstats-batch1` is `duplicate-of` this
  unit) needs them. Acceptable.

## Seam audit

- `crates/backend-access-transam-xlogstats` declares no seams and makes no
  outward seam calls — consistent with the C unit, whose only external
  references are header macros/structs (now in `types-wal`) and which calls no
  other translation unit.
- `init_seams()` is an empty no-op (nothing to install) and is invoked by
  `crates/seams-init/src/lib.rs::init_all()` (line 10). Wiring correct; no
  `set()` calls exist anywhere for this crate.
- No `*-seams` crate exists for this unit, as expected for a leaf.

## Build and tests

- `cargo test -p backend-access-transam-xlogstats -p types-wal`: 8 tests pass
  (length split, XACT masking, wrapping accumulation, table bounds).
- `cargo build --workspace`: clean.

## Verdict

**PASS** — both functions MATCH, constants verified against the headers, no
seam findings. Fix rounds required: 0.
