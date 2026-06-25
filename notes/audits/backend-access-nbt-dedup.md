# Audit: backend-access-nbt-dedup

Date: 2026-06-13
Model: Claude Opus 4.8 (1M context)
Verdict: PASS (independent re-audit)

C source: `src/backend/access/nbtree/nbtdedup.c` (PostgreSQL 18.3, 1105 lines).
Port: `crates/backend-access-nbt-dedup/src/lib.rs`.
Method: independent re-derivation from the C + c2rust rendering
(`c2rust-runs/backend-access-nbt-dedup/src/nbtdedup.rs`, completeness oracle).

## Function inventory (all 12 functions)

| C fn | C loc | Port loc | Verdict | Notes |
|------|-------|----------|---------|-------|
| `_bt_dedup_pass` | 58 | `_bt_dedup_pass` | MATCH | newitemsz += ItemIdData; maxpostingsize = Min(BTMaxItemSize/2, INDEX_SIZE_MASK); singleval gate; temp-page copy + LSN; high-key copy; per-offset loop (start/equal-merge/finalize+singleval steps); final finish; nintervals==0 early return; BTP_HAS_GARBAGE clear; crit-section page write-back + WAL. `_bt_keep_natts_fast` SEAMED; `nkeyatts` read direct off RelationData. |
| `_bt_bottomupdel_pass` | 307 | `_bt_bottomupdel_pass` | MATCH | maxpostingsize=BLCKSZ; delstate init (bottomupfreespace=Max(BLCKSZ/16,newitemsz)); per-offset loop; final finish; neverdedup; `_bt_delitems_delete_check` SEAMED; return PageGetExactFreeSpace>=Max(BLCKSZ/24,newitemsz). |
| `_bt_dedup_start_pending` | 433 | `_bt_dedup_start_pending` | MATCH | posting vs plain htids copy; basetupsize = IndexTupleSize / BTreeTupleGetPostingOffset; phystupsize = MAXALIGN(size)+ItemIdData; intervals[nintervals].baseoff set. |
| `_bt_dedup_save_htid` | 484 | `_bt_dedup_save_htid` | MATCH | mergedtupsz = MAXALIGN(basetupsize + (nhtids+n)*IPD); over-limit: `nhtids>50` bumps nmaxitems, return false; else append + phystupsize. nhtids checked BEFORE append (matches C). |
| `_bt_dedup_finish_pending` | 555 | `_bt_dedup_finish_pending` | MATCH | nitems==1: add base as-is, spacesaving=0; else form posting, set interval nitems, add, spacesaving = phystupsize-(tuplesz+ItemIdData), nintervals++. elog(ERROR) PageAddItem failures → Err. |
| `_bt_bottomupdel_finish_pending` | 647 | `_bt_bottomupdel_finish_pending` | MATCH | dupinterval = nitems>1; plain: promising=dupinterval, freespace=len+ItemIdData; posting: first/last-promising rule via min/mid/max blockno, per-TID freespace=IPD; interval bump on dupinterval. |
| `_bt_do_singleval` | 781 | `_bt_do_singleval` | MATCH | minoff & maxoff item vs newitem `_bt_keep_natts_fast > nkeyatts`. SEAMED keep_natts; nkeyatts direct. |
| `_bt_singleval_fillfactor` | 821 | `_bt_singleval_fillfactor` | MATCH | leftfree = PageSize - SizeOfPageHeaderData - MAXALIGN(sizeof BTPageOpaqueData) - newitemsz - MAXALIGN(IPD); reduction = leftfree*((100-96)/100.0); subtract or zero. C casts reduction to int then Size; value range (≤~327) makes the casts equivalent. |
| `_bt_form_posting` | 864 | `_bt_form_posting` | MATCH | keysize from posting offset / IndexTupleSize; newsize MAXALIGN; palloc0; copy keysize; t_info size; nhtids>1 → SetPosting+write posting; else clear ALT_TID + copy single TID. |
| `_bt_update_posting` | 924 | `_bt_update_posting` | MATCH | nhtids = norig-ndeletedtids; size calc matches form_posting; ui/d survivor loop identical; posting vs plain final. (Takes `&[u8]`+`&[u16]`, returns updated bytes — the vacposting->itup reassignment is the caller's; BTVacuumPosting lives in types-nbtree.) |
| `_bt_swap_posting` | 1022 | `_bt_swap_posting` | MATCH | postingoff range check → elog(ERROR) Err; CopyIndexTuple; memmove shift (nmovebytes=(nhtids-postingoff-1)*IPD) via copy_within; fill gap with newitem TID; newitem gets oposting max TID; asserts. |
| `_bt_posting_valid` | 1077 | `_bt_posting_valid` | MATCH | not-posting or <2 → false; first TID valid; ascending strict-increasing check over remaining TIDs. Ported unconditionally, used in debug_assert (matches USE_ASSERT_CHECKING). |

## Constants verified against headers

- `BTMaxItemSize` = 2704: MAXALIGN_DOWN((8192 - MAXALIGN(24+12) - MAXALIGN(16))/3) - MAXALIGN(6) = 2712-8. ✓ (nbtree.h:165)
- `BTREE_SINGLEVAL_FILLFACTOR` = 96 ✓ (nbtree.h:203)
- `INDEX_ALT_TID_MASK` = 0x2000 (= INDEX_AM_RESERVED_BIT) ✓ (itup.h:66, nbtree.h:460)
- `BT_OFFSET_MASK` = 0x0FFF, `BT_PIVOT_HEAP_TID_ATTR` = 0x1000, `BT_IS_POSTING` = 0x2000 ✓ (nbtree.h:463-467)
- `XLOG_BTREE_DEDUP` = 0x60 ✓ (nbtxlog.h:33); `SizeOfBtreeDedup` = 2 (offsetof(nintervals)+sizeof(uint16)=0+2) ✓ (nbtxlog.h:177)
- `RM_BTREE_ID` = 11 (XLOG..BTREE ordering) ✓ (rmgrlist.h)
- `REGBUF_STANDARD` = 0x08 ✓; `MaxTIDsPerBTreePage` = 1358 ✓; `MaxIndexTuplesPerPage` = 408 ✓
- `BTPageOpaqueData`/`BTDedupInterval` field order + widths ✓; `TM_Index*` field widths ✓ (tableam.h)

## Seam / wiring audit

Ownership by C-source coverage: nbtdedup.c. This unit owns **no inward seam
crate** — no sibling unit calls back into nbtdedup across a cycle (grep of all
`*-seams` confirms no `bt_dedup*`/`bt_form_posting`/`bt_swap_posting`/etc.
declaration). Therefore `init_seams()` is correctly a no-op. It is wired into
`seams-init::init_all()` (sorted insertion + dep line) for the uniform wiring
contract; the recurrence_guard wiring test does not require it (it only fires
for installers that contain `::set(`), so wiring it is conservative and cannot
regress. Both recurrence_guard tests pass.

Outward seams, all thin marshal+delegate, all into genuinely-unported owners
(panic until they land — mirror-pg-and-panic):

- `nbtcore::bt_keep_natts_fast` (nbtutils.c) — relcache+datum-image compare.
- `nbtcore::bt_delitems_delete_check` (nbtpage.c) — tableam delete + WAL.
  Owner `backend-access-nbtree-core` is `todo` (no crate dir) → exempt from the
  installed-by-owner guard.
- bufmgr: buffer_get_page / buffer_get_block_number / with_buffer_page
  (PageRestoreTempPage in-place memcpy) / mark_buffer_dirty / page_set_lsn.
- xloginsert: begin / register_buffer / register_data / register_buf_data /
  insert_record — the exact C XLogInsert sequence for XLOG_BTREE_DEDUP.
- miscinit: start/end_crit_section. relcache: relation_needs_wal.

No branching/computation in any seam path; the dedup loop, page write-back
sequence, and WAL record assembly (intervals serialize) are all in-crate.

## Design conformance

- Allocating fns take `Mcx` + return `PgResult` (form/update/swap_posting,
  new_dedup_state, both passes). ✓
- No invented opacity: tuples are `&[u8]`/`PgVec<u8>` matching C's `IndexTuple`
  pointer-into-bytes contract (inherited; same model as the already-merged
  nbtree-nbtree posting helpers). ✓
- No statics / ambient-global seams; nkeyatts is a parameter-shaped
  RelationData field read, not a getter seam. ✓
- The only `.unwrap()` is inside a `debug_assert!` mirroring a C `Assert`. No
  `todo!`/`unimplemented!`/`unreachable!`. ✓
- Crit-section `?` early-returns: in C any ERROR inside START/END_CRIT_SECTION
  is promoted to PANIC (backend exit), so the counter imbalance is moot; matches
  the repo convention (e.g. freespace.rs fsm set-and-replace). Not a finding.

## Verdict: PASS

All 12 functions MATCH; zero seam findings; constants verified against headers;
11 in-crate unit tests pass; `cargo check --workspace` clean; both
recurrence_guard tests green.
