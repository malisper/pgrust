# Audit: backend-rmgrdesc-next

Unit: `backend-rmgrdesc-next` — the nine rmgr descriptor files
`src/backend/access/rmgrdesc/{brindesc,gindesc,gistdesc,hashdesc,heapdesc,mxactdesc,nbtdesc,spgdesc,standbydesc}.c`
ported as `crates/backend-rmgrdesc-next` (one module per C file), with the
consumed-but-unported `rmgrdesc_utils.c` helpers declared in
`crates/backend-rmgrdesc-small-seams`.

Audited independently against the C sources
(`pgrust/postgres-18.3/src/backend/access/rmgrdesc/*.c`), the headers
(`brin_xlog.h`, `ginxlog.h`, `ginblock.h`, `gistxlog.h`, `hash_xlog.h`,
`heapam_xlog.h`, `multixact.h`, `nbtxlog.h`, `spgxlog.h`, `standbydefs.h`,
`lockdefs.h`, `sinval.h`, `xlogrecord.h`, `rmgrdesc_utils.h`), and the c2rust
rendering (`pgrust/c2rust-runs/backend-rmgrdesc-next/src/*.rs`).

## Inventory cross-check

The c2rust rendering contains exactly the 33 functions listed below plus the
header-inline helpers `BlockIdGetBlockNumber`, `ItemPointerGetBlockNumber`,
`ItemPointerGetBlockNumberNoCheck`, `ItemPointerGetOffsetNumber(NoCheck)`
(duplicated per file). The inline helpers are realized in the port as
`lib.rs::block_id_at` and direct `u16_at` reads at the verified struct
offsets; their `Assert`-only validity checks have no output effect. No
function in the C files or the c2rust rendering is absent from the port.

## Function table

| # | C function (file:line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `brin_desc` (brindesc.c:20) | `brindesc.rs::brin_desc` | MATCH | All six opcodes; `xl_brin_*` offsets verified against brin_xlog.h (createidx pagesPerRange@0/version@4; insert 0/4/8; update oldOffnum@0 + insert@4; desummarize 0/4/8). OPMASK 0x70 applied as in C. |
| 2 | `brin_identify` (brindesc.c:73) | `brindesc.rs::brin_identify` | MATCH | All 8 cases incl. `|INIT_PAGE` combos; constants 0x00–0x50/0x80 verified. |
| 3 | `desc_recompress_leaf` (gindesc.c:21) | `gindesc.rs::desc_recompress_leaf` | MATCH | walbuf cursor walk identical; `SizeOfGinPostingList` = 8 + SHORTALIGN(nbytes) (nbytes@6) and outer SHORTALIGN verified against c2rust; ADDITEMS consumes 2 + 6*nitems; unknown action prints and returns early. |
| 4 | `gin_desc` (gindesc.c:72) | `gindesc.rs::gin_desc` | MATCH | ginxlogInsert flags@0; children BlockIdData[2] at rec+2; FPI/apply branches; ginxlogInsertEntry.isDelete@2; ginxlogInsertDataInternal newitem (child_blkno@2, key blkid@6, posid@10); ginxlogSplit.flags@24 (locator 12 + rrlink 4 + 2×child 4); ginxlogDeleteListPages.ndeleted@56 (sizeof(GinMetaPageData)=56, layout re-derived from ginblock.h and confirmed in c2rust). |
| 5 | `gin_identify` (gindesc.c:180) | `gindesc.rs::gin_identify` | MATCH | 9 opcodes 0x10–0x90 verified. |
| 6 | `out_gistxlogPageUpdate` (gistdesc.c:21) | `gistdesc.rs::out_gistxlog_page_update` | MATCH | Intentionally empty in C; empty in port. |
| 7 | `out_gistxlogPageReuse` (gistdesc.c:26) | `gistdesc.rs::out_gistxlog_page_reuse` | MATCH | locator 0/4/8, block@12, FullTransactionId@16 (8-aligned), isCatalogRel@24; epoch/xid split = hi/lo 32 bits. |
| 8 | `out_gistxlogDelete` (gistdesc.c:37) | `gistdesc.rs::out_gistxlog_delete` | MATCH | snapshotConflictHorizon@0, ntodelete@4, isCatalogRel@6. |
| 9 | `out_gistxlogPageSplit` (gistdesc.c:45) | `gistdesc.rs::out_gistxlog_page_split` | MATCH | npage@18 (origrlink@0, orignsn u64@8, origleaf@16). |
| 10 | `out_gistxlogPageDelete` (gistdesc.c:52) | `gistdesc.rs::out_gistxlog_page_delete` | MATCH | deleteXid u64@0, downlinkOffset@8. |
| 11 | `gist_desc` (gistdesc.c:61) | `gistdesc.rs::gist_desc` | MATCH | 6 opcodes dispatched identically; ASSIGN_LSN no-op. |
| 12 | `gist_identify` (gistdesc.c:90) | `gistdesc.rs::gist_identify` | MATCH | 0x00–0x70 (0x40/0x50 retired) verified. |
| 13 | `hash_desc` (hashdesc.c:20) | `hashdesc.rs::hash_desc` | MATCH | 11 formatted opcodes; all xl_hash_* offsets verified (init_meta num_tuples f64@0/ffactor@12; vacuum_one_page horizon@0/ntuples@4/isCatalogRel@6; split_allocate new_bucket@0/flags@8; squeeze 0/4/8/10; etc). `%g` rendered by `GFmt` (6 sig digits, sci for exp <-4 or >=6, zero-trim) — matches printf `%g` on the reachable values. |
| 14 | `hash_identify` (hashdesc.c:126) | `hashdesc.rs::hash_identify` | MATCH | 13 opcodes 0x00–0xC0 verified. |
| 15 | `infobits_desc` (heapdesc.c:26) | `heapdesc.rs::infobits_desc` | MATCH | Same 5 XLHL_* bits (0x01–0x10 verified), same ", " truncation; C `Assert`s mapped to `debug_assert!`. |
| 16 | `truncate_flags_desc` (heapdesc.c:55) | `heapdesc.rs::truncate_flags_desc` | MATCH | CASCADE/RESTART_SEQS bits 1<<0 / 1<<1 verified; same truncation. |
| 17 | `plan_elem_desc` (heapdesc.c:76) | inlined closure in `heapdesc.rs::heap2_desc` | MATCH | Prints xmax@0, t_infomask@6, t_infomask2@4, ntuples@10 in C's order (infomask before infomask2); consumes ntuples offsets from the shared frz_offsets cursor exactly as the C `*offsets += ntuples`. xlhp_freeze_plan layout (12 bytes) verified. |
| 18 | `heap_xlog_deserialize_prune_and_freeze` (heapdesc.c:104) | `heapdesc.rs::heap_xlog_deserialize_prune_and_freeze` | MATCH | Same four flag-gated sub-records in the same order; offsetof(xlhp_freeze_plans, plans)=4 and offsetof(xlhp_prune_items, data)=2 confirmed in c2rust; redirections advance 2*2*n. Out-params returned as `PruneFreezeSubRecords` (empty slice where C sets NULL + 0). Exported for heap2_redo/pg_waldump as in C. |
| 19 | `heap_desc` (heapdesc.c:184) | `heapdesc.rs::heap_desc` | MATCH | OPMASK applied; INSERT/DELETE/UPDATE/HOT_UPDATE/TRUNCATE/CONFIRM/LOCK/INPLACE all present; xl_heap_* offsets verified (update new_xmax@8/new_offnum@12; truncate relids@12; inplace nmsgs@16/msgs@20). TRUNCATE relids via `array_desc`/`oid_elem_desc` seams; INPLACE calls in-crate `standby_desc_invalidations`. |
| 20 | `heap2_desc` (heapdesc.c:264) | `heapdesc.rs::heap2_desc` | MATCH | PRUNE_* trio: conflict_xid memcpy at SizeOfHeapPrune=2; unconditional ", isCatalogRel"; block-0 deserialize + 4 conditional arrays in C's order. VISIBLE/MULTI_INSERT (isinit from raw info & 0x80)/LOCK_UPDATED/NEW_CID (locator@16, tid blkid@28/posid@32) all verified. |
| 21 | `heap_identify` (heapdesc.c:389) | `heapdesc.rs::heap_identify` | MATCH | 11 cases incl. the three `|INIT_PAGE` combos. |
| 22 | `heap2_identify` (heapdesc.c:434) | `heapdesc.rs::heap2_identify` | MATCH | 9 cases incl. MULTI_INSERT+INIT and REWRITE. |
| 23 | `out_member` (mxactdesc.c:20) | `mxactdesc.rs::out_member` | MATCH | 6 MultiXactStatus values 0x00–0x05 verified; default "(unk) ". |
| 24 | `multixact_desc` (mxactdesc.c:50) | `mxactdesc.rs::multixact_desc` | MATCH | ZERO_*: int64 pageno via unaligned read; CREATE_ID: mid@0, moff@4, nmembers@8, members@12 stride 8; negative nmembers loops zero times in both. TRUNCATE_ID skips oldestMultiDB@0 as C does. |
| 25 | `multixact_identify` (mxactdesc.c:84) | `mxactdesc.rs::multixact_identify` | MATCH | 4 opcodes verified. |
| 26 | `delvacuum_desc` (nbtdesc.c:196) | `nbtdesc.rs::delvacuum_desc` | MATCH | deleted offsets via `array_desc`+`offset_elem_desc` seams; manual updated-objects loop with SizeOfBtreeUpdate=2 stride and per-update ptid walk; `p < ndeletedtids - 1` / `i < nupdated - 1` separators reproduced (int-promotion edge at 0 behaves identically). |
| 27 | `btree_desc` (nbtdesc.c:24) | `nbtdesc.rs::btree_desc` | MATCH | All 15 opcodes; xl_btree_* offsets verified (split 0/4/6/8; delete 0/4/6/8; halfdead topparent@16; unlink_page safexid@16 + leaf trio @24/28/32; newroot level@4; reuse_page horizon@16/isCatalogRel@24; metadata last_cleanup_num_delpages@20 from block 0 data). |
| 28 | `btree_identify` (nbtdesc.c:139) | `nbtdesc.rs::btree_identify` | MATCH | 15 opcodes 0x00–0xE0 verified. |
| 29 | `spg_desc` (spgdesc.c:20) | `spgdesc.rs::spg_desc` | MATCH | 8 opcodes; spgxlog* offsets re-derived (AddLeaf 0/1/2/4/6/8; MoveLeafs 0/2/3/4/6/8; AddNode parentBlk int8@5 printed signed; SplitTuple 0/2/4/5; PickSplit 0/2/4/8/11/12/14/16; VacuumRedirect 0/2/4/8). |
| 30 | `spg_identify` (spgdesc.c:132) | `spgdesc.rs::spg_identify` | MATCH | 8 opcodes 0x10–0x80 verified (0x00 retired). |
| 31 | `standby_desc_running_xacts` (standbydesc.c:19) | `standbydesc.rs::standby_desc_running_xacts` | MATCH | xl_running_xacts xcnt@0/subxcnt@4/overflow@8/nextXid@12/oldestRunningXid@16/latestCompletedXid@20/xids@24; prints latestCompleted before oldestRunning as C does; subxact ids indexed at xcnt+i. |
| 32 | `standby_desc` (standbydesc.c:46) | `standbydesc.rs::standby_desc` | MATCH | LOCK: xl_standby_lock {xid,dbOid,relOid} stride 12 from @4; RUNNING_XACTS delegate; INVALIDATIONS: dbId@0/tsId@4/inval@8/nmsgs@12/msgs@16. |
| 33 | `standby_identify` (standbydesc.c:78) | `standbydesc.rs::standby_identify` | MATCH | 3 opcodes 0x00/0x10/0x20 verified. |
| 34 | `standby_desc_invalidations` (standbydesc.c:100) | `standbydesc.rs::standby_desc_invalidations` | MATCH | nmsgs<=0 early return; relcache-init-file line; per-message dispatch on int8 id with SHAREDINVAL*_ID -1..-6 verified against sinval.h; payload offsets cat.catId@8, rc.relId@8, rm.dbId@4, sn.relId@8, rs.relid@8; sizeof(SharedInvalidationMessage)=16 (SmgrMsg). Exported for xactdesc/heap-inplace consumers as in C. |

`XLR_INFO_MASK` = 0x0F verified against xlogrecord.h (lives in `types-wal`).

Convention note: record fields are read at verified C struct offsets from the
raw record bytes; a record physically shorter than the struct the C code
casts it to panics in the port where the C reads out of bounds. This is the
crate's documented contract and was checked at each read site.

## Seam audit

`crates/backend-rmgrdesc-small-seams` declares exactly four seams —
`array_desc`, `offset_elem_desc`, `redirect_elem_desc`, `oid_elem_desc` —
mirroring `rmgrdesc_utils.h`. Findings: none.

- Justification: the four functions are defined in `rmgrdesc_utils.c`, which
  belongs to catalog unit `backend-rmgrdesc-small` (status `todo`). A direct
  cargo dependency on the owner crate fails because the crate does not exist;
  porting the bodies here would move another unit's logic into this crate.
- Every call site (`heapdesc.rs` TRUNCATE/PRUNE arrays, `nbtdesc.rs`
  delvacuum) is thin marshal + delegate: slice the element bytes, one
  `::call`, no node construction or branching in the seam path. The C
  `void *data` cursor argument is folded into the `elem_desc` closure
  capture; the logic of `plan_elem_desc` (a heapdesc.c static) correctly
  stays in this crate.
- No `set()` call exists anywhere for these seams (verified by grep); the
  owner installs them when `backend-rmgrdesc-small` lands. Until then a call
  panics loudly with the seam path — the documented unported-callee
  behavior. Neither crate owns installable seams, so neither has an
  `init_seams()` and `seams-init` correctly has no entry (same convention as
  `backend-access-transam-twophase-rmgr`).
- No function body in this unit was replaced by a seam: all 34 rows above are
  MATCH with their logic in-crate.

## Build and tests

`cargo build -p backend-rmgrdesc-next -p backend-rmgrdesc-small-seams`: clean.
`cargo test -p backend-rmgrdesc-next`: 29/29 pass.

## Verdict

**PASS** — every function MATCH, zero seam findings.
