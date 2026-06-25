# Audit: backend-access-nbtree-nbtsort

Independent logic audit of the Rust port of `src/backend/access/nbtree/nbtsort.c`
(PostgreSQL 18.3, B-tree index build) — re-derived from C + c2rust, not from
port comments or the green build.

Sources:
- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/access/nbtree/nbtsort.c`
- c2rust: `/Users/malisper/workspace/work/pgrust/c2rust-runs/backend-access-nbtree-nbtsort/src/nbtsort.rs`
- Port: `/Users/malisper/workspace/work/pgrust-fabled/.claude/worktrees/agent-aeebd1af93738e95b/crates/backend-access-nbtree-nbtsort/src/{lib.rs,deferred.rs}`

Policy: sort+leaf-load half is GROUNDED (must be full logic); closure-based
build-scan + parallel half is honestly panic-DEFERRED (DEFERRED-OK iff genuinely
blocked on parallel/DSM/tableam/closure infra, not this-crate own logic).

## Constant verification (vs `src/include/access/nbtree.h`)
- BTREE_DEFAULT_FILLFACTOR=90, BTREE_NONLEAF_FILLFACTOR=70 — OK (used as `90`/`70`).
- BTREE_METAPAGE=0, BTREE_MAGIC=0x053162, BTREE_VERSION=4 — OK (via types_nbtree).
- P_HIKEY=1, P_FIRSTKEY=2 — OK.
- BT_OFFSET_MASK=0x0FFF, BT_PIVOT_HEAP_TID_ATTR=0x1000, BT_IS_POSTING=0x2000 — OK.
- `btps_full`: leaf=`BLCKSZ*(100-fillfactor)/100`, nonleaf=`BLCKSZ*(100-70)/100` — OK.
- dedup `maxpostingsize = MAXALIGN_DOWN(BLCKSZ*10/100) - sizeof(ItemIdData)` — OK.

## Per-function table

| C function | Kind | Verdict | Notes |
|---|---|---|---|
| `btbuild` | extern | DEFERRED-OK | drives `table_index_build_scan` w/ per-tuple callback closure + `_bt_begin_parallel`; both cross-subsystem. `deferred::btbuild` loud panic. |
| `_bt_spools_heapscan` | static | DEFERRED-OK | closure build-scan + parallel orchestration. Panic. |
| `_bt_spool` | static | DEFERRED-OK | push side (`tuplesort_putindextuplevalues`) fed only by deferred scan; grounded pull side is `_bt_load`. `_bt_spool()` routes to `deferred::_bt_spool`. |
| `_bt_spooldestroy` | static | MATCH/SEAMED | `tuplesort_end` seam over boxed sortstate + drop of spool (= pfree). Faithful. |
| `_bt_leafbuild` | static | MATCH/SEAMED | performsort_1/_2 progress + seam, `_bt_mkscankey` seam, allequalimage set via inskey, metapage reserve `BTREE_METAPAGE+1`, `_bt_load`. Faithful. |
| `_bt_build_callback` | static | DEFERRED-OK | the scan-callback boundary; panic. |
| `_bt_blnewpage` | static | MATCH | `_bt_pageinit`, opaque prev/next=P_NONE, level, flags `(level>0)?0:BTP_LEAF`, cycleid=0, pd_lower += sizeof(ItemIdData). 1:1. |
| `_bt_blwritepage` | static | MATCH/SEAMED | `smgr_bulk_write(...,true)` seam, ownership moved. Faithful. |
| `_bt_pagestate` | static | MATCH | blnewpage, btps_blkno=alloced++, lowkey=None, lastoff=P_HIKEY, lastextra=0, btps_full per level, next=None. 1:1. |
| `_bt_slideleft` | static | MATCH | maxoff>=P_FIRSTKEY assert; copy line ptrs P_FIRSTKEY..=maxoff back one slot; pd_lower -= sizeof(ItemIdData). 1:1 byte-faithful. |
| `_bt_sortaddtup` | static | MATCH/SEAMED | newfirstdataitem path: trunctuple, t_info=sizeof(IndexTupleData), SetNAtts(0), itemsize=sizeof. PageAddItem via seam; InvalidOffset -> error. 1:1. |
| `_bt_buildadd` | static | **DIVERGES** | See FAIL-1. All other logic (page-full test, oitup->P_FIRSTKEY copy, hikey move + pd_lower, leaf truncate via `_bt_truncate` seam + PageIndexTupleOverwrite, parent link/downlink, lowkey copy, sibling links, blwritepage, minus-inf lowkey, final sortaddtup) is faithful. |
| `_bt_sort_dedup_finish_pending` | static | MATCH/SEAMED | nitems==1 -> buildadd(base,0); else `_bt_form_posting` + truncextra=IndexTupleSize-PostingOffset + buildadd; reset counters. 1:1. |
| `_bt_uppershutdown` | static | MATCH/SEAMED | per-level loop: top -> BTP_ROOT + record root; else downlink+buildadd into parent; slideleft + blwritepage rightmost; metapage `_bt_initmetapage(root,level,allequalimage)` + write at BTREE_METAPAGE. 1:1. |
| `_bt_load` | static | MATCH/SEAMED | all three paths present: merge (unique build, per-key compare folded into `bt_load_compare_index_tuples` seam + equal-key `ItemPointerCompare` inline), dedup (start_pending / keep_natts_fast seam / save_htid / finish_pending), plain. bulk_start/finish seams. `deduplicate = allequalimage && !isunique && BTGetDeduplicateItems`. Faithful. |
| `_bt_begin_parallel` | static | DEFERRED-OK | ParallelContext/DSM/shm_toc/SpinLock/ConditionVariable/table_parallelscan. Panic. |
| `_bt_end_parallel` | static | DEFERRED-OK | WaitForParallelWorkersToFinish/InstrAccum/DestroyParallelContext. Panic. |
| `_bt_parallel_estimate_shared` | static | DEFERRED-OK | `BUFFERALIGN(sizeof(BTShared)) + table_parallelscan_estimate`; BTShared (spinlock+condvar) + tableam unported. Panic. |
| `_bt_parallel_heapscan` | static | DEFERRED-OK | SpinLock + ConditionVariable wait loop over shared state. Panic. |
| `_bt_leader_participate_as_worker` | static | DEFERRED-OK | parallel orchestration -> `_bt_parallel_scan_and_sort`. Panic. |
| `_bt_parallel_build_main` | extern | DEFERRED-OK | worker entry: shm_toc lookups, table/index_open, tuplesort_attach_shared, Instr*. Panic. |
| `_bt_parallel_scan_and_sort` | static | DEFERRED-OK | per-worker tuplesort + table_beginscan_parallel + build callback + shared accounting. Panic. |

Supporting helpers (re-ported page/byte codecs) checked: `BTPageGetOpaque`/`write_opaque`, `_bt_pageinit`, `_bt_initmetapage`/`write_meta` (field order + offsets + pd_lower past metadata), `_bt_form_posting`, `_bt_dedup_start_pending`, `_bt_dedup_save_htid`, `BTreeTuple*` accessors, `ItemPointerCompare` — all MATCH.

## Spot-checked grounded MATCH verdicts (full detail)

1. **`_bt_slideleft`** (C 685-702 / port lib.rs 852-868): `maxoff = PageGetMaxOffsetNumber`, assert `>=P_FIRSTKEY`; C copies `*previi=*thisii` walking previi from P_HIKEY while off runs P_FIRSTKEY..=maxoff. Port reads line ptr at `off` and writes to `OffsetNumberPrev(off)` for the same range — equivalent slide (previi always trails off by one). pd_lower -= sizeof(ItemIdData). Byte-identical.

2. **`_bt_sortaddtup`** (C 716-736 / port 874-902): newfirstdataitem branch: C `trunctuple=*itup; trunctuple.t_info=sizeof(IndexTupleData); BTreeTupleSetNAtts(&trunctuple,0,false); itemsize=sizeof`. Port copies first `SIZE_OF_INDEX_TUPLE_DATA(8)` bytes, sets `t_info=8`, `BTreeTupleSetNAtts(0,false)`, passes 8-byte item. Else passes `itup[..itemsize]`. PageAddItem (seam) result == InvalidOffset -> "failed to add item to the index page" error (matches C elog ERROR). MATCH.

3. **`_bt_uppershutdown`** (C 1064-1130 / port 1139-1216): loop over levels; if `btps_next==NULL` set BTP_ROOT + rootblkno/rootlevel; else `BTreeTupleSetDownLink(lowkey,blkno)` + `_bt_buildadd(parent,lowkey,0)` + free lowkey; both NAtts asserts present as debug_assert. Then `_bt_slideleft` + `_bt_blwritepage` on the rightmost buffer. After loop: `smgr_bulk_get_buf` metabuf + `_bt_initmetapage(rootblkno,rootlevel,allequalimage)` + write at BTREE_METAPAGE. P_NONE root for empty tree preserved (rootblkno initialized P_NONE). MATCH.

## FAIL findings

### FAIL-1 (DIVERGES) — `_bt_buildadd` passes wrong arg to `_bt_check_third_page`
- Port: `crates/backend-access-nbtree-nbtsort/src/lib.rs:930-941`
- C: `src/backend/access/nbtree/nbtsort.c:832-834`
- c2rust: `c2rust-runs/.../nbtsort.rs:3846`

C calls `_bt_check_third_page(wstate->index, wstate->heap, isleaf, npage, itup)`
— the third parameter is `needheaptidspace`, fed with **`isleaf`**. c2rust line
3846 confirms `isleaf`. The seam decl
(`backend-access-nbtree-build-seams/src/lib.rs:40-46`) correctly names the param
`needheaptidspace`.

The port instead passes **`heapkeyspace`** (`inskey_heapkeyspace(&wstate.inskey)`),
and its inline `// C:` comment misattributes the C source as passing
`wstate->inskey->heapkeyspace` — it does not.

Effect: `needheaptidspace` selects the stricter heap-TID-reserving size limit
(`BTMaxItemSize`) vs the larger no-heap-TID limit (`BTMaxItemSizeNoHeapTid`) and
the version number in the diagnostic. For index builds `inskey->heapkeyspace` is
always true (v4 build scankey), so on **leaf** pages the values coincide
(isleaf=true). They diverge on **internal** pages (isleaf=false): C would allow
the larger `BTMaxItemSizeNoHeapTid`/report BTREE_NOVAC_VERSION, the port forces
the stricter `BTMaxItemSize`/BTREE_VERSION. The outer `itupsz > BTMaxItemSize`
guard plus the "internal insertions cannot fail here" invariant make this
largely a diagnostic/error-message divergence in practice, but it is **not
byte-for-byte faithful** and the value passed is semantically the wrong field.
Fix: pass `isleaf` (computed at lib.rs:928) and correct the misleading comment.

## Verdict

NOT PASS. One grounded DIVERGES (FAIL-1, `_bt_buildadd` -> `_bt_check_third_page`
arg). All other grounded functions MATCH/SEAMED with full logic; all deferred
functions are DEFERRED-OK (genuinely blocked on parallel/DSM/tableam/closure
infra, no hidden portable own-logic). Legitimate seams: tuplesort_*,
smgr_bulk_*, _bt_mkscankey/_bt_allequalimage/_bt_keep_natts_fast, _bt_truncate,
_bt_check_third_page, bt_load_compare_index_tuples — all panic-until-owner.
