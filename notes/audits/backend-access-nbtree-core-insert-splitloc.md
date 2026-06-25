# Audit: backend-access-nbtree-core — nbtinsert.c + nbtsplitloc.c slices

Independent logic audit (merge-blocking). Re-derived from C sources; port
comments and green build NOT trusted.

- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/access/nbtree/nbtinsert.c`, `nbtsplitloc.c`
- Port: `crates/backend-access-nbtree-core/src/insert.rs`, `splitloc.rs`

## Verdict: **FAIL**

Two FAIL findings, both in `nbtinsert.c`/`insert.rs`. `nbtsplitloc.c`/`splitloc.rs`
is clean (all MATCH).

---

## nbtinsert.c per-function table (19 functions)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `_bt_doinsert` | nbtinsert.c:101 | insert.rs:613 (`_bt_doinsert_inner`, +602 wrapper) | MATCH | NULL-bypass, scantid restore, SSI/findinsertloc/insertonpg, UNIQUE_CHECK_EXISTING all faithful. xwait branch `panic!`s on SpeculativeInsertionWait/XactLockTableWait — acceptable seam-and-panic (the C body past the probe result is pure wait+`goto search`). |
| `_bt_search_insert` | nbtinsert.c:316 | insert.rs:769 | MATCH | Fastpath faithfully disabled (`relation_get_target_block`→InvalidBlockNumber); slow `_bt_search` descent always correct. Cosmetic: the retained-for-fidelity fastpath block omits the C suitability test (dead code, behavior preserved). |
| `_bt_check_unique` | nbtinsert.c:407 | insert.rs:812 | **MISSING** | **FINDING 1** — see below. The unique-violation enforcement (xwait derivation + `ERRCODE_UNIQUE_VIOLATION` ereport) is replaced by a deferral `Err`. |
| `_bt_findinsertloc` | nbtinsert.c:814 | insert.rs:1077 | **PARTIAL** | **FINDING 2** — see below. The !heapkeyspace "get tired" random-stop disjunct is dropped. Rest matches. |
| `_bt_stepright` | nbtinsert.c:1026 | insert.rs:1254 | MATCH | INCOMPLETE_SPLIT→finish, P_IGNORE skip, fell-off-end error, buf swap + bounds_valid=false all faithful. |
| `_bt_insertonpg` | nbtinsert.c:1104 | insert.rs:1318 | MATCH | Posting-list split, page-split vs in-place, split_only_page meta update, INCOMPLETE_SPLIT clear, full WAL assembly (INSERT_LEAF/POST/UPPER/META), fastpath cache gating — all match. INDEX_CORRUPTED overlap error present. |
| `_bt_split` | nbtinsert.c:1466 | insert.rs:1638 | MATCH | Left/right assembly, suffix-truncation high-key (leaf) vs firstright (internal), minusinfoff, data-transfer loop control flow (origpagepostingoff / newitemoff / firstrightoff split) byte-for-byte, sibling prev-link + SPLIT_END cycleid, xl_btree_split WAL (postingoff guard `origpagepostingoff<firstrightoff`, SPLIT_L/R), rightpage-zeroing error cleanup, INDEX_CORRUPTED left-link error. Faithful. |
| `_bt_insert_parent` | nbtinsert.c:2098 | insert.rs:2106 | MATCH | isroot→newlevel, concurrent-root phony stack via `_bt_get_endpoint`, downlink build, getstackbuf re-find, rbuf unlock ordering, INDEX_CORRUPTED re-find-parent error, recursive insertonpg at bts_offset+1. |
| `_bt_finish_split` | nbtinsert.c:2240 | insert.rs:2205 | MATCH | rbuf lock, wasroot via metapage, wasonly, delegate to insert_parent. |
| `_bt_getstackbuf` | nbtinsert.c:2318 | insert.rs:2249 | MATCH | INCOMPLETE_SPLIT finish+continue, start clamp to [minoff, maxoff+1], right-then-left scan, downlink match, rightmost→InvalidBuffer. Left-loop unsigned-underflow guarded equivalently to C. |
| `_bt_newlevel` | nbtinsert.c:2444 | insert.rs:2328 | MATCH | left "minus infinity" item (t_info=sizeof(IndexTupleData), NAtts 0), right item from P_HIKEY, metapage upgrade+update, root opaque, two PageAddItems at P_HIKEY/P_FIRSTKEY, INCOMPLETE_SPLIT clear, xl_btree_newroot WAL. |
| `_bt_pgaddtup` | nbtinsert.c:2629 | insert.rs:2505 | MATCH | newfirstdataitem truncation to sizeof(IndexTupleData)+NAtts 0. |
| `_bt_delete_or_dedup_one_page` | nbtinsert.c:2682 | insert.rs:2535 | MATCH | LP_DEAD scan, simpledel pass, early-return on freed space, simpleonly/checkingunique-!uniquedup early return, bottom-up del, dedup pass gating (`BTGetDeduplicateItems && allequalimage`). |
| `_bt_simpledel_pass` | nbtinsert.c:2811 | insert.rs:2627 | MATCH | deadblocks bsearch, deltids/status build for plain + posting tuples, knowndeletable=ItemIdIsDead, delitems_delete_check seam (genuine tableam dep). |
| `_bt_deadblocks` | nbtinsert.c:2937 | insert.rs:2715 | MATCH | newitem block first, plain+posting accumulation, qsort+qunique (sort_unstable+dedup). |
| `_bt_blk_cmp` | nbtinsert.c:3010 | insert.rs:2753 | MATCH | u32 compare. |
| `bt_doinsert` wrapper | (public btinsert entry) | insert.rs:602 | MATCH | Mcx setup + delegate. |
| `mark_item_dead` / `overwrite_item` / codec helpers | (inline macros/headers) | insert.rs:106-510,1048,1616 | MATCH | All bit layouts/constants verified against PG headers (ItemPointerData, IndexTupleData t_info masks, ItemIdData lp_off:15/lp_flags:2/lp_len:15 LP_DEAD=3, BTPageOpaqueData 16B + flag bits, pivot/posting bits, SizeOfBtreeInsert/Split/Newroot, xl_btree_metadata serialization). No constant mismatch. |

## nbtsplitloc.c per-function table (14 functions) — all MATCH

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `_bt_findsplitloc` | nbtsplitloc.c:128 | splitloc.rs:315 | MATCH | leftspace/rightspace overhead, hikey rightspace deduction, olddataitemstotal, newitemsz+ItemIdData, strategy dispatch (nonleaf/rightmost-leaf/afternewitemoff/50:50), precise-split-after-newitem early return, deltasort, defaultinterval, strategy switch (DEFAULT/MANY_DUPLICATES interval=nsplits/SINGLE_VALUE resort+interval=1), bestsplitloc. Feasible-split elog→`Err`. |
| `_bt_recsplitloc` | nbtsplitloc.c:448 | splitloc.rs:581 | MATCH | newitemisfirstright, posting suffix saving (>64B), leftfree/rightfree, leaf `firstrightsz+MAXALIGN(ItemPointerData)-postingsz`, newitem accounting, non-leaf rightfree adjustment, `(int16)` truncation semantics preserved, legality+minfirstrightsz. |
| `_bt_deltasortsplits` | nbtsplitloc.c:565 | splitloc.rs:683 | MATCH | usemult delta `ff*leftfree-(1-ff)*rightfree` (f64→i16 trunc-toward-zero == C), abs, qsort. |
| `_bt_splitcmp` | nbtsplitloc.c:593 | splitloc.rs:707 | MATCH | pg_cmp_s16 on curdelta. |
| `_bt_afternewitemoff` | nbtsplitloc.c:629 | splitloc.rs:725 | MATCH | nkeyatts==1, P_FIRSTKEY, equisize (`newitemsz==minfirstrightsz`, `newitemsz*(maxoff-1)==olddataitemstotal`), width cap `MAXALIGN(IndexTupleData+2*int64)+ItemIdData`, rightmost-item keepnatts, posting/adjacenthtid test, interp>leaffillfactormult. |
| `_bt_adjacenthtid` | nbtsplitloc.c:748 | splitloc.rs:838 | MATCH | same-block, next-block+FirstOffsetNumber. |
| `_bt_bestsplitloc` | nbtsplitloc.c:787 | splitloc.rs:866 | MATCH | min-penalty scan with perfectpenalty early-break, MANY_DUPLICATES 50:50 fallback (`!rightmost && !newitemonleft && firstrightoff in [newitemoff, newitemoff+9)`). |
| `_bt_defaultinterval` | nbtsplitloc.c:875 | splitloc.rs:931 | MATCH | tolerance LEAF/INTERNAL_SPLIT_DISTANCE, low/high left/right bounds, first out-of-tolerance split index. |
| `_bt_strategy` | nbtsplitloc.c:933 | splitloc.rs:986 | MATCH | internal→minfirstrightsz, interval-edge keep_natts, default/many-dup (return indnkeyatts)/single-value (rightmost or hikey-vs-newitem) selection. |
| `_bt_interval_edges` | nbtsplitloc.c:1051 | splitloc.rs:1087 | MATCH | reverse scan, left/right interval via firstrightoff + newitemonleft tie-break. |
| `_bt_split_penalty` | nbtsplitloc.c:1130 | splitloc.rs:1159 | MATCH | internal newitem-firstright→newitemsz else MAXALIGN(len)+ItemIdData; leaf keep_natts(lastleft,firstright). |
| `_bt_split_lastleft` | nbtsplitloc.c:1158 | splitloc.rs:1177 | MATCH | newitem when newitemonleft&firstrightoff==newitemoff else prev item. |
| `_bt_split_firstright` | nbtsplitloc.c:1174 | splitloc.rs:1190 | MATCH | newitem when !newitemonleft&firstrightoff==newitemoff else item. |
| constants/codec helpers | (headers) | splitloc.rs:55-232 | MATCH | maxalign, sizes, fillfactors, pg_cmp_s16, opaque/itup codec verified. |

---

## FAIL findings

### FINDING 1 (MISSING) — `_bt_check_unique`: unique-violation enforcement absent
- **File:** `crates/backend-access-nbtree-core/src/insert.rs:936-947` (the duplicate-found branch).
- **C reference:** nbtinsert.c:582-674.
- **Divergence:** When `table_index_fetch_tuple_check` reports a duplicate and the
  check is not `UNIQUE_CHECK_PARTIAL`, the C code:
  1. derives `xwait = TransactionIdIsValid(SnapshotDirty.xmin) ? xmin : xmax` and,
     if valid, sets `*speculativeToken = SnapshotDirty.speculativeToken`,
     `bounds_valid=false`, and returns `xwait` (C 586-598);
  2. otherwise re-probes the inserting tuple with `SnapshotSelf`; if dead, `break`
     (no error) (C 617-630);
  3. calls `CheckForSerializableConflictIn` (C 639);
  4. releases buffers, deforms the tuple, and **`ereport(ERROR, errcode(ERRCODE_UNIQUE_VIOLATION), "duplicate key value violates unique constraint \"%s\"" ...)`** (C 655-674).
  The port replaces this entire block with
  `return Err(PgError::error("_bt_check_unique: SnapshotDirty conflict handling not yet ported"))`.
  No `ERRCODE_UNIQUE_VIOLATION` / "duplicate key value" exists anywhere in the crate
  (verified by grep). The xwait derivation that `_bt_doinsert` depends on is also gone.
- **Why FAIL:** This is the core purpose of `_bt_check_unique` — the logic belongs in
  this crate; a body replaced by a deferral error is MISSING (panicking is only
  acceptable on the unported *callee* `table_index_fetch_tuple_check`, which is
  correctly a seam-and-panic). The current seam signature (`-> bool`, only `all_dead`
  out-param) also does not expose `xmin/xmax/speculativeToken`, so the path is
  structurally unportable until that seam is widened.

### FINDING 2 (PARTIAL) — `_bt_findinsertloc`: !heapkeyspace "get tired" random stop dropped
- **File:** `crates/backend-access-nbtree-core/src/insert.rs:1199-1208`.
- **C reference:** nbtinsert.c:969-972.
- **Divergence:** C breaks the right-scan loop when
  `P_RIGHTMOST(opaque) || _bt_compare(...P_HIKEY...) != 0 || pg_prng_uint32(&pg_global_prng_state) <= (PG_UINT32_MAX / 100)`.
  The port keeps only the first two disjuncts and drops the
  `pg_prng_uint32(...) <= PG_UINT32_MAX/100` (~1% per-page) random give-up.
- **Why FAIL:** Not behavior-preserving. The C comment explicitly states this
  condition "is not flippant; it is important ... it does an excellent job of
  preventing O(N^2) behavior with many equal keys." Removing it makes the port scan
  to the legal limit every time on long runs of duplicate keys in !heapkeyspace
  (pg_upgrade'd v2/v3) indexes, reintroducing the documented O(N²) pathology.
  `pg_prng_uint32` is a pure PRNG (portable in-crate). The "purely a performance
  heuristic" comment in the port is incorrect.

---

## Seam audit — clean
- Owned inward seam: `bt_doinsert` (only one). Installed in
  `crates/backend-access-nbtree-core/src/lib.rs` `init_seams()` and reachable from
  `seams-init::init_all`.
- `backend_access_nbtree_core_seams::bt_delitems_delete_check` declared by this crate,
  installed by `page::bt_delitems_delete_check`. Thin delegate.
- Outward seams (`bufmgr::*`, `xloginsert::*`, `relcache::*`, `miscinit::*`) are all
  thin marshal+delegate to genuinely cross-crate subsystems; no in-crate logic embedded.
- Seam-and-panics (`table_index_fetch_tuple_check`, `_bt_allocbuf`,
  `_bt_conditionallockbuf`, `_bt_vacuum_cycleid`, `read_buffer_unlocked`,
  SpeculativeInsertionWait/XactLockTableWait) sit at real unported-subsystem
  boundaries with surrounding logic ported — acceptable, EXCEPT they do not excuse
  FINDING 1's in-crate deferral `Err`.

## Spot-checked MATCH verdicts (re-derived in full)
- `_bt_split` data-transfer loop + WAL postingoff guard — byte-for-byte.
- `_bt_recsplitloc` leaf/non-leaf free-space arithmetic incl. `(int16)` truncation.
- `_bt_bestsplitloc` MANY_DUPLICATES `[newitemoff, newitemoff+9)` fallback.
- All numeric constants/bit layouts (codec helpers) vs PG headers.
