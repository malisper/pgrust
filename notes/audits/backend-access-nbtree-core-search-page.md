# Logic audit: backend-access-nbtree-core (nbtsearch.c + nbtpage.c slices)

Independent, merge-blocking logic audit. Re-derived from C sources; port comments and
green build NOT trusted.

- C: `src/backend/access/nbtree/nbtsearch.c`, `nbtpage.c` (PostgreSQL 18.3)
- c2rust: `c2rust-runs/backend-access-nbtree-core/src/{nbtsearch,nbtpage}.rs`
- Port: `crates/backend-access-nbtree-core/src/{search.rs,page.rs,helpers.rs}`

**VERDICT: 1 FAIL (`_bt_first` skip-array boundary-key clobber). Everything else MATCH/SEAMED.**

---

## nbtsearch.c -> search.rs

| Function | C line | Port line | Verdict | Note |
|---|---|---|---|---|
| `_bt_drop_lock_and_maybe_pin` | 64 | 498 | MATCH | dropPin branch, lsn set, relbuf+InvalidBuffer faithful; RelationNeedsWAL Assert dropped (debug-only) |
| `_bt_search` | 107 | 528/541 | MATCH | root fetch, moveright, leaf break, binsrch, downlink, stack push, level-1 write-lock promotion, BT_WRITE relock+moveright |
| `_bt_moveright` | 246 | 652/667 | MATCH | cmpval=nextkey?0:1; incomplete-split finish (re-reads page for still-incomplete recheck, equivalent); P_IGNORE/hikey>=cmpval move-right; final P_IGNORE ereport |
| `_bt_binsrch` | 348 | 755/766 | MATCH | high++ invariant, mid, cmpval, leaf backward OffsetNumberPrev, non-leaf OffsetNumberPrev(low) |
| `_bt_binsrch_insert` | 479 | 831/841 | MATCH | bounds_valid restore, stricthigh tracking, posting overlap -> postingoff, double-set ereport, cache writeback |
| `_bt_binsrch_posting` | 607 | 941 | MATCH | non-posting->0, LP_DEAD->-1, high=nposting, 3-way ItemPointerCompare loop |
| `_bt_compare` | 693 | 995/1123 | MATCH | first-data-key->1, ncmpkey=min, NULL/NULLS_FIRST matrix, SK_BT_DESC sign-flip, keysz>ntupatts->1, forward truncated-pivot->1, scantid posting-range tiebreak. (value path panics in index_getattr -- unported callee, acceptable) |
| `_bt_first` | 887 | 1140 | **DIVERGES (FAIL)** | skip-array MINVAL/MAXVAL handling overwrites `so.keyData[bk]` (see FAIL) |
| `_bt_next` | 1597 | 1631 | MATCH | ++/-- itemIndex vs last/firstItem, steppage, returnitem |
| `_bt_readpage` | 1649 | 1669 | MATCH | fwd/bwd item loops, killed-tuple skip, array skip, posting expansion, high-key/finaltup checkkeys, forcenonrequired resets, firstItem/lastItem |
| `_bt_saveitem` | 2055 | 1970 | MATCH | heapTid, indexOffset, currTuples memcpy + MAXALIGN nextTupleOffset |
| `_bt_setuppostingitems` | 2085 | 2003 | MATCH | base memcpy at MAXALIGN(postingoffset), t_info size rewrite at +6/+7, return offset |
| `_bt_savepostingitem` | 2123 | 2044 | MATCH | heapTid/indexOffset, shared tupleOffset |
| `_bt_returnitem` | 2144 | 2080 | SEAMED | AM driver reads back via current_heaptid/currTuples (documented seam) |
| `_bt_steppage` | 2169 | 2092 | MATCH | killitems, markPos clone, needPrimScan moreLeft/Right, unpin, blkno pick, opposite-dir primscan cancel, readnextpage |
| `_bt_readfirstpage` | 2269 | 2171 | MATCH | needPrimScan/dir moreLeft-Right init, readpage->drop-lock, else unlock+steppage |
| `_bt_readnextpage` | 2362 | 2224 | MATCH | moreLeft/Right seed, P_NONE/!more end, fwd getbuf / bwd lock_and_validate_left, P_IGNORE link-walk, relbuf+seized=false, drop-lock (parallel seize/release deferred-by-panic) |
| `_bt_lock_and_validate_left` | 2497 | 2347 | MATCH | 4-hop right walk, P_ISDELETED test, deleted-page recovery, origblkno infinite-loop guard, P_LEFTMOST exit |
| `_bt_get_endpoint` | 2614 | 2459/2470 | MATCH | level-0 fast root vs true root, P_IGNORE/rightmost step-right, level found/corrupt, downlink descend |
| `_bt_endpoint` | 2697 | 2557 | MATCH | get_endpoint(0,backward), empty->predlock+done, fwd P_FIRSTDATAKEY / bwd maxoff, invalid-dir error, readfirstpage |

nbtsearch totals: 21 functions. MATCH 19, SEAMED 1, DIVERGES 1.

Byte codec verified bit-for-bit: INDEX_ALT_TID_MASK=0x2000, BT_OFFSET_MASK=0x0FFF,
BT_IS_POSTING=0x2000, BT_PIVOT_HEAP_TID_ATTR=0x1000, INDEX_SIZE_MASK=0x1FFF, P_HIKEY=1,
P_FIRSTKEY=2, P_NONE=0, MaxTIDsPerBTreePage=1358, BTORDER_PROC=1, sizeof(ItemPointerData)=6.
IndexTuple header (t_tid@0-5, t_info@6-7) and BTPageOpaqueData (prev@0, next@4, level@8,
flags@12) match C layout.

---

## nbtpage.c -> page.rs

| Function | C line | Port line | Verdict | Note |
|---|---|---|---|---|
| `_bt_initmetapage` | 66 | 422 | MATCH | byte codec offsets verified; pd_lower = MAXALIGN(hdr)+sizeof(BTMeta) |
| `_bt_upgrademetapage` | 106 | 452 | MATCH | asserts->debug_assert; same field writes |
| `_bt_getmeta` | 141 | 474 | MATCH | both sanity checks + error msgs |
| `_bt_vacuum_needs_cleanup` | 178 | 503 | MATCH | version<NOVAC->relbuf+true; >5% (n/20) check; relbuf paired |
| `_bt_set_cleanup_info` | 231 | 529 | MATCH | read->write lock trade; upgrade; WAL META_CLEANUP; relbuf paired |
| `_bt_getroot` | 343 | 614 | MATCH | rd_amcache cache dropped (behaviour-preserving); create path reaches _bt_allocbuf panic; lock trades + relbuf paired; recursion releases metabuf first |
| `_bt_gettrueroot` | 579 | 755 | MATCH | follows btm_root; loop steps right; relandgetbuf chain |
| `_bt_getrootheight` | 674 | 812 | MATCH | returns fastlevel; P_NONE->0 |
| `_bt_metaversion` | 738 | 828 | MATCH | heapkeyspace = version>NOVAC; allequalimage |
| `_bt_checkpage` | 796 | 845 | MATCH | PageIsNew + special-size==MAXALIGN(sizeof opaque) |
| `_bt_getbuf` | 844 | 872 | MATCH | ReadBuffer->read_buffer_extended; lock+checkpage |
| `_bt_allocbuf` | 868 | 888 | SEAMED | panic on GetFreeIndexPage/ConditionalLockBuffer/ExtendBufferedRel -- unported bufmgr/FSM primitives |
| `_bt_relandgetbuf` | 1002 | 905 | MATCH | unlock+release+read (documented equiv) |
| `_bt_relbuf` | 1022 | 941 | MATCH | unlock+release |
| `_bt_lockbuf` | 1038 | 951 | MATCH | LockBuffer(access) |
| `_bt_unlockbuf` | 1069 | 960 | MATCH | LockBuffer(UNLOCK) |
| `_bt_conditionallockbuf` | 1092 | 968 | SEAMED | ConditionalLockBuffer unported (only used by _bt_allocbuf) |
| `_bt_upgradelockbufcleanup` | 1108 | 974 | MATCH | unlock + LockBufferForCleanup |
| `_bt_pageinit` | 1128 | 996 | MATCH | PageInit(size, sizeof opaque) |
| `_bt_delitems_vacuum` | 1153 | 1112 | MATCH | overwrite-before-delete; clears cycleid+HAS_GARBAGE; WAL VACUUM |
| `_bt_delitems_delete` | 1283 | 1175 | MATCH | does NOT clear cycleid; clears HAS_GARBAGE; WAL DELETE |
| `_bt_delitems_update` | 1404 | 1064 | MATCH | _bt_update_posting; xl_btree_update buf |
| `_bt_delitems_cmp` | 1463 | 1298 | MATCH | sort by id |
| `_bt_delitems_delete_check` | 1512 | 1308 | MATCH | table_index_delete_tuples + logical-decoding panic = unported callees; posting nested-loop equivalent |
| `_bt_leftsib_splitflag` | 1694 | 1515 | MATCH | P_NONE->false; btpo_next==target && INCOMPLETE_SPLIT; relbuf paired |
| `_bt_rightsib_halfdeadflag` | 1751 | 1537 | MATCH | P_ISHALFDEAD; relbuf paired |
| `_bt_pagedel` | 1801 | 1561 | MATCH | bail conditions, halfdead loop, rightsib chain, stack reuse |
| `_bt_mark_page_halfdead` | 2087 | 1708 | MATCH | rightsib-halfdead bail; downlink validate; copy-downlink+delete-next; trunctuple SetTopParent; WAL (PredicateLockPageCombine panic = unported callee) |
| `_bt_unlink_halfdead_page` | 2313 | 1928 | MATCH | left->target->right lock order; sibling relinks; BTPageSetDeleted; metapage fastroot; WAL; all relbuf/release paths paired |
| `_bt_lock_subtree_parent` | 2812 | 2312 | MATCH | parentoffset<maxoff fast path; rightmost recursion; leftsib splitflag (_bt_getstackbuf panic = unported sibling) |
| `_bt_pendingfsm_init` | 2953 | 2412 | MATCH | bufsize=256; work_mem caps; Min/Max clamps |
| `_bt_pendingfsm_finalize` | 2995 | 2442 | MATCH | GetOldestNonRemovable side-effect; safexid-order break; RecordFreeIndexPage |
| `_bt_pendingfsm_add` | 3062 | 2486 | MATCH | maxbufsize discard; double-on-full capped; safexid-order assert |
| `BTPageIsRecyclable`/`_bt_page_recyclable` | inline | 1026 | MATCH | P_ISDELETED->GlobalVisCheckRemovableFullXid; BTPageGetDeleteXid |
| `BTPageSetDeleted` (inline) | nbtree.h | 2289 | MATCH | clears HALF_DEAD, sets DELETED\|HAS_FULLXID; pd_lower/pd_upper; safexid |
| `build_empty_metapage` (btbuildempty helper) | - | 2532 | MATCH | smgr bulk-write INIT fork; fully wired |

nbtpage totals: 35 functions (incl. inline helpers). MATCH 31, SEAMED 2 (+ in-body
unported-callee panics), DIVERGES 0. No lock leaks, no unpaired locks. Metapage codec
(6xu32 @0-23, u32 @24, f64 @32 w/ pad @28-31, bool @40) and BTPageOpaqueData codec match
C layout.

Constants verified vs headers: BTREE_MAGIC=0x053162, BTREE_VERSION=4, BTREE_MIN_VERSION=2,
BTREE_NOVAC_VERSION=3.

Informational (not a FAIL): `serialize_xl_btree_metadata` emits 25 packed bytes vs C
sizeof=28 (3 struct-padding bytes) -- consistent with repo-wide packed-WAL convention and
the matching nbt-xlog redo port.

---

## helpers.rs (inline page/tuple seams)

All 8 inline-helper seams (page_get_item, page_get_max_offset_number, tuple_is_pivot,
tuple_is_posting, tuple_heap_tid, tuple_n_posting, tuple_posting_tid, page_opaque) plus
page_is_new verified against bufpage.h / nbtree.h inline macros. ItemId masks, item offset
arithmetic, posting-list ALT_TID layout, and posting-stride (sizeof IPD=6) all MATCH.
tuple_heap_tid pivot truncated-attr falls back to a zero ItemPointer (matches the C
NULL-ish result). PASS.

---

## FAIL findings (1)

### F1 -- `_bt_first` skip-array MINVAL/MAXVAL boundary-key clobber
- File: `/Users/malisper/workspace/work/pgrust-fabled/.claude/worktrees/agent-aeebd1af93738e95b/crates/backend-access-nbtree-core/src/search.rs:1248-1254`
- C: `nbtsearch.c:1099` / `1104` -- `bkey = array->low_compare;` (resp. `high_compare`)
  reassigns the **local pointer** to the array's separately-stored compare ScanKey;
  `so->keyData[ikey]` is never written.
- Port instead does:
  ```rust
  if let Some(nb) = new_bkey {
      so.keyData[bk] = nb;   // clobbers the scan's persistent keyData[bk]
  } else {
      bkey = None;
  }
  ```
  `so.keyData` is the preprocessed key array consumed by `_bt_checkkeys` on every page for
  the whole scan. Overwriting `so.keyData[bk]` (the skip-array `=` key for that attribute)
  with its low_compare/high_compare inequality permanently corrupts the scan's key set for
  subsequent checkkeys / array-advance on skip-array scans.
- Fix: do not write into `so.keyData`. Carry the substituted compare key as a local owned
  value -- set `chosen` directly from a clone of `arr.low_compare`/`high_compare`, mirroring
  the way the NOT-NULL `deduced_notnull` path already produces a local key -- so `so.keyData`
  is left intact exactly as C does.
- Severity: currently latent -- every `_bt_first` path that builds the insertion scankey
  reaches `index_getprocinfo_oid` (an honest unported-callee panic), so the corrupted
  `so.keyData` is never observed at runtime today. Must be fixed before that callee lands.
