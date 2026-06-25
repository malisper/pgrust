# Audit: backend-access-nbtree-core — nbtutils.c + nbtpreprocesskeys.c slices

Scope: `crates/backend-access-nbtree-core/src/utils.rs` (← nbtutils.c, 4302 LOC, 50 fns) and
`crates/backend-access-nbtree-core/src/preprocesskeys.rs` (← nbtpreprocesskeys.c, 2855 LOC, 30 fns).

Method: independent re-derivation from C + c2rust. Read-only. Constants checked against
`src/include/access/nbtree.h`, `skey.h`, `stratnum.h`, `itup.h`.

Sources:
- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/access/nbtree/{nbtutils,nbtpreprocesskeys}.c`
- c2rust: `/Users/malisper/workspace/work/pgrust/c2rust-runs/backend-access-nbtree-core/src/{nbtutils,nbtpreprocesskeys}.rs`
- Port: `/Users/malisper/workspace/work/pgrust-fabled/.claude/worktrees/agent-aeebd1af93738e95b/crates/backend-access-nbtree-core/src/{utils,preprocesskeys}.rs`

## VERDICT: FAIL — 2 logic divergences (merge-blocking)

1. `bt_start_array_keys` — SAOP array `cur_elem` not reset at scan start (utils.rs:1020-1028).
2. `bt_killitems_inner` — non-posting LP_DEAD compare uses the fixed (not the read-ahead-advanced) kitem (utils.rs:2795).

Plus 1 minor benign divergence (`btproperty` extra `*isnull` write) and 1 absent trivial fn
(`_bt_end_vacuum_callback`). All other 76 functions MATCH or are legitimately SEAMED on genuinely
unported external callees.

---

## nbtutils.c — function table

| C fn (line) | port (line) | verdict | note |
|---|---|---|---|
| _bt_mkscankey (93) | bt_mkscankey (596) | PARTIAL/SEAMED | logic faithful; `index_getattr`/`index_getprocinfo` seam external callees. `indnullsnotdistinct` branch dropped (relcache model gap — field absent in trimmed FormData_pg_index). |
| _bt_freestack (185) | bt_freestack (690) | MATCH | owned-box drop mirrors pfree walk. |
| _bt_compare_array_skey (214) | bt_compare_array_skey (705) | MATCH | NULL/ISNULL/NULLS_FIRST/DESC/INVERT exact. |
| _bt_binsrch_array_skey (285) | bt_binsrch_array_skey (758) | MATCH | cur_elem_trig bounds, midpoint `low+(high-low)/2`, result==0 break, final recompare exact. |
| _bt_binsrch_skiparray_skey (441) | bt_binsrch_skiparray_skey (878) | MATCH | null_elem/tupnull, fwd/bwd cur_elem_trig short-circuit exact. |
| _bt_skiparray_set_element (548) | bt_skiparray_set_element (951) | MATCH/SEAMED | low/high, tupnull, flag clears; datumCopy seamed. |
| _bt_skiparray_set_isnull (585) | bt_skiparray_set_isnull (989) | MATCH | flag clear/set exact. |
| _bt_start_array_keys (609) | bt_start_array_keys (1012) | **DIVERGES** | SAOP `cur_elem` never reset — see FAIL #1. |
| _bt_array_set_low_or_high (637) | bt_array_set_low_or_high (1043) | MATCH (split) | `cur_elem` write factored into companion `array_set_cur_elem`; correct only if every caller pairs it — bt_start_array_keys does NOT. |
| _bt_array_decrement (701) | bt_array_decrement (1117) | MATCH/SEAMED | MINVAL/ISNULL/NULLS_FIRST/PRIOR/low_compare exact; skip_decrement + by-ref pfree/datumCopy seamed. |
| _bt_array_increment (834) | bt_array_increment (1201) | MATCH/SEAMED | symmetric; skip_increment seamed. |
| _bt_advance_array_keys_increment (973) | bt_advance_array_keys_increment (1300) | MATCH | reverse loop, roll-over set_low_or_high+cur_elem, exhaust→start_array_keys(neg_dir) exact. |
| _bt_tuple_before_array_skeys (1078) | bt_tuple_before_array_skeys (1351) | MATCH/SEAMED | non-required/truncated/inequality/MINVAL-MAXVAL/NEXT-PRIOR/fwd<0-bwd>0 exact; index_getattr seamed. |
| _bt_start_prim_scan (1270) | bt_start_prim_scan (1491) | MATCH | needPrimScan; _bt_parallel_done seamed (single-process no-op). |
| _bt_advance_array_keys (1389) | bt_advance_array_keys (1520) | MATCH/SEAMED | full ~680-line spot-check: per-key loop, beyond_end_advance, all/required_satisfied, sktrig, recheck, second-pass recursion, disposition machine (new_prim_scan/continue_scan/end_toplevel) all faithful. index_getattr/parallel-primscan seamed. |
| _bt_verify_keys_with_arraykeys (2068) | bt_verify_keys_with_arraykeys (1952) | MATCH | qual_ok/arrayidx walk/sk_argument==elem/required-ordering exact (debug-only). |
| _bt_checkkeys (2146) | bt_checkkeys (2008) | MATCH | continuescan/array short-circuit/before_array recheck/look-ahead/advance_array. debug recheck block correctly omitted. |
| _bt_scanbehind_checkkeys (2274) | bt_scanbehind_checkkeys (2074) | MATCH | scanBehind/oppositeDirCheck exact. |
| _bt_oppodir_checkkeys (2329) | bt_oppodir_checkkeys (2117) | MATCH | flipped=-dir, !continuescan && !=BTEqual short-circuit exact. |
| _bt_set_startikey (2387) | bt_set_startikey (2158) | MATCH | all 4 key classes, `>` vs `>=` firstchangingattnum, null_elem, start_past_saop_eq, forcenonrequired finalization exact. one debug Assert dropped (benign). |
| _bt_check_compare (2693) | bt_check_compare (2360) | MATCH | continuescan/required-dir, sentinel skip-array fallback, IS NULL/NOT NULL, NULLS_FIRST, advancenonrequired exact. one debug Assert dropped (utils.rs:2435, benign). |
| _bt_check_rowcompare (2960) | bt_check_rowcompare (2504) | MATCH | member iteration, NULL+reqflags first-member widening, SK_BT_DESC inversion, SK_ROW_END, deciding-column switch exact. |
| _bt_checkkeys_look_ahead (3194) | bt_checkkeys_look_ahead (2630) | MATCH | targetdistance ramp ×2 / decay /8 min 1, clamp, skip±1 exact. minor u16-vs-int guard-subtraction width note (unreachable). |
| _bt_killitems (3294) | bt_killitems / _inner (2698) | **DIVERGES** | non-posting kitem — see FAIL #2. Plus rel_mcx/bt_unlockbuf internal panics (BufferGetPage mcx not threaded; _bt_unlockbuf external). |
| _bt_vacuum_cycleid (3513) | bt_vacuum_cycleid (2921) | SEAMED | btvacinfo shmem array — no producer. |
| _bt_start_vacuum (3547) | bt_start_vacuum (2928) | SEAMED | btvacinfo shmem. |
| _bt_end_vacuum (3604) | bt_end_vacuum (2947) | SEAMED | btvacinfo shmem. |
| _bt_end_vacuum_callback (3632) | — | MISSING (trivial) | on_shmem_exit wrapper absent; would itself be seamed behind btvacinfo. Low impact. |
| BTreeShmemSize (3641) | bt_shmem_size (2955) | SEAMED | BTVacInfo shmem layout. |
| BTreeShmemInit (3654) | bt_shmem_init (2963) | SEAMED | ShmemInitStruct. |
| btoptions (3682) | btoptions (2980) | SEAMED | build_reloptions(RELOPT_KIND_BTREE) — crate doesn't dep reloptions. |
| btproperty (3705) | btproperty (2994) | DIVERGES (benign) | extra `*isnull=false` write in AMPROP_RETURNABLE case (utils.rs:3010); C only sets `*res`. Harmless (caller pre-inits isnull). |
| btbuildphasename (3728) | btbuildphasename (3018) | MATCH | phase strings + numbers 1-5 exact; None=NULL. |
| _bt_truncate (3776) | bt_truncate (3035) | PARTIAL/SEAMED | suffix-trunc, posting-offset, heap-TID pivot attr, newsize all faithful; blocked only by index_truncate_tuple_bytes (indextuple.c). |
| _bt_keep_natts (3921) | bt_keep_natts (3141) | PARTIAL/SEAMED | loop faithful; index_getattr + function_call2_coll seamed. debug_assert calls _inner→datum_image_eq panic (debug-build landmine). |
| _bt_keep_natts_fast (3995) | bt_keep_natts_fast / _inner (3192) | PARTIAL/SEAMED | loop faithful; index_getattr + datum_image_eq seamed (datum.c). |
| _bt_check_natts (4042) | bt_check_natts (3241) | MATCH | posting/pivot/neg-inf/P_HIKEY/P_FIRSTDATAKEY branches exact. |
| _bt_check_third_page (4202) | bt_check_third_page (3344) | MATCH | size limits, both ereports, exact message/detail/hint, SQLSTATE (ERRCODE_PROGRAM_LIMIT_EXCEEDED) exact. errtableconstraint context drop = project-wide gap. |
| _bt_allequalimage (4259) | bt_allequalimage / _dbg (3421) | MATCH | INCLUDE check, per-attr proc loop, DEBUG1 messages, fmgr call faithful. |

Static helpers _bt_skiparray_set_element/isnull, _bt_array_set_low_or_high, _bt_binsrch_*, _bt_compare_array_skey, _bt_oppodir_checkkeys, _bt_check_compare, _bt_check_rowcompare, _bt_checkkeys_look_ahead all covered above.

## nbtpreprocesskeys.c — function table

| C fn (line) | port (line) | verdict | note |
|---|---|---|---|
| _bt_preprocess_keys (201) | _bt_preprocess_keys (355) | MATCH/SEAMED | per-attr grouping, =/</<= /> redundancy + contradiction elimination, xform-slot tracking, required-marking via priorNumberOfEqualCols, fast paths (0/1 keys), redundant_key_kept→unmark all faithful. rd_indoption/rd_opfamily seamed. |
| _bt_fix_scankey_strategy (666) | _bt_fix_scankey_strategy (774) | MATCH/SEAMED | DESC commute + indoption shift, IS NULL→Eq, IS NOT NULL→Less/Greater per NULLS_FIRST, row-header subkey loop. rd_indoption seamed. |
| _bt_mark_scankey_required (778) | _bt_mark_scankey_required (849) | MATCH | REQFWD/REQBKWD per strategy/direction; row subkey propagation w/ attno/strategy breaks exact. |
| _bt_compare_scankey_args (868) | _bt_compare_scankey_args (905) | MATCH/SEAMED | NULL/skip, NULLS_FIRST commute, null-bool, row→None, array dispatch, same/cross-type fmgr, DESC un-flip before opfamily. all 5 call-site arg wirings verified. |
| _bt_compare_array_scankey_args (1095) | _bt_compare_array_scankey_args (1069) | MATCH | num_elems==-1 → skiparray vs saoparray dispatch. |
| _bt_saoparray_shrink (1133) | _bt_saoparray_shrink (1102) | MATCH/SEAMED | cross-type ORDER lookup, all 5 strategies incl. Less/GreaterEqual fall-through cmpexact carry, memmove-to-start, qual_ok. binsrch/fmgr_info seamed. |
| _bt_skiparray_shrink (1258) | _bt_skiparray_shrink (1222) | MATCH | high/low_compare replacement via compare_scankey_args (2-elem pair), strategy switch. |
| _bt_skiparray_strat_adjust (1380) | _bt_skiparray_strat_adjust (1279) | MATCH/SEAMED | high<→decrement, low>→increment. MemoryContextSwitchTo elided (no allocation crosses). |
| _bt_skiparray_strat_decrement (1410) | _bt_skiparray_strat_decrement (1315) | MATCH/SEAMED | subtype gate, underflow→qual_ok=false, DESC commute, opfamily member, →`<=`. skip_decrement seamed. |
| _bt_skiparray_strat_increment (1468) | _bt_skiparray_strat_increment (1361) | MATCH/SEAMED | mirror →`>=`; DESC commute. skip_increment seamed. |
| _bt_unmark_keys (1541) | _bt_unmark_keys (1422) | MATCH | equality-priority pass, firsti back-unmark, REQFWD/REQBKWD one-each retention, keep/unmark partition + reorder, orderProcs reposition, array remap+sort. |
| _bt_reorder_array_cmp (1791) | _bt_reorder_array_cmp (1585) | MATCH | pg_cmp_s32(scan_key,scan_key). |
| _bt_preprocess_array_keys (1843) | _bt_preprocess_array_keys (1598) | MATCH/SEAMED | skip-array backfill, provisional copy, NULL-array→qual_ok=false, inequality→extreme degenerate, sort+dedup, merge-with-prior, single-attr origarray. prepare_skip_support/rd_* seamed. |
| _bt_preprocess_array_keys_final (2223) | _bt_preprocess_array_keys_final (1948) | MATCH/SEAMED | equality filter, lazy ORDER proc, orderProcs reorder, 1-elem→non-array forward-shift (Vec::remove==memmove), skiparray strat adjust, parallel limit. |
| _bt_num_array_keys (2411) | _bt_num_array_keys (2072) | MATCH/SEAMED | SAOP count, backfill-gap skip arrays, prev_numSkipArrayKeys rollback, rowcompare stop, one-past-end break order (no OOB). |
| _bt_find_extreme_element (2577) | _bt_find_extreme_element (2179) | MATCH | opfamily member/opcode + error msgs, linear extreme via FunctionCall2Coll. |
| _bt_setup_array_cmp (2650) | _bt_setup_array_cmp (2228) | MATCH/SEAMED | same-type cached (is_order flag) + cross-type ORDER + same-type sort proc lookups + error msgs. index_getprocinfo/fmgr_info seamed. |
| _bt_sort_array_elements (2730) | _bt_sort_array_elements (2294) | MATCH | nelems<=1 short-circuit, qsort_arg+qunique_arg; fallible comparator captured. |
| _bt_merge_arrays (2774) | _bt_merge_arrays (2360) | MATCH/SEAMED | cross-type mergeproc→false on missing, intersection merge into elems_orig, nelems_orig update. |
| _bt_compare_array_elements (2842) | _bt_compare_array_elements (2424) | MATCH/SEAMED | DatumGetInt32(FunctionCall2Coll), INVERT_COMPARE_RESULT exact. sortproc oid via handle low-32 (seam convention). |

---

## FAIL details

### FAIL #1 — `bt_start_array_keys`: SAOP `cur_elem` not reset at scan start
- Port: `utils.rs:1020-1028`.
- C `_bt_start_array_keys` (nbtutils.c:617-626) relies on `_bt_array_set_low_or_high` to set
  BOTH `skey->sk_argument` AND, for SAOP arrays, `array->cur_elem = set_elem` (nbtutils.c:656).
- The port deliberately split that `cur_elem` write out of `bt_array_set_low_or_high` into the
  companion `array_set_cur_elem` (utils.rs:1100-1109). Every other call site pairs the two
  (utils.rs:1331-1332, 1646-1647, 1660-1661), but `bt_start_array_keys` (line 1026) calls only
  `bt_array_set_low_or_high` and writes the clone back without `array_set_cur_elem`.
- Effect: at scan start a SAOP array's `sk_argument` is set to the correct low/high value but
  `cur_elem` keeps its stale prior value. Desyncs `_bt_binsrch_array_skey` cur_elem_trig bound
  optimization (utils.rs:782/807), `_bt_array_increment`/`decrement` boundary tests
  (utils.rs:1128/1212), and trips the `sk_argument == elem_values[cur_elem]` invariant asserted
  in `_bt_verify_keys_with_arraykeys` / top of `bt_advance_array_keys`. Recurs on every roll-over
  because `_bt_advance_array_keys_increment` exhaustion calls `bt_start_array_keys`
  (utils.rs:1340).
- Fix: add `array_set_cur_elem(&mut so.arrayKeys[i], low_not_high)` in the loop after line 1026.

### FAIL #2 — `bt_killitems_inner`: non-posting compare uses fixed kitem, not the advanced one
- Port: `utils.rs:2795`.
- C (nbtutils.c:3429): non-posting branch is `ItemPointerEquals(&ituple->t_tid, &kitem->heapTid)`,
  where `kitem` is a SINGLE moving pointer that the posting-list read-ahead advances at
  nbtutils.c:3414 (`kitem = &so->currPos.items[so->killedItems[pi++]]`).
- The port keeps two variables: a moving `kitem` (line 2767, used correctly in the posting branch)
  and a FIXED `kitem_heaptid` (line 2756). Line 2795 compares against the fixed `kitem_heaptid`
  instead of the advanced `kitem`.
- Trigger (within one outer-`i` iteration): an earlier `offnum` is a posting tuple whose `j`-loop
  advances `kitem` past several killed items, then a later `offnum` in the same inner search loop
  is a plain (non-posting) tuple. C matches against the advanced kitem; the port matches against
  the original — so the port can miss or mis-target an LP_DEAD hint. Hint-only (no index
  corruption), but a faithful-logic divergence.
- Fix: use the moving `kitem` in the else branch at line 2795.

## Acceptable seams (genuinely unported external callees — NOT faults)
index_getattr (index-tuple deform, unported repo-wide), index_getprocinfo/fmgr_info (no ORDER-proc
handle producer), index_truncate_tuple (indextuple.c), datum_image_eq/datumCopy/by-ref pfree
(datum.c), skip_decrement/skip_increment + PrepareSkipSupportFromOpclass (opclass sortsupport),
_bt_getbuf/_bt_unlockbuf (nbtpage.c), _bt_parallel_done/_bt_parallel_primscan_schedule (nbtree.c,
single-process no-ops), btvacinfo shmem array (vacuum_cycleid/start_vacuum/end_vacuum/shmem),
build_reloptions (btoptions), rd_indoption/rd_opfamily/rd_indcollation (relcache index-metadata
arrays not in trimmed RelationData). In every SEAMED function the contradiction/redundancy/
required-marking/array-advancement LOGIC itself is ported in-crate, never replaced by a panic.

## Minor / debug-only (non-blocking)
- `bt_keep_natts` debug_assert (utils.rs:3182) calls `_inner`→datum_image_eq panic — a debug-build
  landmine (release builds unaffected); C's `_bt_keep_natts_fast` is fully impl'd, here seamed.
- Dropped debug Asserts in bt_set_startikey, bt_check_compare (utils.rs:2435), bt_killitems
  (scan!=NULL — not expressible, no scan param). Cosmetic.
- `bt_mkscankey` `indnullsnotdistinct` branch dropped (relcache model gap): behaviorally wrong for
  NULLS NOT DISTINCT unique indexes.
- `btproperty` extra `*isnull=false` write (benign).
- `bt_check_third_page` NULL-heap-TID fallback `(0,0)` (unreachable).
- `bt_checkkeys_look_ahead` u16 guard-subtraction vs C int (unreachable given minoff guard).

## Constants verified vs nbtree.h / skey.h / stratnum.h
BTMaxItemSize=2704, BTMaxItemSizeNoHeapTid=2712, BT_PIVOT_HEAP_TID_ATTR=0x1000, BT_OFFSET_MASK=
0x0FFF, INDEX_ALT_TID_MASK, P_HIKEY=1, BTREE_VERSION=4, BTREE_NOVAC_VERSION=3, MAX_BT_CYCLE_ID=
0xFF7F, BTEQUALIMAGE_PROC=4, BTORDER_PROC=1, SK_BT_INDOPTION_SHIFT=24, strategy 1..5,
INDOPTION_DESC=0x1/NULLS_FIRST=0x2, SK_BT_{REQFWD,REQBKWD,DESC,NULLS_FIRST,SKIP,MINVAL,MAXVAL,
NEGATEDNULLS,NEXT,PRIOR} — all correct.
