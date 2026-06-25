# Audit: backend-access-nbtree-nbtree (`src/backend/access/nbtree/nbtree.c`)

Independent re-derivation from the C source (PostgreSQL 18.3) and the c2rust
rendering (`c2rust-runs/backend-access-nbtree-nbtree/src/nbtree.rs`), compared
function-by-function against the port in
`crates/backend-access-nbtree-nbtree/src/lib.rs` and the owned seam crates.

## Function inventory and verdicts

| # | C function (nbtree.c) | Port location | Verdict |
|---|---|---|---|
| 1 | `bthandler` (115) | `bthandler` | MATCH |
| 2 | `btbuildempty` (179) | `btbuildempty` | MATCH (smgr bulk write SEAMED to nbtree-core) |
| 3 | `btinsert` (202) | `btinsert` | MATCH (`index_form_tuple`/`_bt_doinsert` SEAMED) |
| 4 | `btgettuple` (226) | `btgettuple` | MATCH |
| 5 | `btgetbitmap` (288) | `btgetbitmap` | MATCH |
| 6 | `btbeginscan` (336) | `btbeginscan` | MATCH |
| 7 | `btrescan` (385) | `btrescan` | MATCH |
| 8 | `btendscan` (470) | `btendscan` | MATCH |
| 9 | `btmarkpos` (506) | `btmarkpos` | MATCH |
| 10 | `btrestrpos` (532) | `btrestrpos` | MATCH |
| 11 | `btestimateparallelscan` (590) | `btestimateparallelscan` | MATCH (fixed; was MISSING) |
| 12 | `_bt_parallel_serialize_arrays` (663) | `_bt_parallel_serialize_arrays` | MATCH (fixed; was MISSING) |
| 13 | `_bt_parallel_restore_arrays` (706) | `_bt_parallel_restore_arrays` | MATCH (fixed; was MISSING) |
| 14 | `btinitparallelscan` (757) | `btinitparallelscan` | MATCH (fixed; was MISSING) |
| 15 | `btparallelrescan` (773) | `btparallelrescan` | MATCH (fixed; was MISSING) |
| 16 | `_bt_parallel_seize` (816) | `bt_parallel_seize_core` (+ `_bt_parallel_seize` wrapper) | MATCH (fixed; was MISSING) |
| 17 | `_bt_parallel_release` (954) | `bt_parallel_release_core` | MATCH (fixed; was MISSING) |
| 18 | `_bt_parallel_done` (981) | `bt_parallel_done_core` | MATCH (fixed; was MISSING) |
| 19 | `_bt_parallel_primscan_schedule` (1031) | `bt_parallel_primscan_schedule_core` | MATCH (fixed; was MISSING) |
| 20 | `btbulkdelete` (1065) | `btbulkdelete` | MATCH |
| 21 | `btvacuumcleanup` (1095) | `btvacuumcleanup` | MATCH |
| 22 | `btvacuumscan` (1183) | `btvacuumscan` | MATCH |
| 23 | `btvacuumpage` (1358) | `btvacuumpage` + `btvacuumpage_leaf` | MATCH |
| 24 | `btreevacuumposting` (1696) | `btreevacuumposting` | MATCH |
| 25 | `btcanreturn` (1745) | `btcanreturn` | MATCH |
| 26 | `btgettreeheight` (1754) | `btgettreeheight` | MATCH |
| 27 | `bttranslatestrategy` (1760) | `bttranslatestrategy` | MATCH |
| 28 | `bttranslatecmptype` (1780) | `bttranslatecmptype` | MATCH |

SEAMED callees (logic owned by unported neighbors, panic until they land):
`_bt_allequalimage`, `_bt_doinsert`, `_bt_first`, `_bt_next`,
`_bt_start_prim_scan`, `_bt_killitems`, `_bt_start_array_keys`, `_bt_pagedel`,
`_bt_delitems_vacuum`, `_bt_lockbuf`/`_bt_relbuf`/`_bt_upgradelockbufcleanup`,
`_bt_checkpage`, `_bt_getrootheight`, `_bt_start_vacuum`/`_bt_end_vacuum`,
`_bt_pendingfsm_init`/`_finalize`, `_bt_set_cleanup_info`,
`_bt_vacuum_needs_cleanup`, page-format reads (nbtree-core-seams);
`index_form_tuple` (indextuple-seams); `RecordFreeIndexPage`/
`IndexFreeSpaceMapVacuum` (freespace-seams); `vacuum_delay_point`/
`vacuum_tid_is_dead` (vacuum-seams); buffer/read-stream/lmgr/relcache/tbm
substrate; and the genuinely-foreign parallel primitives below.

## Constants verified against headers

- `BTMaxStrategyNumber=5`, `BTNProcs=6`, `BTOPTIONS_PROC=5` (nbtree.h / stratnum.h)
- `BTP_LEAF=1<<0`, `BTP_DELETED=1<<2`, `BTP_HALF_DEAD=1<<4`, `BTP_SPLIT_END=1<<5` (nbtree.h)
- `P_NONE=0`, `P_HIKEY=1`, `P_FIRSTKEY=2`, `BTREE_METAPAGE=0` (nbtree.h)
- `MaxTIDsPerBTreePage = (8192-24-16)/6 = 1358`; `MaxIndexTuplesPerPage = (8192-24)/20 = 408`
- `BTMaxItemSize = 2704` (MAXALIGN_DOWN((8192-40-16)/3) - 8)
- `VACUUM_OPTION_PARALLEL_BULKDEL=1<<0`, `VACUUM_OPTION_PARALLEL_COND_CLEANUP=1<<1` (vacuum.h)
- `COMPARE_LT=1..COMPARE_GT=5`, `COMPARE_INVALID=0` (cmptype.h); strategy nums 1..5 (stratnum.h)
- `PROGRESS_SCAN_BLOCKS_TOTAL=15`, `PROGRESS_SCAN_BLOCKS_DONE=16` (progress.h)
- `SK_ISNULL=0x1`, `SK_SEARCHNULL=0x40` (skey.h); `SK_BT_SKIP=0x40000`,
  `SK_BT_MINVAL=0x80000`, `SK_BT_MAXVAL=0x100000` (nbtree.h)
- `WAIT_EVENT_BTREE_PAGE = PG_WAIT_IPC|7 = 0x08000007 = 134217735` (matches c2rust)
- `LWTRANCHE_PARALLEL_BTREE_SCAN` (lwlock.h)

## Round 1 finding (FAIL) — fixed in this audit

The nine parallel-scan functions defined in `nbtree.c`
(`btestimateparallelscan`, `_bt_parallel_serialize_arrays`,
`_bt_parallel_restore_arrays`, `btinitparallelscan`, `btparallelrescan`,
`_bt_parallel_seize`, `_bt_parallel_release`, `_bt_parallel_done`,
`_bt_parallel_primscan_schedule`) had their **entire bodies relocated to seam
declarations in `backend-access-index-indexam-seams`** (`bt_estimate_parallel_scan`,
`bt_init_parallel_scan`, `bt_parallel_rescan`, `bt_parallel_seize_dsm`,
`bt_parallel_release_dsm`, `bt_parallel_done_dsm`,
`bt_parallel_primscan_schedule_dsm`) — none installed, all panicking. Per
audit-crate §3, "a function whose *body* was replaced by a seam call to
'somewhere else' is not SEAMED, it is MISSING — the logic must live in this
crate." The logic was therefore **absent** (FAIL): the LWLock-protected
page-status state machine, the array serialize/restore, and the storage-sizing
loop are all nbtree's own logic.

### Fix

- Defined the real `BTParallelScanDescData` + `BTPS_State` enum in
  `types-nbtree` (neighbor-type rule), with the embedded `LWLock` /
  `ConditionVariable` and a `*mut u8` `btps_arrtail` modelling the DSM
  flexible-array (`btps_arrElems[]` + flattened skip-array datums), values
  verified against the C struct. Added `indnkeyatts` to `FormData_pg_index`
  (`IndexRelationGetNumberOfKeyAttributes`) and the `SK_*` flag constants to
  `types-scan`.
- Ported all nine functions in-crate, operating on the resolved
  `&mut BTParallelScanDescData`. The state machine, the SAOP/skip-array
  serialize/restore branching, the init/rescan field writes, and the
  `btestimateparallelscan` sizing loop now live in
  `crates/backend-access-nbtree-nbtree/src/lib.rs`.
- Replaced the seven indexam logic-seams with a **single thin DSM-pointer
  resolver** `bt_resolve_parallel_scan` (`OffsetToPointer(parallel_scan,
  ps_offset_am)`), the only genuinely-foreign operation; it still panics until
  the parallel index-scan infrastructure lands, so the serial path is
  unaffected.
- Foreign primitives reach their owners' seams: `lwlock_initialize`/
  `lwlock_acquire` (lwlock-seams), `condition_variable_init`/`_sleep`/
  `_signal`/`_broadcast`/`_cancel_sleep` (condition-variable-seams, three new
  decls), and `datum_estimate_space`/`datum_serialize`/`datum_restore` (new
  `backend-utils-adt-datum-seams`, owner `utils/adt/datum.c`). `add_size`'s
  overflow check is reproduced in-crate.
- The inward `backend-access-nbtree-nbtree-seams` (`bt_parallel_*`) now return
  `PgResult` and are installed by `init_seams()` to the projected core
  functions.

Re-audited the nine fixed functions from scratch against the C: state
transitions, lock acquire/release ordering relative to `ConditionVariableSleep`/
`Signal`/`Broadcast`, the `endscan → _bt_parallel_done` tail, the
`first`/`needPrimScan` early returns, and the serialize/restore datum-cursor
arithmetic all match.

## Seam / wiring audit

- Owned inward seam crate `backend-access-nbtree-nbtree-seams`: all four
  declarations installed by `init_seams()` (only `set()` calls), invoked by
  `seams-init::init_all()`. PASS.
- Outward seam calls are thin marshal+delegate; no branching/computation in any
  seam path. The new `bt_resolve_parallel_scan` is a pure pointer resolution.
- `add_size` overflow handling is in-crate (not a seam) — acceptable: a trivial
  checked add, not foreign logic.

## Verdict: PASS

Every function MATCH (or SEAMED on a genuinely-foreign callee). No MISSING /
PARTIAL / DIVERGES remain after the round-1 fix. Workspace builds; crate tests
pass.
