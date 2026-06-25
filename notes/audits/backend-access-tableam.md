# Audit: backend-access-tableam

Unit: `backend-access-tableam` (`src/backend/access/table/tableam.c`, 761
lines, plus the companion `src/backend/access/table/table.c`, 148 lines,
PostgreSQL 18.3).
Crates audited: `crates/backend-access-table-tableam`,
`crates/backend-access-table-table`, `crates/backend-access-table-table-seams`,
and the new seam/type crates the port introduced
(`backend-access-common-relation-seams`, `backend-catalog-pg-class-seams`,
`backend-utils-time-snapmgr-seams`, `backend-access-common-syncscan-seams`,
`backend-storage-smgr-seams`, `backend-storage-buffer-bufmgr-seams`,
`backend-optimizer-util-plancat-seams`, `backend-optimizer-path-costsize-seams`,
`types-snapshot`, `types-tableam`).
Cross-checked against `../pgrust/c2rust-runs/backend-access-tableam/src/tableam.rs`.
Auditor: independent re-derivation from the C sources and headers
(`access/tableam.h`, `access/relscan.h`, `access/htup_details.h`,
`utils/rel.h`, `nodes/lockoptions.h`, `catalog/pg_class.h`,
`common/relpath.h`, `storage/block.h`, `port/pg_bitutils.h`,
`utils/snapshot.h`, `utils/errcodes.txt`, `storage/ipc/shmem.c`).

## Function inventory — tableam.c (every definition)

| # | C function (tableam.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `table_slot_callbacks` (:58) | `tableam/src/lib.rs::table_slot_callbacks` | MATCH | Three arms in C order: `rd_tableam` present → AM's `slot_callbacks`; `relkind == RELKIND_FOREIGN_TABLE` ('f') → heap slot; else `Assert(VIEW \|\| PARTITIONED_TABLE)` (`debug_assert`) → virtual slot. `TTSOpsHeapTuple`/`TTSOpsVirtual` cross as `TupleSlotKind::{HeapTuple,Virtual}`. RELKIND chars verified against pg_class.h. |
| 2 | `table_slot_create` (:91) | `::table_slot_create` | MATCH | `table_slot_callbacks` + `MakeSingleTupleTableSlot(RelationGetDescr, tts_cb)` (execTuples seam). C's optional `*reglist = lappend(*reglist, slot)` becomes caller ownership of the returned slot (documented); identical for every input — registration is the caller's list either way. |
| 3 | `table_beginscan_catalog` (:112) | `::table_beginscan_catalog` | MATCH | flags = `SO_TYPE_SEQSCAN \| SO_ALLOW_STRAT \| SO_ALLOW_SYNC \| SO_ALLOW_PAGEMODE \| SO_TEMP_SNAPSHOT` (bit values 1<<0,6,7,8,9 verified against tableam.h); `RegisterSnapshot(GetCatalogSnapshot(relid))` via snapmgr seams; `rd_tableam->scan_begin(rel, snapshot, nkeys, key, NULL, flags)`. Unconditional `rd_tableam` deref → `am()` panic (C NULL-deref). |
| 4 | `table_parallelscan_estimate` (:130) | `::table_parallelscan_estimate` | MATCH | `IsMVCCSnapshot` → `add_size(0, EstimateSnapshotSpace)`; else `Assert(snapshot == SnapshotAny)` (`debug_assert!(is_none())` — `None` is the SnapshotAny identity); then `add_size(sz, parallelscan_estimate(rel))`. `add_size` re-derived from shmem.c: overflow check, ERRCODE_PROGRAM_LIMIT_EXCEEDED (54000), exact message. |
| 5 | `table_parallelscan_initialize` (:145) | `::table_parallelscan_initialize` | MATCH | `phs_snapshot_off` from AM's `parallelscan_initialize`; MVCC → `SerializeSnapshot` (bytes stored as `phs_snapshot_data`, the owned stand-in for "at offset in the DSM chunk") + `phs_snapshot_any = false`; else assert SnapshotAny + `phs_snapshot_any = true`. |
| 6 | `table_beginscan_parallel` (:165) | `::table_beginscan_parallel` | MATCH | flags without SO_TEMP_SNAPSHOT initially; `Assert(RelFileLocatorEquals(rd_locator, phs_locator))` → `debug_assert`; `!phs_snapshot_any` → Restore + Register + `flags \|= SO_TEMP_SNAPSHOT`; else SnapshotAny (`None`). C ignores RegisterSnapshot's return but RestoreSnapshot's result is freshly allocated so Register returns it unchanged — using the return value is identical. `scan_begin(rel, snapshot, 0, NULL, pscan, flags)`. |
| 7 | `table_index_fetch_tuple_check` (:208) | `::table_index_fetch_tuple_check` | MATCH | slot create → fetch begin → `table_index_fetch_tuple` (with `call_again = false`) → fetch end → `ExecDropSingleTupleTableSlot` → return found. `tid` mutable (HOT note), `all_dead` passthrough. |
| 8 | `table_tuple_get_latest_tid` (:235) | `::table_tuple_get_latest_tid` | MATCH | Guard `TransactionIdIsValid(CheckXidAlive) && !bsysscan` → `elog(ERROR, "unexpected table_tuple_get_latest_tid call during logical decoding")` (xact seams, exact text); `!tuple_tid_valid` → ereport ERROR, ERRCODE_INVALID_PARAMETER_VALUE (22023, verified against errcodes.txt and the c2rust-encoded sqlstate), message `tid (%u, %u) is not valid for relation "%s"` with `ItemPointerGet{Block,Offset}NumberNoCheck` (raw field reads — `ip_blkid.block_number()`/`ip_posid`, no validity assert, matching NoCheck); then `tuple_get_latest_tid`. |
| 9 | `simple_table_tuple_insert` (:276) | `::simple_table_tuple_insert` | MATCH | `table_tuple_insert(rel, slot, GetCurrentCommandId(true), 0, NULL)`. |
| 10 | `simple_table_tuple_delete` (:290) | `::simple_table_tuple_delete` | MATCH | `table_tuple_delete(..., GetCurrentCommandId(true), snapshot, InvalidSnapshot (None), wait=true, &tmfd, changingPart=false)`; switch arms SelfModified/Ok/Updated/Deleted/default with exact elog texts incl. `unrecognized table_tuple_delete status: %u` (`as u32` — TM_Result discriminants verified against tableam.h order: Ok=0, Invisible, SelfModified, Updated, Deleted, BeingModified, WouldBlock). |
| 11 | `simple_table_tuple_update` (:335) | `::simple_table_tuple_update` | MATCH | As #10 with `&mut lockmode` and `update_indexes`; C leaves `lockmode` uninitialized before the call (pure out-param), port pre-seeds `LockTupleNoKeyExclusive` (=2, verified against lockoptions.h) — unobservable since the AM writes it. Exact elog texts. |
| 12 | `table_block_parallelscan_estimate` (:382) | `::table_block_parallelscan_estimate` | MATCH | C: `sizeof(ParallelBlockTableScanDescData)`; port returns the size of its own (base + block-ext) descriptor — the value is the DSM space estimate for the descriptor this implementation actually uses; semantically identical role. |
| 13 | `table_block_parallelscan_initialize` (:388) | `::table_block_parallelscan_initialize` | MATCH | `phs_locator = rd_locator`; `phs_nblocks = RelationGetNumberOfBlocks` (= `RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM)`, bufmgr seam); `phs_syncscan = synchronize_seqscans && !RelationUsesLocalBuffers && phs_nblocks > NBuffers/4` (RelationUsesLocalBuffers re-derived: `relpersistence == RELPERSISTENCE_TEMP` 't'; int division then u32 compare, same conversions); SpinLockInit + `phs_startblock = InvalidBlockNumber` + atomic 0 == fresh `ParallelBlockTableScanExt::default()` (default verified: `Mutex::new(InvalidBlockNumber)`, `AtomicU64::new(0)`); returns the descriptor size. |
| 14 | `table_block_parallelscan_reinitialize` (:406) | `::table_block_parallelscan_reinitialize` | MATCH | `pg_atomic_write_u64(&phs_nallocated, 0)` → `store(0)`. |
| 15 | `table_block_parallelscan_startblock_init` (:421) | `::table_block_parallelscan_startblock_init` | MATCH | memset of worker state → `Default`; compile-time `MaxBlockNumber <= 0xFFFFFFFE` assert kept; `phsw_chunk_size = pg_nextpower2_32(Max(phs_nblocks/2048, 1))` then `Min(.., 8192)` (constants 2048/64/8192 verified); `pg_nextpower2_32` re-derived from pg_bitutils.h (power-of-2 identity, else `1 << (31 - clz + 1)`) and unit-tested. retry loop: lock (Mutex == spinlock over `phs_startblock`), if uninitialized: non-sync → 0; `sync_startpage` cached → use it; else release, `ss_get_location(rel, phs_nblocks)` (syncscan seam), goto retry — exact goto shape via `loop`/`continue`/`break`, lock released on every exit. |
| 16 | `table_block_parallelscan_nextpage` (:491) | `::table_block_parallelscan_nextpage` | MATCH | Re-derived against C and c2rust line-by-line. `chunk_remaining > 0` arm: `nallocated = ++phsw_nallocated; chunk_remaining--`. Else arm: ramp-down predicate `chunk_size > 1 && phsw_nallocated > phs_nblocks - chunk_size*64` with the subtraction in wrapping u32 arithmetic then widened to u64 — exactly C's BlockNumber arithmetic (port replicates with `wrapping_sub`/`wrapping_mul` then `as u64`); `fetch_add(chunk_size)`; `chunk_remaining = chunk_size - 1`. Page: `nallocated >= phs_nblocks as u64` → InvalidBlockNumber else `(nallocated + startblock) % nblocks` in u64, truncated to BlockNumber. Syncscan report: current page, or startblock exactly once when `nallocated == phs_nblocks`. |
| 17 | `table_block_relation_size` (:616) | `::table_block_relation_size` | MATCH | `InvalidForkNumber` (−1) → sum over `0..MAX_FORKNUM` (loop bound `i < MAX_FORKNUM` — PG's own quirk of excluding INIT_FORKNUM — preserved; MAX_FORKNUM = INIT_FORKNUM = 3 verified against relpath.h and c2rust); else single fork. `RelationGetSmgr(rel)` becomes the `(rd_locator, rd_backend)` pair keyed by `smgropen` (the cache/pin is the smgr owner's concern, no logic lost); `nblocks * BLCKSZ` (8192) in u64. |
| 18 | `table_block_relation_estimate_size` (:653) | `::table_block_relation_estimate_size` | MATCH | curpages from bufmgr seam; relpages/reltuples/relallvisible coerced as in C (int32→BlockNumber, float4→double); 10-page HACK predicate `curpages < 10 && reltuples < 0 && !relhassubclass`; `*pages`; empty quick-exit zeroing tuples/allvisfrac; density branch `reltuples >= 0 && relpages > 0` → ratio; else `RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR)` (100, verified utils/rel.h:360, via relcache seam), `get_rel_data_width` (plancat seam), `tuple_width += overhead` in Size arithmetic truncated back to int32 (`wrapping_add ... as i32`, matches c2rust), integer division `(usable * fillfactor / 100) / tuple_width` in Size arithmetic, `clamp_row_est` (costsize seam); `*tuples = rint(density * curpages)` → `round_ties_even` (rint under default FE_TONEAREST); allvisfrac three-way with `curpages <= 0` ≡ `== 0` for unsigned and the `(double)` comparisons preserved. |

GUC globals: `default_table_access_method` (default `DEFAULT_TABLE_ACCESS_METHOD`
= "heap", verified tableam.h:29) and `synchronize_seqscans` (default true) are
`thread_local` with get/set accessors — MATCH (backend-local state).

## Function inventory — tableam.h inlines this unit instantiates

The c2rust rendering instantiates these static inlines inside the unit; the
catalog row claims them for this crate.

| # | C function (tableam.h) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 19 | `table_index_fetch_begin` (:1156) | `::table_index_fetch_begin` | MATCH | `rd_tableam->index_fetch_begin(rel)`. |
| 20 | `table_index_fetch_end` (:1175) | `::table_index_fetch_end` | MATCH | `scan->rel->rd_tableam->index_fetch_end(scan)`; descriptor consumed by value (C frees it inside the AM). |
| 21 | `table_index_fetch_tuple` (:1205) | `::table_index_fetch_tuple` | MATCH | CheckXidAlive/!bsysscan guard with exact elog text `unexpected table_index_fetch_tuple call during logical decoding`, then vtable dispatch with all six args. |
| 22 | `table_tuple_insert` (:1366) | `::table_tuple_insert` | MATCH | Pure dispatch, no guard (verified in header). |
| 23 | `table_tuple_delete` (:1455) | `::table_tuple_delete` | MATCH | Pure dispatch, 8 args in order, no guard. |
| 24 | `table_tuple_update` (:1499) | `::table_tuple_update` | MATCH | Pure dispatch, 10 args in order, no guard. |

Other c2rust-instantiated helpers: `pg_nextpower2_32`/`pg_leftmost_one_pos32`
(re-derived + tested, see #15), `BlockIdGetBlockNumber`/`ItemPointerGet*NoCheck`
(types-tuple `BlockIdData::block_number` / raw field reads), `RelationGetSmgr`
(see #17), `pg_atomic_*`/`tas`/`s_lock` (C spinlock/atomic substrate →
`Mutex`/`AtomicU64`), `add_size` (see #4). All accounted for; none MISSING.

## Function inventory — table.c (every definition)

| # | C function (table.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 25 | `table_open` (:39) | `table/src/lib.rs::table_open` | MATCH | `relation_open` (relation.c seam) + `validate_relation_kind`. |
| 26 | `try_table_open` (:59) | `::try_table_open` | MATCH | `try_relation_open`; NULL → `Ok(None)` early return; else validate. |
| 27 | `table_openrv` (:82) | `::table_openrv` | MATCH | `relation_openrv` + validate. |
| 28 | `table_openrv_extended` (:102) | `::table_openrv_extended` | MATCH | `relation_openrv_extended(missing_ok)`; validate only `if (r)`. |
| 29 | `table_close` (:125) | `::table_close` | MATCH | Delegates to `relation_close`. |
| 30 | `validate_relation_kind` (:137, static inline) | `::validate_relation_kind` | MATCH | `relkind == RELKIND_INDEX ('i') \|\| RELKIND_PARTITIONED_INDEX ('I') \|\| RELKIND_COMPOSITE_TYPE ('c')` → ereport ERROR, ERRCODE_WRONG_OBJECT_TYPE (42809, verified), `cannot open relation "%s"`, detail from `errdetail_relkind_not_supported` (pg_class seam, returns the detail string for attachment — thin marshal). |

## Seam audit

Outward seams (all target unported owner units; each call is thin
marshal + one delegate + result conversion, no branching/logic in seam paths):

- `backend-utils-cache-relcache-seams` — added pure field reads
  (`relation_relkind`, `relation_name`, `relation_rd_tableam`,
  `relation_rd_locator`, `relation_rd_backend`, `relation_relpersistence`,
  `relation_relpages`, `relation_reltuples`, `relation_relallvisible`,
  `relation_relhassubclass`, `relation_get_fillfactor`). Declaration-only.
- `backend-access-transam-xact-seams` — added `get_current_command_id`,
  `check_xid_alive`, `bsysscan` (globals/calls used by the guards).
- `backend-executor-execTuples-seams` — added `make_single_tuple_table_slot`,
  `exec_drop_single_tuple_table_slot`.
- `backend-utils-init-small-seams` — added `nbuffers` (NBuffers global).
- New seam crates (declaration-only, owner unported, panic-until-installed):
  `backend-access-common-relation-seams` (5 fns),
  `backend-catalog-pg-class-seams` (`errdetail_relkind_not_supported`),
  `backend-utils-time-snapmgr-seams` (5 fns),
  `backend-access-common-syncscan-seams` (`ss_get_location`,
  `ss_report_location`), `backend-storage-smgr-seams` (`smgrnblocks`),
  `backend-storage-buffer-bufmgr-seams`
  (`relation_get_number_of_blocks_in_fork`),
  `backend-optimizer-util-plancat-seams` (`get_rel_data_width`),
  `backend-optimizer-path-costsize-seams` (`clamp_row_est`).

Owned seams and wiring:

- `backend-access-table-table-seams` declares `table_open`/`table_close`
  (pre-existing consumer: `backend-utils-misc-queryenvironment`).
  `backend_access_table_table::init_seams()` contains exactly the two
  `set()` calls; no `set()` on these seams anywhere else (grepped).
- `backend_access_table_tableam::init_seams()` is empty (no crate declares
  tableam-owned seams yet) — correct.
- `seams-init::init_all()` calls both crates' `init_seams()`.
- No function body was replaced by a seam call to its own logic; every seam
  target is a genuinely different C translation unit.

Carrier types spot-verified against headers: `SO_*` ScanOptions bits,
`TM_Result`/`TU_UpdateIndexes` discriminant order, `LockTupleMode` values,
`SnapshotType` order and `IsMVCCSnapshot` (MVCC ∨ HISTORIC_MVCC),
`RelFileLocator`/`RelFileLocatorEquals`, `InvalidBlockNumber`/`MaxBlockNumber`,
fork numbers, `BLCKSZ`, `TransactionIdIsValid`. All exact.

## Build and tests

`cargo build --workspace` clean; `cargo test -p backend-access-table-tableam
-p backend-access-table-table` passes (3 tests: `pg_nextpower2_32` vector,
`add_size` overflow, GUC defaults).

## Verdict

**PASS** — all 30 functions MATCH (with documented owned-model
restructurings that are behaviorally identical); seam wiring clean; zero
findings. Fix rounds: 0.
