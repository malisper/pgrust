# Audit: backend-replication-slotfuncs

C source: `src/backend/replication/slotfuncs.c` (PostgreSQL 18.3, 938 LOC).
Port: `crates/backend-replication-slotfuncs/src/lib.rs`.
Method: re-derived from the C and the c2rust rendering, independent of the
port's comments and a green build.

## Function inventory

Enumerated every function definition in slotfuncs.c (statics + SQL entry
points). slotfuncs.c defines no inline header helpers of its own; the
`XLByteToSeg` / `XLogSegNoOffsetToRecPtr` / `XLogMBVarToSegs` /
`XLogRecPtrIsInvalid` / `LSN_FORMAT_ARGS` macros are header macros, ported as
private `fn`s and audited inline.

| # | C function (line) | Port | Verdict | Notes |
|---|---|---|---|---|
| 1 | `create_physical_replication_slot` (35, static) | `create_physical_replication_slot` | MATCH | `Assert(!MyReplicationSlot)`→`debug_assert!(!my_replication_slot_is_set())`. `ReplicationSlotCreate(name,false,temporary?RS_TEMPORARY:RS_PERSISTENT,false,false,false)` — db_specific=false so the added `my_database_id` arg is `InvalidOid` (slot.c ignores it when !db_specific, line 439). `immediately_reserve` branch: `XLogRecPtrIsInvalid(restart_lsn)`→ReserveWal else set restart_lsn; then MarkDirty+Save. |
| 2 | `pg_create_physical_replication_slot` (64) | `pg_create_physical_replication_slot` | MATCH | get_call_result_type≠COMPOSITE assertion elided (catalog-typed; fmgr-dispatch builds the record — repo convention, cf. xlogfuncs/walsummaryfuncs). CheckSlotPermissions(mcx,GetUserId)/CheckSlotRequirements(wal_level). create_physical(...,InvalidXLogRecPtr=0). values[0]=name; values[1]=restart_lsn iff immediately_reserve else NULL. Release. |
| 3 | `create_logical_replication_slot` (116, static) | `create_logical_replication_slot` | MATCH | ReplicationSlotCreate(name,true,temporary?RS_TEMPORARY:RS_EPHEMERAL,two_phase,failover,false,MyDatabaseId). CreateInitDecodingContext(plugin,NIL,need_full_snapshot=false,restart_lsn,XL_ROUTINE local-read=default handle,NULL/NULL/NULL write/progress=false,wal_level,wal_segment_size,MyDatabaseId). find_startpoint→DecodingContextFindStartpoint. FreeDecodingContext. |
| 4 | `pg_create_logical_replication_slot` (168) | `pg_create_logical_replication_slot` | MATCH | assertion elided. CheckSlotPermissions; CheckLogicalDecodingRequirements(wal_level,MyDatabaseId). create_logical(...,InvalidXLogRecPtr,find_startpoint=true). values[0]=name,values[1]=confirmed_flush, nulls all false. `!temporary`→Persist. Release. |
| 5 | `pg_drop_replication_slot` (217) | `pg_drop_replication_slot` | MATCH | CheckSlotPermissions; CheckSlotRequirements; ReplicationSlotDrop(name,true). PG_RETURN_VOID→`Ok(())`. |
| 6 | `pg_get_replication_slots` (235) | `pg_get_replication_slots` | MATCH | SRF. InitMaterializedSRF(fcinfo,0); currlsn=GetXLogWriteRecPtr. Control-lock slot-array walk delegated to slot.c-owned `snapshot_all_slots` (in_use filter + per-slot spinlock copy inside). 20-column build: field order, every branch (database==InvalidOid plugin/type/datoid; active_pid; xmin/catalog_xmin/restart_lsn/confirmed_flush invalid tests; WALAvailability switch incl. WALAVAIL_REMOVED re-read of (active_pid,restart_lsn) via `reread_slot_active_pid_and_restart_lsn`; safe_wal_size arithmetic XLByteToSeg/XLogMBVarToSegs/Max/+1/XLogSegNoOffsetToRecPtr, failLSN-currlsn as int8; two_phase/two_phase_at; inactive_since>0; conflicting (physical→null else HORIZON/WAL_LEVEL); invalidation_reason; failover; synced). `i` reaches exactly 20 (39 i++ in C == 39 i+=1 in port; debug_assert_eq!(i,20)). putvalues per row; `(Datum)0`→`Datum::null()`. |
| 7 | `pg_physical_replication_slot_advance` (464, static) | `pg_physical_replication_slot_advance` | MATCH | startlsn=restart_lsn; Assert(moveto!=Invalid)→debug_assert. `startlsn<moveto`: spinlocked set restart_lsn=moveto (`set_my_slot_restart_lsn_locked`), retlsn=moveto, MarkDirty, PhysicalWakeupLogicalWalSnd(). returns retlsn. |
| 8 | `pg_logical_replication_slot_advance` (500, static) | `pg_logical_replication_slot_advance` | MATCH | `LogicalSlotAdvanceAndCheckSnapState(moveto,NULL)` → `(moveto,None,wal_segment_size,MyDatabaseId)` (the extra args are the foreign per-backend globals threaded by the repo's logical.c contract). |
| 9 | `pg_replication_slot_advance` (509) | `pg_replication_slot_advance` | MATCH | Assert(!MyReplicationSlot). CheckSlotPermissions. XLogRecPtrIsInvalid(moveto)→ERRCODE_INVALID_PARAMETER_VALUE "invalid target WAL LSN". get_call_result_type assertion elided. clamp: !RecoveryInProgress→Min(moveto,GetFlushRecPtr) else Min(moveto,GetXLogReplayRecPtr). Acquire(name,true,true). restart_lsn invalid→ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE "...cannot be advanced"+errdetail. minlsn=OidIsValid(database)?confirmed_flush:restart_lsn. moveto<minlsn→"cannot advance...minimum is..." with LSN_FORMAT_ARGS. dispatch logical/physical. values[0]=name. ComputeRequiredXmin(false)+ComputeRequiredLSN. Release. values[1]=endlsn. |
| 10 | `copy_replication_slot` (603, static) | `copy_replication_slot` | MATCH | assertion elided. CheckSlotPermissions. logical?CheckLogicalDecodingRequirements:CheckSlotRequirements. Control-lock src search via `snapshot_slot_by_name` (strcmp NameStr match, per-slot spinlock copy); None→ERRCODE_UNDEFINED_OBJECT "does not exist". src_islogical/src_restart_lsn/temporary/plugin(logical?). type-mismatch→FEATURE_NOT_SUPPORTED (both message variants). invalid src_restart_lsn→PREREQ "doesn't reserve WAL". invalidated→PREREQ "invalidated...". PG_NARGS overrides (Option args). create logical(...,src_restart_lsn,find_startpoint=false)/physical(...,true,...,src_restart_lsn). Second spinlocked read `reread_slot_snapshot(slotno)`: copy_* fields. recheck: copy_restart_lsn<src||islogical mismatch||name changed→"could not copy"+errdetail. src_islogical&&invalid confirmed_flush→FEATURE_NOT_SUPPORTED "unfinished"+errhint. invalidated→"cannot copy"+errdetail. install copied values under MyReplicationSlot spinlock. MarkDirty+ComputeRequiredXmin(false)+ComputeRequiredLSN+Save. USE_ASSERT_CHECKING block under cfg(debug_assertions): XLByteToSeg+XLogGetLastRemovedSegno<segno. logical&&!temporary→Persist. values[0]=dst_name; values[1]=confirmed_flush iff valid else NULL. Release. |
| 11 | `pg_copy_logical_replication_slot_a/b/c` (860/866/872) | `pg_copy_logical_replication_slot_a/b/c` | MATCH | `copy_replication_slot(...,logical=true)`; a=2-arg(None,None), b=3-arg(Some(temp),None), c=4-arg(Some(temp),Some(plugin)). The C 3 logical wrappers all call `copy(true)` and differ only by SQL arity; the Option args carry PG_NARGS. |
| 12 | `pg_copy_physical_replication_slot_a/b` (878/884) | `pg_copy_physical_replication_slot_a/b` | MATCH | `copy_replication_slot(...,logical=false)`; a=2-arg, b=3-arg(Some(temp)). |
| 13 | `pg_sync_replication_slots` (894) | `pg_sync_replication_slots` | MATCH | CheckSlotPermissions. !RecoveryInProgress→PREREQ "can only be synchronized to a standby server". ValidateSlotSyncParams(ERROR) (`elevel=ERROR.0`; the seam raises when elevel>=ERROR). load_file("libpqwalreceiver",false). CheckAndGetDbnameFromConninfo (discarded). app_name = cluster_name[0]? "%s_slotsync":"slotsync". walrcv_connect(PrimaryConnInfo,replication=false,logical=false,must_use_password=false,app_name); !wrconn→CONNECTION_FAILURE "...could not connect...: %s". SyncReplicationSlots(wrconn). walrcv_disconnect(wrconn). |

Helper macros (header, ported as private fns): `xlog_rec_ptr_is_invalid`
(==0), `xlbyte_to_seg` (÷wal_segsz), `xlog_segno_offset_to_rec_ptr`
(seg*sz+off), `xlog_mb_var_to_segs` (mb÷(sz÷1MB)), `lsn_format` (`%X/%X`,
high32/low32 uppercase no-pad). All MATCH; `lsn_format` + the segment
arithmetic covered by unit tests.

## Seam / wiring audit

**Owned seam crates:** none. The only C caller of any slotfuncs.c function
besides the SQL catalog (system_functions.sql → fmgr dispatch) is slotfuncs.c
itself; no in-tree C unit calls these by name, so there is no cycle partner and
no `backend-replication-slotfuncs-seams` crate. Consequently the crate has no
`init_seams()` and seams-init is unchanged. (Verified: grep of the C backend
tree found no external callers; the old src-idiomatic `seams/` decls for this
unit were the *outward* per-field stand-ins of a different model, not inward.)

**Direct (acyclic) calls** — justified because none of these owners call
slotfuncs:
- `backend_replication_slot` — lifecycle (Create/Acquire/Release/Drop/Reserve
  Wal/MarkDirty/Save/Persist/ComputeRequired{Xmin,LSN}), CheckSlot{Permissions,
  Requirements}, GetSlotInvalidationCauseName, and the new pub
  snapshot/MyReplicationSlot accessors. The control-lock + per-slot-spinlock
  substrate stays slot.c-owned (snapshots are taken *inside* slot.c under its
  locks); slotfuncs only orchestrates. This is the correct ownership boundary,
  not seam-able branching.
- `backend_replication_logical_logical` — CreateInitDecodingContext,
  DecodingContextFindStartpoint, FreeDecodingContext,
  LogicalSlotAdvanceAndCheckSnapState, CheckLogicalDecodingRequirements.
- `backend_replication_walsender` — PhysicalWakeupLogicalWalSnd.

**Outward seams** (call through the owner's `-seams`; panic until the owner
installs them — the owners are unported or have not advanced their contract):
- xlog (`backend-access-transam-xlog-seams`): wal_level, wal_segment_size,
  recovery_in_progress, get_flush_rec_ptr, get_xlog_replay_rec_ptr,
  xlog_get_last_removed_segno (pre-existing) + four newly-declared **xlog-owned**
  decls (get_xlog_write_rec_ptr, get_wal_availability, max_slot_wal_keep_size_mb,
  wal_keep_size_mb). These are declared in the xlog owner's seam crate (correct
  owner) and installed by xlog when it lands; slotfuncs does not install them.
- funcapi (`-funcapi-seams`): InitMaterializedSRF, materialized_srf_putvalues,
  cstring_get_text_datum — thin marshal, no logic in the seam path.
- slotsync, libpqwalreceiver, dfmgr(load_file), guc(cluster_name),
  miscinit(get_user_id), small-init(my_database_id), xlogrecovery
  (primary_conninfo) — all thin reads/delegates.

No branching, node construction, or computation lives on any seam path; every
adapter is marshal+delegate. No `set()` outside an owner. No uninstalled seam
owned by this crate (it owns none).

## Design conformance

- No invented opacity / type-alias stand-ins (grep clean). `SlotNameLsnRow`
  carries real typed fields (`NameData`, `Option<XLogRecPtr>`).
- No `&[u8]` byte-blobs in pub signatures; names are `&str`, the SRF builds real
  `NameData` Datums (`name_datum` → `Datum::ByRef` of the 64-byte image).
- Allocations: `format!`/`to_string` occur only in error-message construction at
  return-Err sites (justified) and the app_name build in pg_sync (mirrors the C
  StringInfo, on an ereport-bearing path). `mcx::slice_in` is the fallible mcx
  pattern; `core::array::from_fn(Datum::null)` is a stack array (C stack array,
  no alloc). No infallible palloc-shaped allocation on a hot path.
- No shared statics / Atomics / Mutex (per-backend `MyReplicationSlot` is the
  slot crate's thread_local, accessed via its accessors).
- No zero-arg global getter seams introduced beyond the established GUC-read
  convention already used repo-wide.
- No `unwrap`/`panic!`/`unreachable!`/`todo!` in owned logic (grep clean); the
  one `.expect("InitMaterializedSRF set fcinfo->resultinfo")` mirrors the C
  unconditional `(ReturnSetInfo *) fcinfo->resultinfo` cast (the SRF contract
  guarantees it) — same as the walsummaryfuncs precedent.
- Locks: no lock is acquired in this crate; the control-lock/spinlock pairs are
  acquire+release-balanced *inside* the slot.c-owned snapshot helpers (the
  `WALAVAIL_REMOVED` re-read and copy reread are single spinlocked reads with
  matched release).
- No unledgered divergence markers (grep for for-now/simplified/hack/FIXME/TODO
  clean).

## Verdict: PASS

Every function MATCH; no MISSING/PARTIAL/DIVERGES; zero seam findings; design
conformance clean. The crate may merge.
