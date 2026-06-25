# Audit: backend-replication-logical-origin (`origin.c`)

Unit: `backend-replication-logical-origin`
C source: `src/backend/replication/logical/origin.c` (1607 lines, 31 functions)
Crates: `backend-replication-logical-origin`, `backend-replication-logical-origin-extern-seams`,
inward-seam crate `backend-replication-logical-origin-seams`.
c2rust cross-check: `c2rust-runs/backend-replication-logical-origin/src/origin.rs`.

Verdict: **PASS** (after one fix round — see Seam findings).

## Constants verified against headers

| Constant | Port value | Header / source | OK |
| --- | --- | --- | --- |
| `XLOG_REPLORIGIN_SET` | `0x00` | `replication/origin.h:30` | yes |
| `XLOG_REPLORIGIN_DROP` | `0x10` | `replication/origin.h:31` | yes |
| `InvalidRepOriginId` | `0` | `replication/origin.h:33` | yes |
| `DoNotReplicateId` | `PG_UINT16_MAX` (`u16::MAX`) | `replication/origin.h:34` | yes |
| `MAX_RONAME_LEN` | `512` | `replication/origin.h:41` | yes |
| `REPLICATION_STATE_MAGIC` | `0x1257DADE` | `origin.c:187` | yes |
| `RM_REPLORIGIN_ID` | `19` | `access/rmgrlist.h` (0-based count of `PG_RMGR`, REPLORIGIN is entry 19) | yes |
| `LOGICALREP_ORIGIN_NONE`/`_ANY` | `"none"`/`"any"` | `catalog/pg_subscription.h:156,162` | yes |
| `PG_LOGICAL_DIR` (checkpoint path) | `"pg_logical"` | `replication/reorderbuffer.h:22` | yes |
| `WAIT_EVENT_REPLICATION_ORIGIN_DROP` | `PG_WAIT_IPC \| 0x30` | `PG_WAIT_IPC=0x08000000U` (`wait_classes.h:24`); IPC event 0-based index 48 = `0x30` (alphabetical position in `wait_event_names.txt`; ABI marker follows all listed events so no gap) | yes |
| `DEFAULT_MAX_ACTIVE_REPLICATION_ORIGINS` | `10` | `guc_tables.c:3396` (PGC_POSTMASTER) | yes |

## Per-function table

| # | C function (line) | Port location | Verdict | Notes |
| --- | --- | --- | --- | --- |
| 1 | `replorigin_check_prerequisites` (190) | lib.rs:194 | MATCH | Both ereports: max==0 -> ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE; `!recoveryOK && RecoveryInProgress()` -> ERRCODE_READ_ONLY_SQL_TRANSACTION. `RecoveryInProgress` seamed. |
| 2 | `IsReservedOriginName` (209) | lib.rs:170 | MATCH | `pg_strcasecmp` vs the two literals; `strcaseeq` is ASCII case-insensitive equality (only compared vs lowercase literals). |
| 3 | `replorigin_by_name` (226) | lib.rs:217 | SEAMED+MATCH | `syscache_roident_by_name` seam; missing+!ok -> ERRCODE_UNDEFINED_OBJECT; InvalidOid when missing_ok. |
| 4 | `replorigin_create` (257) | lib.rs:238 | SEAMED+MATCH | >512 -> ERRCODE_PROGRAM_LIMIT_EXCEEDED + errdetail kept in-crate; dirty-snapshot free-id scan + insert + CCI is the catalog/genam composite (`create_catalog_insert` -> `Option<Oid>`); None -> "could not find free replication origin ID" (ERRCODE_PROGRAM_LIMIT_EXCEEDED). |
| 5 | `replorigin_state_clear` (369) | lib.rs:270 | MATCH | `goto restart` -> `'restart: loop`. busy+nowait -> ERRCODE_OBJECT_IN_USE without releasing lock (error unwind releases). busy+wait -> release, untimed CV sleep (timeout -1), continue. WAL-drop seam + clear roident/remote/local; trailing release + cancel-sleep. |
| 6 | `replorigin_drop_by_name` (439) | lib.rs:337 | SEAMED+MATCH | open(RowExclusive) -> by_name -> LockSharedObject(AccessExclusive). missing tuple: !ok -> elog cache-lookup-failed; ok -> unlock+close+return. else state_clear -> delete+CCI -> close(NoLock). Catalog/lmgr legs seamed; control flow matches. |
| 7 | `replorigin_by_oid` (493) | lib.rs:380 | SEAMED+MATCH | debug asserts on roident; found -> name; missing+!ok -> ERRCODE_UNDEFINED_OBJECT; missing+ok -> None. `syscache_roname_by_oid` seam. |
| 8 | `ReplicationOriginShmemSize` (534) | lib.rs:400 | MATCH (note) | max==0 -> 0; else proportional to `max * size_of::<ReplicationState>()`. Drops C `offsetof(ReplicationStateCtl,states)` (=8 in c2rust). No consumer uses the byte count (real allocation is the count-sized `Vec` in ShmemInit), and `size_of::<ReplicationState>()` != C sizeof in this model, so a byte-exact match is neither possible nor meaningful; zero-case + proportionality preserved. |
| 9 | `ReplicationOriginShmemInit` (549) | lib.rs:416 | MATCH | max==0 early return; n zeroed entries (`try_reserve` -> OOM Err), `LWLockInitialize(tranche)` + `ConditionVariableInit` each; published once via `OnceLock` (double-publish panics, mirroring single shmem carve / `!found`). |
| 10 | `CheckPointReplicationOrigin` (596) | lib.rs:455 | SEAMED+MATCH | max==0 return; SHARED origin lock; skip Invalid; per-slot SHARED snapshot of `(roident,remote,local)`, `XLogFlush(local)`; release; hand `(roident,remote)` to `checkpoint_write` seam (unlink/open/magic/CRC/durable_rename = transient I/O). |
| 11 | `StartupReplicationOrigin` (722) | lib.rs:514 | SEAMED+MATCH (note) | USE_ASSERT_CHECKING `already_started` one-shot (debug thread_local). max==0 return. `checkpoint_read`: `Ok(None)` = ENOENT return; else decoded `(roident,remote)`; `last_state==max` -> PANIC + ERRCODE_CONFIGURATION_LIMIT_EXCEEDED; copy each into array. Dropped `elog(DEBUG2)` + per-state `elog(LOG)` are informational (no control-flow/return effect) -> MATCH per sibling audits (e.g. standby). Magic/CRC verify inside reader seam. |
| 12 | `replorigin_redo` (850) | lib.rs:556 | MATCH (inward seam owner) | `info = GetInfo & ~XLR_INFO_MASK`; SET -> decode + `replorigin_advance(node,remote,EndRecPtr,force,false)`; DROP -> decode + linear clear of matching slot; default -> PANIC "unknown op code". Decoders reproduce C byte layout (set: remote@0..8,node@8..10,force@10; drop: node@0..2; native-endian) — verified by tests. |
| 13 | `replorigin_advance` (911) | lib.rs:641 | SEAMED+MATCH | DoNotReplicateId early return; EXCLUSIVE origin lock; slot/free-slot search; found+acquired!=0 -> ERRCODE_OBJECT_IN_USE (holding both locks; error unwinds); both-NULL -> ERRCODE_CONFIGURATION_LIMIT_EXCEEDED + hint; new-slot init under its lock; `wal_log` -> `wal_insert_replorigin_set` seam; LSN predicates `go_backward || cur<new` (+ `local!=Invalid`) exact; release slot then origin lock. |
| 14 | `replorigin_get_progress` (1037) | lib.rs:758 | SEAMED+MATCH | SHARED origin lock; linear search; per-slot SHARED snapshot; release; `flush && local!=Invalid` -> `XLogFlush`. |
| 15 | `ReplicationOriginExitCleanup` (1078) | lib.rs:796 | MATCH | NULL session -> return; EXCLUSIVE lock; if `acquired_by==MyProcPid` clear + null cache + remember CV; release; broadcast if set. |
| 16 | `replorigin_session_setup` (1120) | lib.rs:828 | SEAMED+MATCH | `registered_cleanup` one-shot -> `on_shmem_exit` seam; already-setup -> ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE; EXCLUSIVE lock; search with `acquired_by!=0 && acquired_by==0` busy branch -> ERRCODE_OBJECT_IN_USE; both-NULL -> ERRCODE_CONFIGURATION_LIMIT_EXCEEDED + hint; new-slot init; `acquired_by==0` sets MyProcPid else mismatch elog; release; broadcast. |
| 17 | `replorigin_session_reset` (1213) | lib.rs:934 | MATCH | NULL -> ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE; EXCLUSIVE lock; clear acquired_by + null cache; release; broadcast. |
| 18 | `replorigin_session_advance` (1242) | lib.rs:965 | MATCH (inward seam owner) | asserts non-NULL; per-slot EXCLUSIVE lock; advance local then remote with `<` guards; release. |
| 19 | `replorigin_session_get_progress` (1260) | lib.rs:990 | SEAMED+MATCH | per-slot SHARED snapshot; `flush && local!=Invalid` -> `XLogFlush`. |
| 20 | `pg_replication_origin_create` (1292) | lib.rs:1041 | MATCH | prereq(false,false); `IsReservedName`("pg_" prefix) or `IsReservedOriginName` -> ERRCODE_RESERVED_NAME + errdetail; ENFORCE_REGRESSION switch compiled out (off by default); `replorigin_create`. |
| 21 | `pg_replication_origin_drop` (1333) | lib.rs:1062 | MATCH | prereq(false,false); `replorigin_drop_by_name(name,false,true)`. |
| 22 | `pg_replication_origin_oid` (1352) | lib.rs:1070 | MATCH | prereq(false,false); by_name(true); OidIsValid -> Some else None. |
| 23 | `pg_replication_origin_session_setup` (1373) | lib.rs:1084 | MATCH | prereq(true,false); by_name(false) -> session_setup(origin,0); set `replorigin_session_origin`. |
| 24 | `pg_replication_origin_session_reset` (1395) | lib.rs:1097 | MATCH | prereq(true,false); session_reset; clear three session globals. |
| 25 | `pg_replication_origin_session_is_setup` (1412) | lib.rs:1111 | MATCH | prereq(false,false); `replorigin_session_origin != Invalid`. |
| 26 | `pg_replication_origin_session_progress` (1428) | lib.rs:1119 | MATCH | prereq(true,false); NULL session -> prereq error; `replorigin_session_get_progress(flush)`; Invalid -> None. |
| 27 | `pg_replication_origin_xact_setup` (1449) | lib.rs:1139 | MATCH | prereq(true,false); NULL session -> prereq error; set session lsn + timestamp. |
| 28 | `pg_replication_origin_xact_reset` (1467) | lib.rs:1159 | MATCH | prereq(true,false); clear session lsn + timestamp. |
| 29 | `pg_replication_origin_advance` (1479) | lib.rs:1170 | SEAMED+MATCH | prereq(true,false); `LockRelationOid(RowExclusive)` seam; by_name(false); `replorigin_advance(node,remote,Invalid,go_backward=true,wal_log=true)`; `UnlockRelationOid` seam. |
| 30 | `pg_replication_origin_progress` (1514) | lib.rs:1194 | MATCH | prereq(true,**true**); by_name(false); `replorigin_get_progress(roident,flush)`; Invalid -> None. recoveryOK=true matches C. |
| 31 | `pg_show_replication_origin_status` (1539) | lib.rs:1213 | SEAMED+MATCH | prereq(false,**true**); `InitMaterializedSRF` seam; SHARED origin lock; skip Invalid; col0=roident; col1=name via `replorigin_by_oid(...,true)` (None => nulls[1]); per-slot SHARED lock for remote/local; emit row via `put_replication_origin_status_row` seam; release. `REPLICATION_ORIGIN_PROGRESS_COLS=4` -> 4-field typed row. |

## Seam audit

### Extern (outward) seams — `-extern-seams`

Every outward seam is a real dependency on an unported owner, thin marshal+delegate:
catalog/syscache/heapam/genam (`syscache_*`, `create_catalog_insert`, `drop_*`), lmgr object/relation
locks, WAL (`wal_insert_replorigin_set/drop`, `XLogFlush`), xact/recovery predicates
(`IsTransactionState`, `RecoveryInProgress`), ipc (`register_origin_exit_cleanup`), SRF/tuplestore
(`InitMaterializedSRF`, `put_replication_origin_status_row`), checkpoint transient I/O + CRC32C
(`checkpoint_write`, `checkpoint_read`). `ReplicationOriginLock` is two seams (acquire/release) rather
than a scope guard because origin.c releases it mid-function and re-acquires across `goto restart`.
No branching/node-construction/computation in any seam path; origin logic (free-id loop control, LSN
predicates, slot search) stays in-crate. All `PgResult` where the C leg can `ereport`. No findings.
The per-entry `LWLock`/`ConditionVariable` use real ported primitives, not seams elsewhere. Correct.

### Inward seams — `-seams`

Declares **9** seams (functions this unit owns, consumed by xact / twophase / rmgr / conflict):
`replorigin_redo`, `replorigin_session_origin`, `replorigin_session_origin_lsn`,
`replorigin_session_origin_timestamp`, `set_replorigin_session_origin_timestamp`,
`set_replorigin_session_timestamp`, `replorigin_session_advance`, `replorigin_advance`,
`replorigin_by_oid`.

**FINDING (fixed this round).** Original `init_seams()` installed only `replorigin_redo`; the other
**8 declarations were uninstalled** — a skill section 3 finding ("an uninstalled seam ... is a
finding"). The logic for all of them lives in this crate, so consumers
(`backend-access-transam-xact`, `-twophase`, `-rmgr`, `backend-replication-logical-conflict`) would
hit the unfilled-seam panic despite present logic.

Fix: `init_seams()` now installs all 9 via `set()` only. Two thin marshal adapters are defined
*outside* `init_seams` (init body stays pure `set()` calls):
- `replorigin_by_oid_seam` — marshals this crate's `String` result into a caller-`mcx` `PgString`
  (mirroring C's `text_to_cstring` palloc'd in the calling context); OOM -> `Err`. (Added the `mcx`
  dependency.)
- `set_replorigin_session_timestamp_seam` — twophase's setter name for the same
  `replorigin_session_origin_timestamp` global write xact reaches via
  `set_replorigin_session_origin_timestamp`; delegates to that one setter.

`seams-init::init_all()` calls `backend_replication_logical_origin::init_seams()`
(seams-init/src/lib.rs:38). Re-audited from scratch after the fix: all 9 declarations installed,
init body is `set()`-only, build of the crate + all four consumers + `seams-init` is clean, crate
tests pass (5/5). No remaining seam findings.

## Design conformance (section 3b)

- Shared `replication_states[]` is a process-global `OnceLock<OriginShmem>` (AGENTS "Backend-global
  state") with real LWLock/CV per entry and atomics for the lock-protected scalar words — not a shared
  static for per-backend globals.
- Per-backend globals (GUC mirror, session cache index, three session-origin globals,
  `registered_cleanup`, debug `already_started`) are `thread_local!`. The GUC is PGC_POSTMASTER
  (constant per process); modeling it as a thread_local mirror with the published array length
  authoritative plus the `(i as usize) < n` loop guard follows the repo's GUC-mirror convention.
- Allocating paths use `try_reserve`/`PgResult` with `ERRCODE_OUT_OF_MEMORY`; the `mcx` copy in the
  `replorigin_by_oid` seam allocates in the caller's `Mcx` and returns `PgResult`.
- No registry-shaped side tables, no ambient-global seams. C's intentional hold of
  `ReplicationOriginLock`/slot lock at `ereport` (relying on `LWLockReleaseAll`) is mirrored faithfully.
  No unledgered divergence markers.

## Conclusion

After installing the 8 missing inward seams, every function is MATCH or SEAMED (per section 3 rules)
and there are zero remaining seam findings. **PASS.**
