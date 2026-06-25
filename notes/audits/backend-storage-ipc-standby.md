# Audit: backend-storage-ipc-standby

- **C source**: `src/backend/storage/ipc/standby.c` (PostgreSQL 18.3, 1524 lines)
- **c2rust**: `../pgrust/c2rust-runs/backend-storage-ipc-standby/src/standby.rs`
- **Port**: `crates/backend-storage-ipc-standby/src/lib.rs` (branch `port/backend-storage-ipc-standby`, commit 41d11c9)
- **Auditor**: independent re-derivation from C + headers + c2rust; port comments not trusted.

## Function inventory and verdicts

The C file defines 31 functions (verified by enumerating definitions in
standby.c and cross-checking the c2rust rendering, which contains the same 31
plus inlined header helpers `pg_atomic_read/write_u64`,
`pgstat_report_wait_start/end` — those are seam calls in the port). Every
function has a row; none were audited "by category".

| # | C function (standby.c line) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `InitRecoveryTransactionEnvironment` (95) | `InitRecoveryTransactionEnvironment` (220) | MATCH | Hash tables → thread-local `HashMap`s (C tables are process-local dynahash, no shared mem). Assert-not-twice kept. `SharedInvalBackendInit(true)`, `MyProc->vxid.procNumber = MyProcNumber` stamp, `GetNextLocalTransactionId`, `VirtualXactLockTableInsert`, `standbyState = STANDBY_INITIALIZED` (via xlogutils seam) — all present, same order. |
| 2 | `ShutdownRecoveryTransactionEnvironment` (161) | (255) | MATCH | NULL-guard early return; Expire → ReleaseAll → destroy tables → `VirtualXactLockTableCleanup`, same order. |
| 3 | `GetStandbyLimitTime` (201) | (285) | MATCH | `GetXLogReceiptTime` via xlogrecovery seam; -1 ⇒ 0 (wait forever); `TimestampTzPlusMilliseconds` reimplemented as `tz + ms*1000` — identical to the C macro. |
| 4 | `WaitExceedsMaxStandbyDelay` (234) | (313) | MATCH | CHECK_FOR_INTERRUPTS (tcop seam, fallible like the C longjmp); `ltime && now >= ltime` ⇒ `ltime != 0 &&`; sleep + wait-event bracketing; `standbyWait_us *= 2` capped at 1,000,000. |
| 5 | `LogRecoveryConflict` (274) | (344) | MATCH | Assert(still_waiting \|\| wait_list==NULL); msecs/usecs split identical; pid-list construction: `ProcNumberGetProc(...)->pid` collapsed to a `proc_number_get_proc_pid` seam returning 0 for NULL — verified exact: procarray.c `ProcNumberGetProc` returns NULL iff out-of-range or `pid == 0`, so `pid != 0` ⇔ `proc != NULL`. `errdetail_log_plural` only when nprocs>0; singular at n==1 (matches ngettext for English); both LOG messages verbatim. |
| 6 | `ResolveRecoveryConflictWithVirtualXIDs` (360) | (415) | MATCH | Fast exit on invalid head; waitStart only if `report_waiting && (log_recovery_conflict_waits \|\| update_process_title)`; per-vxid `standbyWait_us` reset; inner `VirtualXactLock(.., false)` loop with cancel-on-delay (+5 ms grace `pg_usleep(5000)` iff pid!=0); ps "waiting" after 500 ms; LogRecoveryConflict at DeadlockTimeout passing the *remaining* waitlist (`&waitlist[idx..]` = C's advanced pointer); final resolved-log + suffix removal. Branch-for-branch identical. |
| 7 | `ResolveRecoveryConflictWithSnapshot` (468) | (526) | MATCH | InvalidTransactionId no-op; Assert normal; `GetConflictingVirtualXIDs(horizon, locator.dbOid)`; PROCSIG_RECOVERY_CONFLICT_SNAPSHOT=10 + WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT=0x0800002C, report_waiting=true; `wal_level >= WAL_LEVEL_LOGICAL && isCatalogRel` ⇒ `InvalidateObsoleteReplicationSlots(RS_INVAL_HORIZON, 0, dbOid, horizon)` (RS_INVAL_HORIZON=1<<1 per slot.h). |
| 8 | `ResolveRecoveryConflictWithSnapshotFullXid` (512) | (563) | MATCH | `diff = U64(next) - U64(horizon)` as `wrapping_sub`; `< MaxTransactionId/2` (0xFFFFFFFF/2); truncation = low 32 bits (`FullTransactionId::xid`). |
| 9 | `ResolveRecoveryConflictWithTablespace` (539) | (580) | MATCH | `GetConflictingVirtualXIDs(InvalidTransactionId, InvalidOid)`; TABLESPACE=8, wait event 0x0800002D, report_waiting=true. tsid unused in C too. |
| 10 | `ResolveRecoveryConflictWithDatabase` (569) | (594) | MATCH | `while CountDBBackends > 0 { CancelDBBackends(dbid, DATABASE=7, true); pg_usleep(10000) }`. |
| 11 | `ResolveRecoveryConflictWithLock` (623) | (619) | MATCH | Re-derived against C and c2rust line by line: Assert(InHotStandby) (standbyState >= STANDBY_SNAPSHOT_PENDING=2 via xlogutils seam); waitStart write-once via atomic seam; immediate path (`now >= ltime && ltime != 0`) → GetLockConflicts + RRCWVXIDs(report_waiting=false, `PG_WAIT_LOCK \| locktag_type`); else arm STANDBY_LOCK_TIMEOUT(6)/TMPARAM_AT and STANDBY_DEADLOCK_TIMEOUT(4)/TMPARAM_AFTER(DeadlockTimeout); ProcWaitForSignal; gotos rendered as labeled block — lock-timeout exit, deadlock path signals all conflicting vxids (STARTUP_DEADLOCK=13), `logging_conflict` early-exit, second wait; cleanup disables all timeouts + clears both flags. Equivalence with the two `goto cleanup` paths verified against the c2rust nesting. |
| 12 | `ResolveRecoveryConflictWithBufferPin` (793) | (737) | MATCH | Immediate path sends BUFFERPIN(12); else STANDBY_TIMEOUT(5)/AT + STANDBY_DEADLOCK_TIMEOUT/AFTER (only deadlock flag pre-cleared, matching C); ProcWaitForSignal(WAIT_EVENT_BUFFER_PIN=0x04000000); delay-timeout ⇒ BUFFERPIN, else deadlock-timeout ⇒ STARTUP_DEADLOCK; disable_all_timeouts(false) + clear delay+deadlock flags. |
| 13 | `SendRecoveryConflictWithBufferPin` (877) | (804) | MATCH | Assert on reason ∈ {BUFFERPIN, STARTUP_DEADLOCK}; `CancelDBBackends(InvalidOid, reason, false)`. |
| 14 | `CheckRecoveryConflictDeadlock` (905) | (819) | MATCH | Assert(!InRecovery); `HoldingBufferPinThatDelaysRecovery()` guard; ERROR with SQLSTATE 40P01 (ERRCODE_T_R_DEADLOCK_DETECTED), message + detail verbatim, returned as `Err`. |
| 15 | `StandbyDeadLockHandler` (936) | (840) | MATCH | Sets deadlock flag. |
| 16 | `StandbyTimeoutHandler` (945) | (845) | MATCH | Sets delay flag. |
| 17 | `StandbyLockTimeoutHandler` (954) | (850) | MATCH | Sets lock flag. |
| 18 | `StandbyAcquireAccessExclusiveLock` (986) | (865) | MATCH | Skip if !valid \|\| DidCommit \|\| DidAbort (short-circuit order preserved); DEBUG4; Assert(OidIsValid(relOid)); xid entry created if absent; lock entry de-dup; head-prepend chain (`insert(0, ..)` preserves C's LIFO release order); `LockAcquire(tag, AccessExclusiveLock=8, sessionLock=true, dontWait=false)` only when new; SET_LOCKTAG_RELATION field-for-field (field1=dbOid, field2=relOid, type=LOCKTAG_RELATION=0, method=DEFAULT_LOCKMETHOD=1). |
| 19 | `StandbyReleaseXidEntryLocks` (1035) | (912) | MATCH | Per-entry DEBUG4, LockRelease(.., true), LOG + Assert(false) (→ `debug_assert!`) on failure, per-lock hash removal; chain order preserved. |
| 20 | `StandbyReleaseLocks` (1068) | (940) | MATCH | Valid xid → find+release+remove xid entry; invalid → StandbyReleaseAllLocks. |
| 21 | `StandbyReleaseLockTree` (1092) | (958) | MATCH | xid then each subxid; `(nsubxids, subxids)` folded into a slice. |
| 22 | `StandbyReleaseAllLocks` (1106) | (967) | MATCH | DEBUG2; full scan, release + remove every xid entry (drain). |
| 23 | `StandbyReleaseOldLocks` (1130) | (985) | MATCH | Assert valid; skip prepared (`StandbyTransactionIdIsPrepared`); skip `!TransactionIdPrecedes(xid, oldxid)`; else release + remove. Key snapshot before iteration is equivalent to C's hash_seq with delete-current (no inserts occur during the scan). |
| 24 | `standby_redo` (1163) | (1117) | MATCH | `info & ~XLR_INFO_MASK` (0x0F); Assert(!has_any_block_refs); STANDBY_DISABLED no-op; XLOG_STANDBY_LOCK=0x00 → per-lock acquire; XLOG_RUNNING_XACTS=0x10 → RunningTransactionsData (subxid_status SUBXIDS_MISSING=1/SUBXIDS_IN_ARRAY=0) → ProcArrayApplyRecoveryInfo → pgstat_report_stat(true); XLOG_INVALIDATIONS=0x20 → ProcessCommittedInvalidationMessages; else PANIC "standby_redo: unknown op code %u". Record-byte parsing (the seam carries `&[u8]` instead of a C struct pointer) verified against standbydefs.h layouts: xl_standby_locks (nlocks@0, locks@4, 12-byte entries), xl_running_xacts (xcnt@0, subxcnt@4, overflow@8, nextXid@12, oldest@16, latestCompleted@20, xids@24=MinSizeOfXactRunningXacts), xl_invalidations (dbId@0, tsId@4, inval@8, nmsgs@12, msgs@16=MinSizeOfInvalidations). Note: on a *corrupt* record (negative count / truncated body) the port returns an error where C would read garbage or no-op — identical on every record the writers in this file can produce. |
| 25 | `LogStandbySnapshot` (1282) | (1179) | MATCH | Assert(XLogStandbyInfoActive) (`wal_level >= WAL_LEVEL_REPLICA=1`); USE_INJECTION_POINTS block absent from the build (confirmed absent in c2rust); GetRunningTransactionLocks (caller-context palloc → `Mcx` parameter) + LogAccessExclusiveLocks iff nlocks>0 + pfree; GetRunningTransactionData; ProcArrayLock released before insert iff `wal_level < LOGICAL` else after; XidGenLock release; returns recptr. LWLock ids verified vs lwlocklist.h: XidGen=3, ProcArray=4. |
| 26 | `LogCurrentRunningXacts` (1353) | (1222) | MATCH | xl_running_xacts header serialized to the exact C layout (24 bytes, pad after bool); `subxid_overflow = status != SUBXIDS_IN_ARRAY`; BeginInsert → SetRecordFlags(XLOG_MARK_UNIMPORTANT=0x02) → RegisterData(header) → xids registered iff `xcnt > 0` with `(xcnt+subxcnt)` ids → XLogInsert(RM_STANDBY_ID=8, XLOG_RUNNING_XACTS); both DEBUG2 messages with LSN_FORMAT_ARGS (`%X/%X` = `{:X}` on hi/lo u32); XLogSetAsyncXactLSN(recptr). |
| 27 | `LogAccessExclusiveLocks` (1413) | (1293) | MATCH | header = nlocks (offsetof(xl_standby_locks,locks)=4); body = packed 12-byte xl_standby_lock entries; register order and SetRecordFlags-after-RegisterData order preserved; XLOG_STANDBY_LOCK insert. |
| 28 | `LogAccessExclusiveLock` (1431) | (1315) | MATCH | xid = GetCurrentTransactionId (fallible seam, like the C ereport path); LogAccessExclusiveLocks(1, &xlrec); `MyXactFlags \|= XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` via dedicated xact seam. |
| 29 | `LogAccessExclusiveLockPrepare` (1448) | (1333) | MATCH | `(void) GetCurrentTransactionId()`. |
| 30 | `LogStandbyInvalidations` (1470) | (1341) | MATCH | header = dbId(MyDatabaseId seam), tsId(MyDatabaseTableSpace seam), inval flag, pad, nmsgs (16 bytes = MinSizeOfInvalidations; memset-zero matched by explicit pad bytes); body = raw 16-byte SharedInvalidationMessage payloads (union size 16 verified vs sinval.h SharedInvalSmgrMsg packing); XLOG_INVALIDATIONS insert. |
| 31 | `get_recovery_conflict_desc` (1492) | (1367) | MATCH | All seven strings verbatim; default "unknown reason". |

Constants verified against headers / the c2rust rendering (not from memory):
RM_STANDBY_ID=8 (rmgrlist.h), XLOG_STANDBY_LOCK/RUNNING_XACTS/INVALIDATIONS =
0x00/0x10/0x20 (standbydefs.h), PROCSIG_RECOVERY_CONFLICT_* = 7..13
(procsignal.h via c2rust), WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT/TABLESPACE =
0x0800002C/0x0800002D, WAIT_EVENT_BUFFER_PIN = 0x04000000, PG_WAIT_LOCK =
0x03000000 (wait_classes.h + generated ids in c2rust), TimeoutId
STANDBY_DEADLOCK/STANDBY/STANDBY_LOCK = 4/5/6 and the full enum order
(timeout.h), TMPARAM_AFTER/AT = 0/1, AccessExclusiveLock=8,
DEFAULT_LOCKMETHOD=1, LOCKTAG_RELATION=0, SUBXIDS_IN_ARRAY/MISSING = 0/1,
STANDBY_DISABLED/INITIALIZED/SNAPSHOT_PENDING = 0/1/2, WAL_LEVEL_REPLICA/
LOGICAL = 1/2, XLR_INFO_MASK=0x0F, XLOG_MARK_UNIMPORTANT=0x02,
RS_INVAL_HORIZON=1<<1 (slot.h), LWLock XidGen=3/ProcArray=4 (lwlocklist.h),
MaxTransactionId=0xFFFFFFFF, sizeof(SharedInvalidationMessage)=16,
SQLSTATE 40P01, STANDBY_INITIAL_WAIT_US=1000.

## Seam audit

Inward (`crates/backend-storage-ipc-standby-seams`): 22 declarations, all
pure `seam_core::seam!` declarations; all 22 installed by this crate's
`init_seams()` (nothing but `set()` calls); `seams-init::init_all()` calls
`backend_storage_ipc_standby::init_seams()`. No `set()` for these seams
anywhere else in the workspace.

Outward seam crates (new): procarray, sinval, lmgr-proc, transam, twophase,
varsup, xlog, xloginsert, xlogrecovery, xlogutils, replication-slot, bufmgr,
cache-inval, misc-timeout, ps-status, activity-waitevent, pgsleep — each maps
1:1 to a C function/global owned by an unported unit (real dependency: none
of those units exist as crates yet, so a direct dep is impossible). Extended
existing seam crates (lock, lwlock, xact, adt-timestamp, pgstat, init-small,
tcop) gained only declarations. All checked: declaration-only crates, no
branching/computation; every call site in the port is thin marshal +
delegate. Notable shapes, each verified as faithful marshalling:

- `proc_number_get_proc_pid` collapses `ProcNumberGetProc(n) ? ->pid : 0` —
  exact because the C function itself returns NULL iff `pid == 0` or
  out-of-range.
- `get_lock_conflicts`/`get_conflicting_virtual_xids` drop the C
  InvalidVirtualTransactionId terminator into a `Vec` length (the port still
  re-checks `is_valid()` per element, so even a terminated array behaves
  identically).
- `lwlock_release_builtin(id)` carries the lwlocklist.h index for
  `LWLockRelease(&MainLWLockArray[id].lock)`.
- `standby_redo` seam carries `(info, data, has_any_block_refs)` — the
  XLogReaderState accessors evaluated at the boundary; the rmgr mask and all
  dispatch logic stay in this crate.
- `set_my_proc_vxid_proc_number` / `my_proc_wait_start` /
  `set_my_proc_wait_start` are field accessors for `MyProc` (owned by
  unported proc.c), no logic.

No function body was replaced by a seam call to "somewhere else": all 31
bodies live in this crate; outward calls correspond exactly to the C
call-outs to other translation units (or globals owned by them).

Globals: GUCs (`max_standby_archive_delay`, `max_standby_streaming_delay`,
`log_recovery_conflict_waits`), `standbyWait_us`, the three sig_atomic_t
flags, and the two lock tables are thread-locals here, matching their
per-backend/process-local nature in C. `standbyState` is correctly *not*
owned here (xlogutils.c owns it); read/written through xlogutils seams.

Build/tests: `cargo build --workspace` clean; `cargo test -p
backend-storage-ipc-standby` 9/9 pass.

## Verdict

**PASS** — all 31 functions MATCH; seam wiring clean (no findings).
