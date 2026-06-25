# Audit: backend-storage-lmgr-lmgr

- **Unit:** backend-storage-lmgr-lmgr (`src/backend/storage/lmgr/lmgr.c`)
- **Branch:** port/backend-storage-lmgr-lmgr
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (claude-opus-4-8[1m])
- **Verdict:** **PASS** (independent re-audit; seam-wiring findings fixed 2026-06-13)

## Top line

This is an independent from-scratch re-audit. All 47 lmgr.c functions match the
C exactly (constants, control flow, error paths, message strings re-derived from
the headers; a sample re-derived in full — see below). The previously-recorded
FAIL (misplaced/uninstalled `virtual_xact_lock_table_insert` in the lmgr-owned
seam crate, consumed by xact.c) **is resolved** on the branch: that declaration
no longer exists in `backend-storage-lmgr-lmgr-seams`, and xact.c
(`engine.rs:383`) now consumes the correct owner
`backend_storage_lmgr_lock_seams::virtual_xact_lock_table_insert`.

This re-audit, however, found **two further seam-wiring findings** that the
prior pass missed (it audited only `…-lmgr-seams`, never enumerating the unit's
full set of owned seam crates per audit step 3). Both are now fixed on the
branch; with the fixes the unit passes.

## Owned seam crates (audit step 3: ownership by C-source coverage)

The unit's only c_source is `lmgr.c`. The seam crates whose declarations map to
lmgr.c functions are:

1. `backend-storage-lmgr-lmgr-seams` — the general inward namespace.
2. `backend-storage-lmgr-lmgr-pc-seams` — plancache's slice of lmgr.c
   (bare-OID `LockRelationOid`/`UnlockRelationOid` for plan revalidation).

`backend-storage-lmgr-lock-seams` is NOT owned here — it maps to lock.c (a
separate, unported unit); its declarations (incl. `virtual_xact_lock_table_insert`)
are legitimate seam-and-panic on unported callees.

## Seam findings (fixed)

**FINDING A (lmgr-seams):** four declarations in
`backend-storage-lmgr-lmgr-seams` were never installed by `init_seams()` (nor
anywhere else), yet are consumed by live production callers — a call would panic:

| seam | consumer |
|---|---|
| `lock_shared_object` | `backend-catalog-pg-shdepend` (`lib.rs:1150`) |
| `lock_relation_for_extension` | `backend-access-nbtree-nbtree` (`lib.rs:1167`) |
| `unlock_relation_for_extension` | `RelationExtensionLockGuard` release/drop |
| `describe_lock_tag` | `backend-storage-lmgr-deadlock` (`detector.rs:1135`) |

**FINDING B (lmgr-pc-seams):** the entire owned `…-lmgr-pc-seams` crate
(`lock_relation_oid`, `unlock_relation_oid`, both `fn(Oid, i32) -> PgResult<()>`)
was never installed by anyone, and is consumed by plancache
(`backend-utils-cache-plancache/src/lib.rs:1567,1569,1609,1611,1617,1619`). The
prior audit did not enumerate this crate at all.

### Fixes applied

`init_seams()` (`crates/backend-storage-lmgr-lmgr/src/lib.rs`) now installs every
declaration in both owned seam crates:

- `lock_shared_object` → new `seam_lock_shared_object` adapter (delegates to
  `LockSharedObject`, returns a `LockGuard::database_object` — a shared object
  is the OBJECT release path with `dbid = 0`).
- `lock_relation_for_extension` → new `seam_lock_relation_for_extension` adapter:
  rebuilds the C `relation->rd_lockInfo.lockRelId` from the relation
  (`relId = rd_id`, `dbId = rd_locator.dbOid`, the same value
  `RelationInitLockInfo` computes), calls `LockRelationForExtension` with
  `ExclusiveLock`, returns a `RelationExtensionLockGuard`.
- `unlock_relation_for_extension` → `seam_unlock_relation_for_extension`
  (reconstructs the tag with `dbId = MyDatabaseId`, matching acquisition for the
  non-shared relations the extension lock is ever taken on).
- `describe_lock_tag` → `seam_describe_lock_tag` (transient `MemoryContext`,
  delegates to `DescribeLockTag`, returns the rendered `String`).
- `…-lmgr-pc-seams::lock_relation_oid`/`unlock_relation_oid` → installed directly
  with `LockRelationOid`/`UnlockRelationOid` (the signatures are identical:
  `LOCKMODE` is `i32`).

Added the `types-rel` and `backend-storage-lmgr-lmgr-pc-seams` deps and
`extern crate alloc` to the lmgr crate. Workspace builds clean; lmgr tests pass.

## Function inventory

C source has 47 function definitions (line 61 is a forward declaration of the
static `XactLockTableWaitErrorCb`, defined at line 846 — one function). The
c2rust run (`c2rust-runs/backend-storage-lmgr-lmgr/src/lmgr.rs`) carries the
same set. Every function gets a row.

| # | C function (line) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `RelationInitLockInfo` (70) | `RelationInitLockInfo` | MATCH | C writes `rd_lockInfo.lockRelId` in place; port returns the `LockRelId` value (relisshared/MyDatabaseId threaded in per no-ambient-global rule). dbId=0 when shared else MyDatabaseId. |
| 2 | `SetLocktagRelationOid` (88, static inline) | `set_locktag_relation_oid` | MATCH | shared→InvalidOid(0), else MyDatabaseId via `catalog::is_shared_relation` + `initsmall::my_database_id` seams. |
| 3 | `LockRelationOid` (107) | `LockRelationOid` | MATCH | acquire(false,false); if `!= ALREADY_CLEAR` → AcceptInvalidationMessages + MarkLockClear. |
| 4 | `ConditionalLockRelationOid` (151) | `ConditionalLockRelationOid` | MATCH | acquire(false,true); NOT_AVAIL→false; else inval+mark; true. |
| 5 | `LockRelationId` (185) | `LockRelationId` | MATCH | LockRelId in, SET_LOCKTAG_RELATION. |
| 6 | `UnlockRelationId` (214) | `UnlockRelationId` | MATCH | LockRelease(false). |
| 7 | `UnlockRelationOid` (229) | `UnlockRelationOid` | MATCH | |
| 8 | `LockRelation` (246) | `LockRelation` | MATCH | takes LockRelId (read off rd_lockInfo). |
| 9 | `ConditionalLockRelation` (278) | `ConditionalLockRelation` | MATCH | |
| 10 | `UnlockRelation` (314) | `UnlockRelation` | MATCH | |
| 11 | `CheckRelationLockedByMe` (334) | `CheckRelationLockedByMe` | MATCH | LockHeldByMe via `lock::lock_held_by_me`. |
| 12 | `CheckRelationOidLockedByMe` (351) | `CheckRelationOidLockedByMe` | MATCH | |
| 13 | `LockHasWaitersRelation` (367) | `LockHasWaitersRelation` | MATCH | LockHasWaiters(...,false). |
| 14 | `LockRelationIdForSession` (391) | `LockRelationIdForSession` | MATCH | session=true. |
| 15 | `UnlockRelationIdForSession` (404) | `UnlockRelationIdForSession` | MATCH | release(session=true). |
| 16 | `LockRelationForExtension` (424) | `LockRelationForExtension` | MATCH | RELATION_EXTEND tag; no inval (C comment). |
| 17 | `ConditionalLockRelationForExtension` (442) | `ConditionalLockRelationForExtension` | MATCH | `res != NOT_AVAIL`. |
| 18 | `RelationExtensionLockWaiterCount` (459) | `RelationExtensionLockWaiterCount` | MATCH | LockWaiterCount. |
| 19 | `UnlockRelationForExtension` (474) | `UnlockRelationForExtension` | MATCH | |
| 20 | `LockDatabaseFrozenIds` (491) | `LockDatabaseFrozenIds` | MATCH | DATABASE_FROZEN_IDS tag, dbid=MyDatabaseId. |
| 21 | `LockPage` (507) | `LockPage` | MATCH | |
| 22 | `ConditionalLockPage` (526) | `ConditionalLockPage` | MATCH | |
| 23 | `UnlockPage` (542) | `UnlockPage` | MATCH | |
| 24 | `LockTuple` (562) | `LockTuple` | MATCH | block/offset from ItemPointer, threaded in. |
| 25 | `ConditionalLockTuple` (582) | `ConditionalLockTuple` | MATCH | LockAcquireExtended with logLockFailure passthrough; `!= NOT_AVAIL`. |
| 26 | `UnlockTuple` (601) | `UnlockTuple` | MATCH | |
| 27 | `XactLockTableInsert` (622) | `XactLockTableInsert` | MATCH | ExclusiveLock acquire. |
| 28 | `XactLockTableDelete` (639) | `XactLockTableDelete` | MATCH | ExclusiveLock release. |
| 29 | `XactLockTableWait` (663) | `XactLockTableWait` + `xact_lock_table_wait_inner` | MATCH (re-derived) | ShareLock acquire+release loop; TransactionIdIsInProgress break; sleep-unless-first (CHECK_FOR_INTERRUPTS + pg_usleep(1000)); SubTransGetTopmostTransaction. Error-context attaches on propagation via `map_err`+`add_context` per query-lifecycle-raii.md (vs C's error_context_stack callback). The debug-only `Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()))` is dropped — no ambient-global getter; assertion only, no runtime behavior. |
| 30 | `ConditionalXactLockTableWait` (739) | `ConditionalXactLockTableWait` | MATCH | LockAcquireExtended(...,true,...); NOT_AVAIL→false; same loop. Same Assert dropped. |
| 31 | `SpeculativeInsertionLockAcquire` (786) | `SpeculativeInsertionLockAcquire` | MATCH | per-backend `speculativeInsertionToken` is `thread_local`; ++ with wrap (0→1); ExclusiveLock. |
| 32 | `SpeculativeInsertionLockRelease` (812) | `SpeculativeInsertionLockRelease` | MATCH | uses current token; ExclusiveLock release. |
| 33 | `SpeculativeInsertionWait` (828) | `SpeculativeInsertionWait` | MATCH | ShareLock acquire+release; `debug_assert_ne!(token,0)`. |
| 34 | `XactLockTableWaitErrorCb` (846, static) | `xact_wait_error_context` | MATCH | all 8 XLTW_Oper message strings verbatim from gettext_noop; XLTW_None / invalid ctid / invalid rel → None (C `default: return;` and the validity guards). |
| 35 | `WaitForLockersMultiple` (911) | `WaitForLockersMultiple` | MATCH | empty→return; GetLockConflicts per tag, progress total accumulation; progress PROGRESS_WAITFOR_* params; per-holder VirtualXactLock(...,true); final multi_param reset {0,0,0}. `holders` over PgVec in `mcx`. ProcNumberGetProc NULL-check collapsed into `proc_number_get_proc_pid` returning pid with `pid != 0` standing in for non-NULL holder (an active PGPROC always has nonzero pid). |
| 36 | `WaitForLockers` (989) | `WaitForLockers` | MATCH | single-tag delegation. |
| 37 | `LockDatabaseObject` (1008) | `LockDatabaseObject` | MATCH | OBJECT tag dbid=MyDatabaseId; unconditional AcceptInvalidationMessages. |
| 38 | `ConditionalLockDatabaseObject` (1032) | `ConditionalLockDatabaseObject` | MATCH | |
| 39 | `UnlockDatabaseObject` (1068) | `UnlockDatabaseObject` | MATCH | |
| 40 | `LockSharedObject` (1088) | `LockSharedObject` | MATCH | dbid=InvalidOid(0); unconditional inval. |
| 41 | `ConditionalLockSharedObject` (1112) | `ConditionalLockSharedObject` | MATCH | |
| 42 | `UnlockSharedObject` (1148) | `UnlockSharedObject` | MATCH | |
| 43 | `LockSharedObjectForSession` (1169) | `LockSharedObjectForSession` | MATCH | session=true. |
| 44 | `UnlockSharedObjectForSession` (1187) | `UnlockSharedObjectForSession` | MATCH | release session=true. |
| 45 | `LockApplyTransactionForSession` (1209) | `LockApplyTransactionForSession` | MATCH | APPLY_TRANSACTION tag dbid=MyDatabaseId, session=true. |
| 46 | `UnlockApplyTransactionForSession` (1227) | `UnlockApplyTransactionForSession` | MATCH | |
| 47 | `DescribeLockTag` (1249) | `DescribeLockTag` | MATCH | all 13 branches + default; strings verbatim; VIRTUALTRANSACTION field1 printed signed (`as i32`) matching C `%d`; appendStringInfo OOM (`ereport ERROR`) surfaced via fallible `append_fmt`. |
| 48 | `GetLockNameFromTagType` (1346) | `GetLockNameFromTagType` | MATCH (re-derived) | `> LOCKTAG_LAST_TYPE` → "???"; LockTagTypeNames[12] verbatim. |

### Constants verified against headers (not memory)

- `LOCKTAG_*` 0..11 and `LOCKTAG_LAST_TYPE = LOCKTAG_APPLY_TRANSACTION (11)` —
  `storage/lock.h` → `types-storage/src/lock.rs` exact.
- `XLTW_Oper` None..RecheckExclusionConstr = 0..8 — `storage/lmgr.h` → port exact.
- `LockTagTypeNames[]` (12 entries) — re-derived against `lockfuncs.c:28`:
  relation/extend/frozenid/page/tuple/transactionid/virtualxid/spectoken/object/
  userlock/advisory/applytransaction — verbatim.
- `DEFAULT_LOCKMETHOD = 1`, `DescribeLockTag` format strings, and all 8
  `XactLockTableWaitErrorCb` strings — verbatim.

### Auditor self-check (re-derived MATCH verdicts)

- `XactLockTableWait` (#29): read C (lmgr.c:663-738) line-by-line against the
  port; the for(;;) body — SET_LOCKTAG_TRANSACTION → LockAcquire(ShareLock) →
  LockRelease → break unless TransactionIdIsInProgress → (!first ⇒
  CHECK_FOR_INTERRUPTS + pg_usleep(1000)) → first=false →
  SubTransGetTopmostTransaction — has an exact counterpart. Error-context is the
  RAII map_err+add_context shape; dropped Assert is ledgered.
- `GetLockNameFromTagType` + `LockTagTypeNames` (#48): bounds check and all 12
  table entries match the C source verbatim.

## Seam audit (step 3)

- `init_seams()` (lib.rs) contains nothing but `set()` calls and, after the
  fixes, installs **every** declaration of both owned seam crates
  (`…-lmgr-seams`: check_relation_locked_by_me, lock_relation_oid,
  conditional_lock_relation_oid, lock_database_object, lock_shared_object,
  unlock_relation_oid, unlock_database_object, lock_relation_for_extension,
  unlock_relation_for_extension, describe_lock_tag, xact_lock_table_insert,
  xact_lock_table_delete; `…-lmgr-pc-seams`: lock_relation_oid,
  unlock_relation_oid).
- `seams-init` (`crates/seams-init/src/lib.rs:49`) calls
  `backend_storage_lmgr_lmgr::init_seams()`. No `set()` outside the owner.
- Outward seam calls (lock/inval/procarray/subtrans/tcop/port-pgsleep/init-small/
  catalog) are thin marshal+delegate, justified by the lock.c→lmgr.c-style
  dependency cycle; no branching/computation in seam paths.
- The new seam adapters are thin (one delegated call + value conversion); the
  `lockRelId` reconstruction in the extension adapters is the same arithmetic
  `RelationInitLockInfo` performs, not new logic.

## Design conformance (3b)

- No invented opacity: relation entry points take `LockRelId` (the real
  `rd_lockInfo.lockRelId` value), not a stand-in handle; the extension adapter
  reads the relation's real `rd_id`/`rd_locator`. OK.
- Allocating `WaitForLockersMultiple` takes `Mcx` and returns `PgResult`
  (holders over PgVec). OK.
- Per-backend `speculativeInsertionToken` is `thread_local!`, not a shared
  static. OK.
- No ambient-global seams: MyDatabaseId/IsSharedRelation resolved through owner
  seams; the GetTopTransactionIdIfAny debug Assert is dropped (ledgered) rather
  than introducing an ambient getter. OK.
- Locks return `LockGuard`/`RelationExtensionLockGuard` (RAII) rather than ()+
  ambient release. OK.

## Conclusion

47/47 lmgr.c functions MATCH. Two seam-wiring findings (uninstalled owned seam
declarations across both `…-lmgr-seams` and `…-lmgr-pc-seams`) found and fixed
on the branch; the previously-recorded `virtual_xact_lock_table_insert` finding
is confirmed resolved. After fixes: zero seam findings, workspace builds clean,
tests pass → **PASS**.
