# Audit: backend-access-transam-xact

- **C source**: `src/backend/access/transam/xact.c` (PostgreSQL 18.3, 6446 lines),
  plus the `ParseCommitRecord`/`ParseAbortRecord` parsers from
  `src/backend/access/rmgrdesc/xactdesc.c` (re-exported for the rmgr-desc unit).
- **c2rust cross-check**: `../pgrust/c2rust-runs/backend-access-transam-xact/src/xact.rs`
  (68 public fns + the file statics; all accounted for below).
- **Port**: `crates/backend-access-transam-xact/src/{lib,engine,wal,redo}.rs`.
- **Auditor**: independent re-derivation from the C sources; constants verified
  against `access/xact.h`, `storage/proc.h`, `utils/timeout.h`,
  `storage/sinval.h`, `access/xlogrecord.h`, `access/xloginsert.h`,
  `replication/origin.h`, `utils/errcodes` values in `types-error`.

## Sanctioned divergences (repo-wide conventions, not findings)

- Resource owners dissolve into RAII guards; `if (s->curTransactionOwner)`
  control flow preserved via `has_resource_owner` (docs/query-lifecycle-raii.md).
- No ambient memory context: `MemoryContextSwitchTo`/`priorContext`
  choreography has no equivalent; the transaction-lifetime contexts are owned
  by the backend-local state and created/reset/deleted at the same points.
- `AtEOXact_ComboCid` / `AtEOXact_HashTables` (and sub-xact twins) dissolve.
- Backend-local file statics are `thread_local!`.
- `TRACE_POSTGRESQL_*` dtrace probes are no-ops.

## Constant verification (against headers, not memory)

| Constant group | Header | Result |
|---|---|---|
| `XLOG_XACT_*` opcodes (0x00..0x60), `OPMASK` 0x70, `HAS_INFO` 0x80 | xact.h:169-182 | match |
| `XACT_XINFO_HAS_*` bits (1<<0 .. 1<<8) | xact.h:188-196 | match |
| `XACT_COMPLETION_*` bits (1<<29..1<<31) | xact.h:206-208 | match |
| `XACT_FLAGS_*` (1<<0..1<<3) | xact.h:102-121 | match |
| Isolation levels 0..3, `SYNCHRONOUS_COMMIT_*` 0..4, `ON`=3 | xact.h:36-80 | match |
| `TransState` 0..5, `TBlockState` 0..19 (enum order) | xact.c:143-183 | match |
| `XactEvent` 0..7, `SubXactEvent` 0..3 (enum order) | xact.h:128-145 | match |
| `TRANSACTION_TIMEOUT` = 8 (TimeoutId enum position) | timeout.h | match |
| `PGPROC_MAX_CACHED_SUBXIDS` = 64 | proc.h:39 | match |
| `MaxAllocSize` = 0x3fffffff | memutils.h | match |
| `sizeof(SharedInvalidationMessage)` = 16 | sinval.h (largest member 16B) | match |
| `MinSizeOfXactAssignment` = 8, `MinSizeOfXactCommit` = 8, `MinSizeOfXactAbort` = 8, `MinSizeOfXactSubxacts`/`RelfileLocators`/`StatsItems`/`Invals` = 4 | xact.h | match (byte emission checked) |
| `xl_xact_stats_item` = {int kind; Oid dboid; u32 objid_lo; u32 objid_hi} (lo first) | xact.h:282-293 | match |
| `SerializedTransactionStateHeaderSize` = 32 (field offsets 0/4/8/16/24/28) | xact.h struct layout | match |
| `XLR_SPECIAL_REL_UPDATE` 0x01, `XLOG_INCLUDE_ORIGIN` 0x01, `RM_XACT_ID` 1 | xlogrecord.h/xloginsert.h/rmgrlist.h | match |
| `DoNotReplicateId` = u16::MAX, `InvalidRepOriginId` = 0 | origin.h | match |
| SQLSTATEs 25000/25001/25P01/3B001/54000/0A000 | errcodes ↔ types-error | match |
| `STANDBY_DISABLED..SNAPSHOT_READY` 0..3 | xlogutils.h | match |

## Per-function table

Verdicts after fix round 1 (see Findings). C line numbers from xact.c unless noted.

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| IsTransactionState (387) | lib.rs `IsTransactionState` | MATCH | `state == TRANS_INPROGRESS` only |
| IsAbortedTransactionBlockState (407) | lib.rs | MATCH | ABORT/SUBABORT |
| GetTopTransactionId (426) | lib.rs | MATCH | assigns at stack[0] (= `&TopTransactionStateData`) |
| GetTopTransactionIdIfAny (441) | lib.rs | MATCH | |
| GetCurrentTransactionId (454) | lib.rs | MATCH | |
| GetCurrentTransactionIdIfAny (471) | lib.rs | MATCH | |
| GetTopFullTransactionId (483) | lib.rs | MATCH | |
| GetTopFullTransactionIdIfAny (499) | lib.rs | MATCH | |
| GetCurrentFullTransactionId (512) | lib.rs | MATCH | |
| GetCurrentFullTransactionIdIfAny (530) | lib.rs | MATCH | |
| MarkCurrentTransactionIdLoggedIfAny (541) | lib.rs | MATCH | |
| IsSubxactTopXidLogPending (559) | lib.rs | MATCH | same 5 predicates, same order; `is_subxact()` ≡ `nestingLevel>=2` (stack len>1 iff a pushed subxact node) |
| MarkSubxactTopXidLogged (591) | lib.rs | MATCH | Assert → debug_assert |
| GetStableLatestTransactionId (607) | lib.rs | MATCH | static latch → `stable_latest` keyed on lxid; same compute path |
| AssignTransactionId (635) | lib.rs `assign_transaction_id_at` | MATCH | parallel-mode ERROR (25000) same predicate/message; iterative parent assignment; `log_unknown_top` before GetNewTransactionId; subtrans/predicate/lock calls in C order; unreported-xids flush at >= 64 or log_unknown_top; `xl_xact_assignment` bytes (xtop + nsubxacts + xsub) match `MinSizeOfXactAssignment`; resowner swap dissolves |
| GetCurrentSubTransactionId (791) | lib.rs | MATCH | |
| SubTransactionIsActive (805) | lib.rs | MATCH | skips TRANS_ABORT nodes |
| GetCurrentCommandId (829) | lib.rs | MATCH | parallel-worker ERROR same code/message |
| SetParallelStartTimestamps (859) | lib.rs | MATCH | |
| GetCurrentTransactionStartTimestamp (870) | lib.rs | MATCH | |
| GetCurrentStatementStartTimestamp (879) | lib.rs | MATCH | |
| GetCurrentTransactionStopTimestamp (891) | lib.rs | MATCH | lazily sets via timestamp seam; C state Assert is debug-only, dropped |
| SetCurrentStatementStartTimestamp (914) | lib.rs | MATCH | |
| GetCurrentTransactionNestLevel (929) | lib.rs | MATCH | |
| TransactionIdIsCurrentTransactionId (941) | lib.rs | MATCH | non-normal-XID early false; top-xid fast path; ParallelCurrentXids numeric binary search (`binary_search`); per-node `TransactionIdPrecedes`-ordered binary search; skips TRANS_ABORT and unassigned nodes |
| (TransactionIdPrecedes, transam.c) | lib.rs `transaction_id_precedes` | MATCH | special-id fallback + wrapping i32 diff |
| TransactionStartedDuringRecovery (1042) | lib.rs | MATCH | |
| EnterParallelMode (1051) | lib.rs | MATCH | |
| ExitParallelMode (1064) | lib.rs | MATCH | `ParallelContextActive` assert dropped (debug-only, callee unported) |
| IsInParallelMode (1089) | lib.rs | MATCH | |
| CommandCounterIncrement (1100) | lib.rs | MATCH | overflow ERROR 54000 with counter restored first; SnapshotSetCommandId + AtCCI_LocalCache order |
| ForceSyncCommit (1152) | lib.rs | MATCH | |
| AtStart_Cache (1167) | lib.rs | MATCH | AcceptInvalidationMessages seam |
| AtStart_Memory (1176) | lib.rs | MATCH (sanctioned) | create-once abort/top contexts; switch-to dissolves |
| AtStart_ResourceOwner (1226) | lib.rs | MATCH (sanctioned) | flag only |
| AtSubStart_Memory (1254) | lib.rs | MATCH (sanctioned) | child of parent's CurTransactionContext (top falls back to TopTransactionContext) |
| AtSubStart_ResourceOwner (1283) | lib.rs | MATCH (sanctioned) | |
| RecordTransactionCommit (1315) | engine.rs | MATCH | LogLogicalInvalidations gate; data gathering order; no-xid path: real ERROR on pending deletes/stats, LogStandbyInvalidations + wrote_xlog, goto-cleanup ≡ early return Invalid; commit path: replorigin predicate (≠Invalid && ≠DoNotReplicate), crit section + delayChkptFlags, XactLogCommitRecord, replorigin_session_advance, commit-ts data; sync/async flush predicate verbatim; clog commit tree both arms; SyncRepWaitForLSN gate; XactLastCommitEnd/XactLastRecEnd reset |
| AtCCI_LocalCache (1579) | lib.rs | MATCH | relmap before inval |
| AtCommit_Memory (1598) | lib.rs | MATCH (sanctioned) | reset TopTransactionContext; retained subxact contexts die |
| AtSubCommit_Memory (1635) | lib.rs | MATCH (sanctioned) | empty → delete; else retained on parent (C: survives as child of parent ctx) |
| AtSubCommit_childXids (1664) | lib.rs | MATCH | limit check ≡ C's `new_maxChildXids < new_nChildXids` (errors iff n > MaxAllocSize/4 = 268435455, same message); my-xid-then-children ordering; child array reset |
| RecordTransactionAbort (1754) | engine.rs | MATCH | no-xid early return + XactLastRecEnd reset; PANIC on already-committed; crit section spans abort record → clog abort tree; async-LSN nudge for top only; XidCacheRemoveRunningXids for subxact; latestXid computed |
| AtAbort_Memory (1884) | lib.rs | MATCH (sanctioned) | ensure abort context exists |
| AtSubAbort_Memory (1904) | lib.rs | MATCH (sanctioned) | |
| AtAbort_ResourceOwner (1916) | lib.rs | MATCH (sanctioned) | dissolves |
| AtSubAbort_ResourceOwner (1929) | lib.rs | MATCH (sanctioned) | dissolves |
| AtSubAbort_childXids (1942) | lib.rs | MATCH | clears; unreportedXids deliberately not pruned (as C) |
| AtCleanup_Memory (1974) | lib.rs | MATCH (sanctioned) | abort-ctx reset; top-ctx reset if exists |
| AtSubCleanup_Memory (2022) | lib.rs | MATCH (sanctioned) | drops subxact contexts incl. retained children |
| StartTransaction (2064) | engine.rs | MATCH | state asserts; xact_is_sampled prng short-circuit identical; recovery/read-only branch; GUC-backed resets; counters; vxid create→lock→advertise order; SPI-nonatomic timestamp rule; pgstat report; AtStart_GUC/Cache/Trigger order; TRANS_INPROGRESS; transaction-timeout arming |
| CommitTransaction (2228) | engine.rs | MATCH | parallel-worker EnterParallelMode; WARNING on bad state; trigger/portal pre-commit loop; PRE_COMMIT callbacks; AtEOXact_Parallel + level warnings (1 vs 0); full shutdown sequence in C order; RecordTransactionCommit vs ParallelWorkerReportLastRecEnd; ProcArrayEndTransaction; post-commit cleanup order incl. smgrDoPendingDeletes after multixact, AtCommit_Notify after; state reset block |
| PrepareTransaction (2515) | engine.rs | MATCH | xid first; temp-namespace and exported-snapshots ERRORs (0A000, after ON COMMIT actions); MarkAsPreparing(gid taken); StartPrepare→AtPrepare_* (Notify, Locks, PredicateLocks, PgStat, MultiXact, RelationMap)→EndPrepare; XactLastRecEnd=0; PostPrepare_Locks→ProcArrayClearTransaction→PREPARE callbacks→PostPrepare_* order; GUC-as-commit tail; launcher/lr-workers `false` |
| AbortTransaction (2809) | engine.rs | MATCH | hold interrupts; timeout disable; LWLockReleaseAll → wait/progress/aio/buffers/xloginsert/condvar/LockErrorCleanup/reschedule/sigprocmask(UnBlockSig); state WARNING; SetUserIdAndSecContext; reindex/logical/snapbuild resets; AtEOXact_Parallel(false); abort processing list in C order; RecordTransactionAbort vs async-LSN nudge; ProcArrayEndTransaction; resource-owner-gated cleanup block in C order |
| CleanupTransaction (3009) | engine.rs | MATCH | FATAL on bad state; portals → snapshots → owner flag → memory → state reset |
| StartTransactionCommand (3059) | engine.rs | MATCH | 3 live arms + invalid-state ERROR; context switch dissolves |
| SaveTransactionCharacteristics (3136) | engine.rs | MATCH | |
| RestoreTransactionCharacteristics (3144) | engine.rs | MATCH | |
| CommitTransactionCommand (3157) | engine.rs | MATCH | while !internal loop |
| CommitTransactionCommandInternal (3175) | engine.rs | MATCH | every arm compared: DEFAULT/PARALLEL FATAL; STARTED/END/ABORT_END/ABORT_PENDING with chain-restart blocks (savetc); SUBRELEASE/SUBCOMMIT loops with post-loop dispatch (END/PREPARE/else ERROR); SUBABORT_END/SUBABORT_PENDING return-false; SUBRESTART/SUBABORT_RESTART name+savepointLevel preservation through DefineSavepoint(None) |
| AbortCurrentTransaction (3451) | engine.rs | MATCH | |
| AbortCurrentTransactionInternal (3469) | engine.rs | MATCH | all arms incl. TRANS_START suppression in DEFAULT |
| PreventInTransactionBlock (3648) | lib.rs | MATCH | 3 ERRORs (25001) + FATAL chain check + NEEDIMMEDIATECOMMIT flag |
| WarnNoTransactionBlock (3710) | lib.rs | MATCH | |
| RequireTransactionBlock (3716) | lib.rs | MATCH | |
| CheckTransactionBlock (3725) | lib.rs | MATCH | WARNING/ERROR 25P01 |
| IsInTransactionBlock (3769) | lib.rs | MATCH | |
| RegisterXactCallback (3804) | lib.rs | MATCH (fixed) | now prepends (C head-insert); keyed unregistration is the (callback,arg) analog |
| UnregisterXactCallback (3817) | lib.rs | MATCH (fixed) | removes first match only |
| CallXactCallbacks (3838) | lib.rs | MATCH (fixed) | serial-snapshot walk ≡ C saved-`next` walk: newest-first order, self-unregistration safe, mid-iteration registrations not invoked this round |
| RegisterSubXactCallback (3864) | lib.rs | MATCH (fixed) | |
| UnregisterSubXactCallback (3877) | lib.rs | MATCH (fixed) | |
| CallSubXactCallbacks (3898) | lib.rs | MATCH (fixed) | |
| BeginTransactionBlock (3924) | engine.rs | MATCH | STARTED/IMPLICIT → BEGIN; 5-state WARNING; rest FATAL |
| PrepareTransactionBlock (3992) | engine.rs | MATCH | walks to top (stack[0]); gid stored; degraded-commit false path |
| EndTransactionBlock (4044) | engine.rs | MATCH | all arms incl. SUBINPROGRESS/SUBABORT stack walks with FATAL on unexpected; chain errors before state change; `s->chain = chain` lands on top node |
| UserAbortTransactionBlock (4204) | engine.rs | MATCH | ditto; STARTED/IMPLICIT warn-then-ABORT_PENDING |
| BeginImplicitTransactionBlock (4326) | engine.rs | MATCH | |
| EndImplicitTransactionBlock (4351) | engine.rs | MATCH | |
| DefineSavepoint (4373) | engine.rs | MATCH | parallel ERROR; IMPLICIT ERROR (25P01, "SAVEPOINT"); push + name |
| ReleaseSavepoint (4458) | engine.rs | MATCH | state dispatch; innermost-first name search (`rposition`); not-found / wrong-level ERRORs (3B001); SUBRELEASE marking with SUBINPROGRESS assertion |
| RollbackToSavepoint (4567) | engine.rs | MATCH | ditto with SUBABORT_PENDING/SUBABORT_END marking and SUBRESTART/SUBABORT_RESTART target |
| BeginInternalSubTransaction (4694) | engine.rs | MATCH | ExitOnAnyError save/set/restore via seam; extra states allowed; CommitTransactionCommand + StartTransactionCommand inside |
| ReleaseCurrentSubTransaction (4768) | engine.rs | MATCH | ERROR (not FATAL) on bad state, as C |
| RollbackAndReleaseCurrentSubTransaction (4796) | engine.rs | MATCH | |
| AbortOutOfAnyTransaction (4862) | engine.rs | MATCH | loop over block states; ABORT/ABORT_END portal zap; SUBABORT* portal zap gated on owner; TopMemoryContext revert dissolves |
| IsTransactionBlock (4971) | lib.rs | MATCH | |
| IsTransactionOrTransactionBlock (4989) | lib.rs | MATCH | |
| TransactionBlockStatusCode (5003) | lib.rs | MATCH | I/T/E mapping; unreachable arm panics (C: elog FATAL on out-of-enum) |
| IsSubTransaction (5044) | lib.rs | MATCH | |
| StartSubTransaction (5067) | engine.rs | MATCH | |
| CommitSubTransaction (5104) | engine.rs | MATCH | full sequence in C order; parent nesting passed to portals; XID-lock delete gated on assigned xid; XactReadOnly restore |
| AbortSubTransaction (5219) | engine.rs | MATCH | same preamble as AbortTransaction; owner-gated block in C order; no snapbuild reset (as C) |
| CleanupSubTransaction (5383) | engine.rs | MATCH | |
| PushTransaction (5416) | engine.rs | MATCH | subxid wraparound ERROR 54000; field init incl. parallelChildXact propagation, savepoint inherit, GetUserIdAndSecContext |
| PopTransaction (5478) | engine.rs | MATCH | WARNING + FATAL-no-parent; relinking dissolves |
| EstimateTransactionStateSpace (5512) | engine.rs | MATCH | header 32 + 4/xid |
| SerializeTransactionState (5540) | engine.rs | MATCH | byte-exact layout; parallel re-pass; numeric (`xidComparator`) sort |
| StartParallelWorkerTransaction (5611) | engine.rs | MATCH | StartTransaction first; field restore; PARALLEL_INPROGRESS; adds bounds checks C lacks (safe-Rust superset) |
| EndParallelWorkerTransaction (5636) | engine.rs | MATCH | |
| ShowTransactionState (5648) | lib.rs | MATCH | DEBUG5 gate via message_level_is_interesting |
| ShowTransactionStateRec (5660) | lib.rs | MATCH | parent-first order via stack iteration; same format string; stack-depth guard unneeded (iterative) |
| BlockStateAsString (5707) | lib.rs | MATCH | all 20 strings |
| TransStateAsString (5760) | lib.rs | MATCH | |
| xactGetCommittedChildren (5790) | lib.rs | MATCH | clone instead of in-place pointer |
| XactLogCommitRecord (5814) | wal.rs | MATCH | opcode/xinfo derivation verbatim (incl. APPLY_FEEDBACK on sync_commit >= REMOTE_APPLY, DBINFO on nmsgs>0 || logical, GID only with logical, SPECIAL_REL_UPDATE); register order and byte layouts identical; XLOG_INCLUDE_ORIGIN; origin lsn/ts read at register time (same backend values) |
| XactLogAbortRecord (5986) | wal.rs | MATCH | abort-specific xinfo set (DBINFO only for 2PC+logical); same order |
| xact_redo_commit (6130) | redo.rs | MATCH | TransactionIdLatest → AdvanceNextFullTransactionIdPastXid → commit-ts → standbyState branch (commit tree vs known-assigned/async/expire/invals/AE-lock release) → replorigin_advance → rels/stats XLogFlush+drops → force-sync flush → apply-feedback reply; exact order |
| xact_redo_abort (6284) | redo.rs | MATCH | abort-tree both arms; no invals; same tail |
| xact_redo (6363) | redo.rs | MATCH | OPMASK dispatch; COMMIT/ABORT use record xid, *_PREPARED use parsed.twophase_xid + PrepareRedoRemove; PREPARE → PrepareRedoAdd; ASSIGNMENT gated on standbyState >= STANDBY_INITIALIZED; INVALIDATIONS no-op; PANIC on unknown. TwoPhaseStateLock acquire/release moved behind the twophase seam (documented; impl owns the lock) — see seam audit |
| ParseCommitRecord (xactdesc.c) | redo.rs `parse_commit_record` | MATCH | same conditional field walk; adds bounds checks (C trusts WAL bytes); gid kept whole (C strlcpy-truncates at GIDSIZE=200, but gids are validated ≤ GIDSIZE at PREPARE so no reachable difference) |
| ParseAbortRecord (xactdesc.c) | redo.rs `parse_abort_record` | MATCH | ditto |

GUC-backed globals (xact.c:78-87) are exposed as getter/setter pairs
(`XactIsoLevel`, `XactReadOnly`, … `IsolationUsesXactSnapshot`,
`IsolationIsSerializable`) — values and predicates match xact.h macros.

## Seam audit

- `crates/backend-access-transam-xact-seams` declares exactly two seams
  (`command_counter_increment`, `transaction_id_is_current_transaction_id`);
  both are installed by `init_seams()` (nothing but `set()` calls), and
  `seams-init::init_all()` calls `backend_access_transam_xact::init_seams()`.
  Signatures reconciled against the C prototypes. No `set()` calls outside the
  owner. ✓
- The ~44 outward per-owner seam crates created by this port
  (varsup/transam/subtrans/commit-ts/twophase/xlog/xloginsert/xlogutils/
  lmgr/proc/condvar/procarray/standby/sinval/inval/relmapper/typcache/
  snapmgr/portalmem/trigger/tablecmds/async/catalog-storage/pg-enum/
  namespace/index/smgr/bufmgr/aio/fd/spi/pgstat-xact/waitevent/more2/
  miscinit/origin/syncrep/launcher/lr-worker/logical/snapbuild/
  walreceiverfuncs/be-fsstubs) were spot-checked: declaration-only
  (`seam_core::seam!` + doc comments), no logic, no node construction. They
  panic loudly until their owners land — sanctioned. ✓
- Every outward call site is thin marshal + delegate (argument conversion,
  one call, result conversion); the only marshaling beyond that is byte
  serialization of C structs for `xlog_register_data`, which mirrors the C
  caller's own struct-to-bytes responsibility. ✓
- One delegation note: in `xact_redo`, C wraps `PrepareRedoRemove`/`PrepareRedoAdd`
  in `LWLockAcquire/Release(TwoPhaseStateLock)`. The port documents that the
  twophase seam implementations carry the lock. The lock pair is pure
  synchronization around a single twophase-owned call (no xact logic inside);
  the contract is recorded at the call sites and must be honored when
  twophase is ported. Accepted.
- Direct deps used (per catalog): elog, mcx, pg-prng, pqsignal
  (`signal_masks().unblock_sig()` for `sigprocmask(SIG_SETMASK, &UnBlockSig)`),
  backend_progress (`pgstat_progress_end_command`). ✓

## Findings and fixes

### Round 1 — FAIL: xact callback machinery diverged from C (fixed)

`RegisterXactCallback`/`CallXactCallbacks` (and SubXact twins) diverged from
xact.c:3804-3911 in four ways:

1. **Invocation order**: C prepends registrations (`item->next = Xact_callbacks;
   Xact_callbacks = item`), so callbacks run most-recently-registered first.
   The port pushed to the Vec tail and iterated front-to-back (oldest first).
2. **Mid-iteration registration**: C's saved-`next` walk never visits items
   prepended during the walk; the port's index walk would invoke callbacks
   registered during the same `CallXactCallbacks` round.
3. **Self-unregistration corruption**: the port's swap/restore by *index*
   would, when a callback unregistered itself (the C-sanctioned pattern),
   overwrite the *next* registration's closure with the removed one's and
   skip that callback for the round.
4. **Unregister scope**: C removes only the first `(callback, arg)` match;
   the port's `retain` removed every registration with the key.

**Fix** (commit on this branch): registrations now carry a unique `serial`,
`Register*` prepends, `Unregister*` removes the first match in list order, and
`Call*Callbacks` snapshots the serial list up front and walks it, taking each
closure out by serial and restoring it only if the registration still exists —
behaviorally equivalent to the C saved-`next` walk for every C-sanctioned
pattern. Regression tests added (`callback_tests`): newest-first order,
self-unregistration, and mid-iteration registration.

Re-audit of the fixed functions from scratch: verified against xact.c
3804-3911 line by line; all six now MATCH. `cargo test -p
backend-access-transam-xact` passes (3 tests).

No other findings. `MISSING`: none. `PARTIAL`: none. `DIVERGES`: none
remaining.

## Verdict

**PASS** (after fix round 1). Every function MATCH (or sanctioned-dissolved
per repo conventions); seam wiring clean; constants verified against headers.

## Design-debt fix round (2026-06-12, post-audit)

A design review against the newer repo rules (opacity is inherited never
introduced; no positional constant transcription) drove these
behavior-neutral reshapes:

- `SharedInvalidationMessage` is now the real `storage/sinval.h` union as an
  enum (`types-storage::sinval`), carried typed across the inval/standby
  seams and `ParsedCommit.msgs`; serialization to the 16-byte union form
  happens only at the XLog boundary (`wal.rs::inval_msgs_bytes`,
  `redo.rs` parse). The hand-transcribed `SHARED_INVAL_MESSAGE_SIZE = 16`
  magic in types-wal is gone (the constant lives next to the type and is
  checked by round-trip tests).
- `TransState`, `TBlockState`, `XactEvent`, `SubXactEvent` (types-core) and
  `HotStandbyState` (types-wal), `TimeoutId` (types-core) are real Rust
  enums, values verified against xact.c / xact.h / xlogutils.h / timeout.h.
  The C `elog(FATAL, "...unexpected state...")` default arms in
  `TransactionBlockStatusCode`, `CommitTransactionCommand`,
  `AbortCurrentTransaction`, and `AbortOutOfAnyTransaction` are now
  statically unreachable (exhaustive matches) and were removed.
- `RegisterXactCallback`/`RegisterSubXactCallback` return typed registration
  tokens (the C `(callback, arg)` identity) instead of taking a
  caller-chosen `usize` key.
- Allocating paths whose C counterparts palloc are now fallible:
  `prepareGID`/savepoint-name strdups (`try_strdup`), the
  `AssignTransactionId` parents workspace, `xactGetCommittedChildren`
  (returns `PgResult`), `SerializeTransactionState`'s parallel-xids copy,
  and the `Call*Callbacks` serial snapshots.
- **Ledgered divergence** (also in the module doc and DESIGN_DEBT.md):
  transaction-lifetime collections (`childXids`, savepoint names,
  `prepareGID`, `unreportedXids`, `ParallelCurrentXids`) remain std
  `Vec`/`String` in the `thread_local!` `XactState` rather than
  `PgVec<'mcx>` in `TopTransactionContext` — the state cannot borrow a
  context it owns. OOM surfaces match C; context accounting/reset coupling
  is the open debt.
- pg-prng's process-wide `Mutex` global became `thread_local!`
  (`pg_global_prng_state` is backend-private in C).
