# Audit: backend-replication-logical-logical

- **Unit:** `backend-replication-logical-logical`
- **C source:** `src/backend/replication/logical/logical.c` (PostgreSQL 18.3)
- **Port crate:** `crates/backend-replication-logical-logical/src/lib.rs`
- **c2rust reference:** `../pgrust/c2rust-runs/backend-replication-logical-logical/src/logical.rs`
- **Date:** 2026-06-12
- **Model:** Opus
- **Verdict:** PASS

This is an independent audit: every function was re-derived from the C and
compared against the port (control flow, error paths, error messages/SQLSTATEs,
constants, edge cases) without trusting the prior workflow audit (whose report
was never committed).

## Function inventory

All 41 function definitions in `logical.c` were enumerated and compared. The 22
static `*_cb_wrapper` forward declarations (logical.c:59-110) resolve to the
wrapper definitions below.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `CheckLogicalDecodingRequirements` | 111 | lib.rs:266 | MATCH | wal_level/MyDatabaseId threaded as params; both ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE errmsgs verbatim; standby GetActiveWalLevelOnStandby branch present; CheckSlotRequirements seamed |
| `StartupDecodingContext` | 152 | lib.rs:306 | MATCH | palloc0 field init, !fast_forward LoadOutputPlugin, !IsTransactionOrTransactionBlock proc-flag block, XLogReaderAllocate OOM ereport, streaming/twophase capability OR-chains identical; reorderbuffer callback wiring seamed (set unconditionally as in C) |
| `CreateInitDecodingContext` | 332 | lib.rs:447 | MATCH | all 5 sanity ereports + SQLSTATEs match; namestrcpy/mutex plugin register; restart_lsn reserve-vs-set branch; ProcArray/SlotControl lock order; xmin horizon block; `twophase &= two_phase`; NIL output_plugin_options passed |
| `CreateDecodingContext` | 500 | lib.rs:596 | MATCH | physical/db-mismatch(+!fast_forward)/synced-slot ereports; invalidated/restart_lsn asserts; start_lsn forward-to-confirmed_flush + LOG message; `twophase &= (two_phase \|\| twophase_opt_given)`; two-phase mark block; final LOG ereport |
| `DecodingContextReady` | 626 | lib.rs:742 | MATCH | == SNAPBUILD_CONSISTENT (2) |
| `DecodingContextFindStartpoint` | 635 | lib.rs:747 | MATCH | XLogBeginRead; read loop with err/!record elog(ERROR); ready-break; CHECK_FOR_INTERRUPTS; confirmed_flush/two_phase_at set under mutex |
| `FreeDecodingContext` | 679 | lib.rs:797 | MATCH | shutdown_cb guard; free reorder/snapbuild/reader; MemoryContextDelete |
| `OutputPluginPrepareWrite` | 694 | lib.rs:811 | MATCH | !accept_writes elog(ERROR); prepared_write=true |
| `OutputPluginWrite` | 707 | lib.rs:824 | MATCH | !prepared_write elog(ERROR); prepared_write=false |
| `OutputPluginUpdateProgress` | 720 | lib.rs:837 | MATCH | !update_progress early return |
| `LoadOutputPlugin` | 735 | lib.rs:853 | MATCH | _PG_output_plugin_init missing-symbol error owned by dfmgr seam; begin/change/commit required-callback elog(ERROR) messages verbatim |
| `output_plugin_error_callback` | 757 | lib.rs:877 | MATCH | both errcontext branches (with/without associated LSN) verbatim |
| `startup_cb_wrapper` | 776 | lib.rs:917 | MATCH | accept_writes=false,end_xact=false; assert !fast_forward |
| `shutdown_cb_wrapper` | 804 | lib.rs:937 | MATCH | same output-state setup |
| `begin_cb_wrapper` | 837 | lib.rs:956 | MATCH | accept_writes=true, write_xid=xid, write_location=first_lsn, end_xact=false |
| `commit_cb_wrapper` | 868 | lib.rs:982 | MATCH | report_location=final_lsn, write_location=end_lsn, end_xact=true |
| `begin_prepare_cb_wrapper` | 907 | lib.rs:1010 | MATCH | twophase assert; mandatory begin_prepare_cb guard ("at prepare time") |
| `prepare_cb_wrapper` | 951 | lib.rs:1041 | MATCH | mandatory prepare_cb guard |
| `commit_prepared_cb_wrapper` | 996 | lib.rs:1074 | MATCH | mandatory commit_prepared_cb guard |
| `rollback_prepared_cb_wrapper` | 1041 | lib.rs:1107 | MATCH | prepare_end_lsn+prepare_time args; mandatory rollback_prepared_cb guard |
| `change_cb_wrapper` | 1088 | lib.rs:1145 | MATCH | write_location=change_lsn, end_xact=false |
| `truncate_cb_wrapper` | 1127 | lib.rs:1177 | MATCH | optional truncate_cb early return; nrelations/relations args |
| `filter_prepare_cb_wrapper` | 1169 | lib.rs:1215 | MATCH | accept_writes=false; returns bool |
| `filter_by_origin_cb_wrapper` | 1201 | lib.rs:1239 | MATCH | accept_writes=false; returns bool |
| `message_cb_wrapper` | 1232 | lib.rs:1262 | MATCH | optional message_cb early return; write_xid = txn?xid:Invalid |
| `stream_start_cb_wrapper` | 1269 | lib.rs:1305 | MATCH | streaming assert; mandatory stream_start_cb guard ("logical streaming requires") |
| `stream_stop_cb_wrapper` | 1318 | lib.rs:1336 | MATCH | mandatory stream_stop_cb guard |
| `stream_abort_cb_wrapper` | 1367 | lib.rs:1367 | MATCH | end_xact=true; mandatory stream_abort_cb guard |
| `stream_prepare_cb_wrapper` | 1408 | lib.rs:1398 | MATCH | streaming+twophase asserts; "logical streaming at prepare time requires" guard (distinct message) |
| `stream_commit_cb_wrapper` | 1453 | lib.rs:1438 | MATCH | mandatory stream_commit_cb guard |
| `stream_change_cb_wrapper` | 1494 | lib.rs:1471 | MATCH | mandatory stream_change_cb guard |
| `stream_message_cb_wrapper` | 1543 | lib.rs:1508 | MATCH | optional stream_message_cb early return |
| `stream_truncate_cb_wrapper` | 1584 | lib.rs:1553 | MATCH | optional stream_truncate_cb early return |
| `update_progress_txn_cb_wrapper` | 1631 | lib.rs:1595 | MATCH | accept_writes=false; delegates to OutputPluginUpdateProgress(ctx,false) |
| `LogicalIncreaseXminForSlot` | 1678 | lib.rs:1612 | MATCH | empty first `if` branch preserved; 3-way candidate logic; got_new_xmin DEBUG1; updated_xmin -> LogicalConfirmReceivedLocation |
| `LogicalIncreaseRestartDecodingForSlot` | 1746 | lib.rs:1664 | MATCH | restart_lsn/current_lsn asserts; 4-way branch incl. failure-DEBUG1 with all 5 LSN args; mutex release placement per-branch matches |
| `LogicalConfirmReceivedLocation` | 1822 | lib.rs:1735 | MATCH | unlocked candidate pre-check; confirmed_flush no-backwards; xmin/restart apply blocks; injection-point segment-change check seamed with old+new restart_lsn; updated_xmin effective_catalog_xmin + ComputeRequiredXmin/LSN; else-branch confirmed_flush update |
| `ResetLogicalStreamingState` | 1944 | lib.rs:1837 | MATCH | CheckXidAlive=Invalid, bsysscan=false (xact seam) |
| `UpdateDecodingStats` | 1954 | lib.rs:1843 | MATCH | spill/stream/total<=0 early return; DEBUG2 stats line; pgstat_report_replslot; reset all 8 counters |
| `LogicalReplicationSlotHasPendingWal` | 2001 | lib.rs:1876 | MATCH | PG_TRY/PG_CATCH InvalidateSystemCaches-and-rethrow; CreateDecodingContext fast_forward; loop bound !has_pending_wal && EndRecPtr<end_of_wal; processing_required read via decode seam |
| `LogicalSlotAdvanceAndCheckSnapState` | 2083 | lib.rs:1951 | MATCH | found_consistent_snapshot=false pre-set before body; WaitForStandbyConfirmation; loop EndRecPtr<moveto; CurrentResourceOwner restore; EndRecPtr!=Invalid -> Confirm+MarkDirty; retlsn=confirmed_flush; PG_CATCH cleanup |

## Constants verified against headers

- `WAL_LEVEL_LOGICAL`, `SNAPBUILD_CONSISTENT`(2)/`START`(-1)/`BUILDING`(0)/`FULL`(1),
  `RS_INVAL_NONE`(0), `OUTPUT_PLUGIN_BINARY/TEXTUAL_OUTPUT` (0/1),
  `InvalidXLogRecPtr`(0), `InvalidTransactionId`, `FIRST_NORMAL_TRANSACTION_ID`(3,
  used in TransactionIdPrecedesOrEquals modular compare) — all match.
- `OutputPluginCallbacks` presence bitmask field order matches the C struct
  (startup..stream_truncate, 21 callbacks), used only for NULL-presence tests.
- SQLSTATEs: ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE on all requirement/
  mandatory-callback ereports, ERRCODE_ACTIVE_SQL_TRANSACTION on the
  writes-in-txn check, ERRCODE_OUT_OF_MEMORY on reader-alloc — all match.
- Error/log severities: ERROR, LOG, DEBUG1, DEBUG2 placements match C.

## Seam audit

Owned seam crate (by C-source coverage of `logical.c`):
`crates/backend-replication-logical-logical-seams`.

- Declares exactly one inward seam, `dispatch_reorderbuffer_callback(cb)`,
  installed by `backend_replication_logical_logical::init_seams()`, which
  contains only the `set()` call. `seams-init::init_all()` calls `init_seams()`
  (crates/seams-init/src/lib.rs:26). No uninstalled declaration; no `set()`
  outside the owner.
- All 41 functions' bodies live in this crate. No function body was replaced by
  a seam-call-to-elsewhere (no MISSING-by-delegation). The per-callback wrapper
  logic, the capability computation, the requirement checks, and the entire
  candidate xmin/restart state machine are in-crate.
- Outward seam calls (slot/snapbuild/reorderbuffer/xlogreader/xlog/xact/
  procarray/inval/walsender/resowner/mcxt/dfmgr/decode/tcop) are thin
  marshal+delegate: argument conversion, one call, result conversion. No
  branching/node-construction/computation observed inside a seam path. Each
  owner is unported, so the seams panic loudly (REAL-OR-LOUD) — acceptable
  per the skill (panicking on an unported callee is fine).
- The installed `dispatch_reorderbuffer_callback_seam` thunk panics because the
  seam carries only `cb` while the live `&mut LogicalDecodingContext` is
  `rb->private_data`, resolved by the (unported) reorderbuffer owner. The real
  per-cb dispatch logic (`dispatch_reorderbuffer_callback`) is fully present
  in-crate. Ledgered in DESIGN_DEBT.md.

## Design conformance

- Cross-subsystem pointers (reader/reorder/snapbuilder/out/context/slot and the
  txn/relation/change/prefix/message/gid handles + output_plugin_options) are
  modeled as opaque handle newtypes resolved by their owners — `logical.c` only
  forwards, never dereferences them. Ledgered (DESIGN_DEBT.md "Logical-decoding
  cross-subsystem handles modeled as opaque tokens").
- `wal_level`, `wal_segment_size`, `MyDatabaseId` threaded as explicit params
  (no-ambient-global-seams rule) — conforms.
- Inward dispatch seam carrying no ctx — ledgered (DESIGN_DEBT.md
  "Logical-decoding inward dispatch seam carries no ctx").
- No invented opacity, no shared statics for per-backend globals, no
  registry-shaped side tables, no unledgered divergence markers found.

## Re-derived spot checks

Re-derived in full from C without trusting the port:
`LogicalConfirmReceivedLocation` (the trickiest: unlocked pre-check, two apply
blocks, injection-point segment compare, post-disk effective-xmin advance),
`LogicalIncreaseRestartDecodingForSlot` (4-way branch with per-branch mutex
release), `CreateDecodingContext` (the `twophase_opt_given` distinction vs
`CreateInitDecodingContext`), and `LogicalSlotAdvanceAndCheckSnapState`
(out-param pre-init, PG_CATCH rethrow). All confirmed MATCH.

## Build

`cargo check -p backend-replication-logical-logical -p
backend-replication-logical-logical-seams` is clean (only unrelated
doc-comment-on-macro warnings from a dependency).

## Verdict: PASS

Every function MATCH (or thin SEAMED outward call per the rules); zero seam
findings; design rules conform with all divergences ledgered. The unit may
merge.

---

## Re-audit (reconcile/slot-seams, 2026-06-12, Opus)

Triggered by the slot-seams shared-vocabulary reconciliation: every `slot::*`
call site was rewritten from logical-logical's PascalCase accessor guesses to
`backend-replication-slot`'s authoritative snake_case seam contract. This is a
seam-name/type change, not a logic change — each call still maps to the exact
same C operation on `MyReplicationSlot`/`slot.c`. Re-derived every changed call
site from the C:

- `CheckLogicalDecodingRequirements` (logical.c:110): `CheckSlotRequirements()`
  -> `check_slot_requirements(wal_level.0)` — the owner's seam takes the
  `wal_level` value explicitly (no ambient global), and `CheckSlotRequirements`
  in slot.c reads exactly that global. MATCH.
- `LogicalIncreaseXminForSlot` / `LogicalIncreaseRestartDecodingForSlot` /
  `LogicalConfirmReceivedLocation`: the `MyReplicationSlot->{data.*,candidate_*}`
  reads/writes under `SpinLockAcquire(&MyReplicationSlot->mutex)` now go through
  `slot::slot_*` / `slot::slot_set_*` + `slot::slot_mutex_{acquire,release}`;
  `ReplicationSlotMarkDirty`/`ReplicationSlotSave` -> snake_case;
  `ReplicationSlotsComputeRequired{Xmin,LSN}` -> snake_case with `?` (the owner
  declares them fallible). `ReplicationSlotControlLock` acquire/release and the
  `ProcArrayLock` interplay preserved. MATCH.
- `slot_invalidated()` now returns the typed
  `types_replication_slot::ReplicationSlotInvalidationCause`; the
  `Assert(... == RS_INVAL_NONE)` compares against the typed `RS_INVAL_NONE`
  variant. MATCH.
- `UpdateDecodingStats` (logical.c:1983): `pgstat_report_replslot(ctx->slot,
  &repSlotStat)` -> `slot::pgstat_report_replslot(stats)`; the slot owner
  resolves `MyReplicationSlot`'s index and forwards to pgstat_replslot.c's seam.
  MATCH.
- `RecoveryInProgress()` -> `xlog::recovery_in_progress()`;
  `IsSyncingReplicationSlots()` -> `slotsync::is_syncing_replication_slots()`
  (re-homed to its proper owner). `GetActiveWalLevelOnStandby()` unchanged.
  MATCH.

`cargo check -p backend-replication-logical-logical` is clean. (The full
`cargo check/test --workspace` GATE could not be run: the shared disk volume is
at 100%, so a full-tree build exhausts space; every individually-touched crate
compiles.)

## Verdict: PASS
