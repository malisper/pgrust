# Audit: backend-postmaster-bgwriter

C source: `src/backend/postmaster/bgwriter.c` (PostgreSQL 18.3, single file).
Port: `crates/backend-postmaster-bgwriter/src/lib.rs`.
c2rust: `c2rust-runs/backend-postmaster-bgwriter/src/bgwriter.rs`.

Re-derived independently from the C source; the file defines exactly **one**
function plus three file-scope objects. Everything `BackgroundWriterMain`
invokes (`BgBufferSync`, `WritebackContextInit`, `StrategyNotifyBgWriter`,
`FirstCallSinceLastCheckpoint`, `LogStandbySnapshot`, the abort-cleanup
substrate, the latch syscalls, the stats reports) is owned by *other* catalog
units, reached either through the real ported owner crate (bufmgr, checkpointer,
activity-small) or through that owner's `-seams` crate.

## File-scope globals / constants

| C object (line) | Port | Verdict | Notes |
|---|---|---|---|
| `int BgWriterDelay = 200` (58) | `BGWRITER_DELAY` thread_local `Cell<i32>` + `BgWriterDelay()`/`set_BgWriterDelay()` | MATCH | per-backend GUC global → process-local cell (AGENTS.md backend-global-state); default 200 |
| `#define HIBERNATE_FACTOR 50` (64) | `const HIBERNATE_FACTOR: i32 = 50` | MATCH | |
| `#define LOG_SNAPSHOT_INTERVAL_MS 15000` (70) | `const LOG_SNAPSHOT_INTERVAL_MS: TimestampTz = 15000` | MATCH | |
| `static TimestampTz last_snapshot_ts` (77) | `LoopState.last_snapshot_ts` | MATCH | per-backend state, threaded through the loop |
| `static XLogRecPtr last_snapshot_lsn = InvalidXLogRecPtr` (78) | `LoopState.last_snapshot_lsn`, init `InvalidXLogRecPtr` | MATCH | |

## Functions

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `BackgroundWriterMain` (88-339) | `BackgroundWriterMain` + `error_recovery` + `main_loop_cycle` | MATCH | decomposed into setup / sigsetjmp landing-pad / per-cycle body; see block-by-block below |

### `BackgroundWriterMain` block-by-block

* `Assert(startup_data_len == 0)` → `debug_assert!(matches!(startup_data, StartupData::None))` — MATCH.
* `MyBackendType = B_BG_WRITER; AuxiliaryProcessMainCommon()` →
  `miscinit::set_my_backend_type_bg_writer::call()` (newly declared + installed
  in the miscinit owner, mirroring `set_my_backend_type_checkpointer`) +
  `auxprocess::auxiliary_process_main_common::call()?` — MATCH (SEAMED).
* `pqsignal(...)` block (SIGHUP/SIGINT/SIGTERM/SIGALRM/SIGPIPE/SIGUSR1/SIGUSR2)
  + `pqsignal(SIGCHLD, SIG_DFL)` → host/aux-process-bootstrap-owned, exactly as
  the landed checkpointer port treats its identical `pqsignal` block (comment in
  source). MATCH (host-owned, consistent with sibling).
* `last_snapshot_ts = GetCurrentTimestamp()` → `LoopState.last_snapshot_ts =
  timestamp::get_current_timestamp::call()` — MATCH.
* `AllocSetContextCreate(...,"Background Writer",...)` + `MemoryContextSwitchTo`
  → host-owned memory context (per-cycle allocs owned by the cycle in this port,
  same treatment as checkpointer). MATCH.
* `WritebackContextInit(&wb_context, &bgwriter_flush_after)` →
  `writeback_context_init(&mut wb_context, bufmgr::bgwriter_flush_after::call())`
  over the real `WritebackContext` (bufmgr crate). MATCH.
* `sigsetjmp(local_sigjmp_buf, 1) != 0` landing pad (163-218) → outer `loop`
  whose body (`main_loop_cycle`) returns `PgResult`; `Err` runs `error_recovery`
  then `prev_hibernate = false`. The C re-enters `for(;;)` after longjmp; the
  port re-enters the outer loop. MATCH.
  * error branch step order verified identical: HOLD_INTERRUPTS, EmitErrorReport,
    LWLockReleaseAll, ConditionVariableCancelSleep, pgaio_error_cleanup,
    UnlockBuffers, ReleaseAuxProcessResources(false), AtEOXact_Buffers(false),
    AtEOXact_SMgr, AtEOXact_Files(false), AtEOXact_HashTables(false),
    FlushErrorState (+ host-owned ctx reset), WritebackContextInit,
    RESUME_INTERRUPTS, pg_usleep(1000000L), pgstat_report_wait_end. MATCH.
    `EmitErrorReport()` → `emit_error_report_for(err)` (error carried by the
    `Err` value), same idiom the checkpointer audit accepted.
* `PG_exception_stack = &local_sigjmp_buf; sigprocmask(SIG_SETMASK, &UnBlockSig)`
  → the host/aux bootstrap unblocks signals; the Rust landing pad is the outer
  loop. MATCH (host-owned framing).
* `prev_hibernate = false` (after exception stack set) → `LoopState.prev_hibernate
  = false` initial, and reset to false in the `Err` arm. MATCH.
* `for (;;)` body (230-338) → `main_loop_cycle`:
  * `ResetLatch(MyLatch)` → `latch::reset_latch_my_latch::call()` — MATCH.
  * `ProcessMainLoopInterrupts()` → `interrupt::ProcessMainLoopInterrupts()?`
    (ported; proc_exit(0)s on ShutdownRequestPending, ereport→Err). MATCH.
  * `can_hibernate = BgBufferSync(&wb_context)` → real
    `BufferManager::BgBufferSync(&mut wb_context, &mut bg_buffer_sync_state)?`;
    the `BgBufferSyncState` cross-call state (C function-statics) is owned by the
    loop and threaded back in, per the bufmgr crate's documented contract. MATCH.
  * `pgstat_report_bgwriter()` → `backend_utils_activity_small::pgstat_report_bgwriter()?`
    (real ported owner). MATCH.
  * `pgstat_report_wal(true)` → `walstats::pgstat_report_wal::call(true)` — MATCH.
  * `if (FirstCallSinceLastCheckpoint()) smgrdestroyall()` →
    `if backend_postmaster_checkpointer::FirstCallSinceLastCheckpoint() {
    smgr::smgrdestroyall::call()? }` (real checkpointer accessor; smgr via seam).
    MATCH.
  * `if (XLogStandbyInfoActive() && !RecoveryInProgress())` snapshot block →
    same predicate via `xlog::xlog_standby_info_active` /
    `xlog::recovery_in_progress`; `timeout =
    TimestampTzPlusMilliseconds(last_snapshot_ts, LOG_SNAPSHOT_INTERVAL_MS)`;
    `if now >= timeout && last_snapshot_lsn <= GetLastImportantRecPtr()` (the
    `<=` per the C comment) then `last_snapshot_lsn = LogStandbySnapshot();
    last_snapshot_ts = now`. MATCH. `TimestampTzPlusMilliseconds` = `tz + ms*1000`
    (us granularity) — MATCH against utils/timestamp.h.
  * `rc = WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH,
    BgWriterDelay, WAIT_EVENT_BGWRITER_MAIN)` → `latch::wait_latch_my_latch::call(
    WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, BgWriterDelay() as i64,
    WAIT_EVENT_BGWRITER_MAIN)?`. MATCH. (`WAIT_EVENT_BGWRITER_MAIN` newly added to
    types-pgstat as `PG_WAIT_ACTIVITY + 3`, verified against the Activity-section
    ordering in wait_event_names.txt.)
  * hibernation: `if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate) {
    StrategyNotifyBgWriter(MyProcNumber); WaitLatch(..., BgWriterDelay *
    HIBERNATE_FACTOR, WAIT_EVENT_BGWRITER_HIBERNATE); StrategyNotifyBgWriter(-1) }`
    → identical guard; `buffer_manager().StrategyNotifyBgWriter(MyProcNumber())?`,
    `wait_latch_my_latch(..., BgWriterDelay() as i64 * HIBERNATE_FACTOR as i64,
    WAIT_EVENT_BGWRITER_HIBERNATE)?`, `StrategyNotifyBgWriter(INVALID_PROC_NUMBER)?`
    (C `-1` == `INVALID_PROC_NUMBER`). MATCH.
  * `prev_hibernate = can_hibernate` → MATCH.

## Seam audit

Owned inward seam crate: `backend-postmaster-bgwriter-seams` — declares
`background_writer_main(&StartupData) -> !`. Installed by this crate's
`init_seams()` (`background_writer_main::set(background_writer_main_entry)`), and
`seams-init::init_all()` calls `backend_postmaster_bgwriter::init_seams()`. The
`-> !` entry adapter runs `BackgroundWriterMain` and `proc_exit`s on the
(in-practice unreachable) return — same shape as `checkpointer_main_entry`. OK.

Outward seam calls — each is a thin delegate into an owner this crate cannot
depend on directly without a cycle, or whose owner is unported (mirror-and-panic):
auxprocess, latch, lwlock, condition-variable, aio, fd, smgr, dsm-core
(`proc_exit`), init-small (`my_proc_pid`/`my_proc_number`), miscinit
(`set_my_backend_type_bg_writer`/`hold_interrupts`/`resume_interrupts`),
resowner, dynahash, waitevent, pgstat-wal, timestamp, xlog, port-pgsleep, and the
`bgwriter_flush_after` GUC accessor. No branching/computation lives in any seam
path — all marshal+delegate. OK.

Real ported owners driven directly (no cycle — the bgwriter is a top-level leaf):
`backend-storage-buffer-bufmgr` (BgBufferSync / WritebackContextInit /
StrategyNotifyBgWriter / WritebackContext / BgBufferSyncState),
`backend-postmaster-checkpointer` (FirstCallSinceLastCheckpoint),
`backend-utils-activity-small` (pgstat_report_bgwriter),
`backend-postmaster-interrupt` (ProcessMainLoopInterrupts),
`backend-utils-error` (EmitErrorReport/FlushErrorState). OK.

### New cross-unit declarations introduced by this port

* `types_pgstat::wait_event::WAIT_EVENT_BGWRITER_{MAIN,HIBERNATE}` — values
  derived from the Activity-section order (ARCHIVER_MAIN=0, AUTOVACUUM_MAIN=1,
  BGWRITER_HIBERNATE=2, BGWRITER_MAIN=3, ...). Correct.
* `backend-utils-init-miscinit-seams::set_my_backend_type_bg_writer` — declared
  in the seam crate and **installed** by the miscinit owner's `init_seams`
  (`SetMyBackendType(BackendType::BgWriter)`), exactly paralleling the
  checkpointer/walsummarizer entries. No new uninstalled inward seam.
* `backend-storage-buffer-bufmgr-seams::bgwriter_flush_after` — the
  `int bgwriter_flush_after` bufmgr.c GUC global. Same class as the existing
  `checkpoint_flush_after` GUC seam: no backing GUC variable installed in the
  owner (the GUC machinery seeds the boot value but does not `::set` the seam).
  Added to the seams-init `CONTRACT_RECONCILE_PENDING` allowlist with a note
  (alongside maintenance_io_concurrency / io_method_sync), because the consumer
  (bgwriter) is a different crate than the owner so the OUTWARD-seam exclusion
  that hides `checkpoint_flush_after` does not apply. Tracked debt, not a
  regression; retire when the GUC source installs these accessors.
* `BufferManager::StrategyNotifyBgWriter` — new public method delegating to the
  existing `BufferStrategyControl::notify_bgwriter`; pure wiring, no new logic.
* bufmgr lib re-exports `WritebackContext` / `BgBufferSyncState` /
  `writeback_context_init` (already `pub` items in a private module). No logic
  change.

## Design conformance

No invented opacity (all carriers are the real ported types). No shared statics
for per-backend globals (`BgWriterDelay`, `last_snapshot_*`, `prev_hibernate`,
`wb_context`, `bg_buffer_sync_state` are all process-local / loop-owned). No
ambient-global seams introduced. No locks held across `?`. No registry-shaped
side tables. The one ledger entry (`bgwriter_flush_after`) is recorded in
`CONTRACT_RECONCILE_PENDING`.

## Verdict: PASS

Every C object and the single C function are `MATCH` (or `SEAMED` per the seam
rules). The owned inward seam is installed and wired. Gates: `cargo check
--workspace`, `cargo test -p no-todo-guard`, and `cargo test -p seams-init`
(both recurrence guards) all green; crate unit tests pass.
