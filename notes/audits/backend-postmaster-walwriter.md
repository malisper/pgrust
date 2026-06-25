# Audit: backend-postmaster-walwriter

- **C source**: `src/backend/postmaster/walwriter.c` (PostgreSQL 18.3)
- **src-idiomatic ref**: `src-idiomatic/crates/backend-postmaster-walwriter`
- **Port**: `crates/backend-postmaster-walwriter/src/lib.rs`
- **Audit verdict**: **PASS**

Independent re-derivation from the C; the port's comments and self-review were
not trusted.

## 1. Function inventory (every C definition)

walwriter.c has exactly **one** function definition: `WalWriterMain`. The port
decomposes its `for(;;)` body and its sigsetjmp landing pad into private
helpers, all logic-preserving.

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `WalWriterMain` (80-243) | `WalWriterMain` + `main_loop_cycle` + `walwriter_abort_cleanup` | MATCH | See breakdown below. |

### `WalWriterMain` setup (80-201)

| C step (line) | Port | Verdict |
|---|---|---|
| `Assert(startup_data_len == 0)` (88) | `debug_assert!(matches!(startup_data, StartupData::None))` | MATCH |
| `MyBackendType = B_WAL_WRITER` (90) | `miscinit::set_my_backend_type_wal_writer::call()` | MATCH (new seam in miscinit owner → `SetMyBackendType(BackendType::WalWriter)`) |
| `AuxiliaryProcessMainCommon()` (91) | `auxprocess::auxiliary_process_main_common::call()?` | MATCH |
| pqsignal block + SIGCHLD reset (98-114) | host aux-process bootstrap | MATCH (delegated; documented) |
| `AllocSetContextCreate("Wal Writer")` + switch (122-126) | host per-process memctx lifecycle | MATCH (delegated; documented) |
| `sigsetjmp(local_sigjmp_buf, 1) != 0` block (145-187) | outer `loop { match … Err => walwriter_abort_cleanup }` | MATCH (see §2) |
| `PG_exception_stack = &local_sigjmp_buf` (190) | implicit (the outer recovery loop) | MATCH |
| `sigprocmask(SIG_SETMASK, &UnBlockSig)` (195) | host aux bootstrap | MATCH (delegated; documented) |
| `left_till_hibernate = LOOPS_UNTIL_HIBERNATE; hibernating = false` (198-199) | `LoopState { left_till_hibernate: LOOPS_UNTIL_HIBERNATE, hibernating: false }` | MATCH |
| `SetWalWriterSleeping(false)` (200) | `SetWalWriterSleeping(false)` (real ported xlog crate) | MATCH |
| `ProcGlobal->walwriterProc = MyProcNumber` (201) | `proc::set_walwriter_proc_to_self::call()?` | MATCH (new seam in proc owner → `pg.walwriterProc = MyProcNumber`) |

### `for(;;)` body — `main_loop_cycle` (203-242)

| C step (line) | Port | Verdict |
|---|---|---|
| `if (hibernating != (left_till_hibernate <= 1)) { hibernating = …; SetWalWriterSleeping(hibernating); }` (206-211) | `if let Some(h) = state.recompute_hibernation() { SetWalWriterSleeping(h); }` | MATCH (flag only re-published on change) |
| `ResetLatch(MyLatch)` (214) | `latch::reset_latch_my_latch::call()` | MATCH |
| `ProcessMainLoopInterrupts()` (217) | `interrupt::ProcessMainLoopInterrupts()?` | MATCH (ported crate; ShutdownRequestPending→proc_exit(0) inside it) |
| `if (XLogBackgroundFlush()) left_till_hibernate = LOOPS_UNTIL_HIBERNATE; else if (left_till_hibernate > 0) left_till_hibernate--;` (221-225) | `let w = XLogBackgroundFlush()?; state.apply_flush_result(w);` | MATCH (XLogBackgroundFlush is real ported xlog → `PgResult<bool>`; the `Err` is the C `ereport(ERROR)` longjmp routed to the recovery loop) |
| `pgstat_report_wal(false)` (228) | `walstats::pgstat_report_wal::call(false)` | MATCH |
| `cur_timeout = (left_till_hibernate > 0) ? WalWriterDelay : WalWriterDelay * HIBERNATE_FACTOR` (235-238) | `state.cur_timeout()` | MATCH |
| `WaitLatch(MyLatch, WL_LATCH_SET\|WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH, cur_timeout, WAIT_EVENT_WAL_WRITER_MAIN)` (239-242) | `latch::wait_latch_my_latch::call(WL_LATCH_SET\|WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH, cur_timeout, WAIT_EVENT_WAL_WRITER_MAIN)?` | MATCH |

## 2. sigsetjmp landing pad — `walwriter_abort_cleanup` (145-187)

The C landing pad runs once per error then *falls through* to the post-recovery
resets (L196-201) and re-enters `for(;;)`. The port realizes this as the outer
`loop`: on `Err`, it runs `walwriter_abort_cleanup`, then **replays** the
post-recovery resets (`left_till_hibernate`/`hibernating`/`SetWalWriterSleeping(false)`/
`set_walwriter_proc_to_self`) and loops back into the body. Behaviorally identical.

| C step (line) | Port | Verdict |
|---|---|---|
| `error_context_stack = NULL` (148) | folded into `FlushErrorState()` (host-owned framing) | MATCH |
| `HOLD_INTERRUPTS()` (151) | `miscinit::hold_interrupts::call()` | MATCH |
| `EmitErrorReport()` (154) | `backend_utils_error::emit_error_report_for(err)` | MATCH (the caught `PgError` is the reported error) |
| `LWLockReleaseAll()` (161) | `lwlock::lwlock_release_all::call()` | MATCH |
| `ConditionVariableCancelSleep()` (162) | `cv::condition_variable_cancel_sleep::call()` | MATCH |
| `pgstat_report_wait_end()` (163) | `waitevent::pgstat_report_wait_end::call()` | MATCH |
| `pgaio_error_cleanup()` (164) | `aio::pgaio_error_cleanup::call()` | MATCH |
| `UnlockBuffers()` (165) | `bufmgr::unlock_buffers::call()` | MATCH |
| `ReleaseAuxProcessResources(false)` (166) | `resowner::release_aux_process_resources::call(false)?` | MATCH |
| `AtEOXact_Buffers(false)` (167) | `bufmgr::at_eoxact_buffers::call(false)` | MATCH |
| `AtEOXact_SMgr()` (168) | `smgr::at_eoxact_smgr::call()` | MATCH |
| `AtEOXact_Files(false)` (169) | `fd::at_eoxact_files::call(false)` | MATCH |
| `AtEOXact_HashTables(false)` (170) | `dynahash::at_eoxact_hash_tables::call(false)` | MATCH |
| `MemoryContextSwitchTo(walwriter_context)` (176) | host-owned | MATCH (delegated) |
| `FlushErrorState()` (177) | `backend_utils_error::FlushErrorState()` | MATCH |
| `MemoryContextReset(walwriter_context)` (180) | host-owned | MATCH (delegated) |
| `RESUME_INTERRUPTS()` (183) | `miscinit::resume_interrupts::call()` | MATCH |
| `pg_usleep(1000000L)` (186) | `ipc_pg_usleep(1_000_000)` → `port_pgsleep_seams::pg_usleep::call` | MATCH |

Order is preserved exactly (the C calls `pgstat_report_wait_end()` interleaved
within the AtEOXact block; the port keeps the same relative ordering).

## 3. Constants verified against C headers

| Constant | Port | C | Result |
|---|---|---|---|
| `LOOPS_UNTIL_HIBERNATE` | `50` | walwriter.c:72 | MATCH |
| `HIBERNATE_FACTOR` | `25` | walwriter.c:73 | MATCH |
| `WalWriterDelay` default | `200` | walwriter.c:65 | MATCH |
| `WalWriterFlushAfter` default | `DEFAULT_WAL_WRITER_FLUSH_AFTER` | walwriter.c:66 | MATCH |
| `DEFAULT_WAL_WRITER_FLUSH_AFTER` | `(1024*1024)/XLOG_BLCKSZ` = 128 | xlog.h | MATCH |
| `WAIT_EVENT_WAL_WRITER_MAIN` | `PG_WAIT_ACTIVITY + 17` | wait_event_names.txt Activity §, 0-based index 17 (last entry) | MATCH (verified ordering against PG source) |
| Wait flags | `WL_LATCH_SET\|WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH` | walwriter.c:240 | MATCH |

## 4. Divergences / model notes

- **GUCs as `thread_local`** (WalWriterDelay/WalWriterFlushAfter): backend-global
  per-process state per AGENTS.md, matching the checkpointer port. No divergence.
- **sigsetjmp → outer `Err`-recovery loop**: the canonical workspace model for
  aux-process main loops (identical to checkpointer/walsummarizer). No divergence.
- **`XLogBackgroundFlush` returns `PgResult<bool>`**: the ported xlog crate's real
  signature; the `Err` faithfully carries the C `ereport(ERROR)` longjmp into the
  recovery loop. `SetWalWriterSleeping` is the same crate's xlog-driver
  deferred-panic (needs XLogCtl shmem) — mirror-and-panic into the correct owner.
- **No invented opacity / registries / stubs.** No `todo!`/`unimplemented!`.

## 5. Seams

- **Inward (owned, installed by `init_seams`, wired into seams-init)**:
  `backend_postmaster_walwriter_seams::wal_writer_main` (entry `-> !` adapter;
  `backend-postmaster-launch-backend` already calls it).
- **New seams added to COMPLETE owners (installed there)**:
  `backend-storage-lmgr-proc-seams::set_walwriter_proc_to_self` (proc owner
  `inward_seams.rs`); `backend-utils-init-miscinit-seams::set_my_backend_type_wal_writer`
  (miscinit owner). `types-pgstat::WAIT_EVENT_WAL_WRITER_MAIN` added.
- **Consumed outward seams** (all from already-present owner -seams crates, as in
  checkpointer): auxprocess, latch, lwlock, condition-variable, aio, bufmgr, fd,
  smgr, resowner, dynahash, activity-waitevent, activity-pgstat-wal, miscinit
  (hold/resume_interrupts), init-small (my_proc_pid), dsm-core (proc_exit),
  port-pgsleep.

## 6. Tests

12 unit tests covering the pure cadence + GUC logic
(`recompute_hibernation`/`apply_flush_result`/`cur_timeout`, defaults,
roundtrips, constants). All pass.
