# Audit: backend-postmaster-walsummarizer

- **C source**: `src/backend/postmaster/walsummarizer.c` (PostgreSQL 18.3)
- **c2rust**: `c2rust-runs/backend-postmaster-walsummarizer/src/walsummarizer.rs`
- **Port**: `crates/backend-postmaster-walsummarizer/src/lib.rs`
- **Audit verdict**: **PASS**

Independent re-derivation from the C and headers; the port's comments and
self-review were not trusted.

## 1. Function inventory (every C definition)

18 function definitions in walsummarizer.c (6 static fwd-declared + 12 extern),
all present in the port.

| # | C function (C line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `WalSummarizerShmemSize` (173) | `WalSummarizerShmemSize` | MATCH | `size_of::<WalSummarizerData>()`. |
| 2 | `WalSummarizerShmemInit` (182) | `WalSummarizerShmemInit` | MATCH | `ShmemInitStruct` via seam (returns `(*mut u8, bool)`); on `!found` fills dummy values + `ConditionVariableInit` = in-place `ConditionVariable::new()`. Field-by-field identical. |
| 3 | `WalSummarizerMain` (213) | `WalSummarizerMain` + `wal_summarizer_main_iteration` + `error_recovery_cleanup` | MATCH | `MyBackendType=B_WAL_SUMMARIZER` + `AuxiliaryProcessMainCommon` (seams); pqsignal/sigprocmask block delegated to aux bootstrap; on_shmem_exit(WalSummarizerShutdown); lock+pgprocno store; GetOldestUnsummarizedLSN(&tli,&exact); proc_exit(0) on invalid; for(;;) loop; sigsetjmp realized as per-iteration `Err` catch → cleanup + WaitLatch(NULL, WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH, 10000, WAL_SUMMARIZER_ERROR). |
| 4 | `GetWalSummarizerState` (450) | `GetWalSummarizerState` | MATCH | LW_SHARED; `!initialized`→all-zero/-1; else 3-way (pgprocno==INVALID → pending=summarized, pid=-1; else pending=ctl.pending, pid via `proc_number_get_proc_pid`, normalize `<=0`→-1). |
| 5 | `GetOldestUnsummarizedLSN` (508) | `GetOldestUnsummarizedLSN` | MATCH | not-summarizing early return; non-summarizer shmem read fast-path under LW_SHARED; latest_tli history walk newest→oldest using `XLogGetOldestSegno`; existing-summaries end_lsn bump + should_make_exact; tli==0 → ereport(ERROR, INTERNAL_ERROR, "no WAL found on timeline %u"); store condition `am_wal_summarizer \|\| !initialized`; out-params. |
| 6 | `WakeupWalSummarizer` (640) | `WakeupWalSummarizer` | MATCH | NULL-ctl guard; LW_SHARED read of pgprocno; SetLatch by procno (seam) if != INVALID. |
| 7 | `WaitForWalSummarization` (663) | `WaitForWalSummarization` | MATCH | CHECK_FOR_INTERRUPTS (postgres seam); disabled→return; LW_SHARED read summarized/pending; `>= lsn` break; cycle timing; deadcycles; `>=6` → ereport(ERROR, OBJECT_NOT_IN_PREREQUISITE_STATE) with errdetail; else WARNING errmsg_plural + errdetail; drift align; ConditionVariableTimedSleep; final CancelSleep. |
| 8 | `WalSummarizerShutdown` (791) | `WalSummarizerShutdown` (+ `wal_summarizer_shutdown_callback`) | MATCH | LW_EXCLUSIVE; pgprocno=INVALID. on_shmem_exit adapter marshals (code, Datum). |
| 9 | `GetLatestLSN` (803) | `GetLatestLSN` | MATCH | `!RecoveryInProgress`→GetFlushRecPtr; insert_tli!=0→(GetXLogReplayRecPtr.lsn, insert_tli); else max(WalRcvFlush, XLogReplay). |
| 10 | `ProcessWalSummarizerInterrupts` (861) | `ProcessWalSummarizerInterrupts` | MATCH | barrier-pending→process; ConfigReloadPending→clear+ProcessConfigFile(PGC_SIGHUP); Shutdown\|\|!enabled→DEBUG1+proc_exit(0); LogMemoryContextPending→process. |
| 11 | `SummarizeWAL` (909) | `SummarizeWAL`+`summarize_wal_in`+`summarize_wal_body`+`summarize_wal_loop` | MATCH | private_data init; XLogReaderAllocate (seam, OOM→Err); exact→XLogBeginRead else XLogFindNextRecord w/ end_of_wal branch; loop (below); unconditional reader free (RAII via body?+free); file-write block when `end>start && !ff`; skip-debug when `end>start && ff`. |
| 11a | — loop body (1014) | `summarize_wal_loop` | MATCH | read record (Record/EndOfWal/Error); switch_lsn ReadRecPtr early break; rmid==XLOG→SummarizeXlogRecord(stop/ff); else !ff→DBASE/SMGR/XACT; !ff block-tag loop w/ FSM skip; summary_end=EndRecPtr; LW_EXCLUSIVE pending_lsn update; switch_lsn EndRecPtr break. |
| 12 | `SummarizeDbaseRecord` (1249) | `SummarizeDbaseRecord` | MATCH | info&~XLR_INFO_MASK; CREATE_FILE_COPY/CREATE_WAL_LOG → rlocator(spc,db,0) SetLimitBlock(MAIN,0); DROP → per-tablespace loop from offset 8. Offsets verified vs `dbcommands_xlog.h`. |
| 13 | `SummarizeSmgrRecord` (1318) | `SummarizeSmgrRecord` | MATCH | CREATE → rlocator@0 + forkNum@12, FSM skip, SetLimitBlock(forknum,0); TRUNCATE → blkno@0 rlocator@4 flags@16, HEAP→MAIN, VM→VISIBILITYMAP. Offsets verified vs `storage_xlog.h`. |
| 14 | `SummarizeXactRecord` (1367) | `SummarizeXactRecord` | MATCH | info&~XLR_INFO_MASK then &XLOG_XACT_OPMASK; COMMIT/COMMIT_PREPARED→ParseCommitRecord(raw_info, data)→per-xloc forknum loop (FSM skip, SetLimitBlock 0); ABORT/ABORT_PREPARED→ParseAbortRecord same. forknum loop walks enum values 0..=MAX_FORKNUM via `next_forknum`. |
| 15 | `SummarizeXlogRecord` (1427) | `SummarizeXlogRecord` | MATCH | CHECKPOINT_REDO→wal_level@0; CHECKPOINT_SHUTDOWN→@20; PARAMETER_CHANGE→@20; END_OF_RECOVERY→@16; else None; return `wal_level==WAL_LEVEL_MINIMAL`. CheckPoint/xl_parameter_change/xl_end_of_recovery offsets verified vs `pg_control.h`/`xlog_internal.h`. |
| 16 | `summarizer_read_local_xlog_page` (1500) | `summarizer_read_local_xlog_page` | MATCH | ProcessInterrupts; loop: `+XLOG_BLCKSZ<=read_upto`→count=BLCKSZ; `+req_len>read_upto`→ if historic set end_of_wal+return -1 else wait_for_wal + recheck (same tli→update read_upto / else historic+tliSwitchPoint+read_upto=switchpoint+DEBUG1); else count=read_upto-target; WALRead (seam, raise→Err); pages_read++. |
| 17 | `summarizer_wait_for_wal` (1614) | `summarizer_wait_for_wal` | MATCH | pages==0→sleep_quanta=min(*2,MAX); pages>1→ `>sleep-1`?1:sleep-pages; pgstat_report_wal; WaitLatch(MyLatch, LATCH_SET\|TIMEOUT\|EXIT_ON_PM_DEATH, sleep*MS_PER_QUANTUM, WAL_SUMMARIZER_WAL); ResetLatch(MyLatch); pages=0. |
| 18 | `MaybeRemoveOldWalSummaries` (1660) | `MaybeRemoveOldWalSummaries` | MATCH | GetRedoRecPtr; keep_time==0→return; redo unchanged→return; cutoff=time(NULL)-keep*SECS_PER_MINUTE; GetWalSummaries(0,..); while-list: ProcessInterrupts, pick tli, oldest_segno→oldest_lsn, residual-rebuild loop (other-tli kept; matching tli removed-if-old via RemoveWalSummaryIfOlderThan). foreach_delete_current modeled by residual list rebuild. |

## 2. Constants & layouts verified against C headers

| Constant | Port | C header | Result |
|---|---|---|---|
| MAX_SLEEP_QUANTA / MS_PER_SLEEP_QUANTUM | 150 / 200 | walsummarizer.c | OK |
| wal_summary_keep_time default | 10*24*60 | `10*HOURS_PER_DAY*MINS_PER_HOUR` | OK |
| WALSummarizerLock offset | 49 | `lwlocklist.h` PG_LWLOCK(49,...) | OK |
| XLOGDIR / XLOG_BLCKSZ | "pg_wal" / 8192 | xlog_internal.h / pg_config | OK |
| WAL_LEVEL_MINIMAL | 0 | access/xlog.h | OK |
| RM_XLOG/XACT/SMGR/DBASE_ID | 0/1/2/4 | rmgrlist.h | OK |
| XLOG_CHECKPOINT_SHUTDOWN/PARAMETER_CHANGE/END_OF_RECOVERY/CHECKPOINT_REDO | 0x00/0x60/0x90/0xE0 | pg_control.h | OK |
| XLOG_SMGR_CREATE/TRUNCATE | 0x10/0x20 | storage_xlog.h | OK |
| SMGR_TRUNCATE_HEAP/VM | 0x0001/0x0002 | storage_xlog.h | OK |
| XLOG_DBASE_CREATE_FILE_COPY/CREATE_WAL_LOG/DROP | 0x00/0x10/0x20 | dbcommands_xlog.h | OK |
| XLOG_XACT_COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED/OPMASK | 0x00/0x20/0x30/0x40/0x70 | access/xact.h | OK |
| XLR_INFO_MASK | 0x0F | xlogrecord.h | OK |
| record offsets (dbase/smgr/checkpoint/param/end_of_recovery) | as coded | per struct layouts | OK |
| WAIT_EVENT_WAL_SUMMARIZER_WAL/ERROR | PG_WAIT_ACTIVITY\|16 / PG_WAIT_TIMEOUT\|9 | wait_event_names.txt indices | OK |

`XLogSegNoOffsetToRecPtr` = `segno*wal_segsz+offset` (wrapping) matches the macro.

## 3. Seams and wiring

Owned inward seam crate: `backend-postmaster-walsummarizer-seams` (one decl,
`wal_summarizer_main(&StartupData) -> !`). Installed by `init_seams()` (the only
statement), and `seams-init::init_all()` calls `backend_postmaster_walsummarizer::init_seams()`.
`launch-backend` already routes `B_WAL_SUMMARIZER` to this seam. PASS.

Outward calls — all thin marshal+delegate into owner -seams crates for unported
owners (xlog, xlogrecovery, xlogreader, timeline, walreceiver, walsummary,
blkreftable, xactdesc, walstats, shmem, lwlock, condvar, latch, ipc, procarray,
procsignal, auxprocess, miscinit, guc-file, mcxt, resowner, fd, dynahash, aio,
waitevent, timestamp) — each panics until its owner lands (correct). Direct deps
for ported leaves: `backend-utils-error` (ereport/elog/EmitErrorReport/
FlushErrorState) and `backend-postmaster-interrupt` (ConfigReloadPending/
ShutdownRequestPending). No outward seam contains branching/computation.

**Reclaimed leaked logic**: the src-idiomatic `seam-init/wire_r2_remainder.rs`
`install_walsummarizer` carried the xlogreader registry, the page-read
dispatcher, the `WsMeta` private-data, and the WALRead error-message shaping.
That belonged to the owner; here the summarizer-owned half (the
`SummarizerReadLocalXLogPrivate` private-data lifecycle and `summarizer_read_
local_xlog_page` itself) lives in this crate, and the xlogreader-side operations
are declared in `backend-access-transam-xlogreader-seams` (the owner's crate) for
the owner to install. The `error_recovery_cleanup` orchestration (formerly a
single `error_recovery_cleanup` seam) is reclaimed as a real function here that
calls each cleanup through its owner seam. No logic left in any installer.

## 4. Design conformance

- **Opacity (types.md 6/7)**: `XLogReaderHandle` is a newtype (not a bare `u64`
  alias) for the genuinely-opaque `XLogReaderState *` of an unported owner —
  inherited opacity, newtype'd. PASS. (`BlockRefTableHandle` retired: the
  block-reference table is now the genuine owned `BlockRefTable` value, threaded
  by `&mut` through `SummarizeWAL`/`Summarize{Dbase,Smgr,Xact}Record`.)
- **Allocating fns take Mcx + return PgResult**: `read_timeline_history`,
  `get_wal_summaries`, `parse_commit/abort_record`, `create_empty_block_ref_table`,
  `write_block_ref_table` all take `Mcx` and return `PgResult`; the work context
  is an owned root (`MemoryContext::new`) threaded as `Mcx` (no ambient context).
  The few `format!`/`to_string` are error-message / DEBUG-log construction at
  return-Err sites and short snprintf-into-stack path buffers (non-allocating C
  counterparts). PASS.
- **Per-backend globals → thread_local**: all five file statics + two GUCs +
  the `SummarizerReadLocalXLogPrivate` map + the `WalSummarizerCtl` shmem
  pointer are `thread_local!` Cells/RefCell; no shared statics. PASS.
- **No ambient-global seams**: SetLatch/pgproc-pid take an explicit
  `ProcNumber`; no zero-arg foreign-global getters. PASS.
- **Locks**: `WALSummarizerLock` acquire/release pairs bracket field
  reads/writes with no intervening `?`; the abort path uses
  `lwlock_release_all` (C `LWLockReleaseAll`). No lock held across `?`. PASS.
- **FATAL/PANIC → Err**: error paths return `PgResult`; the only `panic!` is an
  internal-invariant unknown-handle (just-inserted) and one `unreachable!` for
  the main-never-returns-Ok contract. No registry side-table with release
  authority. No unledgered divergence markers. PASS.

## Sanctioned divergence

- `WriteWalSummary` streaming is replaced by `write_block_ref_table` returning
  serialized bytes + `write_wal_summary_file(temp,final,bytes)` (open temp,
  write, close, durable_rename) — same on-disk result; matches the idiomatic
  blkreftable port's byte-returning shape (recorded in CATALOG note).
- The summarizer work context is a fresh owned root per top-level work unit
  (dropped == the C `MemoryContextReset(context)` each loop), since there is no
  ambient CurrentMemoryContext (`docs/mctx-design.md`).

## Verdict: PASS

All 18 functions MATCH; constants/offsets verified against headers; seams thin
and installed; design rules satisfied. `cargo check -p
backend-postmaster-walsummarizer -p backend-postmaster-walsummarizer-seams -p
seams-init` and `cargo check --workspace` pass; 9 unit tests pass.
