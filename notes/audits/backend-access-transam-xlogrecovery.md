# Audit: backend-access-transam-xlogrecovery

C source: `src/backend/access/transam/xlogrecovery.c` (PostgreSQL 18.3, ~5105 LOC).

## Verdict: IN-PROGRESS (scaffold). CATALOG status corrected off `merged`.

This crate was catalogued `merged` but hid 18 `panic!("decomp: … not yet filled")`
stubs across the core WAL-recovery replay path. That is a panic-audit DEFECT: the
crate is a scaffold, not a merged port. Status corrected to `in-progress`; the
honest "not yet filled" panics were converted to blocker-naming
`panic!("blocked: … pending <family> fill")` panics (no "not yet filled" remains),
and CATALOG notes now describe the real shape.

## Genuinely landed (real logic, verified present)

- `shmem.rs` — F0 recovery-shmem keystone: `XLogRecoveryShared` `#[repr(C)]`
  region, `XLogRecoveryShmemSize/Init`, the `info_lck`-protected accessors
  (`GetXLogReplayRecPtr`, `GetLatestXTime`, `Get/SetRecoveryPauseState`,
  `WakeupRecovery`, `PromoteIsTriggered`, `HotStandbyActive`, receipt-time, etc.).
  Installed via `init_seams()`.
- `pageread.rs` — the `XLogPageRead` page-read driver.
- `walrecovery.rs` — the reader/prefetcher holder (`InitWalRecovery` reader leg):
  holds the recovery `XLogReaderState` / `XLogPrefetcher` behind process-lifetime
  `thread_local` raw pointers (mirroring the C file-statics), resolves `RecordRef`
  directly against the held reader, and installs the 5 xlogreader/xlogprefetcher
  record seams the `ReadRecord` retry loop drives.
- `core.rs` — backend-local `XLogRecoveryState` carrier (C file-static globals),
  recovery enums, `EndOfWalRecoveryInfo`, info-opcode/signal constants.
- `promote.rs`, `readrecord.rs`, `guc.rs` — landed.

## NOT filled — 18 blocker-naming panics, 4 families

### replay.rs (7)
- `perform_wal_recovery` — the `ReadRecord`->apply redo loop.
- `apply_wal_record` — per-AM redo dispatch via `GetRmgr(rmid).rm_redo` over the
  held reader + replay-end-state bookkeeping.
- `xlogrecovery_redo` — RM_XLOG_ID handler (checkpoint / end-of-recovery /
  overwrite-contrecord / restore-point; updates ControlFile + minRecoveryPoint).
- `check_recovery_consistency` — backup-end / min-recovery-point test +
  PMSIGNAL_RECOVERY_CONSISTENT / BEGIN_HOT_STANDBY signaling (unported postmaster
  pmsignal owner).
- `check_time_line_switch` — timeline-history validation over `expected_tles`.
- `get_record_timestamp` — needs decoding the record data area into
  `xl_restore_point.rp_time` / `xl_xact_commit.xact_time` / `xl_xact_abort.xact_time`
  (xact WAL record struct decode over the held reader). This is the hinge for the
  stop family.
- `verify_backup_page_consistency` — `wal_consistency_checking` masked-page
  comparison (RestoreBlockImage + rm_mask + ReadBufferWithoutRelcache).

### stop.rs (5)
- `recovery_stops_before` / `recovery_stops_after` / `recovery_apply_delay` —
  recovery-target comparison logic is self-contained, but all three call
  `replay::get_record_timestamp` (and after also decodes the restore-point name),
  so they are gated on the replay family.
- `get_recovery_stop_reason` — reason string from the `recovery_stop_*` fields.
- `recovery_pauses_here` — pause loop needs `ProcessStartupProcInterrupts` +
  recovery-pause CV timed sleep + `CheckForStandbyTrigger`.
- (`get_recovery_pause_state` / `set_recovery_pause` / `confirm_recovery_paused`
  in stop.rs are already real, delegating to `shmem`.)

### desc.rs (4)
- `xlog_outdesc` / `xlog_outrec` / `xlog_block_info` / `rm_redo_error_callback` —
  rmgr name/identify/desc dispatch + per-block-ref rendering. The substrate exists
  (`backend-access-transam-rmgr::GetRmgr` -> `RmgrData.rm_desc/rm_identify/rm_name`,
  xlogreader block-tag accessors), BUT `RmDesc = fn(&mut PgString, &XLogReaderState)
  -> PgResult<()>` requires an allocation context: the current stub signatures take
  bare `&mut String` with no `Mcx`. Filling faithfully requires re-signing these
  debug fns to thread `Mcx`/`PgString` and propagating `PgResult`. Tractable but a
  signature cascade onto callers (replay error-context), so done with the replay
  family.

### startupxlog.rs (2)
- `startup_xlog` / `startup_xlog_after_init` — recovery entry (control-file read,
  InitWalRecovery, redo loop via `replay::perform_wal_recovery`) + process
  integration (StartupProcShutdownHandler, checkpointer signaling) into the
  unported postmaster/startup owners.

## Why decomposed rather than fully filled

Full faithful fill is a multi-subsystem effort beyond one auditable pass:
- The repo substrate diverges from src-idiomatic's port (held-reader model here vs
  src-idiomatic's `RecordRef`-seam + `crate::state` model; `seams_ub_xlogcore` does
  not exist here), so src-idiomatic `replay.rs`/`stop.rs`/`desc.rs`/`startupxlog.rs`
  cannot be copied — each function must be re-grounded against the held reader.
- `desc` needs an `Mcx`/`PgString` re-signing cascade for `rm_desc`.
- `stop` is gated on `replay::get_record_timestamp` (xact record struct decode).
- `startupxlog`/`replay` need per-AM redo dispatch + postmaster/checkpointer legs.

Marking any family `merged` without per-function audit confidence is the
silent-corruption failure the repo rules forbid; the honest landing is the corrected
scaffold status + blocker-naming panics. Suggested next-step order:
replay (`get_record_timestamp` first) -> stop -> desc (with Mcx re-sign) -> startupxlog.
