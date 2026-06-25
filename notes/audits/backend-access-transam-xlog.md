# Audit: backend-access-transam-xlog

Unit: `backend-access-transam-xlog`
C sources: `src/backend/access/transam/xlog.c` (PostgreSQL 18.3); headers
`catalog/pg_control.h`, `access/xlog.h`, `access/xlog_internal.h`,
`access/xlogdefs.h`, `storage/proc.h`, `replication/slot.h`, `storage/fd.h`.
Crates: `backend-access-transam-xlog`, `backend-access-transam-xlog-seams`,
`types-control`.

Final verdict: **PASS** (after 1 fix round).

## Scope and method

The inventory was re-derived from `xlog.c` (134 function definitions; the three
`c2rust_pg_*_barrier` shims are atomics.h inlines, not xlog.c logic) cross-checked
against `c2rust-runs/backend-access-transam-xlog/src/xlog.rs`. Constants were
verified against the headers (not from memory): PG_CONTROL_VERSION=1800,
MOCK_AUTH_NONCE_LEN=32, FLOATFORMAT_VALUE=1234567.0, PG_CONTROL_FILE_SIZE=8192,
FirstNormalUnloggedLSN=1000, DBState 0..6, the XLOG opcodes 0x00..0xE0, the
CHECKPOINT_* flags (0x1/0x2/0x8), DELAY_CHKPT_{START,COMPLETE} (1<<0,1<<1),
RS_INVAL_{WAL_REMOVED,IDLE_TIMEOUT} (1<<0,1<<3), XLOG_FNAME_LEN=24,
SizeOfXLogShortPHD=24 / SizeOfXLogLongPHD=40 (MAXALIGN of the page-header
structs), and the CheckPoint field order / 88-byte LP64 image.

This unit deliberately grounds the pure arithmetic/codec core + the
checkpoint/restartpoint state machine + the redo dispatch, and defers the WAL
shmem/fd/spinlock *driver* (XLogWrite, XLogFlush, StartupXLOG, BootStrapXLOG,
the file/control-file I/O, backup) behind loud panics. Per the skill, a panic is
acceptable only when the function's body genuinely depends on an unported callee;
a function whose **own** logic was simply dropped is MISSING and fails the audit.

## Per-function verdicts (grounded subset + the deferred driver)

| C function (xlog.c) | Port location | Verdict | Notes |
|---|---|---|---|
| XLogBytePosToRecPtr (1885) | lib.rs:263 | MATCH | byte-pos arithmetic re-derived line-for-line |
| XLogBytePosToEndRecPtr (1925) | lib.rs:288 | MATCH | page-boundary zero-offset arm matches |
| XLogRecPtrToBytePos (1968) | lib.rs:321 | MATCH | inverse; Assert(offset>=PHD) is debug-only in C |
| CalculateCheckpointSegments (2195) | lib.rs:469 | MATCH | round-down, floor at 1 |
| assign_max_wal_size (2224) | lib.rs (added) | MATCH | recompute wrapper (was MISSING, fixed) |
| assign_checkpoint_completion_target (2231) | lib.rs (added) | MATCH | recompute wrapper (was MISSING, fixed) |
| check_wal_segment_size (2238) | lib.rs:184 | MATCH | GUC check hook |
| XLOGfileslop (2254) | retention.rs:43 | MATCH | min/max clamp, +10%, ceil |
| XLogCheckpointNeeded (2304) | lib.rs (added) | MATCH | RedoRecPtr/CheckPointSegments arith (was MISSING, fixed) |
| XLOGChooseNumBuffers (4681) | lib.rs (added) | MATCH | NBuffers/32 clamp [8, segsz/BLCKSZ] (was MISSING, fixed) |
| check_wal_buffers (4697) | lib.rs (added) | MATCH | -1 autotune + floor-4 (was MISSING, fixed) |
| get_sync_bit (8678) | lib.rs (added) | MATCH | sync-flag mapping core (was MISSING, fixed) |
| KeepLogSeg (8020) | retention.rs:77 | MATCH | slot/summarizer/wal_keep floors, underflow guard |
| GetWALAvailability (7936) | retention.rs:132 | MATCH | classification; externals lifted to params |
| XLogGetOldestSegno (3794) | retention.rs:173 | MATCH | dir-scan filter as iterator |
| RemoveOldXlogFiles name filter (3885) | retention.rs:204 | MATCH | strcmp(d+8, lastoff+8)<=0 |
| UpdateLastRemovedPtr parse (3832) | retention.rs:214 | MATCH | strips .partial then XLogFromFileName |
| UpdateCheckPointDistanceEstimate (6848) | retention.rs:224 | MATCH | bump-fast/decay-slow MA |
| XLogFileName/ById/Path, IsXLogFileName, XLogFromFileName, TLHistory*, StatusFilePath, BackupHistory* (xlog_internal.h) | lib.rs:352-462 | MATCH | name/path codec, hex parse |
| Segment/byte macros (XLogSegmentsPerXLogId, XLByteToSeg/PrevSeg, XLByteInSeg/PrevSeg, XLogSegmentOffset, XLogSegNoOffsetToRecPtr, XLogMBVarToSegs, ConvertToXSegs, XRecOffIsValid, UsableBytesInPage/Segment) | lib.rs:196-260 | MATCH | xlog_internal.h macros 1:1 |
| WalConfig predicates (XLogArchivingActive/Always, XLogIsNeeded, XLogHintBitIsNeeded, XLogStandbyInfoActive, XLogLogicalInfoActive) | lib.rs:139-167 | MATCH | xlog.h inline predicates |
| XLogRecPtrIsInvalid, IsValidWalSegSize | lib.rs:174-181 | MATCH | xlogdefs/xlog_internal predicates |
| CreateCheckPoint (6951) | checkpoint.rs:149 | MATCH | full control flow vs C; shmem/subsystem reads are deferred ext::* |
| CheckPointGuts (7574) | checkpoint.rs:335 | MATCH | callback fan-out + sync timing |
| CreateRestartPoint (7655) | checkpoint.rs:353 | MATCH | full control flow vs C |
| LocalSetXLogInsertAllowed (6484) | checkpoint.rs:455 | MATCH | CheckpointState-scoped |
| CheckPoint <-> 88-byte image | checkpoint.rs:86 | MATCH | LP64 field/pad layout verified |
| xlog_redo (8304) | redo.rs:45 | MATCH | opcode dispatch; unknown-opcode no-op (fixed) |
| XLogWrite, AdvanceXLInsertBuffer, XLogFlush, XLogBackgroundFlush, XLogNeedsFlush, StartupXLOG, BootStrapXLOG, XLOGShmemSize/Init, Read/WriteControlFile, file I/O (XLogFileInit/Open/Close/Copy/InitInternal, InstallXLogFileSegment, PreallocXlogFiles, RemoveOldXlogFiles, RemoveXlogFile, RemoveTempXlogFiles, RemoveNonParentXlogFiles, ValidateXLOGDirectoryStructure, CleanupBackupHistory), the WAL-insert engine (XLogInsertRecord, Reserve*, CopyXLogRecordToWAL, WaitXLogInsertionsToFinish, GetXLogBuffer, WALReadFromBuffers, WALInsertLock*), shmem getters (GetRedoRecPtr, GetInsertRecPtr, GetFlushRecPtr, GetXLogInsert/WriteRecPtr, GetLastImportantRecPtr, GetWALInsertionTimeLine[IfSet], GetFullPageWriteInfo, RecoveryInProgress, GetRecoveryState, XLogInsertAllowed, GetSystemIdentifier, GetMockAuthenticationNonce, DataChecksumsEnabled, GetDefaultCharSignedness, GetFakeLSNForUnloggedRel, XLogGetLastRemovedSegno, GetLastSegSwitchData, GetOldestRestartPoint, GetActiveWalLevelOnStandby, XLogGet/SetReplicationSlotMinimumLSN, XLogSetAsyncXactLSN), record writers (XLogPutNextOid, RequestXLogSwitch, XLogRestorePoint, UpdateFullPageWrites, CreateEndOfRecoveryRecord, CreateOverwriteContrecordRecord, XLogReportParameters), startup/recovery legs (XLogInitNewTimeline, CleanupAfterArchiveRecovery, CheckRequiredParameterValues, SwitchIntoArchiveRecovery, ReachedEndOfBackup, PerformRecoveryXLogAction, RecoveryRestartPoint, UpdateMinRecoveryPoint, LocalProcessControlFile, InitializeWalConsistencyChecking, InitControlFile, UpdateControlFile, str_time), backup (do_pg_backup_start/stop, do_pg_abort_backup, get_backup_status, register_persistent_abort_backup_handler), fsync/GUC-IO (issue_xlog_fsync, assign_wal_sync_method), ShutdownXLOG, CheckXLogRemoved, XLogShutdownWalRcv, SetWalWriterSleeping, SetInstallXLogFileSegmentActive et al, LogCheckpointStart/End, update_checkpoint_display, check/assign_wal_consistency_checking, show_archive_command, show_in_hot_standby, GetWALAvailability process entry, CreateCheckPoint/CreateRestartPoint process entries | lib.rs:512-649, checkpoint.rs ext::*, redo.rs ext::* | SEAMED/DEFERRED (acceptable) | Each panics loudly with the `xlog-driver` / `xlog-checkpoint-deps` / `xlog-redo-deps` debt tag. All genuinely require the unported shmem (`ShmemInitStruct`)/fd.c/spinlock substrate, the XLogReaderState record handle, or owner subsystems (bufmgr, sync.c, slot.c, multixact, varsup, commit_ts, walsummarizer, walreceiver, xlogrecovery, GUC, timezone). No own logic dropped. |

## Fix round 1 (findings repaired)

1. **`XLogCheckpointNeeded` (xlog.c:2304) — was MISSING.** Pure arithmetic over
   `RedoRecPtr`/`CheckPointSegments`; no unported callee. Added to lib.rs with
   the externals lifted to parameters; test re-derives the `old_segno + (CPS-1)`
   threshold.
2. **`assign_max_wal_size` / `assign_checkpoint_completion_target`
   (xlog.c:2224/2231) — were MISSING.** Trivial GUC assign hooks that recompute
   `CheckPointSegments`. Added as recompute wrappers over
   `CalculateCheckpointSegments`.
3. **`XLOGChooseNumBuffers` (xlog.c:4681) — was MISSING.** Pure clamp arithmetic;
   added (NBuffers param), test covers the [8, segsz/BLCKSZ] clamp.
4. **`check_wal_buffers` (xlog.c:4697) — was MISSING.** Pure GUC check logic;
   added, test covers the -1 autotune and floor-4 arms.
5. **`get_sync_bit` (xlog.c:8678) — was MISSING.** The sync-flag mapping switch
   (incl. the `!enableFsync` short-circuit) is portable logic; the platform
   `O_SYNC`/`O_DSYNC`/`o_direct_flag` values are lifted to parameters. Added; the
   C `default: elog(ERROR)` is unreachable over the exhaustive `WalSyncMethod`
   enum, faithfully represented by the total match.
6. **`xlog_redo` unknown-opcode arm (redo.rs) — was DIVERGES.** The original
   `else` returned `Err("unrecognized XLOG record info")`. C's `xlog_redo` has
   **no** `default`/error arm: an unrecognized `info` falls through to the end and
   the void function returns (a no-op). Confirmed in both `xlog.c:8666-8671` and
   the c2rust rendering. Fixed the else arm to `Ok(())`.

## Seam / wiring audit

- `backend-access-transam-xlog-seams` declares no inward seams: the one inward
  entry (`xlog_redo`) needs the unported `XLogReaderState` handle, so the
  declaration lands with that type. `init_seams()` is a justified no-op and is
  called from `seams-init::init_all()` (seams-init/src/lib.rs:14). Cargo dep
  present.
- The deferred driver functions panic in-crate; none delegate logic across a
  seam to "somewhere else", so no SEAMED-should-be-MISSING violations. The
  checkpoint/redo deferred `ext::*` modules contain only thin
  marshal-and-panic stubs (no branching/computation), consistent with the seam
  rules.

## Design conformance

- `CheckpointState` owns the per-backend file-scope globals
  (`RedoRecPtr`/`ControlFile`/`LocalXLogInsertAllowed`) as an owned struct passed
  `&mut`, rather than a shared `static mut` — conforms to the per-backend-global
  rule.
- Allocating codec (`checkpoint_to_bytes`, name formatters) return owned
  `Vec`/`String`; fallible parses return `PgResult`. No invented opacity, no
  ambient-global seams, no locks held across `?`, no registry side tables.
- Constants live in `types-wal`/`types-control` and were verified against
  headers.

## Verification

`cargo test -p backend-access-transam-xlog`: 32 passed. `cargo check --workspace`:
clean.
