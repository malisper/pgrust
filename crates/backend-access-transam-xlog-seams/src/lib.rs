//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_core::uint8;
use types_error::PgResult;
use types_wal::{ArchiveMode, WalLevel, WalSyncMethod};

seam_core::seam!(
    /// `xlog_redo(record)` (xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogSetAsyncXactLSN(asyncXactLSN)` — mark the LSN as to-be-synced and
    /// nudge the WAL writer.
    pub fn xlog_set_async_xact_lsn(async_xact_lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogArchiveLibrary` (xlog.c GUC string): the configured archive
    /// library name, "" when unset. Returns an owned copy of the current
    /// value (the C bare read is a `char *` global; pgarch copies it via
    /// `pstrdup` before reload). Infallible.
    pub fn xlog_archive_library() -> alloc::string::String
);

seam_core::seam!(
    /// `XLogArchiveCommand` (xlog.c GUC string): the configured archive shell
    /// command, "" when unset. Returns an owned copy of the current value.
    /// Infallible.
    pub fn xlog_archive_command() -> alloc::string::String
);

seam_core::seam!(
    /// `int wal_level` (xlog.c GUC) — the effective `wal_level` value.
    pub fn wal_level() -> WalLevel
);

seam_core::seam!(
    /// `enableFsync` (xlog.c GUC) — whether the server issues `fsync`/
    /// `fdatasync` for durability. fd.c's `pg_fsync` family early-outs when
    /// this is off.
    pub fn enable_fsync() -> bool
);

seam_core::seam!(
    /// `DataChecksumsEnabled()` (xlog.c) — whether data-page checksums are on
    /// for this cluster. Read from the control file's
    /// `data_checksum_version`; `bufpage.c`'s verify/set-checksum paths gate on
    /// it. Panics until xlog installs the control-file-backed implementation.
    pub fn data_checksums_enabled() -> bool
);

seam_core::seam!(
    /// `wal_sync_method` (xlog.c GUC) — the WAL sync method, consulted by
    /// fd.c's `pg_fsync` to choose the writethrough vs. plain fsync path.
    pub fn wal_sync_method() -> WalSyncMethod
);

seam_core::seam!(
    /// `wal_segment_size` (xlog.c global, bytes-per-WAL-segment). A plain
    /// global read — infallible.
    pub fn wal_segment_size() -> i32
);

seam_core::seam!(
    /// `InRecovery` (xlog.c global) — true in the startup process during
    /// crash/archive recovery.
    pub fn in_recovery() -> bool
);

seam_core::seam!(
    /// `XLogHintBitIsNeeded()` (xlog.h) — `(XLogIsNeeded() &&
    /// (data_checksums || wal_log_hints))`: whether hint-bit-style page
    /// changes must be WAL-logged. Pure read of `wal_level` / checksum / GUC
    /// state.
    pub fn xlog_hint_bit_is_needed() -> bool
);

seam_core::seam!(
    /// `XLogCheckBufferNeedsBackup(buffer)` (xloginsert.c) — would inserting a
    /// WAL record that touches `buffer` need to take a full-page image (the
    /// buffer's page LSN predates the current redo recptr)? Used by
    /// `heap_page_prune_and_freeze` to decide whether opportunistic freezing is
    /// "free" because an FPI is being emitted anyway.
    pub fn xlog_check_buffer_needs_backup(
        buffer: types_storage::Buffer,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `StartupXLOG()` (xlog.c) — perform crash/archive recovery and bring
    /// the system to a consistent, writable state. Many of its paths
    /// `ereport(ERROR)` (besides the FATAL/PANIC ones), so the error
    /// propagates to the caller.
    pub fn startup_xlog() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `bool RecoveryInProgress(void)` (xlog.c) — true if WAL recovery is
    /// still in progress (we are a standby / in crash recovery). Reads
    /// backend-local + shared state; cannot `ereport`.
    pub fn recovery_in_progress() -> bool
);

seam_core::seam!(
    /// `XLogLogicalInfoActive()` (`access/xlog.h`): `wal_level >= logical`.
    /// The `wal_level` global is owned by xlog.c.
    pub fn xlog_logical_info_active() -> bool
);

seam_core::seam!(
    /// `XLogStandbyInfoActive()` (`access/xlog.h`): `wal_level >= replica`.
    pub fn xlog_standby_info_active() -> bool
);

seam_core::seam!(
    /// `XLogEnsureRecordSpace(max_block_id, ndatas)` (xloginsert.c, owned with
    /// the xlog insert path): ensure the WAL insertion buffers can register
    /// `ndatas` rdata chunks. Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_ensure_record_space(max_block_id: i32, ndatas: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `EndPrepare`'s WAL insert: `XLogBeginInsert` + per-chunk
    /// `XLogRegisterData` + `XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN)` +
    /// `XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE)`. `body` is the assembled
    /// prepare-record buffer (flat). Returns the prepare-record *end* LSN. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_insert_prepare(body: &[u8]) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `ProcLastRecPtr` (xlog.c global): the *start* LSN of the record this
    /// backend most recently inserted. Pure read of backend-local state.
    pub fn proc_last_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogFlush(lsn)` — ensure WAL is flushed up to `lsn`; I/O errors
    /// `ereport(ERROR)` (PANIC inside critical sections).
    pub fn xlog_flush(lsn: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogNeedsFlush(lsn)` — true iff `lsn` is past the currently-flushed WAL
    /// position and would therefore need a flush before being relied upon
    /// (the hint-bit LSN interlock in `SetHintBits`).
    pub fn xlog_needs_flush(lsn: XLogRecPtr) -> PgResult<bool>
);

seam_core::seam!(
    /// Read `XactLastRecEnd` (xlog.c per-backend global): end of the last WAL
    /// record this transaction inserted; 0 if none.
    pub fn xact_last_rec_end() -> XLogRecPtr
);

seam_core::seam!(
    /// Write `XactLastRecEnd` (the xact engine resets it to 0 at transaction
    /// end).
    pub fn set_xact_last_rec_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// Write `XactLastCommitEnd` (xlog.c per-backend global): end of the last
    /// commit record.
    pub fn set_xact_last_commit_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XlogReadTwoPhaseData(lsn, &buf, &len)` (xlog.c): re-read the prepare
    /// record body from WAL (used when COMMIT/ABORT PREPARED happens before the
    /// next checkpoint, and by `CheckPointTwoPhase`). Returns the rmgr data
    /// bytes. Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_read_twophase_data(lsn: XLogRecPtr) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// `BootStrapXLOG(data_checksum_version)` (xlog.c): create the initial WAL
    /// segment and control file at bootstrap. `ereport(PANIC)` on an I/O
    /// failure (modeled as `Err`).
    pub fn boot_strap_xlog(data_checksum_version: u32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckpointStats.ckpt_slru_written++` (xlog.c's `CheckpointStats`
    /// global, bumped directly by slru.c during checkpoint write-all).
    /// Narrow write-side capability on the owner's global, same shape as
    /// `set_my_backend_type` (see DESIGN_DEBT.md).
    pub fn count_ckpt_slru_written()
);

seam_core::seam!(
    /// `GetActiveWalLevelOnStandby()` (xlog.c): the effective `wal_level` on a
    /// standby, read from the control file's last checkpoint. Shared-state
    /// read; infallible.
    pub fn GetActiveWalLevelOnStandby() -> types_logical::WalLevel
);

seam_core::seam!(
    /// `log_recovery_conflict_waits` (the GUC, owned by xlog.c) — whether the
    /// startup process should log long recovery-conflict waits.
    pub fn log_recovery_conflict_waits() -> bool
);

seam_core::seam!(
    /// `GetFlushRecPtr(*insertTLI)` (xlog.c) — the LSN up to which WAL is
    /// flushed, with the corresponding insert timeline. Returns `(lsn, tli)`.
    pub fn get_flush_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    /// `GetWALInsertionTimeLineIfSet()` (xlog.c) — the insert TLI once it has
    /// been initialized in shared memory, else `0` (the C `InvalidTimeLineID`
    /// / 0 sentinel before recovery finishes).
    pub fn get_wal_insertion_timeline_if_set() -> TimeLineID
);

seam_core::seam!(
    /// `XLogRecPtr GetRedoRecPtr(void)` (xlog.c) — the current redo pointer.
    pub fn get_redo_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `bool XLogInsertAllowed(void)` (xlog.c) — whether new WAL records may be
    /// inserted (false during recovery, except for the startup process).
    pub fn xlog_insert_allowed() -> bool
);

seam_core::seam!(
    /// `GetFullPageWriteInfo(*RedoRecPtr_p, *doPageWrites_p)` (xlog.c, shmem
    /// read) — the values `XLogInsert` needs to decide on full-page writes
    /// before it holds an insertion lock. Returns `(RedoRecPtr, doPageWrites)`.
    pub fn get_full_page_write_info() -> (XLogRecPtr, bool)
);

seam_core::seam!(
    /// `wal_compression` GUC (xlog.c) — the configured WAL full-page-image
    /// compression method (`WAL_COMPRESSION_*` / `WalCompression` ordinal).
    pub fn wal_compression() -> i32
);

seam_core::seam!(
    /// `wal_consistency_checking[rmid]` (xlog.c) — whether WAL consistency
    /// checking is enabled for the given resource manager (the per-rmgr boolean
    /// array built by `assign_wal_consistency_checking`).
    pub fn wal_consistency_checking(rmid: types_core::RmgrId) -> bool
);

seam_core::seam!(
    /// `XLogRecPtr GetXLogInsertRecPtr(void)` (xlog.c) — current insert position.
    pub fn get_xlog_insert_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecPtr GetInsertRecPtr(void)` (xlog.c:6544) — the approximate WAL
    /// insert position (the latest fully-inserted record). Used by GiST VACUUM as
    /// the scan-start NSN interlock (`gistvacuumscan`).
    pub fn get_insert_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecPtr GetXLogReplayRecPtr(TimeLineID *)` (xlogrecovery.c) — last
    /// replayed position (called with NULL by slot.c, so no TLI out).
    pub fn get_xlog_replay_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn)` (xlog.c) —
    /// publish the oldest LSN required by replication slots.
    pub fn xlog_set_replication_slot_minimum_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogSegNo XLogGetLastRemovedSegno(void)` (xlog.c).
    pub fn xlog_get_last_removed_segno() -> XLogSegNo
);

seam_core::seam!(
    /// `XLogRecPtr GetXLogWriteRecPtr(void)` (xlog.c) — the LSN of the last byte
    /// written to (but not necessarily flushed to disk) the WAL. Used by
    /// `pg_get_replication_slots` to bound `safe_wal_size`.
    pub fn get_xlog_write_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `WALAvailability GetWALAvailability(XLogRecPtr targetLSN)` (xlog.c) —
    /// classify whether the WAL at `target_lsn` is still reserved / available.
    pub fn get_wal_availability(target_lsn: XLogRecPtr) -> types_wal::WALAvailability
);

seam_core::seam!(
    /// `int max_slot_wal_keep_size_mb` (xlog.c GUC) — the configured maximum WAL
    /// size a slot may retain, in MB (`-1` == unlimited). A plain global read.
    pub fn max_slot_wal_keep_size_mb() -> i32
);

seam_core::seam!(
    /// `int wal_keep_size_mb` (xlog.c GUC) — the minimum WAL kept for standbys,
    /// in MB. A plain global read.
    pub fn wal_keep_size_mb() -> i32
);

seam_core::seam!(
    /// `XLogRecPtr LogStandbySnapshot(void)` (standby.c) — log an
    /// `xl_running_xacts` record and return the end LSN. Can `ereport(ERROR)`.
    pub fn log_standby_snapshot() -> types_error::PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `bool StandbyMode` (xlogrecovery.c) — running in standby mode.
    pub fn standby_mode() -> bool
);

seam_core::seam!(
    /// `bool EnableHotStandby` (xlog.c) — the `hot_standby` GUC value.
    pub fn enable_hot_standby() -> bool
);

seam_core::seam!(
    /// `GetSystemIdentifier()` (xlog.c) — the cluster's 64-bit system id.
    pub fn get_system_identifier() -> u64
);

seam_core::seam!(
    /// `XLogArchiveMode` (xlog.c GUC) — the `archive_mode` setting.
    pub fn xlog_archive_mode() -> ArchiveMode
);

seam_core::seam!(
    /// `XLogFileInit(segno, tli)` (xlog.c) — create/open the given WAL segment
    /// file, returning the fd. `ereport(ERROR)` on failure.
    pub fn xlog_file_init(segno: XLogSegNo, tli: TimeLineID) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `issue_xlog_fsync(fd, segno, tli)` (xlog.c) — fsync the WAL segment;
    /// `ereport` on failure.
    pub fn issue_xlog_fsync(
        fd: i32,
        segno: XLogSegNo,
        tli: TimeLineID
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogGetOldestSegno(tli)` (xlog.c) — the oldest WAL segment number that
    /// still exists on disk for `tli`, or `0` if none.
    pub fn xlog_get_oldest_segno(tli: TimeLineID) -> XLogSegNo
);

// ---------------------------------------------------------------------------
// Local WAL read, consumed by xlogutils.c's read_local_xlog_page page-read
// callback. (The flush position uses the `get_flush_rec_ptr` seam above.)
// ---------------------------------------------------------------------------

/// The `WALReadError` fields needed by `WALReadRaiseError`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalReadErrorInfo {
    /// `wre_errno`.
    pub wre_errno: i32,
    /// `wre_off` — the offset within the segment at which the read failed.
    pub wre_off: i32,
    /// `wre_req` — the number of bytes requested.
    pub wre_req: i32,
    /// `wre_read` — the number of bytes actually read (<0 error, 0 short).
    pub wre_read: i32,
    /// `wre_seg.ws_segno`.
    pub wre_seg_segno: XLogSegNo,
    /// `wre_seg.ws_tli`.
    pub wre_seg_tli: TimeLineID,
}

/// Outcome of `WALRead`, mirroring the C `bool`-return plus `WALReadError`
/// out-parameter contract consumed by `WALReadRaiseError`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalReadOutcome {
    /// `WALRead` returned true; the read bytes (the C writes them through the
    /// borrowed `cur_page` pointer; the owned model returns them).
    Ok(Vec<u8>),
    /// `WALRead` returned false; the populated `WALReadError` to be raised.
    Error(WalReadErrorInfo),
}

seam_core::seam!(
    /// `WALRead(state, cur_page, targetPagePtr, count, tli, &errinfo)` (xlog.c)
    /// — read `count` bytes of WAL at `target_page_ptr` on timeline `tli`. On
    /// success returns `WalReadOutcome::Ok(bytes)` with the `count` valid
    /// bytes; on failure returns `WalReadOutcome::Error(errinfo)`.
    pub fn wal_read(
        target_page_ptr: XLogRecPtr,
        count: i32,
        tli: TimeLineID,
    ) -> WalReadOutcome
);

seam_core::seam!(
    /// `XLogGetReplicationSlotMinimumLSN()` (xlog.c): the oldest LSN required
    /// by any replication slot, or `InvalidXLogRecPtr` if none. Read under the
    /// `info_lck` spinlock by the owner.
    pub fn xlog_get_replication_slot_minimum_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLOGShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn xlog_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `XLOGShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn xlog_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogPutNextOid(nextOid)` (xlog.c): emit the `XLOG_NEXTOID` record that
    /// logs the next OID to be allocated, so a crash recovery sees the
    /// preallocated OID range. Called by `GetNewObjectId` while holding
    /// `OidGenLock`. The WAL insert can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_put_next_oid(next_oid: types_core::Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogInsertRecord(rdata, fpw_lsn, flags, num_fpi, topxid_included)`
    /// (xlog.c:772) — the low-level WAL-record insertion entry that xloginsert.c's
    /// `XLogInsert` calls once it has assembled the `XLogRecData` chain.
    ///
    /// The boundary between `xloginsert.c` and `xlog.c` is the assembled record:
    /// `rdata` carries the chain in order as a slice of byte fragments, where
    /// `rdata[0]` is the fixed `XLogRecord` header whose `xl_crc` field holds the
    /// caller's running CRC accumulated over the data (xloginsert.c side), and
    /// `xl_prev`/`xl_crc` are filled in by `XLogInsertRecord` once it holds the
    /// insertion lock. Returns the record's end LSN, or `InvalidXLogRecPtr`
    /// (`= 0`) when the caller must recompute and retry (RedoRecPtr / doPageWrites
    /// raced); `Err` carries the `ereport(ERROR)`/`PANIC` surface.
    pub fn xlog_insert_record(
        rdata: &[&[u8]],
        fpw_lsn: XLogRecPtr,
        flags: uint8,
        num_fpi: i32,
        topxid_included: bool,
    ) -> PgResult<XLogRecPtr>
);

// ---------------------------------------------------------------------------
// Online-backup workhorses (xlog.c) consumed by xlogfuncs.c's
// pg_backup_start()/pg_backup_stop(). These are owned by xlog.c but not yet
// exposed by the ported xlog crate; they are declared here so xlogfuncs.c can
// call them and panic loudly (mirror-PG-and-panic) until xlog.c installs them.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `do_pg_backup_start(backupidstr, fast, tablespaces, state, tblspcmapfile)`
    /// (xlog.c:8866) — the workhorse of `pg_backup_start()`: writes a
    /// checkpoint, fills `state` with the backup-start metadata, and renders the
    /// tablespace-map file contents. The `pg_backup_start()` caller passes
    /// `tablespaces = NULL`, so that out-list is dropped at this boundary.
    /// Returns the freshly-populated [`BackupState`] together with the
    /// tablespace-map bytes (the C `StringInfo tblspcmapfile`). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn do_pg_backup_start(
        backupidstr: &str,
        fast: bool,
    ) -> PgResult<(types_wal::BackupState, Vec<u8>)>
);

seam_core::seam!(
    /// `do_pg_backup_stop(state, waitforarchive)` (xlog.c:9194) — finish an
    /// online backup: write the stop WAL record, optionally wait for it to be
    /// archived, and fill the stop fields of `state`. Returns the updated
    /// [`BackupState`]. Can `ereport(ERROR)`, carried on `Err`.
    pub fn do_pg_backup_stop(
        state: types_wal::BackupState,
        waitforarchive: bool,
    ) -> PgResult<types_wal::BackupState>
);

seam_core::seam!(
    /// `register_persistent_abort_backup_handler(void)` (xlog.c:9384) — register
    /// the `before_shmem_exit` cleanup that aborts an in-progress backup if the
    /// session ends without `pg_backup_stop()`. Can `ereport(ERROR)` (out of
    /// before_shmem_exit slots), carried on `Err`.
    pub fn register_persistent_abort_backup_handler() -> PgResult<()>
);

seam_core::seam!(
    /// `get_backup_status(void)` (xlog.c:9175) — the session-level backup state
    /// (`sessionBackupState`). Pure read of backend-local state.
    pub fn get_backup_status() -> types_wal::SessionBackupState
);

// ===========================================================================
// Consumed by `access/transam/xlogrecovery.c` to flip the running server into
// archive recovery (`SwitchIntoArchiveRecovery`). Declared here (xlog.c owns
// `InArchiveRecovery` / `ControlFile->state` and the control-file update) but
// NOT installed: the recovery crate stays `needs-decomp` and the xlog driver
// leg that performs the control-file write is not yet ported, so a call panics
// loudly until the owner lands.
// ===========================================================================

seam_core::seam!(
    /// `SwitchIntoArchiveRecovery(EndRecPtr, replayTLI)` (xlog.c) — transition
    /// the cluster into archive recovery: set `InArchiveRecovery`, update the
    /// control file's state, and (re)enable WAL archiving accounting. `Err`
    /// carries the control-file-update `ereport(ERROR)`.
    pub fn switch_into_archive_recovery(
        end_rec_ptr: XLogRecPtr,
        replay_tli: TimeLineID,
    ) -> PgResult<()>
);

// ===========================================================================
// WAL segment-file path / checkpoint-need / install-active helpers consumed by
// the recovery page-read driver (xlogrecovery.c XLogFileRead / XLogPageRead /
// WaitForWALToBecomeAvailable). Owned by xlog.c (the `static`
// XLogFileSegmentInstallActive flag, XLogCheckpointNeeded, and XLogFilePath).
// xlog.c is needs-decomp, so these stay seam-and-panic until it lands.
// ===========================================================================

seam_core::seam!(
    /// `XLogFilePath(path, tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h) —
    /// the full path of a WAL segment file inside `pg_wal` (`XLOGDIR "/%s"`
    /// where `%s` is the bare segment file name). Pure path arithmetic.
    pub fn xlog_file_path(tli: TimeLineID, log_seg_no: XLogSegNo) -> alloc::string::String
);

seam_core::seam!(
    /// `XLogCheckpointNeeded(new_segno)` (xlog.c) — whether enough WAL has been
    /// written since the last checkpoint that a new one should be requested.
    pub fn xlog_checkpoint_needed(new_segno: XLogSegNo) -> bool
);

seam_core::seam!(
    /// `IsInstallXLogFileSegmentActive()` (xlog.c) — whether WAL-segment
    /// installation into `pg_wal` is currently permitted (used by the recovery
    /// page-read driver to assert it is OFF before restoring from archive).
    pub fn is_install_xlog_file_segment_active() -> bool
);

seam_core::seam!(
    /// `GetFakeLSNForUnloggedRel()` (xlog.c) — hand out a monotonically
    /// increasing fake LSN for an unlogged relation (advances the shmem
    /// `unloggedLSN` atomic). Reached by `gistGetFakeLSN`'s unlogged-relation
    /// branch; the shmem `unloggedLSN` accessor is still an extern in the xlog
    /// owner, so this panics until xlog installs it.
    pub fn get_fake_lsn_for_unlogged_rel() -> XLogRecPtr
);

// ---------------------------------------------------------------------------
// Checkpointer-consumed seams (checkpointer.c reaches these in xlog.c /
// xlogrecovery.c). The owning unit holds the `CheckpointState` machine and the
// shmem WAL-write driver, so these run inside the xlog owner.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `CreateCheckPoint(flags)` (xlog.c:6951) — perform a checkpoint with the
    /// given `CHECKPOINT_*` flags. Returns `true` if a checkpoint was performed
    /// (`false` only when it was skipped because nothing changed). The xlog
    /// owner holds the `CheckpointState` machine. Can `ereport(ERROR)`.
    pub fn create_checkpoint(flags: i32) -> PgResult<bool>
);

seam_core::seam!(
    /// `CreateRestartPoint(flags)` (xlog.c:7655) — establish a restartpoint
    /// during recovery. Returns `true` if a new restartpoint was established.
    /// Can `ereport(ERROR)`.
    pub fn create_restartpoint(flags: i32) -> PgResult<bool>
);

seam_core::seam!(
    /// `ShutdownXLOG(code, arg)` (xlog.c) — write the shutdown checkpoint (or a
    /// restartpoint in recovery) and shut down the WAL machinery. Can
    /// `ereport(ERROR)`.
    pub fn shutdown_xlog() -> PgResult<()>
);

seam_core::seam!(
    /// `GetLastSegSwitchData(&last_switch_lsn)` (xlog.c) — return
    /// `(lastSegSwitchTime, lastSegSwitchLSN)` under `info_lck`. The time is a
    /// `pg_time_t` (seconds). Infallible (a spinlock read).
    pub fn get_last_seg_switch_data() -> (i64, XLogRecPtr)
);

seam_core::seam!(
    /// `GetLastImportantRecPtr()` (xlog.c) — start of the last WAL record that
    /// mattered for archiving / replication. Infallible (a per-lock scan).
    pub fn get_last_important_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `RequestXLogSwitch(mark_unimportant)` (xlog.c) — force a WAL segment
    /// switch and return the resulting switch LSN. Can `ereport(ERROR)`.
    pub fn request_xlog_switch(mark_unimportant: bool) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `UpdateFullPageWrites()` (xlog.c) — propagate a SIGHUP change of
    /// `full_page_writes` into shmem, writing `XLOG_FPW_CHANGE` if needed. Can
    /// `ereport(ERROR)` (it inserts WAL).
    pub fn update_full_page_writes() -> PgResult<()>
);

seam_core::seam!(
    /// `XLogArchiveTimeout` (xlog.c GUC, seconds) — `archive_timeout`. A plain
    /// global read; infallible.
    pub fn xlog_archive_timeout() -> i32
);

seam_core::seam!(
    /// `CheckPointSegments` (xlog.c global) — the floating segment budget used
    /// by `IsCheckpointOnSchedule`. A `double`; infallible read.
    pub fn check_point_segments() -> f64
);

seam_core::seam!(
    /// Store post-sync metrics into `CheckpointStats` (xlog.c global):
    /// `ckpt_sync_rels`, `ckpt_longest_sync`, `ckpt_agg_sync_time`. sync.c
    /// reports its aggregate fsync timings up to xlog.c through this. Pure
    /// bookkeeping; infallible.
    pub fn checkpoint_stats_set(
        ckpt_sync_rels: i32,
        ckpt_longest_sync: u64,
        ckpt_agg_sync_time: u64,
    )
);

seam_core::seam!(
    /// `XLogBackgroundFlush()` (xlog.c) — flush as much WAL to disk as is
    /// cheaply available; called by a stopping walsender to trigger pending WAL
    /// to be written out. Returns whether it flushed anything (discarded by the
    /// walsender caller).
    pub fn xlog_background_flush() -> bool
);

seam_core::seam!(
    /// `ReachedEndOfBackup(XLogRecPtr EndRecPtr, TimeLineID tli)` (xlog.c) —
    /// at the end of a backup, mark the control file consistent
    /// (`ControlFile->minRecoveryPoint`/`backupStartPoint`/`backupEndRequired`
    /// updates) and `UpdateControlFile()`. `ereport(ERROR)` on the control-file
    /// write path.
    pub fn reached_end_of_backup(
        end_rec_ptr: XLogRecPtr,
        tli: TimeLineID
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveNonParentXlogFiles(XLogRecPtr switchpoint, TimeLineID newTLI)`
    /// (xlog.c) — after a timeline switch during recovery, remove any WAL
    /// segments on the old timeline(s) that are not part of `newTLI`'s history.
    /// `ereport(ERROR)` on the directory-scan / unlink path.
    pub fn remove_non_parent_xlog_files(
        switchpoint: XLogRecPtr,
        new_tli: TimeLineID
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AllowCascadeReplication()` (xlog.h) — `EnableHotStandby && server is in
    /// archive recovery`: whether cascading walsenders may run, governing
    /// whether the redo loop wakes physical/logical walsenders.
    pub fn allow_cascade_replication() -> bool
);
