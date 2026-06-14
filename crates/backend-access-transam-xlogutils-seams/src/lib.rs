//! Seam declarations for the `backend-access-transam-xlogutils` unit
//! (`access/transam/xlogutils.c`): accessors for the `standbyState` global it
//! owns. The owning unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

use types_storage::Buffer;
use types_wal::rmgr::XLogReaderState;
use types_wal::{HotStandbyState, XLogRedoAction};

seam_core::seam!(
    /// Read `standbyState` (xlogutils.c global).
    pub fn standby_state() -> HotStandbyState
);

seam_core::seam!(
    /// Write `standbyState` (standby.c sets `STANDBY_INITIALIZED`;
    /// xlogrecovery.c drives the rest of the machine).
    pub fn set_standby_state(state: HotStandbyState)
);

seam_core::seam!(
    /// Read `InRecovery` (xlogutils.c global, declared in `access/xlogutils.h`)
    /// — true while this process is replaying WAL records.
    pub fn in_recovery() -> bool
);

seam_core::seam!(
    /// Write `InRecovery` (xlog.c / xlogrecovery.c drive it during recovery).
    pub fn set_in_recovery(value: bool)
);

seam_core::seam!(
    /// Read `ignore_invalid_pages` (xlogutils.c GUC, declared in
    /// `access/xlogutils.h`) — when set, invalid-page references downgrade
    /// from PANIC to WARNING.
    pub fn ignore_invalid_pages() -> bool
);

seam_core::seam!(
    /// Write `ignore_invalid_pages` (set by the GUC machinery).
    pub fn set_ignore_invalid_pages(value: bool)
);

seam_core::seam!(
    /// `XLogReadBufferForRedo(record, block_id, &buf)` (xlogutils.c) — read and
    /// lock the buffer for the given block reference during redo, deciding via
    /// LSN whether replay is needed. Returns the [`XLogRedoAction`] and the
    /// buffer (which may be `InvalidBuffer` for `BlkNotFound`). Reads through
    /// the buffer manager and can `ereport(ERROR)` on a read failure, carried
    /// on `Err`.
    pub fn xlog_read_buffer_for_redo(
        record: &XLogReaderState<'_>,
        block_id: u8,
    ) -> types_error::PgResult<(XLogRedoAction, Buffer)>
);

seam_core::seam!(
    /// `XLogReadBufferForRedoExtended(record, block_id, mode, get_cleanup_lock,
    /// &buf)` (xlogutils.c) — the general form of [`xlog_read_buffer_for_redo`],
    /// allowing a non-`RBM_NORMAL` read mode and a cleanup (rather than
    /// exclusive) lock. Returns the [`XLogRedoAction`] and the buffer. Can
    /// `ereport(ERROR)` on a read failure, carried on `Err`.
    pub fn xlog_read_buffer_for_redo_extended(
        record: &XLogReaderState<'_>,
        block_id: u8,
        mode: types_storage::storage::ReadBufferMode,
        get_cleanup_lock: bool,
    ) -> types_error::PgResult<(XLogRedoAction, Buffer)>
);

seam_core::seam!(
    /// `XLogInitBufferForRedo(record, block_id)` (xlogutils.c) — pin and lock a
    /// buffer referenced by a WAL record, for re-initializing it from scratch
    /// (`RBM_ZERO_AND_LOCK`). Returns the locked buffer. Can `ereport(ERROR)`
    /// on a read failure, carried on `Err`.
    pub fn xlog_init_buffer_for_redo(
        record: &XLogReaderState<'_>,
        block_id: u8,
    ) -> types_error::PgResult<Buffer>
);

seam_core::seam!(
    /// `XLogReadBufferExtended(rlocator, FSM_FORKNUM, blkno, RBM_ZERO_ON_ERROR,
    /// InvalidBuffer)` (xlogutils.c) — read (extending/creating the FSM fork if
    /// the page is past EOF, per the redo extension rules) and pin a block of
    /// the relation's FSM fork during WAL replay, returning the pinned buffer.
    /// Used by `XLogRecordPageWithFreeSpace`. `Err` carries the smgr/read
    /// `ereport(ERROR)`s.
    pub fn xlog_read_buffer_extended_fsm(
        rlocator: types_storage::RelFileLocator,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<Buffer>
);
