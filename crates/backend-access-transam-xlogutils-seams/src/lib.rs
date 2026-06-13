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
