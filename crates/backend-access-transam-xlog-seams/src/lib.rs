//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `xlog_redo(record)` (xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogFlush(record)` (xlog.c) — ensure WAL through `record` is flushed
    /// to durable storage. `Err` carries its `ereport`s (e.g. the
    /// "xlog flush request is not satisfied" PANIC-in-crit-section path and
    /// the write errors underneath).
    pub fn xlog_flush(record: types_core::primitive::XLogRecPtr) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckpointStats.ckpt_slru_written++` (xlog.c's `CheckpointStats`
    /// global, bumped directly by slru.c during checkpoint write-all).
    /// Narrow write-side capability on the owner's global, same shape as
    /// `set_my_backend_type` (see DESIGN_DEBT.md).
    pub fn count_ckpt_slru_written()
);
