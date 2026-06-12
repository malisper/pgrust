//! Seam declarations for the `backend-replication-logical-decode` unit (`decode.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `xlog_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn xlog_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `xact_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn xact_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `standby_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn standby_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap2_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn heap2_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn heap_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `logicalmsg_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn logicalmsg_decode(ctx: &mut types_wal::rmgr::LogicalDecodingContext<'_>, buf: &mut types_wal::rmgr::XLogRecordBuffer<'_, '_>) -> types_error::PgResult<()>
);
