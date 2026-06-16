//! Seam declarations for the `backend-replication-logical-decode` unit (`decode.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`), plus the top-level
//! `LogicalDecodingProcessRecord` dispatch entry consumed by `logical.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! Every handler takes the unified [`types_logical::LogicalDecodingContext`]
//! (the single canonical decoding context shared with `logical.c`) plus the
//! [`types_wal::rmgr::XLogRecordBuffer`] carrying the `XLogReaderHandle` the
//! handler reads the WAL record off through the xlogreader owner's accessors.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `xlog_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn xlog_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `xact_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn xact_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `standby_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn standby_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap2_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn heap2_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn heap_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `logicalmsg_decode(ctx, buf)` (decode.c) — logical-decoding dispatch for this
    /// resource manager's records (`rm_decode` slot). `elog(ERROR)`s on
    /// unexpected record info, carried on `Err`.
    pub fn logicalmsg_decode(ctx: &mut types_logical::LogicalDecodingContext, buf: &mut types_wal::rmgr::XLogRecordBuffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LogicalDecodingProcessRecord(ctx, record)` (decode.c:88): the top-level
    /// top-xid assign + rmgr `rm_decode` dispatch of one decoded record. `ctx`
    /// is the live decoding context (`logical.c` owns and threads it); `record`
    /// is the reader handle positioned on the record. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn LogicalDecodingProcessRecord(ctx: &mut types_logical::LogicalDecodingContext, record: types_logical::XLogReaderHandle) -> types_error::PgResult<()>
);
