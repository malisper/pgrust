//! Seam-boundary handle/view types for `replication/logical/logical.c`.
//!
//! `logical.c` only *forwards* a set of cross-subsystem pointers — it never
//! dereferences their internals — so they are modeled as opaque handles the
//! owning subsystem resolves. The handles are newtypes (not bare `usize`
//! aliases): logical.c neither integer-inspects nor constructs them, it only
//! passes them through the owner's seams. The owner widens/replaces them with
//! the real struct reference when it lands.
//!
//! The view types (`XLogReadResult` / `ReorderBufferStats` /
//! `OutputPluginCallbackArgs` / `CallbackInvocation` / `WalLevel`) are the
//! shapes that cross those seams; they mirror the C arguments the wrappers
//! pass after they have set the ctx output-state fields.

#![allow(non_upper_case_globals)]

use types_core::primitive::{RepOriginId, Size, TimestampTz, TransactionId, XLogRecPtr};

/// `WalLevel` (`access/xlog.h`) — `WAL_LEVEL_MINIMAL=0`, `WAL_LEVEL_REPLICA=1`,
/// `WAL_LEVEL_LOGICAL=2`. The `wal_level`/`GetActiveWalLevelOnStandby` reads
/// compare against `WAL_LEVEL_LOGICAL`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalLevel(pub i32);

/// `WAL_LEVEL_MINIMAL`.
pub const WAL_LEVEL_MINIMAL: WalLevel = WalLevel(0);
/// `WAL_LEVEL_REPLICA`.
pub const WAL_LEVEL_REPLICA: WalLevel = WalLevel(1);
/// `WAL_LEVEL_LOGICAL`.
pub const WAL_LEVEL_LOGICAL: WalLevel = WalLevel(2);

macro_rules! opaque_handle {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
        pub struct $name(pub usize);
    };
}

opaque_handle!(
    /// Opaque `XLogReaderState *`.
    XLogReaderHandle
);
opaque_handle!(
    /// Opaque `ReorderBuffer *`.
    ReorderBufferHandle
);
opaque_handle!(
    /// Opaque `SnapBuild *` (snapshot builder).
    SnapBuildHandle
);
opaque_handle!(
    /// Opaque `StringInfo` (output buffer, `ctx->out`).
    StringInfoHandle
);
opaque_handle!(
    /// Opaque `MemoryContext`.
    MemoryContextHandle
);
opaque_handle!(
    /// Opaque `XLogReaderRoutine *`.
    XLogReaderRoutineHandle
);
opaque_handle!(
    /// Opaque `ResourceOwner` (`CurrentResourceOwner`).
    ResourceOwnerHandle
);
opaque_handle!(
    /// Opaque `ReorderBufferTXN *`.
    TxnHandle
);
opaque_handle!(
    /// Opaque `Relation` — only forwarded to the output-plugin callbacks.
    RelationHandle
);
opaque_handle!(
    /// Opaque `Relation[]` — `nrelations`/`relations` forwarded to `truncate_cb`.
    RelationsHandle
);
opaque_handle!(
    /// Opaque `ReorderBufferChange *`.
    ChangeHandle
);
opaque_handle!(
    /// Opaque `const char *prefix`.
    PrefixHandle
);
opaque_handle!(
    /// Opaque `const char *message`.
    MessageHandle
);
opaque_handle!(
    /// Opaque `const char *gid`.
    GidHandle
);
opaque_handle!(
    /// Opaque `List *output_plugin_options` (a `List *` of `DefElem`). The
    /// decoding caller builds it; `logical.c` only stores it in `ctx` and
    /// forwards it to the plugin startup callback. `0` is the C `NIL`.
    OutputPluginOptionsHandle
);

/// One decoded record read by `XLogReadRecord`: whether a record was returned,
/// and whether an error string was set. Mirrors the C
/// `record = XLogReadRecord(reader, &err)` two-out-param shape.
pub struct XLogReadResult {
    /// True if `XLogReadRecord` returned a non-NULL `XLogRecord *`.
    pub record: bool,
    /// `Some(err)` if the `errm`/`err` out-parameter was set, else `None`.
    pub err: Option<String>,
}

/// Snapshot of the eight `ReorderBuffer` stat counters, mapped 1:1 onto
/// `PgStat_StatReplSlotEntry` in `UpdateDecodingStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReorderBufferStats {
    /// `rb->spillTxns`.
    pub spill_txns: i64,
    /// `rb->spillCount`.
    pub spill_count: i64,
    /// `rb->spillBytes`.
    pub spill_bytes: i64,
    /// `rb->streamTxns`.
    pub stream_txns: i64,
    /// `rb->streamCount`.
    pub stream_count: i64,
    /// `rb->streamBytes`.
    pub stream_bytes: i64,
    /// `rb->totalTxns`.
    pub total_txns: i64,
    /// `rb->totalBytes`.
    pub total_bytes: i64,
}

/// Which output-plugin callback to invoke, plus its non-`ctx` arguments.
///
/// Each variant carries the same values the C wrapper passes to the plugin
/// function pointer after the wrapper has set `ctx->accept_writes` /
/// `write_xid` / `write_location` / `end_xact`. The `txn`/`relation`/`change`
/// pointers are the opaque handles `logical.c` only forwards.
pub enum OutputPluginCallbackArgs {
    /// `startup_cb(ctx, opt, is_init)`.
    Startup { is_init: bool },
    /// `shutdown_cb(ctx)`.
    Shutdown,
    /// `begin_cb(ctx, txn)`.
    Begin { txn: TxnHandle },
    /// `commit_cb(ctx, txn, commit_lsn)`.
    Commit { txn: TxnHandle, commit_lsn: XLogRecPtr },
    /// `begin_prepare_cb(ctx, txn)`.
    BeginPrepare { txn: TxnHandle },
    /// `prepare_cb(ctx, txn, prepare_lsn)`.
    Prepare {
        txn: TxnHandle,
        prepare_lsn: XLogRecPtr,
    },
    /// `commit_prepared_cb(ctx, txn, commit_lsn)`.
    CommitPrepared {
        txn: TxnHandle,
        commit_lsn: XLogRecPtr,
    },
    /// `rollback_prepared_cb(ctx, txn, prepare_end_lsn, prepare_time)`.
    RollbackPrepared {
        txn: TxnHandle,
        prepare_end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
    },
    /// `change_cb(ctx, txn, relation, change)`.
    Change {
        txn: TxnHandle,
        relation: RelationHandle,
        change: ChangeHandle,
    },
    /// `truncate_cb(ctx, txn, nrelations, relations, change)`.
    Truncate {
        txn: TxnHandle,
        nrelations: i32,
        relations: RelationsHandle,
        change: ChangeHandle,
    },
    /// `message_cb(ctx, txn, message_lsn, transactional, prefix, sz, message)`.
    Message {
        txn: TxnHandle,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: PrefixHandle,
        message_size: Size,
        message: MessageHandle,
    },
    /// `filter_prepare_cb(ctx, xid, gid)` — returns a bool.
    FilterPrepare { xid: TransactionId, gid: GidHandle },
    /// `filter_by_origin_cb(ctx, origin_id)` — returns a bool.
    FilterByOrigin { origin_id: RepOriginId },
    /// `stream_start_cb(ctx, txn)`.
    StreamStart { txn: TxnHandle },
    /// `stream_stop_cb(ctx, txn)`.
    StreamStop { txn: TxnHandle },
    /// `stream_abort_cb(ctx, txn, abort_lsn)`.
    StreamAbort {
        txn: TxnHandle,
        abort_lsn: XLogRecPtr,
    },
    /// `stream_prepare_cb(ctx, txn, prepare_lsn)`.
    StreamPrepare {
        txn: TxnHandle,
        prepare_lsn: XLogRecPtr,
    },
    /// `stream_commit_cb(ctx, txn, commit_lsn)`.
    StreamCommit {
        txn: TxnHandle,
        commit_lsn: XLogRecPtr,
    },
    /// `stream_change_cb(ctx, txn, relation, change)`.
    StreamChange {
        txn: TxnHandle,
        relation: RelationHandle,
        change: ChangeHandle,
    },
    /// `stream_message_cb(ctx, txn, message_lsn, transactional, prefix, sz, m)`.
    StreamMessage {
        txn: TxnHandle,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: PrefixHandle,
        message_size: Size,
        message: MessageHandle,
    },
    /// `stream_truncate_cb(ctx, txn, nrelations, relations, change)`.
    StreamTruncate {
        txn: TxnHandle,
        nrelations: i32,
        relations: RelationsHandle,
        change: ChangeHandle,
    },
}

/// The output-state fields a wrapper sets on the live ctx before invoking the
/// plugin callback, plus the errcontext `callback_name`/`report_location`.
/// The owner applies these onto the ctx it owns and pushes the
/// `output_plugin_error_callback` errcontext around the real plugin call.
pub struct CallbackInvocation {
    /// `state.callback_name`.
    pub callback_name: &'static str,
    /// `state.report_location`.
    pub report_location: XLogRecPtr,
    /// `ctx->accept_writes`.
    pub accept_writes: bool,
    /// `ctx->write_xid`.
    pub write_xid: TransactionId,
    /// `ctx->write_location`.
    pub write_location: XLogRecPtr,
    /// `ctx->end_xact`.
    pub end_xact: bool,
    /// Which plugin callback to call, plus its non-ctx arguments.
    pub args: OutputPluginCallbackArgs,
}

/// Which ReorderBuffer-driven `*_cb_wrapper` to run, with the fields the C
/// wrapper reads off `txn`/`change`/`relation` (already projected by the
/// reorderbuffer owner, since those structs live in its crate).
///
/// In C, `StartupDecodingContext` stores the addresses of the file-static
/// `*_cb_wrapper` functions into `ctx->reorder->{begin,apply_change,...}`, and
/// ReorderBuffer later calls them through those pointers with
/// `cache->private_data == ctx`. Here the reorderbuffer owner re-enters
/// logical decoding via this enum across the inward dispatch seam.
pub enum ReorderBufferCallback {
    /// `begin` -> `begin_cb_wrapper`.
    Begin {
        txn: TxnHandle,
        txn_first_lsn: XLogRecPtr,
        txn_xid: TransactionId,
    },
    /// `apply_change` -> `change_cb_wrapper`.
    ApplyChange {
        txn: TxnHandle,
        txn_xid: TransactionId,
        relation: RelationHandle,
        change: ChangeHandle,
        change_lsn: XLogRecPtr,
    },
    /// `apply_truncate` -> `truncate_cb_wrapper`.
    ApplyTruncate {
        txn: TxnHandle,
        txn_xid: TransactionId,
        nrelations: i32,
        relations: RelationsHandle,
        change: ChangeHandle,
        change_lsn: XLogRecPtr,
    },
    /// `commit` -> `commit_cb_wrapper`.
    Commit {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        commit_lsn: XLogRecPtr,
    },
    /// `message` -> `message_cb_wrapper`. `txn` is NULL for non-transactional.
    Message {
        txn: Option<(TxnHandle, TransactionId)>,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: PrefixHandle,
        message_size: Size,
        message: MessageHandle,
    },
    /// `begin_prepare` -> `begin_prepare_cb_wrapper`.
    BeginPrepare {
        txn: TxnHandle,
        txn_first_lsn: XLogRecPtr,
        txn_xid: TransactionId,
    },
    /// `prepare` -> `prepare_cb_wrapper`.
    Prepare {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        prepare_lsn: XLogRecPtr,
    },
    /// `commit_prepared` -> `commit_prepared_cb_wrapper`.
    CommitPrepared {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        commit_lsn: XLogRecPtr,
    },
    /// `rollback_prepared` -> `rollback_prepared_cb_wrapper`.
    RollbackPrepared {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        prepare_end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
    },
    /// `stream_start` -> `stream_start_cb_wrapper`.
    StreamStart {
        txn: TxnHandle,
        txn_xid: TransactionId,
        first_lsn: XLogRecPtr,
    },
    /// `stream_stop` -> `stream_stop_cb_wrapper`.
    StreamStop {
        txn: TxnHandle,
        txn_xid: TransactionId,
        last_lsn: XLogRecPtr,
    },
    /// `stream_abort` -> `stream_abort_cb_wrapper`.
    StreamAbort {
        txn: TxnHandle,
        txn_xid: TransactionId,
        abort_lsn: XLogRecPtr,
    },
    /// `stream_prepare` -> `stream_prepare_cb_wrapper`.
    StreamPrepare {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        prepare_lsn: XLogRecPtr,
    },
    /// `stream_commit` -> `stream_commit_cb_wrapper`.
    StreamCommit {
        txn: TxnHandle,
        txn_xid: TransactionId,
        txn_final_lsn: XLogRecPtr,
        txn_end_lsn: XLogRecPtr,
        commit_lsn: XLogRecPtr,
    },
    /// `stream_change` -> `stream_change_cb_wrapper`.
    StreamChange {
        txn: TxnHandle,
        txn_xid: TransactionId,
        relation: RelationHandle,
        change: ChangeHandle,
        change_lsn: XLogRecPtr,
    },
    /// `stream_message` -> `stream_message_cb_wrapper`.
    StreamMessage {
        txn: Option<(TxnHandle, TransactionId)>,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: PrefixHandle,
        message_size: Size,
        message: MessageHandle,
    },
    /// `stream_truncate` -> `stream_truncate_cb_wrapper`.
    StreamTruncate {
        txn: TxnHandle,
        txn_xid: TransactionId,
        nrelations: i32,
        relations: RelationsHandle,
        change: ChangeHandle,
        change_lsn: XLogRecPtr,
    },
    /// `update_progress_txn` -> `update_progress_txn_cb_wrapper`.
    UpdateProgressTxn {
        txn_xid: TransactionId,
        lsn: XLogRecPtr,
    },
}
