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

use types_core::primitive::{Oid, RepOriginId, Size, TimestampTz, TransactionId, XLogRecPtr};

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
/// `ResourceOwner` (`CurrentResourceOwner`) — the one canonical
/// [`types_resowner::ResourceOwner`] handle, re-exported so logical decoding
/// keeps naming it `ResourceOwnerHandle`.
pub type ResourceOwnerHandle = types_resowner::ResourceOwner;
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

/* =========================================================================
 * LogicalDecodingContext + output-plugin descriptor structs (output_plugin.h,
 * logical.h)
 *
 * These live here — in the shared seam-boundary types crate — so that the
 * single canonical [`LogicalDecodingContext`] is namable by *both* of its
 * users: `backend-replication-logical-logical` (which owns its lifecycle) and
 * `backend-replication-logical-decode` (whose rmgr `rm_decode` callbacks
 * receive it), as well as `types-wal`'s [`RmDecode`](../wal) callback
 * type which keys the resource-manager table on it. Before unification there
 * were two divergent definitions (a trimmed `context`/`fast_forward` shape in
 * `types-wal::rmgr` and this rich one in `logical.c`); the trimmed one could
 * not carry the `snapshot_builder`/`slot`/`twophase`/`callbacks` state that
 * `decode.c` reads, so it has been retired in favour of this rich struct.
 * ========================================================================= */

/// `OutputPluginOutputType` (`output_plugin.h`).
pub type OutputPluginOutputType = i32;
/// `OUTPUT_PLUGIN_BINARY_OUTPUT = 0`.
pub const OUTPUT_PLUGIN_BINARY_OUTPUT: OutputPluginOutputType = 0;
/// `OUTPUT_PLUGIN_TEXTUAL_OUTPUT = 1`.
pub const OUTPUT_PLUGIN_TEXTUAL_OUTPUT: OutputPluginOutputType = 1;

/// `OutputPluginOptions` (`output_plugin.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputPluginOptions {
    /// `output_type`.
    pub output_type: OutputPluginOutputType,
    /// `receive_rewrites`.
    pub receive_rewrites: bool,
}

/// `OutputPluginCallbacks` (`output_plugin.h`) — which plugin callbacks the
/// loaded plugin registered. `logical.c` only tests these for NULL and invokes
/// the corresponding pointer (via the dfmgr seam); presence is a bool. Field
/// order matches the C struct.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputPluginCallbacks {
    pub startup_cb: bool,
    pub begin_cb: bool,
    pub change_cb: bool,
    pub truncate_cb: bool,
    pub commit_cb: bool,
    pub message_cb: bool,
    pub filter_by_origin_cb: bool,
    pub shutdown_cb: bool,
    pub filter_prepare_cb: bool,
    pub begin_prepare_cb: bool,
    pub prepare_cb: bool,
    pub commit_prepared_cb: bool,
    pub rollback_prepared_cb: bool,
    pub stream_start_cb: bool,
    pub stream_stop_cb: bool,
    pub stream_abort_cb: bool,
    pub stream_prepare_cb: bool,
    pub stream_commit_cb: bool,
    pub stream_change_cb: bool,
    pub stream_message_cb: bool,
    pub stream_truncate_cb: bool,
}

impl OutputPluginCallbacks {
    /// Decode the callback-presence bitmask `load_output_plugin` returns (one
    /// bit per callback, C struct field order, LSB = `startup_cb`).
    pub fn from_bits(bits: u32) -> Self {
        OutputPluginCallbacks {
            startup_cb: bits & (1 << 0) != 0,
            begin_cb: bits & (1 << 1) != 0,
            change_cb: bits & (1 << 2) != 0,
            truncate_cb: bits & (1 << 3) != 0,
            commit_cb: bits & (1 << 4) != 0,
            message_cb: bits & (1 << 5) != 0,
            filter_by_origin_cb: bits & (1 << 6) != 0,
            shutdown_cb: bits & (1 << 7) != 0,
            filter_prepare_cb: bits & (1 << 8) != 0,
            begin_prepare_cb: bits & (1 << 9) != 0,
            prepare_cb: bits & (1 << 10) != 0,
            commit_prepared_cb: bits & (1 << 11) != 0,
            rollback_prepared_cb: bits & (1 << 12) != 0,
            stream_start_cb: bits & (1 << 13) != 0,
            stream_stop_cb: bits & (1 << 14) != 0,
            stream_abort_cb: bits & (1 << 15) != 0,
            stream_prepare_cb: bits & (1 << 16) != 0,
            stream_commit_cb: bits & (1 << 17) != 0,
            stream_change_cb: bits & (1 << 18) != 0,
            stream_message_cb: bits & (1 << 19) != 0,
            stream_truncate_cb: bits & (1 << 20) != 0,
        }
    }
}

/// `LogicalDecodingContext` (`logical.h`) — the single canonical decoding
/// context. Fields in C struct order. The cross-subsystem handles
/// (`reader`/`reorder`/`snapshot_builder`/`out`/`context`/`slot`) are opaque
/// values the owners resolve; the bool/LSN/xid state fields are written
/// directly by `logical.c`'s in-crate wrappers and read by `decode.c`'s rmgr
/// handlers.
pub struct LogicalDecodingContext {
    /// `MemoryContext context`.
    pub context: MemoryContextHandle,
    /// `ReplicationSlot *slot`. `logical.c` keeps `ctx->slot =
    /// MyReplicationSlot`; the runtime always operates on `MyReplicationSlot`,
    /// so this records that the slot is set.
    pub slot: bool,
    /// `ctx->slot->data.database` — the slot's database OID. `decode.c` reads
    /// it (the only `slot` field it touches) to filter changes to the slot's
    /// database. `logical.c` sets it when it wires `ctx->slot`.
    pub slot_database: Oid,
    /// `XLogReaderState *reader`.
    pub reader: XLogReaderHandle,
    /// `ReorderBuffer *reorder`.
    pub reorder: ReorderBufferHandle,
    /// `SnapBuild *snapshot_builder`.
    pub snapshot_builder: SnapBuildHandle,
    /// `bool fast_forward`.
    pub fast_forward: bool,
    /// `OutputPluginCallbacks callbacks`.
    pub callbacks: OutputPluginCallbacks,
    /// `OutputPluginOptions options`.
    pub options: OutputPluginOptions,
    /// `List *output_plugin_options`.
    pub output_plugin_options: OutputPluginOptionsHandle,
    /// `prepare_write` callback presence.
    pub prepare_write: bool,
    /// `write` callback presence.
    pub write: bool,
    /// `update_progress` callback presence.
    pub update_progress: bool,
    /// `StringInfo out`.
    pub out: StringInfoHandle,
    /// `bool streaming`.
    pub streaming: bool,
    /// `bool twophase`.
    pub twophase: bool,
    /// `bool twophase_opt_given`.
    pub twophase_opt_given: bool,
    /// `bool accept_writes`.
    pub accept_writes: bool,
    /// `bool prepared_write`.
    pub prepared_write: bool,
    /// `XLogRecPtr write_location`.
    pub write_location: XLogRecPtr,
    /// `TransactionId write_xid`.
    pub write_xid: TransactionId,
    /// `bool end_xact`.
    pub end_xact: bool,
    /// `void *output_plugin_private` — opaque per-plugin state the loaded output
    /// plugin stows in its `startup_cb` (e.g. pgoutput's `PGOutputData`) and
    /// recovers in every later callback. The owned, value-typed replacement for
    /// the C `void *`: any plugin's private struct, type-erased.
    pub output_plugin_private: Option<Box<dyn core::any::Any>>,
    /// `bool processing_required`.
    pub processing_required: bool,
    /// The name of the BUILTIN (in-process ported) output plugin backing this
    /// context, set by `LoadOutputPlugin` when the slot's plugin resolves to a
    /// registered builtin (e.g. `"test_decoding"`). `None` means the plugin is a
    /// genuine OS-loaded `.so` (unreachable in this build) — the dispatch path
    /// then falls back to the flattened OS-loader seam. In C there is no analog:
    /// `ctx->callbacks` are real function pointers; here the builtin vtable is
    /// keyed by name in the dfmgr-seams registry, and this carries the key so the
    /// per-change dispatch can reach the vtable WITH the live `&mut ctx`.
    pub builtin_plugin: Option<&'static str>,
    /// Per-context output writer (#351). C stores the
    /// `prepare_write`/`write`/`update_progress` function pointers plus
    /// `output_writer_private` directly on the context, so each
    /// `CreateDecodingContext` caller routes output to its own sink. This repo
    /// collapses the writer choice to a closed enum and a private payload.
    pub writer: OutputWriter,
    /// `void *output_writer_private` (#351) — the SQL-function decode path's
    /// private `DecodingOutputState` (a tuplestore sink), type-erased. The
    /// walsender path leaves this `None` (its writer needs no private state).
    pub output_writer_private: Option<Box<dyn core::any::Any>>,
}

/// Per-context output-writer target (#351). Selects where
/// `OutputPluginPrepareWrite`/`OutputPluginWrite`/`OutputPluginUpdateProgress`
/// route the decoded output, replacing C's per-context `prepare_write`/`write`/
/// `update_progress` function pointers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputWriter {
    /// No writer installed (`fast_forward`, or before `CreateDecodingContext`
    /// sets one). Writes are rejected, matching the C `accept_writes` guard.
    #[default]
    None,
    /// `WalSndPrepareWrite`/`WalSndWriteData`/`WalSndUpdateProgress`
    /// (walsender.c) — the streaming-replication path. Routes through the
    /// single global `walsender::call_*` seams.
    WalSnd,
    /// `LogicalOutputPrepareWrite`/`LogicalOutputWrite` (logicalfuncs.c) — the
    /// SQL-function path. `prepare` resets `ctx->out`; `write` reads `ctx->out`
    /// and appends an `(lsn, xid, data text)` row into the
    /// `output_writer_private` tuplestore sink.
    SqlSrf,
}

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
    /// `filter_prepare_cb(ctx, xid, gid)` — returns a bool. The C `const char
    /// *gid` is the real (NUL-stripped) gid bytes decode.c parsed out of the
    /// 2PC record and forwards verbatim to the plugin.
    FilterPrepare { xid: TransactionId, gid: Vec<u8> },
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

// ---------------------------------------------------------------------------
// Historic-snapshot tuplecid map (the `static HTAB *tuplecid_data` of
// snapmgr.c).
//
// In C the `(relfilelocator, ctid) -> (cmin, cmax)` lookup hash is built by
// `ReorderBufferBuildTupleCidHash` (reorderbuffer.c) into a per-txn `HTAB`,
// then handed to `SetupHistoricSnapshot(snapshot, tuplecid_hash)` which stores
// it in snapmgr's file-scope `tuplecid_data`; `HistoricSnapshotGetTupleCids()`
// hands it back to `ResolveCminCmaxDuringDecoding` for catalog visibility
// during logical decode. The key/entry structs are declared in reorderbuffer.c
// but the *storage* lives in snapmgr.
//
// To let snapmgr own the map value-typed (rather than the opaque `*mut HTAB`)
// without snapmgr depending on the reorderbuffer crate — the dependency runs
// the other way — the key/entry types and the map alias live here in
// `types-logical`, which both snapmgr (owner of the storage) and reorderbuffer
// (builder/resolver) already depend on.

use types_core::xact::CommandId;
use types_storage::RelFileLocator;
use types_tuple::ItemPointerData;

/// `ReorderBufferTupleCidKey` (reorderbuffer.c) — `(relfilelocator, ctid)`.
///
/// The C struct is hashed with `HASH_BLOBS` over its raw bytes after a
/// `memset(&key, 0, ...)`; the owned fields hash identically because the
/// derived `Hash`/`Eq` consider exactly the same logical values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ReorderBufferTupleCidKey {
    /// `RelFileLocator rlocator`.
    pub rlocator: RelFileLocator,
    /// `ItemPointerData tid`.
    pub tid: ItemPointerData,
}

/// `ReorderBufferTupleCidEnt` (reorderbuffer.c) — the resolved cmin/cmax for a
/// catalog tuple seen during decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReorderBufferTupleCidEnt {
    /// `CommandId cmin`.
    pub cmin: CommandId,
    /// `CommandId cmax`.
    pub cmax: CommandId,
    /// `CommandId combocid` — just for debugging.
    pub combocid: CommandId,
}

/// The `(relfilelocator, ctid) -> (cmin, cmax)` lookup hash itself — the value
/// behind snapmgr's `tuplecid_data` / C's `HTAB *`.
pub type TupleCidHash =
    std::collections::HashMap<ReorderBufferTupleCidKey, ReorderBufferTupleCidEnt>;
