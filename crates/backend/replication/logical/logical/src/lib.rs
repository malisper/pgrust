#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

#[cfg(test)]
mod tests;

use std::rc::Rc;

use elog::{elog, ereport};
use init_small::globals as g;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use mcx::{Mcx, MemoryContext};
use reorderbuffer::{ReorderBuffer, ReorderBufferCallbacks, ReorderBufferChange, TxnId};
use slot::{
    CheckSlotRequirements, MyReplicationSlot, ReplicationSlot, ReplicationSlotMarkDirty,
    ReplicationSlotReserveWal, ReplicationSlotSave, ReplicationSlotsComputeRequiredLSN,
    ReplicationSlotsComputeRequiredXmin, SlotIsPhysical, RS_INVAL_NONE,
};
use snapbuild::SnapBuild;
use types_core::{
    InvalidOid, InvalidTransactionId, RepOriginId, TimestampTz, TransactionId,
    TransactionIdIsValid, TransactionIdPrecedes, XLogRecPtr,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};
use types_rel::RelationData;
use types_storage::storage::{PROC_ARRAY_LOCK, REPLICATION_SLOT_CONTROL_LOCK};
use xlogreader::XLogReaderState;

const InvalidXLogRecPtr: XLogRecPtr = 0;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn lsn_pair(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum OutputPluginOutputType {
    Binary = 0,
    Textual = 1,
}

#[derive(Clone, Copy)]
pub struct OutputPluginOptions {
    pub output_type: OutputPluginOutputType,
    pub receive_rewrites: bool,
}

pub type PluginOption = (String, Option<String>);

pub type StartupCB = fn(&mut OutputPluginContext, is_init: bool) -> PgResult<()>;
pub type ShutdownCB = fn(&mut OutputPluginContext) -> PgResult<()>;
pub type BeginCB = fn(&mut OutputPluginContext, &mut ReorderBuffer, TxnId) -> PgResult<()>;
pub type ChangeCB = fn(
    &mut OutputPluginContext,
    &mut ReorderBuffer,
    TxnId,
    &RelationData<'static>,
    &mut ReorderBufferChange,
) -> PgResult<()>;
pub type TruncateCB = fn(
    &mut OutputPluginContext,
    &mut ReorderBuffer,
    TxnId,
    &[Rc<RelationData<'static>>],
    &mut ReorderBufferChange,
) -> PgResult<()>;
pub type CommitCB =
    fn(&mut OutputPluginContext, &mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>;
// The prefix is a NUL-terminated C string in C, copied verbatim (no encoding
// check), so it travels as raw bytes.
pub type MessageCB = fn(
    &mut OutputPluginContext,
    &mut ReorderBuffer,
    Option<TxnId>,
    XLogRecPtr,
    bool,
    &[u8],
    &[u8],
) -> PgResult<()>;
pub type FilterByOriginCB = fn(&mut OutputPluginContext, RepOriginId) -> PgResult<bool>;
// Two-phase family (logical.h): begin_prepare shares BeginCB's shape and
// prepare/commit_prepared share CommitCB's (opc, rb, txn, lsn).
pub type BeginPrepareCB = BeginCB;
pub type PrepareCB = CommitCB;
pub type CommitPreparedCB = CommitCB;
pub type RollbackPreparedCB = fn(
    &mut OutputPluginContext,
    &mut ReorderBuffer,
    TxnId,
    XLogRecPtr,   // prepare_end_lsn
    TimestampTz,  // prepare_time
) -> PgResult<()>;
pub type FilterPrepareCB =
    fn(&mut OutputPluginContext, TransactionId, &str) -> PgResult<bool>;
// Streaming family (logical.h): start/stop share BeginCB's shape; abort/
// prepare/commit share CommitCB's (opc, rb, txn, lsn); change/message/
// truncate mirror their non-stream counterparts.
pub type StreamStartCB = BeginCB;
pub type StreamStopCB = BeginCB;
pub type StreamAbortCB = CommitCB;
pub type StreamPrepareCB = CommitCB;
pub type StreamCommitCB = CommitCB;
pub type StreamChangeCB = ChangeCB;
pub type StreamMessageCB = MessageCB;
pub type StreamTruncateCB = TruncateCB;

#[derive(Default)]
pub struct OutputPluginCallbacks {
    pub startup_cb: Option<StartupCB>,
    pub begin_cb: Option<BeginCB>,
    pub change_cb: Option<ChangeCB>,
    pub truncate_cb: Option<TruncateCB>,
    pub commit_cb: Option<CommitCB>,
    pub message_cb: Option<MessageCB>,
    pub filter_prepare_cb: Option<FilterPrepareCB>,
    pub begin_prepare_cb: Option<BeginPrepareCB>,
    pub prepare_cb: Option<PrepareCB>,
    pub commit_prepared_cb: Option<CommitPreparedCB>,
    pub rollback_prepared_cb: Option<RollbackPreparedCB>,
    pub filter_by_origin_cb: Option<FilterByOriginCB>,
    pub shutdown_cb: Option<ShutdownCB>,
    pub stream_start_cb: Option<StreamStartCB>,
    pub stream_stop_cb: Option<StreamStopCB>,
    pub stream_abort_cb: Option<StreamAbortCB>,
    pub stream_prepare_cb: Option<StreamPrepareCB>,
    pub stream_commit_cb: Option<StreamCommitCB>,
    pub stream_change_cb: Option<StreamChangeCB>,
    pub stream_message_cb: Option<StreamMessageCB>,
    pub stream_truncate_cb: Option<StreamTruncateCB>,
}

// C's ctx->out is a StringInfo carrying either textual (test_decoding) or
// binary (pgoutput 'w'-payload) data. A String can't hold arbitrary bytes, so
// the buffer is a Vec<u8> with the String-flavored methods the textual
// plugins use; binary writers (logicalproto) reach the Vec directly.
#[derive(Default)]
pub struct OutBuf {
    buf: Vec<u8>,
}

impl OutBuf {
    pub fn clear(&mut self) {
        self.buf.clear();
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
    pub fn push_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
    }
    pub fn push(&mut self, c: char) {
        let mut b = [0u8; 4];
        self.buf.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
    }
}

impl core::fmt::Write for OutBuf {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.buf.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

pub type LogicalOutputPluginWriterWrite =
    fn(&mut OutputPluginContext, XLogRecPtr, TransactionId, bool) -> PgResult<()>;
pub type LogicalOutputPluginWriterPrepareWrite = LogicalOutputPluginWriterWrite;
pub type LogicalOutputPluginWriterUpdateProgress =
    fn(&mut OutputPluginContext, XLogRecPtr, TransactionId, bool) -> PgResult<()>;

// C's LogicalDecodingContext is split in two: the plugin/writer-visible half
// lives in its own heap allocation reachable from rb.private_data so replay
// callbacks can take &mut to it while &mut ReorderBuffer is live.
pub struct OutputPluginContext {
    pub slot: &'static ReplicationSlot,
    pub callbacks: OutputPluginCallbacks,
    pub options: OutputPluginOptions,
    pub output_plugin_options: Vec<PluginOption>,
    pub prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    pub write: Option<LogicalOutputPluginWriterWrite>,
    pub update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
    pub out: OutBuf,
    pub output_plugin_private: usize,
    pub output_writer_private: usize,
    pub accept_writes: bool,
    pub prepared_write: bool,
    pub write_location: XLogRecPtr,
    pub write_xid: TransactionId,
    pub end_xact: bool,
    pub fast_forward: bool,
    pub streaming: bool,
    pub twophase: bool,
    pub twophase_opt_given: bool,
}

pub struct LogicalDecodingContext {
    context: *mut MemoryContext,
    pub slot: &'static ReplicationSlot,
    pub reader: XLogReaderState<'static>,
    pub reorder: ReorderBuffer,
    pub snapshot_builder: Box<SnapBuild>,
    pub fast_forward: bool,
    pub processing_required: bool,
    opc: *mut OutputPluginContext,
}

impl LogicalDecodingContext {
    // SAFETY contract: callers never hold two live &mut OutputPluginContext at
    // once; the allocation is owned by this context (reclaimed in free()) and
    // is disjoint from reorder/reader/snapshot_builder.
    #[allow(clippy::mut_from_ref)]
    pub fn opc(&self) -> &mut OutputPluginContext {
        unsafe { &mut *self.opc }
    }
}

fn opc_from_rb(rb: &ReorderBuffer) -> &'static mut OutputPluginContext {
    debug_assert!(rb.private_data != 0);
    // SAFETY: private_data is set by StartupDecodingContext to the live
    // OutputPluginContext of the owning decoding context.
    unsafe { &mut *(rb.private_data as *mut OutputPluginContext) }
}

// logical.c:148 standby wal_level guard. C's message is "... on the primary"
// with no trailing "server".
fn standby_wal_level_below_logical_error() -> Box<PgError> {
    Box::new(
        PgError::error(
            "logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary"
                .to_string(),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

pub fn CheckLogicalDecodingRequirements() -> PgResult<()> {
    CheckSlotRequirements()?;

    if transam_xlog::wal_level() < transam_xlog::WAL_LEVEL_LOGICAL {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("logical decoding requires \"wal_level\" >= \"logical\"")
            .finish(loc("CheckLogicalDecodingRequirements"))?;
        unreachable!();
    }

    if g::MyDatabaseId() == InvalidOid {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("logical decoding requires a database connection")
            .finish(loc("CheckLogicalDecodingRequirements"))?;
        unreachable!();
    }

    if transam_xlog::RecoveryInProgress() {
        // Logical decoding on standby is allowed when the primary runs
        // wal_level >= logical (GetActiveWalLevelOnStandby =
        // ControlFile->wal_level; xlog.c). Race notes as in C: rechecked at
        // slot creation and at decoding startup, and XLOG_PARAMETER_CHANGE
        // invalidates existing logical slots on a wal_level drop.
        if transam_xlog::GetActiveWalLevelOnStandby() < transam_xlog::WAL_LEVEL_LOGICAL {
            return Err(standby_wal_level_below_logical_error());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn StartupDecodingContext(
    output_plugin_options: Vec<PluginOption>,
    start_lsn: XLogRecPtr,
    xmin_horizon: TransactionId,
    need_full_snapshot: bool,
    fast_forward: bool,
    in_create: bool,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> PgResult<Box<LogicalDecodingContext>> {
    let slot = MyReplicationSlot().expect("decoding context requires an acquired slot");

    let context = Box::into_raw(Box::new(MemoryContext::new("Logical decoding context")));
    // SAFETY: `context` outlives the reader; both are reclaimed together in free().
    let mcx: Mcx<'static> = unsafe { std::mem::transmute((*context).mcx()) };

    let mut callbacks = OutputPluginCallbacks::default();
    if !fast_forward {
        let plugin = unsafe { slot.data.get() }.plugin;
        let plugin = std::str::from_utf8(plugin.name_str()).expect("plugin name is utf8");
        // upstream 2a29b607dbbb (18.6): Add an output_plugin_libraries GUC to bless trusted output plugins
        check_output_plugin_allowed(plugin)?;
        LoadOutputPlugin(&mut callbacks, plugin)?;
    }

    if !xact::IsTransactionOrTransactionBlock() {
        // Walsender path (logical.c:199): the slot enforces our xmin, so
        // announce this backend as skippable for horizon computation.
        procarray::ProcSetStatusFlagInLogicalDecoding()?;
    }

    let reader = XLogReaderState::allocate(mcx, transam_xlog::wal_segment_size())?;

    let slot_name_nd = unsafe { slot.data.get() }.name;
    let slot_name = std::str::from_utf8(slot_name_nd.name_str())
        .expect("slot name is utf8")
        .to_string();
    let mut reorder = ReorderBuffer::allocate(&slot_name)?;
    let snapshot_builder = snapbuild::allocate_snapshot_builder(
        xmin_horizon,
        start_lsn,
        need_full_snapshot,
        in_create,
        unsafe { slot.data.get() }.two_phase_at,
    );

    // To support streaming, start/stop/abort/commit/change callbacks are
    // required; message and truncate are optional. Streaming turns on when
    // any one of them is set so a missing member fails loudly in its wrapper
    // (logical.c:231).
    let streaming = callbacks.stream_start_cb.is_some()
        || callbacks.stream_stop_cb.is_some()
        || callbacks.stream_abort_cb.is_some()
        || callbacks.stream_commit_cb.is_some()
        || callbacks.stream_change_cb.is_some()
        || callbacks.stream_message_cb.is_some()
        || callbacks.stream_truncate_cb.is_some();
    // To support two-phase logical decoding we require the whole prepare
    // family; enabling on any one of them makes a missing member fail loudly
    // in its wrapper (logical.c:246).
    let twophase = twophase_from_callbacks(&callbacks);

    let opc = Box::into_raw(Box::new(OutputPluginContext {
        slot,
        callbacks,
        options: OutputPluginOptions {
            output_type: OutputPluginOutputType::Textual,
            receive_rewrites: false,
        },
        output_plugin_options,
        prepare_write,
        write: do_write,
        update_progress,
        out: OutBuf::default(),
        output_plugin_private: 0,
        output_writer_private: 0,
        accept_writes: false,
        prepared_write: false,
        write_location: InvalidXLogRecPtr,
        write_xid: InvalidTransactionId,
        end_xact: false,
        fast_forward,
        streaming,
        twophase,
        twophase_opt_given: false,
    }));

    reorder.private_data = opc as usize;
    reorder.update_stats = Some(update_decoding_stats_hook);
    reorder.callbacks = ReorderBufferCallbacks {
        begin: begin_cb_wrapper,
        apply_change: change_cb_wrapper,
        apply_truncate: truncate_cb_wrapper,
        commit: commit_cb_wrapper,
        message: message_cb_wrapper,
        begin_prepare: begin_prepare_cb_wrapper,
        prepare: prepare_cb_wrapper,
        commit_prepared: commit_prepared_cb_wrapper,
        rollback_prepared: rollback_prepared_cb_wrapper,
        update_progress_txn: update_progress_txn_cb_wrapper,
        ..ReorderBufferCallbacks::unset()
    };

    Ok(Box::new(LogicalDecodingContext {
        context,
        slot,
        reader,
        reorder,
        snapshot_builder,
        fast_forward,
        processing_required: false,
        opc,
    }))
}

pub fn CreateInitDecodingContext(
    plugin: &str,
    output_plugin_options: Vec<PluginOption>,
    need_full_snapshot: bool,
    restart_lsn: XLogRecPtr,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> PgResult<Box<LogicalDecodingContext>> {
    CheckLogicalDecodingRequirements()?;

    let Some(slot) = MyReplicationSlot() else {
        elog(
            ERROR,
            "cannot perform logical decoding without an acquired slot",
        )?;
        unreachable!();
    };

    if SlotIsPhysical(slot) {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot use physical replication slot for logical decoding")
            .finish(loc("CreateInitDecodingContext"))?;
        unreachable!();
    }

    if unsafe { slot.data.get() }.database != g::MyDatabaseId() {
        let name = String::from_utf8_lossy(unsafe { slot.data.get() }.name.name_str()).into_owned();
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "replication slot \"{name}\" was not created in this database"
            ))
            .finish(loc("CreateInitDecodingContext"))?;
        unreachable!();
    }

    if xact::IsTransactionState() && xact::GetTopTransactionIdIfAny() != InvalidTransactionId {
        ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(
                "cannot create logical replication slot in transaction that has performed writes",
            )
            .finish(loc("CreateInitDecodingContext"))?;
        unreachable!();
    }

    slot.with_mutex(|| {
        let mut d = unsafe { slot.data.get() };
        d.plugin.namestrcpy(plugin);
        unsafe { slot.data.set(d) };
    });

    if restart_lsn == InvalidXLogRecPtr {
        ReplicationSlotReserveWal()?;
    } else {
        slot.with_mutex(|| {
            let mut d = unsafe { slot.data.get() };
            d.restart_lsn = restart_lsn;
            unsafe { slot.data.set(d) };
        });
    }

    let control_lock = lwlock::main_lock(REPLICATION_SLOT_CONTROL_LOCK);
    let proc_array_lock = lwlock::main_lock(PROC_ARRAY_LOCK);
    LWLockAcquire(control_lock, LW_EXCLUSIVE, g::MyProcNumber())?;
    LWLockAcquire(proc_array_lock, LW_EXCLUSIVE, g::MyProcNumber())?;

    let xmin_horizon = procarray::GetOldestSafeDecodingTransactionId(!need_full_snapshot)?;

    slot.with_mutex(|| {
        unsafe { slot.effective_catalog_xmin.set(xmin_horizon) };
        let mut d = unsafe { slot.data.get() };
        d.catalog_xmin = xmin_horizon;
        unsafe { slot.data.set(d) };
        if need_full_snapshot {
            unsafe { slot.effective_xmin.set(xmin_horizon) };
        }
    });

    ReplicationSlotsComputeRequiredXmin(true)?;

    LWLockRelease(proc_array_lock)?;
    LWLockRelease(control_lock)?;

    ReplicationSlotMarkDirty();
    ReplicationSlotSave()?;

    let mut ctx = StartupDecodingContext(
        output_plugin_options,
        restart_lsn,
        xmin_horizon,
        need_full_snapshot,
        false,
        true,
        prepare_write,
        do_write,
        update_progress,
    )?;

    startup_cb_maybe(&mut ctx, true)?;

    let receive_rewrites = {
        let opc = ctx.opc();
        opc.twophase &= unsafe { slot.data.get() }.two_phase;
        opc.options.receive_rewrites
    };
    let mut ctx = ctx;
    ctx.reorder.output_rewrites = receive_rewrites;

    Ok(ctx)
}

pub fn CreateDecodingContext(
    start_lsn: XLogRecPtr,
    output_plugin_options: Vec<PluginOption>,
    fast_forward: bool,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> PgResult<Box<LogicalDecodingContext>> {
    let Some(slot) = MyReplicationSlot() else {
        elog(
            ERROR,
            "cannot perform logical decoding without an acquired slot",
        )?;
        unreachable!();
    };

    if SlotIsPhysical(slot) {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot use physical replication slot for logical decoding")
            .finish(loc("CreateDecodingContext"))?;
        unreachable!();
    }

    if unsafe { slot.data.get() }.database != g::MyDatabaseId() && !fast_forward {
        let name = String::from_utf8_lossy(unsafe { slot.data.get() }.name.name_str()).into_owned();
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "replication slot \"{name}\" was not created in this database"
            ))
            .finish(loc("CreateDecodingContext"))?;
        unreachable!();
    }

    // Slots being synced from the primary can't be used for decoding (they
    // are for use after failover) — but the slot sync machinery itself may
    // advance their LSNs (update_local_synced_slot).
    if transam_xlog::RecoveryInProgress()
        && unsafe { slot.data.get() }.synced != 0
        && !slot::syncing_replication_slots()
    {
        let name = String::from_utf8_lossy(unsafe { slot.data.get() }.name.name_str()).into_owned();
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot use replication slot \"{name}\" for logical decoding"
            ))
            .errdetail("This replication slot is being synchronized from the primary server.")
            .errhint("Specify another replication slot.")
            .finish(loc("CreateDecodingContext"))?;
        unreachable!();
    }

    debug_assert!(unsafe { slot.data.get() }.invalidated == RS_INVAL_NONE);
    debug_assert!(unsafe { slot.data.get() }.restart_lsn != InvalidXLogRecPtr);

    let mut start_lsn = start_lsn;
    if start_lsn == InvalidXLogRecPtr {
        start_lsn = unsafe { slot.data.get() }.confirmed_flush;
    } else if start_lsn < unsafe { slot.data.get() }.confirmed_flush {
        let (sh, sl) = lsn_pair(start_lsn);
        let (ch, cl) = lsn_pair(unsafe { slot.data.get() }.confirmed_flush);
        elog(
            LOG,
            format!("{sh:X}/{sl:X} has been already streamed, forwarding to {ch:X}/{cl:X}"),
        )?;
        start_lsn = unsafe { slot.data.get() }.confirmed_flush;
    }

    let mut ctx = StartupDecodingContext(
        output_plugin_options,
        start_lsn,
        InvalidTransactionId,
        false,
        fast_forward,
        false,
        prepare_write,
        do_write,
        update_progress,
    )?;

    startup_cb_maybe(&mut ctx, false)?;

    let (receive_rewrites, mark_two_phase) = {
        let opc = ctx.opc();
        opc.twophase &= unsafe { slot.data.get() }.two_phase || opc.twophase_opt_given;
        (opc.options.receive_rewrites, opc.twophase && !unsafe { slot.data.get() }.two_phase)
    };
    let mut ctx = ctx;
    // Mark slot to allow two_phase decoding if not already marked
    // (logical.c:597).
    if mark_two_phase {
        slot.with_mutex(|| {
            let mut d = unsafe { slot.data.get() };
            d.two_phase = true;
            d.two_phase_at = start_lsn;
            unsafe { slot.data.set(d) };
        });
        ReplicationSlotMarkDirty();
        ReplicationSlotSave()?;
        ctx.snapshot_builder.set_two_phase_at(start_lsn);
    }
    ctx.reorder.output_rewrites = receive_rewrites;

    let name = String::from_utf8_lossy(unsafe { slot.data.get() }.name.name_str()).into_owned();
    let (ch, cl) = lsn_pair(unsafe { slot.data.get() }.confirmed_flush);
    let (rh, rl) = lsn_pair(unsafe { slot.data.get() }.restart_lsn);
    ereport(LOG)
        .errmsg(format!("starting logical decoding for slot \"{name}\""))
        .errdetail(format!(
            "Streaming transactions committing after {ch:X}/{cl:X}, reading WAL from {rh:X}/{rl:X}."
        ))
        .finish(loc("CreateDecodingContext"))?;

    Ok(ctx)
}

fn startup_cb_maybe(ctx: &mut LogicalDecodingContext, is_init: bool) -> PgResult<()> {
    let opc = ctx.opc();
    if let Some(cb) = opc.callbacks.startup_cb {
        debug_assert!(!opc.fast_forward);
        opc.accept_writes = false;
        opc.end_xact = false;
        let r = cb(&mut *opc, is_init);
        with_output_plugin_context(opc, "startup", InvalidXLogRecPtr, r)?;
    }
    // The plugin's startup callback finalizes ctx->streaming (pgoutput turns
    // it off unless the subscriber negotiated it); only then wire the
    // reorderbuffer's stream slots — their presence IS the buffer's
    // ReorderBufferCanStream (C keys that off ctx->streaming instead,
    // reorderbuffer.c:4272).
    if opc.streaming {
        ctx.reorder.callbacks.stream_start = Some(stream_start_cb_wrapper);
        ctx.reorder.callbacks.stream_stop = Some(stream_stop_cb_wrapper);
        ctx.reorder.callbacks.stream_abort = Some(stream_abort_cb_wrapper);
        ctx.reorder.callbacks.stream_prepare = Some(stream_prepare_cb_wrapper);
        ctx.reorder.callbacks.stream_commit = Some(stream_commit_cb_wrapper);
        ctx.reorder.callbacks.stream_change = Some(stream_change_cb_wrapper);
        ctx.reorder.callbacks.stream_message = Some(stream_message_cb_wrapper);
        ctx.reorder.callbacks.stream_truncate = Some(stream_truncate_cb_wrapper);
    }
    Ok(())
}

pub fn DecodingContextReady(ctx: &LogicalDecodingContext) -> bool {
    ctx.snapshot_builder.current_state() == snapbuild::SnapBuildState::Consistent
}

impl LogicalDecodingContext {
    // FreeDecodingContext.
    pub fn free(self: Box<Self>) -> PgResult<()> {
        let opc = self.opc();
        if let Some(cb) = opc.callbacks.shutdown_cb {
            debug_assert!(!opc.fast_forward);
            opc.accept_writes = false;
            opc.end_xact = false;
            let r = cb(&mut *opc);
            with_output_plugin_context(opc, "shutdown", InvalidXLogRecPtr, r)?;
        }

        let LogicalDecodingContext {
            context,
            reader,
            reorder,
            snapshot_builder,
            opc,
            ..
        } = *self;
        snapbuild::free_snapshot_builder(snapshot_builder);
        reorder.free()?;
        drop(reader);
        // SAFETY: exclusive owner; the reader (the only mcx user) is already dropped.
        unsafe {
            drop(Box::from_raw(opc));
            drop(Box::from_raw(context));
        }
        Ok(())
    }
}

pub fn OutputPluginPrepareWrite(opc: &mut OutputPluginContext, last_write: bool) -> PgResult<()> {
    if !opc.accept_writes {
        return elog(
            ERROR,
            "writes are only accepted in commit, begin and change callbacks",
        );
    }
    let prepare_write = opc.prepare_write.expect("prepare_write installed");
    prepare_write(opc, opc.write_location, opc.write_xid, last_write)?;
    opc.prepared_write = true;
    Ok(())
}

pub fn OutputPluginWrite(opc: &mut OutputPluginContext, last_write: bool) -> PgResult<()> {
    if !opc.prepared_write {
        return elog(
            ERROR,
            "OutputPluginPrepareWrite needs to be called before OutputPluginWrite",
        );
    }
    let write = opc.write.expect("write installed");
    write(opc, opc.write_location, opc.write_xid, last_write)?;
    opc.prepared_write = false;
    Ok(())
}

pub fn OutputPluginUpdateProgress(
    opc: &mut OutputPluginContext,
    skipped_xact: bool,
) -> PgResult<()> {
    let Some(update_progress) = opc.update_progress else {
        return Ok(());
    };
    update_progress(opc, opc.write_location, opc.write_xid, skipped_xact)
}

// upstream 2a29b607dbbb (18.6): Add an output_plugin_libraries GUC to bless trusted output plugins
// Exact-name allowlist applied before LoadOutputPlugin (which applies no LOAD
// restrictions); superusers included. The detail is log-only, the hint goes
// to the client.
fn check_output_plugin_allowed(plugin: &str) -> PgResult<()> {
    let libraries = guc_tables::vars::output_plugin_libraries_string
        .read()
        .unwrap_or_default();
    let mut plugin_allowed = false;
    if !libraries.is_empty() {
        match pg_string::split_guc_list(&libraries, b',') {
            Ok(elemlist) => plugin_allowed = elemlist.iter().any(|allowed| allowed == plugin),
            Err(()) => {
                let _ = ereport(LOG)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("invalid list syntax in parameter \"output_plugin_libraries\"")
                    .finish(loc("StartupDecodingContext"));
            }
        }
    }
    if !plugin_allowed {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("library \"{plugin}\" may not be used as an output plugin"))
            .errdetail_log(format!(
                "The configuration parameter \"output_plugin_libraries\" (currently '{libraries}') does not name this library as a trusted output plugin."
            ))
            .errhint("If it is safe for all REPLICATION users to use this library as an output plugin, add it to \"output_plugin_libraries\" and reload the server configuration.")
            .finish(loc("StartupDecodingContext"));
    }
    Ok(())
}

fn LoadOutputPlugin(callbacks: &mut OutputPluginCallbacks, plugin: &str) -> PgResult<()> {
    let init = dfmgr::load_external_function(plugin, "_PG_output_plugin_init", false)?;
    let Some(init) = init else {
        return elog(
            ERROR,
            "output plugins have to declare the _PG_output_plugin_init symbol",
        );
    };

    // The registered init symbol is a PGFunction; arg 0 carries the callbacks
    // struct pointer (C casts the dlsym result instead).
    let mut flinfo = types_fmgr::FmgrInfo {
        fn_addr: init,
        fn_oid: InvalidOid,
        fn_nargs: 1,
        fn_strict: false,
        fn_retset: false,
        fn_stats: 0,
        fn_extra: None,
        fn_expr: None,
    };
    let scratch = MemoryContext::new("LoadOutputPlugin");
    types_fmgr::function_call1_coll_in(
        &mut flinfo,
        InvalidOid,
        scratch.mcx(),
        datum::Datum::from_usize(callbacks as *mut OutputPluginCallbacks as usize),
    )?;

    if callbacks.begin_cb.is_none() {
        return elog(ERROR, "output plugins have to register a begin callback");
    }
    if callbacks.change_cb.is_none() {
        return elog(ERROR, "output plugins have to register a change callback");
    }
    if callbacks.commit_cb.is_none() {
        return elog(ERROR, "output plugins have to register a commit callback");
    }
    Ok(())
}

// output_plugin_error_callback (logical.c:825): every output-plugin callback
// wrapper runs with an error-context frame naming the slot, plugin, callback
// and (where the callback has an associated LSN) its report location, so an
// ERROR raised inside the plugin carries the CONTEXT line. pgrust has no live
// error_context_stack for the ERROR path, so the context attaches on error
// propagation where C pushed the callback.
#[cold]
#[inline(never)]
fn attach_output_plugin_context(
    opc: &OutputPluginContext,
    callback_name: &str,
    report_location: XLogRecPtr,
    err: Box<PgError>,
) -> Box<PgError> {
    let d = unsafe { opc.slot.data.get() };
    let name = String::from_utf8_lossy(d.name.name_str()).into_owned();
    let plugin = String::from_utf8_lossy(d.plugin.name_str()).into_owned();
    let ctx = if report_location != InvalidXLogRecPtr {
        format!(
            "slot \"{name}\", output plugin \"{plugin}\", in the {callback_name} callback, associated LSN {:X}/{:X}",
            (report_location >> 32) as u32,
            report_location as u32
        )
    } else {
        format!("slot \"{name}\", output plugin \"{plugin}\", in the {callback_name} callback")
    };
    Box::new(PgError::from(err).add_context(ctx))
}

fn with_output_plugin_context<T>(
    opc: &OutputPluginContext,
    callback_name: &str,
    report_location: XLogRecPtr,
    res: PgResult<T>,
) -> PgResult<T> {
    res.map_err(|e| attach_output_plugin_context(opc, callback_name, report_location, e))
}

fn begin_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).first_lsn;
    opc.end_xact = false;
    let cb = opc.callbacks.begin_cb.expect("begin callback registered");
    let report_location = rb.txn(txn).first_lsn;
    let r = cb(&mut *opc, rb, txn);
    with_output_plugin_context(opc, "begin", report_location, r)
}

fn commit_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, commit_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn;
    opc.end_xact = true;
    let cb = opc.callbacks.commit_cb.expect("commit callback registered");
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, commit_lsn);
    with_output_plugin_context(opc, "commit", report_location, r)
}

#[cold]
fn missing_prepare_family_cb(which: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!(
            "logical replication at prepare time requires a {which} callback"
        ))
        .finish(loc("prepare_cb_wrapper"))?;
    unreachable!();
}

fn begin_prepare_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.twophase);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).first_lsn;
    opc.end_xact = false;
    // If the plugin supports two-phase commits then the begin prepare
    // callback is mandatory (logical.c:900).
    let Some(cb) = opc.callbacks.begin_prepare_cb else {
        return missing_prepare_family_cb("begin_prepare_cb");
    };
    let report_location = rb.txn(txn).first_lsn;
    let r = cb(&mut *opc, rb, txn);
    with_output_plugin_context(opc, "begin_prepare", report_location, r)
}

fn prepare_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, prepare_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.twophase);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn; // points to the end of the record
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.prepare_cb else {
        return missing_prepare_family_cb("prepare_cb");
    };
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, prepare_lsn);
    with_output_plugin_context(opc, "prepare", report_location, r)
}

fn commit_prepared_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.twophase);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn; // points to the end of the record
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.commit_prepared_cb else {
        return missing_prepare_family_cb("commit_prepared_cb");
    };
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, commit_lsn);
    with_output_plugin_context(opc, "commit_prepared", report_location, r)
}

fn rollback_prepared_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.twophase);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn; // points to the end of the record
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.rollback_prepared_cb else {
        return missing_prepare_family_cb("rollback_prepared_cb");
    };
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, prepare_end_lsn, prepare_time);
    with_output_plugin_context(opc, "rollback_prepared", report_location, r)
}

#[cold]
fn missing_stream_cb(which: &str) -> PgResult<()> {
    // logical.c:1230 — in streaming mode the start/stop/abort/commit/change
    // callbacks are required (and prepare, for streamed two-phase).
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!("logical streaming requires a {which} callback"))
        .finish(loc("stream_cb_wrapper"))?;
    unreachable!();
}

// stream_prepare_cb_wrapper's missing-callback error (logical.c:1512): in
// streaming mode with two-phase commits, stream_prepare_cb is required.
fn missing_stream_prepare_cb() -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg("logical streaming at prepare time requires a stream_prepare_cb callback")
        .finish(loc("stream_prepare_cb_wrapper"))?;
    unreachable!();
}

// logical.c:340: two-phase decoding is enabled when any of the prepare-family
// callbacks (including stream_prepare_cb and filter_prepare_cb) is provided;
// a missing member fails loudly in its wrapper.
pub(crate) fn twophase_from_callbacks(callbacks: &OutputPluginCallbacks) -> bool {
    callbacks.begin_prepare_cb.is_some()
        || callbacks.prepare_cb.is_some()
        || callbacks.commit_prepared_cb.is_some()
        || callbacks.rollback_prepared_cb.is_some()
        || callbacks.stream_prepare_cb.is_some()
        || callbacks.filter_prepare_cb.is_some()
}

fn stream_start_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, first_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = first_lsn;
    opc.end_xact = false;
    let Some(cb) = opc.callbacks.stream_start_cb else {
        return missing_stream_cb("stream_start_cb");
    };
    let report_location = first_lsn;
    let r = cb(&mut *opc, rb, txn);
    with_output_plugin_context(opc, "stream_start", report_location, r)
}

fn stream_stop_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, last_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = last_lsn;
    opc.end_xact = false;
    let Some(cb) = opc.callbacks.stream_stop_cb else {
        return missing_stream_cb("stream_stop_cb");
    };
    let report_location = last_lsn;
    let r = cb(&mut *opc, rb, txn);
    with_output_plugin_context(opc, "stream_stop", report_location, r)
}

fn stream_abort_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, abort_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = abort_lsn;
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.stream_abort_cb else {
        return missing_stream_cb("stream_abort_cb");
    };
    let report_location = abort_lsn;
    let r = cb(&mut *opc, rb, txn, abort_lsn);
    with_output_plugin_context(opc, "stream_abort", report_location, r)
}

fn stream_prepare_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, prepare_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    // We're only supposed to call this when streaming and two-phase commits
    // are both supported (logical.c:1281).
    debug_assert!(opc.twophase);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn;
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.stream_prepare_cb else {
        // In streaming mode with two-phase, stream_prepare_cb is required
        // (logical.c:1288).
        return missing_stream_prepare_cb();
    };
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, prepare_lsn);
    with_output_plugin_context(opc, "stream_prepare", report_location, r)
}

fn stream_commit_cb_wrapper(rb: &mut ReorderBuffer, txn: TxnId, commit_lsn: XLogRecPtr) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = rb.txn(txn).end_lsn;
    opc.end_xact = true;
    let Some(cb) = opc.callbacks.stream_commit_cb else {
        return missing_stream_cb("stream_commit_cb");
    };
    let report_location = rb.txn(txn).final_lsn;
    let r = cb(&mut *opc, rb, txn, commit_lsn);
    with_output_plugin_context(opc, "stream_commit", report_location, r)
}

fn stream_change_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relation: &RelationData<'static>,
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = change.lsn;
    opc.end_xact = false;
    let Some(cb) = opc.callbacks.stream_change_cb else {
        return missing_stream_cb("stream_change_cb");
    };
    let report_location = change.lsn;
    let r = cb(&mut *opc, rb, txn, relation, change);
    with_output_plugin_context(opc, "stream_change", report_location, r)
}

fn stream_message_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: &[u8],
    message: &[u8],
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    // This callback is optional (logical.c:1385).
    let Some(cb) = opc.callbacks.stream_message_cb else {
        return Ok(());
    };
    opc.accept_writes = true;
    opc.write_xid = txn.map(|t| rb.txn(t).xid).unwrap_or(InvalidTransactionId);
    opc.write_location = lsn;
    opc.end_xact = false;
    let report_location = lsn;
    let r = cb(&mut *opc, rb, txn, lsn, transactional, prefix, message);
    with_output_plugin_context(opc, "stream_message", report_location, r)
}

fn stream_truncate_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relations: &[Rc<RelationData<'static>>],
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    debug_assert!(opc.streaming);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = change.lsn;
    opc.end_xact = false;
    // This callback is optional (logical.c:1667).
    let Some(cb) = opc.callbacks.stream_truncate_cb else {
        return Ok(());
    };
    let report_location = change.lsn;
    let r = cb(&mut *opc, rb, txn, relations, change);
    with_output_plugin_context(opc, "stream_truncate", report_location, r)
}

pub fn filter_prepare_cb_wrapper(
    opc: &mut OutputPluginContext,
    xid: TransactionId,
    gid: &str,
) -> PgResult<bool> {
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = false;
    opc.end_xact = false;
    let cb = opc
        .callbacks
        .filter_prepare_cb
        .expect("filter_prepare callback registered");
    let r = cb(&mut *opc, xid, gid);
    with_output_plugin_context(opc, "filter_prepare", InvalidXLogRecPtr, r)
}

fn change_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relation: &RelationData<'static>,
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = change.lsn;
    opc.end_xact = false;
    let cb = opc.callbacks.change_cb.expect("change callback registered");
    let report_location = change.lsn;
    let r = cb(&mut *opc, rb, txn, relation, change);
    with_output_plugin_context(opc, "change", report_location, r)
}

fn truncate_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relations: &[Rc<RelationData<'static>>],
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    let Some(cb) = opc.callbacks.truncate_cb else {
        return Ok(());
    };
    opc.accept_writes = true;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = change.lsn;
    opc.end_xact = false;
    let report_location = change.lsn;
    let r = cb(&mut *opc, rb, txn, relations, change);
    with_output_plugin_context(opc, "truncate", report_location, r)
}

fn message_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: &[u8],
    message: &[u8],
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    let Some(cb) = opc.callbacks.message_cb else {
        return Ok(());
    };
    opc.accept_writes = true;
    opc.write_xid = match txn {
        Some(t) => rb.txn(t).xid,
        None => InvalidTransactionId,
    };
    opc.write_location = message_lsn;
    opc.end_xact = false;
    let report_location = message_lsn;
    let r = cb(&mut *opc, rb, txn, message_lsn, transactional, prefix, message);
    with_output_plugin_context(opc, "message", report_location, r)
}

fn update_progress_txn_cb_wrapper(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    let opc = opc_from_rb(rb);
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = false;
    opc.write_xid = rb.txn(txn).xid;
    opc.write_location = lsn;
    opc.end_xact = false;
    let r = OutputPluginUpdateProgress(&mut *opc, false);
    with_output_plugin_context(opc, "update_progress_txn", lsn, r)
}

pub fn filter_by_origin_cb_wrapper(
    opc: &mut OutputPluginContext,
    origin_id: RepOriginId,
) -> PgResult<bool> {
    debug_assert!(!opc.fast_forward);
    opc.accept_writes = false;
    opc.end_xact = false;
    let cb = opc
        .callbacks
        .filter_by_origin_cb
        .expect("filter_by_origin callback registered");
    let r = cb(&mut *opc, origin_id);
    with_output_plugin_context(opc, "filter_by_origin", InvalidXLogRecPtr, r)
}

pub fn LogicalIncreaseXminForSlot(current_lsn: XLogRecPtr, xmin: TransactionId) -> PgResult<()> {
    let slot = MyReplicationSlot().expect("LogicalIncreaseXminForSlot requires a slot");

    let mut updated_xmin = false;
    slot.with_mutex(|| {
        if TransactionIdPrecedes(xmin, unsafe { slot.data.get() }.catalog_xmin)
            || xmin == unsafe { slot.data.get() }.catalog_xmin
        {
        } else if current_lsn <= unsafe { slot.data.get() }.confirmed_flush {
            unsafe { slot.candidate_catalog_xmin.set(xmin) };
            unsafe { slot.candidate_xmin_lsn.set(current_lsn) };
            updated_xmin = true;
        } else if unsafe { slot.candidate_xmin_lsn.get() } == InvalidXLogRecPtr {
            unsafe { slot.candidate_catalog_xmin.set(xmin) };
            unsafe { slot.candidate_xmin_lsn.set(current_lsn) };
        }
    });

    if updated_xmin {
        LogicalConfirmReceivedLocation(unsafe { slot.data.get() }.confirmed_flush)?;
    }
    Ok(())
}

pub fn LogicalIncreaseRestartDecodingForSlot(
    current_lsn: XLogRecPtr,
    restart_lsn: XLogRecPtr,
) -> PgResult<()> {
    let slot = MyReplicationSlot().expect("LogicalIncreaseRestartDecodingForSlot requires a slot");
    debug_assert!(restart_lsn != InvalidXLogRecPtr);
    debug_assert!(current_lsn != InvalidXLogRecPtr);

    let mut updated_lsn = false;
    slot.with_mutex(|| {
        if restart_lsn <= unsafe { slot.data.get() }.restart_lsn {
        } else if current_lsn <= unsafe { slot.data.get() }.confirmed_flush {
            unsafe { slot.candidate_restart_valid.set(current_lsn) };
            unsafe { slot.candidate_restart_lsn.set(restart_lsn) };
            updated_lsn = true;
        } else if unsafe { slot.candidate_restart_valid.get() } == InvalidXLogRecPtr {
            unsafe { slot.candidate_restart_valid.set(current_lsn) };
            unsafe { slot.candidate_restart_lsn.set(restart_lsn) };
        }
    });

    if updated_lsn {
        LogicalConfirmReceivedLocation(unsafe { slot.data.get() }.confirmed_flush)?;
    }
    Ok(())
}

pub fn LogicalConfirmReceivedLocation(lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(lsn != InvalidXLogRecPtr);
    let slot = MyReplicationSlot().expect("LogicalConfirmReceivedLocation requires a slot");

    if unsafe { slot.candidate_xmin_lsn.get() } != InvalidXLogRecPtr
        || unsafe { slot.candidate_restart_valid.get() } != InvalidXLogRecPtr
    {
        let mut updated_xmin = false;
        let mut updated_restart = false;

        // logical.c:1824: remember the old restart lsn (consumed by the
        // logical-replication-slot-advance-segment injection point below).
        let old_restart_lsn = unsafe { slot.data.get() }.restart_lsn;

        slot.with_mutex(|| {
            if lsn > unsafe { slot.data.get() }.confirmed_flush {
                let mut d = unsafe { slot.data.get() };
                d.confirmed_flush = lsn;
                unsafe { slot.data.set(d) };
            }

            if unsafe { slot.candidate_xmin_lsn.get() } != InvalidXLogRecPtr
                && unsafe { slot.candidate_xmin_lsn.get() } <= lsn
            {
                let candidate = unsafe { slot.candidate_catalog_xmin.get() };
                if TransactionIdIsValid(candidate) && unsafe { slot.data.get() }.catalog_xmin != candidate {
                    let mut d = unsafe { slot.data.get() };
                    d.catalog_xmin = candidate;
                    unsafe { slot.data.set(d) };
                    unsafe { slot.candidate_catalog_xmin.set(InvalidTransactionId) };
                    unsafe { slot.candidate_xmin_lsn.set(InvalidXLogRecPtr) };
                    updated_xmin = true;
                }
            }

            if unsafe { slot.candidate_restart_valid.get() } != InvalidXLogRecPtr
                && unsafe { slot.candidate_restart_valid.get() } <= lsn
            {
                debug_assert!(unsafe { slot.candidate_restart_lsn.get() } != InvalidXLogRecPtr);
                let mut d = unsafe { slot.data.get() };
                d.restart_lsn = unsafe { slot.candidate_restart_lsn.get() };
                unsafe { slot.data.set(d) };
                unsafe { slot.candidate_restart_lsn.set(InvalidXLogRecPtr) };
                unsafe { slot.candidate_restart_valid.set(InvalidXLogRecPtr) };
                updated_restart = true;
            }
        });

        if updated_xmin || updated_restart {
            // logical.c:1901 (USE_INJECTION_POINTS): trigger only when the
            // slot's restart_lsn crossed into a new WAL segment.
            if injection_point::is_attached("logical-replication-slot-advance-segment") {
                let segsz = transam_xlog::wal_segment_size();
                let seg1 = transam_xlog::XLByteToSeg(old_restart_lsn, segsz);
                let seg2 = transam_xlog::XLByteToSeg(unsafe { slot.data.get() }.restart_lsn, segsz);
                if seg1 != seg2 {
                    injection_point::injection_point(
                        "logical-replication-slot-advance-segment",
                    )?;
                }
            }
            ReplicationSlotMarkDirty();
            ReplicationSlotSave()?;
        }

        if updated_xmin {
            slot.with_mutex(|| {
                unsafe { slot.effective_catalog_xmin
                    .set(slot.data.get().catalog_xmin) };
            });
            ReplicationSlotsComputeRequiredXmin(false)?;
            ReplicationSlotsComputeRequiredLSN()?;
        }
    } else {
        slot.with_mutex(|| {
            if lsn > unsafe { slot.data.get() }.confirmed_flush {
                let mut d = unsafe { slot.data.get() };
                d.confirmed_flush = lsn;
                unsafe { slot.data.set(d) };
            }
        });
    }
    Ok(())
}

// Clear logical streaming state during (sub)transaction abort (logical.c:1944).
// Called from AbortTransaction/AbortSubTransaction via the
// reset_logical_streaming_state seam, C's xact.c:2902/:5297 sites; this is
// what clears CheckXidAlive when a concurrent-abort error (or any other
// error) unwinds a prepared/streamed replay.
pub fn ResetLogicalStreamingState() {
    xact::SetCheckXidAlive(types_core::InvalidTransactionId);
    xact::SetBsysscan(false);
}

pub fn UpdateDecodingStats(ctx: &mut LogicalDecodingContext) {
    let slot = ctx.slot;
    update_decoding_stats_rb(&mut ctx.reorder, slot);
}

// The reorderbuffer's in-serialize stats flush (C reaches UpdateDecodingStats
// through rb->private_data, reorderbuffer.c:4042; the crate boundary makes
// that a hook installed by StartupDecodingContext).
fn update_decoding_stats_hook(rb: &mut ReorderBuffer) {
    let slot = opc_from_rb(rb).slot;
    update_decoding_stats_rb(rb, slot);
}

fn update_decoding_stats_rb(rb: &mut ReorderBuffer, slot: &'static ReplicationSlot) {
    if rb.spillBytes <= 0 && rb.streamBytes <= 0 && rb.totalBytes <= 0 {
        return;
    }
    let rep = pgstat::replslot::PgStat_StatReplSlotEntry {
        spill_txns: rb.spillTxns,
        spill_count: rb.spillCount,
        spill_bytes: rb.spillBytes,
        stream_txns: rb.streamTxns,
        stream_count: rb.streamCount,
        stream_bytes: rb.streamBytes,
        total_txns: rb.totalTxns,
        total_bytes: rb.totalBytes,
        stat_reset_timestamp: 0,
    };
    pgstat::replslot::pgstat_report_replslot(slot::ReplicationSlotIndex(slot), &rep);
    rb.spillTxns = 0;
    rb.spillCount = 0;
    rb.spillBytes = 0;
    rb.streamTxns = 0;
    rb.streamCount = 0;
    rb.streamBytes = 0;
    rb.totalTxns = 0;
    rb.totalBytes = 0;
}

// has_rolreplication (miscinit.c), hosted here until miscinit's deferred half
// lands: CheckSlotPermissions needs the seam installed.
fn has_rolreplication(roleid: types_core::Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    const Anum_pg_authid_rolreplication: i32 = 8;
    match cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::AUTHOID,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(roleid)),
    )? {
        Some(tuple) => {
            let result = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::AUTHOID,
                &tuple,
                Anum_pg_authid_rolreplication,
            )?
            .as_bool();
            cache_syscache::ReleaseSysCache(tuple);
            Ok(result)
        }
        None => Ok(false),
    }
}

pub fn init_seams() {
    logical_seams::reset_logical_streaming_state::set(ResetLogicalStreamingState);
    snapbuild::logical_hooks::logical_increase_xmin_for_slot::set(LogicalIncreaseXminForSlot);
    snapbuild::logical_hooks::logical_increase_restart_decoding_for_slot::set(
        LogicalIncreaseRestartDecodingForSlot,
    );
    if !miscinit_seams::has_rolreplication::is_installed() {
        miscinit_seams::has_rolreplication::set(has_rolreplication);
    }
}
