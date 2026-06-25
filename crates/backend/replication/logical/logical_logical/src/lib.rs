#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_late_init)]

//! Port of `src/backend/replication/logical/logical.c` (PostgreSQL 18.3) — the
//! logical-decoding coordination layer.
//!
//! In-crate logic: the [`LogicalDecodingContext`] lifecycle and its field
//! writes; every output-plugin callback *wrapper* (their accept_writes/
//! write_xid/write_location/end_xact setup, asserts, optional-callback early
//! returns, mandatory-callback ERROR guards); the streaming/twophase capability
//! computation; the requirement and sanity checks; the candidate xmin/restart
//! state machine; and the WAL-reading loops.
//!
//! Everything below the coordination layer is another subsystem crossed through
//! that owner's seam crate (snapbuild, reorderbuffer, xlogreader, decode, the
//! slot machinery, procarray, xact, xlog, dfmgr/output-plugin load, inval,
//! mcxt, pgstat, resowner, walsender, interrupts). Each seam panics until its
//! owner lands.
//!
//! `wal_level`, `wal_segment_size`, and `MyDatabaseId` are foreign per-backend
//! globals, taken as explicit parameters (the no-ambient-global-seams rule).

use ::utils_error::{ereport, PgError, PgResult};
use ::types_error::{
    DEBUG1, DEBUG2, ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_OUT_OF_MEMORY, ERROR, LOG,
};

use ::types_core::primitive::{
    InvalidOid, Oid, RepOriginId, Size, TimestampTz, TransactionId, XLogRecPtr,
};
use ::types_core::xact::InvalidTransactionId;

use ::types_logical::{
    CallbackInvocation, ChangeHandle, GidHandle, MessageHandle,
    OutputPluginCallbackArgs, OutputPluginOptionsHandle, PrefixHandle, RelationHandle,
    RelationsHandle, ReorderBufferCallback, ReorderBufferHandle, ResourceOwnerHandle,
    SnapBuildHandle, StringInfoHandle, TxnHandle, WalLevel, XLogReaderHandle,
    XLogReaderRoutineHandle, WAL_LEVEL_LOGICAL,
};

// Outward owner-seam crates.
use transam_xlog_seams as xlog;
use xlogreader_seams as xlogreader;
use transam_xact_seams as xact;
use decode_seams as decode;
use reorderbuffer_seams as reorder;
use logical_snapbuild_seams as snapbuild;
use slot_seams as slot;
use slotsync_seams as slotsync;
use walsender_seams as walsender;
use procarray_seams as procarray;
use postgres_seams as tcop;
use inval_seams as inval;
use dfmgr_seams as dfmgr;
use mcxt_seams as mcxt;
use resowner_seams as resowner;

/// `InvalidXLogRecPtr` (`xlogdefs.h`).
const InvalidXLogRecPtr: XLogRecPtr = 0;

/// `SnapBuildState` (`replication/snapbuild.h`).
pub type SnapBuildState = i32;
/// `SNAPBUILD_START = -1`.
pub const SNAPBUILD_START: SnapBuildState = -1;
/// `SNAPBUILD_BUILDING_SNAPSHOT = 0`.
pub const SNAPBUILD_BUILDING_SNAPSHOT: SnapBuildState = 0;
/// `SNAPBUILD_FULL_SNAPSHOT = 1`.
pub const SNAPBUILD_FULL_SNAPSHOT: SnapBuildState = 1;
/// `SNAPBUILD_CONSISTENT = 2`.
pub const SNAPBUILD_CONSISTENT: SnapBuildState = 2;

/* =========================================================================
 * Data structures (output_plugin.h, logical.h)
 * ========================================================================= */

// The output-plugin descriptor structs and the canonical
// `LogicalDecodingContext` now live in `types-logical` (the shared
// seam-boundary types crate) so that `decode.c`'s rmgr handlers and
// `types-wal`'s `RmDecode` callback type can name the *same* rich context as
// this owner. Re-exported here to preserve `logical.c`'s public surface.
pub use ::types_logical::{
    LogicalDecodingContext, OutputPluginCallbacks, OutputPluginOptions, OutputPluginOutputType,
    OUTPUT_PLUGIN_BINARY_OUTPUT, OUTPUT_PLUGIN_TEXTUAL_OUTPUT,
};

// The `rb->private_data` bridge (the per-backend parked-ctx thread-local and
// `with_parked_decoding_ctx`) lives in the seams crate so that both decode.c
// (which parks the ctx around the reorder buffer entry points) and this owner
// (whose dispatch seam reads it) can reach it without a dependency cycle.
pub use ::logical_seams::with_parked_decoding_ctx;

/// `LogicalErrorCallbackState` (logical.c:50) — data for the errcontext
/// callback.
#[derive(Clone, Copy, Debug)]
pub struct LogicalErrorCallbackState {
    /// `const char *callback_name`.
    pub callback_name: &'static str,
    /// `XLogRecPtr report_location`.
    pub report_location: XLogRecPtr,
}

/* =========================================================================
 * Inline helpers from headers used verbatim by logical.c
 * ========================================================================= */

/// `XLogRecPtrIsInvalid(r)` — `(r) == InvalidXLogRecPtr` (`xlogdefs.h`).
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `TransactionIdIsValid(xid)` — `(xid) != InvalidTransactionId` (`transam.h`).
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdPrecedesOrEquals(id1, id2)` — modular-arithmetic `<=` on xids
/// (`transam.c`).
#[inline]
fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;
    if id1 < FIRST_NORMAL_TRANSACTION_ID || id2 < FIRST_NORMAL_TRANSACTION_ID {
        return id1 <= id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff <= 0
}

/// `SlotIsPhysical(slot)` — `(slot)->data.database == InvalidOid` (`slot.h`).
#[inline]
fn SlotIsPhysical() -> bool {
    slot::slot_is_physical::call()
}

/// `LSN_FORMAT_ARGS(lsn)` rendered into the `%X/%X` string (`xlogdefs.h`).
#[inline]
fn lsn_str(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/* =========================================================================
 * logical.c body
 * ========================================================================= */

/// `CheckLogicalDecodingRequirements` (logical.c:111).
pub fn CheckLogicalDecodingRequirements(wal_level: WalLevel, my_database_id: Oid) -> PgResult<()> {
    slot::check_slot_requirements::call(wal_level.0)?;

    /*
     * NB: Adding a new requirement likely means that RestoreSlotFromDisk()
     * needs the same check.
     */

    if wal_level < WAL_LEVEL_LOGICAL {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("logical decoding requires \"wal_level\" >= \"logical\"")
            .into_error());
    }

    if my_database_id == InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("logical decoding requires a database connection")
            .into_error());
    }

    if xlog::recovery_in_progress::call() {
        /*
         * This check may have race conditions, but whenever
         * XLOG_PARAMETER_CHANGE indicates that wal_level has changed, we
         * verify that there are no existing logical replication slots.
         */
        if xlog::GetActiveWalLevelOnStandby::call() < WAL_LEVEL_LOGICAL {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary")
                .into_error());
        }
    }

    Ok(())
}

/// `StartupDecodingContext` (logical.c:151).
#[allow(clippy::too_many_arguments)]
fn StartupDecodingContext(
    output_plugin_options: OutputPluginOptionsHandle,
    start_lsn: XLogRecPtr,
    xmin_horizon: TransactionId,
    need_full_snapshot: bool,
    fast_forward: bool,
    in_create: bool,
    xl_routine: XLogReaderRoutineHandle,
    prepare_write: bool,
    do_write: bool,
    update_progress: bool,
    writer: ::types_logical::OutputWriter,
    wal_segment_size: i32,
) -> PgResult<Box<LogicalDecodingContext>> {
    let _slot = slot::my_replication_slot_is_set::call();

    let context = mcxt::create_logical_decoding_context_memcxt::call();
    let old_context = mcxt::MemoryContextSwitchTo::call(context);

    // palloc0(sizeof(LogicalDecodingContext))
    let mut ctx = Box::new(LogicalDecodingContext {
        context,
        slot: false,
        slot_database: InvalidOid,
        reader: XLogReaderHandle::default(),
        reorder: ReorderBufferHandle::default(),
        snapshot_builder: SnapBuildHandle::default(),
        fast_forward: false,
        callbacks: OutputPluginCallbacks::default(),
        options: OutputPluginOptions::default(),
        output_plugin_options: OutputPluginOptionsHandle::default(),
        prepare_write: false,
        write: false,
        update_progress: false,
        out: StringInfoHandle::default(),
        streaming: false,
        twophase: false,
        twophase_opt_given: false,
        accept_writes: false,
        prepared_write: false,
        write_location: 0,
        write_xid: 0,
        end_xact: false,
        output_plugin_private: None,
        processing_required: false,
        builtin_plugin: None,
        writer: ::types_logical::OutputWriter::None,
        output_writer_private: None,
    });

    ctx.context = context;

    /*
     * (re-)load output plugins, so we detect a bad (removed) output plugin
     * now.
     */
    if !fast_forward {
        let plugin = slot::slot_plugin::call();
        ctx.callbacks = LoadOutputPlugin(&plugin)?;
        // In C `ctx->callbacks` holds the plugin's real function pointers; here
        // the loadable plugin's C body is ported in-process as a registered
        // BUILTIN, keyed by name. Record that key so the per-change dispatch can
        // reach the builtin vtable WITH the live `&mut ctx`.
        ctx.builtin_plugin = dfmgr::builtin_output_plugin_name::call(&plugin);
    }

    /*
     * Now that the slot's xmin has been set, we can announce ourselves as a
     * logical decoding backend which doesn't need to be checked individually
     * when computing the xmin horizon because the xmin is enforced via
     * replication slots.
     *
     * We can only do so if we're outside of a transaction (i.e. the case when
     * streaming changes via walsender), otherwise an already setup
     * snapshot/xid would end up being ignored.
     */
    if !xact::is_transaction_or_transaction_block::call() {
        procarray_lock_exclusive();
        procarray::mark_proc_in_logical_decoding::call();
        procarray::ProcArrayLock_release::call();
    }

    ctx.slot = true;
    // `ctx->slot = MyReplicationSlot`; decode.c reads `ctx->slot->data.database`
    // to filter changes to the slot's database, carried here as `slot_database`.
    ctx.slot_database = slot::slot_database::call();

    ctx.reader = match xlogreader::XLogReaderAllocate::call(wal_segment_size, xl_routine) {
        Some(reader) => reader,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OUT_OF_MEMORY)
                .errmsg("out of memory")
                .errdetail("Failed while allocating a WAL reading processor.")
                .into_error());
        }
    };

    ctx.reorder = reorder::ReorderBufferAllocate::call();
    ctx.snapshot_builder = snapbuild::AllocateSnapshotBuilder::call(
        ctx.reorder,
        xmin_horizon,
        start_lsn,
        need_full_snapshot,
        in_create,
        slot::slot_two_phase_at::call(),
    );

    // ctx->reorder->private_data = ctx; plus all the wrapper callback pointer
    // stores (begin/apply_change/apply_truncate/commit/message, the eight
    // streaming callbacks, the four two-phase callbacks, update_progress_txn).
    reorder::wire_reorderbuffer_callbacks::call(ctx.reorder);

    /*
     * To support streaming, we require start/stop/abort/commit/change
     * callbacks. The message and truncate callbacks are optional. We enable
     * streaming when at least one of the methods is enabled so that we can
     * easily identify missing methods. We decide it here, but only check it
     * later in the wrappers.
     */
    ctx.streaming = ctx.callbacks.stream_start_cb
        || ctx.callbacks.stream_stop_cb
        || ctx.callbacks.stream_abort_cb
        || ctx.callbacks.stream_commit_cb
        || ctx.callbacks.stream_change_cb
        || ctx.callbacks.stream_message_cb
        || ctx.callbacks.stream_truncate_cb;

    /*
     * To support two-phase logical decoding, we require
     * begin_prepare/prepare/commit-prepare/abort-prepare callbacks. The
     * filter_prepare callback is optional.
     */
    ctx.twophase = ctx.callbacks.begin_prepare_cb
        || ctx.callbacks.prepare_cb
        || ctx.callbacks.commit_prepared_cb
        || ctx.callbacks.rollback_prepared_cb
        || ctx.callbacks.stream_prepare_cb
        || ctx.callbacks.filter_prepare_cb;

    ctx.out = mcxt::makeStringInfo::call();
    ctx.prepare_write = prepare_write;
    ctx.write = do_write;
    ctx.update_progress = update_progress;
    // #351: the per-context output-writer target. In C the three function
    // pointers above are the writer; here they collapse to this enum (the bools
    // remain as the presence flags `OutputPluginUpdateProgress` reads).
    ctx.writer = writer;

    ctx.output_plugin_options = output_plugin_options;

    ctx.fast_forward = fast_forward;

    mcxt::MemoryContextSwitchTo::call(old_context);

    Ok(ctx)
}

/// `CreateInitDecodingContext` (logical.c:331).
#[allow(unused_assignments)]
#[allow(clippy::too_many_arguments)]
pub fn CreateInitDecodingContext(
    plugin: Option<&str>,
    output_plugin_options: OutputPluginOptionsHandle,
    need_full_snapshot: bool,
    restart_lsn: XLogRecPtr,
    xl_routine: XLogReaderRoutineHandle,
    prepare_write: bool,
    do_write: bool,
    update_progress: bool,
    writer: ::types_logical::OutputWriter,
    wal_level: WalLevel,
    wal_segment_size: i32,
    my_database_id: Oid,
) -> PgResult<Box<LogicalDecodingContext>> {
    let mut xmin_horizon: TransactionId = InvalidTransactionId;

    /*
     * On a standby, this check is also required while creating the slot.
     */
    CheckLogicalDecodingRequirements(wal_level, my_database_id)?;

    let slot_set = slot::my_replication_slot_is_set::call();

    /* first some sanity checks that are unlikely to be violated */
    if !slot_set {
        return Err(elog_error(
            "cannot perform logical decoding without an acquired slot",
        ));
    }

    if plugin.is_none() {
        return Err(elog_error(
            "cannot initialize logical decoding without a specified plugin",
        ));
    }
    let plugin = plugin.unwrap();

    /* Make sure the passed slot is suitable. These are user facing errors. */
    if SlotIsPhysical() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot use physical replication slot for logical decoding")
            .into_error());
    }

    if slot::slot_database::call() != my_database_id {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "replication slot \"{}\" was not created in this database",
                slot::slot_name::call()
            ))
            .into_error());
    }

    if xact::is_transaction_state::call()
        && xact::get_top_transaction_id_if_any::call() != InvalidTransactionId
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(
                "cannot create logical replication slot in transaction that has performed writes",
            )
            .into_error());
    }

    /*
     * Register output plugin name with slot. We need the mutex to avoid
     * concurrent reading of a partially copied string.
     */
    slot::slot_mutex_acquire::call();
    slot::slot_set_plugin::call(plugin.to_string());
    slot::slot_mutex_release::call();

    if XLogRecPtrIsInvalid(restart_lsn) {
        slot::replication_slot_reserve_wal::call()?;
    } else {
        slot::slot_mutex_acquire::call();
        slot::slot_set_restart_lsn::call(restart_lsn);
        slot::slot_mutex_release::call();
    }

    /* ----
     * We need to determine a safe xmin horizon to start decoding from, to
     * avoid starting from a running xacts record referring to xids whose rows
     * have been vacuumed or pruned already.
     *
     * So we acquire both the ReplicationSlotControlLock and the ProcArrayLock,
     * get the safe decoding xid, and inform the slot machinery about the new
     * limit. Once that's done both locks can be released.
     * ----
     */
    slot::replication_slot_control_lock_acquire_exclusive::call();
    procarray_lock_exclusive();

    xmin_horizon = procarray::GetOldestSafeDecodingTransactionId::call(!need_full_snapshot);

    slot::slot_mutex_acquire::call();
    slot::slot_set_effective_catalog_xmin::call(xmin_horizon);
    slot::slot_set_catalog_xmin::call(xmin_horizon);
    if need_full_snapshot {
        slot::slot_set_effective_xmin::call(xmin_horizon);
    }
    slot::slot_mutex_release::call();

    slot::replication_slots_compute_required_xmin::call(true)?;

    procarray::ProcArrayLock_release::call();
    slot::replication_slot_control_lock_release::call();

    slot::replication_slot_mark_dirty::call();
    slot::replication_slot_save::call()?;

    let mut ctx = StartupDecodingContext(
        OutputPluginOptionsHandle::default(),
        restart_lsn,
        xmin_horizon,
        need_full_snapshot,
        false,
        true,
        xl_routine,
        prepare_write,
        do_write,
        update_progress,
        writer,
        wal_segment_size,
    )?;

    let _ = output_plugin_options; // CreateInitDecodingContext passes NIL above.

    /* call output plugin initialization callback */
    let old_context = mcxt::MemoryContextSwitchTo::call(ctx.context);
    if ctx.callbacks.startup_cb {
        startup_cb_wrapper(&mut ctx, true)?;
    }
    mcxt::MemoryContextSwitchTo::call(old_context);

    /*
     * We allow decoding of prepared transactions when the two_phase is enabled
     * at the time of slot creation, or when the two_phase option is given at
     * the streaming start, provided the plugin supports all the callbacks for
     * two-phase.
     */
    ctx.twophase &= slot::slot_two_phase::call();

    reorder::set_output_rewrites::call(ctx.reorder, ctx.options.receive_rewrites);

    Ok(ctx)
}

/// `CreateDecodingContext` (logical.c:499).
#[allow(clippy::too_many_arguments)]
pub fn CreateDecodingContext(
    mut start_lsn: XLogRecPtr,
    output_plugin_options: OutputPluginOptionsHandle,
    fast_forward: bool,
    xl_routine: XLogReaderRoutineHandle,
    prepare_write: bool,
    do_write: bool,
    update_progress: bool,
    writer: ::types_logical::OutputWriter,
    wal_segment_size: i32,
    my_database_id: Oid,
) -> PgResult<Box<LogicalDecodingContext>> {
    let slot_set = slot::my_replication_slot_is_set::call();

    /* first some sanity checks that are unlikely to be violated */
    if !slot_set {
        return Err(elog_error(
            "cannot perform logical decoding without an acquired slot",
        ));
    }

    /* make sure the passed slot is suitable, these are user facing errors */
    if SlotIsPhysical() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot use physical replication slot for logical decoding")
            .into_error());
    }

    /*
     * We need to access the system tables during decoding to build the logical
     * changes unless we are in fast_forward mode where no changes are
     * generated.
     */
    if slot::slot_database::call() != my_database_id && !fast_forward {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "replication slot \"{}\" was not created in this database",
                slot::slot_name::call()
            ))
            .into_error());
    }

    /*
     * The slots being synced from the primary can't be used for decoding as
     * they are used after failover. However, we do allow advancing the LSNs
     * during the synchronization of slots.
     */
    if xlog::recovery_in_progress::call()
        && slot::slot_synced::call()
        && !slotsync::is_syncing_replication_slots::call()
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot use replication slot \"{}\" for logical decoding",
                slot::slot_name::call()
            ))
            .errdetail("This replication slot is being synchronized from the primary server.")
            .errhint("Specify another replication slot.")
            .into_error());
    }

    /* slot must be valid to allow decoding */
    debug_assert_eq!(
        slot::slot_invalidated::call(),
        replication_slot_2::ReplicationSlotInvalidationCause::RS_INVAL_NONE
    );
    debug_assert!(slot::slot_restart_lsn::call() != InvalidXLogRecPtr);

    if start_lsn == InvalidXLogRecPtr {
        /* continue from last position */
        start_lsn = slot::slot_confirmed_flush::call();
    } else if start_lsn < slot::slot_confirmed_flush::call() {
        /*
         * It might seem like we should error out in this case, but it's pretty
         * common for a client to acknowledge a LSN it doesn't have to do
         * anything for, and thus didn't store persistently.
         */
        ::utils_error::elog(
            LOG,
            format!(
                "{} has been already streamed, forwarding to {}",
                lsn_str(start_lsn),
                lsn_str(slot::slot_confirmed_flush::call())
            ),
        )?;

        start_lsn = slot::slot_confirmed_flush::call();
    }

    let mut ctx = StartupDecodingContext(
        output_plugin_options,
        start_lsn,
        InvalidTransactionId,
        false,
        fast_forward,
        false,
        xl_routine,
        prepare_write,
        do_write,
        update_progress,
        writer,
        wal_segment_size,
    )?;

    /* call output plugin initialization callback */
    let old_context = mcxt::MemoryContextSwitchTo::call(ctx.context);
    if ctx.callbacks.startup_cb {
        startup_cb_wrapper(&mut ctx, false)?;
    }
    mcxt::MemoryContextSwitchTo::call(old_context);

    /*
     * We allow decoding of prepared transactions when the two_phase is enabled
     * at the time of slot creation, or when the two_phase option is given at
     * the streaming start, provided the plugin supports all the callbacks for
     * two-phase.
     */
    ctx.twophase &= slot::slot_two_phase::call() || ctx.twophase_opt_given;

    /* Mark slot to allow two_phase decoding if not already marked */
    if ctx.twophase && !slot::slot_two_phase::call() {
        slot::slot_mutex_acquire::call();
        slot::slot_set_two_phase::call(true);
        slot::slot_set_two_phase_at::call(start_lsn);
        slot::slot_mutex_release::call();
        slot::replication_slot_mark_dirty::call();
        slot::replication_slot_save::call()?;
        snapbuild::SnapBuildSetTwoPhaseAt::call(ctx.snapshot_builder, start_lsn);
    }

    reorder::set_output_rewrites::call(ctx.reorder, ctx.options.receive_rewrites);

    ereport(LOG)
        .errmsg(format!(
            "starting logical decoding for slot \"{}\"",
            slot::slot_name::call()
        ))
        .errdetail(format!(
            "Streaming transactions committing after {}, reading WAL from {}.",
            lsn_str(slot::slot_confirmed_flush::call()),
            lsn_str(slot::slot_restart_lsn::call())
        ))
        .finish(error_location())?;

    Ok(ctx)
}

/// `DecodingContextReady` (logical.c:625).
pub fn DecodingContextReady(ctx: &LogicalDecodingContext) -> bool {
    snapbuild::SnapBuildCurrentState::call(ctx.snapshot_builder) == SNAPBUILD_CONSISTENT
}

/// `DecodingContextFindStartpoint` (logical.c:635).
pub fn DecodingContextFindStartpoint(ctx: &mut LogicalDecodingContext) -> PgResult<()> {
    /* Initialize from where to start reading WAL. */
    xlogreader::XLogBeginRead::call(ctx.reader, slot::slot_restart_lsn::call());

    ::utils_error::elog(
        DEBUG1,
        format!(
            "searching for logical decoding starting point, starting at {}",
            lsn_str(slot::slot_restart_lsn::call())
        ),
    )?;

    /* Wait for a consistent starting point */
    loop {
        /* the read_page callback waits for new WAL */
        let read = xlogreader::XLogReadRecord::call(ctx.reader);
        if let Some(err) = read.err {
            return Err(elog_error(format!(
                "could not find logical decoding starting point: {err}"
            )));
        }
        if !read.record {
            return Err(elog_error(
                "could not find logical decoding starting point",
            ));
        }

        let reader = ctx.reader;
        decode::LogicalDecodingProcessRecord::call(ctx, reader)?;

        /* only continue till we found a consistent spot */
        if DecodingContextReady(ctx) {
            break;
        }

        tcop::check_for_interrupts::call()?;
    }

    slot::slot_mutex_acquire::call();
    let end = xlogreader::reader_EndRecPtr::call(ctx.reader);
    slot::slot_set_confirmed_flush::call(end);
    if slot::slot_two_phase::call() {
        slot::slot_set_two_phase_at::call(end);
    }
    slot::slot_mutex_release::call();

    Ok(())
}

/// `FreeDecodingContext` (logical.c:679).
pub fn FreeDecodingContext(ctx: &mut LogicalDecodingContext) -> PgResult<()> {
    if ctx.callbacks.shutdown_cb {
        shutdown_cb_wrapper(ctx)?;
    }

    reorder::ReorderBufferFree::call(ctx.reorder);
    snapbuild::FreeSnapshotBuilder::call(ctx.snapshot_builder);
    xlogreader::XLogReaderFree::call(ctx.reader);
    mcxt::MemoryContextDelete::call(ctx.context);

    Ok(())
}

/// `OutputPluginPrepareWrite` (logical.c:694).
///
/// In C this calls `ctx->prepare_write(ctx, write_location, write_xid,
/// last_write)`, the per-context writer's prepare hook. #351 collapses the
/// function pointer to the `ctx.writer` enum: the walsender path keeps the
/// global `walsender::call_prepare_write` seam; the SQL-function path's
/// `LogicalOutputPrepareWrite` is just `resetStringInfo(ctx->out)`.
pub fn OutputPluginPrepareWrite(ctx: &mut LogicalDecodingContext, last_write: bool) -> PgResult<()> {
    if !ctx.accept_writes {
        return Err(elog_error(
            "writes are only accepted in commit, begin and change callbacks",
        ));
    }

    match ctx.writer {
        ::types_logical::OutputWriter::WalSnd => {
            walsender::call_prepare_write::call(ctx.write_location, ctx.write_xid, last_write)?;
        }
        ::types_logical::OutputWriter::SqlSrf => {
            // LogicalOutputPrepareWrite: resetStringInfo(ctx->out).
            mcxt::store_reset_string_info(ctx.out);
        }
        ::types_logical::OutputWriter::None => {
            return Err(elog_error("no output writer is installed on this context"));
        }
    }
    ctx.prepared_write = true;
    Ok(())
}

/// `OutputPluginWrite` (logical.c:707).
///
/// In C this calls `ctx->write(ctx, write_location, write_xid, last_write)`. The
/// walsender path keeps the global `walsender::call_write` seam; the
/// SQL-function path routes to `LogicalOutputWrite` (owned by logicalfuncs),
/// which builds an `(lsn, xid, data text)` row from `ctx->out` and puts it into
/// the SRF tuplestore.
pub fn OutputPluginWrite(ctx: &mut LogicalDecodingContext, last_write: bool) -> PgResult<()> {
    if !ctx.prepared_write {
        return Err(elog_error(
            "OutputPluginPrepareWrite needs to be called before OutputPluginWrite",
        ));
    }

    match ctx.writer {
        ::types_logical::OutputWriter::WalSnd => {
            walsender::call_write::call(ctx.write_location, ctx.write_xid, last_write)?;
        }
        ::types_logical::OutputWriter::SqlSrf => {
            let (lsn, xid) = (ctx.write_location, ctx.write_xid);
            ::logical_seams::sql_srf_output_write::call(ctx, lsn, xid)?;
        }
        ::types_logical::OutputWriter::None => {
            return Err(elog_error("no output writer is installed on this context"));
        }
    }
    ctx.prepared_write = false;
    Ok(())
}

/// `OutputPluginUpdateProgress` (logical.c:720).
pub fn OutputPluginUpdateProgress(
    ctx: &mut LogicalDecodingContext,
    skipped_xact: bool,
) -> PgResult<()> {
    if !ctx.update_progress {
        return Ok(());
    }

    // Only the walsender path has an update_progress callback; the SQL-function
    // path passes NULL (`update_progress == false` keeps us out of here).
    if ctx.writer == ::types_logical::OutputWriter::WalSnd {
        walsender::call_update_progress::call(ctx.write_location, ctx.write_xid, skipped_xact)?;
    }
    Ok(())
}

/// The `(key, value)` DefElem pairs of `ctx->output_plugin_options` — the list
/// the decode caller deconstructed from the `text[]` options and forwarded to
/// the plugin's `startup_cb`. The output plugin (test_decoding / pgoutput) reads
/// it in `pg_decode_startup` to parse `include-xids`/`skip-empty-xacts`/etc.
pub fn output_plugin_options(
    ctx: &LogicalDecodingContext,
) -> Vec<(String, Option<String>)> {
    ::logical_seams::output_plugin_options_list(
        ctx.output_plugin_options,
    )
}

/// `LoadOutputPlugin` (logical.c:735). The shared-library load +
/// `plugin_init(callbacks)` happen behind the dfmgr seam (it raises the
/// "_PG_output_plugin_init symbol" error if the symbol is missing); the
/// required-callback checks are owned here.
pub fn LoadOutputPlugin(plugin: &str) -> PgResult<OutputPluginCallbacks> {
    let callbacks =
        OutputPluginCallbacks::from_bits(dfmgr::load_output_plugin::call(plugin.to_string())?);

    if !callbacks.begin_cb {
        return Err(elog_error("output plugins have to register a begin callback"));
    }
    if !callbacks.change_cb {
        return Err(elog_error(
            "output plugins have to register a change callback",
        ));
    }
    if !callbacks.commit_cb {
        return Err(elog_error(
            "output plugins have to register a commit callback",
        ));
    }

    Ok(callbacks)
}

/// `output_plugin_error_callback` (logical.c:757) — the `errcontext` callback
/// pushed around every output-plugin call. Renders the context line C
/// produces; the runtime attaches the returned string to an in-flight error.
pub fn output_plugin_error_callback(state: &LogicalErrorCallbackState) -> String {
    /* not all callbacks have an associated LSN */
    if state.report_location != InvalidXLogRecPtr {
        format!(
            "slot \"{}\", output plugin \"{}\", in the {} callback, associated LSN {}",
            slot::slot_name::call(),
            slot::slot_plugin::call(),
            state.callback_name,
            lsn_str(state.report_location)
        )
    } else {
        format!(
            "slot \"{}\", output plugin \"{}\", in the {} callback",
            slot::slot_name::call(),
            slot::slot_plugin::call(),
            state.callback_name
        )
    }
}

/// Build the [`CallbackInvocation`] handed to the runtime.
#[inline]
fn invocation(
    ctx: &LogicalDecodingContext,
    callback_name: &'static str,
    report_location: XLogRecPtr,
    args: OutputPluginCallbackArgs,
) -> CallbackInvocation {
    CallbackInvocation {
        callback_name,
        report_location,
        accept_writes: ctx.accept_writes,
        write_xid: ctx.write_xid,
        write_location: ctx.write_location,
        end_xact: ctx.end_xact,
        args,
    }
}

/// Dispatch one output-plugin callback. In C the `*_cb_wrapper` calls the
/// function pointer in `ctx->callbacks` directly with the live `ctx`. Here the
/// loadable plugin's C body is ported in-process as a registered BUILTIN
/// (`ctx.builtin_plugin` is set by `StartupDecodingContext`), so the dispatch
/// routes through the builtin seam WITH the live `&mut ctx` (the plugin writes
/// into `ctx->out` and reads/stows `ctx->output_plugin_private`). If no builtin
/// is flagged (a genuine OS `.so`, unreachable in this build), it falls back to
/// the flattened OS-loader seam.
#[inline]
fn dispatch_callback(
    ctx: &mut LogicalDecodingContext,
    callback_name: &'static str,
    report_location: XLogRecPtr,
    args: OutputPluginCallbackArgs,
) -> PgResult<bool> {
    let inv = invocation(ctx, callback_name, report_location, args);
    if ctx.builtin_plugin.is_some() {
        dfmgr::invoke_builtin_output_plugin_callback::call(ctx, &inv)
    } else {
        dfmgr::invoke_output_plugin_callback::call(inv)
    }
}

/// `startup_cb_wrapper` (logical.c:776).
fn startup_cb_wrapper(ctx: &mut LogicalDecodingContext, is_init: bool) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "startup";
    let report_location = InvalidXLogRecPtr;

    /* set output state */
    ctx.accept_writes = false;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Startup { is_init },
    )?;
    Ok(())
}

/// `shutdown_cb_wrapper` (logical.c:804).
fn shutdown_cb_wrapper(ctx: &mut LogicalDecodingContext) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "shutdown";
    let report_location = InvalidXLogRecPtr;

    ctx.accept_writes = false;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Shutdown,
    )?;
    Ok(())
}

/// `begin_cb_wrapper` (logical.c:837).
fn begin_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    first_lsn: XLogRecPtr,
    txn_xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "begin";
    let report_location = first_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = first_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Begin { txn },
    )?;
    Ok(())
}

/// `commit_cb_wrapper` (logical.c:868).
fn commit_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "commit";
    let report_location = txn_final_lsn; /* beginning of commit record */

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn; /* points to the end of the record */
    ctx.end_xact = true;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Commit { txn, commit_lsn },
    )?;
    Ok(())
}

/// `begin_prepare_cb_wrapper` (logical.c:907).
fn begin_prepare_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    first_lsn: XLogRecPtr,
    txn_xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.twophase);

    let callback_name = "begin_prepare";
    let report_location = first_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = first_lsn;
    ctx.end_xact = false;

    if !ctx.callbacks.begin_prepare_cb {
        return Err(missing_prepare_callback("begin_prepare_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::BeginPrepare { txn },
    )?;
    Ok(())
}

/// `prepare_cb_wrapper` (logical.c:951).
fn prepare_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.twophase);

    let callback_name = "prepare";
    let report_location = txn_final_lsn; /* beginning of prepare record */

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.prepare_cb {
        return Err(missing_prepare_callback("prepare_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Prepare { txn, prepare_lsn },
    )?;
    Ok(())
}

/// `commit_prepared_cb_wrapper` (logical.c:996).
fn commit_prepared_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.twophase);

    let callback_name = "commit_prepared";
    let report_location = txn_final_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.commit_prepared_cb {
        return Err(missing_prepare_callback("commit_prepared_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::CommitPrepared { txn, commit_lsn },
    )?;
    Ok(())
}

/// `rollback_prepared_cb_wrapper` (logical.c:1041).
fn rollback_prepared_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.twophase);

    let callback_name = "rollback_prepared";
    let report_location = txn_final_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.rollback_prepared_cb {
        return Err(missing_prepare_callback("rollback_prepared_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::RollbackPrepared {
            txn,
            prepare_end_lsn,
            prepare_time,
        },
    )?;
    Ok(())
}

/// `change_cb_wrapper` (logical.c:1088).
fn change_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    relation: RelationHandle,
    change: ChangeHandle,
    change_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "change";
    let report_location = change_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = change_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Change {
            txn,
            relation,
            change,
        },
    )?;
    Ok(())
}

/// `truncate_cb_wrapper` (logical.c:1127).
fn truncate_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    nrelations: i32,
    relations: RelationsHandle,
    change: ChangeHandle,
    change_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    if !ctx.callbacks.truncate_cb {
        return Ok(());
    }

    let callback_name = "truncate";
    let report_location = change_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = change_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Truncate {
            txn,
            nrelations,
            relations,
            change,
        },
    )?;
    Ok(())
}

/// `filter_prepare_cb_wrapper` (logical.c:1169).
pub fn filter_prepare_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    xid: TransactionId,
    gid: Vec<u8>,
) -> PgResult<bool> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "filter_prepare";
    let report_location = InvalidXLogRecPtr;

    ctx.accept_writes = false;
    ctx.end_xact = false;

    let ret = dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::FilterPrepare { xid, gid },
    )?;

    Ok(ret)
}

/// `filter_by_origin_cb_wrapper` (logical.c:1201).
pub fn filter_by_origin_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    origin_id: RepOriginId,
) -> PgResult<bool> {
    debug_assert!(!ctx.fast_forward);

    let callback_name = "filter_by_origin";
    let report_location = InvalidXLogRecPtr;

    ctx.accept_writes = false;
    ctx.end_xact = false;

    let ret = dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::FilterByOrigin { origin_id },
    )?;

    Ok(ret)
}

/// `message_cb_wrapper` (logical.c:1232).
fn message_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: Option<(TxnHandle, TransactionId)>,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: PrefixHandle,
    message_size: Size,
    message: MessageHandle,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    if !ctx.callbacks.message_cb {
        return Ok(());
    }

    let callback_name = "message";
    let report_location = message_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = match txn {
        Some((_, xid)) => xid,
        None => InvalidTransactionId,
    };
    ctx.write_location = message_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::Message {
            txn: txn.map(|(h, _)| h).unwrap_or_default(),
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        },
    )?;
    Ok(())
}

/// `stream_start_cb_wrapper` (logical.c:1269).
fn stream_start_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    first_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    let callback_name = "stream_start";
    let report_location = first_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = first_lsn;
    ctx.end_xact = false;

    if !ctx.callbacks.stream_start_cb {
        return Err(missing_streaming_callback("stream_start_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamStart { txn },
    )?;
    Ok(())
}

/// `stream_stop_cb_wrapper` (logical.c:1318).
fn stream_stop_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    last_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    let callback_name = "stream_stop";
    let report_location = last_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = last_lsn;
    ctx.end_xact = false;

    if !ctx.callbacks.stream_stop_cb {
        return Err(missing_streaming_callback("stream_stop_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamStop { txn },
    )?;
    Ok(())
}

/// `stream_abort_cb_wrapper` (logical.c:1367).
fn stream_abort_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    abort_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    let callback_name = "stream_abort";
    let report_location = abort_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = abort_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.stream_abort_cb {
        return Err(missing_streaming_callback("stream_abort_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamAbort { txn, abort_lsn },
    )?;
    Ok(())
}

/// `stream_prepare_cb_wrapper` (logical.c:1408).
fn stream_prepare_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);
    debug_assert!(ctx.twophase);

    let callback_name = "stream_prepare";
    let report_location = txn_final_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.stream_prepare_cb {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "logical streaming at prepare time requires a {} callback",
                "stream_prepare_cb"
            ))
            .into_error());
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamPrepare { txn, prepare_lsn },
    )?;
    Ok(())
}

/// `stream_commit_cb_wrapper` (logical.c:1453).
fn stream_commit_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    txn_final_lsn: XLogRecPtr,
    txn_end_lsn: XLogRecPtr,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    let callback_name = "stream_commit";
    let report_location = txn_final_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = txn_end_lsn;
    ctx.end_xact = true;

    if !ctx.callbacks.stream_commit_cb {
        return Err(missing_streaming_callback("stream_commit_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamCommit { txn, commit_lsn },
    )?;
    Ok(())
}

/// `stream_change_cb_wrapper` (logical.c:1494).
fn stream_change_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    relation: RelationHandle,
    change: ChangeHandle,
    change_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    let callback_name = "stream_change";
    let report_location = change_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = change_lsn;
    ctx.end_xact = false;

    if !ctx.callbacks.stream_change_cb {
        return Err(missing_streaming_callback("stream_change_cb"));
    }

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamChange {
            txn,
            relation,
            change,
        },
    )?;
    Ok(())
}

/// `stream_message_cb_wrapper` (logical.c:1543).
fn stream_message_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: Option<(TxnHandle, TransactionId)>,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: PrefixHandle,
    message_size: Size,
    message: MessageHandle,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    /* this callback is optional */
    if !ctx.callbacks.stream_message_cb {
        return Ok(());
    }

    let callback_name = "stream_message";
    let report_location = message_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = match txn {
        Some((_, xid)) => xid,
        None => InvalidTransactionId,
    };
    ctx.write_location = message_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamMessage {
            txn: txn.map(|(h, _)| h).unwrap_or_default(),
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        },
    )?;
    Ok(())
}

/// `stream_truncate_cb_wrapper` (logical.c:1584).
fn stream_truncate_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn: TxnHandle,
    txn_xid: TransactionId,
    nrelations: i32,
    relations: RelationsHandle,
    change: ChangeHandle,
    change_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);
    debug_assert!(ctx.streaming);

    /* this callback is optional */
    if !ctx.callbacks.stream_truncate_cb {
        return Ok(());
    }

    let callback_name = "stream_truncate";
    let report_location = change_lsn;

    ctx.accept_writes = true;
    ctx.write_xid = txn_xid;
    ctx.write_location = change_lsn;
    ctx.end_xact = false;

    dispatch_callback(
        ctx,
        callback_name,
        report_location,
        OutputPluginCallbackArgs::StreamTruncate {
            txn,
            nrelations,
            relations,
            change,
        },
    )?;
    Ok(())
}

/// `update_progress_txn_cb_wrapper` (logical.c:1631). The errcontext frame the
/// C pushes around the call is attached on `Err` propagation by the runtime
/// (the new model attaches context on propagation, not via an ambient stack).
fn update_progress_txn_cb_wrapper(
    ctx: &mut LogicalDecodingContext,
    txn_xid: TransactionId,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!ctx.fast_forward);

    ctx.accept_writes = false;
    ctx.write_xid = txn_xid;
    ctx.write_location = lsn;
    ctx.end_xact = false;

    OutputPluginUpdateProgress(ctx, false)?;
    Ok(())
}

/// `LogicalIncreaseXminForSlot` (logical.c:1678).
pub fn LogicalIncreaseXminForSlot(current_lsn: XLogRecPtr, xmin: TransactionId) -> PgResult<()> {
    let mut updated_xmin = false;
    let mut got_new_xmin = false;

    debug_assert!(slot::my_replication_slot_is_set::call());

    slot::slot_mutex_acquire::call();

    /*
     * don't overwrite if we already have a newer xmin.
     */
    if TransactionIdPrecedesOrEquals(xmin, slot::slot_catalog_xmin::call()) {
    }
    /*
     * If the client has already confirmed up to this lsn, we directly can mark
     * this as accepted.
     */
    else if current_lsn <= slot::slot_confirmed_flush::call() {
        slot::slot_set_candidate_catalog_xmin::call(xmin);
        slot::slot_set_candidate_xmin_lsn::call(current_lsn);

        /* our candidate can directly be used */
        updated_xmin = true;
    }
    /*
     * Only increase if the previous values have been applied, otherwise we
     * might never end up updating if the receiver acks too slowly.
     */
    else if slot::slot_candidate_xmin_lsn::call() == InvalidXLogRecPtr {
        slot::slot_set_candidate_catalog_xmin::call(xmin);
        slot::slot_set_candidate_xmin_lsn::call(current_lsn);

        got_new_xmin = true;
    }
    slot::slot_mutex_release::call();

    if got_new_xmin {
        ::utils_error::elog(
            DEBUG1,
            format!("got new catalog xmin {} at {}", xmin, lsn_str(current_lsn)),
        )?;
    }

    /* candidate already valid with the current flush position, apply */
    if updated_xmin {
        LogicalConfirmReceivedLocation(slot::slot_confirmed_flush::call())?;
    }

    Ok(())
}

/// `LogicalIncreaseRestartDecodingForSlot` (logical.c:1746).
pub fn LogicalIncreaseRestartDecodingForSlot(
    current_lsn: XLogRecPtr,
    restart_lsn: XLogRecPtr,
) -> PgResult<()> {
    let mut updated_lsn = false;

    debug_assert!(slot::my_replication_slot_is_set::call());
    debug_assert!(restart_lsn != InvalidXLogRecPtr);
    debug_assert!(current_lsn != InvalidXLogRecPtr);

    slot::slot_mutex_acquire::call();

    /* don't overwrite if have a newer restart lsn */
    if restart_lsn <= slot::slot_restart_lsn::call() {
        slot::slot_mutex_release::call();
    }
    /*
     * We might have already flushed far enough to directly accept this lsn.
     */
    else if current_lsn <= slot::slot_confirmed_flush::call() {
        slot::slot_set_candidate_restart_valid::call(current_lsn);
        slot::slot_set_candidate_restart_lsn::call(restart_lsn);
        slot::slot_mutex_release::call();

        /* our candidate can directly be used */
        updated_lsn = true;
    }
    /*
     * Only increase if the previous values have been applied.
     */
    else if slot::slot_candidate_restart_valid::call() == InvalidXLogRecPtr {
        slot::slot_set_candidate_restart_valid::call(current_lsn);
        slot::slot_set_candidate_restart_lsn::call(restart_lsn);
        slot::slot_mutex_release::call();

        ::utils_error::elog(
            DEBUG1,
            format!(
                "got new restart lsn {} at {}",
                lsn_str(restart_lsn),
                lsn_str(current_lsn)
            ),
        )?;
    } else {
        let candidate_restart_lsn = slot::slot_candidate_restart_lsn::call();
        let candidate_restart_valid = slot::slot_candidate_restart_valid::call();
        let confirmed_flush = slot::slot_confirmed_flush::call();
        slot::slot_mutex_release::call();

        ::utils_error::elog(
            DEBUG1,
            format!(
                "failed to increase restart lsn: proposed {}, after {}, current candidate {}, current after {}, flushed up to {}",
                lsn_str(restart_lsn),
                lsn_str(current_lsn),
                lsn_str(candidate_restart_lsn),
                lsn_str(candidate_restart_valid),
                lsn_str(confirmed_flush)
            ),
        )?;
    }

    /* candidates are already valid with the current flush position, apply */
    if updated_lsn {
        LogicalConfirmReceivedLocation(slot::slot_confirmed_flush::call())?;
    }

    Ok(())
}

/// `LogicalConfirmReceivedLocation` (logical.c:1822).
pub fn LogicalConfirmReceivedLocation(lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(lsn != InvalidXLogRecPtr);

    /* Do an unlocked check for candidate_lsn first. */
    if slot::slot_candidate_xmin_lsn::call() != InvalidXLogRecPtr
        || slot::slot_candidate_restart_valid::call() != InvalidXLogRecPtr
    {
        let mut updated_xmin = false;
        let mut updated_restart = false;
        let restart_lsn: XLogRecPtr;

        slot::slot_mutex_acquire::call();

        /* remember the old restart lsn */
        restart_lsn = slot::slot_restart_lsn::call();

        /*
         * Prevent moving the confirmed_flush backwards, as this could lead to
         * data duplication issues caused by replicating already replicated
         * changes.
         */
        if lsn > slot::slot_confirmed_flush::call() {
            slot::slot_set_confirmed_flush::call(lsn);
        }

        /* if we're past the location required for bumping xmin, do so */
        if slot::slot_candidate_xmin_lsn::call() != InvalidXLogRecPtr
            && slot::slot_candidate_xmin_lsn::call() <= lsn
        {
            /*
             * We have to write the changed xmin to disk *before* we change the
             * in-memory value, otherwise after a crash we wouldn't know that
             * some catalog tuples might have been removed already.
             */
            if TransactionIdIsValid(slot::slot_candidate_catalog_xmin::call())
                && slot::slot_catalog_xmin::call() != slot::slot_candidate_catalog_xmin::call()
            {
                slot::slot_set_catalog_xmin::call(slot::slot_candidate_catalog_xmin::call());
                slot::slot_set_candidate_catalog_xmin::call(InvalidTransactionId);
                slot::slot_set_candidate_xmin_lsn::call(InvalidXLogRecPtr);
                updated_xmin = true;
            }
        }

        if slot::slot_candidate_restart_valid::call() != InvalidXLogRecPtr
            && slot::slot_candidate_restart_valid::call() <= lsn
        {
            debug_assert!(slot::slot_candidate_restart_lsn::call() != InvalidXLogRecPtr);

            slot::slot_set_restart_lsn::call(slot::slot_candidate_restart_lsn::call());
            slot::slot_set_candidate_restart_lsn::call(InvalidXLogRecPtr);
            slot::slot_set_candidate_restart_valid::call(InvalidXLogRecPtr);
            updated_restart = true;
        }

        slot::slot_mutex_release::call();

        /* first write new xmin to disk, so we know what's up after a crash */
        if updated_xmin || updated_restart {
            slot::maybe_injection_point_slot_advance_segment::call(
                restart_lsn,
                slot::slot_restart_lsn::call(),
            );

            slot::replication_slot_mark_dirty::call();
            slot::replication_slot_save::call()?;
            ::utils_error::elog(
                DEBUG1,
                format!(
                    "updated xmin: {} restart: {}",
                    updated_xmin as u32, updated_restart as u32
                ),
            )?;
        }

        /*
         * Now the new xmin is safely on disk, we can let the global value
         * advance.
         */
        if updated_xmin {
            slot::slot_mutex_acquire::call();
            slot::slot_set_effective_catalog_xmin::call(slot::slot_catalog_xmin::call());
            slot::slot_mutex_release::call();

            slot::replication_slots_compute_required_xmin::call(false)?;
            slot::replication_slots_compute_required_lsn::call()?;
        }
    } else {
        slot::slot_mutex_acquire::call();

        /* Prevent moving the confirmed_flush backwards. */
        if lsn > slot::slot_confirmed_flush::call() {
            slot::slot_set_confirmed_flush::call(lsn);
        }

        slot::slot_mutex_release::call();
    }

    Ok(())
}

/// `ResetLogicalStreamingState` (logical.c:1944).
pub fn ResetLogicalStreamingState() {
    xact::set_check_xid_alive::call(InvalidTransactionId);
    xact::set_bsysscan::call(false);
}

/// `UpdateDecodingStats` (logical.c:1954).
pub fn UpdateDecodingStats(ctx: &LogicalDecodingContext) -> PgResult<()> {
    let rb = ctx.reorder;
    let s = reorder::reorderbuffer_stats::call(rb);

    /* Nothing to do if we don't have any replication stats to be sent. */
    if s.spill_bytes <= 0 && s.stream_bytes <= 0 && s.total_bytes <= 0 {
        return Ok(());
    }

    ::utils_error::elog(
        DEBUG2,
        format!(
            "UpdateDecodingStats: updating stats {:#x} {} {} {} {} {} {} {} {}",
            rb.0,
            s.spill_txns,
            s.spill_count,
            s.spill_bytes,
            s.stream_txns,
            s.stream_count,
            s.stream_bytes,
            s.total_txns,
            s.total_bytes
        ),
    )?;

    slot::pgstat_report_replslot::call(s);

    reorder::reorderbuffer_reset_stats::call(rb);

    Ok(())
}

/// `LogicalReplicationSlotHasPendingWal` (logical.c:2001).
pub fn LogicalReplicationSlotHasPendingWal(
    end_of_wal: XLogRecPtr,
    wal_segment_size: i32,
    my_database_id: Oid,
) -> PgResult<bool> {
    let mut has_pending_wal = false;

    debug_assert!(slot::my_replication_slot_is_set::call());

    // PG_TRY:
    let body = (|| -> PgResult<()> {
        /*
         * Create our decoding context in fast_forward mode, passing start_lsn
         * as InvalidXLogRecPtr, so that we start processing from the slot's
         * confirmed_flush.
         */
        let mut ctx = CreateDecodingContext(
            InvalidXLogRecPtr,
            OutputPluginOptionsHandle::default(),
            true, /* fast_forward */
            xl_routine_default(),
            false,
            false,
            false,
            ::types_logical::OutputWriter::None,
            wal_segment_size,
            my_database_id,
        )?;

        /*
         * Start reading at the slot's restart_lsn, which we know points to a
         * valid record.
         */
        xlogreader::XLogBeginRead::call(ctx.reader, slot::slot_restart_lsn::call());

        /* Invalidate non-timetravel entries */
        inval::invalidate_system_caches::call()?;

        /* Loop until the end of WAL or some changes are processed */
        while !has_pending_wal && xlogreader::reader_EndRecPtr::call(ctx.reader) < end_of_wal {
            let read = xlogreader::XLogReadRecord::call(ctx.reader);

            if let Some(errm) = read.err {
                return Err(elog_error(format!(
                    "could not find record for logical decoding: {errm}"
                )));
            }

            if read.record {
                let reader = ctx.reader;
                decode::LogicalDecodingProcessRecord::call(&mut ctx, reader)?;
            }

            // `LogicalDecodingProcessRecord` sets `ctx->processing_required` as
            // a side effect (e.g. a non-transactional message in fast_forward
            // mode); the unified ctx is mutated in place.
            has_pending_wal = ctx.processing_required;

            tcop::check_for_interrupts::call()?;
        }

        /* Clean up */
        FreeDecodingContext(&mut ctx)?;
        inval::invalidate_system_caches::call()?;

        Ok(())
    })();

    if let Err(e) = body {
        // PG_CATCH: clear all timetravel entries, then PG_RE_THROW().
        inval::invalidate_system_caches::call()?;
        return Err(e);
    }

    Ok(has_pending_wal)
}

/// `LogicalSlotAdvanceAndCheckSnapState` (logical.c:2083).
pub fn LogicalSlotAdvanceAndCheckSnapState(
    moveto: XLogRecPtr,
    found_consistent_snapshot: Option<&mut bool>,
    wal_segment_size: i32,
    my_database_id: Oid,
) -> PgResult<XLogRecPtr> {
    let old_resowner: ResourceOwnerHandle = resowner::CurrentResourceOwner::call();
    let retlsn: XLogRecPtr;

    debug_assert!(moveto != InvalidXLogRecPtr);

    // C sets `*found_consistent_snapshot = false` up front, before the PG_TRY
    // body, so the caller's out-parameter is initialized even if the body
    // throws. Mirror that by writing through the out reference before the body.
    let want_fcs = found_consistent_snapshot.is_some();
    let mut found_consistent_snapshot = found_consistent_snapshot;
    if let Some(out) = found_consistent_snapshot.as_deref_mut() {
        *out = false;
    }

    // PG_TRY:
    let body = (|| -> PgResult<XLogRecPtr> {
        /*
         * Create our decoding context in fast_forward mode, passing start_lsn
         * as InvalidXLogRecPtr, so that we start processing from my slot's
         * confirmed_flush.
         */
        let mut ctx = CreateDecodingContext(
            InvalidXLogRecPtr,
            OutputPluginOptionsHandle::default(),
            true, /* fast_forward */
            xl_routine_default(),
            false,
            false,
            false,
            ::types_logical::OutputWriter::None,
            wal_segment_size,
            my_database_id,
        )?;

        /*
         * Wait for specified streaming replication standby servers (if any) to
         * confirm receipt of WAL up to moveto lsn.
         */
        walsender::WaitForStandbyConfirmation::call(moveto)?;

        /*
         * Start reading at the slot's restart_lsn, which we know to point to a
         * valid record.
         */
        xlogreader::XLogBeginRead::call(ctx.reader, slot::slot_restart_lsn::call());

        /* invalidate non-timetravel entries */
        inval::invalidate_system_caches::call()?;

        /* Decode records until we reach the requested target */
        while xlogreader::reader_EndRecPtr::call(ctx.reader) < moveto {
            /*
             * Read records.  No changes are generated in fast_forward mode, but
             * snapbuilder/slot statuses are updated properly.
             */
            let read = xlogreader::XLogReadRecord::call(ctx.reader);
            if let Some(errm) = read.err {
                return Err(elog_error(format!(
                    "could not find record while advancing replication slot: {errm}"
                )));
            }

            /*
             * Process the record.  Storage-level changes are ignored in
             * fast_forward mode, but other modules (such as snapbuilder) might
             * still have critical updates to do.
             */
            if read.record {
                let reader = ctx.reader;
                decode::LogicalDecodingProcessRecord::call(&mut ctx, reader)?;
            }

            tcop::check_for_interrupts::call()?;
        }

        if want_fcs && DecodingContextReady(&ctx) {
            if let Some(out) = found_consistent_snapshot.as_deref_mut() {
                *out = true;
            }
        }

        /*
         * Logical decoding could have clobbered CurrentResourceOwner during
         * transaction management, so restore the executor's value.
         */
        resowner::set_CurrentResourceOwner::call(old_resowner);

        if xlogreader::reader_EndRecPtr::call(ctx.reader) != InvalidXLogRecPtr {
            LogicalConfirmReceivedLocation(moveto)?;

            /*
             * If only the confirmed_flush LSN has changed the slot won't get
             * marked as dirty by the above. SQL-interface users cannot specify
             * their own start positions, so dirty the slot so it is written
             * out at the next checkpoint.
             */
            slot::replication_slot_mark_dirty::call();
        }

        let retlsn = slot::slot_confirmed_flush::call();

        /* free context, call shutdown callback */
        FreeDecodingContext(&mut ctx)?;

        inval::invalidate_system_caches::call()?;

        Ok(retlsn)
    })();

    match body {
        Ok(v) => {
            retlsn = v;
        }
        Err(e) => {
            // PG_CATCH: clear all timetravel entries, then PG_RE_THROW().
            inval::invalidate_system_caches::call()?;
            return Err(e);
        }
    }

    Ok(retlsn)
}

/// `XL_ROUTINE(.page_read = read_local_xlog_page, .segment_open =
/// wal_segment_open, .segment_close = wal_segment_close)` — the fast-forward
/// XLogReaderRoutine used by the two slot-advance helpers. The routine lives in
/// xlogutils; here it is the default handle which the owner maps to the real
/// routine.
#[inline]
fn xl_routine_default() -> XLogReaderRoutineHandle {
    XLogReaderRoutineHandle::default()
}

/// `XL_ROUTINE(.page_read = logical_read_xlog_page, .segment_open =
/// WalSndSegmentOpen, .segment_close = wal_segment_close)` — the walsender's
/// streaming XLogReaderRoutine (the page-read waits for new WAL through
/// `WalSndWaitForWal`, processing replies/keepalives, vs the default local-xlog
/// routine's bare busy-wait). The concrete routine lives in xlogutils, keyed off
/// this non-default handle (the C `XLogReaderRoutine *` the walsender passes);
/// `xlog_reader_routine_for_handle` maps a non-zero handle to the walsnd
/// routine.
#[inline]
fn xl_routine_walsnd() -> XLogReaderRoutineHandle {
    XLogReaderRoutineHandle(1)
}

/* =========================================================================
 * Small owned helpers (not C functions)
 * ========================================================================= */

/// `elog(ERROR, msg)` (internal error): no SQLSTATE field, message recorded as
/// the message id, propagated as `Err`.
#[inline]
fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// The mandatory-two-phase-callback ERROR guard body shared by the prepare-time
/// wrappers (`logical replication at prepare time requires a <cb> callback`).
#[inline]
fn missing_prepare_callback(cb: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!(
            "logical replication at prepare time requires a {cb} callback"
        ))
        .into_error()
}

/// The mandatory-streaming-callback ERROR guard body shared by the streaming
/// wrappers (`logical streaming requires a <cb> callback`).
#[inline]
fn missing_streaming_callback(cb: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!("logical streaming requires a {cb} callback"))
        .into_error()
}

/// `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)` shorthand.
#[inline]
fn procarray_lock_exclusive() {
    procarray::ProcArrayLock_acquire_exclusive::call();
}

/// The `ErrorLocation` for `ereport`s that `finish` here (LOG-level reports).
#[inline]
fn error_location() -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation {
        filename: None,
        lineno: 0,
        funcname: None,
    }
}

/* =========================================================================
 * ReorderBuffer callback re-entry (inward seam)
 * ========================================================================= */

/// Re-enter the crate's ReorderBuffer-driven wrapper for `cb`, with `ctx ==
/// cache->private_data` (resolved by the reorderbuffer runtime, which holds the
/// live ctx). Installed as the `dispatch_reorderbuffer_callback` inward seam.
///
/// The reorderbuffer owner holds the `&mut LogicalDecodingContext` (it stored
/// the ctx as `rb->private_data`); when it lands it will pass that borrow in.
/// Until then this path is unreachable (the reorderbuffer trampolines panic),
/// so the dispatch resolves the ctx through the same owner that installs the
/// seam — modeled here by re-borrowing through the reorderbuffer's stored
/// pointer when the owner provides it.
pub fn dispatch_reorderbuffer_callback(
    ctx: &mut LogicalDecodingContext,
    cb: ReorderBufferCallback,
) -> PgResult<()> {
    match cb {
        ReorderBufferCallback::Begin {
            txn,
            txn_first_lsn,
            txn_xid,
        } => begin_cb_wrapper(ctx, txn, txn_first_lsn, txn_xid),
        ReorderBufferCallback::ApplyChange {
            txn,
            txn_xid,
            relation,
            change,
            change_lsn,
        } => change_cb_wrapper(ctx, txn, txn_xid, relation, change, change_lsn),
        ReorderBufferCallback::ApplyTruncate {
            txn,
            txn_xid,
            nrelations,
            relations,
            change,
            change_lsn,
        } => truncate_cb_wrapper(ctx, txn, txn_xid, nrelations, relations, change, change_lsn),
        ReorderBufferCallback::Commit {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            commit_lsn,
        } => commit_cb_wrapper(ctx, txn, txn_xid, txn_final_lsn, txn_end_lsn, commit_lsn),
        ReorderBufferCallback::Message {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => message_cb_wrapper(
            ctx,
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        ),
        ReorderBufferCallback::BeginPrepare {
            txn,
            txn_first_lsn,
            txn_xid,
        } => begin_prepare_cb_wrapper(ctx, txn, txn_first_lsn, txn_xid),
        ReorderBufferCallback::Prepare {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            prepare_lsn,
        } => prepare_cb_wrapper(ctx, txn, txn_xid, txn_final_lsn, txn_end_lsn, prepare_lsn),
        ReorderBufferCallback::CommitPrepared {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            commit_lsn,
        } => commit_prepared_cb_wrapper(ctx, txn, txn_xid, txn_final_lsn, txn_end_lsn, commit_lsn),
        ReorderBufferCallback::RollbackPrepared {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            prepare_end_lsn,
            prepare_time,
        } => rollback_prepared_cb_wrapper(
            ctx,
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            prepare_end_lsn,
            prepare_time,
        ),
        ReorderBufferCallback::StreamStart {
            txn,
            txn_xid,
            first_lsn,
        } => stream_start_cb_wrapper(ctx, txn, txn_xid, first_lsn),
        ReorderBufferCallback::StreamStop {
            txn,
            txn_xid,
            last_lsn,
        } => stream_stop_cb_wrapper(ctx, txn, txn_xid, last_lsn),
        ReorderBufferCallback::StreamAbort {
            txn,
            txn_xid,
            abort_lsn,
        } => stream_abort_cb_wrapper(ctx, txn, txn_xid, abort_lsn),
        ReorderBufferCallback::StreamPrepare {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            prepare_lsn,
        } => stream_prepare_cb_wrapper(ctx, txn, txn_xid, txn_final_lsn, txn_end_lsn, prepare_lsn),
        ReorderBufferCallback::StreamCommit {
            txn,
            txn_xid,
            txn_final_lsn,
            txn_end_lsn,
            commit_lsn,
        } => stream_commit_cb_wrapper(ctx, txn, txn_xid, txn_final_lsn, txn_end_lsn, commit_lsn),
        ReorderBufferCallback::StreamChange {
            txn,
            txn_xid,
            relation,
            change,
            change_lsn,
        } => stream_change_cb_wrapper(ctx, txn, txn_xid, relation, change, change_lsn),
        ReorderBufferCallback::StreamMessage {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => stream_message_cb_wrapper(
            ctx,
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        ),
        ReorderBufferCallback::StreamTruncate {
            txn,
            txn_xid,
            nrelations,
            relations,
            change,
            change_lsn,
        } => stream_truncate_cb_wrapper(ctx, txn, txn_xid, nrelations, relations, change, change_lsn),
        ReorderBufferCallback::UpdateProgressTxn { txn_xid, lsn } => {
            update_progress_txn_cb_wrapper(ctx, txn_xid, lsn)
        }
    }
}

/// Install this crate's inward seams.
pub fn init_seams() {
    ::logical_seams::reset_logical_streaming_state::set(
        ResetLogicalStreamingState,
    );
    // The inward seam carries only the callback variant; the live ctx
    // (rb->private_data) is resolved from the per-backend thread-local parked by
    // `with_parked_decoding_ctx` around the reorder buffer entry point that is
    // driving us (decode.c's ReorderBufferCommit / Prepare / FinishPrepared / …
    // call sites). This is the faithful single-threaded analog of C's
    // `rb->private_data` back-pointer.
    ::logical_seams::dispatch_reorderbuffer_callback::set(
        dispatch_reorderbuffer_callback_seam,
    );
    ::logical_seams::logical_confirm_received_location::set(LogicalConfirmReceivedLocation);
    ::logical_seams::logical_slot_advance_and_check_snap_state::set(
        |moveto, found_consistent_snapshot, wal_segment_size, my_database_id| {
            LogicalSlotAdvanceAndCheckSnapState(
                moveto,
                found_consistent_snapshot,
                wal_segment_size,
                my_database_id,
            )
        },
    );
    ::logical_seams::logical_increase_xmin_for_slot::set(
        LogicalIncreaseXminForSlot,
    );
    ::logical_seams::logical_increase_restart_decoding_for_slot::set(
        LogicalIncreaseRestartDecodingForSlot,
    );
    // decode.c-facing output-plugin filter wrappers + stats reporter.
    ::logical_seams::filter_prepare_cb_wrapper::set(
        filter_prepare_cb_wrapper,
    );
    ::logical_seams::filter_by_origin_cb_wrapper::set(
        filter_by_origin_cb_wrapper,
    );
    ::logical_seams::UpdateDecodingStats::set(|ctx| {
        UpdateDecodingStats(ctx)
    });

    // The walsender-facing decoding-context lifecycle seams (the walsender owns
    // the returned ctx for its streaming loop, mirroring C's file-static
    // `logical_decoding_ctx`). These avoid a walsender -> logical.c dependency
    // cycle (logical.c depends on walsender_seams for the write callbacks).
    ::logical_seams::check_logical_decoding_requirements::set(|wal_level, db| {
        CheckLogicalDecodingRequirements(wal_level, db)
    });
    ::logical_seams::create_decoding_context_walsnd::set(
        create_decoding_context_walsnd_seam,
    );
    ::logical_seams::free_decoding_context::set(|ctx| FreeDecodingContext(ctx));
}

/// Seam body for [`logical_seams::create_decoding_context_walsnd`]. Registers the
/// walsender's plugin options, then `CreateDecodingContext(cmd->startpoint,
/// cmd->options, false, XL_ROUTINE(.page_read = logical_read_xlog_page, ...),
/// WalSndPrepareWrite, WalSndWriteData, WalSndUpdateProgress)` (walsender.c's
/// `StartLogicalReplication`). The three writer function pointers collapse to the
/// `OutputWriter::WalSnd` enum (#351); `prepare_write`/`do_write`/
/// `update_progress` are all present on the walsender path.
fn create_decoding_context_walsnd_seam(
    start_lsn: XLogRecPtr,
    options: Vec<(String, Option<String>)>,
    wal_segment_size: i32,
    my_database_id: Oid,
) -> PgResult<Box<LogicalDecodingContext>> {
    let opts_handle = ::logical_seams::register_output_plugin_options(options);
    CreateDecodingContext(
        start_lsn,
        opts_handle,
        false, /* fast_forward */
        xl_routine_walsnd(),
        true,  /* prepare_write present (WalSndPrepareWrite) */
        true,  /* do_write present (WalSndWriteData) */
        true,  /* update_progress present (WalSndUpdateProgress) */
        ::types_logical::OutputWriter::WalSnd,
        wal_segment_size,
        my_database_id,
    )
}

/// Seam thunk: the inward seam carries only the callback; we resolve
/// `rb->private_data` to the live ctx parked by [`with_parked_decoding_ctx`]
/// around the reorder buffer entry point that is driving us, then run the
/// selected `*_cb_wrapper`. A null parked pointer means the reorder buffer was
/// driven outside a decode-context scope, which is a programming error (the C
/// `rb->private_data` would be NULL).
fn dispatch_reorderbuffer_callback_seam(cb: ReorderBufferCallback) -> PgResult<()> {
    // SAFETY: the parked pointer comes from a live `&mut LogicalDecodingContext`
    // whose borrow outlives this call (the parking guard sits on the same stack
    // frame that drives the reorder buffer; decoding is single-threaded
    // per-backend). No other `&mut` to the ctx is live across the seam: the
    // reorder buffer entry point borrows `ctx.reorder` (the handle) by value to
    // call the seam and does not retain a `&mut ctx`.
    ::logical_seams::with_current_decoding_ctx(|ctx| {
        dispatch_reorderbuffer_callback(ctx, cb)
    })
}
