//! Port of PostgreSQL 18.3 `src/backend/replication/logical/logicalfuncs.c` —
//! the SQL-callable support functions for logical decoding.
//!
//! ## `pg_logical_emit_message_*`
//!
//! Fully ported; both bottom out on
//! [`::message::LogLogicalMessage`].
//!
//! ## `pg_logical_slot_{get,peek}[_binary]_changes`
//!
//! The shared [`run_changes_into_collector`] now decodes end-to-end. The #351
//! per-context output-writer keystone (see `backend-replication-logical-logical`)
//! gives the context an `OutputWriter::SqlSrf` writer: `OutputPluginPrepareWrite`
//! resets `ctx->out` and `OutputPluginWrite` routes here
//! ([`install_sql_srf_output_write`]), which reads the finished `ctx->out` bytes
//! and emits an `(lsn, xid, data)` row. The rows are collected in a backend-local
//! stack (the lifetime-free analog of C's `DecodingOutputState` tuplestore) that
//! the SRF entry point drains into its result tuplestore.
//!
//! The decode loop ([`run_changes_into_collector`]) mirrors the C
//! `pg_logical_slot_get_changes_guts`: `CreateDecodingContext(SqlSrf)`,
//! `XLogBeginRead`, the `XLogReadRecord` + `LogicalDecodingProcessRecord` loop
//! bounded by `upto_lsn` / `upto_nchanges`, optional
//! `LogicalConfirmReceivedLocation` (`confirm`), `FreeDecodingContext`.
//!
//! ## Remaining step (the fmgr SRF entry)
//!
//! The four SQL functions are not yet registered as executor-frame SRFs (the
//! `text[]` options array-deconstruct + `InitMaterializedSRF` /
//! `materialized_srf_putvalues` fmgr boundary). [`run_changes_into_collector`]
//! is the decode core they call; wiring the fmgr SRF entry (which acquires the
//! slot, inits the SRF tuplestore, parses the options array, calls this core,
//! and drains [`logical_seams::sql_srf_take_rows`])
//! is the final step.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use ::types_core::primitive::{TransactionId, XLogRecPtr};
use ::types_error::PgResult;
use types_logical::{LogicalDecodingContext, OutputWriter};

use ::message::LogLogicalMessage;

use transam_xlog_seams as xlog;
use xlogreader_seams as xlogreader;
use decode_seams as decode;
use logical_logical as logical;
use logical_seams as logical_seams;
use replication_slot as slot;
use postgres_seams as tcop;
use inval_seams as inval;
use mcxt_seams as mcxt;

const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

/// `DecodingOutputState` (logicalfuncs.c:40) — private state for writing the
/// decoded change stream out. `binary_output` only governs the C
/// `pg_verify_mbstr` assert (the data bytes are emitted verbatim either way).
/// Carried on `ctx.output_writer_private`.
struct DecodingOutputState {
    #[allow(dead_code)]
    binary_output: bool,
}

/// `LogicalOutputWrite` (logicalfuncs.c:61): build `(lsn, xid, text(ctx->out))`
/// and `tuplestore_putvalues` it. Installed as the `sql_srf_output_write` seam
/// (`OutputPluginWrite` routes here for an `OutputWriter::SqlSrf` context). The
/// row is pushed onto the backend-local collector (the SRF entry drains it into
/// the result tuplestore after the decode loop); the collector length is C's
/// `p->returned_rows`.
fn install_sql_srf_output_write(
    ctx: &mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
) -> PgResult<()> {
    // p = (DecodingOutputState *) ctx->output_writer_private;
    debug_assert!(
        ctx.output_writer_private.is_some(),
        "LogicalOutputWrite: ctx->output_writer_private (DecodingOutputState) not set"
    );
    // values[2] = cstring_to_text_with_len(ctx->out->data, ctx->out->len);
    let data = mcxt::store_read_string_info(ctx.out);
    // tuplestore_putvalues(p->tupstore, p->tupdesc, values, nulls);  p->returned_rows++;
    logical_seams::sql_srf_push_row(lsn, xid, data);
    Ok(())
}

/// The shared decode core of all four change-stream functions
/// (`pg_logical_slot_get_changes_guts`, logicalfuncs.c:98). `confirm` advances
/// the slot's `confirmed_flush`; `binary` selects binary vs textual output.
///
/// The fmgr arg-reading / tuplestore-init / `text[]` array-deconstruct and the
/// `ReplicationSlotAcquire`/`Release` belong to the (not-yet-wired) SRF entry,
/// which passes the already-parsed `upto_lsn` / `upto_nchanges` / `options` and
/// then drains the collected rows via
/// [`logical_seams::sql_srf_take_rows`]. The slot must already be acquired.
pub fn run_changes_into_collector(
    upto_lsn: XLogRecPtr,
    upto_nchanges: i32,
    options: alloc::vec::Vec<(alloc::string::String, Option<alloc::string::String>)>,
    confirm: bool,
    binary: bool,
    my_database_id: ::types_core::Oid,
) -> PgResult<()> {
    // state to write output to.
    let p = DecodingOutputState { binary_output: binary };

    // Compute the current end-of-wal.
    let end_of_wal = if !xlog::recovery_in_progress::call() {
        xlog::get_flush_rec_ptr::call().0
    } else {
        xlog::get_xlog_replay_rec_ptr::call()
    };

    // Clear any stale collector rows from a prior (failed) call.
    logical_seams::sql_srf_clear_rows();

    let result = (|| -> PgResult<()> {
        // The options list the plugin's startup_cb reads.
        let opts_handle = logical_seams::register_output_plugin_options(options);

        // ctx = CreateDecodingContext(InvalidXLogRecPtr, options, false, XL_ROUTINE,
        //          LogicalOutputPrepareWrite, LogicalOutputWrite, NULL); the writer
        //          is SqlSrf (#351) — prepare/write present, update_progress NULL.
        let mut ctx = logical::CreateDecodingContext(
            INVALID_XLOG_REC_PTR,
            opts_handle,
            false, /* fast_forward */
            Default::default(),
            true,  /* prepare_write present */
            true,  /* do_write present */
            false, /* update_progress NULL */
            OutputWriter::SqlSrf,
            xlog::wal_segment_size::call(),
            my_database_id,
        )?;

        // ctx->output_writer_private = p;
        ctx.output_writer_private = Some(alloc::boxed::Box::new(p));

        // Check whether the output plugin writes textual output if that's what we
        // need.
        if !binary && ctx.options.output_type != ::types_logical::OUTPUT_PLUGIN_TEXTUAL_OUTPUT
        {
            logical::FreeDecodingContext(&mut ctx)?;
            return Err(::types_error::PgError::error(
                "logical decoding output plugin produces binary output, but function \
                 expects textual data",
            )
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        // Decoding of WAL must start at restart_lsn.
        xlogreader::XLogBeginRead::call(ctx.reader, slot::my_slot_restart_lsn());

        // invalidate non-timetravel entries.
        inval::invalidate_system_caches::call()?;

        // Decode until we run out of records.
        loop {
            if xlogreader::reader_EndRecPtr::call(ctx.reader) >= end_of_wal {
                break;
            }

            let read = xlogreader::XLogReadRecord::call(ctx.reader);
            if let Some(err) = read.err {
                logical::FreeDecodingContext(&mut ctx)?;
                return Err(::types_error::PgError::error(format!(
                    "could not find record for logical decoding: {err}"
                )));
            }

            // The {begin_txn,change,commit_txn}_wrapper callbacks store the
            // description into our collector.
            if read.record {
                let reader = ctx.reader;
                decode::LogicalDecodingProcessRecord::call(&mut ctx, reader)?;
            }

            // check limits.
            if upto_lsn != INVALID_XLOG_REC_PTR
                && upto_lsn <= xlogreader::reader_EndRecPtr::call(ctx.reader)
            {
                break;
            }
            if upto_nchanges != 0 && upto_nchanges as i64 <= logical_seams::sql_srf_returned_rows()
            {
                break;
            }
            tcop::check_for_interrupts::call()?;
        }

        // Next time, start where we left off.
        let end = xlogreader::reader_EndRecPtr::call(ctx.reader);
        if end != INVALID_XLOG_REC_PTR && confirm {
            logical::LogicalConfirmReceivedLocation(end)?;
            // Dirty the slot so its new confirmed_flush is written out.
            slot::ReplicationSlotMarkDirty();
        }

        // free context, call shutdown callback.
        logical::FreeDecodingContext(&mut ctx)?;
        inval::invalidate_system_caches::call()?;
        Ok(())
    })();

    if result.is_err() {
        // PG_CATCH: clear all timetravel entries, drop the partial rows.
        let _ = inval::invalidate_system_caches::call();
        logical_seams::sql_srf_clear_rows();
    }
    result
}

mod fmgr_builtins;

/// Install this crate's owned seams.
pub fn init_seams() {
    // The `OutputWriter::SqlSrf` write callback (LogicalOutputWrite).
    logical_seams::sql_srf_output_write::set(install_sql_srf_output_write);
    // Register the `pg_logical_emit_message_{text,bytea}` fmgr builtins.
    fmgr_builtins::register_logicalfuncs_builtins();
}

// ---------------------------------------------------------------------------
// pg_logical_emit_message (unchanged — fully ported).
// ---------------------------------------------------------------------------

/// `pg_logical_emit_message_bytea` (logicalfuncs.c:367).
pub fn pg_logical_emit_message_bytea(
    transactional: bool,
    prefix: &[u8],
    data: &[u8],
    flush: bool,
) -> PgResult<XLogRecPtr> {
    LogLogicalMessage(prefix, data, data.len(), transactional, flush)
}

/// `pg_logical_emit_message_text` (logicalfuncs.c:381).
pub fn pg_logical_emit_message_text(
    transactional: bool,
    prefix: &[u8],
    data: &[u8],
    flush: bool,
) -> PgResult<XLogRecPtr> {
    pg_logical_emit_message_bytea(transactional, prefix, data, flush)
}
