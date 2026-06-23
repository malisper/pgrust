//! Port of `src/backend/commands/explain_dr.c` (PostgreSQL 18.3) — the
//! `DestReceiver` used by `EXPLAIN (SERIALIZE)` to measure serialization
//! overhead.
//!
//! A `SerializeDestReceiver` serializes the query's result rows into DataRow
//! messages (exactly as `printtup()` in `printtup.c` does) while measuring the
//! resources expended and the total serialized byte count, and *never* actually
//! sending the data to the client. This lets `EXPLAIN (SERIALIZE)` measure the
//! cost of deTOASTing and the datatype out/send functions, which are not
//! otherwise exercisable without hitting the network.
//!
//! The full translation unit is ported here: `serialize_prepare_info`, the four
//! `DestReceiver` callbacks (`serializeAnalyzeReceive` / `serializeAnalyzeStartup`
//! / `serializeAnalyzeShutdown` / `serializeAnalyzeDestroy`),
//! `CreateExplainSerializeDestReceiver` and `GetSerializationMetrics`.
//!
//! The DataRow message bytes are built in this crate with the ported
//! `backend-libpq-pqformat` send-side primitives over a [`StringInfo`] charged to
//! the threaded `Mcx`, exactly as the C code uses `pqformat.h`. Matching the C
//! comment, `pq_endmessage_reuse()` is **never** called, so the data is *not*
//! sent; we only count `buf.len()` into `metrics.bytesSent`.
//!
//! The per-column descriptor (`TupleDesc`) is the reused owned
//! [`types_tuple::heaptuple::TupleDescData`], passed by reference, so
//! `TupleDescAttr` / `natts` / `atttypid` are pure in-process reads done here.
//! C's `myState->attrinfo != typeinfo` identity check compares the descriptor
//! *pointer*; the owned model records the borrowed descriptor's address as an
//! opaque identity token (never dereferenced — only compared) to reproduce the
//! "did the slot's descriptor change?" trigger exactly.
//!
//! Every genuinely-external subsystem is reached through its owner's per-owner
//! `-seams` crate (loud-panic until the owner installs it), exactly as the
//! sibling COPY-OUT path (`copyto.c`) does for the same calls: the catalog
//! type-output lookups go through `backend-utils-cache-lsyscache-seams`
//! (`getTypeOutputInfo` / `getTypeBinaryOutputInfo`); the fmgr lookup +
//! calling convention go through `backend-utils-fmgr-fmgr-seams` (`fmgr_info` /
//! `OutputFunctionCall` / `SendFunctionCall`); the executor `TupleTableSlot`
//! deconstruction (`slot_getallattrs` + the subsequent `tts_values` /
//! `tts_isnull` reads) goes through `backend-executor-execTuples-seams`. The
//! timing clock (`INSTR_TIME_SET_CURRENT`) and the global buffer-usage snapshot
//! (`pgBufferUsage`) are direct calls into the already-ported
//! `portability-instr-time` / `backend-executor-instrument` crates, exactly as
//! `explain.c` reads them. The pure in-process arithmetic the C performs inline
//! (`INSTR_TIME_ACCUM_DIFF`, `BufferUsageAccumDiff`) is ported directly here.
//!
//! The per-row `tmpcontext` (`AllocSetContextCreate` + `MemoryContextReset`)
//! exists in C only to recover the output strings allocated per row; in the
//! owned model those allocations are the per-row `cols`/output `PgVec`s, dropped
//! as locals at the end of each `serializeAnalyzeReceive` call (the same
//! technique `CopyOneRowTo` uses), so there is no separate context to juggle.

#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::Mcx;
use types_core::fmgr::FmgrInfo;
use types_core::instrument::{instr_time, BufferUsage};
use types_dest::dest::CommandDest;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_explain::{ExplainSerializeOption, ExplainState};
use ::nodes::tuptable::SlotData;
use stringinfo::StringInfo;
use types_tuple::heaptuple::TupleDescData;

use execTuples_seams as exectuples_s;
use lsyscache_seams as lsyscache_s;
use fmgr_seams as fmgr_s;

use pqformat::{
    pq_beginmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16, pq_sendint32,
};

/// `PqMsg_DataRow` (`libpq/protocol.h`).
pub const PqMsg_DataRow: u8 = b'D';

/// `typedef struct SerializeMetrics` (commands/explain_dr.h) — instrumentation
/// data for EXPLAIN's SERIALIZE option. `Default` zeroes every field, matching
/// the C `memset(&metrics, 0, ...)` + `INSTR_TIME_SET_ZERO(timeSpent)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SerializeMetrics {
    /// `uint64 bytesSent` — # of bytes serialized.
    pub bytesSent: u64,
    /// `instr_time timeSpent` — time spent serializing.
    pub timeSpent: instr_time,
    /// `BufferUsage bufferUsage` — buffers accessed during serialization.
    pub bufferUsage: BufferUsage,
}

/// C's `myState->attrinfo` is a `TupleDesc` pointer that `serializeAnalyzeReceive`
/// compares for raw equality against the slot's descriptor to decide whether the
/// cached per-attribute info needs re-deriving. The owned model records the
/// borrowed descriptor's address as a plain token (never dereferenced).
fn descriptor_identity(typeinfo: &TupleDescData) -> usize {
    core::ptr::from_ref(typeinfo) as *const () as usize
}

/// C: `typedef struct SerializeDestReceiver` — private state for an
/// `EXPLAIN (SERIALIZE)` destination object. The I/O buffer (`buf`) and the
/// per-row `tmpcontext` belong to external subsystems (reached via a
/// caller-supplied buffer / the owned per-row-local discipline); `es` is held
/// as an owned borrow rather than a raw `*const`. What remains is the receiver
/// bookkeeping `explain_dr.c` owns directly.
pub struct SerializeDestReceiver<'es> {
    /// C: `DestReceiver pub` — the `CommandDest` this receiver targets
    /// (`DestExplainSerialize`), set by [`SerializeDestReceiver::create`].
    pub mydest: CommandDest,
    /// C: `ExplainState *es` — this EXPLAIN statement's `ExplainState`, held as
    /// an owned borrow so the callbacks can read `es.serialize` / `es.timing` /
    /// `es.buffers` exactly as the C reads `myState->es->...`.
    pub es: &'es ExplainState<'es>,
    /// C: `int8 format` — text (0) or binary (1), like the pq wire protocol.
    pub format: i8,
    /// C: `TupleDesc attrinfo` — the output tuple desc we are set up for, held
    /// as the descriptor's *identity token* (never dereferenced). `None` means
    /// "not set up".
    attrinfo: Option<usize>,
    /// C: `int nattrs` — current number of columns.
    pub nattrs: i32,
    /// C: `FmgrInfo *finfos` — precomputed call info for the output fns. Empty
    /// means `finfos == NULL`.
    pub finfos: Vec<FmgrInfo>,
    /// C: `SerializeMetrics metrics` — collected metrics.
    pub metrics: SerializeMetrics,
}

impl<'es> SerializeDestReceiver<'es> {
    /// True if the receiver's cached descriptor identity matches `typeinfo`
    /// (C: `myState->attrinfo == typeinfo`).
    fn attrinfo_matches(&self, typeinfo: &TupleDescData) -> bool {
        self.attrinfo == Some(descriptor_identity(typeinfo))
    }

    /// C: `CreateExplainSerializeDestReceiver(ExplainState *es)` — build a
    /// `DestReceiver` for `EXPLAIN (SERIALIZE)` instrumentation.
    ///
    /// In C this `palloc0`s the receiver, installs the four callbacks, records
    /// `mydest = DestExplainSerialize`, and stores `es`. Here the callbacks are
    /// the free functions below; the constructor records `mydest`, `es` and the
    /// palloc0-zeroed remainder.
    pub fn create(es: &'es ExplainState<'es>) -> Self {
        SerializeDestReceiver {
            // self->pub.mydest = DestExplainSerialize;
            mydest: CommandDest::ExplainSerialize,
            // self->es = es;
            es,
            // palloc0-zeroed remainder:
            format: 0,
            attrinfo: None,
            nattrs: 0,
            finfos: Vec::new(),
            metrics: SerializeMetrics::default(),
        }
    }
}

/// `CreateExplainSerializeDestReceiver(ExplainState *es)` — free-function alias
/// for [`SerializeDestReceiver::create`], matching the C entry-point name.
pub fn CreateExplainSerializeDestReceiver<'es>(
    es: &'es ExplainState<'es>,
) -> SerializeDestReceiver<'es> {
    SerializeDestReceiver::create(es)
}

/// `INSTR_TIME_ACCUM_DIFF(x, y, z)` (`instr_time.h`): `x += (y - z)`. Pure
/// in-process arithmetic on the ported [`instr_time`].
fn instr_time_accum_diff(x: &mut instr_time, y: instr_time, z: instr_time) {
    x.accum_diff(y, z);
}

/// `BufferUsageAccumDiff(dst, add, sub)` (`instrument.c`): `*dst += (*add -
/// *sub)` field-by-field. Pure in-process arithmetic on the ported
/// [`BufferUsage`].
fn buffer_usage_accum_diff(dst: &mut BufferUsage, add: &BufferUsage, sub: &BufferUsage) {
    dst.shared_blks_hit += add.shared_blks_hit - sub.shared_blks_hit;
    dst.shared_blks_read += add.shared_blks_read - sub.shared_blks_read;
    dst.shared_blks_dirtied += add.shared_blks_dirtied - sub.shared_blks_dirtied;
    dst.shared_blks_written += add.shared_blks_written - sub.shared_blks_written;
    dst.local_blks_hit += add.local_blks_hit - sub.local_blks_hit;
    dst.local_blks_read += add.local_blks_read - sub.local_blks_read;
    dst.local_blks_dirtied += add.local_blks_dirtied - sub.local_blks_dirtied;
    dst.local_blks_written += add.local_blks_written - sub.local_blks_written;
    dst.temp_blks_read += add.temp_blks_read - sub.temp_blks_read;
    dst.temp_blks_written += add.temp_blks_written - sub.temp_blks_written;
    instr_time_accum_diff(
        &mut dst.shared_blk_read_time,
        add.shared_blk_read_time,
        sub.shared_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.shared_blk_write_time,
        add.shared_blk_write_time,
        sub.shared_blk_write_time,
    );
    instr_time_accum_diff(
        &mut dst.local_blk_read_time,
        add.local_blk_read_time,
        sub.local_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.local_blk_write_time,
        add.local_blk_write_time,
        sub.local_blk_write_time,
    );
    instr_time_accum_diff(
        &mut dst.temp_blk_read_time,
        add.temp_blk_read_time,
        sub.temp_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.temp_blk_write_time,
        add.temp_blk_write_time,
        sub.temp_blk_write_time,
    );
}

/// `INSTR_TIME_SET_CURRENT(t)` (`instr_time.h`): read the monotonic clock,
/// directly through the ported `portability-instr-time`, exactly as
/// `explain.c` does.
fn instr_time_current() -> instr_time {
    let mut t = instr_time::default();
    instr_time::instr_time_set_current(&mut t);
    t
}

/// `pgBufferUsage` (`instrument.c`): a snapshot of the global per-backend
/// buffer-usage counters, read directly through the ported
/// `backend-executor-instrument`, exactly as `explain.c` does.
fn pg_buffer_usage() -> BufferUsage {
    instrument::pgBufferUsage()
}

/// C: `serialize_prepare_info(SerializeDestReceiver *receiver, TupleDesc
/// typeinfo, int nattrs)` — get the function lookup info we need for output.
///
/// This is a subset of what `printtup_prepare_info()` does. We don't need to
/// cope with format choices varying across columns, so it's slightly simpler:
/// every column uses `receiver.format`. Rejects format codes other than 0
/// (text) and 1 (binary) with the C `ERRCODE_INVALID_PARAMETER_VALUE`
/// "unsupported format code: %d" error.
pub fn serialize_prepare_info(
    receiver: &mut SerializeDestReceiver<'_>,
    typeinfo: &TupleDescData,
    nattrs: i32,
) -> PgResult<()> {
    // get rid of any old data (C: if (receiver->finfos) pfree(...);
    // receiver->finfos = NULL).
    receiver.finfos.clear();

    receiver.attrinfo = Some(descriptor_identity(typeinfo));
    receiver.nattrs = nattrs;
    if nattrs <= 0 {
        return Ok(());
    }

    // C: receiver->finfos = palloc0(nattrs * sizeof(FmgrInfo)).
    let mut finfos: Vec<FmgrInfo> = Vec::new();
    finfos
        .try_reserve(nattrs as usize)
        .map_err(|_| PgError::error("serialize_prepare_info: out of memory"))?;

    for i in 0..nattrs as usize {
        // Form_pg_attribute attr = TupleDescAttr(typeinfo, i);
        let attr = typeinfo.attr(i);
        let finfo;

        if receiver.format == 0 {
            // wire protocol format text
            // getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
            let (typoutput, _typisvarlena) =
                lsyscache_s::get_type_output_info::call(attr.atttypid)?;
            // fmgr_info(typoutput, finfo);
            fmgr_s::fmgr_info_check::call(typoutput)?;
            finfo = FmgrInfo {
                fn_oid: typoutput,
                ..Default::default()
            };
        } else if receiver.format == 1 {
            // wire protocol format binary
            // getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
            let (typsend, _typisvarlena) =
                lsyscache_s::get_type_binary_output_info::call(attr.atttypid)?;
            // fmgr_info(typsend, finfo);
            fmgr_s::fmgr_info_check::call(typsend)?;
            finfo = FmgrInfo {
                fn_oid: typsend,
                ..Default::default()
            };
        } else {
            return Err(
                PgError::error(format!("unsupported format code: {}", receiver.format))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            );
        }
        finfos.push(finfo);
    }
    receiver.finfos = finfos;
    Ok(())
}

/// C: `serializeAnalyzeReceive(TupleTableSlot *slot, DestReceiver *self)` —
/// collect tuples for `EXPLAIN (SERIALIZE)`.
///
/// This matches `printtup()` in `printtup.c` as closely as possible, except for
/// the addition of measurement code (timing + buffers) and that the constructed
/// message is **never sent**: instead of `pq_endmessage_reuse()`, we just count
/// `buf.len()` into `metrics.bytesSent`, leaving the buffer to be reset on the
/// next iteration (as also happens in `printtup()`).
///
/// The caller owns the reusable per-message `buf` (created in
/// [`serializeAnalyzeStartup`]) and supplies the slot (`slot->tts_tupleDescriptor`
/// is read off `typeinfo`); we re-derive attr info if the slot's `TupleDesc`
/// changed, fully deconstruct the tuple (`slot_getallattrs` seam), then build the
/// DataRow bytes. The C per-row `tmpcontext` is the owned `cols`/output `PgVec`s
/// dropped as locals at the end of the call.
pub fn serializeAnalyzeReceive<'mcx>(
    myState: &mut SerializeDestReceiver<'_>,
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    typeinfo: &TupleDescData,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let natts = typeinfo.natts;

    // es->timing / es->buffers are read off the held ExplainState.
    let timing = myState.es.timing;
    let buffers = myState.es.buffers;

    // only measure time, buffers if requested
    let mut start = instr_time::default();
    if timing {
        // INSTR_TIME_SET_CURRENT(start);
        start = instr_time_current();
    }
    let mut instr_start = BufferUsage::default();
    if buffers {
        // instr_start = pgBufferUsage;
        instr_start = pg_buffer_usage();
    }

    // Set or update my derived attribute info, if needed.
    if !myState.attrinfo_matches(typeinfo) || myState.nattrs != natts {
        serialize_prepare_info(myState, typeinfo, natts)?;
    }

    // Make sure the tuple is fully deconstructed (C: slot_getallattrs(slot);
    // then the per-column reads are slot->tts_values[i] / slot->tts_isnull[i]).
    let cols = exectuples_s::slot_getallattrs::call(mcx, slot)?;

    // Switch into per-row context so we can recover memory below: in the owned
    // model the per-row workspace is `cols` plus the output `PgVec`s built
    // below, all dropped as locals at the end of this call.

    // Prepare a DataRow message (note buffer is in per-query context). We fill a
    // StringInfo buffer the same as printtup() does, so as to capture the costs
    // of manipulating the strings accurately.
    pq_beginmessage_reuse(buf, PqMsg_DataRow);
    pq_sendint16(buf, natts as u16)?;

    // send the attributes of this tuple
    for i in 0..natts as usize {
        let (value, isnull) = &cols[i];

        if *isnull {
            pq_sendint32(buf, (-1i32) as u32)?;
            continue;
        }

        if myState.format == 0 {
            // Text output
            let outputstr = {
                let finfo = &myState.finfos[i];
                fmgr_s::output_function_call::call(mcx, finfo, value)?
            };
            pq_sendcountedtext(buf, &outputstr)?;
        } else {
            // Binary output
            let outputbytes = {
                let finfo = &myState.finfos[i];
                fmgr_s::send_function_call::call(mcx, finfo, value)?
            };
            // C: pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ); the seam
            // returns exactly those VARDATA payload bytes.
            pq_sendint32(buf, outputbytes.len() as u32)?;
            pq_sendbytes(buf, &outputbytes)?;
        }
    }

    // We mustn't call pq_endmessage_reuse(), since that would actually send the
    // data to the client.  Just count the data, instead.  We can leave the
    // buffer alone; it'll be reset on the next iteration (as would also happen in
    // printtup()).
    myState.metrics.bytesSent += buf.len() as u64;

    // Return to caller's context, and flush row's temporary memory: the owned
    // per-row `cols`/output `PgVec`s are dropped as locals at the end of the
    // call (C: MemoryContextSwitchTo(oldcontext); MemoryContextReset(tmpcontext)).

    // Update timing data
    if timing {
        // INSTR_TIME_SET_CURRENT(end);
        let end = instr_time_current();
        // INSTR_TIME_ACCUM_DIFF(myState->metrics.timeSpent, end, start);
        instr_time_accum_diff(&mut myState.metrics.timeSpent, end, start);
    }

    // Update buffer metrics
    if buffers {
        // BufferUsageAccumDiff(&myState->metrics.bufferUsage, &pgBufferUsage,
        //                      &instr_start);
        let now = pg_buffer_usage();
        buffer_usage_accum_diff(&mut myState.metrics.bufferUsage, &now, &instr_start);
    }

    Ok(true)
}

/// C: `serializeAnalyzeStartup(DestReceiver *self, int operation, TupleDesc
/// typeinfo)` — start up the serializeAnalyze receiver.
///
/// Asserts `receiver.es != NULL` (always true for an owned borrow), selects the
/// wire-protocol `format` from `es.serialize`, initializes the re-used I/O
/// buffer (returned to the caller; it must be re-used across rows and live
/// outside the per-row workspace, mirroring `initStringInfo(&receiver->buf)`),
/// and zeroes the metrics. The C per-row `tmpcontext`
/// (`AllocSetContextCreate("SerializeTupleReceive", ...)`) is the owned per-row-
/// local discipline in [`serializeAnalyzeReceive`], so nothing is created here.
/// The `operation` and `typeinfo` arguments are unused in the C code (matched
/// here).
pub fn serializeAnalyzeStartup<'mcx>(
    receiver: &mut SerializeDestReceiver<'_>,
    mcx: Mcx<'mcx>,
    _operation: i32,
    _typeinfo: &TupleDescData,
) -> PgResult<StringInfo<'mcx>> {
    // Assert(receiver->es != NULL); — an owned borrow is always non-null.

    match receiver.es.serialize {
        ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE => {
            // C: Assert(false);
            debug_assert!(
                false,
                "serializeAnalyzeStartup called for EXPLAIN_SERIALIZE_NONE"
            );
        }
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT => {
            receiver.format = 0; // wire protocol format text
        }
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY => {
            receiver.format = 1; // wire protocol format binary
        }
    }

    // The output buffer is re-used across rows, as in printtup.c
    // (C: initStringInfo(&receiver->buf)).
    let buf = StringInfo::new_in(mcx);

    // Initialize results counters (C: memset(&receiver->metrics, 0, ...);
    // INSTR_TIME_SET_ZERO(receiver->metrics.timeSpent)). `Default` zeroes the
    // whole SerializeMetrics, which includes a zeroed instr_time.
    receiver.metrics = SerializeMetrics::default();

    Ok(buf)
}

/// C: `serializeAnalyzeShutdown(DestReceiver *self)` — shut down the
/// serializeAnalyze receiver.
///
/// Frees the cached `finfos` (and resets the descriptor identity). The I/O
/// buffer is released by the caller and the per-row workspace is the owned
/// per-row-local discipline (nothing to delete).
pub fn serializeAnalyzeShutdown(receiver: &mut SerializeDestReceiver<'_>) -> PgResult<()> {
    // C: if (receiver->finfos) pfree(receiver->finfos); receiver->finfos = NULL.
    receiver.finfos.clear();
    receiver.attrinfo = None;
    receiver.nattrs = 0;

    // C: if (receiver->buf.data) pfree(receiver->buf.data); buf.data = NULL.
    // The buffer is owned by the caller.

    // C: if (receiver->tmpcontext) MemoryContextDelete(receiver->tmpcontext);
    //    receiver->tmpcontext = NULL. The per-row workspace is the owned
    //    per-row-local discipline; there is no separate context to delete.
    Ok(())
}

/// C: `serializeAnalyzeDestroy(DestReceiver *self)` — `pfree(self)`. The receiver
/// is dropped by its owner; this is the explicit consuming free.
pub fn serializeAnalyzeDestroy(self_: SerializeDestReceiver<'_>) {
    drop(self_);
}

/// C: `GetSerializationMetrics(DestReceiver *dest)` — collect metrics.
///
/// We have to be careful here since the receiver could be an `IntoRel` receiver
/// if the subject statement is `CREATE TABLE AS`. In that case, return
/// all-zeroes stats (matching the C `memset(&empty, 0, ...)` +
/// `INSTR_TIME_SET_ZERO(empty.timeSpent)`).
///
/// In C the discriminant test is `dest->mydest == DestExplainSerialize` on the
/// bare `DestReceiver`; here it is the typed receiver's `mydest`. A caller
/// holding a different receiver kind passes `None`, yielding the all-zeroes
/// result, exactly as the C `else` branch does.
pub fn GetSerializationMetrics(dest: Option<&SerializeDestReceiver<'_>>) -> SerializeMetrics {
    match dest {
        Some(receiver) if receiver.mydest == CommandDest::ExplainSerialize => receiver.metrics,
        _ => {
            // memset(&empty, 0, sizeof(SerializeMetrics));
            // INSTR_TIME_SET_ZERO(empty.timeSpent);
            SerializeMetrics::default()
        }
    }
}

/// This crate owns no inward seams (it only calls outward through other
/// owners' `-seams` crates), so `init_seams` is empty.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
