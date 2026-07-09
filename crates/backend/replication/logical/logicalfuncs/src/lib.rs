#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use elog::{elog, ereport};
use funcapi::{InitMaterializedSRF, MaterializedSRF};
use logical::{
    CheckLogicalDecodingRequirements, CreateDecodingContext, LogicalConfirmReceivedLocation,
    OutputPluginContext, OutputPluginOutputType, PluginOption,
};
use logical_decode::LogicalDecodingProcessRecord;
use mcx::Mcx;
use slot::{
    CheckSlotPermissions, MyReplicationSlot, ReplicationSlotAcquire, ReplicationSlotMarkDirty,
    ReplicationSlotRelease, WaitForStandbyConfirmation,
};
use types_core::{Oid, TransactionId, XLogRecPtr, TEXTOID};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERROR,
};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction};
use xlogreader::LocalPageRead;

const InvalidXLogRecPtr: XLogRecPtr = 0;
const MaxAllocSize: usize = 0x3fffffff;
const VARHDRSZ: usize = 4;

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("logicalfuncs.c", 0, func)
}

struct DecodingOutputState {
    srf: *mut MaterializedSRF<'static>,
    mcx: Mcx<'static>,
    binary_output: bool,
    returned_rows: i64,
}

fn output_state(opc: &OutputPluginContext) -> &'static mut DecodingOutputState {
    debug_assert!(opc.output_writer_private != 0);
    // SAFETY: set by pg_logical_slot_get_changes_guts to a live
    // DecodingOutputState that outlives the decode loop.
    unsafe { &mut *(opc.output_writer_private as *mut DecodingOutputState) }
}

fn LogicalOutputPrepareWrite(
    opc: &mut OutputPluginContext,
    _lsn: XLogRecPtr,
    _xid: TransactionId,
    _last_write: bool,
) -> PgResult<()> {
    opc.out.clear();
    Ok(())
}

fn LogicalOutputWrite(
    opc: &mut OutputPluginContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    _last_write: bool,
) -> PgResult<()> {
    if opc.out.len() > MaxAllocSize - VARHDRSZ {
        return elog(ERROR, "too much output for sql interface");
    }

    let p = output_state(opc);

    let mut values = [Datum::from_usize(0); 3];
    let nulls = [false; 3];
    values[0] = Datum::from_u64(lsn);
    values[1] = Datum::from_transaction_id(xid);
    values[2] = varlena_result(varlena::cstring_to_text(p.mcx, opc.out.as_bytes())?);

    // SAFETY: srf points to the live MaterializedSRF owned by the guts frame.
    unsafe { (*p.srf).putvalues(&values, &nulls)? };
    p.returned_rows += 1;
    Ok(())
}

fn detoast_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let ptr = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(ptr, types_tuple::varatt::varsize_any(ptr)) };
    detoast::detoast_attr(mcx, raw)
}

fn text_datum_to_string(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let img = detoast_datum(mcx, d)?;
    let bytes = varlena::text_to_cstring(mcx, &img)?;
    Ok(String::from_utf8_lossy(&bytes[..bytes.len().saturating_sub(1)]).into_owned())
}

fn pg_logical_slot_get_changes_guts(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    confirm: bool,
    binary: bool,
) -> PgResult<Datum> {
    CheckSlotPermissions()?;
    CheckLogicalDecodingRequirements()?;

    if fcinfo.argisnull(0) {
        ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("slot name must not be null")
            .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
    }
    // SAFETY: arg 0 is a non-null name datum.
    let name_bytes = unsafe { *fcinfo.arg_name(0) };
    let name = types_tuple::NameData { data: name_bytes };
    let name = String::from_utf8_lossy(name.name_str()).into_owned();

    let upto_lsn: XLogRecPtr = if fcinfo.argisnull(1) {
        InvalidXLogRecPtr
    } else {
        fcinfo.arg_i64(1) as XLogRecPtr
    };
    let upto_nchanges: i32 = if fcinfo.argisnull(2) {
        0
    } else {
        fcinfo.arg_i32(2)
    };
    if fcinfo.argisnull(3) {
        ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("options array must not be null")
            .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
    }

    // SAFETY: caller arms result_mcx for the query lifetime.
    let mcx: Mcx<'static> = unsafe { fcinfo.result_mcx_detached() };

    let arr_img = detoast_datum(mcx, fcinfo.arg(3))?;
    let (ndim, _, _) = arrayfuncs::read_dims_lbounds(&arr_img);
    let mut options: Vec<PluginOption> = Vec::new();
    if ndim > 1 {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("array must be one-dimensional")
            .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
    } else if arrayfuncs::array_contains_nulls(&arr_img) {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("array must not contain nulls")
            .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
    } else if ndim == 1 {
        let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &arr_img, TEXTOID, false)?;
        if elems.len() % 2 != 0 {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("array must have even number of elements")
                .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
        }
        let mut i = 0;
        while i < elems.len() {
            let optname = text_datum_to_string(mcx, elems[i])?;
            let opt = text_datum_to_string(mcx, elems[i + 1])?;
            options.push((optname, Some(opt)));
            i += 2;
        }
    }

    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    if transam_xlog::RecoveryInProgress() {
        panic!("unported callee reached from logicalfuncs.c: recovery end-of-wal");
    }
    let end_of_wal = transam_xlog::write::GetFlushRecPtr(None);

    ReplicationSlotAcquire(&name, true, true)?;

    let mut p = DecodingOutputState {
        srf: &mut srf as *mut MaterializedSRF<'_> as *mut MaterializedSRF<'static>,
        mcx,
        binary_output: binary,
        returned_rows: 0,
    };

    let result = decode_loop(
        flinfo,
        &mut p,
        options,
        upto_lsn,
        upto_nchanges,
        end_of_wal,
        confirm,
        binary,
    );

    match result {
        Ok(()) => {
            inval::local::InvalidateSystemCaches()?;
            Ok(srf.finish(fcinfo))
        }
        Err(e) => {
            // C releases the slot via resource-owner abort; do it directly.
            let _ = inval::local::InvalidateSystemCaches();
            if MyReplicationSlot().is_some() {
                let _ = ReplicationSlotRelease();
            }
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_loop(
    flinfo: &mut FmgrInfo,
    p: &mut DecodingOutputState,
    options: Vec<PluginOption>,
    upto_lsn: XLogRecPtr,
    upto_nchanges: i32,
    end_of_wal: XLogRecPtr,
    confirm: bool,
    binary: bool,
) -> PgResult<()> {
    let mut ctx = CreateDecodingContext(
        InvalidXLogRecPtr,
        options,
        false,
        Some(LogicalOutputPrepareWrite),
        Some(LogicalOutputWrite),
        None,
    )?;

    if !binary && ctx.opc().options.output_type != OutputPluginOutputType::Textual {
        let plugin = String::from_utf8_lossy(ctx.slot.data.get().plugin.name_str()).into_owned();
        let procname = adt_regproc::format_procedure(p.mcx, flinfo.fn_oid)?;
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "logical decoding output plugin \"{plugin}\" produces binary output, but function \"{procname}\" expects textual data"
            ))
            .finish(loc("pg_logical_slot_get_changes_guts"))?;
        unreachable!();
    }

    let wait_for_wal_lsn = if upto_lsn == InvalidXLogRecPtr {
        end_of_wal
    } else {
        upto_lsn.min(end_of_wal)
    };
    WaitForStandbyConfirmation(wait_for_wal_lsn)?;

    ctx.opc().output_writer_private = p as *mut DecodingOutputState as usize;

    let restart_lsn = ctx.slot.data.get().restart_lsn;
    ctx.reader.XLogBeginRead(restart_lsn);

    inval::local::InvalidateSystemCaches()?;

    let mut routine = LocalPageRead { wait_for_wal: true };
    while ctx.reader.v.EndRecPtr < end_of_wal {
        let record = ctx.reader.XLogReadRecord(&mut routine)?;
        if record.is_none() {
            let msg = ctx.reader.errormsg().unwrap_or_default().to_string();
            return elog(
                ERROR,
                format!("could not find record for logical decoding: {msg}"),
            );
        }

        LogicalDecodingProcessRecord(&mut ctx)?;

        if upto_lsn != InvalidXLogRecPtr && upto_lsn <= ctx.reader.v.EndRecPtr {
            break;
        }
        if upto_nchanges != 0 && upto_nchanges as i64 <= p.returned_rows {
            break;
        }
    }

    if ctx.reader.v.EndRecPtr != InvalidXLogRecPtr && confirm {
        LogicalConfirmReceivedLocation(ctx.reader.v.EndRecPtr)?;
        ReplicationSlotMarkDirty();
    }

    ctx.free()?;
    ReplicationSlotRelease()?;
    Ok(())
}

pub fn fc_pg_logical_slot_get_changes(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_logical_slot_get_changes requires flinfo");
    pg_logical_slot_get_changes_guts(flinfo, fcinfo, true, false)
}

pub fn fc_pg_logical_slot_peek_changes(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_logical_slot_peek_changes requires flinfo");
    pg_logical_slot_get_changes_guts(flinfo, fcinfo, false, false)
}

pub fn fc_pg_logical_slot_get_binary_changes(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_logical_slot_get_binary_changes requires flinfo");
    pg_logical_slot_get_changes_guts(flinfo, fcinfo, true, true)
}

pub fn fc_pg_logical_slot_peek_binary_changes(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_logical_slot_peek_binary_changes requires flinfo");
    pg_logical_slot_get_changes_guts(flinfo, fcinfo, false, true)
}

// pg_logical_emit_message_bytea/_text (logicalfuncs.c): text and bytea are
// binary compatible, so both OIDs share one body — text_to_cstring's
// strlen() truncation on an embedded NUL in prefix is reproduced by slicing
// at the first zero byte before handing prefix to LogLogicalMessage.
fn pg_logical_emit_message_guts(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let transactional = fcinfo.arg_bool(0);
    // SAFETY: caller arms result_mcx for the query lifetime.
    let mcx: Mcx<'static> = unsafe { fcinfo.result_mcx_detached() };

    let prefix_img = detoast_datum(mcx, fcinfo.arg(1))?;
    let prefix_payload = &prefix_img[VARHDRSZ..];
    let prefix_len = prefix_payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(prefix_payload.len());
    let prefix = &prefix_payload[..prefix_len];

    let data_img = detoast_datum(mcx, fcinfo.arg(2))?;
    let message = &data_img[VARHDRSZ..];

    let flush = fcinfo.arg_bool(3);

    let lsn = logical_message::LogLogicalMessage(prefix, message, transactional, flush)?;
    Ok(Datum::from_u64(lsn))
}

pub fn fc_pg_logical_emit_message_bytea(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    pg_logical_emit_message_guts(fcinfo)
}

pub fn fc_pg_logical_emit_message_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    pg_logical_emit_message_guts(fcinfo)
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs: 4,
        strict: false,
        retset: true,
        func,
    }
}

const fn be(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs: 4,
        strict: true,
        retset: false,
        func,
    }
}

pub const LOGICALFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    be(3577, "pg_logical_emit_message_text", fc_pg_logical_emit_message_text),
    be(3578, "pg_logical_emit_message_bytea", fc_pg_logical_emit_message_bytea),
    b(3782, "pg_logical_slot_get_changes", fc_pg_logical_slot_get_changes),
    b(
        3783,
        "pg_logical_slot_get_binary_changes",
        fc_pg_logical_slot_get_binary_changes,
    ),
    b(3784, "pg_logical_slot_peek_changes", fc_pg_logical_slot_peek_changes),
    b(
        3785,
        "pg_logical_slot_peek_binary_changes",
        fc_pg_logical_slot_peek_binary_changes,
    ),
];
