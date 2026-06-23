//! `pg_walfile_name_offset(pg_lsn)` (OID 2850) and
//! `pg_split_walfile_name(text)` (OID 6213) registered as executor-frame
//! record-returning functions.
//!
//! Both are declared `RETURNS record` with OUT params, so a `SELECT * FROM
//! pg_split_walfile_name('...')` reaches them through nodeFunctionscan →
//! [`crate::ExecMakeTableFunctionResult`] → the executor-frame SRF table. Neither
//! is set-returning: each produces exactly one composite row.
//!
//! The value cores (the WAL-file-name / segment-offset computation and the
//! WAL-file-name parse) live in [`xlogfuncs`]; here we
//! marshal the one executor-frame argument, run the core, and build the result
//! tuple against the call's expected descriptor (C: `get_call_result_type`,
//! reached here through `rsinfo.expectedDesc`).

use types_core::Oid;
use types_error::PgResult;
use nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use heaptuple::heap_form_tuple;

use crate::register_srf;

/// `pg_proc` OID of `pg_walfile_name_offset(pg_lsn) RETURNS record`.
const PG_WALFILE_NAME_OFFSET: Oid = 2850;
/// `pg_proc` OID of `pg_split_walfile_name(text) RETURNS record`.
const PG_SPLIT_WALFILE_NAME: Oid = 6213;

/// Register the record-returning WAL filename functions in the executor-frame
/// SRF table (the by-OID builtin registry's tag-only `resultinfo` cannot carry
/// the live `expectedDesc` these record functions need — WONTFIX dual-home).
pub(crate) fn register_pg_walfile() {
    register_srf(PG_WALFILE_NAME_OFFSET, pg_walfile_name_offset);
    register_srf(PG_SPLIT_WALFILE_NAME, pg_split_walfile_name);
}

/// Read a `text` argument off the executor frame as its header-less payload.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_split_walfile_name: text arg missing from the by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. No-op while `SHORT_VARLENA_PACKING` is off.
    let bytes: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    };
    core::str::from_utf8(bytes).expect("pg_split_walfile_name: text arg is valid UTF-8")
}

/// `pg_walfile_name_offset(PG_FUNCTION_ARGS)` (xlogfuncs.c:374) over the frame.
fn pg_walfile_name_offset<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_walfile_name_offset: fn_mcxt set by the SRF caller");

    // C: locationpoint = PG_GETARG_LSN(0) — a by-value uint64.
    let locationpoint = fcinfo.args[0].value.as_u64();

    // Resolve the expected 2-column composite descriptor (file_name text,
    // file_offset int4) before running the core, mirroring C's
    // get_call_result_type composite check.
    let tupdesc = fcinfo
        .resultinfo
        .as_ref()
        .and_then(|rsi| rsi.expectedDesc.as_ref())
        .expect("pg_walfile_name_offset: expected composite result descriptor")
        .clone_in(mcx)
        .expect("pg_walfile_name_offset: clone result descriptor");

    let row = xlogfuncs::pg_walfile_name_offset(mcx, locationpoint)?;

    // C: values[0] = CStringGetTextDatum(...); values[1] = Int32GetDatum(offset).
    let values = [Datum::ByRef(row.file_name), Datum::from_i32(row.file_offset as i32)];
    let isnull = [false, false];
    let formed = heap_form_tuple(mcx, &tupdesc, &values, &isnull)
        .expect("pg_walfile_name_offset: heap_form_tuple");

    fcinfo.isnull = false;
    Ok(Datum::Composite(formed))
}

/// `pg_split_walfile_name(PG_FUNCTION_ARGS)` (xlogfuncs.c:463) over the frame.
fn pg_split_walfile_name<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_split_walfile_name: fn_mcxt set by the SRF caller");

    let fname = arg_text(fcinfo, 0).to_string();

    let tupdesc = fcinfo
        .resultinfo
        .as_ref()
        .and_then(|rsi| rsi.expectedDesc.as_ref())
        .expect("pg_split_walfile_name: expected composite result descriptor")
        .clone_in(mcx)
        .expect("pg_split_walfile_name: clone result descriptor");

    // The WAL-file-name validation hard-errors on a bad name; raise it through
    // the one dispatch point (invoke_pgfunction's catch_unwind), as C ereports.
    let row = xlogfuncs::pg_split_walfile_name(mcx, &fname)?;

    // C: values[0] = numeric segment_number; values[1] = Int64GetDatum(tli).
    let values = [
        Datum::ByRef(row.segment_number),
        Datum::from_i64(row.timeline_id),
    ];
    let isnull = [false, false];
    let formed = heap_form_tuple(mcx, &tupdesc, &values, &isnull)
        .expect("pg_split_walfile_name: heap_form_tuple");

    fcinfo.isnull = false;
    Ok(Datum::Composite(formed))
}
