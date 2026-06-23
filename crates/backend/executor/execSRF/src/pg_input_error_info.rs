//! `pg_input_error_info(text, text)` (misc.c:716) registered as an
//! executor-frame record-returning function.
//!
//! `pg_input_error_info` is declared `RETURNS record` with four OUT params
//! (`message`, `detail`, `hint`, `sql_error_code`), so a `SELECT * FROM
//! pg_input_error_info('junk', 'bool')` reaches it through
//! nodeFunctionscan → [`crate::ExecMakeTableFunctionResult`] → the executor-frame
//! SRF table. It is NOT a set-returning function: it produces exactly one
//! composite row (a single `HeapTupleGetDatum(heap_form_tuple(...))`), so this
//! wrapper returns one `Datum::Composite` with `isDone` left at
//! `ExprSingleResult` (the value-per-call loop stores the one row and stops).
//!
//! The value core — the soft-error probe and the `message`/`detail`/`hint`/
//! `unpack_sql_state(sqlerrcode)` field extraction (misc.c:744-757) — lives in
//! [`misc::pg_input_error_info`]. Here we only marshal the two
//! `text` arguments off the executor frame, run the core, and build the result
//! tuple against the call's expected descriptor (C: `get_call_result_type`,
//! reached here through `rsinfo.expectedDesc`).

use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `pg_proc` OID of `pg_input_error_info(text, text) RETURNS record`
/// (pg_proc.dat: `oid => '6211'`).
const PG_INPUT_ERROR_INFO: Oid = 6211;

/// Register `pg_input_error_info` in the executor-frame SRF table (the by-OID
/// builtin registry's tag-only `resultinfo` cannot carry the live
/// `ReturnSetInfo`/`expectedDesc` this record function needs — WONTFIX
/// dual-home).
pub(crate) fn register_pg_input_error_info() {
    register_srf(PG_INPUT_ERROR_INFO, pg_input_error_info);
}

/// Read a `text` argument off the executor frame as its NUL-free payload bytes.
///
/// `ExecEvalFuncArgs` marshals a by-reference `text` argument as a null word
/// plus its detoasted `VARDATA_ANY` payload bytes (no varlena header) in
/// `ref_args[i]` — the same header-less by-ref-lane convention the
/// `pg_input_is_valid` fmgr-builtin reads. These are exactly the `str` bytes the
/// misc value core wants (C: `text_to_cstring(PG_GETARG_TEXT_PP(i))`).
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_input_error_info: text arg missing from the by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. A small stored text reaches an fmgr arg verbatim
    // once `SHORT_VARLENA_PACKING` is on; a fixed 4-byte strip would drop three
    // payload bytes. No-op while the flag is off (every value is 4-byte).
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

/// `pg_input_error_info(PG_FUNCTION_ARGS)` (misc.c:716) over the executor frame.
fn pg_input_error_info<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_input_error_info: fn_mcxt set by the SRF caller");

    // C: get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE ->
    //    elog(ERROR, "return type must be a row type"). The expected composite
    //    descriptor reaches us through the live ReturnSetInfo (resultinfo).
    let tupdesc = fcinfo
        .resultinfo
        .as_ref()
        .and_then(|rsi| rsi.expectedDesc.as_ref())
        .expect("pg_input_error_info: expected composite result descriptor (resultinfo.expectedDesc)")
        .clone_in(mcx)
        .expect("pg_input_error_info: clone result descriptor");

    // C: txt = PG_GETARG_TEXT_PP(0); typname = PG_GETARG_TEXT_PP(1);
    let txt = arg_text_payload(fcinfo, 0);
    let typname = arg_text_payload(fcinfo, 1);

    // Run the value core (the soft-error probe + field extraction). A bad TYPE
    // NAME is a hard `ereport(ERROR)` (C passes NULL escontext to the type-name
    // parse); raise it through the one dispatch point every PGFunction crosses
    // (`invoke_pgfunction`'s `catch_unwind`), exactly as the fmgr-builtin
    // adapters do.
    let info = misc::pg_input_error_info(txt, typname)?;

    // C: heap_form_tuple(tupdesc, values, isnull). Each non-NULL column is a
    // CStringGetTextDatum (a `text` varlena `Datum::ByRef`); a missing
    // detail/hint is SQL NULL.
    let cols: [Option<&[u8]>; 4] = [
        info.message.as_deref(),
        info.detail.as_deref(),
        info.hint.as_deref(),
        info.sql_error_code.as_deref(),
    ];

    let mut values: [Datum<'mcx>; 4] = [
        Datum::null(),
        Datum::null(),
        Datum::null(),
        Datum::null(),
    ];
    let mut isnull = [true; 4];
    for (k, col) in cols.iter().enumerate() {
        if let Some(bytes) = col {
            let s = core::str::from_utf8(bytes)
                .expect("pg_input_error_info: error text is valid UTF-8");
            values[k] = varlena_seams::cstring_to_text_v::call(mcx, s)
                .expect("pg_input_error_info: cstring_to_text");
            isnull[k] = false;
        }
    }

    let formed = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &isnull)
        .expect("pg_input_error_info: heap_form_tuple");

    // C: return HeapTupleGetDatum(...). One single-result row; the value-per-call
    // loop stores it and stops (isDone stays ExprSingleResult).
    fcinfo.isnull = false;
    Ok(Datum::Composite(formed))
}
