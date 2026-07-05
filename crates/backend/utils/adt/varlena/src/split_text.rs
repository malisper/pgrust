use datum::array_build::ArrayBuildState;
use datum::Datum;
use mcx::Mcx;
use types_core::{Oid, TEXTOID};
use types_error::PgResult;
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::{text_position_get_match_len, text_position_get_match_off, text_position_next,
    text_position_setup, texteq};

fn accum_field<'m>(
    mcx: Mcx<'m>,
    astate: Option<ArrayBuildState<'m>>,
    scratch: &mut Vec<u8>,
    field: &[u8],
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<ArrayBuildState<'m>> {
    let is_null = match null_string {
        Some(ns) => texteq(field, ns, collation)?,
        None => false,
    };
    scratch.clear();
    scratch.extend_from_slice(&datum::varlena::set_varsize_4b(4 + field.len()));
    scratch.extend_from_slice(field);
    let d = if is_null {
        Datum::null()
    } else {
        Datum::from_usize(scratch.as_ptr() as usize)
    };
    ::arrayfuncs::accum_array_result(mcx, astate, d, is_null, TEXTOID)
}

// C split_text (varlena.c): field boundaries only, shared by the array
// (text_to_array) and table/SRF (text_to_table) output arms below.
struct TableField {
    bytes: Vec<u8>,
    is_null: bool,
}

fn split_fields(fcinfo: &Fcinfo) -> PgResult<Vec<TableField>> {
    let mut out = Vec::new();
    if fcinfo.argisnull(0) {
        return Ok(out);
    }
    // SAFETY: arg 0 checked non-null; a live text varlena.
    let inputstring: Vec<u8> = unsafe { fcinfo.arg_varlena_packed(0) }?.data().to_vec();
    let fldsep: Option<Vec<u8>> = if !fcinfo.argisnull(1) {
        // SAFETY: arg 1 checked non-null; a live text varlena.
        Some(unsafe { fcinfo.arg_varlena_packed(1) }?.data().to_vec())
    } else {
        None
    };
    let null_string: Option<Vec<u8>> = if fcinfo.nargs() > 2 && !fcinfo.argisnull(2) {
        // SAFETY: arg 2 checked non-null; a live text varlena.
        Some(unsafe { fcinfo.arg_varlena_packed(2) }?.data().to_vec())
    } else {
        None
    };
    let collation = fcinfo.get_collation();
    let ns = null_string.as_deref();

    let push = |out: &mut Vec<TableField>, field: &[u8]| -> PgResult<()> {
        let is_null = match ns {
            Some(n) => texteq(field, n, collation)?,
            None => false,
        };
        out.push(TableField { bytes: field.to_vec(), is_null });
        Ok(())
    };

    match &fldsep {
        Some(sep) => {
            if inputstring.is_empty() {
                // empty input: valid, zero fields
            } else if sep.is_empty() {
                push(&mut out, &inputstring)?;
            } else {
                let mut state = text_position_setup(&inputstring, sep, collation)?;
                let mut start = 0usize;
                loop {
                    let found = text_position_next(&mut state)?;
                    let chunk = if found {
                        &inputstring[start..text_position_get_match_off(&state)]
                    } else {
                        &inputstring[start..]
                    };
                    push(&mut out, chunk)?;
                    if !found {
                        break;
                    }
                    start = text_position_get_match_off(&state) + text_position_get_match_len(&state);
                }
            }
        }
        None => {
            let mut off = 0usize;
            while off < inputstring.len() {
                let l = mbutils::pg_mblen_range(&inputstring[off..])? as usize;
                push(&mut out, &inputstring[off..off + l])?;
                off += l;
            }
        }
    }
    Ok(out)
}

// C: text_to_table/text_to_table_null — same field split as text_to_array,
// materialized via the ValuePerCall SRF protocol (ports funcapi_srf, not
// funcapi, to avoid a varlena<->funcapi dependency cycle). A NULL input
// string (split_text's early-false return) yields zero rows, matching C.
pub fn fc_text_to_table(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("text_to_table: resolved FmgrInfo required");
    if !flinfo.has_fn_extra() {
        let fields = split_fields(fcinfo)?;
        let fctx = funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.max_calls = fields.len() as u64;
        fctx.user_fctx = Some(Box::new(fields));
    }
    let fctx = funcapi_srf::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let fields = fctx
        .user_fctx
        .as_ref()
        .expect("text_to_table: fields set at first call")
        .downcast_ref::<Vec<TableField>>()
        .expect("text_to_table: user_fctx is Vec<TableField>");
    match fields.get(idx) {
        Some(field) if field.is_null => Ok(funcapi_srf::srf_return_next_null(flinfo, fcinfo)),
        Some(field) => {
            let mcx = fcinfo.result_mcx();
            let t = types_fmgr::varlena_result(crate::cstring_to_text(mcx, &field.bytes)?);
            Ok(funcapi_srf::srf_return_next(flinfo, fcinfo, t))
        }
        None => Ok(funcapi_srf::srf_return_done(flinfo, fcinfo)),
    }
}

// C split_text (varlena.c), array output arm.
pub fn fc_text_to_array(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = flinfo;
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 checked non-null; a live text varlena.
    let inputstring: Vec<u8> = unsafe { fcinfo.arg_varlena_packed(0) }?.data().to_vec();
    let fldsep: Option<Vec<u8>> = if !fcinfo.argisnull(1) {
        // SAFETY: arg 1 checked non-null; a live text varlena.
        Some(unsafe { fcinfo.arg_varlena_packed(1) }?.data().to_vec())
    } else {
        None
    };
    let null_string: Option<Vec<u8>> = if fcinfo.nargs() > 2 && !fcinfo.argisnull(2) {
        // SAFETY: arg 2 checked non-null; a live text varlena.
        Some(unsafe { fcinfo.arg_varlena_packed(2) }?.data().to_vec())
    } else {
        None
    };
    let collation = fcinfo.get_collation();
    let ns = null_string.as_deref();

    let mut astate: Option<ArrayBuildState<'_>> = None;
    let mut scratch: Vec<u8> = Vec::new();

    match &fldsep {
        Some(sep) => {
            if inputstring.is_empty() {
                // empty input: valid, zero elements
            } else if sep.is_empty() {
                astate =
                    Some(accum_field(mcx, astate.take(), &mut scratch, &inputstring, ns, collation)?);
            } else {
                let mut state = text_position_setup(&inputstring, sep, collation)?;
                let mut start = 0usize;
                loop {
                    let found = text_position_next(&mut state)?;
                    let chunk = if found {
                        &inputstring[start..text_position_get_match_off(&state)]
                    } else {
                        &inputstring[start..]
                    };
                    astate =
                        Some(accum_field(mcx, astate.take(), &mut scratch, chunk, ns, collation)?);
                    if !found {
                        break;
                    }
                    start = text_position_get_match_off(&state)
                        + text_position_get_match_len(&state);
                }
            }
        }
        None => {
            let mut off = 0usize;
            while off < inputstring.len() {
                let l = mbutils::pg_mblen_range(&inputstring[off..])? as usize;
                astate = Some(accum_field(
                    mcx,
                    astate.take(),
                    &mut scratch,
                    &inputstring[off..off + l],
                    ns,
                    collation,
                )?);
                off += l;
            }
        }
    }

    let img = match &astate {
        None => ::arrayfuncs::construct_empty_array(mcx, TEXTOID)?,
        Some(st) => ::arrayfuncs::make_array_result(mcx, st)?,
    };
    byref_result(mcx, &img)
}
