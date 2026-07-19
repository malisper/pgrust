use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{
    catalog::{CHAROID, INT2ARRAYOID, INT2OID, RECORDOID, TEXTARRAYOID, TEXTOID},
    Oid,
};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_ZERO_LENGTH_CHARACTER_STRING,
};
use ::types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::io::{tsvector_in_core, tsvector_out_core, tsvector_recv_core, tsvector_send_core};
use crate::layout::{wep_getpos, wep_getweight, TsVec};
use crate::op::*;
use crate::query::TsQueryRef;

// PG_GETARG_TSVECTOR: full image on the by-ref lane; a short-header stored
// value is expanded so payload offsets stay 2/4-aligned (PG_DETOAST_DATUM).
pub fn arg_tsvector<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<TsVec<'a>> {
    // SAFETY: catalog arg is a non-null live tsvector varlena.
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }?;
    let payload = if pv.is_short() {
        pv.data_expanded(fcinfo.result_mcx())?
    } else {
        pv.data()
    };
    Ok(TsVec { payload })
}

pub fn arg_tsquery<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<TsQueryRef<'a>> {
    // SAFETY: catalog arg is a non-null live tsquery varlena (plain storage).
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }?;
    let payload = if pv.is_short() {
        pv.data_expanded(fcinfo.result_mcx())?
    } else {
        pv.data()
    };
    Ok(TsQueryRef { payload })
}

fn image_result(img: PgVec<'_, u8>) -> Datum {
    varlena_result(::datum::Varlena::from_image(img))
}

pub fn fc_tsvectorin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: input-function arg 0 is a live NUL-terminated cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: context, if set, is a live ErrorSaveNode for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    match tsvector_in_core(mcx, s, esc)? {
        Some(img) => Ok(image_result(img)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_tsvectorout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(cstring_result(tsvector_out_core(mcx, v)?))
}

pub fn fc_tsvectorsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(varlena_result(tsvector_send_core(mcx, v)?))
}

pub fn fc_tsvectorrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let img = tsvector_recv_core(mcx, buf)?;
    Ok(image_result(img))
}

macro_rules! fc_tsvector_cmp {
    ($($fc:ident: $conv:expr;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_tsvector(fcinfo, 0)?;
            let b = arg_tsvector(fcinfo, 1)?;
            let res = silly_cmp_tsvector(a, b);
            #[allow(clippy::redundant_closure_call)]
            Ok(($conv)(res))
        }
    )*};
}

fc_tsvector_cmp! {
    fc_tsvector_lt: |r: i32| Datum::from_bool(r < 0);
    fc_tsvector_le: |r: i32| Datum::from_bool(r <= 0);
    fc_tsvector_eq: |r: i32| Datum::from_bool(r == 0);
    fc_tsvector_ne: |r: i32| Datum::from_bool(r != 0);
    fc_tsvector_ge: |r: i32| Datum::from_bool(r >= 0);
    fc_tsvector_gt: |r: i32| Datum::from_bool(r > 0);
    fc_tsvector_cmp: |r: i32| Datum::from_i32(r);
}

pub fn fc_tsvector_strip(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(image_result(tsvector_strip_core(mcx, v)?))
}

pub fn fc_tsvector_length(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    Ok(Datum::from_i32(v.size() as i32))
}

pub fn fc_tsvector_setweight(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    let w = weight_code(fcinfo.arg_char(1) as u8)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(image_result(tsvector_setweight_core(mcx, v, w)?))
}

fn text_datum_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: deconstructed text array element datums point into the live
    // detoasted array image for the duration of the call.
    let pv = unsafe { ::types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    pv.data()
}

fn arg_text_array<'a>(
    mcx: Mcx<'a>,
    fcinfo: &'a Fcinfo,
    i: usize,
) -> PgResult<(PgVec<'a, Datum>, PgVec<'a, bool>)> {
    // SAFETY: catalog arg is a non-null live text[] varlena.
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }?;
    let img = if pv.is_short() { pv.data_expanded(mcx)? } else { pv.data() };
    let mut full = vec_with_capacity_in(mcx, img.len() + 4)?;
    full.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut full, img)?;
    let full = full.leak();
    ::arrayfuncs::deconstruct_array_builtin(mcx, full, TEXTOID, true)
}

pub fn fc_tsvector_setweight_by_filter(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    let cw = fcinfo.arg_char(1) as u8;
    // C (tsvector_op.c tsvector_setweight_by_filter): elog(ERROR,
    // "unrecognized weight: %c", char_weight) — the raw byte lands in the
    // message (invalid UTF-8 for high bytes; a NUL byte ends the cstring).
    let w = weight_code(cw).map_err(|_| {
        let mut m = b"unrecognized weight: ".to_vec();
        m.push(cw);
        PgError::error_raw_message(m)
    })?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (elems, nulls) = arg_text_array(mcx, fcinfo, 2)?;
    let mut lexemes: PgVec<&[u8]> = PgVec::new_in(mcx);
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if !*isnull {
            lexemes.push(text_datum_bytes(*d));
        }
    }
    Ok(image_result(tsvector_setweight_by_filter_core(mcx, v, w, &lexemes)?))
}

pub fn fc_tsvector_concat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_tsvector(fcinfo, 0)?;
    let b = arg_tsvector(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(image_result(tsvector_concat_core(mcx, a, b)?))
}

pub fn fc_tsvector_delete_str(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: catalog arg 1 is a non-null live text varlena.
    let lex_pv = unsafe { fcinfo.arg_varlena_packed(1) }?;
    let lexeme = lex_pv.data();
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut skip: PgVec<usize> = PgVec::new_in(mcx);
    if let Some(i) = tsvector_bsearch(v, lexeme) {
        skip.push(i);
    }
    if skip.is_empty() {
        let mut img = vec_with_capacity_in(mcx, v.payload.len() + 4)?;
        img.extend_from_slice(&[0u8; 4]);
        ::mcx::vec_append_bytes(&mut img, v.payload)?;
        return Ok(image_result(img));
    }
    Ok(image_result(tsvector_delete_by_indices(mcx, v, &mut skip)?))
}

pub fn fc_tsvector_delete_arr(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (elems, nulls) = arg_text_array(mcx, fcinfo, 1)?;
    let mut skip: PgVec<usize> = PgVec::new_in(mcx);
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            continue;
        }
        if let Some(i) = tsvector_bsearch(v, text_datum_bytes(*d)) {
            skip.push(i);
        }
    }
    Ok(image_result(tsvector_delete_by_indices(mcx, v, &mut skip)?))
}

pub fn fc_tsvector_to_array(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut elems: PgVec<Datum> = vec_with_capacity_in(mcx, v.size())?;
    for i in 0..v.size() {
        let e = v.entry(i);
        elems.push(varlena_result(::varlena::cstring_to_text(mcx, v.lexeme(e))?));
    }
    let arr = ::arrayfuncs::construct_array(
        mcx,
        &elems,
        TEXTOID,
        -1,
        false,
        ::arrayfuncs::foundation::TYPALIGN_INT,
    )?;
    Ok(Datum::from_usize(arr.leak().as_ptr() as usize))
}

pub fn fc_array_to_tsvector(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (elems, nulls) = arg_text_array(mcx, fcinfo, 0)?;
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            return Err(PgError::error("lexeme array may not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .into());
        }
        if text_datum_bytes(*d).is_empty() {
            return Err(PgError::error("lexeme array may not contain empty strings")
                .with_sqlstate(ERRCODE_ZERO_LENGTH_CHARACTER_STRING)
                .into());
        }
    }
    let mut lexemes: PgVec<&[u8]> = vec_with_capacity_in(mcx, elems.len())?;
    for d in elems.iter() {
        lexemes.push(text_datum_bytes(*d));
    }
    lexemes.sort_by(|a, b| match crate::layout::ts_compare_string(a, b, false) {
        n if n < 0 => core::cmp::Ordering::Less,
        0 => core::cmp::Ordering::Equal,
        _ => core::cmp::Ordering::Greater,
    });
    lexemes.dedup();
    let datalen: usize = lexemes.iter().map(|l| l.len()).sum();
    let mut b = crate::layout::TsVecBuilder::with_capacity(mcx, lexemes.len(), datalen)?;
    for lex in &lexemes {
        b.push(lex, &[])?;
    }
    Ok(image_result(b.finish(mcx)?))
}

pub fn fc_tsvector_filter(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: catalog arg 1 is a non-null live "char"[] varlena.
    let pv = unsafe { fcinfo.arg_varlena_packed(1) }?;
    let img = if pv.is_short() { pv.data_expanded(mcx)? } else { pv.data() };
    let mut full = vec_with_capacity_in(mcx, img.len() + 4)?;
    full.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut full, img)?;
    let full = full.leak();
    let (elems, nulls) = ::arrayfuncs::deconstruct_array_builtin(mcx, full, CHAROID, true)?;
    let mut mask = 0u8;
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            return Err(PgError::error("weight array may not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .into());
        }
        match d.as_char() as u8 {
            b'A' | b'a' => mask |= 8,
            b'B' | b'b' => mask |= 4,
            b'C' | b'c' => mask |= 2,
            b'D' | b'd' => mask |= 1,
            other => {
                return Err(PgError::error(format!(
                    "unrecognized weight: \"{}\"",
                    other as char
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into())
            }
        }
    }
    Ok(image_result(tsvector_filter_core(mcx, v, mask)?))
}

pub fn fc_ts_match_vq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = arg_tsvector(fcinfo, 0)?;
    let q = arg_tsquery(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(Datum::from_bool(ts_match_vq_core(mcx, v, q)?))
}

pub fn fc_ts_match_qv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let q = arg_tsquery(fcinfo, 0)?;
    let v = arg_tsvector(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(Datum::from_bool(ts_match_vq_core(mcx, v, q)?))
}

// tsvector_unnest: rows materialized at first call (funcapi ValuePerCall).
enum UnnestRows {
    Tuples(Vec<Vec<u8>>),
}

pub fn fc_tsvector_unnest(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("tsvector_unnest: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = unnest_collect(fcinfo)?;
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(UnnestRows::Tuples(rows)));
    }
    let fctx = ::funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let UnnestRows::Tuples(rows) = fctx
        .user_fctx
        .as_ref()
        .expect("rows set at first call")
        .downcast_ref::<UnnestRows>()
        .expect("user_fctx is UnnestRows");
    match rows.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(::funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn unnest_collect(fcinfo: &mut Fcinfo) -> PgResult<Vec<Vec<u8>>> {
    let v = arg_tsvector(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 1, Some("lexeme"), TEXTOID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 2, Some("positions"), INT2ARRAYOID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 3, Some("weights"), TEXTARRAYOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc: register the anonymous record typmod so record_out can
    // resolve tuples that escape into the targetlist.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(v.size());
    for i in 0..v.size() {
        let e = v.entry(i);
        let lex = varlena_result(::varlena::cstring_to_text(mcx, v.lexeme(e))?);
        let poss = v.positions(e);
        let (mut values, mut nulls) = ([lex, Datum::null(), Datum::null()], [false, true, true]);
        if !poss.is_empty() {
            let mut positions: PgVec<Datum> = vec_with_capacity_in(mcx, poss.len())?;
            let mut weights: PgVec<Datum> = vec_with_capacity_in(mcx, poss.len())?;
            for &p in poss {
                positions.push(Datum::from_i16(wep_getpos(p) as i16));
                let w = [b'D' - wep_getweight(p) as u8];
                weights.push(varlena_result(::varlena::cstring_to_text(mcx, &w)?));
            }
            let posarr = ::arrayfuncs::construct_array(
                mcx,
                &positions,
                INT2OID,
                2,
                true,
                ::arrayfuncs::foundation::TYPALIGN_SHORT,
            )?;
            let warr = ::arrayfuncs::construct_array(
                mcx,
                &weights,
                TEXTOID,
                -1,
                false,
                ::arrayfuncs::foundation::TYPALIGN_INT,
            )?;
            values[1] = Datum::from_usize(posarr.leak().as_ptr() as usize);
            values[2] = Datum::from_usize(warr.leak().as_ptr() as usize);
            nulls[1] = false;
            nulls[2] = false;
        }
        let tuple = ::heaptuple::heap_form_tuple(mcx, &desc, &values, &nulls)?;
        rows.push(tuple.image().to_vec());
    }
    Ok(rows)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

pub const TSVECTOR_BUILTINS: &[FmgrBuiltin] = &[
    b(3319, "tsvector_filter", 2, fc_tsvector_filter),
    b(3320, "tsvector_setweight_by_filter", 3, fc_tsvector_setweight_by_filter),
    b(3321, "tsvector_delete_str", 2, fc_tsvector_delete_str),
    srf(3322, "tsvector_unnest", 1, fc_tsvector_unnest),
    b(3323, "tsvector_delete_arr", 2, fc_tsvector_delete_arr),
    b(3326, "tsvector_to_array", 1, fc_tsvector_to_array),
    b(3327, "array_to_tsvector", 1, fc_array_to_tsvector),
    b(3610, "tsvectorin", 1, fc_tsvectorin),
    b(3611, "tsvectorout", 1, fc_tsvectorout),
    b(3616, "tsvector_lt", 2, fc_tsvector_lt),
    b(3617, "tsvector_le", 2, fc_tsvector_le),
    b(3618, "tsvector_eq", 2, fc_tsvector_eq),
    b(3619, "tsvector_ne", 2, fc_tsvector_ne),
    b(3620, "tsvector_ge", 2, fc_tsvector_ge),
    b(3621, "tsvector_gt", 2, fc_tsvector_gt),
    b(3622, "tsvector_cmp", 2, fc_tsvector_cmp),
    b(3623, "tsvector_strip", 1, fc_tsvector_strip),
    b(3624, "tsvector_setweight", 2, fc_tsvector_setweight),
    b(3625, "tsvector_concat", 2, fc_tsvector_concat),
    b(3634, "ts_match_vq", 2, fc_ts_match_vq),
    b(3635, "ts_match_qv", 2, fc_ts_match_qv),
    b(3638, "tsvectorsend", 1, fc_tsvectorsend),
    b(3639, "tsvectorrecv", 1, fc_tsvectorrecv),
    b(3711, "tsvector_length", 1, fc_tsvector_length),
];
