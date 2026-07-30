//! fmgr wrappers (`fc_*`) + the `VARLENA_BUILTINS` table for fmgr-core.
//! bttextsortsupport is registered as a loud panic (sort lane, unrelated to
//! aggregation); value cores live in the crate root. text/bytea recv/send
//! ride the binary-wire fmgr frame (types_fmgr::wire), as do
//! unknownrecv/unknownsend (2416/2417 — pg_type's unknown row names them as
//! typreceive/typsend, so binary-format clients do reach them).

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: cstring result needs a resolved FmgrInfo's scratch; direct callers use the value core")
}

// C pallocs each cstring result per row; the resolved FmgrInfo owns retained
// scratch instead (rule 7; std Vec rides the open-set fn_extra Box slot).
// The result datum aliases it until the next call through the same FmgrInfo.
struct OutBuf(Vec<u8>);

fn out_scratch<'a>(flinfo: Option<&'a mut FmgrInfo>, name: &'static str) -> &'a mut Vec<u8> {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0
}

macro_rules! fc_textcmp {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null text/bytea varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::$conv(crate::$core(a.data(), b.data(), fcinfo.get_collation())?))
        }
    )*};
}

fc_textcmp! {
    fc_texteq: texteq -> from_bool;
    fc_textne: textne -> from_bool;
    fc_text_lt: text_lt -> from_bool;
    fc_text_le: text_le -> from_bool;
    fc_text_gt: text_gt -> from_bool;
    fc_text_ge: text_ge -> from_bool;
    fc_bttextcmp: bttextcmp -> from_i32;
    fc_text_starts_with: text_starts_with -> from_bool;
}

// hashtext/hashtextextended (hashfunc.c); nondeterministic collations hash
// the pg_strnxfrm sort key (seam Ok(Some)), deterministic hash the raw bytes.
pub fn fc_hashtext(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    if let Some(h) = hashtext_nondeterministic(fcinfo.get_collation(), key.data(), None)? {
        return Ok(Datum::from_u32(h as u32));
    }
    Ok(Datum::from_u32(::hashfn::hash_bytes(key.data())))
}

pub fn fc_hashtextextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    let seed = seed.value.as_u64();
    if let Some(h) = hashtext_nondeterministic(fcinfo.get_collation(), key.data(), Some(seed))? {
        return Ok(Datum::from_u64(h));
    }
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(key.data(), seed)))
}

// hashvarlena/hashbytea (hashfunc.c): raw-byte hash, no collation leg.
pub fn fc_hashvarlena(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_u32(::hashfn::hash_bytes(key.data())))
}

pub fn fc_hashvarlenaextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    let seed = seed.value.as_u64();
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(key.data(), seed)))
}

#[track_caller]
#[cold]
#[inline(never)]
fn hash_collation_err() -> Box<PgError> {
    Box::new(
        PgError::error("could not determine which collation to use for string hashing")
            .with_sqlstate(types_error::ERRCODE_INDETERMINATE_COLLATION)
            .with_hint("Use the COLLATE clause to set the collation explicitly."),
    )
}

pub(crate) fn hashtext_nondeterministic(
    collid: types_core::Oid,
    data: &[u8],
    seed: Option<u64>,
) -> PgResult<Option<u64>> {
    // hashtext (hashfunc.c) raises its own hashing-specific
    // indeterminate-collation message, not check_collation_set's.
    if !types_core::OidIsValid(collid) {
        return Err(hash_collation_err());
    }
    if crate::collation_is_c_known_pub(collid) {
        return Ok(None);
    }
    pg_locale_seams::varstr_nondeterministic_hash::call(collid, data, seed)
}

macro_rules! fc_text_pattern {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null text varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::$conv(crate::$core(a.data(), b.data())))
        }
    )*};
}

fc_text_pattern! {
    fc_text_pattern_lt: text_pattern_lt -> from_bool;
    fc_text_pattern_le: text_pattern_le -> from_bool;
    fc_text_pattern_ge: text_pattern_ge -> from_bool;
    fc_text_pattern_gt: text_pattern_gt -> from_bool;
    fc_bttext_pattern_cmp: bttext_pattern_cmp -> from_i32;
}

macro_rules! fc_byteacmp {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null bytea varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::$conv(crate::bytea::$core(a.data(), b.data())))
        }
    )*};
}

fc_byteacmp! {
    fc_byteaeq: byteaeq -> from_bool;
    fc_byteane: byteane -> from_bool;
    fc_bytealt: bytealt -> from_bool;
    fc_byteale: byteale -> from_bool;
    fc_byteagt: byteagt -> from_bool;
    fc_byteage: byteage -> from_bool;
    fc_byteacmp: byteacmp -> from_i32;
}

// C returns one of the PG_GETARG_TEXT_PP pointers — the DETOASTED (packed)
// image, never the raw compressed/external arg datum. Returning the raw arg
// (the old shape) leaked compressed transvalues into MIN/MAX transitions,
// breaking lanefold's inline-transvalue contract (q22coexist toast smoke)
// and diverging from C's transvalue bytes/memory accounting.
pub fn fc_text_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_usize(
        if crate::text_cmp(a.data(), b.data(), fcinfo.get_collation())? > 0 {
            a.as_ptr()
        } else {
            b.as_ptr()
        } as usize,
    ))
}

pub fn fc_text_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_usize(
        if crate::text_cmp(a.data(), b.data(), fcinfo.get_collation())? < 0 {
            a.as_ptr()
        } else {
            b.as_ptr()
        } as usize,
    ))
}

pub fn fc_bytea_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(if crate::bytea::byteacmp(a.data(), b.data()) > 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

pub fn fc_bytea_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(if crate::bytea::byteacmp(a.data(), b.data()) < 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

// Result varlena lives in the resolved FmgrInfo's scratch (see OutBuf);
// callers that outlive the FmgrInfo copy it out (C pallocs per call).
pub fn fc_textin(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of textin is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let buf = out_scratch(flinfo, "textin");
    buf.clear();
    buf.reserve(datum::varlena::VARHDRSZ + s.len());
    buf.extend_from_slice(&datum::varlena::set_varsize_4b(datum::varlena::VARHDRSZ + s.len()));
    buf.extend_from_slice(s);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_textout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let buf = out_scratch(flinfo, "textout");
    buf.clear();
    buf.reserve(payload.len() + 1);
    buf.extend_from_slice(payload);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_byteaout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let buf = out_scratch(flinfo, "byteaout");
    crate::bytea::byteaout_into(payload, crate::get_bytea_output(), buf)?;
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_unknownout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of unknownout is a non-null cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let buf = out_scratch(flinfo, "unknownout");
    buf.clear();
    buf.extend_from_slice(s.to_bytes_with_nul());
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

// New-by-ref results follow the result-mcx convention (notes/fc-result-convention.md):
// built in the frame's armed context, freed by that context's reset.
pub fn fc_textcat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_catenate(mcx, a.data(), b.data())?))
}

pub fn fc_byteacat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_catenate(
        mcx,
        a.data(),
        b.data(),
    )?))
}

pub fn fc_textrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::textrecv(mcx, buf)?))
}

pub fn fc_textsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of textsend is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::textsend(mcx, payload)?))
}

pub fn fc_bytearecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytearecv(mcx, buf)?))
}

pub fn fc_byteasend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of byteasend is a non-null bytea varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::byteasend(mcx, payload)?))
}

pub fn fc_byteain(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of byteain is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    match crate::bytea::byteain(mcx, s, esc)? {
        Some(v) => Ok(varlena_result(v)),
        // Soft error already saved; the value is C's garbage datum.
        None if had_esc => Ok(Datum::null()),
        None => panic!("byteain: soft-error escape without an escontext"),
    }
}

pub fn fc_unknownin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of unknownin is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::unknownin(mcx, s)?))
}

pub fn fc_unknownrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::unknownrecv(mcx, buf)?))
}

pub fn fc_unknownsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of unknownsend is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::unknownsend(mcx, s)?))
}

pub fn fc_textlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::text_length(payload)?))
}

pub fn fc_textoctetlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::textoctetlen(payload)))
}

pub fn fc_byteaoctetlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::bytea::byteaoctetlen(payload)))
}

pub fn fc_bytea_get_byte(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg_i32(1);
    Ok(Datum::from_i32(crate::bytea::bytea_get_byte(v.data(), n)?))
}

pub fn fc_bytea_get_bit(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg_i64(1);
    Ok(Datum::from_i32(crate::bytea::bytea_get_bit(v.data(), n)?))
}

pub fn fc_bytea_set_byte(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg_i32(1);
    let new_byte = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_set_byte(
        mcx,
        v.data(),
        n,
        new_byte,
    )?))
}

pub fn fc_bytea_set_bit(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg_i64(1);
    let new_bit = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_set_bit(
        mcx,
        v.data(),
        n,
        new_bit,
    )?))
}

pub fn fc_bytea_substr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let img = unsafe { fcinfo.arg_varlena_raw(0) };
    let s = fcinfo.arg_i32(1);
    let l = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_substring(
        mcx, img, s, l, false,
    )?))
}

pub fn fc_bytea_substr_no_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let img = unsafe { fcinfo.arg_varlena_raw(0) };
    let s = fcinfo.arg_i32(1);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_substring(
        mcx, img, s, -1, true,
    )?))
}

pub fn fc_text_substr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let img = unsafe { fcinfo.arg_varlena_raw(0) };
    let s = fcinfo.arg_i32(1);
    let l = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_substring(
        mcx, img, s, l, false,
    )?))
}

pub fn fc_text_substr_no_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let img = unsafe { fcinfo.arg_varlena_raw(0) };
    let s = fcinfo.arg_i32(1);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_substring(
        mcx, img, s, -1, true,
    )?))
}

pub fn fc_byteapos(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_i32(crate::bytea::byteapos(a.data(), b.data())))
}

pub fn fc_btvarstrequalimage(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(crate::btvarstrequalimage(
        fcinfo.get_collation(),
    )?))
}

pub fn fc_textpos(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_i32(crate::text_position(
        a.data(),
        b.data(),
        fcinfo.get_collation(),
    )?))
}


pub fn fc_replace_text(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (src, from_sub, to_sub) = unsafe {
        (
            fcinfo.arg_varlena_packed(0)?,
            fcinfo.arg_varlena_packed(1)?,
            fcinfo.arg_varlena_packed(2)?,
        )
    };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::replace_text(
        mcx,
        src.data(),
        from_sub.data(),
        to_sub.data(),
        fcinfo.get_collation(),
    )?))
}

pub fn fc_split_part(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let fldnum = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::split_part(
        mcx,
        a.data(),
        b.data(),
        fldnum,
        fcinfo.get_collation(),
    )?))
}

// string_agg_transfn / bytea_string_agg_transfn share one body in spirit; C
// keeps two symbols, so both fmgr rows point at the same appender.
fn string_agg_transfn_common(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let state = crate::string_agg::string_agg_transfn(fcinfo)?;
    if state.is_null() {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_string_agg_transfn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    string_agg_transfn_common(fcinfo)
}

pub fn fc_bytea_string_agg_transfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    string_agg_transfn_common(fcinfo)
}

fn string_agg_finalfn_common(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match crate::string_agg::string_agg_finalfn(fcinfo) {
        None => Ok(fcinfo.return_null()),
        Some(stripped) => Ok(varlena_result(crate::cstring_to_text(
            fcinfo.result_mcx(),
            stripped,
        )?)),
    }
}

pub fn fc_string_agg_finalfn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    string_agg_finalfn_common(fcinfo)
}

pub fn fc_bytea_string_agg_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    string_agg_finalfn_common(fcinfo)
}

macro_rules! fc_unported {
    ($($fname:ident: $cname:literal, $why:literal;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            panic!(concat!($cname, " (varlena.c): ", $why))
        }
    )*};
}

fc_unported! {
    fc_bttextsortsupport: "bttextsortsupport", "abbreviated-key SortSupport unported (sort lane)";
}

// string_agg_combine/serialize/deserialize (varlena.c): one C symbol each,
// shared by string_agg(text) and string_agg(bytea) (both trans to the same
// StringInfo-backed state).
pub fn fc_string_agg_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let state = crate::string_agg::string_agg_combine(fcinfo)?;
    if state.is_null() {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_string_agg_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 is the aggcontext-lived state (transfn/combine
    // contract), read-only here.
    let st =
        unsafe { &*(fcinfo.arg(0).as_usize() as *const crate::string_agg::StringAggState) };
    let mcx = fcinfo.result_mcx();
    let out = crate::string_agg::string_agg_serialize(mcx, st)?;
    Ok(varlena_result(out))
}

pub fn fc_string_agg_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 is a non-null live bytea.
    let sstate = unsafe { fcinfo.arg_varlena_packed(0)? };
    // SAFETY: deserial is only ever invoked with a live AggStateNode context
    // (matches C's Assert(AggCheckCallContext(fcinfo, NULL))).
    let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
        panic!("aggregate function called in non-aggregate context");
    };
    let state = crate::string_agg::string_agg_deserialize(agg_mcx, sstate.data())?;
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_unistr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::unistr(mcx, a.data())?))
}

pub fn fc_bytea_int2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i16(crate::bytea::bytea_int2(v.data())?))
}

pub fn fc_bytea_int4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i32(crate::bytea::bytea_int4(v.data())?))
}

pub fn fc_bytea_int8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::bytea::bytea_int8(v.data())?))
}

pub fn fc_int2_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::int_bytea(mcx, &fcinfo.arg_i16(0).to_be_bytes())?))
}

pub fn fc_int4_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::int_bytea(mcx, &fcinfo.arg_i32(0).to_be_bytes())?))
}

pub fn fc_int8_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::int_bytea(mcx, &fcinfo.arg_i64(0).to_be_bytes())?))
}

pub fn fc_bytea_reverse(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_reverse(mcx, v.data())?))
}

pub fn fc_unicode_version(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::unicode::unicode_version(mcx)?))
}

// C icu_unicode_version (varlena.c): U_UNICODE_VERSION under USE_ICU, else
// NULL. pgrust's USE_ICU analog is "libicu is loadable" (pg_locale icu_ffi);
// the seam owner reports the loaded library's Unicode version, which equals
// the C constant for the same ICU major. Seam uninstalled (substrate test
// binaries without pg_locale) or ICU unloadable = C-without-ICU NULL.
pub fn fc_icu_unicode_version(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if pg_locale_seams::icu_unicode_version::is_installed() {
        if let Some(v) = pg_locale_seams::icu_unicode_version::call() {
            let mcx = fcinfo.result_mcx();
            return Ok(varlena_result(crate::cstring_to_text(mcx, v.as_bytes())?));
        }
    }
    Ok(fcinfo.return_null())
}

pub fn fc_unicode_assigned(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_bool(crate::unicode::unicode_assigned(
        input.data(),
    )?))
}

pub fn fc_unicode_normalize_func(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (input, form) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::unicode::unicode_normalize_func(
        mcx,
        input.data(),
        form.data(),
    )?))
}

pub fn fc_unicode_is_normalized(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (input, form) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_bool(crate::unicode::unicode_is_normalized(
        mcx,
        input.data(),
        form.data(),
    )?))
}


pub fn fc_textoverlay(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null text varlenas (strict fn).
    // t1 stays the raw image: text_overlay substrings it via the
    // detoast_attr_slice fetch (fc_text_substr convention).
    let t1 = unsafe { fcinfo.arg_varlena_raw(0) };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg_i32(2);
    let sl = fcinfo.arg_i32(3);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_overlay(mcx, t1, t2.data(), sp, sl)?))
}

pub fn fc_textoverlay_no_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null text varlenas (strict fn).
    // t1 stays the raw image (fc_text_substr convention).
    let t1 = unsafe { fcinfo.arg_varlena_raw(0) };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg_i32(2);
    let sl = crate::text_length(t2.data())?;
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_overlay(mcx, t1, t2.data(), sp, sl)?))
}

pub fn fc_byteaoverlay(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null bytea varlenas (strict fn).
    // t1 stays the raw image (fc_bytea_substr convention).
    let t1 = unsafe { fcinfo.arg_varlena_raw(0) };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg_i32(2);
    let sl = fcinfo.arg_i32(3);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_overlay(mcx, t1, t2.data(), sp, sl)?))
}

pub fn fc_byteaoverlay_no_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null bytea varlenas (strict fn).
    // t1 stays the raw image (fc_bytea_substr convention).
    let t1 = unsafe { fcinfo.arg_varlena_raw(0) };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg_i32(2);
    let sl = crate::bytea::byteaoctetlen(t2.data());
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_overlay(mcx, t1, t2.data(), sp, sl)?))
}

pub fn fc_bytea_bit_count(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::bytea::bytea_bit_count(v.data())))
}

macro_rules! fc_convert_to_base {
    ($($fname:ident: $argfn:ident as $argty:ty, $base:literal;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let value = fcinfo.$argfn(0) as $argty as u64;
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::convert_to_base(mcx, value, $base)?))
        }
    )*};
}

fc_convert_to_base! {
    fc_to_bin32: arg_i32 as u32, 2;
    fc_to_bin64: arg_i64 as u64, 2;
    fc_to_oct32: arg_i32 as u32, 8;
    fc_to_oct64: arg_i64 as u64, 8;
    fc_to_hex32: arg_i32 as u32, 16;
    fc_to_hex64: arg_i64 as u64, 16;
}

// varlena.c: pg_column_size/pg_column_compression/pg_column_toast_chunk_id
// share the fn_extra-memoized argtype typlen lookup.
struct ArgTypLen(i16);

#[track_caller]
#[cold]
#[inline(never)]
fn type_cache_lookup_failed(typid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {typid}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_compression_method_id(cmid: u32) -> Box<PgError> {
    Box::new(PgError::error(format!("invalid compression method id {cmid}")))
}

// pub for proofs/strings-scalar (Kani stub target); behavior unchanged.
pub fn cached_arg_typlen(flinfo: &mut FmgrInfo, argno: i16) -> PgResult<i16> {
    if !flinfo.has_fn_extra() {
        let argtype = fmgr_seams::get_fn_expr_argtype::call(flinfo, argno);
        let typlen = lsyscache::get_typlen(argtype)?;
        if typlen == 0 {
            return Err(type_cache_lookup_failed(argtype));
        }
        flinfo.set_fn_extra(ArgTypLen(typlen));
    }
    Ok(flinfo.fn_extra_ref::<ArgTypLen>().unwrap().0)
}

pub fn fc_pg_column_size(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_column_size needs a resolved FmgrInfo");
    let typlen = cached_arg_typlen(flinfo, 0)?;
    let result: i32 = if typlen == -1 {
        // SAFETY: catalog arg 0 is a non-null varlena (proisstrict 't' "any").
        ::detoast::toast_datum_size(unsafe { fcinfo.arg_varlena_raw(0) }) as i32
    } else if typlen == -2 {
        // SAFETY: catalog arg 0 is a non-null cstring (typlen == -2).
        unsafe { fcinfo.arg_cstring(0) }.to_bytes().len() as i32 + 1
    } else {
        typlen as i32
    };
    Ok(Datum::from_i32(result))
}

pub fn fc_pg_column_compression(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_column_compression needs a resolved FmgrInfo");
    let typlen = cached_arg_typlen(flinfo, 0)?;
    if typlen != -1 {
        return Ok(fcinfo.return_null());
    }
    // SAFETY: catalog arg 0 is a non-null varlena (proisstrict 't' "any").
    let attr = unsafe { fcinfo.arg_varlena_raw(0) };
    let Some(cmid) = ::detoast::toast_get_compression_id(attr) else {
        return Ok(fcinfo.return_null());
    };
    let name: &[u8] = match cmid {
        ::detoast::TOAST_PGLZ_COMPRESSION_ID => b"pglz",
        ::detoast::TOAST_LZ4_COMPRESSION_ID => b"lz4",
        _ => return Err(invalid_compression_method_id(cmid)),
    };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::cstring_to_text(mcx, name)?))
}

pub fn fc_pg_column_toast_chunk_id(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_column_toast_chunk_id needs a resolved FmgrInfo");
    let typlen = cached_arg_typlen(flinfo, 0)?;
    if typlen != -1 {
        return Ok(fcinfo.return_null());
    }
    // SAFETY: catalog arg 0 is a non-null varlena (proisstrict 't' "any").
    let attr = unsafe { fcinfo.arg_varlena_raw(0) };
    if !::detoast::varatt_is_external_ondisk(attr) {
        return Ok(fcinfo.return_null());
    }
    let tp = ::detoast::VarattExternal::from_image(attr);
    Ok(Datum::from_oid(tp.va_valueid))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

const fn n(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: false,
        retset: false,
        func,
    }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: false,
        retset: true,
        func,
    }
}

// pg_proc.dat rows (none retset; the string_agg trans/final/combine rows are
// proisstrict 'f'); 1317/1369/1381 = textlen aliases, 936/937 = substr aliases.
pub const VARLENA_BUILTINS: &[FmgrBuiltin] = &[
    b(31, "byteaout", 1, fc_byteaout),
    b(46, "textin", 1, fc_textin),
    b(47, "textout", 1, fc_textout),
    b(2412, "bytearecv", 1, fc_bytearecv),
    b(2413, "byteasend", 1, fc_byteasend),
    b(2414, "textrecv", 1, fc_textrecv),
    b(2415, "textsend", 1, fc_textsend),
    b(67, "texteq", 2, fc_texteq),
    b(3696, "text_starts_with", 2, fc_text_starts_with),
    b(2160, "text_pattern_lt", 2, fc_text_pattern_lt),
    b(2161, "text_pattern_le", 2, fc_text_pattern_le),
    b(2163, "text_pattern_ge", 2, fc_text_pattern_ge),
    b(2164, "text_pattern_gt", 2, fc_text_pattern_gt),
    b(2166, "bttext_pattern_cmp", 2, fc_bttext_pattern_cmp),
    b(400, "hashtext", 1, fc_hashtext),
    b(448, "hashtextextended", 2, fc_hashtextextended),
    b(456, "hashvarlena", 1, fc_hashvarlena),
    b(772, "hashvarlenaextended", 2, fc_hashvarlenaextended),
    b(6413, "hashbytea", 1, fc_hashvarlena),
    b(6414, "hashbyteaextended", 2, fc_hashvarlenaextended),
    b(109, "unknownin", 1, fc_unknownin),
    b(110, "unknownout", 1, fc_unknownout),
    b(2416, "unknownrecv", 1, fc_unknownrecv),
    b(2417, "unknownsend", 1, fc_unknownsend),
    b(157, "textne", 2, fc_textne),
    b(360, "bttextcmp", 2, fc_bttextcmp),
    b(458, "text_larger", 2, fc_text_larger),
    b(459, "text_smaller", 2, fc_text_smaller),
    b(720, "byteaoctetlen", 1, fc_byteaoctetlen),
    b(721, "byteaGetByte", 2, fc_bytea_get_byte),
    b(722, "byteaSetByte", 3, fc_bytea_set_byte),
    b(723, "byteaGetBit", 2, fc_bytea_get_bit),
    b(724, "byteaSetBit", 3, fc_bytea_set_bit),
    b(6367, "int2_bytea", 1, fc_int2_bytea),
    b(6368, "int4_bytea", 1, fc_int4_bytea),
    b(6369, "int8_bytea", 1, fc_int8_bytea),
    b(6370, "bytea_int2", 1, fc_bytea_int2),
    b(6371, "bytea_int4", 1, fc_bytea_int4),
    b(6372, "bytea_int8", 1, fc_bytea_int8),
    b(6382, "bytea_reverse", 1, fc_bytea_reverse),
    b(740, "text_lt", 2, fc_text_lt),
    b(849, "textpos", 2, fc_textpos),
    b(868, "strpos", 2, fc_textpos),
    b(877, "text_substr", 3, fc_text_substr),
    b(883, "text_substr_no_len", 2, fc_text_substr_no_len),
    b(936, "text_substr", 3, fc_text_substr),
    b(937, "text_substr_no_len", 2, fc_text_substr_no_len),
    b(741, "text_le", 2, fc_text_le),
    b(742, "text_gt", 2, fc_text_gt),
    b(743, "text_ge", 2, fc_text_ge),
    b(1244, "byteain", 1, fc_byteain),
    b(1257, "textlen", 1, fc_textlen),
    b(1258, "textcat", 2, fc_textcat),
    b(1317, "textlen", 1, fc_textlen),
    b(1369, "textlen", 1, fc_textlen),
    b(1374, "textoctetlen", 1, fc_textoctetlen),
    b(1381, "textlen", 1, fc_textlen),
    b(1948, "byteaeq", 2, fc_byteaeq),
    b(1949, "bytealt", 2, fc_bytealt),
    b(1950, "byteale", 2, fc_byteale),
    b(1951, "byteagt", 2, fc_byteagt),
    b(1952, "byteage", 2, fc_byteage),
    b(1953, "byteane", 2, fc_byteane),
    b(1954, "byteacmp", 2, fc_byteacmp),
    b(2010, "byteaoctetlen", 1, fc_byteaoctetlen),
    b(2011, "byteacat", 2, fc_byteacat),
    b(2012, "bytea_substr", 3, fc_bytea_substr),
    b(2013, "bytea_substr_no_len", 2, fc_bytea_substr_no_len),
    b(2014, "byteapos", 2, fc_byteapos),
    b(2085, "bytea_substr", 3, fc_bytea_substr),
    b(2086, "bytea_substr_no_len", 2, fc_bytea_substr_no_len),
    b(3058, "text_concat", 1, crate::concat_format::fc_text_concat),
    b(3059, "text_concat_ws", 2, crate::concat_format::fc_text_concat_ws),
    b(3539, "text_format", 2, crate::concat_format::fc_text_format),
    b(3540, "text_format_nv", 1, crate::concat_format::fc_text_format),
    b(2087, "replace_text", 3, fc_replace_text),
    b(2088, "split_part", 3, fc_split_part),
    b(3255, "bttextsortsupport", 1, fc_bttextsortsupport),
    n(3535, "string_agg_transfn", 3, fc_string_agg_transfn),
    n(3536, "string_agg_finalfn", 1, fc_string_agg_finalfn),
    n(3543, "bytea_string_agg_transfn", 3, fc_bytea_string_agg_transfn),
    n(3544, "bytea_string_agg_finalfn", 1, fc_bytea_string_agg_finalfn),
    b(5050, "btvarstrequalimage", 1, fc_btvarstrequalimage),
    n(6299, "string_agg_combine", 2, fc_string_agg_combine),
    b(6300, "string_agg_serialize", 1, fc_string_agg_serialize),
    b(6301, "string_agg_deserialize", 2, fc_string_agg_deserialize),
    b(6198, "unistr", 1, fc_unistr),
    b(6393, "bytea_larger", 2, fc_bytea_larger),
    b(6394, "bytea_smaller", 2, fc_bytea_smaller),
    n(394, "text_to_array", 2, crate::split_text::fc_text_to_array),
    n(376, "text_to_array_null", 3, crate::split_text::fc_text_to_array),
    b(4350, "unicode_normalize_func", 2, fc_unicode_normalize_func),
    b(4351, "unicode_is_normalized", 2, fc_unicode_is_normalized),
    b(4549, "unicode_version", 0, fc_unicode_version),
    b(6099, "icu_unicode_version", 0, fc_icu_unicode_version),
    b(6105, "unicode_assigned", 1, fc_unicode_assigned),
    b(749, "byteaoverlay", 4, fc_byteaoverlay),
    b(752, "byteaoverlay_no_len", 3, fc_byteaoverlay_no_len),
    b(1404, "textoverlay", 4, fc_textoverlay),
    b(1405, "textoverlay_no_len", 3, fc_textoverlay_no_len),
    b(2089, "to_hex32", 1, fc_to_hex32),
    b(2090, "to_hex64", 1, fc_to_hex64),
    b(6163, "bytea_bit_count", 1, fc_bytea_bit_count),
    b(6330, "to_bin32", 1, fc_to_bin32),
    b(6331, "to_bin64", 1, fc_to_bin64),
    b(6332, "to_oct32", 1, fc_to_oct32),
    b(6333, "to_oct64", 1, fc_to_oct64),
    srf(6160, "text_to_table", 2, crate::split_text::fc_text_to_table),
    srf(6161, "text_to_table_null", 3, crate::split_text::fc_text_to_table),
    b(1269, "pg_column_size", 1, fc_pg_column_size),
    b(2121, "pg_column_compression", 1, fc_pg_column_compression),
    b(6316, "pg_column_toast_chunk_id", 1, fc_pg_column_toast_chunk_id),
];
