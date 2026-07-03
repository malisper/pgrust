//! fmgr wrappers (`fc_*`) + the `VARLENA_BUILTINS` table for fmgr-core.
//! bttextsortsupport and the string_agg combine/serialize/deserialize rows are
//! registered loud panics (sort lane / parallel agg); value cores live
//! in the crate root. text/bytea recv/send ride the binary-wire fmgr frame
//! (types_fmgr::wire). unknownrecv/unknownsend stay value-core-only (unknown is
//! a pseudo-type; no binary wire registration in pg_proc.dat).

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
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
}

// hashtext/hashtextextended (hashfunc.c), deterministic-collation lane; the
// nondeterministic (ICU sort-key) leg is loud.
pub fn fc_hashtext(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    hashtext_check_collation(fcinfo.get_collation())?;
    Ok(Datum::from_u32(::hashfn::hash_bytes(key.data())))
}

pub fn fc_hashtextextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    hashtext_check_collation(fcinfo.get_collation())?;
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(key.data(), seed.value.as_u64())))
}

fn hashtext_check_collation(collid: types_core::Oid) -> PgResult<()> {
    crate::check_collation_set_pub(collid)?;
    if !crate::collation_is_c_known_pub(collid)
        && !pg_locale_seams::collation_is_deterministic::call(collid)?
    {
        panic!("hashtext (hashfunc.c): nondeterministic collation hashing not ported");
    }
    Ok(())
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

// C returns one of the input pointers — so do the larger/smaller wrappers.
pub fn fc_text_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(
        if crate::text_cmp(a.data(), b.data(), fcinfo.get_collation())? > 0 {
            fcinfo.arg(0)
        } else {
            fcinfo.arg(1)
        },
    )
}

pub fn fc_text_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(
        if crate::text_cmp(a.data(), b.data(), fcinfo.get_collation())? < 0 {
            fcinfo.arg(0)
        } else {
            fcinfo.arg(1)
        },
    )
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

pub fn fc_textlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::text_length(payload)))
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
    fc_string_agg_combine: "string_agg_combine", "parallel (partial) aggregation unported";
    fc_string_agg_serialize: "string_agg_serialize", "parallel (partial) aggregation unported";
    fc_string_agg_deserialize: "string_agg_deserialize", "parallel (partial) aggregation unported";
    fc_bttextsortsupport: "bttextsortsupport", "abbreviated-key SortSupport unported (sort lane)";
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
    b(400, "hashtext", 1, fc_hashtext),
    b(448, "hashtextextended", 2, fc_hashtextextended),
    b(109, "unknownin", 1, fc_unknownin),
    b(110, "unknownout", 1, fc_unknownout),
    b(157, "textne", 2, fc_textne),
    b(360, "bttextcmp", 2, fc_bttextcmp),
    b(458, "text_larger", 2, fc_text_larger),
    b(459, "text_smaller", 2, fc_text_smaller),
    b(720, "byteaoctetlen", 1, fc_byteaoctetlen),
    b(721, "byteaGetByte", 2, fc_bytea_get_byte),
    b(722, "byteaSetByte", 3, fc_bytea_set_byte),
    b(723, "byteaGetBit", 2, fc_bytea_get_bit),
    b(724, "byteaSetBit", 3, fc_bytea_set_bit),
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
    b(6393, "bytea_larger", 2, fc_bytea_larger),
    b(6394, "bytea_smaller", 2, fc_bytea_smaller),
];
