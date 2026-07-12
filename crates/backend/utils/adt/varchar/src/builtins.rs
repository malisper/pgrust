//! fmgr wrappers (`fc_*`) + the `VARCHAR_BUILTINS` table for fmgr-core.

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::VARHDRSZ;

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: cstring result needs a resolved FmgrInfo's scratch; direct callers use the value core")
}

// C pallocs each cstring/varlena result per row; the resolved FmgrInfo owns
// retained scratch instead (rule 7; std Vec rides the open-set fn_extra slot).
// The result datum aliases it until the next call through the same FmgrInfo.
struct OutBuf(Vec<u8>);

fn out_scratch<'a>(flinfo: Option<&'a mut FmgrInfo>, name: &'static str) -> &'a mut Vec<u8> {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0
}

pub fn fc_bpcharin(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of bpcharin is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let atttypmod = fcinfo.arg_i32(2);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    let Some(clip) = crate::bpchar_clip(s, atttypmod, esc)? else {
        if had_esc {
            return Ok(Datum::null());
        }
        panic!("bpcharin: soft-error escape without an escontext");
    };
    let buf = out_scratch(flinfo, "bpcharin");
    buf.clear();
    buf.reserve(VARHDRSZ + clip.total);
    buf.extend_from_slice(&datum::varlena::set_varsize_4b(VARHDRSZ + clip.total));
    buf.extend_from_slice(&s[..clip.copy]);
    buf.resize(VARHDRSZ + clip.total, b' ');
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_varcharin(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of varcharin is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let atttypmod = fcinfo.arg_i32(2);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    let Some(len) = crate::varchar_clip(s, atttypmod, esc)? else {
        if had_esc {
            return Ok(Datum::null());
        }
        panic!("varcharin: soft-error escape without an escontext");
    };
    let buf = out_scratch(flinfo, "varcharin");
    buf.clear();
    buf.reserve(VARHDRSZ + len);
    buf.extend_from_slice(&datum::varlena::set_varsize_4b(VARHDRSZ + len));
    buf.extend_from_slice(&s[..len]);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

fn fc_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo, name: &'static str) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar/varchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let buf = out_scratch(flinfo, name);
    buf.clear();
    buf.reserve(payload.len() + 1);
    buf.extend_from_slice(payload);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_bpcharout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_out(flinfo, fcinfo, "bpcharout")
}

pub fn fc_varcharout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_out(flinfo, fcinfo, "varcharout")
}

pub fn fc_bpcharrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let atttypmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bpcharrecv(mcx, buf, atttypmod)?))
}

pub fn fc_varcharrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let atttypmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::varcharrecv(mcx, buf, atttypmod)?))
}

pub fn fc_bpcharsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bpcharsend(mcx, payload)?))
}

pub fn fc_varcharsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::varcharsend(mcx, payload)?))
}

pub fn fc_bpchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let maxlen = fcinfo.arg_i32(1);
    let is_explicit = fcinfo.arg_bool(2);
    let mcx = fcinfo.result_mcx();
    match crate::bpchar(mcx, src.data(), maxlen, is_explicit)? {
        Some(v) => Ok(varlena_result(v)),
        None => Ok(fcinfo.arg(0)),
    }
}

pub fn fc_varchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varchar varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let typmod = fcinfo.arg_i32(1);
    let is_explicit = fcinfo.arg_bool(2);
    let mcx = fcinfo.result_mcx();
    match crate::varchar(mcx, src.data(), typmod, is_explicit)? {
        Some(v) => Ok(varlena_result(v)),
        None => Ok(fcinfo.arg(0)),
    }
}

pub fn fc_char_bpchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let c = fcinfo.arg_char(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::char_bpchar(mcx, c)?))
}

pub fn fc_bpchar_name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let name = crate::bpchar_name(payload);
    let mcx = fcinfo.result_mcx();
    byref_result(mcx, &name)
}

pub fn fc_name_bpchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of name_bpchar is a non-null name block (strict fn).
    let name = unsafe { fcinfo.arg_name(0) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::name_bpchar(mcx, name)?))
}

fn arg_array_image<'a>(fcinfo: &'a Fcinfo) -> &'a [u8] {
    // SAFETY: strict arg 0 is a non-null, in-memory (never toasted) typmod array.
    unsafe {
        let p = fcinfo.arg_ptr(0);
        core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p))
    }
}

pub fn fc_bpchartypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let arr = arg_array_image(fcinfo);
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i32(crate::bpchartypmodin(mcx, arr)?))
}

pub fn fc_varchartypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let arr = arg_array_image(fcinfo);
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i32(crate::varchartypmodin(mcx, arr)?))
}

fn fc_typmodout(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let mut tmp = [0u8; 16];
    let n = crate::anychar_typmodout(typmod, &mut tmp);
    let buf = out_scratch(flinfo, name);
    buf.clear();
    buf.extend_from_slice(&tmp[..n]);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_bpchartypmodout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_typmodout(flinfo, fcinfo, "bpchartypmodout")
}

pub fn fc_varchartypmodout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_typmodout(flinfo, fcinfo, "varchartypmodout")
}

macro_rules! fc_bpcharcmp {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null bpchar varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::$conv(crate::$core(a.data(), b.data(), fcinfo.get_collation())?))
        }
    )*};
}

fc_bpcharcmp! {
    fc_bpchareq: bpchareq -> from_bool;
    fc_bpcharne: bpcharne -> from_bool;
    fc_bpcharlt: bpcharlt -> from_bool;
    fc_bpcharle: bpcharle -> from_bool;
    fc_bpchargt: bpchargt -> from_bool;
    fc_bpcharge: bpcharge -> from_bool;
    fc_bpcharcmp: bpcharcmp -> from_i32;
}

// C returns one of the PG_GETARG_BPCHAR_PP pointers — the DETOASTED (packed)
// image, never the raw compressed/external arg datum (see fc_text_larger).
pub fn fc_bpchar_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bpchar varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_usize(
        if crate::bpcharcmp(a.data(), b.data(), fcinfo.get_collation())? >= 0 {
            a.as_ptr()
        } else {
            b.as_ptr()
        } as usize,
    ))
}

pub fn fc_bpchar_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bpchar varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_usize(
        if crate::bpcharcmp(a.data(), b.data(), fcinfo.get_collation())? <= 0 {
            a.as_ptr()
        } else {
            b.as_ptr()
        } as usize,
    ))
}

pub fn fc_bpcharlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::bpcharlen(payload)?))
}

pub fn fc_bpcharoctetlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::bpcharoctetlen(payload)))
}

pub fn fc_hashbpchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(
        crate::hashbpchar(payload, fcinfo.get_collation())? as i32,
    ))
}

pub fn fc_hashbpcharextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bpchar varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_i64(crate::hashbpcharextended(
        payload,
        fcinfo.get_collation(),
        seed,
    )? as i64))
}

macro_rules! fc_bpchar_pattern {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null bpchar varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::$conv(crate::$core(a.data(), b.data())))
        }
    )*};
}

fc_bpchar_pattern! {
    fc_bpchar_pattern_lt: bpchar_pattern_lt -> from_bool;
    fc_bpchar_pattern_le: bpchar_pattern_le -> from_bool;
    fc_bpchar_pattern_ge: bpchar_pattern_ge -> from_bool;
    fc_bpchar_pattern_gt: bpchar_pattern_gt -> from_bool;
    fc_btbpchar_pattern_cmp: btbpchar_pattern_cmp -> from_i32;
}

macro_rules! fc_unported {
    ($($fname:ident: $cname:literal => $lane:literal;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            panic!(concat!($cname, ": unported (", $lane, ")"))
        }
    )*};
}

// varchar_support (varchar.c): SupportRequestSimplify only — widening (or
// unconstraining) a varchar typmod becomes a RelabelType, no rewrite.
pub fn fc_varchar_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    if unsafe { *p } != NodeTag::T_SupportRequestSimplify {
        return Ok(Datum::from_usize(0));
    }
    // SAFETY: tag checked; the planner owns the request node for the call.
    let req = unsafe { &*(a.value.as_usize() as *const SupportRequestSimplify) };
    let fexpr = req
        .fcall
        .and_then(|n| n.as_func_expr())
        .unwrap_or_else(|| panic!("varchar_support: SupportRequestSimplify without a FuncExpr"));
    assert!(fexpr.args.len() >= 2);
    let Some(c) = fexpr.args.nth(1).as_const() else {
        return Ok(Datum::from_usize(0));
    };
    if c.constisnull {
        return Ok(Datum::from_usize(0));
    }
    let source = fexpr.args.nth(0);
    let old_typmod = nodes_core::expr_typmod(source);
    let new_typmod = c.constvalue.as_i32();
    let old_max = old_typmod - VARHDRSZ as i32;
    let new_max = new_typmod - VARHDRSZ as i32;
    if new_typmod < 0 || (old_typmod >= 0 && old_max <= new_max) {
        let mcx = req.mcx.expect("varchar_support: request carries an mcx");
        let ret = nodes_core::relabel_to_typmod(mcx, source, new_typmod)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

fc_unported! {
    fc_bpchar_sortsupport: "bpchar_sortsupport" => "SortSupport substrate, varlena bttextsortsupport lane";
    fc_btbpchar_pattern_sortsupport: "btbpchar_pattern_sortsupport" => "SortSupport substrate, varlena bttextsortsupport lane";
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

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending;
// 1318/1367/1372 = bpcharlen aliases (length/character_length/char_length).
pub const VARCHAR_BUILTINS: &[FmgrBuiltin] = &[
    b(408, "name_bpchar", 1, fc_name_bpchar),
    b(409, "bpchar_name", 1, fc_bpchar_name),
    b(668, "bpchar", 3, fc_bpchar),
    b(669, "varchar", 3, fc_varchar),
    b(860, "char_bpchar", 1, fc_char_bpchar),
    b(972, "hashbpcharextended", 2, fc_hashbpcharextended),
    b(1044, "bpcharin", 3, fc_bpcharin),
    b(1045, "bpcharout", 1, fc_bpcharout),
    b(1046, "varcharin", 3, fc_varcharin),
    b(1047, "varcharout", 1, fc_varcharout),
    b(1048, "bpchareq", 2, fc_bpchareq),
    b(1049, "bpcharlt", 2, fc_bpcharlt),
    b(1050, "bpcharle", 2, fc_bpcharle),
    b(1051, "bpchargt", 2, fc_bpchargt),
    b(1052, "bpcharge", 2, fc_bpcharge),
    b(1053, "bpcharne", 2, fc_bpcharne),
    b(1063, "bpchar_larger", 2, fc_bpchar_larger),
    b(1064, "bpchar_smaller", 2, fc_bpchar_smaller),
    b(1078, "bpcharcmp", 2, fc_bpcharcmp),
    b(1080, "hashbpchar", 1, fc_hashbpchar),
    b(1318, "bpcharlen", 1, fc_bpcharlen),
    b(1367, "bpcharlen", 1, fc_bpcharlen),
    b(1372, "bpcharlen", 1, fc_bpcharlen),
    b(1375, "bpcharoctetlen", 1, fc_bpcharoctetlen),
    b(2174, "bpchar_pattern_lt", 2, fc_bpchar_pattern_lt),
    b(2175, "bpchar_pattern_le", 2, fc_bpchar_pattern_le),
    b(2177, "bpchar_pattern_ge", 2, fc_bpchar_pattern_ge),
    b(2178, "bpchar_pattern_gt", 2, fc_bpchar_pattern_gt),
    b(2180, "btbpchar_pattern_cmp", 2, fc_btbpchar_pattern_cmp),
    b(2430, "bpcharrecv", 3, fc_bpcharrecv),
    b(2431, "bpcharsend", 1, fc_bpcharsend),
    b(2432, "varcharrecv", 3, fc_varcharrecv),
    b(2433, "varcharsend", 1, fc_varcharsend),
    b(2913, "bpchartypmodin", 1, fc_bpchartypmodin),
    b(2914, "bpchartypmodout", 1, fc_bpchartypmodout),
    b(2915, "varchartypmodin", 1, fc_varchartypmodin),
    b(2916, "varchartypmodout", 1, fc_varchartypmodout),
    b(3097, "varchar_support", 1, fc_varchar_support),
    b(3328, "bpchar_sortsupport", 1, fc_bpchar_sortsupport),
    b(3333, "btbpchar_pattern_sortsupport", 1, fc_btbpchar_pattern_sortsupport),
];
