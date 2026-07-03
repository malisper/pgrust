//! fmgr wrappers (`fc_*`) + the `VARLENA_BUILTINS` table for fmgr-core.
//! Still deferred: *recv/*send (pqformat wire frame is a separate unit) and
//! bttextsortsupport (SortSupport substrate); value cores live in the crate root.

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
                (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1))
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

macro_rules! fc_byteacmp {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null bytea varlenas (strict fn).
            let (a, b) = unsafe {
                (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1))
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
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
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
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
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
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    Ok(if crate::bytea::byteacmp(a.data(), b.data()) > 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

pub fn fc_bytea_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
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
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
    let buf = out_scratch(flinfo, "textout");
    buf.clear();
    buf.reserve(payload.len() + 1);
    buf.extend_from_slice(payload);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_byteaout(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
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
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_catenate(mcx, a.data(), b.data())?))
}

pub fn fc_byteacat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::bytea::bytea_catenate(
        mcx,
        a.data(),
        b.data(),
    )?))
}

#[cold]
#[inline(never)]
fn soft_context_unported(name: &str) -> ! {
    panic!("{name}: fcinfo.context soft-error demux is fmgr-core's unit (not ported)")
}

pub fn fc_byteain(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.context.is_some() {
        soft_context_unported("byteain");
    }
    // SAFETY: catalog arg 0 of byteain is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    let v = crate::bytea::byteain(mcx, s, None)?
        .expect("byteain: soft-error escape without an escontext");
    Ok(varlena_result(v))
}

pub fn fc_unknownin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of unknownin is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::unknownin(mcx, s)?))
}

pub fn fc_textlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
    Ok(Datum::from_i32(crate::text_length(payload)))
}

pub fn fc_textoctetlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
    Ok(Datum::from_i32(crate::textoctetlen(payload)))
}

pub fn fc_byteaoctetlen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
    Ok(Datum::from_i32(crate::bytea::byteaoctetlen(payload)))
}

pub fn fc_btvarstrequalimage(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(crate::btvarstrequalimage(
        fcinfo.get_collation(),
    )?))
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

// pg_proc.dat rows (all proisstrict, none retset); 1317/1369/1381 = textlen aliases.
pub const VARLENA_BUILTINS: &[FmgrBuiltin] = &[
    b(31, "byteaout", 1, fc_byteaout),
    b(46, "textin", 1, fc_textin),
    b(47, "textout", 1, fc_textout),
    b(67, "texteq", 2, fc_texteq),
    b(109, "unknownin", 1, fc_unknownin),
    b(110, "unknownout", 1, fc_unknownout),
    b(157, "textne", 2, fc_textne),
    b(360, "bttextcmp", 2, fc_bttextcmp),
    b(458, "text_larger", 2, fc_text_larger),
    b(459, "text_smaller", 2, fc_text_smaller),
    b(720, "byteaoctetlen", 1, fc_byteaoctetlen),
    b(740, "text_lt", 2, fc_text_lt),
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
    b(5050, "btvarstrequalimage", 1, fc_btvarstrequalimage),
    b(6393, "bytea_larger", 2, fc_bytea_larger),
    b(6394, "bytea_smaller", 2, fc_bytea_smaller),
];
