//! fmgr wrappers (`fc_*`) + `LIKE_BUILTINS` for fmgr-core. like_support.c's
//! prosupport rows answer NULL except the index-condition leg (loud).

use datum::varlena::{set_varsize_4b, VARHDRSZ};
use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::IcScratch;

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: result/scratch needs a resolved FmgrInfo; direct callers use the value core")
}

macro_rules! fc_like {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null text/bpchar varlenas (strict fn).
            let (s, p) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::from_bool(crate::$core(s.data(), p.data(), fcinfo.get_collation())?))
        }
    )*};
}

fc_like! {
    fc_textlike: textlike;
    fc_textnlike: textnlike;
}

macro_rules! fc_namelike {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are a non-null name block and text varlena (strict fn).
            let (s, p) = unsafe { (fcinfo.arg_name(0), fcinfo.arg_varlena_packed(1)?) };
            Ok(Datum::from_bool(crate::$core(s, p.data(), fcinfo.get_collation())?))
        }
    )*};
}

fc_namelike! {
    fc_namelike: namelike;
    fc_namenlike: namenlike;
}

macro_rules! fc_bytealike {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null bytea varlenas (strict fn).
            let (s, p) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            Ok(Datum::from_bool(crate::$core(s.data(), p.data())?))
        }
    )*};
}

fc_bytealike! {
    fc_bytealike: bytealike;
    fc_byteanlike: byteanlike;
}

fn ic_scratch<'a>(flinfo: Option<&'a mut FmgrInfo>, name: &'static str) -> &'a mut IcScratch {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(IcScratch::default());
    }
    flinfo.fn_extra_mut::<IcScratch>().unwrap()
}

macro_rules! arg_bytes {
    (arg_varlena_packed, $v:ident) => {
        $v?.data()
    };
    (arg_name, $v:ident) => {
        $v
    };
}

macro_rules! fc_iclike {
    ($($fname:ident: $core:ident / $arg0:ident;)*) => {$(
        pub fn $fname(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let scratch = ic_scratch(flinfo, stringify!($core));
            // SAFETY: catalog args are non-null per the row's types (strict fn).
            let (s, p) = unsafe { (fcinfo.$arg0(0), fcinfo.arg_varlena_packed(1)?) };
            Ok(Datum::from_bool(crate::$core(
                fcinfo.result_mcx(),
                arg_bytes!($arg0, s),
                p.data(),
                fcinfo.get_collation(),
                scratch,
            )?))
        }
    )*};
}

fc_iclike! {
    fc_texticlike: texticlike / arg_varlena_packed;
    fc_texticnlike: texticnlike / arg_varlena_packed;
    fc_nameiclike: nameiclike / arg_name;
    fc_nameicnlike: nameicnlike / arg_name;
}

// Result varlena lives in the resolved FmgrInfo's retained scratch (the
// varlena textin precedent; C pallocs per call).
struct OutBuf(Vec<u8>);

fn escape_out(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    bytea: bool,
) -> PgResult<Datum> {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    // SAFETY: catalog args are non-null text/bytea varlenas (strict fn).
    let (pat, esc) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    buf.extend_from_slice(&[0; VARHDRSZ]);
    if bytea {
        crate::like_escape_bytea_into(pat.data(), esc.data(), buf)?;
    } else {
        crate::like_escape_into(pat.data(), esc.data(), buf)?;
    }
    let total = buf.len();
    buf[..VARHDRSZ].copy_from_slice(&set_varsize_4b(total));
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_like_escape(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    escape_out(flinfo, fcinfo, "like_escape", false)
}

pub fn fc_like_escape_bytea(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    escape_out(flinfo, fcinfo, "like_escape_bytea", true)
}

// like_regex_support (like_support.c) handles only Selectivity and
// IndexCondition; the planner's closed-set dispatch owns both legs
// (plancat::function_selectivity, indxpath::get_index_clause_from_support),
// so an fmgr arrival of either is a bug. Every other tag is C's NULL return.
macro_rules! fc_like_support {
    ($($fname:ident: $cname:literal;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a] = fcinfo.args_n::<1>();
            let p = a.value.as_usize() as *const types_nodes::NodeTag;
            // SAFETY: prosupport contract — the internal arg points at a live
            // tag-first support-request node.
            let tag = unsafe { *p };
            match tag {
                types_nodes::NodeTag::T_SupportRequestSelectivity
                | types_nodes::NodeTag::T_SupportRequestIndexCondition => panic!(concat!(
                    $cname,
                    ": Selectivity/IndexCondition must ride the planner closed set"
                )),
                _ => Ok(Datum::from_usize(0)),
            }
        }
    )*};
}

fc_like_support! {
    fc_textlike_support: "textlike_support";
    fc_texticlike_support: "texticlike_support";
    fc_textregexeq_support: "textregexeq_support";
    fc_texticregexeq_support: "texticregexeq_support";
    fc_text_starts_with_support: "text_starts_with_support";
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

// pg_proc.dat rows (all proisstrict, none retset); 1569-1572/2007-2009 are the
// like()/notlike() aliases, 1631/1632/1660/1661 the bpchar rows.
pub const LIKE_BUILTINS: &[FmgrBuiltin] = &[
    b(850, "textlike", 2, fc_textlike),
    b(851, "textnlike", 2, fc_textnlike),
    b(858, "namelike", 2, fc_namelike),
    b(859, "namenlike", 2, fc_namenlike),
    b(1023, "textlike_support", 1, fc_textlike_support),
    b(1024, "texticregexeq_support", 1, fc_texticregexeq_support),
    b(1025, "texticlike_support", 1, fc_texticlike_support),
    b(1364, "textregexeq_support", 1, fc_textregexeq_support),
    b(1569, "like", 2, fc_textlike),
    b(1570, "notlike", 2, fc_textnlike),
    b(1571, "like", 2, fc_namelike),
    b(1572, "notlike", 2, fc_namenlike),
    b(1631, "bpcharlike", 2, fc_textlike),
    b(1632, "bpcharnlike", 2, fc_textnlike),
    b(1633, "texticlike", 2, fc_texticlike),
    b(1634, "texticnlike", 2, fc_texticnlike),
    b(1635, "nameiclike", 2, fc_nameiclike),
    b(1636, "nameicnlike", 2, fc_nameicnlike),
    b(1637, "like_escape", 2, fc_like_escape),
    b(1660, "bpchariclike", 2, fc_texticlike),
    b(1661, "bpcharicnlike", 2, fc_texticnlike),
    b(2005, "bytealike", 2, fc_bytealike),
    b(2006, "byteanlike", 2, fc_byteanlike),
    b(2007, "like", 2, fc_bytealike),
    b(2008, "notlike", 2, fc_byteanlike),
    b(2009, "like_escape", 2, fc_like_escape_bytea),
    b(6242, "text_starts_with_support", 1, fc_text_starts_with_support),
];
