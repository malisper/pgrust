//! `citext.c`'s comparison/hash/aggregate functions, dispatched through the
//! `dfmgr` builtin-library registry (citext's SQL install script resolves
//! `$libdir/citext` there — no OS loader exists to dlopen a real `.so`).

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "citext";

fn fc_citext_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_i32(crate::citextcmp(
        a.data(),
        b.data(),
        fcinfo.get_collation(),
    )?))
}

fn fc_citext_pattern_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_i32(crate::citext_pattern_cmp(a.data(), b.data())?))
}

fn fc_citext_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null citext varlena (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_u32(crate::citext_hash(a.data())?))
}

fn fc_citext_hash_extended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null citext varlena (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(crate::citext_hash_extended(
        a.data(),
        seed.value.as_u64(),
    )?))
}

macro_rules! fc_citext_bool_op {
    ($($fname:ident: $pred:expr;)*) => {$(
        fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
            let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
            let r = crate::citextcmp(a.data(), b.data(), fcinfo.get_collation())?;
            Ok(Datum::from_bool(($pred)(r)))
        }
    )*};
}

fc_citext_bool_op! {
    fc_citext_lt: |r: i32| r < 0;
    fc_citext_le: |r: i32| r <= 0;
    fc_citext_gt: |r: i32| r > 0;
    fc_citext_ge: |r: i32| r >= 0;
}

fn fc_citext_eq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_bool(crate::citext_eq(a.data(), b.data())?))
}

fn fc_citext_ne(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok(Datum::from_bool(!crate::citext_eq(a.data(), b.data())?))
}

macro_rules! fc_citext_pattern_bool_op {
    ($($fname:ident: $pred:expr;)*) => {$(
        fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
            let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
            let r = crate::citext_pattern_cmp(a.data(), b.data())?;
            Ok(Datum::from_bool(($pred)(r)))
        }
    )*};
}

fc_citext_pattern_bool_op! {
    fc_citext_pattern_lt: |r: i32| r < 0;
    fc_citext_pattern_le: |r: i32| r <= 0;
    fc_citext_pattern_gt: |r: i32| r > 0;
    fc_citext_pattern_ge: |r: i32| r >= 0;
}

// `citext_smaller`/`citext_larger` (citext.c): return whichever operand sorts
// smaller/larger, verbatim (the winning input's own bytes, not re-lowered).
fn citext_minmax(fcinfo: &mut Fcinfo, want_smaller: bool) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null citext/text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let (a, b) = (a.data(), b.data());
    let cmp = crate::citextcmp(a, b, fcinfo.get_collation())?;
    let winner = if (want_smaller && cmp < 0) || (!want_smaller && cmp > 0) {
        a
    } else {
        b
    };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, winner)?))
}

fn fc_citext_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    citext_minmax(fcinfo, true)
}

fn fc_citext_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    citext_minmax(fcinfo, false)
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "citext_eq" => fc_citext_eq,
        "citext_ne" => fc_citext_ne,
        "citext_lt" => fc_citext_lt,
        "citext_le" => fc_citext_le,
        "citext_gt" => fc_citext_gt,
        "citext_ge" => fc_citext_ge,
        "citext_cmp" => fc_citext_cmp,
        "citext_hash" => fc_citext_hash,
        "citext_hash_extended" => fc_citext_hash_extended,
        "citext_pattern_lt" => fc_citext_pattern_lt,
        "citext_pattern_le" => fc_citext_pattern_le,
        "citext_pattern_gt" => fc_citext_pattern_gt,
        "citext_pattern_ge" => fc_citext_pattern_ge,
        "citext_pattern_cmp" => fc_citext_pattern_cmp,
        "citext_smaller" => fc_citext_smaller,
        "citext_larger" => fc_citext_larger,
        _ => return None,
    })
}

/// Install this unit's inward seam: register the `citext` module with the
/// dynamic-loader's builtin-library registry.
pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // citext.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
