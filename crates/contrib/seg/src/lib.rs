//! contrib/seg — line segments / floating-point intervals with input-time
//! precision tracking: flex/bison-equivalent parser, the significant-digits
//! display rules (`restore`), operators, and the gist_seg_ops opclass.
//!
//! Functions dispatch through the dfmgr builtin-library registry
//! (hstore/ltree precedent). SEG is a fixed 12-byte pass-by-reference type
//! whose byte layout matches C's struct exactly — see `repr`.

pub mod gist;
pub mod ops;
pub mod out;
pub mod parse;
pub mod repr;

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{
    byref_result, cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use repr::Seg;

const LIBRARY: &str = "seg";

// SAFETY wrapper: callers assert arg i is a non-null 12-byte SEG datum.
unsafe fn arg_seg(fcinfo: &Fcinfo, i: usize) -> Seg {
    // SAFETY: caller contract; SEG is fixed-length by-ref, never toasted.
    Seg::from_bytes(unsafe { fcinfo.arg_fixed(i, repr::SEG_SIZE) })
}

fn ret_seg(fcinfo: &Fcinfo, seg: &Seg) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &seg.to_bytes())
}

fn ret_null(fcinfo: &mut Fcinfo) -> Datum {
    fcinfo.isnull = true;
    Datum::null()
}

// ===========================================================================
// Input/output
// ===========================================================================

fn fc_seg_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    match parse::parse_seg(&s) {
        Ok(seg) => ret_seg(fcinfo, &seg),
        Err(e) => {
            // SAFETY: context, if set, rides per the ErrorSaveNode contract.
            let esc = unsafe { fcinfo.soft_error_context() };
            types_error::ereturn(esc, ret_null(fcinfo), *e)
        }
    }
}

fn fc_seg_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null seg arg.
    let seg = unsafe { arg_seg(fcinfo, 0) };
    let mut bytes = out::seg_out(&seg);
    bytes.push(0);
    let mcx = fcinfo.result_mcx();
    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, bytes.len())?;
    mcx::vec_append_bytes(&mut buf, &bytes)?;
    Ok(cstring_result(buf))
}

// ===========================================================================
// Accessors
// ===========================================================================

fn fc_seg_center(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null seg arg.
    let seg = unsafe { arg_seg(fcinfo, 0) };
    // C: ((float) lower + (float) upper) / 2.0 — float add, double divide,
    // float return.
    let center = ((seg.lower + seg.upper) as f64 / 2.0) as f32;
    Ok(Datum::from_f32(center))
}

fn fc_seg_lower(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null seg arg.
    let seg = unsafe { arg_seg(fcinfo, 0) };
    Ok(Datum::from_f32(seg.lower))
}

fn fc_seg_upper(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null seg arg.
    let seg = unsafe { arg_seg(fcinfo, 0) };
    Ok(Datum::from_f32(seg.upper))
}

fn fc_seg_size(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, non-null seg arg.
    let seg = unsafe { arg_seg(fcinfo, 0) };
    Ok(Datum::from_f32(ops::seg_size(&seg)))
}

// ===========================================================================
// Relations and comparisons
// ===========================================================================

macro_rules! seg_bool_fn {
    ($name:ident, $a:ident, $b:ident, $expr:expr) => {
        fn $name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: strict fns, non-null seg args.
            let $a = unsafe { arg_seg(fcinfo, 0) };
            let $b = unsafe { arg_seg(fcinfo, 1) };
            Ok(Datum::from_bool($expr))
        }
    };
}

seg_bool_fn!(fc_seg_contains, a, b, ops::seg_contains(&a, &b));
seg_bool_fn!(fc_seg_contained, a, b, ops::seg_contains(&b, &a));
seg_bool_fn!(fc_seg_overlap, a, b, ops::seg_overlap(&a, &b));
seg_bool_fn!(fc_seg_left, a, b, ops::seg_left(&a, &b));
seg_bool_fn!(fc_seg_over_left, a, b, ops::seg_over_left(&a, &b));
seg_bool_fn!(fc_seg_right, a, b, ops::seg_right(&a, &b));
seg_bool_fn!(fc_seg_over_right, a, b, ops::seg_over_right(&a, &b));

fn cmp_args(fcinfo: &Fcinfo) -> PgResult<i32> {
    // SAFETY: strict fns, non-null seg args.
    let a = unsafe { arg_seg(fcinfo, 0) };
    let b = unsafe { arg_seg(fcinfo, 1) };
    ops::seg_cmp(&a, &b)
}

fn fc_seg_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(cmp_args(fcinfo)?))
}
fn fc_seg_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? == 0))
}
fn fc_seg_different(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? != 0))
}
fn fc_seg_lt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? < 0))
}
fn fc_seg_le(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? <= 0))
}
fn fc_seg_gt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? > 0))
}
fn fc_seg_ge(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? >= 0))
}

// ===========================================================================
// Set operations
// ===========================================================================

fn fc_seg_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null seg args.
    let a = unsafe { arg_seg(fcinfo, 0) };
    let b = unsafe { arg_seg(fcinfo, 1) };
    ret_seg(fcinfo, &ops::seg_union(&a, &b))
}

fn fc_seg_inter(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fns, non-null seg args.
    let a = unsafe { arg_seg(fcinfo, 0) };
    let b = unsafe { arg_seg(fcinfo, 1) };
    ret_seg(fcinfo, &ops::seg_inter(&a, &b))
}

// ===========================================================================
// dfmgr dispatch
// ===========================================================================

fn lookup(name: &str) -> Option<PGFunction> {
    Some(match name {
        "seg_in" => fc_seg_in,
        "seg_out" => fc_seg_out,
        "seg_center" => fc_seg_center,
        "seg_lower" => fc_seg_lower,
        "seg_upper" => fc_seg_upper,
        "seg_size" => fc_seg_size,
        "seg_contains" => fc_seg_contains,
        "seg_contained" => fc_seg_contained,
        "seg_overlap" => fc_seg_overlap,
        "seg_left" => fc_seg_left,
        "seg_over_left" => fc_seg_over_left,
        "seg_right" => fc_seg_right,
        "seg_over_right" => fc_seg_over_right,
        "seg_cmp" => fc_seg_cmp,
        "seg_same" => fc_seg_same,
        "seg_different" => fc_seg_different,
        "seg_lt" => fc_seg_lt,
        "seg_le" => fc_seg_le,
        "seg_gt" => fc_seg_gt,
        "seg_ge" => fc_seg_ge,
        "seg_union" => fc_seg_union,
        "seg_inter" => fc_seg_inter,
        "gseg_consistent" => gist::fc_gseg_consistent,
        "gseg_compress" => gist::fc_gseg_compress,
        "gseg_decompress" => gist::fc_gseg_decompress,
        "gseg_penalty" => gist::fc_gseg_penalty,
        "gseg_picksplit" => gist::fc_gseg_picksplit,
        "gseg_union" => gist::fc_gseg_union,
        "gseg_same" => gist::fc_gseg_same,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
