//! rangetypes_gist.c: GiST support procs for range types (range_ops) and the
//! multirange_ops GiST arms (multirange_gist_consistent/compress share the
//! range key machinery — a multirange is approximated by its union range).
#![allow(non_upper_case_globals)]

mod qsort;
#[cfg(test)]
mod tests;

use ::adt_multirangetypes::{leak_image, multirange_get_bounds, multirange_is_empty};
use ::adt_rangetypes::ops;
use ::adt_rangetypes::{
    make_range, range_bound_slots, range_cmp_bounds, range_deserialize_into,
    range_get_flags, range_is_empty,
    range_type_oid, range_types_do_not_match, RangeBound, RangeInfo, RANGE_CONTAIN_EMPTY,
    RANGE_EMPTY, RANGE_LB_INF, RANGE_UB_INF,
};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{ANYMULTIRANGEOID, ANYRANGEOID, InvalidOid, Oid};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{
    function_call2_coll_in, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};
use ::types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};

pub use qsort::pg_qsort_arg;

// stratnum.h RANGESTRAT_* (via RT*StrategyNumber).
pub const RANGESTRAT_BEFORE: u16 = 1;
pub const RANGESTRAT_OVERLEFT: u16 = 2;
pub const RANGESTRAT_OVERLAPS: u16 = 3;
pub const RANGESTRAT_OVERRIGHT: u16 = 4;
pub const RANGESTRAT_AFTER: u16 = 5;
pub const RANGESTRAT_ADJACENT: u16 = 6;
pub const RANGESTRAT_CONTAINS: u16 = 7;
pub const RANGESTRAT_CONTAINED_BY: u16 = 8;
pub const RANGESTRAT_CONTAINS_ELEM: u16 = 16;
pub const RANGESTRAT_EQ: u16 = 18;

const CLS_NORMAL: usize = 0;
const CLS_LOWER_INF: usize = 1;
const CLS_UPPER_INF: usize = 2;
const CLS_CONTAIN_EMPTY: usize = 4;
const CLS_EMPTY: usize = 8;
const CLS_COUNT: usize = 9;

const LIMIT_RATIO: f32 = 0.3;

const INFINITE_BOUND_PENALTY: f32 = 2.0;
const CONTAIN_EMPTY_PENALTY: f32 = 1.0;
const DEFAULT_SUBTYPE_DIFF_PENALTY: f64 = 1.0;

// range_get_typcache's fn_extra memo, widened with the subtype_diff finfo the
// gist penalty/picksplit paths call (RangeInfo alone doesn't carry it).
pub struct RangeGistCache {
    pub ri: RangeInfo,
    pub subdiff: Option<FmgrInfo>,
}

impl RangeGistCache {
    fn lookup(rngtypid: Oid) -> PgResult<RangeGistCache> {
        let e = ::typcache::lookup_type_cache(rngtypid, ::typcache::TYPECACHE_RANGE_INFO)?;
        let subdiff = {
            let f = e.rng_subdiff_finfo();
            (f.fn_oid != InvalidOid).then(|| f.clone())
        };
        Ok(RangeGistCache { ri: RangeInfo::from_entry(e)?, subdiff })
    }
}

pub fn cached_gist_range_cache<'f>(
    flinfo: Option<&'f mut FmgrInfo>,
    rngtypid: Oid,
) -> PgResult<&'f mut RangeGistCache> {
    let flinfo = flinfo.expect("range gist support proc: NULL flinfo (fn_extra typcache memo)");
    let need = match flinfo.fn_extra_ref::<RangeGistCache>() {
        Some(c) => c.ri.rngtypid != rngtypid,
        None => true,
    };
    if need {
        flinfo.set_fn_extra(RangeGistCache::lookup(rngtypid)?);
    }
    Ok(flinfo.fn_extra_mut::<RangeGistCache>().unwrap())
}

/// DatumGetRangeTypeP: 4-byte-header image of a possibly short/compressed
/// range datum; borrows in place when already 4B-uncompressed.
///
/// # Safety
/// `d` must point at a live varlena readable for its full VARSIZE_ANY, and
/// outlive `'m` when borrowed (gist/spgist entry keys live in the caller's
/// temp context that also backs `mcx`).
pub unsafe fn varlena_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    if raw[0] & 0x03 == 0 {
        Ok(raw)
    } else {
        Ok(leak_image(::detoast_seams::detoast_attr::call(mcx, raw)?))
    }
}

fn image_datum(img: &[u8]) -> Datum {
    Datum::from_usize(img.as_ptr() as usize)
}

fn range_is_or_contains_empty(r: &[u8]) -> bool {
    range_get_flags(r) & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY) != 0
}

fn set_contain_empty_copy<'m>(mcx: Mcx<'m>, r: &[u8]) -> PgResult<&'m [u8]> {
    let mut v: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, r.len())?;
    v.extend_from_slice(r);
    let last = v.len() - 1;
    v[last] |= RANGE_CONTAIN_EMPTY;
    Ok(leak_image(v))
}

fn call_subtype_diff(
    mcx: Mcx<'_>,
    cache: &mut RangeGistCache,
    val1: Datum,
    val2: Datum,
) -> PgResult<f64> {
    let f = cache.subdiff.as_mut().expect("caller checked has_subtype_diff");
    let value = function_call2_coll_in(f, cache.ri.collation, mcx, val1, val2)?.as_f64();
    // C: buggy subtype_diff results (negative or NaN) read as zero.
    Ok(if value >= 0.0 { value } else { 0.0 })
}

// range_super_union: smallest range containing both, absorbing gaps and
// tracking CONTAIN_EMPTY; returns an input image unchanged where C returns
// the input pointer.
fn range_super_union<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeGistCache,
    r1: &'m [u8],
    r2: &'m [u8],
) -> PgResult<&'m [u8]> {
    let ri = &mut cache.ri;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    let flags1 = range_get_flags(r1);
    let flags2 = range_get_flags(r2);

    if empty1 {
        if flags2 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY) != 0 {
            return Ok(r2);
        }
        return set_contain_empty_copy(mcx, r2);
    }
    if empty2 {
        if flags1 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY) != 0 {
            return Ok(r1);
        }
        return set_contain_empty_copy(mcx, r1);
    }

    let lower_is_1 = range_cmp_bounds(mcx, ri, &lower1, &lower2)? <= 0;
    let upper_is_1 = range_cmp_bounds(mcx, ri, &upper1, &upper2)? >= 0;

    if lower_is_1
        && upper_is_1
        && ((flags1 & RANGE_CONTAIN_EMPTY != 0) || (flags2 & RANGE_CONTAIN_EMPTY == 0))
    {
        return Ok(r1);
    }
    if !lower_is_1
        && !upper_is_1
        && ((flags2 & RANGE_CONTAIN_EMPTY != 0) || (flags1 & RANGE_CONTAIN_EMPTY == 0))
    {
        return Ok(r2);
    }

    let mut result_lower = if lower_is_1 { lower1 } else { lower2 };
    let mut result_upper = if upper_is_1 { upper1 } else { upper2 };
    let mut img = make_range(mcx, ri, &mut result_lower, &mut result_upper, false, None)?
        .expect("hard error path returns Some");
    if (flags1 | flags2) & RANGE_CONTAIN_EMPTY != 0 {
        let last = img.len() - 1;
        img[last] |= RANGE_CONTAIN_EMPTY;
    }
    Ok(leak_image(img))
}

fn multirange_union_range_equal(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(range_is_empty(r) && multirange_is_empty(mr));
    }
    let (mut lower1, mut upper1) = range_bound_slots();
    let _empty = range_deserialize_into(&ri.elem, r, &mut lower1, &mut upper1);
    let (lower2, _t1) = multirange_get_bounds(ri, mr, 0);
    let (_t2, upper2) =
        multirange_get_bounds(ri, mr, ::adt_multirangetypes::multirange_count(mr) as usize - 1);
    Ok(range_cmp_bounds(mcx, ri, &lower1, &lower2)? == 0
        && range_cmp_bounds(mcx, ri, &upper1, &upper2)? == 0)
}

#[track_caller]
#[cold]
fn unrecognized_range_strategy(strategy: u16) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized range strategy: {strategy}")))
}

fn consistent_int_range(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: &[u8],
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!ops::range_overright_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_OVERLEFT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!ops::range_after_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_OVERLAPS => ops::range_overlaps_internal(mcx, ri, key, query),
        RANGESTRAT_OVERRIGHT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!ops::range_before_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_AFTER => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!ops::range_overleft_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_ADJACENT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            if ops::range_adjacent_internal(mcx, ri, key, query)? {
                return Ok(true);
            }
            ops::range_overlaps_internal(mcx, ri, key, query)
        }
        RANGESTRAT_CONTAINS => ops::range_contains_internal(mcx, ri, key, query),
        RANGESTRAT_CONTAINED_BY => {
            if range_is_or_contains_empty(key) {
                return Ok(true);
            }
            ops::range_overlaps_internal(mcx, ri, key, query)
        }
        RANGESTRAT_EQ => {
            if range_is_empty(query) {
                return Ok(range_is_or_contains_empty(key));
            }
            ops::range_contains_internal(mcx, ri, key, query)
        }
        other => Err(unrecognized_range_strategy(other)),
    }
}

fn consistent_int_multirange(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: &[u8],
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!::adt_multirangetypes::range_overright_multirange_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_OVERLEFT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!::adt_multirangetypes::range_after_multirange_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_OVERLAPS => {
            ::adt_multirangetypes::range_overlaps_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_OVERRIGHT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!::adt_multirangetypes::range_before_multirange_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_AFTER => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!::adt_multirangetypes::range_overleft_multirange_internal(mcx, ri, key, query)?)
        }
        RANGESTRAT_ADJACENT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            if ::adt_multirangetypes::range_adjacent_multirange_internal(mcx, ri, key, query)? {
                return Ok(true);
            }
            ::adt_multirangetypes::range_overlaps_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_CONTAINS => {
            ::adt_multirangetypes::range_contains_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_CONTAINED_BY => {
            if range_is_or_contains_empty(key) {
                return Ok(true);
            }
            ::adt_multirangetypes::range_overlaps_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_EQ => {
            if multirange_is_empty(query) {
                return Ok(range_is_or_contains_empty(key));
            }
            ::adt_multirangetypes::range_contains_multirange_internal(mcx, ri, key, query)
        }
        other => Err(unrecognized_range_strategy(other)),
    }
}

fn consistent_int_element(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: Datum,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => ops::range_contains_elem_internal(mcx, ri, key, query),
        other => Err(unrecognized_range_strategy(other)),
    }
}

fn consistent_leaf_range(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: &[u8],
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => ops::range_before_internal(mcx, ri, key, query),
        RANGESTRAT_OVERLEFT => ops::range_overleft_internal(mcx, ri, key, query),
        RANGESTRAT_OVERLAPS => ops::range_overlaps_internal(mcx, ri, key, query),
        RANGESTRAT_OVERRIGHT => ops::range_overright_internal(mcx, ri, key, query),
        RANGESTRAT_AFTER => ops::range_after_internal(mcx, ri, key, query),
        RANGESTRAT_ADJACENT => ops::range_adjacent_internal(mcx, ri, key, query),
        RANGESTRAT_CONTAINS => ops::range_contains_internal(mcx, ri, key, query),
        RANGESTRAT_CONTAINED_BY => ops::range_contained_by_internal(mcx, ri, key, query),
        RANGESTRAT_EQ => ops::range_eq_internal(mcx, ri, key, query),
        other => Err(unrecognized_range_strategy(other)),
    }
}

fn consistent_leaf_multirange(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: &[u8],
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => {
            ::adt_multirangetypes::range_before_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_OVERLEFT => {
            ::adt_multirangetypes::range_overleft_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_OVERLAPS => {
            ::adt_multirangetypes::range_overlaps_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_OVERRIGHT => {
            ::adt_multirangetypes::range_overright_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_AFTER => {
            ::adt_multirangetypes::range_after_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_ADJACENT => {
            ::adt_multirangetypes::range_adjacent_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_CONTAINS => {
            ::adt_multirangetypes::range_contains_multirange_internal(mcx, ri, key, query)
        }
        RANGESTRAT_CONTAINED_BY => {
            ::adt_multirangetypes::multirange_contains_range_internal(mcx, ri, query, key)
        }
        RANGESTRAT_EQ => multirange_union_range_equal(mcx, ri, key, query),
        other => Err(unrecognized_range_strategy(other)),
    }
}

fn consistent_leaf_element(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    strategy: u16,
    key: &[u8],
    query: Datum,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => ops::range_contains_elem_internal(mcx, ri, key, query),
        other => Err(unrecognized_range_strategy(other)),
    }
}

// SAFETY helpers over the gist fmgr protocol: pointer args are live for the
// call (GistState frames own them).
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn fc_range_gist_consistent(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol (module contract).
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let query = fcinfo.arg(1);
    let strategy = fcinfo.arg(2).as_u16();
    let subtype = fcinfo.arg_oid(3);
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    let mcx = fcinfo.result_mcx();
    // SAFETY: recheck out-param is a live &mut bool in the caller frame.
    unsafe { *recheck = false };

    // SAFETY: entry key is a live range varlena in the scan's temp context.
    let key = unsafe { varlena_image(mcx, entry.key) }?;
    let cache = cached_gist_range_cache(f, range_type_oid(key))?;
    let ri = &mut cache.ri;

    let result = if entry.page_is_leaf {
        if subtype == InvalidOid || subtype == ANYRANGEOID {
            // SAFETY: range query arg per opclass amop signature.
            consistent_leaf_range(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
        } else if subtype == ANYMULTIRANGEOID {
            // SAFETY: multirange query arg per opclass amop signature.
            consistent_leaf_multirange(mcx, ri, strategy, key, unsafe {
                varlena_image(mcx, query)
            }?)?
        } else {
            consistent_leaf_element(mcx, ri, strategy, key, query)?
        }
    } else if subtype == InvalidOid || subtype == ANYRANGEOID {
        // SAFETY: as above.
        consistent_int_range(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
    } else if subtype == ANYMULTIRANGEOID {
        // SAFETY: as above.
        consistent_int_multirange(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
    } else {
        consistent_int_element(mcx, ri, strategy, key, query)?
    };
    Ok(Datum::from_bool(result))
}

fn fc_multirange_gist_consistent(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let query = fcinfo.arg(1);
    let strategy = fcinfo.arg(2).as_u16();
    let subtype = fcinfo.arg_oid(3);
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    let mcx = fcinfo.result_mcx();
    // The union-range key approximates the multirange with no gaps: every
    // operator served here is inexact.
    // SAFETY: recheck out-param is a live &mut bool in the caller frame.
    unsafe { *recheck = true };

    // SAFETY: the stored key is the compressed union *range*.
    let key = unsafe { varlena_image(mcx, entry.key) }?;
    let cache = cached_gist_range_cache(f, range_type_oid(key))?;
    let ri = &mut cache.ri;

    let result = if entry.page_is_leaf {
        if subtype == InvalidOid || subtype == ANYMULTIRANGEOID {
            // SAFETY: multirange query arg per opclass amop signature.
            consistent_leaf_multirange(mcx, ri, strategy, key, unsafe {
                varlena_image(mcx, query)
            }?)?
        } else if subtype == ANYRANGEOID {
            // SAFETY: range query arg per opclass amop signature.
            consistent_leaf_range(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
        } else {
            consistent_leaf_element(mcx, ri, strategy, key, query)?
        }
    } else if subtype == InvalidOid || subtype == ANYMULTIRANGEOID {
        // SAFETY: as above.
        consistent_int_multirange(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
    } else if subtype == ANYRANGEOID {
        // SAFETY: as above.
        consistent_int_range(mcx, ri, strategy, key, unsafe { varlena_image(mcx, query) }?)?
    } else {
        consistent_int_element(mcx, ri, strategy, key, query)?
    };
    Ok(Datum::from_bool(result))
}

fn fc_multirange_gist_compress(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    let mcx = fcinfo.result_mcx();
    // SAFETY: leaf key is a live multirange varlena.
    let mr = unsafe { varlena_image(mcx, entry.key) }?;
    let mi = ::adt_multirangetypes::cached_multirange_info(
        f.expect("multirange_gist_compress: NULL flinfo"),
        ::adt_multirangetypes::multirange_type_oid(mr),
    )?;
    let r = ::adt_multirangetypes::multirange_get_union_range(mcx, &mut mi.rng, mr)?;
    let retval = GISTENTRY {
        key: image_datum(leak_image(r)),
        offset: entry.offset,
        leafkey: false,
        page_is_leaf: entry.page_is_leaf,
        rel_natts: 0,
    };
    // SAFETY: GISTENTRY is Copy/no-drop; byref image copy.
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (&retval as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    ::types_fmgr::byref_result(mcx, bytes)
}

fn fc_range_gist_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let mcx = fcinfo.result_mcx();

    // SAFETY: union keys are live range varlenas.
    let mut result = unsafe { varlena_image(mcx, entryvec.vector[0].key) }?;
    let cache = cached_gist_range_cache(f, range_type_oid(result))?;
    for i in 1..entryvec.n as usize {
        // SAFETY: as above.
        let r = unsafe { varlena_image(mcx, entryvec.vector[i].key) }?;
        result = range_super_union(mcx, cache, result, r)?;
    }
    Ok(image_datum(result))
}

fn fc_range_gist_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    let mcx = fcinfo.result_mcx();

    // SAFETY: entry keys are live range varlenas.
    let orig = unsafe { varlena_image(mcx, origentry.key) }?;
    // SAFETY: as above.
    let new = unsafe { varlena_image(mcx, newentry.key) }?;
    if range_type_oid(orig) != range_type_oid(new) {
        return Err(range_types_do_not_match());
    }
    let cache = cached_gist_range_cache(f, range_type_oid(orig))?;
    let has_subtype_diff = cache.subdiff.is_some();

    let (mut orig_lower, mut orig_upper) = range_bound_slots();

    let orig_empty = range_deserialize_into(&cache.ri.elem, orig, &mut orig_lower, &mut orig_upper);
    let (mut new_lower, mut new_upper) = range_bound_slots();
    let new_empty = range_deserialize_into(&cache.ri.elem, new, &mut new_lower, &mut new_upper);

    let p: f32 = if new_empty {
        if orig_empty {
            0.0
        } else if range_is_or_contains_empty(orig) {
            CONTAIN_EMPTY_PENALTY
        } else if orig_lower.infinite && orig_upper.infinite {
            2.0 * CONTAIN_EMPTY_PENALTY
        } else if orig_lower.infinite || orig_upper.infinite {
            3.0 * CONTAIN_EMPTY_PENALTY
        } else {
            4.0 * CONTAIN_EMPTY_PENALTY
        }
    } else if new_lower.infinite && new_upper.infinite {
        let mut p = if orig_lower.infinite && orig_upper.infinite {
            0.0
        } else if orig_lower.infinite || orig_upper.infinite {
            INFINITE_BOUND_PENALTY
        } else {
            2.0 * INFINITE_BOUND_PENALTY
        };
        if range_is_or_contains_empty(orig) {
            p += CONTAIN_EMPTY_PENALTY;
        }
        p
    } else if new_lower.infinite {
        if !orig_empty && orig_lower.infinite {
            if orig_upper.infinite {
                0.0
            } else if range_cmp_bounds(mcx, &mut cache.ri, &new_upper, &orig_upper)? > 0 {
                if has_subtype_diff {
                    call_subtype_diff(mcx, cache, new_upper.val, orig_upper.val)? as f32
                } else {
                    DEFAULT_SUBTYPE_DIFF_PENALTY as f32
                }
            } else {
                0.0
            }
        } else {
            f32::INFINITY
        }
    } else if new_upper.infinite {
        if !orig_empty && orig_upper.infinite {
            if orig_lower.infinite {
                0.0
            } else if range_cmp_bounds(mcx, &mut cache.ri, &new_lower, &orig_lower)? < 0 {
                if has_subtype_diff {
                    call_subtype_diff(mcx, cache, orig_lower.val, new_lower.val)? as f32
                } else {
                    DEFAULT_SUBTYPE_DIFF_PENALTY as f32
                }
            } else {
                0.0
            }
        } else {
            f32::INFINITY
        }
    } else if orig_empty || orig_lower.infinite || orig_upper.infinite {
        f32::INFINITY
    } else {
        let mut diff: f64 = 0.0;
        if range_cmp_bounds(mcx, &mut cache.ri, &new_lower, &orig_lower)? < 0 {
            diff += if has_subtype_diff {
                call_subtype_diff(mcx, cache, orig_lower.val, new_lower.val)?
            } else {
                DEFAULT_SUBTYPE_DIFF_PENALTY
            };
        }
        if range_cmp_bounds(mcx, &mut cache.ri, &new_upper, &orig_upper)? > 0 {
            diff += if has_subtype_diff {
                call_subtype_diff(mcx, cache, new_upper.val, orig_upper.val)?
            } else {
                DEFAULT_SUBTYPE_DIFF_PENALTY
            };
        }
        diff as f32
    };

    // SAFETY: penalty out-param is a live &mut f32 in the caller frame.
    unsafe { *penalty = p };
    Ok(fcinfo.arg(2))
}

fn fc_range_gist_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: gist fmgr protocol; args are key datums.
    let r1 = unsafe { varlena_image(mcx, fcinfo.arg(0)) }?;
    // SAFETY: as above.
    let r2 = unsafe { varlena_image(mcx, fcinfo.arg(1)) }?;
    let result = fcinfo.arg(2).as_usize() as *mut bool;

    // Normalized entries with unequal flag bytes are unequal ranges; range_eq
    // ignores CONTAIN_EMPTY, so test all flag bits first.
    let same = if range_get_flags(r1) != range_get_flags(r2) {
        false
    } else {
        let cache = cached_gist_range_cache(f, range_type_oid(r1))?;
        ops::range_eq_internal(mcx, &mut cache.ri, r1, r2)?
    };
    // SAFETY: result out-param is a live &mut bool in the caller frame.
    unsafe { *result = same };
    Ok(fcinfo.arg(2))
}

// ---------------------------------------------------------------------------
// Picksplit
// ---------------------------------------------------------------------------

struct Placer<'m> {
    left: Option<&'m [u8]>,
    right: Option<&'m [u8]>,
}

impl<'m> Placer<'m> {
    fn place_left(
        &mut self,
        mcx: Mcx<'m>,
        cache: &mut RangeGistCache,
        v: &mut GistSplitVec,
        range: &'m [u8],
        off: usize,
    ) -> PgResult<()> {
        self.left = Some(match self.left {
            Some(l) if !v.spl_left.is_empty() => range_super_union(mcx, cache, l, range)?,
            _ => range,
        });
        v.spl_left.push(off as u16);
        Ok(())
    }

    fn place_right(
        &mut self,
        mcx: Mcx<'m>,
        cache: &mut RangeGistCache,
        v: &mut GistSplitVec,
        range: &'m [u8],
        off: usize,
    ) -> PgResult<()> {
        self.right = Some(match self.right {
            Some(r) if !v.spl_right.is_empty() => range_super_union(mcx, cache, r, range)?,
            _ => range,
        });
        v.spl_right.push(off as u16);
        Ok(())
    }

    fn finish(self, v: &mut GistSplitVec) {
        v.spl_ldatum = self.left.map_or(Datum::from_usize(0), image_datum);
        v.spl_rdatum = self.right.map_or(Datum::from_usize(0), image_datum);
    }
}

fn get_gist_range_class(range: &[u8]) -> usize {
    let flags = range_get_flags(range);
    if flags & RANGE_EMPTY != 0 {
        return CLS_EMPTY;
    }
    let mut class_number = 0;
    if flags & RANGE_LB_INF != 0 {
        class_number |= CLS_LOWER_INF;
    }
    if flags & RANGE_UB_INF != 0 {
        class_number |= CLS_UPPER_INF;
    }
    if flags & RANGE_CONTAIN_EMPTY != 0 {
        class_number |= CLS_CONTAIN_EMPTY;
    }
    class_number
}

fn entry_ranges<'m>(mcx: Mcx<'m>, entryvec: &GistEntryVector) -> PgResult<Vec<&'m [u8]>> {
    let maxoff = (entryvec.n - 1) as usize;
    let mut ranges = Vec::with_capacity(maxoff + 1);
    ranges.push(&[][..]);
    for i in 1..=maxoff {
        // SAFETY: gist fmgr protocol; picksplit keys are live range varlenas.
        ranges.push(unsafe { varlena_image(mcx, entryvec.vector[i].key) }?);
    }
    Ok(ranges)
}

fn fc_range_gist_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let mcx = fcinfo.result_mcx();

    let ranges = entry_ranges(mcx, entryvec)?;
    let maxoff = (entryvec.n - 1) as usize;
    let cache = cached_gist_range_cache(f, range_type_oid(ranges[1]))?;

    v.spl_left = Vec::with_capacity(maxoff + 1);
    v.spl_right = Vec::with_capacity(maxoff + 1);

    let mut count_in_classes = [0i32; CLS_COUNT];
    for r in &ranges[1..] {
        count_in_classes[get_gist_range_class(r)] += 1;
    }

    let total_count = maxoff as i32;
    let mut non_empty_classes_count = 0;
    let mut biggest_class = usize::MAX;
    let mut biggest_class_count = 0;
    for (j, &c) in count_in_classes.iter().enumerate() {
        if c > 0 {
            if c > biggest_class_count {
                biggest_class_count = c;
                biggest_class = j;
            }
            non_empty_classes_count += 1;
        }
    }
    debug_assert!(non_empty_classes_count > 0);

    if non_empty_classes_count == 1 {
        if biggest_class & !CLS_CONTAIN_EMPTY == CLS_NORMAL {
            double_sorting_split(mcx, cache, &ranges, v)?;
        } else if biggest_class & !CLS_CONTAIN_EMPTY == CLS_LOWER_INF {
            single_sorting_split(mcx, cache, &ranges, v, true)?;
        } else if biggest_class & !CLS_CONTAIN_EMPTY == CLS_UPPER_INF {
            single_sorting_split(mcx, cache, &ranges, v, false)?;
        } else {
            fallback_split(mcx, cache, &ranges, v)?;
        }
    } else {
        let mut classes_groups = [SplitLR::Left; CLS_COUNT];
        if count_in_classes[CLS_NORMAL] > 0 {
            classes_groups[CLS_NORMAL] = SplitLR::Right;
        } else {
            let non_inf_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_CONTAIN_EMPTY]
                + count_in_classes[CLS_EMPTY];
            let inf_count = total_count - non_inf_count;
            let non_empty_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_LOWER_INF]
                + count_in_classes[CLS_UPPER_INF]
                + count_in_classes[CLS_LOWER_INF | CLS_UPPER_INF];
            let empty_count = total_count - non_empty_count;

            if inf_count > 0
                && non_inf_count > 0
                && ((inf_count - non_inf_count).abs() <= (empty_count - non_empty_count).abs())
            {
                classes_groups[CLS_NORMAL] = SplitLR::Right;
                classes_groups[CLS_CONTAIN_EMPTY] = SplitLR::Right;
                classes_groups[CLS_EMPTY] = SplitLR::Right;
            } else if empty_count > 0 && non_empty_count > 0 {
                classes_groups[CLS_NORMAL] = SplitLR::Right;
                classes_groups[CLS_LOWER_INF] = SplitLR::Right;
                classes_groups[CLS_UPPER_INF] = SplitLR::Right;
                classes_groups[CLS_LOWER_INF | CLS_UPPER_INF] = SplitLR::Right;
            } else {
                classes_groups[biggest_class] = SplitLR::Right;
            }
        }
        class_split(mcx, cache, &ranges, v, &classes_groups)?;
    }

    Ok(fcinfo.arg(1))
}

#[derive(Clone, Copy, PartialEq)]
enum SplitLR {
    Left,
    Right,
}

fn fallback_split<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeGistCache,
    ranges: &[&'m [u8]],
    v: &mut GistSplitVec,
) -> PgResult<()> {
    let maxoff = ranges.len() - 1;
    let split_idx = (maxoff - 1) / 2 + 1;
    v.spl_left.clear();
    v.spl_right.clear();
    let mut placer = Placer { left: None, right: None };
    for (i, &range) in ranges.iter().enumerate().skip(1) {
        if i < split_idx {
            placer.place_left(mcx, cache, v, range, i)?;
        } else {
            placer.place_right(mcx, cache, v, range, i)?;
        }
    }
    placer.finish(v);
    Ok(())
}

fn class_split<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeGistCache,
    ranges: &[&'m [u8]],
    v: &mut GistSplitVec,
    classes_groups: &[SplitLR; CLS_COUNT],
) -> PgResult<()> {
    v.spl_left.clear();
    v.spl_right.clear();
    let mut placer = Placer { left: None, right: None };
    for (i, &range) in ranges.iter().enumerate().skip(1) {
        if classes_groups[get_gist_range_class(range)] == SplitLR::Left {
            placer.place_left(mcx, cache, v, range, i)?;
        } else {
            placer.place_right(mcx, cache, v, range, i)?;
        }
    }
    placer.finish(v);
    Ok(())
}

fn single_sorting_split<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeGistCache,
    ranges: &[&'m [u8]],
    v: &mut GistSplitVec,
    use_upper_bound: bool,
) -> PgResult<()> {
    #[derive(Clone, Copy)]
    struct SingleBoundSortItem {
        index: usize,
        bound: RangeBound,
    }

    let maxoff = ranges.len() - 1;
    let mut sort_items = Vec::with_capacity(maxoff);
    for (i, &range) in ranges.iter().enumerate().skip(1) {
        let (mut lower, mut upper) = range_bound_slots();
        let empty = range_deserialize_into(&cache.ri.elem, range, &mut lower, &mut upper);
        debug_assert!(!empty);
        let _ = empty;
        sort_items.push(SingleBoundSortItem {
            index: i,
            bound: if use_upper_bound { upper } else { lower },
        });
    }

    pg_qsort_arg(&mut sort_items, |a, b| {
        range_cmp_bounds(mcx, &mut cache.ri, &a.bound, &b.bound)
    })?;

    let split_idx = maxoff / 2;
    v.spl_left.clear();
    v.spl_right.clear();
    let mut placer = Placer { left: None, right: None };
    for (i, item) in sort_items.iter().enumerate() {
        let range = ranges[item.index];
        if i < split_idx {
            placer.place_left(mcx, cache, v, range, item.index)?;
        } else {
            placer.place_right(mcx, cache, v, range, item.index)?;
        }
    }
    placer.finish(v);
    Ok(())
}

struct ConsiderSplitContext {
    has_subtype_diff: bool,
    entries_count: i32,
    first: bool,
    left_upper: RangeBound,
    right_lower: RangeBound,
    ratio: f32,
    overlap: f32,
    common_left: i32,
    common_right: i32,
}

fn consider_split(
    mcx: Mcx<'_>,
    cache: &mut RangeGistCache,
    context: &mut ConsiderSplitContext,
    right_lower: &RangeBound,
    min_left_count: i32,
    left_upper: &RangeBound,
    max_left_count: i32,
) -> PgResult<()> {
    let left_count = if min_left_count >= (context.entries_count + 1) / 2 {
        min_left_count
    } else if max_left_count <= context.entries_count / 2 {
        max_left_count
    } else {
        context.entries_count / 2
    };
    let right_count = context.entries_count - left_count;

    let ratio = left_count.min(right_count) as f32 / context.entries_count as f32;
    if ratio > LIMIT_RATIO {
        let overlap = if context.has_subtype_diff {
            call_subtype_diff(mcx, cache, left_upper.val, right_lower.val)? as f32
        } else {
            (max_left_count - min_left_count) as f32
        };
        let selectthis = context.first
            || overlap < context.overlap
            || (overlap == context.overlap && ratio > context.ratio);
        if selectthis {
            context.first = false;
            context.ratio = ratio;
            context.overlap = overlap;
            context.right_lower = *right_lower;
            context.left_upper = *left_upper;
            context.common_left = max_left_count - left_count;
            context.common_right = left_count - min_left_count;
        }
    }
    Ok(())
}

fn double_sorting_split<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeGistCache,
    ranges: &[&'m [u8]],
    v: &mut GistSplitVec,
) -> PgResult<()> {
    #[derive(Clone, Copy)]
    struct NonEmptyRange {
        lower: RangeBound,
        upper: RangeBound,
    }
    #[derive(Clone, Copy)]
    struct CommonEntry {
        index: usize,
        delta: f64,
    }

    let maxoff = ranges.len() - 1;
    let nentries = maxoff as i32;
    let zero_bound = |lower: bool| RangeBound {
        val: Datum::from_usize(0),
        infinite: false,
        inclusive: false,
        lower,
    };
    let mut context = ConsiderSplitContext {
        has_subtype_diff: cache.subdiff.is_some(),
        entries_count: nentries,
        first: true,
        left_upper: zero_bound(false),
        right_lower: zero_bound(true),
        ratio: 0.0,
        overlap: 0.0,
        common_left: 0,
        common_right: 0,
    };

    let mut by_lower = Vec::with_capacity(maxoff);
    for &range in &ranges[1..] {
        let (mut lower, mut upper) = range_bound_slots();
        let empty = range_deserialize_into(&cache.ri.elem, range, &mut lower, &mut upper);
        debug_assert!(!empty);
        let _ = empty;
        by_lower.push(NonEmptyRange { lower, upper });
    }
    let mut by_upper = by_lower.clone();

    pg_qsort_arg(&mut by_lower, |a, b| {
        range_cmp_bounds(mcx, &mut cache.ri, &a.lower, &b.lower)
    })?;
    pg_qsort_arg(&mut by_upper, |a, b| {
        range_cmp_bounds(mcx, &mut cache.ri, &a.upper, &b.upper)
    })?;

    let n = maxoff;

    // First pass: iterate over lower bounds of the right group, finding the
    // smallest possible upper bound of the left group.
    {
        let mut i1 = 0usize;
        let mut i2 = 0usize;
        let mut right_lower = by_lower[i1].lower;
        let mut left_upper = by_upper[i2].lower;
        loop {
            while i1 < n
                && range_cmp_bounds(mcx, &mut cache.ri, &right_lower, &by_lower[i1].lower)? == 0
            {
                if range_cmp_bounds(mcx, &mut cache.ri, &by_lower[i1].upper, &left_upper)? > 0 {
                    left_upper = by_lower[i1].upper;
                }
                i1 += 1;
            }
            if i1 >= n {
                break;
            }
            right_lower = by_lower[i1].lower;

            while i2 < n
                && range_cmp_bounds(mcx, &mut cache.ri, &by_upper[i2].upper, &left_upper)? <= 0
            {
                i2 += 1;
            }

            consider_split(
                mcx,
                cache,
                &mut context,
                &right_lower,
                i1 as i32,
                &left_upper,
                i2 as i32,
            )?;
        }
    }

    // Second pass: iterate over upper bounds of the left group, finding the
    // greatest possible lower bound of the right group.
    {
        let mut i1 = n as isize - 1;
        let mut i2 = n as isize - 1;
        let mut right_lower = by_lower[i1 as usize].upper;
        let mut left_upper = by_upper[i2 as usize].upper;
        loop {
            while i2 >= 0
                && range_cmp_bounds(mcx, &mut cache.ri, &left_upper, &by_upper[i2 as usize].upper)?
                    == 0
            {
                if range_cmp_bounds(
                    mcx,
                    &mut cache.ri,
                    &by_upper[i2 as usize].lower,
                    &right_lower,
                )? < 0
                {
                    right_lower = by_upper[i2 as usize].lower;
                }
                i2 -= 1;
            }
            if i2 < 0 {
                break;
            }
            left_upper = by_upper[i2 as usize].upper;

            while i1 >= 0
                && range_cmp_bounds(
                    mcx,
                    &mut cache.ri,
                    &by_lower[i1 as usize].lower,
                    &right_lower,
                )? >= 0
            {
                i1 -= 1;
            }

            consider_split(
                mcx,
                cache,
                &mut context,
                &right_lower,
                (i1 + 1) as i32,
                &left_upper,
                (i2 + 1) as i32,
            )?;
        }
    }

    if context.first {
        return fallback_split(mcx, cache, ranges, v);
    }

    v.spl_left.clear();
    v.spl_right.clear();
    let mut placer = Placer { left: None, right: None };

    let mut common_entries: Vec<CommonEntry> = Vec::with_capacity(maxoff);
    for (i, &range) in ranges.iter().enumerate().skip(1) {
        let (mut lower, mut upper) = range_bound_slots();
        let _empty = range_deserialize_into(&cache.ri.elem, range, &mut lower, &mut upper);
        if range_cmp_bounds(mcx, &mut cache.ri, &upper, &context.left_upper)? <= 0 {
            if range_cmp_bounds(mcx, &mut cache.ri, &lower, &context.right_lower)? >= 0 {
                let delta = if context.has_subtype_diff {
                    call_subtype_diff(mcx, cache, lower.val, context.right_lower.val)?
                        - call_subtype_diff(mcx, cache, context.left_upper.val, upper.val)?
                } else {
                    0.0
                };
                common_entries.push(CommonEntry { index: i, delta });
            } else {
                placer.place_left(mcx, cache, v, range, i)?;
            }
        } else {
            debug_assert!(
                range_cmp_bounds(mcx, &mut cache.ri, &lower, &context.right_lower)? >= 0
            );
            placer.place_right(mcx, cache, v, range, i)?;
        }
    }

    if !common_entries.is_empty() {
        pg_qsort_arg(&mut common_entries, |a, b| {
            Ok(if a.delta < b.delta {
                -1
            } else if a.delta > b.delta {
                1
            } else {
                0
            })
        })?;

        for (i, ce) in common_entries.iter().enumerate() {
            let range = ranges[ce.index];
            if (i as i32) < context.common_left {
                placer.place_left(mcx, cache, v, range, ce.index)?;
            } else {
                placer.place_right(mcx, cache, v, range, ce.index)?;
            }
        }
    }

    placer.finish(v);
    Ok(())
}

const fn b(
    foid: Oid,
    name: &'static str,
    nargs: i16,
    func: ::types_fmgr::PGFunction,
) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const RANGETYPES_GIST_BUILTINS: &[FmgrBuiltin] = &[
    b(3875, "range_gist_consistent", 5, fc_range_gist_consistent),
    b(3876, "range_gist_union", 2, fc_range_gist_union),
    b(3879, "range_gist_penalty", 3, fc_range_gist_penalty),
    b(3880, "range_gist_picksplit", 2, fc_range_gist_picksplit),
    b(3881, "range_gist_same", 3, fc_range_gist_same),
    b(6154, "multirange_gist_consistent", 5, fc_multirange_gist_consistent),
    b(6156, "multirange_gist_compress", 1, fc_multirange_gist_compress),
];
