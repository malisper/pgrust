use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::typcache::{TYPECACHE_HASH_EXTENDED_PROC_FINFO, TYPECACHE_HASH_PROC_FINFO};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_UNDEFINED_FUNCTION};
use ::types_fmgr::{function_call1_coll_in, function_call2_coll_in, FmgrInfo};

use crate::{
    cmp_elem_vals, make_empty_range, make_range, range_bound_slots, range_cmp_bounds,
    range_deserialize_into,
    range_get_flags, range_is_empty, range_type_oid, range_types_do_not_match, RangeBound,
    RangeInfo, range_cmp_bound_values,
};

fn check_same_type(r1: &[u8], r2: &[u8]) -> PgResult<()> {
    if range_type_oid(r1) != range_type_oid(r2) {
        return Err(range_types_do_not_match());
    }
    Ok(())
}

pub fn range_eq_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    if empty1 && empty2 {
        return Ok(true);
    }
    if empty1 != empty2 {
        return Ok(false);
    }
    if range_cmp_bounds(mcx, ri, &lower1, &lower2)? != 0 {
        return Ok(false);
    }
    if range_cmp_bounds(mcx, ri, &upper1, &upper2)? != 0 {
        return Ok(false);
    }
    Ok(true)
}

pub fn range_ne_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    Ok(!range_eq_internal(mcx, ri, r1, r2)?)
}

pub fn range_contains_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    if empty2 {
        return Ok(true);
    }
    if empty1 {
        return Ok(false);
    }
    if range_cmp_bounds(mcx, ri, &lower1, &lower2)? > 0 {
        return Ok(false);
    }
    if range_cmp_bounds(mcx, ri, &upper1, &upper2)? < 0 {
        return Ok(false);
    }
    Ok(true)
}

pub fn range_contained_by_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    range_contains_internal(mcx, ri, r2, r1)
}

pub fn range_contains_elem_internal(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    r: &[u8],
    val: Datum,
) -> PgResult<bool> {
    let (mut lower, mut upper) = range_bound_slots();
    let empty = range_deserialize_into(&ri.elem, r, &mut lower, &mut upper);
    if empty {
        return Ok(false);
    }
    if !lower.infinite {
        let cmp = cmp_elem_vals(mcx, ri, lower.val, val)?;
        if cmp > 0 {
            return Ok(false);
        }
        if cmp == 0 && !lower.inclusive {
            return Ok(false);
        }
    }
    if !upper.infinite {
        let cmp = cmp_elem_vals(mcx, ri, upper.val, val)?;
        if cmp < 0 {
            return Ok(false);
        }
        if cmp == 0 && !upper.inclusive {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn range_before_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut _lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut _lower1, &mut upper1);
    let (mut lower2, mut _upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut _upper2);
    if empty1 || empty2 {
        return Ok(false);
    }
    Ok(range_cmp_bounds(mcx, ri, &upper1, &lower2)? < 0)
}

pub fn range_after_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut _upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut _upper1);
    let (mut _lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut _lower2, &mut upper2);
    if empty1 || empty2 {
        return Ok(false);
    }
    Ok(range_cmp_bounds(mcx, ri, &lower1, &upper2)? > 0)
}

/// bounds_adjacent (rangetypes.c): A an upper bound, B a lower bound.
pub fn bounds_adjacent(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    mut bound_a: RangeBound,
    mut bound_b: RangeBound,
) -> PgResult<bool> {
    debug_assert!(!bound_a.lower && bound_b.lower);
    let cmp = range_cmp_bound_values(mcx, ri, &bound_a, &bound_b)?;
    if cmp < 0 {
        if ri.canonical_oid == InvalidOid {
            return Ok(false);
        }
        bound_a.inclusive = !bound_a.inclusive;
        bound_b.inclusive = !bound_b.inclusive;
        bound_a.lower = true;
        bound_b.lower = false;
        let r = make_range(mcx, ri, &mut bound_a, &mut bound_b, false, None)?
            .expect("hard error path returns Some");
        Ok(range_is_empty(&r))
    } else if cmp == 0 {
        Ok(bound_a.inclusive != bound_b.inclusive)
    } else {
        Ok(false)
    }
}

pub fn range_adjacent_internal(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    r1: &[u8],
    r2: &[u8],
) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    if empty1 || empty2 {
        return Ok(false);
    }
    Ok(bounds_adjacent(mcx, ri, upper1, lower2)? || bounds_adjacent(mcx, ri, upper2, lower1)?)
}

pub fn range_overlaps_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    if empty1 || empty2 {
        return Ok(false);
    }
    if range_cmp_bounds(mcx, ri, &lower1, &lower2)? >= 0 && range_cmp_bounds(mcx, ri, &lower1, &upper2)? <= 0
    {
        return Ok(true);
    }
    if range_cmp_bounds(mcx, ri, &lower2, &lower1)? >= 0 && range_cmp_bounds(mcx, ri, &lower2, &upper1)? <= 0
    {
        return Ok(true);
    }
    Ok(false)
}

pub fn range_overleft_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut _l1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut _l1, &mut upper1);
    let (mut _l2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut _l2, &mut upper2);
    if empty1 || empty2 {
        return Ok(false);
    }
    Ok(range_cmp_bounds(mcx, ri, &upper1, &upper2)? <= 0)
}

pub fn range_overright_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<bool> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut _u1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut _u1);
    let (mut lower2, mut _u2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut _u2);
    if empty1 || empty2 {
        return Ok(false);
    }
    Ok(range_cmp_bounds(mcx, ri, &lower1, &lower2)? >= 0)
}

#[track_caller]
#[cold]
fn range_minus_not_contiguous() -> Box<PgError> {
    Box::new(
        PgError::error("result of range difference would not be contiguous")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION),
    )
}

/// range_minus_internal: `Ok(None)` mirrors C's returning r1 unchanged (the
/// caller reuses the input image).
pub enum MinusResult<'m> {
    Input1,
    New(PgVec<'m, u8>),
}

pub fn range_minus_internal<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    r1: &[u8],
    r2: &[u8],
) -> PgResult<MinusResult<'m>> {
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);

    if empty1 || empty2 {
        return Ok(MinusResult::Input1);
    }

    let cmp_l1l2 = range_cmp_bounds(mcx, ri, &lower1, &lower2)?;
    let cmp_l1u2 = range_cmp_bounds(mcx, ri, &lower1, &upper2)?;
    let cmp_u1l2 = range_cmp_bounds(mcx, ri, &upper1, &lower2)?;
    let cmp_u1u2 = range_cmp_bounds(mcx, ri, &upper1, &upper2)?;

    if cmp_l1l2 < 0 && cmp_u1u2 > 0 {
        return Err(range_minus_not_contiguous());
    }

    if cmp_l1u2 > 0 || cmp_u1l2 < 0 {
        return Ok(MinusResult::Input1);
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 <= 0 {
        return Ok(MinusResult::New(make_empty_range(mcx, ri)?));
    }

    if cmp_l1l2 <= 0 && cmp_u1l2 >= 0 && cmp_u1u2 <= 0 {
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;
        let mut l1 = lower1;
        return Ok(MinusResult::New(
            make_range(mcx, ri, &mut l1, &mut lower2, false, None)?
                .expect("hard error path returns Some"),
        ));
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 >= 0 && cmp_l1u2 <= 0 {
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;
        let mut u1 = upper1;
        return Ok(MinusResult::New(
            make_range(mcx, ri, &mut upper2, &mut u1, false, None)?
                .expect("hard error path returns Some"),
        ));
    }

    Err(Box::new(PgError::error("unexpected case in range_minus")))
}

#[track_caller]
#[cold]
fn range_union_not_contiguous() -> Box<PgError> {
    Box::new(
        PgError::error("result of range union would not be contiguous")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION),
    )
}

pub enum UnionResult<'m> {
    Input1,
    Input2,
    New(PgVec<'m, u8>),
}

pub fn range_union_internal<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    r1: &[u8],
    r2: &[u8],
    strict: bool,
) -> PgResult<UnionResult<'m>> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);

    if empty1 {
        return Ok(UnionResult::Input2);
    }
    if empty2 {
        return Ok(UnionResult::Input1);
    }

    if strict
        && !range_overlaps_internal(mcx, ri, r1, r2)?
        && !range_adjacent_internal(mcx, ri, r1, r2)?
    {
        return Err(range_union_not_contiguous());
    }

    let mut result_lower =
        if range_cmp_bounds(mcx, ri, &lower1, &lower2)? < 0 { lower1 } else { lower2 };
    let mut result_upper =
        if range_cmp_bounds(mcx, ri, &upper1, &upper2)? > 0 { upper1 } else { upper2 };

    Ok(UnionResult::New(
        make_range(mcx, ri, &mut result_lower, &mut result_upper, false, None)?
            .expect("hard error path returns Some"),
    ))
}

pub fn range_intersect_internal<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    r1: &[u8],
    r2: &[u8],
) -> PgResult<PgVec<'m, u8>> {
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);

    if empty1 || empty2 || !range_overlaps_internal(mcx, ri, r1, r2)? {
        return make_empty_range(mcx, ri);
    }

    let mut result_lower =
        if range_cmp_bounds(mcx, ri, &lower1, &lower2)? >= 0 { lower1 } else { lower2 };
    let mut result_upper =
        if range_cmp_bounds(mcx, ri, &upper1, &upper2)? <= 0 { upper1 } else { upper2 };

    Ok(make_range(mcx, ri, &mut result_lower, &mut result_upper, false, None)?
        .expect("hard error path returns Some"))
}

/// range_split_internal: both outputs, or None when r2 does not split r1.
#[allow(clippy::type_complexity)]
pub fn range_split_internal<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    r1: &[u8],
    r2: &[u8],
) -> PgResult<Option<(PgVec<'m, u8>, PgVec<'m, u8>)>> {
    let (mut lower1, mut upper1) = range_bound_slots();
    let _e1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let _e2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);

    if range_cmp_bounds(mcx, ri, &lower1, &lower2)? < 0 && range_cmp_bounds(mcx, ri, &upper1, &upper2)? > 0 {
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;
        let out1 = make_range(mcx, ri, &mut lower1, &mut lower2, false, None)?
            .expect("hard error path returns Some");
        let out2 = make_range(mcx, ri, &mut upper2, &mut upper1, false, None)?
            .expect("hard error path returns Some");
        return Ok(Some((out1, out2)));
    }
    Ok(None)
}

/// range_cmp core (btree comparator; empties sort first).
pub fn range_cmp_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<i32> {
    check_same_type(r1, r2)?;
    let (mut lower1, mut upper1) = range_bound_slots();
    let empty1 = range_deserialize_into(&ri.elem, r1, &mut lower1, &mut upper1);
    let (mut lower2, mut upper2) = range_bound_slots();
    let empty2 = range_deserialize_into(&ri.elem, r2, &mut lower2, &mut upper2);
    if empty1 && empty2 {
        Ok(0)
    } else if empty1 {
        Ok(-1)
    } else if empty2 {
        Ok(1)
    } else {
        let mut cmp = range_cmp_bounds(mcx, ri, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_cmp_bounds(mcx, ri, &upper1, &upper2)?;
        }
        Ok(cmp)
    }
}

#[track_caller]
#[cold]
fn no_hash_function(elem_typid: Oid) -> Box<PgError> {
    let t = ::format_type::format_type_be(elem_typid)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| format!("{elem_typid}"));
    Box::new(
        PgError::error(format!("could not identify a hash function for type {t}"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

pub fn elem_hash_finfo(ri: &mut RangeInfo) -> PgResult<&mut FmgrInfo> {
    if ri.elem_hash.is_none() {
        let sc = ::typcache::lookup_type_cache(ri.elem_typid, TYPECACHE_HASH_PROC_FINFO)?;
        let f = sc.hash_proc_finfo().clone();
        if f.fn_oid == InvalidOid {
            return Err(no_hash_function(ri.elem_typid));
        }
        ri.elem_hash = Some(f);
    }
    Ok(ri.elem_hash.as_mut().unwrap())
}

pub fn elem_hash_extended_finfo(ri: &mut RangeInfo) -> PgResult<&mut FmgrInfo> {
    if ri.elem_hash_extended.is_none() {
        let sc = ::typcache::lookup_type_cache(ri.elem_typid, TYPECACHE_HASH_EXTENDED_PROC_FINFO)?;
        let f = sc.hash_extended_proc_finfo().clone();
        if f.fn_oid == InvalidOid {
            return Err(no_hash_function(ri.elem_typid));
        }
        ri.elem_hash_extended = Some(f);
    }
    Ok(ri.elem_hash_extended.as_mut().unwrap())
}

/// hash_range (rangetypes.c).
pub fn hash_range_internal(mcx: Mcx<'_>, ri: &mut RangeInfo, r: &[u8]) -> PgResult<u32> {
    let (mut lower, mut upper) = range_bound_slots();
    let _empty = range_deserialize_into(&ri.elem, r, &mut lower, &mut upper);
    let flags = range_get_flags(r);
    let collation = ri.collation;
    elem_hash_finfo(ri)?;

    let lower_hash = if crate::range_has_lbound(flags) {
        function_call1_coll_in(ri.elem_hash.as_mut().unwrap(), collation, mcx, lower.val)?.as_u32()
    } else {
        0
    };
    let upper_hash = if crate::range_has_ubound(flags) {
        function_call1_coll_in(ri.elem_hash.as_mut().unwrap(), collation, mcx, upper.val)?.as_u32()
    } else {
        0
    };

    let mut result = ::hashfn::hash_bytes_uint32(flags as u32);
    result ^= lower_hash;
    result = result.rotate_left(1);
    result ^= upper_hash;
    Ok(result)
}

/// hash_range_extended (rangetypes.c).
pub fn hash_range_extended_internal(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    r: &[u8],
    seed: Datum,
) -> PgResult<u64> {
    let (mut lower, mut upper) = range_bound_slots();
    let _empty = range_deserialize_into(&ri.elem, r, &mut lower, &mut upper);
    let flags = range_get_flags(r);
    let collation = ri.collation;
    elem_hash_extended_finfo(ri)?;

    let lower_hash = if crate::range_has_lbound(flags) {
        function_call2_coll_in(ri.elem_hash_extended.as_mut().unwrap(), collation, mcx, lower.val, seed)?
            .as_u64()
    } else {
        0
    };
    let upper_hash = if crate::range_has_ubound(flags) {
        function_call2_coll_in(ri.elem_hash_extended.as_mut().unwrap(), collation, mcx, upper.val, seed)?
            .as_u64()
    } else {
        0
    };

    let mut result = ::hashfn::hash_bytes_uint32_extended(flags as u32, seed.as_u64());
    result ^= lower_hash;
    result = ::hashfn::rotate_high_and_low_32bits(result);
    result ^= upper_hash;
    Ok(result)
}
