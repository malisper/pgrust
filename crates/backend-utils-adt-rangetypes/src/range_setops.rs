//! Family `range-setops`: set operations over ranges.
//!
//! Mirrors `rangetypes.c`: `range_minus` / `range_minus_internal`,
//! `range_union` / `range_union_internal`, `range_merge`, `range_intersect` /
//! `range_intersect_internal`, `range_split_internal`, and the aggregate
//! transition `range_intersect_agg_transfn`.

use core::marker::PhantomData;

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use types_rangetypes::RangeTypeP;

use crate::range_bounds_compare::{
    range_adjacent_internal, range_cmp_bounds, range_overlaps_internal,
};
use crate::range_repr_serialize::{make_empty_range, make_range, range_deserialize};

/// `RangeTypeGetOid(r)` (rangetypes.h): the range type's own OID, read from the
/// detoasted varlena header (the only directly-readable field of `RangeTypeP`).
#[inline]
fn range_type_get_oid(r: RangeTypeP<'_>) -> types_core::primitive::Oid {
    // SAFETY: `RangeTypeP` is a live detoasted `RangeType *`; `rangetypid` is a
    // header field, always valid to read.
    unsafe { (*r.ptr).rangetypid }
}

/// Re-label a caller-owned input handle to the `'mcx` result lifetime. C returns
/// one of its input `RangeType *` pointers unchanged in several branches; the
/// pointed-at memory is caller-owned and outlives the call, so the handle is
/// just retagged (no copy), mirroring the C exactly.
#[inline]
fn relabel<'mcx>(r: RangeTypeP<'_>) -> RangeTypeP<'mcx> {
    RangeTypeP {
        ptr: r.ptr,
        _marker: PhantomData,
    }
}

/// `range_minus_internal(typcache, r1, r2)` (rangetypes.c:995): `r1 \ r2`.
/// `ereport(ERROR)` when the result would be discontiguous.
pub fn range_minus_internal<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* if either is empty, r1 is the correct answer */
    if empty1 || empty2 {
        return Ok(relabel(r1));
    }

    let cmp_l1l2 = range_cmp_bounds(typcache, &lower1, &lower2)?;
    let cmp_l1u2 = range_cmp_bounds(typcache, &lower1, &upper2)?;
    let cmp_u1l2 = range_cmp_bounds(typcache, &upper1, &lower2)?;
    let cmp_u1u2 = range_cmp_bounds(typcache, &upper1, &upper2)?;

    if cmp_l1l2 < 0 && cmp_u1u2 > 0 {
        return Err(PgError::error(
            "result of range difference would not be contiguous",
        )
        .with_sqlstate(ERRCODE_DATA_EXCEPTION));
    }

    if cmp_l1u2 > 0 || cmp_u1l2 < 0 {
        return Ok(relabel(r1));
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 <= 0 {
        return make_empty_range(mcx, typcache);
    }

    if cmp_l1l2 <= 0 && cmp_u1l2 >= 0 && cmp_u1u2 <= 0 {
        let mut lower2 = lower2;
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false; /* it will become the upper bound */
        return make_range(mcx, typcache, &lower1, &lower2, false);
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 >= 0 && cmp_l1u2 <= 0 {
        let mut upper2 = upper2;
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true; /* it will become the lower bound */
        return make_range(mcx, typcache, &upper2, &upper1, false);
    }

    Err(PgError::error("unexpected case in range_minus"))
}

/// `range_union_internal(typcache, r1, r2, strict)` (rangetypes.c:1054).
pub fn range_union_internal<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
    strict: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return Err(PgError::error("range types do not match"));
    }

    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* if either is empty, the other is the correct answer */
    if empty1 {
        return Ok(relabel(r2));
    }
    if empty2 {
        return Ok(relabel(r1));
    }

    if strict
        && !range_overlaps_internal(typcache, r1, r2)?
        && !range_adjacent_internal(mcx, typcache, r1, r2)?
    {
        return Err(
            PgError::error("result of range union would not be contiguous")
                .with_sqlstate(ERRCODE_DATA_EXCEPTION),
        );
    }

    let result_lower = if range_cmp_bounds(typcache, &lower1, &lower2)? < 0 {
        &lower1
    } else {
        &lower2
    };

    let result_upper = if range_cmp_bounds(typcache, &upper1, &upper2)? > 0 {
        &upper1
    } else {
        &upper2
    };

    make_range(mcx, typcache, result_lower, result_upper, false)
}

/// `range_merge(r1, r2)` body (rangetypes.c:1116): smallest range covering both.
/// The SQL-callable `range_merge` is the non-strict union.
pub fn range_merge<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    range_union_internal(mcx, typcache, r1, r2, false)
}

/// `range_intersect_internal(typcache, r1, r2)` (rangetypes.c:1145).
pub fn range_intersect_internal<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    if empty1 || empty2 || !range_overlaps_internal(typcache, r1, r2)? {
        return make_empty_range(mcx, typcache);
    }

    let result_lower = if range_cmp_bounds(typcache, &lower1, &lower2)? >= 0 {
        &lower1
    } else {
        &lower2
    };

    let result_upper = if range_cmp_bounds(typcache, &upper1, &upper2)? <= 0 {
        &upper1
    } else {
        &upper2
    };

    make_range(mcx, typcache, result_lower, result_upper, false)
}

/// `range_split_internal(typcache, r1, r2, output1, output2)`
/// (rangetypes.c:1184): split `r1` around `r2`, returning the (lower, upper)
/// fragments. Returns `None` when `r2` does not intersect the middle of `r1`
/// (the C `return false` path; outputs left unset). Neither input range should
/// be empty.
pub fn range_split_internal<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<(Option<RangeTypeP<'mcx>>, Option<RangeTypeP<'mcx>>)> {
    let (lower1, upper1, _empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, _empty2) = range_deserialize(typcache, r2)?;

    if range_cmp_bounds(typcache, &lower1, &lower2)? < 0
        && range_cmp_bounds(typcache, &upper1, &upper2)? > 0
    {
        /*
         * Need to invert inclusive/exclusive for the lower2 and upper2
         * points. They can't be infinite though. We're allowed to overwrite
         * these RangeBounds since they only exist locally.
         */
        let mut lower2 = lower2;
        let mut upper2 = upper2;
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;

        let output1 = make_range(mcx, typcache, &lower1, &lower2, false)?;
        let output2 = make_range(mcx, typcache, &upper2, &upper1, false)?;
        return Ok((Some(output1), Some(output2)));
    }

    Ok((None, None))
}

/// Inward seam shape for `range_split_internal`.
///
/// The kernel returns `(Some, Some)` only when `r2` splits `r1` and `(None,
/// None)` otherwise (the two outputs are always set or unset together, exactly
/// as the C `*output1`/`*output2` pair and its `true`/`false` return). The seam
/// folds that into `Some((left, right))` / `None`.
pub fn range_split_internal_seam<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'mcx>,
    r2: RangeTypeP<'mcx>,
) -> PgResult<Option<(RangeTypeP<'mcx>, RangeTypeP<'mcx>)>> {
    match range_split_internal(mcx, typcache, r1, r2)? {
        (Some(output1), Some(output2)) => Ok(Some((output1, output2))),
        _ => Ok(None),
    }
}

/// `range_intersect_agg_transfn(fcinfo)` body (rangetypes.c:1221): running
/// intersection aggregate. Both arguments are strict (the SQL aggregate marks
/// the state and the input non-null), so `state`/`current` model the
/// guaranteed-present operands; the C reads `PG_GETARG_RANGE_P(0/1)` directly.
pub fn range_intersect_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    state: Option<RangeTypeP<'_>>,
    current: Option<RangeTypeP<'_>>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    /* strictness ensures these are non-null */
    let result = state.expect("range_intersect_agg_transfn: strict state arg is non-null");
    let current = current.expect("range_intersect_agg_transfn: strict current arg is non-null");

    let result = range_intersect_internal(mcx, typcache, result, current)?;
    Ok(Some(result))
}
