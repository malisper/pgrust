//! setops + ordering + aggregates: union / minus / intersect, `range_merge`,
//! the btree ordering (`cmp`/`lt`/`le`/`ge`/`gt`), hashing, and the
//! range/multirange aggregates.
//!
//! The set operations and ordering deserialize the member ranges and delegate
//! per-member math across `rangetypes-seams`. The aggregates accumulate member
//! `RangeType`s (C: an `ArrayBuildState` of range datums) and assemble the
//! result through the inward `make_multirange` seam in the finalfn.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_rangetypes::{MultirangeTypeP, RangeTypeP};

// ---------------------------------------------------------------------------
// set operations.
// ---------------------------------------------------------------------------

/// `multirange_union(PG_FUNCTION_ARGS)` (multirangetypes.c:1083).
pub fn multirange_union<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'mcx>,
    mr2: MultirangeTypeP<'mcx>,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, rangetyp, mr1, mr2);
    todo!("port multirange_union (multirangetypes.c:1083)")
}

/// `multirange_minus(PG_FUNCTION_ARGS)` (multirangetypes.c:1115).
pub fn multirange_minus<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'mcx>,
    mr2: MultirangeTypeP<'mcx>,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, rangetyp, mr1, mr2);
    todo!("port multirange_minus (multirangetypes.c:1115)")
}

/// `multirange_minus_internal(mltrngtypoid, rangetyp, range_count1, ranges1,
/// range_count2, ranges2)` (multirangetypes.c:1145).
pub fn multirange_minus_internal<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    ranges1: &[RangeTypeP<'mcx>],
    ranges2: &[RangeTypeP<'mcx>],
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, mltrngtypoid, rangetyp, ranges1, ranges2);
    todo!("port multirange_minus_internal (multirangetypes.c:1145)")
}

/// `multirange_intersect(PG_FUNCTION_ARGS)` (multirangetypes.c:1231).
pub fn multirange_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'mcx>,
    mr2: MultirangeTypeP<'mcx>,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, rangetyp, mr1, mr2);
    todo!("port multirange_intersect (multirangetypes.c:1231)")
}

/// `multirange_intersect_internal(mltrngtypoid, rangetyp, range_count1, ranges1,
/// range_count2, ranges2)` (multirangetypes.c:1261).
pub fn multirange_intersect_internal<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    ranges1: &[RangeTypeP<'mcx>],
    ranges2: &[RangeTypeP<'mcx>],
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, mltrngtypoid, rangetyp, ranges1, ranges2);
    todo!("port multirange_intersect_internal (multirangetypes.c:1261)")
}

/// `range_merge_from_multirange(PG_FUNCTION_ARGS)` (multirangetypes.c:2676): the
/// range spanning the whole multirange (errors on an empty multirange).
pub fn range_merge_from_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    let _ = (mcx, rangetyp, mr);
    todo!("port range_merge_from_multirange (multirangetypes.c:2676)")
}

// ---------------------------------------------------------------------------
// btree ordering.
// ---------------------------------------------------------------------------

/// `multirange_cmp(PG_FUNCTION_ARGS)` (multirangetypes.c:2576): the btree
/// 3-way comparison.
pub fn multirange_cmp(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<i32> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_cmp (multirangetypes.c:2576)")
}

/// `multirange_lt(PG_FUNCTION_ARGS)` (multirangetypes.c:2641).
pub fn multirange_lt(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_lt (multirangetypes.c:2641)")
}

/// `multirange_le(PG_FUNCTION_ARGS)` (multirangetypes.c:2649).
pub fn multirange_le(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_le (multirangetypes.c:2649)")
}

/// `multirange_ge(PG_FUNCTION_ARGS)` (multirangetypes.c:2657).
pub fn multirange_ge(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_ge (multirangetypes.c:2657)")
}

/// `multirange_gt(PG_FUNCTION_ARGS)` (multirangetypes.c:2665).
pub fn multirange_gt(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_gt (multirangetypes.c:2665)")
}

// ---------------------------------------------------------------------------
// hashing.
// ---------------------------------------------------------------------------

/// `hash_multirange(PG_FUNCTION_ARGS)` (multirangetypes.c:2788).
pub fn hash_multirange(rangetyp: &TypeCacheEntry, mr: MultirangeTypeP<'_>) -> PgResult<u32> {
    let _ = (rangetyp, mr);
    todo!("port hash_multirange (multirangetypes.c:2788)")
}

/// `hash_multirange_extended(PG_FUNCTION_ARGS)` (multirangetypes.c:2859).
pub fn hash_multirange_extended(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    seed: u64,
) -> PgResult<u64> {
    let _ = (rangetyp, mr, seed);
    todo!("port hash_multirange_extended (multirangetypes.c:2859)")
}

// ---------------------------------------------------------------------------
// aggregates. The transition state is C's `ArrayBuildState` of range datums;
// modeled here as the accumulated member `RangeType`s, which the finalfns
// assemble through the inward `make_multirange` seam.
// ---------------------------------------------------------------------------

/// `range_agg_transfn(PG_FUNCTION_ARGS)` (multirangetypes.c:1341): accumulate
/// one range into the array-build state.
pub fn range_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut Vec<RangeTypeP<'mcx>>,
    value: Option<RangeTypeP<'mcx>>,
) -> PgResult<()> {
    let _ = (mcx, state, value);
    todo!("port range_agg_transfn (multirangetypes.c:1341)")
}

/// `range_agg_finalfn(PG_FUNCTION_ARGS)` (multirangetypes.c:1373): assemble the
/// accumulated ranges into a multirange.
pub fn range_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    state: &[RangeTypeP<'mcx>],
) -> PgResult<Option<MultirangeTypeP<'mcx>>> {
    let _ = (mcx, mltrngtypoid, rangetyp, state);
    todo!("port range_agg_finalfn (multirangetypes.c:1373)")
}

/// `multirange_agg_transfn(PG_FUNCTION_ARGS)` (multirangetypes.c:1413):
/// accumulate every member range of a multirange into the state.
pub fn multirange_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    state: &mut Vec<RangeTypeP<'mcx>>,
    value: Option<MultirangeTypeP<'mcx>>,
) -> PgResult<()> {
    let _ = (mcx, rangetyp, state, value);
    todo!("port multirange_agg_transfn (multirangetypes.c:1413)")
}

/// `multirange_intersect_agg_transfn(PG_FUNCTION_ARGS)`
/// (multirangetypes.c:1466): fold a multirange into the running intersection.
pub fn multirange_intersect_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    state: Option<MultirangeTypeP<'mcx>>,
    value: Option<MultirangeTypeP<'mcx>>,
) -> PgResult<Option<MultirangeTypeP<'mcx>>> {
    let _ = (mcx, rangetyp, state, value);
    todo!("port multirange_intersect_agg_transfn (multirangetypes.c:1466)")
}
