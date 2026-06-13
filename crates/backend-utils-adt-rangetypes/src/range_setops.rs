//! Family `range-setops`: set operations over ranges.
//!
//! Mirrors `rangetypes.c`: `range_minus` / `range_minus_internal`,
//! `range_union` / `range_union_internal`, `range_merge`, `range_intersect` /
//! `range_intersect_internal`, `range_split_internal`, and the aggregate
//! transition `range_intersect_agg_transfn`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_error::PgResult;
use types_rangetypes::RangeTypeP;

/// `range_minus_internal(typcache, r1, r2)` (rangetypes.c:995): `r1 \ r2`.
/// `ereport(ERROR)` when the result would be discontiguous.
pub fn range_minus_internal<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_minus_internal")
}

/// `range_union_internal(typcache, r1, r2, strict)` (rangetypes.c:1054).
pub fn range_union_internal<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
    _strict: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_union_internal")
}

/// `range_merge(r1, r2)` body (rangetypes.c:1116): smallest range covering both.
pub fn range_merge<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_merge")
}

/// `range_intersect_internal(typcache, r1, r2)` (rangetypes.c:1145).
pub fn range_intersect_internal<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_intersect_internal")
}

/// `range_split_internal(typcache, r1, r2, output1, output2)`
/// (rangetypes.c:1184): split `r1` around `r2`, returning the (lower, upper)
/// fragments (each `None` when empty).
pub fn range_split_internal<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<(Option<RangeTypeP<'mcx>>, Option<RangeTypeP<'mcx>>)> {
    todo!("range_split_internal")
}

/// `range_intersect_agg_transfn(fcinfo)` body (rangetypes.c:1221): running
/// intersection aggregate; `None` models the SQL-NULL state.
pub fn range_intersect_agg_transfn<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _state: Option<RangeTypeP<'_>>,
    _current: Option<RangeTypeP<'_>>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    todo!("range_intersect_agg_transfn")
}
