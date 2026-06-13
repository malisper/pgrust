//! Family `range-bounds-compare`: bound comparison + the `*_internal` predicate
//! kernels.
//!
//! Mirrors `rangetypes.c`: `range_cmp_bounds` / `range_cmp_bound_values`,
//! `range_compare`, `bounds_adjacent`, `range_get_typcache`, and every boolean
//! `*_internal` kernel (`range_eq`/`ne`/`contains`/`contained_by`/`before`/
//! `after`/`adjacent`/`overlaps`/`overleft`/`overright` and the element
//! containment kernels). The subtype `cmp` call goes through the fmgr seam; the
//! type-cache lookup through the typcache seams. This family owns and installs
//! the inward `range_cmp_bounds` and `range_get_typcache` seams.

use types_cache::typcache::TypeCacheEntry;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_core::primitive::Oid;
use types_rangetypes::{RangeBound, RangeTypeP};

/// `range_get_typcache(fcinfo, rngtypid)` (rangetypes.c:1767): the cached
/// `TypeCacheEntry` for the range type. Owns the inward seam.
pub fn range_get_typcache(_rngtypid: Oid) -> PgResult<TypeCacheEntry> {
    todo!("range_get_typcache: lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO)")
}

/// `range_cmp_bounds(typcache, b1, b2)` (rangetypes.c:2080): compare two bounds
/// using the subtype's `cmp` support fn (fmgr seam), tie-breaking on
/// inclusivity/lower/infinite per the C. Owns the inward seam.
pub fn range_cmp_bounds(
    _typcache: &TypeCacheEntry,
    _b1: &RangeBound,
    _b2: &RangeBound,
) -> PgResult<i32> {
    todo!("range_cmp_bounds")
}

/// `range_cmp_bound_values(typcache, b1, b2)` (rangetypes.c:2154): like
/// `range_cmp_bounds` but compares only the bound values (ignores which side).
pub fn range_cmp_bound_values(
    _typcache: &TypeCacheEntry,
    _b1: &RangeBound,
    _b2: &RangeBound,
) -> PgResult<i32> {
    todo!("range_cmp_bound_values")
}

/// `range_compare(arg1, arg2)` body (rangetypes.c:2193): total order over two
/// ranges (empty < non-empty; then lower, then upper).
pub fn range_compare(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<i32> {
    todo!("range_compare")
}

/// `bounds_adjacent(typcache, boundA, boundB)` (rangetypes.c:2759): whether an
/// upper bound and a lower bound are exactly adjacent.
pub fn bounds_adjacent(
    _typcache: &TypeCacheEntry,
    _bound_a: RangeBound,
    _bound_b: RangeBound,
) -> PgResult<bool> {
    todo!("bounds_adjacent")
}

// --- boolean predicate kernels (rangetypes.c) -----------------------------

/// `range_contains_elem_internal(typcache, r, val)` (rangetypes.c:2691).
pub fn range_contains_elem_internal(
    _typcache: &TypeCacheEntry,
    _r: RangeTypeP<'_>,
    _val: Datum,
) -> PgResult<bool> {
    todo!("range_contains_elem_internal")
}

/// `range_eq_internal(typcache, r1, r2)` (rangetypes.c:575).
pub fn range_eq_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_eq_internal")
}

/// `range_ne_internal(typcache, r1, r2)` (rangetypes.c:620).
pub fn range_ne_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_ne_internal")
}

/// `range_contains_internal(typcache, r1, r2)` (rangetypes.c:2650).
pub fn range_contains_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_contains_internal")
}

/// `range_contained_by_internal(typcache, r1, r2)` (rangetypes.c:2682).
pub fn range_contained_by_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_contained_by_internal")
}

/// `range_before_internal(typcache, r1, r2)` (rangetypes.c:666).
pub fn range_before_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_before_internal")
}

/// `range_after_internal(typcache, r1, r2)` (rangetypes.c:704).
pub fn range_after_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_after_internal")
}

/// `range_adjacent_internal(typcache, r1, r2)` (rangetypes.c:800).
pub fn range_adjacent_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_adjacent_internal")
}

/// `range_overlaps_internal(typcache, r1, r2)` (rangetypes.c:843).
pub fn range_overlaps_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_overlaps_internal")
}

/// `range_overleft_internal(typcache, r1, r2)` (rangetypes.c:889).
pub fn range_overleft_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_overleft_internal")
}

/// `range_overright_internal(typcache, r1, r2)` (rangetypes.c:930).
pub fn range_overright_internal(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    todo!("range_overright_internal")
}
