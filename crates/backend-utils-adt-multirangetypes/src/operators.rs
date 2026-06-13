//! operators: containment / overlap / position / adjacency predicates, the
//! bound-pair helpers and bsearch driver, equality, and the accessor / unnest
//! functions.
//!
//! Every `*_internal` predicate is the logic-bearing kernel; the SQL wrappers
//! decode their operands (through `datum_get_*` / `multirange_get_typcache`) and
//! delegate. The per-member range comparisons cross `rangetypes-seams`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::{MultirangeTypeP, RangeBound, RangeTypeP};

// ---------------------------------------------------------------------------
// Bound-pair helpers + binary-search driver (multirangetypes.c).
// ---------------------------------------------------------------------------

/// `range_bounds_overlaps(typcache, lower1, upper1, lower2, upper2)`
/// (multirangetypes.c:859): like `range_overlaps_internal` but over raw bounds.
pub fn range_bounds_overlaps(
    typcache: &TypeCacheEntry,
    lower1: &RangeBound,
    upper1: &RangeBound,
    lower2: &RangeBound,
    upper2: &RangeBound,
) -> PgResult<bool> {
    let _ = (typcache, lower1, upper1, lower2, upper2);
    todo!("port range_bounds_overlaps (multirangetypes.c:859)")
}

/// `range_bounds_contains(typcache, lower1, upper1, lower2, upper2)`
/// (multirangetypes.c:879): like `range_contains_internal` but over raw bounds.
pub fn range_bounds_contains(
    typcache: &TypeCacheEntry,
    lower1: &RangeBound,
    upper1: &RangeBound,
    lower2: &RangeBound,
    upper2: &RangeBound,
) -> PgResult<bool> {
    let _ = (typcache, lower1, upper1, lower2, upper2);
    todo!("port range_bounds_contains (multirangetypes.c:879)")
}

/// `multirange_bsearch_match(typcache, mr, key, cmp_func)`
/// (multirangetypes.c:899): binary-search the member ranges for one matching
/// `key` under `cmp_func`, which returns the search sign and sets the `match`
/// flag (modeled as the `(i32, &mut bool)` closure result). Mirrors the C
/// `multirange_bsearch_comparison` callback.
pub fn multirange_bsearch_match<K>(
    typcache: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    key: &K,
    cmp_func: impl Fn(&TypeCacheEntry, &RangeBound, &RangeBound, &K, &mut bool) -> PgResult<i32>,
) -> PgResult<bool> {
    let _ = (typcache, mr, key, cmp_func);
    todo!("port multirange_bsearch_match (multirangetypes.c:899)")
}

// ---------------------------------------------------------------------------
// contains-elem / contains-range / contained-by predicates.
// ---------------------------------------------------------------------------

/// `multirange_contains_elem_internal(rangetyp, mr, val)`
/// (multirangetypes.c:1708).
pub fn multirange_contains_elem_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    val: Datum,
) -> PgResult<bool> {
    let _ = (rangetyp, mr, val);
    todo!("port multirange_contains_elem_internal (multirangetypes.c:1708)")
}

/// `multirange_contains_range_internal(rangetyp, mr, r)`
/// (multirangetypes.c:1802).
pub fn multirange_contains_range_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    r: RangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr, r);
    todo!("port multirange_contains_range_internal (multirangetypes.c:1802)")
}

/// `range_contains_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:1830).
pub fn range_contains_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_contains_multirange_internal (multirangetypes.c:1830)")
}

/// `multirange_contains_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2267).
pub fn multirange_contains_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_contains_multirange_internal (multirangetypes.c:2267)")
}

// ---------------------------------------------------------------------------
// overlaps predicates.
// ---------------------------------------------------------------------------

/// `range_overlaps_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:1994).
pub fn range_overlaps_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_overlaps_multirange_internal (multirangetypes.c:1994)")
}

/// `multirange_overlaps_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2016).
pub fn multirange_overlaps_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_overlaps_multirange_internal (multirangetypes.c:2016)")
}

// ---------------------------------------------------------------------------
// position (overleft / overright / before / after) predicates.
// ---------------------------------------------------------------------------

/// `range_overleft_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2074).
pub fn range_overleft_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_overleft_multirange_internal (multirangetypes.c:2074)")
}

/// `range_overright_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2159).
pub fn range_overright_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_overright_multirange_internal (multirangetypes.c:2159)")
}

/// `range_before_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2403).
pub fn range_before_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_before_multirange_internal (multirangetypes.c:2403)")
}

/// `range_after_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2447).
pub fn range_after_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_after_multirange_internal (multirangetypes.c:2447)")
}

/// `multirange_before_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2425).
pub fn multirange_before_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_before_multirange_internal (multirangetypes.c:2425)")
}

/// `range_adjacent_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2472).
pub fn range_adjacent_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, r, mr);
    todo!("port range_adjacent_multirange_internal (multirangetypes.c:2472)")
}

// ---------------------------------------------------------------------------
// equality.
// ---------------------------------------------------------------------------

/// `multirange_eq_internal(rangetyp, mr1, mr2)` (multirangetypes.c:1865).
pub fn multirange_eq_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_eq_internal (multirangetypes.c:1865)")
}

/// `multirange_ne_internal(rangetyp, mr1, mr2)` (multirangetypes.c:1915).
pub fn multirange_ne_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let _ = (rangetyp, mr1, mr2);
    todo!("port multirange_ne_internal (multirangetypes.c:1915)")
}

// ---------------------------------------------------------------------------
// accessors: empty / lower / upper / lower_inc / upper_inc / lower_inf /
// upper_inf, and unnest.
// ---------------------------------------------------------------------------

/// `multirange_empty(PG_FUNCTION_ARGS)` (multirangetypes.c:1557).
pub fn multirange_empty(multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    let _ = multirange;
    todo!("port multirange_empty (multirangetypes.c:1557)")
}

/// `multirange_lower(PG_FUNCTION_ARGS)` (multirangetypes.c:1508): the lower
/// bound value of the first member range; SQL-NULL (`None`) if empty/unbounded.
pub fn multirange_lower(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<Option<Datum>> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_lower (multirangetypes.c:1508)")
}

/// `multirange_upper(PG_FUNCTION_ARGS)` (multirangetypes.c:1531).
pub fn multirange_upper(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<Option<Datum>> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_upper (multirangetypes.c:1531)")
}

/// `multirange_lower_inc(PG_FUNCTION_ARGS)` (multirangetypes.c:1566).
pub fn multirange_lower_inc(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_lower_inc (multirangetypes.c:1566)")
}

/// `multirange_upper_inc(PG_FUNCTION_ARGS)` (multirangetypes.c:1585).
pub fn multirange_upper_inc(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_upper_inc (multirangetypes.c:1585)")
}

/// `multirange_lower_inf(PG_FUNCTION_ARGS)` (multirangetypes.c:1604).
pub fn multirange_lower_inf(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_lower_inf (multirangetypes.c:1604)")
}

/// `multirange_upper_inf(PG_FUNCTION_ARGS)` (multirangetypes.c:1623).
pub fn multirange_upper_inf(rangetyp: &TypeCacheEntry, multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    let _ = (rangetyp, multirange);
    todo!("port multirange_upper_inf (multirangetypes.c:1623)")
}

/// `multirange_unnest(PG_FUNCTION_ARGS)` (multirangetypes.c:2714): a
/// set-returning function expanding a multirange into its member ranges.
pub fn multirange_unnest<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'mcx>,
) -> PgResult<Vec<RangeTypeP<'mcx>>> {
    let _ = (mcx, rangetyp, multirange);
    todo!("port multirange_unnest (multirangetypes.c:2714)")
}
