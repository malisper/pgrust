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
use types_error::{PgError, PgResult, ERROR};
use types_rangetypes::{MultirangeTypeP, RangeBound, RangeTypeP, RANGE_EMPTY};

use backend_utils_adt_rangetypes_seams::{
    bounds_adjacent, range_cmp_bounds, range_cmp_elem_values, range_deserialize, range_get_flags,
};

use crate::serialize_core::{multirange_get_bounds, multirange_get_range};

// ---------------------------------------------------------------------------
// Small inline helpers mirroring the C macros over the raw varlena handles.
// ---------------------------------------------------------------------------

/// `MultirangeTypeGetOid(mr)` (multirangetypes.h): the multirange's own type OID.
#[inline]
fn multirange_type_get_oid(mr: MultirangeTypeP<'_>) -> types_core::primitive::Oid {
    mr.multirangetypid()
}

/// `mr->rangeCount` — the number of member ranges.
#[inline]
fn multirange_range_count(mr: MultirangeTypeP<'_>) -> u32 {
    mr.range_count()
}

/// `MultirangeIsEmpty(mr)` (multirangetypes.h): a multirange is empty iff it has
/// no member ranges.
#[inline]
fn multirange_is_empty(mr: MultirangeTypeP<'_>) -> bool {
    multirange_range_count(mr) == 0
}

/// `RangeIsEmpty(r)` (rangetypes.h): the range carries the `RANGE_EMPTY` flag.
#[inline]
fn range_is_empty(r: RangeTypeP<'_>) -> bool {
    range_get_flags::call(r) & RANGE_EMPTY != 0
}

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
    if range_cmp_bounds::call(typcache, lower1, lower2)? >= 0
        && range_cmp_bounds::call(typcache, lower1, upper2)? <= 0
    {
        return Ok(true);
    }

    if range_cmp_bounds::call(typcache, lower2, lower1)? >= 0
        && range_cmp_bounds::call(typcache, lower2, upper1)? <= 0
    {
        return Ok(true);
    }

    Ok(false)
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
    if range_cmp_bounds::call(typcache, lower1, lower2)? <= 0
        && range_cmp_bounds::call(typcache, upper1, upper2)? >= 0
    {
        return Ok(true);
    }

    Ok(false)
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
    let mut l: u32 = 0;
    let mut u: u32 = multirange_range_count(mr);
    let mut matched = false;

    while l < u {
        let idx = (l + u) / 2;
        let (lower, upper) = multirange_get_bounds(typcache, mr, idx)?;
        let comparison = cmp_func(typcache, &lower, &upper, key, &mut matched)?;

        if comparison < 0 {
            u = idx;
        } else if comparison > 0 {
            l = idx + 1;
        } else {
            return Ok(matched);
        }
    }

    Ok(false)
}

// ---------------------------------------------------------------------------
// contains-elem / contains-range / contained-by predicates.
// ---------------------------------------------------------------------------

/// `multirange_elem_bsearch_comparison(typcache, lower, upper, key, match)`
/// (multirangetypes.c:1674): does the range `[lower, upper]` contain the key
/// element value (`*key`)?
fn multirange_elem_bsearch_comparison(
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    key: &Datum,
    matched: &mut bool,
) -> PgResult<i32> {
    let val = *key;

    // C: `DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
    // typcache->rng_collation, lower->val, *key))`. The element values may be
    // by-reference (e.g. `numeric` of a `nummultirange`), so go through the
    // by-reference-capable element-value compare (canonical-Datum lane) rather
    // than the bare-word `function_call2_coll`, which would leave the by-ref
    // referent empty ("by-ref `numeric` arg missing from by-ref lane").
    if !lower.infinite {
        let cmp = range_cmp_elem_values::call(typcache, lower.val, val)?;
        if cmp > 0 || (cmp == 0 && !lower.inclusive) {
            return Ok(-1);
        }
    }

    if !upper.infinite {
        let cmp = range_cmp_elem_values::call(typcache, upper.val, val)?;
        if cmp < 0 || (cmp == 0 && !upper.inclusive) {
            return Ok(1);
        }
    }

    *matched = true;
    Ok(0)
}

/// `multirange_contains_elem_internal(rangetyp, mr, val)`
/// (multirangetypes.c:1708).
pub fn multirange_contains_elem_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    val: Datum,
) -> PgResult<bool> {
    if multirange_is_empty(mr) {
        return Ok(false);
    }

    multirange_bsearch_match(rangetyp, mr, &val, multirange_elem_bsearch_comparison)
}

/// `multirange_range_contains_bsearch_comparison` (multirangetypes.c:1774): the
/// key is the `[keyLower, keyUpper]` bound pair.
fn multirange_range_contains_bsearch_comparison(
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    key: &(RangeBound, RangeBound),
    matched: &mut bool,
) -> PgResult<i32> {
    let (key_lower, key_upper) = key;

    // Check if key range is strictly in the left or in the right.
    if range_cmp_bounds::call(typcache, key_upper, lower)? < 0 {
        return Ok(-1);
    }
    if range_cmp_bounds::call(typcache, key_lower, upper)? > 0 {
        return Ok(1);
    }

    // Found an overlapping range; check real containment.  Either way we stop.
    *matched = range_bounds_contains(typcache, lower, upper, key_lower, key_upper)?;

    Ok(0)
}

/// `multirange_contains_range_internal(rangetyp, mr, r)`
/// (multirangetypes.c:1802).
pub fn multirange_contains_range_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    r: RangeTypeP<'_>,
) -> PgResult<bool> {
    // Every multirange contains an infinite number of empty ranges, even an
    // empty one.
    if range_is_empty(r) {
        return Ok(true);
    }

    if multirange_is_empty(mr) {
        return Ok(false);
    }

    let (lower, upper, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    multirange_bsearch_match(rangetyp, mr, &(lower, upper), |tc, l, u, key, m| {
        multirange_range_contains_bsearch_comparison(tc, l, u, key, m)
    })
}

/// `range_contains_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:1830).
pub fn range_contains_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    // Every range contains an infinite number of empty multiranges, even an
    // empty one.
    if multirange_is_empty(mr) {
        return Ok(true);
    }

    if range_is_empty(r) {
        return Ok(false);
    }

    // Range contains multirange iff it contains its union range.
    let (lower1, upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);
    let (lower2, _tmp) = multirange_get_bounds(rangetyp, mr, 0)?;
    let (_tmp2, upper2) = multirange_get_bounds(rangetyp, mr, multirange_range_count(mr) - 1)?;

    range_bounds_contains(rangetyp, &lower1, &upper1, &lower2, &upper2)
}

/// `multirange_contains_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2267).
pub fn multirange_contains_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    let range_count1 = multirange_range_count(mr1);
    let range_count2 = multirange_range_count(mr2);

    // We follow the same logic for empties as ranges: an empty multirange
    // contains an empty range/multirange; an empty multirange can't contain any
    // other range/multirange; an empty multirange is contained by any other.
    if range_count2 == 0 {
        return Ok(true);
    }
    if range_count1 == 0 {
        return Ok(false);
    }

    // Every range in mr2 must be contained by some range in mr1. To avoid
    // O(n^2) we walk through both ranges in tandem.
    let mut i1: u32 = 0;
    let (mut lower1, mut upper1) = multirange_get_bounds(rangetyp, mr1, i1)?;
    for i2 in 0..range_count2 {
        let (lower2, upper2) = multirange_get_bounds(rangetyp, mr2, i2)?;

        // Discard r1s while r1 << r2.
        while range_cmp_bounds::call(rangetyp, &upper1, &lower2)? < 0 {
            i1 += 1;
            if i1 >= range_count1 {
                return Ok(false);
            }
            let (l, u) = multirange_get_bounds(rangetyp, mr1, i1)?;
            lower1 = l;
            upper1 = u;
        }

        // If r1 @> r2 go to the next r2, otherwise return false (since every
        // r1[n] and r1[n+1] must have a gap).
        if !range_bounds_contains(rangetyp, &lower1, &upper1, &lower2, &upper2)? {
            return Ok(false);
        }
    }

    // All ranges in mr2 are satisfied.
    Ok(true)
}

// ---------------------------------------------------------------------------
// overlaps predicates.
// ---------------------------------------------------------------------------

/// `multirange_range_overlaps_bsearch_comparison` (multirangetypes.c:1976).
fn multirange_range_overlaps_bsearch_comparison(
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    key: &(RangeBound, RangeBound),
    matched: &mut bool,
) -> PgResult<i32> {
    let (key_lower, key_upper) = key;

    if range_cmp_bounds::call(typcache, key_upper, lower)? < 0 {
        return Ok(-1);
    }
    if range_cmp_bounds::call(typcache, key_lower, upper)? > 0 {
        return Ok(1);
    }

    *matched = true;
    Ok(0)
}

/// `range_overlaps_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:1994).
pub fn range_overlaps_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    // Empties never overlap, even with empties.
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (lower, upper, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    multirange_bsearch_match(rangetyp, mr, &(lower, upper), |tc, l, u, key, m| {
        multirange_range_overlaps_bsearch_comparison(tc, l, u, key, m)
    })
}

/// `multirange_overlaps_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2016).
pub fn multirange_overlaps_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    // Empties never overlap, even with empties.
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }

    let range_count1 = multirange_range_count(mr1);
    let range_count2 = multirange_range_count(mr2);

    // Every range in mr1 gets a chance to overlap with the ranges in mr2, but we
    // can use their ordering to avoid O(n^2).
    let mut i1: u32 = 0;
    let (mut lower1, mut upper1) = multirange_get_bounds(rangetyp, mr1, i1)?;
    for i2 in 0..range_count2 {
        let (lower2, upper2) = multirange_get_bounds(rangetyp, mr2, i2)?;

        // Discard r1s while r1 << r2.
        while range_cmp_bounds::call(rangetyp, &upper1, &lower2)? < 0 {
            i1 += 1;
            if i1 >= range_count1 {
                return Ok(false);
            }
            let (l, u) = multirange_get_bounds(rangetyp, mr1, i1)?;
            lower1 = l;
            upper1 = u;
        }

        // If r1 && r2, we're done, otherwise we failed to find an overlap for
        // r2, so go to the next one.
        if range_bounds_overlaps(rangetyp, &lower1, &upper1, &lower2, &upper2)? {
            return Ok(true);
        }
    }

    // We looked through all of mr2 without finding an overlap.
    Ok(false)
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
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (_lower1, upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);
    let (_lower2, upper2) = multirange_get_bounds(rangetyp, mr, multirange_range_count(mr) - 1)?;

    Ok(range_cmp_bounds::call(rangetyp, &upper1, &upper2)? <= 0)
}

/// `range_overright_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2159).
pub fn range_overright_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (lower1, _upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);
    let (lower2, _upper2) = multirange_get_bounds(rangetyp, mr, 0)?;

    Ok(range_cmp_bounds::call(rangetyp, &lower1, &lower2)? >= 0)
}

/// `range_before_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2403).
pub fn range_before_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (_lower1, upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    let (lower2, _upper2) = multirange_get_bounds(rangetyp, mr, 0)?;

    Ok(range_cmp_bounds::call(rangetyp, &upper1, &lower2)? < 0)
}

/// `range_after_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2447).
pub fn range_after_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (lower1, _upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    let range_count = multirange_range_count(mr);
    let (_lower2, upper2) = multirange_get_bounds(rangetyp, mr, range_count - 1)?;

    Ok(range_cmp_bounds::call(rangetyp, &lower1, &upper2)? > 0)
}

/// `multirange_before_multirange_internal(rangetyp, mr1, mr2)`
/// (multirangetypes.c:2425).
pub fn multirange_before_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }

    let (_lower1, upper1) = multirange_get_bounds(rangetyp, mr1, multirange_range_count(mr1) - 1)?;
    let (lower2, _upper2) = multirange_get_bounds(rangetyp, mr2, 0)?;

    Ok(range_cmp_bounds::call(rangetyp, &upper1, &lower2)? < 0)
}

/// `range_adjacent_multirange_internal(rangetyp, r, mr)`
/// (multirangetypes.c:2472).
pub fn range_adjacent_multirange_internal(
    rangetyp: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }

    let (lower1, upper1, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    let range_count = multirange_range_count(mr);
    let (lower2, mut upper2) = multirange_get_bounds(rangetyp, mr, 0)?;

    if bounds_adjacent::call(rangetyp, upper1, lower2)? {
        return Ok(true);
    }

    // The C reassigns both lower2 and upper2 here, but only upper2 is read below.
    if range_count > 1 {
        let (_l, u) = multirange_get_bounds(rangetyp, mr, range_count - 1)?;
        upper2 = u;
    }

    if bounds_adjacent::call(rangetyp, upper2, lower1)? {
        return Ok(true);
    }

    Ok(false)
}

/// `multirange_overleft_range(mr, r)` (multirangetypes.c:2109): the entry point's
/// inlined logic (no C `*_internal` helper). True iff `mr`'s last upper bound is
/// `<=` `r`'s upper bound.
pub fn multirange_overleft_range_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    r: RangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr) || range_is_empty(r) {
        return Ok(false);
    }

    let (_lower1, upper1) = multirange_get_bounds(rangetyp, mr, multirange_range_count(mr) - 1)?;
    let (_lower2, upper2, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    Ok(range_cmp_bounds::call(rangetyp, &upper1, &upper2)? <= 0)
}

/// `multirange_overleft_multirange(mr1, mr2)` (multirangetypes.c:2134): inlined.
/// True iff `mr1`'s last upper bound is `<=` `mr2`'s last upper bound.
pub fn multirange_overleft_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }

    let (_lower1, upper1) = multirange_get_bounds(rangetyp, mr1, multirange_range_count(mr1) - 1)?;
    let (_lower2, upper2) = multirange_get_bounds(rangetyp, mr2, multirange_range_count(mr2) - 1)?;

    Ok(range_cmp_bounds::call(rangetyp, &upper1, &upper2)? <= 0)
}

/// `multirange_overright_range(mr, r)` (multirangetypes.c:2192): inlined. True iff
/// `mr`'s first lower bound is `>=` `r`'s lower bound.
pub fn multirange_overright_range_internal(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    r: RangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr) || range_is_empty(r) {
        return Ok(false);
    }

    let (lower1, _upper1) = multirange_get_bounds(rangetyp, mr, 0)?;
    let (lower2, _upper2, empty) = range_deserialize::call(rangetyp, r)?;
    debug_assert!(!empty);

    Ok(range_cmp_bounds::call(rangetyp, &lower1, &lower2)? >= 0)
}

/// `multirange_overright_multirange(mr1, mr2)` (multirangetypes.c:2216): inlined.
/// True iff `mr1`'s first lower bound is `>=` `mr2`'s first lower bound.
pub fn multirange_overright_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }

    let (lower1, _upper1) = multirange_get_bounds(rangetyp, mr1, 0)?;
    let (lower2, _upper2) = multirange_get_bounds(rangetyp, mr2, 0)?;

    Ok(range_cmp_bounds::call(rangetyp, &lower1, &lower2)? >= 0)
}

/// `multirange_adjacent_multirange(mr1, mr2)` (multirangetypes.c:2535): inlined.
pub fn multirange_adjacent_multirange_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }

    let range_count1 = multirange_range_count(mr1);
    let range_count2 = multirange_range_count(mr2);
    let (mut lower1, upper1) = multirange_get_bounds(rangetyp, mr1, range_count1 - 1)?;
    let (lower2, mut upper2) = multirange_get_bounds(rangetyp, mr2, 0)?;

    if bounds_adjacent::call(rangetyp, upper1, lower2)? {
        return Ok(true);
    }

    // C reassigns lower1/upper1 from mr1's first range and lower2/upper2 from
    // mr2's last range; below only mr1's first lower and mr2's last upper read.
    if range_count1 > 1 {
        let (l, _u) = multirange_get_bounds(rangetyp, mr1, 0)?;
        lower1 = l;
    }
    if range_count2 > 1 {
        let (_l, u) = multirange_get_bounds(rangetyp, mr2, range_count2 - 1)?;
        upper2 = u;
    }

    if bounds_adjacent::call(rangetyp, upper2, lower1)? {
        return Ok(true);
    }
    Ok(false)
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
    // Different types should be prevented by ANYMULTIRANGE matching rules.
    if multirange_type_get_oid(mr1) != multirange_type_get_oid(mr2) {
        return Err(PgError::new(ERROR, "multirange types do not match"));
    }

    let range_count_1 = multirange_range_count(mr1);
    let range_count_2 = multirange_range_count(mr2);

    if range_count_1 != range_count_2 {
        return Ok(false);
    }

    for i in 0..range_count_1 {
        let (lower1, upper1) = multirange_get_bounds(rangetyp, mr1, i)?;
        let (lower2, upper2) = multirange_get_bounds(rangetyp, mr2, i)?;

        if range_cmp_bounds::call(rangetyp, &lower1, &lower2)? != 0
            || range_cmp_bounds::call(rangetyp, &upper1, &upper2)? != 0
        {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `multirange_ne_internal(rangetyp, mr1, mr2)` (multirangetypes.c:1915).
pub fn multirange_ne_internal(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(!multirange_eq_internal(rangetyp, mr1, mr2)?)
}

// ---------------------------------------------------------------------------
// accessors: empty / lower / upper / lower_inc / upper_inc / lower_inf /
// upper_inf, and unnest.
// ---------------------------------------------------------------------------

/// `multirange_empty(PG_FUNCTION_ARGS)` (multirangetypes.c:1557).
pub fn multirange_empty(multirange: MultirangeTypeP<'_>) -> PgResult<bool> {
    Ok(multirange_is_empty(multirange))
}

/// `multirange_lower(PG_FUNCTION_ARGS)` (multirangetypes.c:1508): the lower
/// bound value of the first member range; SQL-NULL (`None`) if empty/unbounded.
pub fn multirange_lower(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<Option<Datum>> {
    if multirange_is_empty(multirange) {
        return Ok(None);
    }

    let (lower, _upper) = multirange_get_bounds(rangetyp, multirange, 0)?;

    if !lower.infinite {
        Ok(Some(lower.val))
    } else {
        Ok(None)
    }
}

/// `multirange_upper(PG_FUNCTION_ARGS)` (multirangetypes.c:1531).
pub fn multirange_upper(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<Option<Datum>> {
    if multirange_is_empty(multirange) {
        return Ok(None);
    }

    let (_lower, upper) =
        multirange_get_bounds(rangetyp, multirange, multirange_range_count(multirange) - 1)?;

    if !upper.infinite {
        Ok(Some(upper.val))
    } else {
        Ok(None)
    }
}

/// `multirange_lower_inc(PG_FUNCTION_ARGS)` (multirangetypes.c:1566).
pub fn multirange_lower_inc(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(multirange) {
        return Ok(false);
    }

    let (lower, _upper) = multirange_get_bounds(rangetyp, multirange, 0)?;
    Ok(lower.inclusive)
}

/// `multirange_upper_inc(PG_FUNCTION_ARGS)` (multirangetypes.c:1585).
pub fn multirange_upper_inc(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(multirange) {
        return Ok(false);
    }

    let (_lower, upper) =
        multirange_get_bounds(rangetyp, multirange, multirange_range_count(multirange) - 1)?;
    Ok(upper.inclusive)
}

/// `multirange_lower_inf(PG_FUNCTION_ARGS)` (multirangetypes.c:1604).
pub fn multirange_lower_inf(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(multirange) {
        return Ok(false);
    }

    let (lower, _upper) = multirange_get_bounds(rangetyp, multirange, 0)?;
    Ok(lower.infinite)
}

/// `multirange_upper_inf(PG_FUNCTION_ARGS)` (multirangetypes.c:1623).
pub fn multirange_upper_inf(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if multirange_is_empty(multirange) {
        return Ok(false);
    }

    let (_lower, upper) =
        multirange_get_bounds(rangetyp, multirange, multirange_range_count(multirange) - 1)?;
    Ok(upper.infinite)
}

/// `multirange_unnest(PG_FUNCTION_ARGS)` (multirangetypes.c:2714): a
/// set-returning function expanding a multirange into its member ranges.
pub fn multirange_unnest<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'mcx>,
) -> PgResult<Vec<RangeTypeP<'mcx>>> {
    // The C is an SRF that, on each call, returns `multirange_get_range(rngtype,
    // mr, index)` for `index` in `0 .. rangeCount`. We materialize the full
    // sequence the SRF would yield, in order.
    let range_count = multirange_range_count(multirange);
    let mut ranges = Vec::with_capacity(range_count as usize);
    for index in 0..range_count {
        ranges.push(multirange_get_range(mcx, rangetyp, multirange, index as i32)?);
    }
    Ok(ranges)
}

/// SRF-friendly entry point for `multirange_unnest` (multirangetypes.c:2714)
/// driven over the executor frame: parse the multirange varlena image, resolve
/// the member-range typcache (C: `multirange_get_typcache(fcinfo,
/// MultirangeTypeGetOid(mr))->rngtype`), and materialize the whole member-range
/// sequence as serialized `RangeType` varlena images — the on-disk
/// `RangeTypePGetDatum` byte image of each member range, in order, exactly what
/// each `SRF_RETURN_NEXT` would hand back. Keeps the one unavoidable raw
/// `VARSIZE` read of a `RangeTypeP` inside this ADT crate (which owns the
/// serialized layout), handing the executor-frame SRF a clean `Vec<Vec<u8>>` it
/// crosses on the by-ref lane one element per call.
pub fn multirange_unnest_images<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    // DatumGetMultirangeTypeP(PG_GETARG_DATUM(0)): copy the by-ref varlena image
    // into `mcx` (so the detoasted handle below lives for `'mcx`) and hand its
    // address to `datum_get_multirange_type_p` as the argument Datum word.
    let word = {
        use core::alloc::Layout;
        use mcx::Allocator;
        mcx::check_alloc_size(image.len())?;
        let layout = Layout::from_size_align(image.len().max(1), 8)
            .expect("valid MultirangeType image layout");
        let block = mcx.allocate(layout).map_err(|_| mcx.oom(image.len()))?;
        let dst = block.as_ptr() as *mut u8;
        // SAFETY: `dst` heads a freshly allocated `image.len()`-byte region.
        unsafe {
            core::ptr::copy_nonoverlapping(image.as_ptr(), dst, image.len());
        }
        Datum::from_usize(dst as usize)
    };
    let multirange = crate::typcache_io::datum_get_multirange_type_p(mcx, word)?;

    // typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    // rangetyp = typcache->rngtype;
    let mtc = crate::typcache_io::multirange_get_typcache(multirange_type_get_oid(multirange))?;
    let rangetyp = *mtc
        .rngtype
        .expect("multirange typcache has a range subtype");

    let ranges = multirange_unnest(mcx, &rangetyp, multirange)?;

    let mut out = Vec::with_capacity(ranges.len());
    for r in ranges {
        // RangeType is a plain 4-byte-header uncompressed varlena (range_serialize
        // writes SET_VARSIZE), so its byte length is VARSIZE_4B(ptr) = (len >> 2).
        // SAFETY: `r.ptr` is a fully-detoasted RangeType image allocated in `mcx`
        // (by `multirange_get_range`); the low 30 bits of its 4-byte header carry
        // the total image length.
        let size = unsafe {
            let raw = (r.ptr as *const u8).cast::<u32>().read_unaligned();
            (raw >> 2) as usize
        };
        // SAFETY: the `size`-byte image is contiguous and live for `'mcx`.
        let bytes = unsafe { core::slice::from_raw_parts(r.ptr as *const u8, size) };
        out.push(bytes.to_vec());
    }
    Ok(out)
}
