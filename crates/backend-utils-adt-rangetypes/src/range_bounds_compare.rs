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

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::{Oid, OidIsValid};
use types_datum::datum::Datum;
use types_error::{PgError, PgResult};
use types_rangetypes::{RangeBound, RangeTypeP, RANGE_EMPTY};

use crate::range_repr_serialize::{make_range, range_deserialize, range_get_flags};

/// Build the canonical, by-reference-capable `Datum<'mcx>` for a range *element*
/// value that the subtype `cmp` support function must receive.
///
/// In C the element value rides a bare `Datum` (`RangeBound.val`): for a
/// by-value subtype the word IS the value; for a by-reference subtype the word
/// is a `Pointer` to the value's (detoasted) image, which the support function
/// dereferences via `PG_GETARG_*`. The owned fmgr boundary, however, never
/// carries a by-reference argument on the bare `args[i].value` word — it must
/// ride the `ref_args` side channel (a `RefPayload::Varlena`), reconstructed
/// from the canonical [`Datum::ByRef`] arm. So a bare pointer word handed to the
/// bare-word `function_call2_coll` seam left the by-reference arg's referent
/// empty and the support function panicked ("by-ref `numeric` arg missing from
/// by-ref lane").
///
/// This bridges the gap exactly as the index-key Datum fix (4e37ce3c7) does for
/// `FormIndexDatum`: lift the bare element word into the rich canonical `Datum`
/// (`ByVal` for a by-value subtype; `ByRef` over a copy of the element's image
/// for a by-reference subtype) so the subsequent `*_coll_datum` dispatch
/// marshals it onto the proper lane. The element type metadata
/// (`typbyval`/`typlen`) comes off `typcache->rngelemtype`.
pub(crate) fn elem_word_to_canon<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    word: Datum,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;

    let elem = typcache
        .rngelemtype
        .as_ref()
        .expect("range element compare: typcache->rngelemtype must be set for a range type");
    if elem.typbyval {
        // By-value subtype: the machine word IS the value.
        return Ok(CanonDatum::from_usize(word.as_usize()));
    }

    // By-reference subtype: the word is a Pointer to the element's image. Copy
    // its bytes into `mcx` and carry them on the canonical `ByRef` arm. For a
    // varlena subtype (typlen == -1) the image length is VARSIZE_ANY; for a
    // fixed-length by-reference subtype (typlen > 0) it is exactly `typlen`
    // bytes. (`typlen == -2`, a cstring, is never a range element type.)
    let ptr = word.as_usize() as *const u8;
    if ptr.is_null() {
        // A NULL pointer-Datum maps to a by-value zero word; the callers guard
        // NULL / infinite bounds before reaching the value compare.
        return Ok(CanonDatum::from_usize(0));
    }
    // SAFETY: for a by-reference range element the bound `Datum` is a live
    // pointer into a detoasted image (the range varlena, or an mcx-materialized
    // element) that stays valid across this call, exactly as C dereferences it.
    // `byref_elem_headerful_image` un-packs a short-header bound (the form
    // `datum_write` may store) into the canonical 4-byte-header varlena the fmgr
    // by-reference lane and the element value cores require.
    let image = unsafe {
        crate::range_repr_serialize::byref_elem_headerful_image(ptr, elem.typlen)
    };
    Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &image).map_err(|_| {
        PgError::error(
            "range element compare: out of memory copying by-reference element image",
        )
    })?))
}

/// Invoke the subtype's 2-arg `cmp` support function over two range *element*
/// bound words, threading by-reference elements onto the proper fmgr lane (see
/// [`elem_word_to_canon`]). Returns the `int4` comparison result.
fn cmp_elem_words(typcache: &TypeCacheEntry, a: Datum, b: Datum) -> PgResult<i32> {
    let scratch = mcx::MemoryContext::new_bump("range element cmp");
    let mcx = scratch.mcx();
    let av = elem_word_to_canon(mcx, typcache, a)?;
    let bv = elem_word_to_canon(mcx, typcache, b)?;
    let r = backend_utils_fmgr_fmgr_seams::function_call2_coll_datum::call(
        mcx,
        typcache.rng_cmp_proc_finfo.fn_oid,
        typcache.rng_collation,
        av,
        bv,
    )?;
    Ok(r.as_i32())
}

/// Seam `range_elem_word_to_canon`: lift a range element bound value word into the
/// rich canonical `Datum` (`ByVal`/`ByRef`). A thin re-export of the in-crate
/// [`elem_word_to_canon`] so the sibling multirange unit's element `hash` lane can
/// reuse the exact by-reference handling.
pub fn range_elem_word_to_canon<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    word: Datum,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    elem_word_to_canon(mcx, typcache, word)
}

/// Seam `range_cmp_elem_values`: compare two range *element* bound values
/// (`RangeBound.val` words) via the subtype's `cmp` support function, threading a
/// by-reference element (e.g. a `numeric` pointer) onto the proper fmgr lane.
/// Mirrors the in-crate [`cmp_elem_words`]; exposed so the sibling multirange
/// unit's element-bsearch comparison can reuse the exact by-reference handling.
pub fn range_cmp_elem_values(typcache: &TypeCacheEntry, v1: Datum, v2: Datum) -> PgResult<i32> {
    cmp_elem_words(typcache, v1, v2)
}

/// `RangeTypeGetOid(r)` (rangetypes.h:35): `(r)->rangetypid`.
#[inline]
fn range_type_get_oid(r: RangeTypeP<'_>) -> Oid {
    // SAFETY: `r` is a detoasted `RangeType *` whose fixed header (`vl_len_`,
    // `rangetypid`) is directly readable, as in C.
    r.rangetypid()
}

/// `RangeIsEmpty(r)` (rangetypes.h:56): `(range_get_flags(r) & RANGE_EMPTY) != 0`.
#[inline]
fn range_is_empty(r: RangeTypeP<'_>) -> bool {
    (range_get_flags(r) & RANGE_EMPTY) != 0
}

/// `elog(ERROR, "range types do not match")` (rangetypes.c): the ANYRANGE
/// invariant violation raised by every two-range kernel.
#[inline]
fn range_types_do_not_match<T>() -> PgResult<T> {
    Err(PgError::error("range types do not match"))
}

/// `TYPECACHE_RANGE_INFO` (typcache.h): the flag selecting the range-info fields
/// (`rngelemtype` / `rng_collation` / `rng_cmp_proc_finfo` /
/// `rng_canonical_finfo` / `rng_subdiff_finfo`) of a range type's
/// `TypeCacheEntry`. Value matches `backend-utils-cache-typcache`'s
/// `TYPECACHE_RANGE_INFO`.
const TYPECACHE_RANGE_INFO: i32 = 0x00800;

/// `range_get_typcache(fcinfo, rngtypid)` (rangetypes.c:1767): the cached
/// `TypeCacheEntry` for the range type. Owns the inward seam.
///
/// C body is `lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO)` plus the
/// `rngelemtype == NULL` "type %u is not a range type" guard, caching the entry
/// in `fcinfo->flinfo->fn_extra`. `lookup_type_cache` is owned by the
/// genuinely-unported `backend-utils-cache-typcache`; its `lookup_type_cache_entry`
/// seam hands back the range-bearing `types_cache::TypeCacheEntry`
/// (`rng_cmp_proc_finfo` / `rng_collation` / `rngelemtype` carried), the same
/// seam the sibling `multirangetypes::multirange_get_typcache` consumes. The
/// owned model re-looks-up each call (the cache is the typcache's own job) and
/// returns the entry by value.
pub fn range_get_typcache(rngtypid: Oid) -> PgResult<TypeCacheEntry> {
    // typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
    let typcache = backend_utils_cache_typcache_seams::lookup_type_cache_entry::call(
        rngtypid,
        TYPECACHE_RANGE_INFO,
    )?;

    // if (typcache->rngelemtype == NULL)
    //     elog(ERROR, "type %u is not a range type", rngtypid);
    if typcache.rngelemtype.is_none() {
        return Err(PgError::error(format!(
            "type {rngtypid} is not a range type"
        )));
    }

    Ok(typcache)
}

/// `range_cmp_bounds(typcache, b1, b2)` (rangetypes.c:2080): compare two bounds
/// using the subtype's `cmp` support fn (fmgr seam), tie-breaking on
/// inclusivity/lower/infinite per the C. Owns the inward seam.
pub fn range_cmp_bounds(
    typcache: &TypeCacheEntry,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i32> {
    let result: i32;

    /*
     * First, handle cases involving infinity, which don't require invoking
     * the comparison proc.
     */
    if b1.infinite && b2.infinite {
        /*
         * Both are infinity, so they are equal unless one is lower and the
         * other not.
         */
        if b1.lower == b2.lower {
            return Ok(0);
        } else {
            return Ok(if b1.lower { -1 } else { 1 });
        }
    } else if b1.infinite {
        return Ok(if b1.lower { -1 } else { 1 });
    } else if b2.infinite {
        return Ok(if b2.lower { 1 } else { -1 });
    }

    /*
     * Both boundaries are finite, so compare the held values.
     */
    result = cmp_elem_words(typcache, b1.val, b2.val)?;

    /*
     * If the comparison is anything other than equal, we're done. If they
     * compare equal though, we still have to consider whether the boundaries
     * are inclusive or exclusive.
     */
    if result == 0 {
        if !b1.inclusive && !b2.inclusive {
            /* both are exclusive */
            if b1.lower == b2.lower {
                return Ok(0);
            } else {
                return Ok(if b1.lower { 1 } else { -1 });
            }
        } else if !b1.inclusive {
            return Ok(if b1.lower { 1 } else { -1 });
        } else if !b2.inclusive {
            return Ok(if b2.lower { -1 } else { 1 });
        } else {
            /*
             * Both are inclusive and the values held are equal, so they are
             * equal regardless of whether they are upper or lower boundaries,
             * or a mix.
             */
            return Ok(0);
        }
    }

    Ok(result)
}

/// `range_cmp_bound_values(typcache, b1, b2)` (rangetypes.c:2154): like
/// `range_cmp_bounds` but compares only the bound values (ignores which side).
pub fn range_cmp_bound_values(
    typcache: &TypeCacheEntry,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i32> {
    /*
     * First, handle cases involving infinity, which don't require invoking
     * the comparison proc.
     */
    if b1.infinite && b2.infinite {
        /*
         * Both are infinity, so they are equal unless one is lower and the
         * other not.
         */
        if b1.lower == b2.lower {
            return Ok(0);
        } else {
            return Ok(if b1.lower { -1 } else { 1 });
        }
    } else if b1.infinite {
        return Ok(if b1.lower { -1 } else { 1 });
    } else if b2.infinite {
        return Ok(if b2.lower { 1 } else { -1 });
    }

    /*
     * Both boundaries are finite, so compare the held values.
     */
    cmp_elem_words(typcache, b1.val, b2.val)
}

/// `range_compare(arg1, arg2)` body (rangetypes.c:2193): total order over two
/// ranges (empty < non-empty; then lower, then upper).
pub fn range_compare(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<i32> {
    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    let cmp = if empty1 && empty2 {
        0
    } else if empty1 {
        -1
    } else if empty2 {
        1
    } else {
        let c = range_cmp_bounds(typcache, &lower1, &lower2)?;
        if c == 0 {
            range_cmp_bounds(typcache, &upper1, &upper2)?
        } else {
            c
        }
    };

    Ok(cmp)
}

/// `bounds_adjacent(typcache, boundA, boundB)` (rangetypes.c:2759): whether an
/// upper bound and a lower bound are exactly adjacent.
///
/// C allocates a transient probe range via `make_range` (CurrentMemoryContext);
/// `mcx` is that context.
pub fn bounds_adjacent<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    mut bound_a: RangeBound,
    mut bound_b: RangeBound,
) -> PgResult<bool> {
    debug_assert!(!bound_a.lower && bound_b.lower);

    let cmp = range_cmp_bound_values(typcache, &bound_a, &bound_b)?;
    if cmp < 0 {
        /*
         * Bounds do not overlap; see if there are points in between.
         */

        /* in a continuous subtype, there are assumed to be points between */
        if !OidIsValid(typcache.rng_canonical_finfo.fn_oid) {
            return Ok(false);
        }

        /*
         * The bounds are of a discrete range type; so make a range A..B and
         * see if it's empty.
         */

        /* flip the inclusion flags */
        bound_a.inclusive = !bound_a.inclusive;
        bound_b.inclusive = !bound_b.inclusive;
        /* change upper/lower labels to avoid Assert failures */
        bound_a.lower = true;
        bound_b.lower = false;
        let r = make_range(mcx, typcache, &bound_a, &bound_b, false)?;
        Ok(range_is_empty(r))
    } else if cmp == 0 {
        Ok(bound_a.inclusive != bound_b.inclusive)
    } else {
        Ok(false) /* bounds overlap */
    }
}

// --- boolean predicate kernels (rangetypes.c) -----------------------------

/// `range_contains_elem_internal(typcache, r, val)` (rangetypes.c:2691).
pub fn range_contains_elem_internal(
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    val: Datum,
) -> PgResult<bool> {
    let (lower, upper, empty) = range_deserialize(typcache, r)?;

    if empty {
        return Ok(false);
    }

    if !lower.infinite {
        let cmp = cmp_elem_words(typcache, lower.val, val)?;
        if cmp > 0 {
            return Ok(false);
        }
        if cmp == 0 && !lower.inclusive {
            return Ok(false);
        }
    }

    if !upper.infinite {
        let cmp = cmp_elem_words(typcache, upper.val, val)?;
        if cmp < 0 {
            return Ok(false);
        }
        if cmp == 0 && !upper.inclusive {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `range_eq_internal(typcache, r1, r2)` (rangetypes.c:575).
pub fn range_eq_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    if empty1 && empty2 {
        return Ok(true);
    }
    if empty1 != empty2 {
        return Ok(false);
    }

    if range_cmp_bounds(typcache, &lower1, &lower2)? != 0 {
        return Ok(false);
    }

    if range_cmp_bounds(typcache, &upper1, &upper2)? != 0 {
        return Ok(false);
    }

    Ok(true)
}

/// `range_ne_internal(typcache, r1, r2)` (rangetypes.c:620).
pub fn range_ne_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(!range_eq_internal(typcache, r1, r2)?)
}

/// `range_contains_internal(typcache, r1, r2)` (rangetypes.c:2650).
pub fn range_contains_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* If either range is empty, the answer is easy */
    if empty2 {
        return Ok(true);
    } else if empty1 {
        return Ok(false);
    }

    /* Else we must have lower1 <= lower2 and upper1 >= upper2 */
    if range_cmp_bounds(typcache, &lower1, &lower2)? > 0 {
        return Ok(false);
    }
    if range_cmp_bounds(typcache, &upper1, &upper2)? < 0 {
        return Ok(false);
    }

    Ok(true)
}

/// `range_contained_by_internal(typcache, r1, r2)` (rangetypes.c:2682).
pub fn range_contained_by_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    range_contains_internal(typcache, r2, r1)
}

/// `range_before_internal(typcache, r1, r2)` (rangetypes.c:666).
pub fn range_before_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (_lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, _upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    Ok(range_cmp_bounds(typcache, &upper1, &lower2)? < 0)
}

/// `range_after_internal(typcache, r1, r2)` (rangetypes.c:704).
pub fn range_after_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, _upper1, empty1) = range_deserialize(typcache, r1)?;
    let (_lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    Ok(range_cmp_bounds(typcache, &lower1, &upper2)? > 0)
}

/// `range_adjacent_internal(typcache, r1, r2)` (rangetypes.c:800).
///
/// C's `bounds_adjacent` probe allocates in CurrentMemoryContext; `mcx` is that
/// context.
pub fn range_adjacent_internal<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range is not adjacent to any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    /*
     * Given two ranges A..B and C..D, the ranges are adjacent if and only if
     * B is adjacent to C, or D is adjacent to A.
     */
    Ok(bounds_adjacent(mcx, typcache, upper1, lower2)?
        || bounds_adjacent(mcx, typcache, upper2, lower1)?)
}

/// `range_overlaps_internal(typcache, r1, r2)` (rangetypes.c:843).
pub fn range_overlaps_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range does not overlap any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    if range_cmp_bounds(typcache, &lower1, &lower2)? >= 0
        && range_cmp_bounds(typcache, &lower1, &upper2)? <= 0
    {
        return Ok(true);
    }

    if range_cmp_bounds(typcache, &lower2, &lower1)? >= 0
        && range_cmp_bounds(typcache, &lower2, &upper1)? <= 0
    {
        return Ok(true);
    }

    Ok(false)
}

/// `range_overleft_internal(typcache, r1, r2)` (rangetypes.c:889).
pub fn range_overleft_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (_lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (_lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    if range_cmp_bounds(typcache, &upper1, &upper2)? <= 0 {
        return Ok(true);
    }

    Ok(false)
}

/// `range_overright_internal(typcache, r1, r2)` (rangetypes.c:930).
pub fn range_overright_internal(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    /* Different types should be prevented by ANYRANGE matching rules */
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return range_types_do_not_match();
    }

    let (lower1, _upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, _upper2, empty2) = range_deserialize(typcache, r2)?;

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return Ok(false);
    }

    if range_cmp_bounds(typcache, &lower1, &lower2)? >= 0 {
        return Ok(true);
    }

    Ok(false)
}

/// Inward seam shape for `bounds_adjacent`.
///
/// The seam carries no `mcx` because its only allocation is the transient probe
/// range C builds in `CurrentMemoryContext` to test emptiness for a discrete
/// subtype; that probe never escapes (only its `RANGE_EMPTY` flag is read). We
/// mirror that by running the real `bounds_adjacent` against a private scratch
/// context that is dropped on return.
pub fn bounds_adjacent_seam(
    typcache: &TypeCacheEntry,
    bound_a: RangeBound,
    bound_b: RangeBound,
) -> PgResult<bool> {
    let scratch = mcx::MemoryContext::new_bump("bounds_adjacent probe");
    bounds_adjacent(scratch.mcx(), typcache, bound_a, bound_b)
}

/// Inward seam shape for `range_adjacent_internal`.
///
/// As with `bounds_adjacent`, the seam omits `mcx` because the only allocation
/// is the transient probe range used to decide adjacency for a discrete
/// subtype, which never escapes the call. Run the real kernel against a private
/// scratch context dropped on return.
pub fn range_adjacent_internal_seam(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<bool> {
    let scratch = mcx::MemoryContext::new_bump("range_adjacent probe");
    range_adjacent_internal(scratch.mcx(), typcache, r1, r2)
}
