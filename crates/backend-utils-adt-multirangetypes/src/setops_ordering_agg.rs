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
use types_datum::datum::Datum;
use types_error::error::ERRCODE_UNDEFINED_FUNCTION;
use types_error::{PgError, PgResult};
use types_rangetypes::{
    MultirangeTypeP, RangeTypeP, RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_NULL, RANGE_UB_INF,
    RANGE_UB_NULL,
};

use crate::serialize_core;

use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_adt_rangetypes_seams as range_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;
use common_hashfn_seams as hashfn_seams;

// ---------------------------------------------------------------------------
// Small varlena / multirange header helpers (mirroring the C macros used
// directly by these functions).
// ---------------------------------------------------------------------------

/// `mr->rangeCount` — the number of member ranges in a serialized multirange.
#[inline]
fn multirange_range_count(mr: MultirangeTypeP<'_>) -> u32 {
    // SAFETY: `mr.ptr` is a detoasted `MultirangeType *` whose header (incl.
    // `rangeCount`) is always readable, as in C.
    mr.range_count()
}

/// `mr->multirangetypid` — the multirange type's own OID.
#[inline]
fn multirange_type_oid(mr: MultirangeTypeP<'_>) -> Oid {
    // SAFETY: see [`multirange_range_count`].
    mr.multirangetypid()
}

/// `MultirangeIsEmpty(mr)` (multirangetypes.h): a multirange with no ranges.
#[inline]
fn multirange_is_empty(mr: MultirangeTypeP<'_>) -> bool {
    multirange_range_count(mr) == 0
}

/// `RangeIsEmpty(r)` (rangetypes.h): `range_get_flags(r) & RANGE_EMPTY`.
#[inline]
fn range_is_empty(r: RangeTypeP<'_>) -> bool {
    range_seams::range_get_flags::call(r) & RANGE_EMPTY != 0
}

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
    if multirange_is_empty(mr1) {
        return Ok(mr2);
    }
    if multirange_is_empty(mr2) {
        return Ok(mr1);
    }

    let mltrngtypoid = multirange_type_oid(mr1);

    let ranges1 = serialize_core::multirange_deserialize(mcx, rangetyp, mr1)?;
    let ranges2 = serialize_core::multirange_deserialize(mcx, rangetyp, mr2)?;

    let mut ranges3: Vec<RangeTypeP<'mcx>> = Vec::with_capacity(ranges1.len() + ranges2.len());
    ranges3.extend_from_slice(&ranges1);
    ranges3.extend_from_slice(&ranges2);

    serialize_core::make_multirange(mcx, mltrngtypoid, rangetyp, &ranges3)
}

/// `multirange_minus(PG_FUNCTION_ARGS)` (multirangetypes.c:1115).
pub fn multirange_minus<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'mcx>,
    mr2: MultirangeTypeP<'mcx>,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let mltrngtypoid = multirange_type_oid(mr1);

    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(mr1);
    }

    let ranges1 = serialize_core::multirange_deserialize(mcx, rangetyp, mr1)?;
    let ranges2 = serialize_core::multirange_deserialize(mcx, rangetyp, mr2)?;

    multirange_minus_internal(mcx, mltrngtypoid, rangetyp, &ranges1, &ranges2)
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
    let range_count1 = ranges1.len();
    let range_count2 = ranges2.len();

    // Worst case: every range in ranges1 makes a different cut to some range
    // in ranges2.
    let mut ranges3: Vec<RangeTypeP<'mcx>> = Vec::with_capacity(range_count1 + range_count2);

    // For each range in mr1, keep subtracting until it's gone or the ranges in
    // mr2 have passed it. After a subtraction we assign what's left back to r1.
    let mut i2: usize = 0;
    let mut r2: Option<RangeTypeP<'mcx>> = if range_count2 == 0 {
        None
    } else {
        Some(ranges2[0])
    };

    for &r1_in in ranges1.iter() {
        let mut r1 = r1_in;

        // Discard r2s while r2 << r1
        while let Some(r2v) = r2 {
            if range_seams::range_before_internal::call(rangetyp, r2v, r1)? {
                i2 += 1;
                r2 = if i2 >= range_count2 {
                    None
                } else {
                    Some(ranges2[i2])
                };
            } else {
                break;
            }
        }

        while let Some(r2v) = r2 {
            if let Some((left, rest)) = range_seams::range_split_internal::call(mcx, rangetyp, r1, r2v)? {
                // If r2 takes a bite out of the middle of r1, we need two
                // outputs.
                ranges3.push(left);
                r1 = rest;
                i2 += 1;
                r2 = if i2 >= range_count2 {
                    None
                } else {
                    Some(ranges2[i2])
                };
            } else if range_seams::range_overlaps_internal::call(rangetyp, r1, r2v)? {
                // If r2 overlaps r1, replace r1 with r1 - r2.
                r1 = range_seams::range_minus_internal::call(mcx, rangetyp, r1, r2v)?;

                // If r2 goes past r1, then we need to stay with it, in case it
                // hits future r1s. Otherwise we need to keep r1, in case future
                // r2s hit it.
                if range_is_empty(r1) || range_seams::range_before_internal::call(rangetyp, r1, r2v)? {
                    break;
                } else {
                    i2 += 1;
                    r2 = if i2 >= range_count2 {
                        None
                    } else {
                        Some(ranges2[i2])
                    };
                }
            } else {
                // This and all future r2s are past r1, so keep them. Also
                // assign whatever is left of r1 to the result.
                break;
            }
        }

        // Nothing else can remove anything from r1, so keep it. Even if r1 is
        // empty here, make_multirange will remove it.
        ranges3.push(r1);
    }

    serialize_core::make_multirange(mcx, mltrngtypoid, rangetyp, &ranges3)
}

/// `multirange_intersect(PG_FUNCTION_ARGS)` (multirangetypes.c:1231).
pub fn multirange_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'mcx>,
    mr2: MultirangeTypeP<'mcx>,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let mltrngtypoid = multirange_type_oid(mr1);

    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return serialize_core::make_empty_multirange(mcx, mltrngtypoid, rangetyp);
    }

    let ranges1 = serialize_core::multirange_deserialize(mcx, rangetyp, mr1)?;
    let ranges2 = serialize_core::multirange_deserialize(mcx, rangetyp, mr2)?;

    multirange_intersect_internal(mcx, mltrngtypoid, rangetyp, &ranges1, &ranges2)
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
    let range_count1 = ranges1.len();
    let range_count2 = ranges2.len();

    if range_count1 == 0 || range_count2 == 0 {
        return serialize_core::make_multirange(mcx, mltrngtypoid, rangetyp, &[]);
    }

    // Worst case is a stitching pattern; range_count1 + range_count2 - 1, but
    // one extra won't hurt.
    let mut ranges3: Vec<RangeTypeP<'mcx>> = Vec::with_capacity(range_count1 + range_count2);

    // For each range in mr1, keep intersecting until the ranges in mr2 have
    // passed it.
    let mut i2: usize = 0;
    let mut r2: Option<RangeTypeP<'mcx>> = Some(ranges2[0]);

    for &r1 in ranges1.iter() {
        // Discard r2s while r2 << r1
        while let Some(r2v) = r2 {
            if range_seams::range_before_internal::call(rangetyp, r2v, r1)? {
                i2 += 1;
                r2 = if i2 >= range_count2 {
                    None
                } else {
                    Some(ranges2[i2])
                };
            } else {
                break;
            }
        }

        while let Some(r2v) = r2 {
            if range_seams::range_overlaps_internal::call(rangetyp, r1, r2v)? {
                // Keep the overlapping part.
                ranges3.push(range_seams::range_intersect_internal::call(mcx, rangetyp, r1, r2v)?);

                // If we "used up" all of r2, go to the next one...
                if range_seams::range_overleft_internal::call(rangetyp, r2v, r1)? {
                    i2 += 1;
                    r2 = if i2 >= range_count2 {
                        None
                    } else {
                        Some(ranges2[i2])
                    };
                } else {
                    // ...otherwise go to the next r1.
                    break;
                }
            } else {
                // We're past r1, so move to the next one.
                break;
            }
        }

        // If we're out of r2s, there can be no more intersections.
        if r2.is_none() {
            break;
        }
    }

    serialize_core::make_multirange(mcx, mltrngtypoid, rangetyp, &ranges3)
}

/// `range_merge_from_multirange(PG_FUNCTION_ARGS)` (multirangetypes.c:2676): the
/// range spanning the whole multirange (errors on an empty multirange).
pub fn range_merge_from_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    let range_count = multirange_range_count(mr);

    if multirange_is_empty(mr) {
        range_seams::make_empty_range::call(mcx, rangetyp)
    } else if range_count == 1 {
        serialize_core::multirange_get_range(mcx, rangetyp, mr, 0)
    } else {
        let (first_lower, _first_upper) = serialize_core::multirange_get_bounds(rangetyp, mr, 0)?;
        let (_last_lower, last_upper) =
            serialize_core::multirange_get_bounds(rangetyp, mr, range_count - 1)?;

        range_seams::make_range::call(mcx, rangetyp, &first_lower, &last_upper, false)
    }
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
    // Different types should be prevented by ANYMULTIRANGE matching rules.
    if multirange_type_oid(mr1) != multirange_type_oid(mr2) {
        return Err(PgError::error("multirange types do not match"));
    }

    let range_count_1 = multirange_range_count(mr1);
    let range_count_2 = multirange_range_count(mr2);

    let mut cmp: i32 = 0; // If both are empty we'll use this.

    // Loop over source data.
    let range_count_max = range_count_1.max(range_count_2);
    for i in 0..range_count_max {
        // If one multirange is shorter, it's as if it had empty ranges at the
        // end; an empty range compares earlier than any other range, so the
        // shorter multirange comes before the longer.
        if i >= range_count_1 {
            cmp = -1;
            break;
        }
        if i >= range_count_2 {
            cmp = 1;
            break;
        }

        let (lower1, upper1) = serialize_core::multirange_get_bounds(rangetyp, mr1, i)?;
        let (lower2, upper2) = serialize_core::multirange_get_bounds(rangetyp, mr2, i)?;

        cmp = range_seams::range_cmp_bounds::call(rangetyp, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_seams::range_cmp_bounds::call(rangetyp, &upper1, &upper2)?;
        }
        if cmp != 0 {
            break;
        }
    }

    Ok(cmp)
}

/// `multirange_lt(PG_FUNCTION_ARGS)` (multirangetypes.c:2641).
pub fn multirange_lt(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(multirange_cmp(rangetyp, mr1, mr2)? < 0)
}

/// `multirange_le(PG_FUNCTION_ARGS)` (multirangetypes.c:2649).
pub fn multirange_le(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(multirange_cmp(rangetyp, mr1, mr2)? <= 0)
}

/// `multirange_ge(PG_FUNCTION_ARGS)` (multirangetypes.c:2657).
pub fn multirange_ge(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(multirange_cmp(rangetyp, mr1, mr2)? >= 0)
}

/// `multirange_gt(PG_FUNCTION_ARGS)` (multirangetypes.c:2665).
pub fn multirange_gt(
    rangetyp: &TypeCacheEntry,
    mr1: MultirangeTypeP<'_>,
    mr2: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    Ok(multirange_cmp(rangetyp, mr1, mr2)? > 0)
}

// ---------------------------------------------------------------------------
// hashing.
// ---------------------------------------------------------------------------

/// `pg_rotate_left32(word, n)` (pg_bitutils.h): rotate a 32-bit word left.
#[inline]
fn pg_rotate_left32(word: u32, n: i32) -> u32 {
    (word << n) | (word >> (32 - n))
}

/// `ROTATE_HIGH_AND_LOW_32BITS(v)` (hashfn.h): rotate the high and low 32-bit
/// halves of a 64-bit value separately by one bit.
#[inline]
fn rotate_high_and_low_32bits(v: u64) -> u64 {
    ((v << 1) & 0xfffffffe_fffffffe) | ((v >> 31) & 0x00000001_00000001)
}

/// `hash_multirange(PG_FUNCTION_ARGS)` (multirangetypes.c:2788).
pub fn hash_multirange(rangetyp: &TypeCacheEntry, mr: MultirangeTypeP<'_>) -> PgResult<u32> {
    let mut result: u32 = 1;

    // scache = typcache->rngtype->rngelemtype, re-looked-up for its hash proc
    // if not already cached.
    let rngelemtype = rangetyp
        .rngelemtype
        .as_deref()
        .expect("range type cache entry missing rngelemtype");

    let hash_fn_oid;
    if rngelemtype.hash_proc_finfo.fn_oid != 0 {
        hash_fn_oid = rngelemtype.hash_proc_finfo.fn_oid;
    } else {
        // C: scache = lookup_type_cache(scache->type_id,
        // TYPECACHE_HASH_PROC_FINFO); then read scache->hash_proc_finfo.fn_oid.
        let scache_fn_oid =
            typcache_seams::lookup_range_elem_hash_proc::call(rngelemtype.type_id, false)?;
        if scache_fn_oid == 0 {
            return Err(could_not_identify_hash_fn(rngelemtype.type_id));
        }
        hash_fn_oid = scache_fn_oid;
    }

    let range_count = multirange_range_count(mr);
    for i in 0..range_count {
        let flags = serialize_core::multirange_get_flags(mr, i);
        let (lower, upper) = serialize_core::multirange_get_bounds(rangetyp, mr, i)?;

        let lower_hash = if flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0 {
            fmgr_seams::function_call1_coll::call(
                hash_fn_oid,
                rangetyp.rng_collation,
                lower.val,
            )?
            .as_u32()
        } else {
            0
        };

        let upper_hash = if flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0 {
            fmgr_seams::function_call1_coll::call(
                hash_fn_oid,
                rangetyp.rng_collation,
                upper.val,
            )?
            .as_u32()
        } else {
            0
        };

        // Merge hashes of flags and bounds.
        let mut range_hash = hashfn_seams::hash_bytes_uint32::call(flags as u32);
        range_hash ^= lower_hash;
        range_hash = pg_rotate_left32(range_hash, 1);
        range_hash ^= upper_hash;

        // Same approach as hash_array to combine the individual elements.
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);
    }

    Ok(result)
}

/// `hash_multirange_extended(PG_FUNCTION_ARGS)` (multirangetypes.c:2859).
pub fn hash_multirange_extended(
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'_>,
    seed: u64,
) -> PgResult<u64> {
    let mut result: u64 = 1;

    let rngelemtype = rangetyp
        .rngelemtype
        .as_deref()
        .expect("range type cache entry missing rngelemtype");

    let hash_fn_oid;
    if rngelemtype.hash_extended_proc_finfo.fn_oid != 0 {
        hash_fn_oid = rngelemtype.hash_extended_proc_finfo.fn_oid;
    } else {
        // C: scache = lookup_type_cache(scache->type_id,
        // TYPECACHE_HASH_EXTENDED_PROC_FINFO); then read
        // scache->hash_extended_proc_finfo.fn_oid.
        let scache_fn_oid =
            typcache_seams::lookup_range_elem_hash_proc::call(rngelemtype.type_id, true)?;
        if scache_fn_oid == 0 {
            return Err(could_not_identify_hash_fn(rngelemtype.type_id));
        }
        hash_fn_oid = scache_fn_oid;
    }

    let seed_datum = Datum::from_u64(seed);

    let range_count = multirange_range_count(mr);
    for i in 0..range_count {
        let flags = serialize_core::multirange_get_flags(mr, i);
        let (lower, upper) = serialize_core::multirange_get_bounds(rangetyp, mr, i)?;

        let lower_hash = if flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0 {
            fmgr_seams::function_call2_coll::call(
                hash_fn_oid,
                rangetyp.rng_collation,
                lower.val,
                seed_datum,
            )?
            .as_u64()
        } else {
            0
        };

        let upper_hash = if flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0 {
            fmgr_seams::function_call2_coll::call(
                hash_fn_oid,
                rangetyp.rng_collation,
                upper.val,
                seed_datum,
            )?
            .as_u64()
        } else {
            0
        };

        // Merge hashes of flags and bounds.
        let mut range_hash = hashfn_seams::hash_bytes_uint32_extended::call(flags as u32, seed);
        range_hash ^= lower_hash;
        range_hash = rotate_high_and_low_32bits(range_hash);
        range_hash ^= upper_hash;

        // Same approach as hash_array to combine the individual elements.
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);
    }

    Ok(result)
}

/// `ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION, "could not identify a hash
/// function for type %s")` (multirangetypes.c).
fn could_not_identify_hash_fn(type_id: Oid) -> PgError {
    // `format_type_be` needs a context for the palloc'd name; a transient
    // context suffices and is dropped with this builder.
    let cx = mcx::MemoryContext::new("hash_multirange error");
    let name = match format_type_seams::format_type_be::call(cx.mcx(), type_id) {
        Ok(s) => s.as_str().to_string(),
        Err(_) => type_id.to_string(),
    };
    PgError::error(format!("could not identify a hash function for type {name}"))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
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
    let _ = mcx;
    // skip NULLs
    if let Some(v) = value {
        state.push(v);
    }
    Ok(())
}

/// `range_agg_finalfn(PG_FUNCTION_ARGS)` (multirangetypes.c:1373): assemble the
/// accumulated ranges into a multirange.
pub fn range_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    state: &[RangeTypeP<'mcx>],
) -> PgResult<Option<MultirangeTypeP<'mcx>>> {
    // Return NULL if we had zero inputs, like other aggregates.
    if state.is_empty() {
        return Ok(None);
    }

    Ok(Some(serialize_core::make_multirange(
        mcx,
        mltrngtypoid,
        rangetyp,
        state,
    )?))
}

/// `multirange_agg_transfn(PG_FUNCTION_ARGS)` (multirangetypes.c:1413):
/// accumulate every member range of a multirange into the state.
pub fn multirange_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    state: &mut Vec<RangeTypeP<'mcx>>,
    value: Option<MultirangeTypeP<'mcx>>,
) -> PgResult<()> {
    // skip NULLs
    if let Some(current) = value {
        let ranges = serialize_core::multirange_deserialize(mcx, rangetyp, current)?;
        if ranges.is_empty() {
            // Add an empty range so we get an empty result (not a null result).
            state.push(range_seams::make_empty_range::call(mcx, rangetyp)?);
        } else {
            for r in ranges {
                state.push(r);
            }
        }
    }
    Ok(())
}

/// `multirange_intersect_agg_transfn(PG_FUNCTION_ARGS)`
/// (multirangetypes.c:1466): fold a multirange into the running intersection.
pub fn multirange_intersect_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    state: Option<MultirangeTypeP<'mcx>>,
    value: Option<MultirangeTypeP<'mcx>>,
) -> PgResult<Option<MultirangeTypeP<'mcx>>> {
    // strictness ensures these are non-null.
    let (result, current) = match (state, value) {
        (Some(s), Some(v)) => (s, v),
        _ => return Ok(state),
    };

    let mltrngtypoid = multirange_type_oid(result);

    let ranges1 = serialize_core::multirange_deserialize(mcx, rangetyp, result)?;
    let ranges2 = serialize_core::multirange_deserialize(mcx, rangetyp, current)?;

    Ok(Some(multirange_intersect_internal(
        mcx,
        mltrngtypoid,
        rangetyp,
        &ranges1,
        &ranges2,
    )?))
}
