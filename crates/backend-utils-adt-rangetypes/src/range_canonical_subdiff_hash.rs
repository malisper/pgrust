//! Family `range-canonical-subdiff-hash`: canonicalization, subtype distance,
//! hashing, and the range total order / sort support.
//!
//! Mirrors `rangetypes.c`: `int4range_canonical` / `int8range_canonical` /
//! `daterange_canonical`, the `*_subdiff` builtins, `hash_range` /
//! `hash_range_extended`, `range_cmp`, and `range_sortsupport` /
//! `range_fast_cmp` / `range_lt`..`range_gt`. This family owns and installs the
//! inward `range_subdiff` seam. The element hash/cmp/sortsupport calls and the
//! date/timestamp arithmetic route through their owners' seams (hashfn /
//! sortsupport / date-ts).

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::OidIsValid;
use types_datum::datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
                  ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
use types_rangetypes::{
    RangeTypeP, RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_NULL, RANGE_UB_INF, RANGE_UB_NULL,
};

use backend_utils_adt_numeric_seams::numeric_subdiff;
use backend_utils_cache_typcache_seams::lookup_range_elem_hash_proc;
use backend_utils_fmgr_fmgr_seams::{
    function_call1_coll_datum, function_call2_coll, function_call2_coll_datum,
};
use common_hashfn_seams::{hash_bytes_uint32, hash_bytes_uint32_extended};

use crate::range_bounds_compare::range_cmp_bounds;
use crate::range_repr_serialize::{range_deserialize, range_get_flags, range_serialize};

// --- helpers mirroring the C header macros ---------------------------------

/// `RANGE_HAS_LBOUND(flags)` (rangetypes.h:48).
#[inline]
fn range_has_lbound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0
}

/// `RANGE_HAS_UBOUND(flags)` (rangetypes.h:52).
#[inline]
fn range_has_ubound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0
}

/// `pg_rotate_left32(word, n)` (pg_bitutils.h:428).
#[inline]
fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    (word << n) | (word >> (32 - n))
}

/// `hash_uint32(k)` (hashfn.h:43): `UInt32GetDatum(hash_bytes_uint32(k))`.
#[inline]
fn hash_uint32(k: u32) -> u32 {
    hash_bytes_uint32::call(k)
}

/// `hash_uint32_extended(k, seed)` (hashfn.h:49):
/// `UInt64GetDatum(hash_bytes_uint32_extended(k, seed))`.
#[inline]
fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    hash_bytes_uint32_extended::call(k, seed)
}

/// `ROTATE_HIGH_AND_LOW_32BITS(v)` (hashfn.h:18).
#[inline]
fn rotate_high_and_low_32bits(v: u64) -> u64 {
    ((v << 1) & 0xffff_fffe_ffff_fffe) | ((v >> 31) & 0x0000_0001_0000_0001)
}

// --- date / timestamp constants mirroring the C header macros --------------

/// `DATEVAL_NOBEGIN` (date.h:36): `PG_INT32_MIN`.
const DATEVAL_NOBEGIN: i32 = i32::MIN;
/// `DATEVAL_NOEND` (date.h:37): `PG_INT32_MAX`.
const DATEVAL_NOEND: i32 = i32::MAX;
/// `USECS_PER_SEC` (timestamp.h:134).
const USECS_PER_SEC: i64 = 1_000_000;

/// `DATE_NOT_FINITE(j)` (date.h:43).
#[inline]
fn date_not_finite(j: i32) -> bool {
    j == DATEVAL_NOBEGIN || j == DATEVAL_NOEND
}

/// `IS_VALID_DATE(d)` (timestamp.h:262): `DATETIME_MIN_JULIAN -
/// POSTGRES_EPOCH_JDATE (= -2451545) <= d && d < DATE_END_JULIAN -
/// POSTGRES_EPOCH_JDATE (= 2145031949)`.
#[inline]
fn is_valid_date(d: i32) -> bool {
    const LOW: i64 = 0 - 2451545; // DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE
    const HIGH: i64 = 2147483494 - 2451545; // DATE_END_JULIAN - POSTGRES_EPOCH_JDATE
    let d = d as i64;
    LOW <= d && d < HIGH
}

// --- canonical functions (rangetypes.c) ------------------------------------

/// `int4range_canonical(r)` body (rangetypes.c:1528): normalize to `[)`.
pub fn int4range_canonical<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (mut lower, mut upper, empty) = range_deserialize(typcache, r)?;

    if empty {
        // PG_RETURN_RANGE_P(r) -- the input is already canonical; copy it into
        // the current context (`mcx`) to match the seam's lifetime contract.
        return datum_get_range_type_p_copy(mcx, r);
    }

    if !lower.infinite && !lower.inclusive {
        let bnd = lower.val.as_i32();

        // Handle possible overflow manually
        if bnd == i32::MAX {
            return Err(PgError::error("integer out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
        lower.val = Datum::from_i32(bnd + 1);
        lower.inclusive = true;
    }

    if !upper.infinite && upper.inclusive {
        let bnd = upper.val.as_i32();

        if bnd == i32::MAX {
            return Err(PgError::error("integer out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
        upper.val = Datum::from_i32(bnd + 1);
        upper.inclusive = false;
    }

    range_serialize(mcx, typcache, &lower, &upper, false)
}

/// `int8range_canonical(r)` body (rangetypes.c:1575).
pub fn int8range_canonical<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (mut lower, mut upper, empty) = range_deserialize(typcache, r)?;

    if empty {
        return datum_get_range_type_p_copy(mcx, r);
    }

    if !lower.infinite && !lower.inclusive {
        let bnd = lower.val.as_i64();

        if bnd == i64::MAX {
            return Err(PgError::error("bigint out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
        lower.val = Datum::from_i64(bnd + 1);
        lower.inclusive = true;
    }

    if !upper.infinite && upper.inclusive {
        let bnd = upper.val.as_i64();

        if bnd == i64::MAX {
            return Err(PgError::error("bigint out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
        upper.val = Datum::from_i64(bnd + 1);
        upper.inclusive = false;
    }

    range_serialize(mcx, typcache, &lower, &upper, false)
}

/// `daterange_canonical(r)` body (rangetypes.c:1622).
pub fn daterange_canonical<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (mut lower, mut upper, empty) = range_deserialize(typcache, r)?;

    if empty {
        return datum_get_range_type_p_copy(mcx, r);
    }

    // DateADT is int32.
    if !lower.infinite && !date_not_finite(lower.val.as_i32()) && !lower.inclusive {
        let mut bnd = lower.val.as_i32();

        // Check for overflow -- note we already eliminated PG_INT32_MAX
        bnd += 1;
        if !is_valid_date(bnd) {
            return Err(PgError::error("date out of range")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
        }
        lower.val = Datum::from_i32(bnd);
        lower.inclusive = true;
    }

    if !upper.infinite && !date_not_finite(upper.val.as_i32()) && upper.inclusive {
        let mut bnd = upper.val.as_i32();

        bnd += 1;
        if !is_valid_date(bnd) {
            return Err(PgError::error("date out of range")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
        }
        upper.val = Datum::from_i32(bnd);
        upper.inclusive = false;
    }

    range_serialize(mcx, typcache, &lower, &upper, false)
}

/// `PG_RETURN_RANGE_P(r)` for the empty fast-path: hand the already-canonical
/// input back. The serialized payload is the range ADT's private encoding owned
/// by the `range_repr_serialize` family, so detoasting/copying into `mcx` goes
/// through that family's `DatumGetRangeTypeP` seam shape. The input `r` is
/// already a detoasted `RangeTypeP`, so re-wrapping its `Datum` produces the
/// equivalent copy in the current context.
fn datum_get_range_type_p_copy<'mcx>(
    mcx: Mcx<'mcx>,
    r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    backend_utils_adt_rangetypes_seams::datum_get_range_type_p::call(
        mcx,
        Datum::from_usize(r.ptr as usize),
    )
}

// --- subtype_diff functions (rangetypes.c) ---------------------------------

/// `range_subdiff(typcache, v1, v2)` — `DatumGetFloat8(FunctionCall2Coll(
/// &typcache->rng_subdiff_finfo, rng_collation, v1, v2))`. Owns the inward seam
/// (consumed by range selectivity).
pub fn range_subdiff(typcache: &TypeCacheEntry, v1: Datum, v2: Datum) -> PgResult<f64> {
    let res = function_call2_coll::call(
        typcache.rng_subdiff_finfo.fn_oid,
        typcache.rng_collation,
        v1,
        v2,
    )?;
    Ok(res.as_f64())
}

/// `numrange_subdiff(v1, v2)` (rangetypes.c:1703).
pub fn numrange_subdiff(v1: Datum, v2: Datum) -> PgResult<f64> {
    // numresult = DirectFunctionCall2(numeric_sub, v1, v2);
    // floatresult = DatumGetFloat8(DirectFunctionCall1(numeric_float8, numresult));
    //
    // `numeric_subdiff` now takes the canonical unified `types_tuple::Datum<'mcx>`
    // by value. `v1`/`v2` are pointer-bearing `numeric` Datums (detoasted at the
    // caller's fmgr boundary), so the machine word forwards unchanged onto the
    // canonical `ByVal` arm, exactly as C's `DirectFunctionCall2` treats the
    // `Datum` as a `Numeric` pointer.
    numeric_subdiff::call(
        types_tuple::Datum::from_usize(v1.as_usize()),
        types_tuple::Datum::from_usize(v2.as_usize()),
    )
}

/// `daterange_subdiff(v1, v2)` (rangetypes.c:1719).
pub fn daterange_subdiff(v1: i32, v2: i32) -> f64 {
    v1 as f64 - v2 as f64
}

/// `tsrange_subdiff(v1, v2)` (rangetypes.c:1728).
pub fn tsrange_subdiff(v1: i64, v2: i64) -> f64 {
    (v1 as f64 - v2 as f64) / USECS_PER_SEC as f64
}

/// `tstzrange_subdiff(v1, v2)` (rangetypes.c:1739).
pub fn tstzrange_subdiff(v1: i64, v2: i64) -> f64 {
    (v1 as f64 - v2 as f64) / USECS_PER_SEC as f64
}

/// `int4range_subdiff(v1, v2)` (rangetypes.c:1685).
pub fn int4range_subdiff(v1: i32, v2: i32) -> f64 {
    v1 as f64 - v2 as f64
}

/// `int8range_subdiff(v1, v2)` (rangetypes.c:1694).
pub fn int8range_subdiff(v1: i64, v2: i64) -> f64 {
    v1 as f64 - v2 as f64
}

// --- hash support (rangetypes.c) -------------------------------------------

/// `hash_range(r)` body (rangetypes.c:1394).
pub fn hash_range(typcache: &TypeCacheEntry, r: RangeTypeP<'_>) -> PgResult<u32> {
    // deserialize
    let (lower, upper, _empty) = range_deserialize(typcache, r)?;
    let flags = range_get_flags(r);

    // Look up the element type's hash function, if not done already.
    let scache = typcache
        .rngelemtype
        .as_deref()
        .expect("range type cache entry has rngelemtype");
    let hash_proc_oid = if OidIsValid(scache.hash_proc_finfo.fn_oid) {
        scache.hash_proc_finfo.fn_oid
    } else {
        // scache = lookup_type_cache(scache->type_id, TYPECACHE_HASH_PROC_FINFO);
        // and ereport if still no hash function. The owner returns the resolved
        // proc OID (or raises the "could not identify a hash function" error).
        lookup_range_elem_hash_proc::call(scache.type_id, false)?
    };

    // Apply the hash function to each bound. A by-reference element subtype
    // (numeric/text/...) carries its bound on the `ref_args` lane, so cross the
    // canonical `Datum` (`*_coll_datum`); the bare-word seam left the referent
    // empty ("by-ref arg missing from by-ref lane"). `function_call1_coll` over a
    // by-value bare word stays equivalent for by-value subtypes.
    let lower_hash = if range_has_lbound(flags) {
        let scratch = mcx::MemoryContext::new_bump("hash_range lower");
        let m = scratch.mcx();
        let v = crate::range_bounds_compare::elem_word_to_canon(m, typcache, lower.val)?;
        let r = function_call1_coll_datum::call(m, hash_proc_oid, typcache.rng_collation, v)?;
        r.as_u32()
    } else {
        0
    };

    let upper_hash = if range_has_ubound(flags) {
        let scratch = mcx::MemoryContext::new_bump("hash_range upper");
        let m = scratch.mcx();
        let v = crate::range_bounds_compare::elem_word_to_canon(m, typcache, upper.val)?;
        let r = function_call1_coll_datum::call(m, hash_proc_oid, typcache.rng_collation, v)?;
        r.as_u32()
    } else {
        0
    };

    // Merge hashes of flags and bounds. C's `flags` is a signed `char`, so
    // `(uint32) flags` sign-extends; mirror that via `i8 as u32`.
    let mut result = hash_uint32(flags as i8 as u32);
    result ^= lower_hash;
    result = pg_rotate_left32(result, 1);
    result ^= upper_hash;

    Ok(result)
}

/// `hash_range_extended(r, seed)` body (rangetypes.c:1460).
pub fn hash_range_extended(
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    seed: u64,
) -> PgResult<u64> {
    let (lower, upper, _empty) = range_deserialize(typcache, r)?;
    let flags = range_get_flags(r);

    let scache = typcache
        .rngelemtype
        .as_deref()
        .expect("range type cache entry has rngelemtype");
    let hash_proc_oid = if OidIsValid(scache.hash_extended_proc_finfo.fn_oid) {
        scache.hash_extended_proc_finfo.fn_oid
    } else {
        // scache = lookup_type_cache(scache->type_id,
        //                            TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        lookup_range_elem_hash_proc::call(scache.type_id, true)?
    };

    // The seed crosses as a by-value `Datum` to the element's extended hash
    // function (C: `FunctionCall2Coll(.., lower.val, seed)`). The bound element
    // (arg 0) must ride the canonical `Datum` lane so a by-reference subtype
    // reaches the function on the `ref_args` side channel; the seed (arg 1) stays
    // a by-value word.
    let seed_canon = types_tuple::backend_access_common_heaptuple::Datum::from_usize(seed as usize);

    let lower_hash = if range_has_lbound(flags) {
        let scratch = mcx::MemoryContext::new_bump("hash_range_extended lower");
        let m = scratch.mcx();
        let v = crate::range_bounds_compare::elem_word_to_canon(m, typcache, lower.val)?;
        let r = function_call2_coll_datum::call(
            m,
            hash_proc_oid,
            typcache.rng_collation,
            v,
            seed_canon.clone(),
        )?;
        r.as_u64()
    } else {
        0
    };

    let upper_hash = if range_has_ubound(flags) {
        let scratch = mcx::MemoryContext::new_bump("hash_range_extended upper");
        let m = scratch.mcx();
        let v = crate::range_bounds_compare::elem_word_to_canon(m, typcache, upper.val)?;
        let r = function_call2_coll_datum::call(
            m,
            hash_proc_oid,
            typcache.rng_collation,
            v,
            seed_canon.clone(),
        )?;
        r.as_u64()
    } else {
        0
    };

    // Merge hashes of flags and bounds. C's `flags` is a signed `char`, so
    // `(uint32) flags` sign-extends; mirror that via `i8 as u32`.
    let mut result = hash_uint32_extended(flags as i8 as u32, seed);
    result ^= lower_hash;
    result = rotate_high_and_low_32bits(result);
    result ^= upper_hash;

    Ok(result)
}

// --- btree total order + sortsupport (rangetypes.c) ------------------------

/// `range_cmp(r1, r2)` body (rangetypes.c:1251): the btree total order.
///
/// The fmgr boundary (the `PG_FUNCTION_ARGS` entry point) performs the
/// `RangeTypeGetOid(r1) != RangeTypeGetOid(r2)` "range types do not match"
/// check, the `range_get_typcache` lookup, and `check_stack_depth`; this kernel
/// receives the resolved `typcache` and the two deserialized-able ranges.
pub fn range_cmp(
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'_>,
    r2: RangeTypeP<'_>,
) -> PgResult<i32> {
    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;

    // For b-tree use, empty ranges sort before all else
    let cmp = if empty1 && empty2 {
        0
    } else if empty1 {
        -1
    } else if empty2 {
        1
    } else {
        let mut cmp = range_cmp_bounds(typcache, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_cmp_bounds(typcache, &upper1, &upper2)?;
        }
        cmp
    };

    Ok(cmp)
}

/// `range_fast_cmp(a, b, ssup)` (rangetypes.c:1309): the sortsupport comparator.
///
/// C caches the `TypeCacheEntry` in `ssup->ssup_extra` across calls; the kernel
/// receives the cached `typcache` from its owner. The body is otherwise
/// identical to [`range_cmp`].
pub fn range_fast_cmp(
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
        let mut cmp = range_cmp_bounds(typcache, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_cmp_bounds(typcache, &upper1, &upper2)?;
        }
        cmp
    };

    Ok(cmp)
}
