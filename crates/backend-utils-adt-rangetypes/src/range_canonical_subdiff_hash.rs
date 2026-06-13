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
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::RangeTypeP;

/// `int4range_canonical(r)` body (rangetypes.c:1572): normalize to `[)`.
pub fn int4range_canonical<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("int4range_canonical")
}

/// `int8range_canonical(r)` body (rangetypes.c).
pub fn int8range_canonical<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("int8range_canonical")
}

/// `daterange_canonical(r)` body (rangetypes.c:1622).
pub fn daterange_canonical<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _r: RangeTypeP<'_>,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("daterange_canonical")
}

/// `range_subdiff(typcache, v1, v2)` — `FunctionCall2Coll(rng_subdiff_finfo,..)`
/// as `DatumGetFloat8`. Owns the inward seam (consumed by range selectivity).
pub fn range_subdiff(_typcache: &TypeCacheEntry, _v1: Datum, _v2: Datum) -> PgResult<f64> {
    todo!("range_subdiff: call the subtype's subdiff support fn")
}

/// `numrange_subdiff(v1, v2)` (rangetypes.c:1703).
pub fn numrange_subdiff(_v1: Datum, _v2: Datum) -> PgResult<f64> {
    todo!("numrange_subdiff")
}

/// `daterange_subdiff(v1, v2)` (rangetypes.c:1719).
pub fn daterange_subdiff(_v1: i32, _v2: i32) -> f64 {
    todo!("daterange_subdiff")
}

/// `tsrange_subdiff(v1, v2)` (rangetypes.c:1728).
pub fn tsrange_subdiff(_v1: i64, _v2: i64) -> f64 {
    todo!("tsrange_subdiff")
}

/// `tstzrange_subdiff(v1, v2)` (rangetypes.c:1739).
pub fn tstzrange_subdiff(_v1: i64, _v2: i64) -> f64 {
    todo!("tstzrange_subdiff")
}

/// `int4range_subdiff(v1, v2)` (rangetypes.c).
pub fn int4range_subdiff(_v1: i32, _v2: i32) -> f64 {
    todo!("int4range_subdiff")
}

/// `int8range_subdiff(v1, v2)` (rangetypes.c).
pub fn int8range_subdiff(_v1: i64, _v2: i64) -> f64 {
    todo!("int8range_subdiff")
}

/// `hash_range(r)` body (rangetypes.c:1394).
pub fn hash_range(_typcache: &TypeCacheEntry, _r: RangeTypeP<'_>) -> PgResult<u32> {
    todo!("hash_range")
}

/// `hash_range_extended(r, seed)` body (rangetypes.c:1460).
pub fn hash_range_extended(
    _typcache: &TypeCacheEntry,
    _r: RangeTypeP<'_>,
    _seed: u64,
) -> PgResult<u64> {
    todo!("hash_range_extended")
}

/// `range_cmp(r1, r2)` body (rangetypes.c:1251): the btree total order.
pub fn range_cmp(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<i32> {
    todo!("range_cmp")
}

/// `range_fast_cmp(a, b, ssup)` (rangetypes.c:1309): the sortsupport comparator.
pub fn range_fast_cmp(
    _typcache: &TypeCacheEntry,
    _r1: RangeTypeP<'_>,
    _r2: RangeTypeP<'_>,
) -> PgResult<i32> {
    todo!("range_fast_cmp")
}
