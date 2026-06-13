//! Seam declarations for the `backend-utils-adt-rangetypes` unit
//! (`utils/adt/rangetypes.c`), trimmed to the range ADT primitives the
//! range/multirange selectivity estimators call across the dependency cycle.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::{RangeBound, RangeTypeP};

seam_core::seam!(
    /// `range_cmp_bounds(typcache, &b1, &b2)` (rangetypes.c): compare two range
    /// bounds with the subtype's `cmp` support function, returning the sign of
    /// `b1 <=> b2`. `Err` carries the support function's `ereport(ERROR)`s.
    pub fn range_cmp_bounds(
        typcache: &TypeCacheEntry,
        b1: &RangeBound,
        b2: &RangeBound,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `FunctionCall2Coll(&typcache->rng_subdiff_finfo, rng_collation, v1, v2)`
    /// returning `DatumGetFloat8(..)` (rangetypes_selfuncs.c usage): the
    /// subtype's `subdiff` distance between two bound values. `Err` carries the
    /// support function's `ereport(ERROR)`s.
    pub fn range_subdiff(typcache: &TypeCacheEntry, v1: Datum, v2: Datum) -> PgResult<f64>
);

seam_core::seam!(
    /// `range_get_typcache(fcinfo, rngtypid)` (rangetypes.c): the cached
    /// `TypeCacheEntry` for the range type `rngtypid`, returned by value (a copy
    /// of the long-lived cache entry's range-support fields). `Err` carries the
    /// type-cache lookup `ereport(ERROR)`s.
    pub fn range_get_typcache(rngtypid: Oid) -> PgResult<TypeCacheEntry>
);

seam_core::seam!(
    /// `range_serialize(typcache, &lower, &upper, empty, NULL)` (rangetypes.c):
    /// build a serialized `RangeType` from in-memory bounds, allocated in `mcx`
    /// (C: the current context). `Err` carries serialization `ereport(ERROR)`s
    /// and OOM.
    pub fn range_serialize<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        lower: &RangeBound,
        upper: &RangeBound,
        empty: bool,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_deserialize(typcache, range, &lower, &upper, &empty)`
    /// (rangetypes.c): explode a serialized `RangeType` into its lower/upper
    /// bounds, returning `(lower, upper, empty)`. `Err` carries deserialization
    /// `ereport(ERROR)`s.
    pub fn range_deserialize(
        typcache: &TypeCacheEntry,
        range: RangeTypeP<'_>,
    ) -> PgResult<(RangeBound, RangeBound, bool)>
);

seam_core::seam!(
    /// `DatumGetRangeTypeP(d)` (rangetypes.h): detoast a `Datum` into a
    /// `RangeType *`, copying into `mcx` if detoasting is needed. `Err` carries
    /// detoast `ereport(ERROR)`s and OOM.
    pub fn datum_get_range_type_p<'mcx>(
        mcx: Mcx<'mcx>,
        d: Datum,
    ) -> PgResult<RangeTypeP<'mcx>>
);
