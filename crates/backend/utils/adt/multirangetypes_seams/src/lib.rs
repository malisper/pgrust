//! Seam declarations for the `backend-utils-adt-multirangetypes` unit
//! (`utils/adt/multirangetypes.c`), trimmed to the multirange ADT primitives the
//! multirange selectivity estimator calls across the dependency cycle.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::{MultirangeTypeP, RangeBound, RangeTypeP};
use types_tuple::heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `multirange_get_typcache(fcinfo, mltrngtypid)` (multirangetypes.c): the
    /// cached `TypeCacheEntry` for the multirange type `mltrngtypid`, returned by
    /// value. Its `rngtype` sub-entry is the range type-cache entry whose `cmp` /
    /// `subdiff` drive the math-time seams. `Err` carries the type-cache lookup
    /// `ereport(ERROR)`s.
    pub fn multirange_get_typcache(mltrngtypid: Oid) -> PgResult<TypeCacheEntry>
);

seam_core::seam!(
    /// `make_multirange(mltrngtypoid, rangetyp, range_count, &ranges)`
    /// (multirangetypes.c): build a serialized `MultirangeType` from
    /// `range_count` ranges, allocated in `mcx`. `Err` carries
    /// `ereport(ERROR)`s and OOM.
    pub fn make_multirange<'mcx>(
        mcx: Mcx<'mcx>,
        mltrngtypoid: Oid,
        rangetyp: &TypeCacheEntry,
        ranges: &[RangeTypeP<'mcx>],
    ) -> PgResult<MultirangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `multirange_get_bounds(rangetyp, multirange, i, &lower, &upper)`
    /// (multirangetypes.c): the lower/upper bounds of the `i`th range in a
    /// multirange. `Err` carries `ereport(ERROR)`s.
    pub fn multirange_get_bounds(
        rangetyp: &TypeCacheEntry,
        multirange: MultirangeTypeP<'_>,
        i: u32,
    ) -> PgResult<(RangeBound, RangeBound)>
);

seam_core::seam!(
    /// `DatumGetMultirangeTypeP(d)` (multirangetypes.h): detoast a `Datum` into
    /// a `MultirangeType *`, copying into `mcx` if detoasting is needed. `Err`
    /// carries detoast `ereport(ERROR)`s and OOM.
    pub fn datum_get_multirange_type_p<'mcx>(
        mcx: Mcx<'mcx>,
        d: Datum,
    ) -> PgResult<MultirangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `DatumGetMultirangeTypeP(d)` (multirangetypes.h) for a value-carrying
    /// canonical [`DatumV`] whose on-disk `MultirangeType` varlena image rides the
    /// `Datum::ByRef` arm (header included). Unlike [`datum_get_multirange_type_p`],
    /// which interprets the `Datum` word as a bare pointer, this reads the image
    /// bytes directly -- the form needed for a planner `Const`'s by-reference
    /// `constvalue` in `multirangesel`, whose bare-word surrogate would be a
    /// non-dereferenceable in-buffer offset. Detoasts only a compressed/external
    /// image, copying into `mcx`. `Err` carries detoast `ereport(ERROR)`s and OOM.
    pub fn datum_get_multirange_type_p_value<'mcx>(
        mcx: Mcx<'mcx>,
        value: &DatumV<'mcx>,
    ) -> PgResult<MultirangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `MultirangeIsEmpty(DatumGetMultirangeTypeP(attval))` (multirangetypes.h
    /// / multirangetypes.c): detoast the by-reference multirange value and
    /// report whether it has zero ranges. Used by `ExecWithoutOverlapsNotEmpty`
    /// (execIndexing.c) to forbid empty multiranges in a WITHOUT OVERLAPS key.
    /// The value crosses as the AM's raw index-input word (`FormIndexDatum`
    /// output). `Err` carries the detoast `ereport(ERROR)` surface.
    pub fn multirange_is_empty<'mcx>(mcx: Mcx<'mcx>, attval: Datum) -> PgResult<bool>
);
