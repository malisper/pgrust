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

// ---------------------------------------------------------------------------
// Extension for the `backend-utils-adt-multirangetypes` unit.
//
// `multirangetypes.c` delegates its per-member range math to these
// `rangetypes.c` internals (in addition to the selectivity-estimator seams
// above). The owning unit installs them from its `init_seams()`; until then a
// call panics loudly. `bool`-returning predicates become `PgResult<bool>`
// because the subtype `cmp` support function they call can `ereport(ERROR)`;
// the allocating constructors take `mcx` and return `PgResult<RangeTypeP>`.
// ---------------------------------------------------------------------------

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

seam_core::seam!(
    /// `make_range(typcache, &lower, &upper, empty, NULL)` (rangetypes.c):
    /// canonicalize and serialize a `RangeType` from in-memory bounds, allocated
    /// in `mcx`. `Err` carries the canonicalize / serialization `ereport(ERROR)`s
    /// and OOM.
    pub fn make_range<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        lower: &RangeBound,
        upper: &RangeBound,
        empty: bool,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `make_empty_range(typcache)` (rangetypes.c): build the serialized empty
    /// `RangeType` for the range type, allocated in `mcx`. `Err` carries
    /// serialization `ereport(ERROR)`s and OOM.
    pub fn make_empty_range<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_get_flags(range)` (rangetypes.c): the range's flags byte
    /// (`RANGE_EMPTY`, `RANGE_LB_INC`, ...). Empty ranges read `RANGE_EMPTY`.
    pub fn range_get_flags(range: RangeTypeP<'_>) -> u8
);

seam_core::seam!(
    /// `range_compare(key1, key2, arg)` (rangetypes.c): the `qsort_arg`
    /// comparator over `RangeType *` pointers (`arg` is the `TypeCacheEntry`),
    /// ordering by lower bound then upper bound. `Err` carries the subtype
    /// `cmp`'s `ereport(ERROR)`s.
    pub fn range_compare(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `range_contains_elem_internal(typcache, r, val)` (rangetypes.c): whether
    /// the range contains the element value. `Err` carries the subtype `cmp`'s
    /// `ereport(ERROR)`s.
    pub fn range_contains_elem_internal(
        typcache: &TypeCacheEntry,
        r: RangeTypeP<'_>,
        val: Datum,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_contains_internal(typcache, r1, r2)` (rangetypes.c): whether `r1`
    /// contains `r2`. `Err` carries the subtype `cmp`'s `ereport(ERROR)`s.
    pub fn range_contains_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_before_internal(typcache, r1, r2)` (rangetypes.c): whether `r1` is
    /// strictly before `r2`. `Err` carries the subtype `cmp`'s `ereport(ERROR)`s.
    pub fn range_before_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_after_internal(typcache, r1, r2)` (rangetypes.c): whether `r1` is
    /// strictly after `r2`. `Err` carries the subtype `cmp`'s `ereport(ERROR)`s.
    pub fn range_after_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_adjacent_internal(typcache, r1, r2)` (rangetypes.c): whether `r1`
    /// is adjacent to `r2`. `Err` carries the subtype `cmp`'s `ereport(ERROR)`s.
    pub fn range_adjacent_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_overlaps_internal(typcache, r1, r2)` (rangetypes.c): whether `r1`
    /// overlaps `r2`. `Err` carries the subtype `cmp`'s `ereport(ERROR)`s.
    pub fn range_overlaps_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_overleft_internal(typcache, r1, r2)` (rangetypes.c): whether `r1`
    /// does not extend to the right of `r2`. `Err` carries the subtype `cmp`'s
    /// `ereport(ERROR)`s.
    pub fn range_overleft_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_overright_internal(typcache, r1, r2)` (rangetypes.c): whether `r1`
    /// does not extend to the left of `r2`. `Err` carries the subtype `cmp`'s
    /// `ereport(ERROR)`s.
    pub fn range_overright_internal(
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'_>,
        r2: RangeTypeP<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `range_union_internal(typcache, r1, r2, strict)` (rangetypes.c): the union
    /// of `r1` and `r2`, allocated in `mcx`. `Err` carries the "result of range
    /// union would not be contiguous" `ereport(ERROR)`, the subtype `cmp`'s
    /// errors, and OOM.
    pub fn range_union_internal<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'mcx>,
        r2: RangeTypeP<'mcx>,
        strict: bool,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_minus_internal(typcache, r1, r2)` (rangetypes.c): `r1` minus `r2`,
    /// allocated in `mcx`. `Err` carries the "result of range difference would
    /// not be contiguous" `ereport(ERROR)`, the subtype `cmp`'s errors, and OOM.
    pub fn range_minus_internal<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'mcx>,
        r2: RangeTypeP<'mcx>,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_intersect_internal(typcache, r1, r2)` (rangetypes.c): the
    /// intersection of `r1` and `r2`, allocated in `mcx`. `Err` carries the
    /// subtype `cmp`'s `ereport(ERROR)`s and OOM.
    pub fn range_intersect_internal<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'mcx>,
        r2: RangeTypeP<'mcx>,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_split_internal(typcache, r1, r2, &output1, &output2)`
    /// (rangetypes.c): if `r2` splits `r1`, returns `Some((left, right))` (the
    /// two surrounding ranges, allocated in `mcx`); else `None`. `Err` carries
    /// the subtype `cmp`'s `ereport(ERROR)`s and OOM.
    pub fn range_split_internal<'mcx>(
        mcx: Mcx<'mcx>,
        typcache: &TypeCacheEntry,
        r1: RangeTypeP<'mcx>,
        r2: RangeTypeP<'mcx>,
    ) -> PgResult<Option<(RangeTypeP<'mcx>, RangeTypeP<'mcx>)>>
);

// ---------------------------------------------------------------------------
// Range type I/O procs, invoked generically by `multirange_in`/`out`/`recv`/
// `send` (multirangetypes.c) through the cached `typioproc` FmgrInfo. All
// built-in range types register the generic `range_in`/`range_out`/
// `range_recv`/`range_send` (rangetypes.c) as their I/O procs, so the
// multirange ADT reaches them through these owner seams keyed by the range
// type OID + typmod. The owning unit installs them from `init_seams()`; until
// then a call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `range_in(cstring, rngtypoid, typmod)` (rangetypes.c) invoked via
    /// `InputFunctionCallSafe`: parse one range literal into a serialized
    /// `RangeType`, allocated in `mcx`. A soft (`escontext`) error yields
    /// `Ok(None)` (the C `InputFunctionCallSafe` returning `false`); a hard
    /// `ereport(ERROR)` is carried on `Err`. `rngtypoid` is the cached
    /// `typioparam` (the range type OID).
    pub fn range_in<'mcx>(
        mcx: Mcx<'mcx>,
        input: &str,
        rngtypoid: Oid,
        typmod: i32,
    ) -> PgResult<Option<RangeTypeP<'mcx>>>
);

seam_core::seam!(
    /// `range_out(anyrange)` (rangetypes.c) invoked via `OutputFunctionCall`:
    /// render one serialized `RangeType` to its text representation. `Err`
    /// carries the output proc's `ereport(ERROR)`s.
    pub fn range_out(range: RangeTypeP<'_>) -> PgResult<String>
);

seam_core::seam!(
    /// `range_recv(internal, rngtypoid, typmod)` (rangetypes.c) invoked via
    /// `ReceiveFunctionCall`: decode one range from its binary wire form
    /// (`buf` is the per-range sub-message bytes), allocated in `mcx`. `Err`
    /// carries the receive proc's `ereport(ERROR)`s.
    pub fn range_recv<'mcx>(
        mcx: Mcx<'mcx>,
        buf: &[u8],
        rngtypoid: Oid,
        typmod: i32,
    ) -> PgResult<RangeTypeP<'mcx>>
);

seam_core::seam!(
    /// `range_send(anyrange)` (rangetypes.c) invoked via `SendFunctionCall`:
    /// encode one serialized `RangeType` into its binary wire form, returning
    /// the `bytea` payload bytes with the varlena header stripped (`VARDATA`,
    /// `VARSIZE - VARHDRSZ`). `Err` carries the send proc's `ereport(ERROR)`s.
    pub fn range_send(range: RangeTypeP<'_>) -> PgResult<Vec<u8>>
);
