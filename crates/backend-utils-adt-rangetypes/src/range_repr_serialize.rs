//! Family `range-repr-serialize`: the `RangeType` serialization engine over the
//! REAL `types-rangetypes` structs and real `Datum` (NOT a byte blob).
//!
//! Mirrors `rangetypes.c`: `range_serialize` / `range_deserialize` /
//! `range_get_flags` / `range_set_contain_empty`, `make_range` /
//! `make_empty_range`, and the private `datum_compute_size` / `datum_write`
//! payload helpers. This family owns and (via `lib::init_seams`) installs the
//! inward `range_serialize` / `range_deserialize` / `DatumGetRangeTypeP` seams.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::{RangeBound, RangeType, RangeTypeP};

/// `range_serialize(typcache, lower, upper, empty, escontext)` (rangetypes.c:1791):
/// build a serialized `RangeType` from in-memory bounds, allocated in `mcx`.
pub fn range_serialize<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _lower: &RangeBound,
    _upper: &RangeBound,
    _empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_serialize: serialize bounds into a RangeType varlena")
}

/// Inward seam shape for `range_serialize` (thin pass-through to
/// [`range_serialize`]). Matches the `backend-utils-adt-rangetypes-seams`
/// signature; C `escontext` is `NULL` here (hard-error path).
pub fn range_serialize_seam<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    range_serialize(mcx, typcache, lower, upper, empty)
}

/// `range_deserialize(typcache, range, &lower, &upper, &empty)`
/// (rangetypes.c:1920): explode a serialized `RangeType` into its bounds.
pub fn range_deserialize(
    _typcache: &TypeCacheEntry,
    _range: RangeTypeP<'_>,
) -> PgResult<(RangeBound, RangeBound, bool)> {
    todo!("range_deserialize: explode a RangeType into (lower, upper, empty)")
}

/// Inward seam shape for `range_deserialize`.
pub fn range_deserialize_seam(
    typcache: &TypeCacheEntry,
    range: RangeTypeP<'_>,
) -> PgResult<(RangeBound, RangeBound, bool)> {
    range_deserialize(typcache, range)
}

/// `range_get_flags(range)` (rangetypes.c:1987): the trailing flags byte.
pub fn range_get_flags(_range: RangeTypeP<'_>) -> u8 {
    todo!("range_get_flags: read the trailing flags byte")
}

/// `range_set_contain_empty(range)` (rangetypes.c:2001): set `RANGE_CONTAIN_EMPTY`.
pub fn range_set_contain_empty(_range: RangeTypeP<'_>) {
    todo!("range_set_contain_empty: OR in RANGE_CONTAIN_EMPTY")
}

/// `make_range(typcache, lower, upper, empty, escontext)` (rangetypes.c:2016):
/// canonicalize (if the type has a canonical fn) and serialize.
pub fn make_range<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
    _lower: &RangeBound,
    _upper: &RangeBound,
    _empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("make_range: validate bounds, canonicalize, serialize")
}

/// `make_empty_range(typcache)` (rangetypes.c:2229): the canonical empty range.
pub fn make_empty_range<'mcx>(
    _mcx: Mcx<'mcx>,
    _typcache: &TypeCacheEntry,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("make_empty_range")
}

/// `DatumGetRangeTypeP(d)` (rangetypes.h): detoast a `Datum` into a `RangeType *`,
/// copying into `mcx` if detoasting is needed. Owns the inward seam.
pub fn datum_get_range_type_p<'mcx>(_mcx: Mcx<'mcx>, _d: Datum) -> PgResult<RangeTypeP<'mcx>> {
    todo!("datum_get_range_type_p: detoast a Datum to a RangeType *")
}

/// `datum_compute_size(data_length, val, typbyval, typalign, typlen, typstorage)`
/// (rangetypes.c:2747): running serialized size of one bound value.
pub fn datum_compute_size(
    _data_length: usize,
    _val: Datum,
    _typbyval: bool,
    _typalign: u8,
    _typlen: i16,
    _typstorage: u8,
) -> usize {
    todo!("datum_compute_size")
}

/// `datum_write(ptr, datum, typbyval, typalign, typlen, typstorage)`
/// (rangetypes.c:2773): write one bound value into the serialized image,
/// returning the advanced write cursor offset.
pub fn datum_write(
    _ptr: *mut RangeType,
    _datum: Datum,
    _typbyval: bool,
    _typalign: u8,
    _typlen: i16,
    _typstorage: u8,
) -> usize {
    todo!("datum_write")
}
