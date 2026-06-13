//! `backend-utils-adt-rangetypes` (`src/backend/utils/adt/rangetypes.c`, ~3017
//! LOC) — the range ADT engine over the REAL `types-rangetypes` structs
//! (`RangeType` / `RangeBound` / `RangeTypeP`) and real `Datum`, NOT a byte
//! blob.
//!
//! SCAFFOLD STAGE. Every family module carries the full function inventory of
//! `rangetypes.c` with C-faithful signatures and `todo!()` bodies so the crate
//! compiles; the logic lands family-by-family afterwards. The crate owns and
//! installs the inward `backend-utils-adt-rangetypes-seams`
//! (`range_cmp_bounds` / `range_subdiff` / `range_get_typcache` /
//! `range_serialize` / `range_deserialize` / `datum_get_range_type_p`) already
//! consumed by `backend-utils-adt-range-selfuncs`.
//!
//! Families:
//! - [`range_repr_serialize`] — `RangeType` engine: serialize/deserialize,
//!   flags, `make_range`/`make_empty_range`, `datum_compute_size`/`datum_write`.
//!   Owns the inward `range_serialize`/`range_deserialize`/`DatumGetRangeTypeP`
//!   seams.
//! - [`range_bounds_compare`] — `range_cmp_bounds[_values]`, `range_compare`,
//!   `bounds_adjacent`, `range_get_typcache`, and the `*_internal` predicate
//!   kernels.
//! - [`range_setops`] — `range_minus`/`union`/`intersect`/`split_internal`,
//!   `range_merge`, `range_intersect_agg_transfn`.
//! - [`range_canonical_subdiff_hash`] — int4/int8/date `*_canonical`,
//!   `*_subdiff`, `hash_range[_extended]`, `range_cmp`, `range_sortsupport`.
//! - [`range_io`] — `range_parse[_flags]`/`parse_bound`/`deparse`/`bound_escape`,
//!   `get_range_io_data`, `range_recv`/`send`.
//! - [`range_fmgr_boundary`] — the ~30 `PG_FUNCTION_ARGS` entry points
//!   marshalling `Datum` <-> typed args.
//! - [`range_planner_support`] — `find_simplified_clause`/`build_bound_expr`
//!   support functions.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

pub mod range_bounds_compare;
pub mod range_canonical_subdiff_hash;
pub mod range_fmgr_boundary;
pub mod range_io;
pub mod range_planner_support;
pub mod range_repr_serialize;
pub mod range_setops;

/// Install the inward `backend-utils-adt-rangetypes-seams` this crate owns.
///
/// Called once at startup via `seams-init`. The range/multirange selectivity
/// estimators (`backend-utils-adt-range-selfuncs`) reach the range ADT through
/// these slots across the dependency cycle.
pub fn init_seams() {
    use backend_utils_adt_rangetypes_seams as seams;

    seams::range_cmp_bounds::set(range_bounds_compare::range_cmp_bounds);
    seams::range_subdiff::set(range_canonical_subdiff_hash::range_subdiff);
    seams::range_get_typcache::set(range_bounds_compare::range_get_typcache);
    seams::range_serialize::set(range_repr_serialize::range_serialize_seam);
    seams::range_deserialize::set(range_repr_serialize::range_deserialize_seam);
    seams::datum_get_range_type_p::set(range_repr_serialize::datum_get_range_type_p);
}
