//! Multirange types (`utils/adt/multirangetypes.c`, PostgreSQL 18.3): the I/O,
//! operator, set-operation, ordering and aggregate functions for the built-in
//! multirange types.
//!
//! A multirange is an ordered, non-overlapping, non-empty set of ranges of one
//! range type, serialized as a varlena: a [`MultirangeType`] header
//! (`types_rangetypes`) followed by a per-range item array, a flags byte array,
//! and the inlined bound payloads, in the multirange ADT's own private
//! encoding. This crate works over the detoasted-pointer handle
//! [`MultirangeTypeP`] (opacity inherited from the range/multirange ADT) plus an
//! [`Mcx`] allocator, mirroring the C `MultirangeType *` + current-context model.
//!
//! ## Family layout (decomposition)
//!
//!  * [`serialize_core`] — the varlena serialization layer (size estimate,
//!    write/make/serialize/deserialize, bounds-offset/get-range/get-bounds,
//!    canonicalize+sort). Owns the inward seams `make_multirange` and
//!    `multirange_get_bounds`.
//!  * [`typcache_io`] — the multirange typcache lookup, `DatumGetMultirangeTypeP`
//!    detoast, and the `in`/`out`/`recv`/`send` I/O functions (routing the range
//!    I/O through `rangetypes-seams`). Owns the inward seams
//!    `multirange_get_typcache` and `datum_get_multirange_type_p`.
//!  * [`operators`] — the containment / overlap / position / adjacency
//!    predicates (`*_internal` + SQL wrappers), equality, and the
//!    accessor / unnest functions.
//!  * [`setops_ordering_agg`] — union / minus / intersect, `range_merge`,
//!    ordering (`cmp`/`lt`/`le`/`ge`/`gt`), hashing, and the range/multirange
//!    aggregates.
//!
//! ## Seams
//!
//! The per-member range math crosses the `backend-utils-adt-rangetypes-seams`
//! cycle (`range_deserialize`, `range_cmp_bounds`, `make_range`,
//! `range_*_internal`, ...). The four inward seams this unit owns
//! (`multirange_get_typcache`, `make_multirange`, `multirange_get_bounds`,
//! `datum_get_multirange_type_p`) are declared in
//! `backend-utils-adt-multirangetypes-seams` and installed here in
//! [`init_seams`].

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod operators;
pub mod serialize_core;
pub mod setops_ordering_agg;
pub mod typcache_io;

/// Install every inward seam this unit owns. Mirrors the `init_seams()`
/// discipline: the owning crate wires its own seam slots, `seams-init` only
/// calls this once at startup.
pub fn init_seams() {
    use backend_utils_adt_multirangetypes_seams as seams;

    seams::multirange_get_typcache::set(typcache_io::multirange_get_typcache);
    seams::datum_get_multirange_type_p::set(typcache_io::datum_get_multirange_type_p);
    seams::multirange_is_empty::set(typcache_io::multirange_is_empty_seam);
    seams::make_multirange::set(serialize_core::make_multirange);
    seams::multirange_get_bounds::set(serialize_core::multirange_get_bounds);
}
