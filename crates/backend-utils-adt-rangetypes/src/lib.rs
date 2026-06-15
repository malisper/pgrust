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
//!
//! ## Datum-completion (Wave 8 — funcapi/rangetypes/sortsupport + top consumers)
//!
//! This crate has NO in-scope *internal* shim site to migrate onto the canonical
//! `types_tuple::Datum<'mcx>`: every surviving use of the bare-word shim newtype
//! `types_datum::Datum` is a genuinely-sanctioned ABI edge (per the
//! datum-redesign plan), a value pinned by an *external* type-carrier, or a value
//! that must cross a still-bare-word seam to an unported/unmigrated owner —
//! never a free-standing internal value:
//!
//! * `range_fmgr_boundary` is wholly the `PGFunction` bare-word arg/return ABI
//!   edge: `PG_GETARG_DATUM` reads `fcinfo.arg(n).value` (bare word) and every
//!   `PG_RETURN_*` produces the bare-word result word.
//! * `range_repr_serialize` is the on-disk codec edge: `store_att_byval` /
//!   `fetch_att` (the two sanctioned by-value codec sites) plus the varlena
//!   *pointer* codec (`val.as_usize() as *const u8` over a serialized image and
//!   `Datum::from_usize(ptr)`), which is a raw pointer into the ADT's private
//!   image — NOT an owned `ByRef(PgVec)` byte image.
//! * Every bound value flows through `types_rangetypes::RangeBound.val`, whose
//!   type is the bare-word `types_datum::Datum` *in the out-of-scope*
//!   `types-rangetypes` crate; this crate cannot widen that carrier here.
//! * Bound/element values that cross a seam (`function_call{1,2}_coll`,
//!   `text_to_cstring`, `datum_get_range_type_p`, `const_value`, `make_const`,
//!   the planner-neighbor seams) ride those seams' still-bare-word contracts,
//!   which are pinned by unmigrated consumers (multirangetypes, range-selfuncs)
//!   and unported owners (the optimizer).
//!
//! The single canonical reference already present — `numrange_subdiff` forwarding
//! to the migrated `numeric_subdiff` seam (which takes `types_tuple::Datum`) — is
//! the only spot the canonical type is reachable, and it is already correct.
//! Migrating any other edge here would diverge from a contract not owned by this
//! crate (the `RangeBound.val` carrier in `types-rangetypes` and the bare-word
//! seam contracts), so it is deferred to those owners (cf. the execExpr / nodeHash
//! Wave-6/7 "contract-blocked, no internal shim" status).

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

    // RangeType engine constructors / flags (range_repr_serialize) — signatures
    // match the seams exactly.
    seams::make_range::set(range_repr_serialize::make_range);
    seams::make_empty_range::set(range_repr_serialize::make_empty_range);
    seams::range_get_flags::set(range_repr_serialize::range_get_flags);

    // Bound comparison + `*_internal` predicate kernels (range_bounds_compare).
    // All but two match the seams exactly; range_adjacent_internal and
    // bounds_adjacent take an extra `mcx` for their transient probe range, so
    // they go through scratch-context adapters.
    seams::range_compare::set(range_bounds_compare::range_compare);
    seams::range_contains_elem_internal::set(range_bounds_compare::range_contains_elem_internal);
    seams::range_contains_internal::set(range_bounds_compare::range_contains_internal);
    seams::range_before_internal::set(range_bounds_compare::range_before_internal);
    seams::range_after_internal::set(range_bounds_compare::range_after_internal);
    seams::range_overlaps_internal::set(range_bounds_compare::range_overlaps_internal);
    seams::range_overleft_internal::set(range_bounds_compare::range_overleft_internal);
    seams::range_overright_internal::set(range_bounds_compare::range_overright_internal);
    seams::range_adjacent_internal::set(range_bounds_compare::range_adjacent_internal_seam);
    seams::bounds_adjacent::set(range_bounds_compare::bounds_adjacent_seam);

    // Set operations (range_setops). union/minus/intersect match the seams
    // exactly; split_internal returns `(Option, Option)` vs the seam's
    // `Option<(_, _)>`, so it goes through an adapter.
    seams::range_union_internal::set(range_setops::range_union_internal);
    seams::range_minus_internal::set(range_setops::range_minus_internal);
    seams::range_intersect_internal::set(range_setops::range_intersect_internal);
    seams::range_split_internal::set(range_setops::range_split_internal_seam);

    // Generic range I/O procs (range_io). Now fully wired: get_range_io_data
    // resolves the element type's typcache + I/O proc OID through the
    // typcache/lsyscache seams, and the element typioproc calls go through the
    // by-OID fmgr I/O seams (input/output/receive/send_function_call). The
    // out/send adapters return owned String/Vec, so their element-fn transient
    // allocations run against a private scratch context.
    seams::range_in::set(range_io::range_in_seam);
    seams::range_out::set(range_io::range_out_seam);
    seams::range_recv::set(range_io::range_recv_seam);
    seams::range_send::set(range_io::range_send_seam);
}
