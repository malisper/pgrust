//! Port of `src/backend/utils/adt/arrayfuncs.c` — the standard varlena
//! `ArrayType` machinery: byte-buffer accessors, construct/deconstruct +
//! `ArrayBuildState*` accumulators, element/slice get/set, I/O (`array_in` /
//! `array_out` / `array_recv` / `array_send`), comparison / hashing /
//! containment operators, iterators, and the SQL-facing functions.
//!
//! # Status
//!
//! The standard varlena-array machinery is ported: the `ARR_*` byte accessors,
//! construct/deconstruct + `ArrayBuildState*` accumulators, element/slice
//! get/set, I/O, comparison/hashing/containment, and the SQL-facing functions
//! all have real bodies. The prerequisite ABI types live in [`types_array`]
//! (authored field-for-field against `array.h` / `c.h`), and the inward/outward
//! seam declarations are wired.
//!
//! A few entry points are deliberately deferred (they mirror the C entry point
//! and panic loudly, per Mirror-PG-and-panic, until their owner lands):
//!
//!  * the **expanded-array** subsystem (`array_expanded.c`) is not ported, so
//!    `array_get_element_expanded`, `array_set_element_expanded`, and
//!    `construct_empty_expanded_array` panic;
//!  * `array_map` needs the `ExprState`/`ExprContext` arguments and the
//!    executor `ExecEvalExpr` seam (executor boundary);
//!  * `array_unnest_support` needs the planner support-request vocabulary
//!    (`SupportRequestRows`, `estimate_*`);
//!  * `arraysubs::value_word` does not yet handle Cstring/Composite/Expanded/
//!    Internal canonical replacement values.
//!
//! An array value is represented as the same contiguous byte buffer the C
//! `ArrayType *` points at (`&[u8]` / `PgVec<'mcx, u8>`); the `ARR_*` accessors
//! in [`foundation`] are the byte-offset equivalents of the C `array.h` macros.
//!
//! # Families
//!
//! - [`foundation`] — `ArrayType`/`int2vector`/`oidvector` byte math: `ARR_*`
//!   accessors, `att_*`/`fetch_att`/`store_att_byval`, `array_seek`,
//!   `array_bitmap_copy`, `array_copy`, the `*OID` element-type constants.
//!   Pure byte math, zero seams.
//! - [`construct`] — `construct_array`/`_md_array`/`_empty_array` +
//!   `deconstruct_array`, the `initArrayResult*`/`accumArrayResult*`/
//!   `makeArrayResult*` build-state families. OWNS + installs the inward
//!   arrayfuncs seams.
//! - [`element_slice`] — `array_get/set_element[_expanded]`, `array_ref` /
//!   `array_set`, `array_get/set_slice`, `array_ndims`/`dims`/`lower`/`upper`/
//!   `length`/`cardinality`.
//! - [`io`] — `array_in` + the `ReadArray*` parser, `array_out`,
//!   `array_recv` / `array_send` + `ReadArrayBinary`.
//! - [`ops`] — `array_eq`/`ne`/`lt`/`gt`/`le`/`ge`, `btarraycmp`, `array_cmp`,
//!   `hash_array`/`hash_array_extended`, `arrayoverlap`/`contains`/`contained`.
//! - [`sql`] — `array_larger`/`smaller`, `generate_subscripts`, `array_fill`/
//!   `remove`/`replace`, `width_bucket_array`, `trim_array`, the iterator,
//!   `array_map`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_variables)]
// Every fallible function here returns the shared project-wide `PgResult`.
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod arraysubs;
pub mod foundation;
pub mod construct;
pub mod element_slice;
pub mod io;
pub mod ops;
pub mod sql;

/// Install every inward seam this crate owns.
///
/// The `backend-utils-adt-arrayfuncs-seams` crate declares the polymorphic
/// array build/construct/deconstruct entry points that callers in other units
/// (reloptions, execExprInterp, …) reach through. This crate is their owner,
/// so it installs each one here exactly once; `seams-init` calls this at
/// startup.
pub fn init_seams() {
    use backend_utils_adt_arrayfuncs_seams as seams;

    seams::init_array_result_any::set(construct::init_array_result_any);
    seams::accum_array_result_any::set(construct::accum_array_result_any);
    seams::make_array_result_any::set(construct::make_array_result_any);
    seams::pfree_array_datum::set(construct::pfree_array_datum);
    seams::construct_array_builtin::set(construct::construct_array_builtin);
    seams::construct_array_expr::set(construct::construct_array_expr);
    seams::deconstruct_array::set(construct::deconstruct_array_seam);
    seams::deconstruct_text_array::set(construct::deconstruct_text_array);
    seams::decode_text_array_to_strings::set(construct::decode_text_array_to_strings);
    seams::deconstruct_tid_array::set(construct::deconstruct_tid_array);
    seams::construct_text_array::set(construct::construct_text_array);
    seams::text_array_out::set(construct::text_array_out);
    seams::build_text_array_nullable::set(construct::build_text_array_nullable);
    seams::build_name_array::set(construct::build_name_array);
    seams::array_to_text_elements::set(io::array_to_text_elements);
    seams::construct_int4_array::set(construct::construct_int4_array);
    seams::array_get_ndim::set(construct::array_get_ndim);
    seams::array_get_elemtype::set(construct::array_get_elemtype);
    seams::oid_array_datum::set(construct::oid_array_datum);
    seams::char_array_datum::set(construct::char_array_datum);
    seams::text_array_datum::set(construct::text_array_datum);
    seams::array_get_float4_values::set(construct::array_get_float4_values);
    seams::deconstruct_array_bytes::set(construct::deconstruct_array_bytes);
    seams::oidvector_to_oids_bytes::set(construct::oidvector_to_oids_bytes);
    seams::int2vector_to_i16s_bytes::set(construct::int2vector_to_i16s_bytes);
    seams::text_array_to_strings_bytes::set(construct::text_array_to_strings_bytes);

    // arraysubs.c — array subscripting exec callbacks.
    seams::array_subscript_fetch::set(arraysubs::array_subscript_fetch);
    seams::array_subscript_fetch_slice::set(arraysubs::array_subscript_fetch_slice);
    seams::array_subscript_assign::set(arraysubs::array_subscript_assign);
    seams::array_subscript_assign_slice::set(arraysubs::array_subscript_assign_slice);
    seams::array_subscript_fetch_old::set(arraysubs::array_subscript_fetch_old);
    seams::array_subscript_fetch_old_slice::set(arraysubs::array_subscript_fetch_old_slice);
}
