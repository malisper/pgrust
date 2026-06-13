//! KEYSTONE family — `src/backend/utils/adt/expandeddatum.c`.
//!
//! Support functions for "expanded" value representations: the
//! `ExpandedObjectHeader` ABI that every expanded container type (expanded
//! records in [`crate::expandedrecord`], expanded arrays in arrayfuncs, etc.)
//! embeds and dispatches through. This is the shared foundation the rest of
//! the unit — and external consumers such as
//! `backend-access-common-heaptuple`'s `heap_compute_data_size` / `fill_val`,
//! which flatten expanded datums into a tuple — compile against, so it is
//! ported in the scaffold phase and its two consumed seams
//! (`eoh_get_flat_size` / `eoh_flatten_into`) are installed here from
//! [`crate::init_seams`].
//!
//! In the owned model an expanded datum crosses as the typed
//! [`types_datum::ExpandedObjectRef`] handle (C's `ExpandedObjectHeader *`
//! reached through `DatumGetEOHP`), not raw `&[u8]`. The method dispatch
//! (`eoh_methods->get_flat_size` / `->flatten_into`) is owned by the concrete
//! expanded type; the keystone routes a flatten request to whichever expanded
//! type produced the datum.

use types_datum::ExpandedObjectRef;
use types_error::PgResult;

/// `EOH_get_flat_size(eohptr)` (expandeddatum.c):
///
/// ```c
/// Size
/// EOH_get_flat_size(ExpandedObjectHeader *eohptr)
/// {
///     return eohptr->eoh_methods->get_flat_size(eohptr);
/// }
/// ```
///
/// A one-line convenience wrapper that chases the object's method table and
/// invokes its `get_flat_size`. The method itself belongs to the concrete
/// expanded type (expanded record / expanded array), so in the owned model it
/// is the [`eom_get_flat_size`](backend_utils_adt_expanded_methods_seams::eom_get_flat_size)
/// seam — exactly mirroring the C indirection through `eoh_methods`.
///
/// This is the implementation installed into
/// `backend_utils_adt_misc2_seams::eoh_get_flat_size` so external flatteners
/// (heaptuple) can size an expanded datum without depending on this crate.
pub fn eoh_get_flat_size(eoh: ExpandedObjectRef<'_>) -> PgResult<usize> {
    backend_utils_adt_expanded_methods_seams::eom_get_flat_size::call(eoh)
}

/// `EOH_flatten_into(eohptr, result, allocated_size)` (expandeddatum.c):
///
/// ```c
/// void
/// EOH_flatten_into(ExpandedObjectHeader *eohptr,
///                  void *result, Size allocated_size)
/// {
///     eohptr->eoh_methods->flatten_into(eohptr, result, allocated_size);
/// }
/// ```
///
/// Flatten the expanded object into `dest`, which is exactly the preceding
/// `eoh_get_flat_size` bytes long (C passes `allocated_size` for the method to
/// cross-check; the slice length carries it here). Dispatches through the
/// object's method table — the concrete expanded type's `flatten_into`, modeled
/// as the [`eom_flatten_into`](backend_utils_adt_expanded_methods_seams::eom_flatten_into)
/// seam.
///
/// Installed into `backend_utils_adt_misc2_seams::eoh_flatten_into`.
pub fn eoh_flatten_into(eoh: ExpandedObjectRef<'_>, dest: &mut [u8]) -> PgResult<()> {
    backend_utils_adt_expanded_methods_seams::eom_flatten_into::call(eoh, dest)
}
