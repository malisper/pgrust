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

/// `EOH_get_flat_size(eohptr)` (expandeddatum.c): the number of bytes the
/// expanded object would occupy once flattened. C dispatches through
/// `eohptr->eoh_methods->get_flat_size`.
///
/// This is the implementation installed into
/// `backend_utils_adt_misc2_seams::eoh_get_flat_size` so external flatteners
/// (heaptuple) can size an expanded datum without depending on this crate.
pub fn eoh_get_flat_size(_eoh: ExpandedObjectRef<'_>) -> PgResult<usize> {
    // Dispatches to the concrete expanded type's get_flat_size method
    // (expanded record / expanded array). Filled when the keystone's method
    // table lands alongside the expandedrecord family.
    todo!("EOH_get_flat_size: dispatch eoh_methods->get_flat_size")
}

/// `EOH_flatten_into(eohptr, result, allocated_size)` (expandeddatum.c):
/// flatten the expanded object into `dest`, which is exactly
/// `eoh_get_flat_size` bytes long. C dispatches through
/// `eohptr->eoh_methods->flatten_into`.
///
/// Installed into `backend_utils_adt_misc2_seams::eoh_flatten_into`.
pub fn eoh_flatten_into(_eoh: ExpandedObjectRef<'_>, _dest: &mut [u8]) -> PgResult<()> {
    todo!("EOH_flatten_into: dispatch eoh_methods->flatten_into")
}
