//! Seam declarations for the dynamic shared-memory allocator (`utils/mmgr/dsa.c`)
//! used by the parallel executor.
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

#![allow(unused_doc_comments)]
use types_execparallel::{DsaAreaHandle, DsmSegmentHandle, DsaPointer, SerializeCursor, Size};

/// `dsa_minimum_size()`.
seam_core::seam!(pub fn dsa_minimum_size() -> Size);
/// `dsa_create_in_place(place, size, tranche_id, segment)`.
seam_core::seam!(pub fn dsa_create_in_place(
    place: SerializeCursor,
    size: Size,
    tranche_id: i32,
    segment: DsmSegmentHandle,
) -> DsaAreaHandle);
/// `dsa_attach_in_place(place, segment)`.
seam_core::seam!(pub fn dsa_attach_in_place(place: SerializeCursor, segment: DsmSegmentHandle) -> DsaAreaHandle);
/// `dsa_detach(area)`.
seam_core::seam!(pub fn dsa_detach(area: DsaAreaHandle));
/// `dsa_allocate(area, size)`.
seam_core::seam!(pub fn dsa_allocate(area: DsaAreaHandle, size: Size) -> DsaPointer);
/// `dsa_free(area, dp)`.
seam_core::seam!(pub fn dsa_free(area: DsaAreaHandle, dp: DsaPointer));
/// `dsa_get_address(area, dp)` — a cursor over the addressed bytes.
seam_core::seam!(pub fn dsa_get_address(area: DsaAreaHandle, dp: DsaPointer) -> SerializeCursor);
