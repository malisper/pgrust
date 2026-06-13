//! Seam declarations for the dynamic shared-memory allocator (`utils/mmgr/dsa.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. A live `dsa_area` is a backend-local handle into
//! the DSA substrate's own structures, so it crosses the seam as the raw
//! `*mut DsaArea` pointer the C code holds — never dereferenced by consumers.

#![allow(unused_doc_comments)]
use types_execparallel::{DsaAreaHandle, DsmSegmentHandle, DsaPointer, SerializeCursor, Size};
use types_storage::{dsa_handle, DsaArea};
use types_error::PgResult;

// --- Parallel-executor in-place DSA seams (consumer: backend-utils-mmgr-dsa parallel path) ---

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

// --- DSM-registry DSA seams (consumer: backend-storage-ipc-dsm-registry) ---

seam_core::seam!(
    /// `dsa_create(tranche_id)` (macro for `dsa_create_ext` with the default
    /// init/max segment sizes) — create a new DSA area in dynamic shared
    /// memory and attach to it, returning the backend-local area handle. `Err`
    /// carries the `ereport(ERROR)` for the underlying DSM allocation failure.
    pub fn dsa_create(tranche_id: i32) -> PgResult<*mut DsaArea>
);

seam_core::seam!(
    /// `dsa_attach(dsa_handle handle)` — attach to an existing DSA area created
    /// by another backend, returning the backend-local area handle. `Err`
    /// carries the `ereport(ERROR)` for a bogus handle / attach failure.
    pub fn dsa_attach(handle: dsa_handle) -> PgResult<*mut DsaArea>
);

seam_core::seam!(
    /// `dsa_pin(dsa_area *area)` — pin the area so it stays allocated even when
    /// every backend has detached. `Err` carries the C
    /// `elog(ERROR, "dsa_area already pinned")`.
    pub fn dsa_pin(area: *mut DsaArea) -> PgResult<()>
);

seam_core::seam!(
    /// `dsa_pin_mapping(dsa_area *area)` — pin this backend's mapping of the
    /// area so it survives the current resource owner. `Err` carries the
    /// `ereport(ERROR)` for an allocation failure while remembering the
    /// mapping.
    pub fn dsa_pin_mapping(area: *mut DsaArea) -> PgResult<()>
);

seam_core::seam!(
    /// `dsa_get_handle(dsa_area *area)` — the area's handle, for passing to
    /// another backend that will `dsa_attach`.
    pub fn dsa_get_handle(area: *mut DsaArea) -> dsa_handle
);
