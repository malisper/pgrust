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

// --- `dsa_area *`-keyed allocation/addressing (consumer: backend-lib-dshash) ---
//
// dshash holds the `dsa_area *` the registry-path `dsa_create`/`dsa_attach`
// (above) returns, and reaches the DSA substrate through it directly — the C
// `dsa_allocate(area, ...)` / `dsa_free(area, dp)` / `dsa_get_address(area, dp)`
// it calls with that pointer. `dsa_get_address` returns the backend-local
// address (`void *`), carried as the `u64` the resolved address is (the same
// blessed `*mut`/`*const` shared-memory substrate exception dsa.c itself
// takes). The owner installs these alongside the registry-path seams.

seam_core::seam!(
    /// `dsa_allocate_extended(dsa_area *area, size_t size, int flags)` — allocate
    /// `size` bytes in the area, returning the pseudo-pointer (or
    /// `InvalidDsaPointer` when `DSA_ALLOC_NO_OOM` is set and the request fails).
    /// `Err` carries the C `ereport(ERROR)` for an out-of-memory failure when
    /// `DSA_ALLOC_NO_OOM` is not set.
    pub fn dsa_allocate_extended(area: *mut DsaArea, size: Size, flags: i32) -> PgResult<DsaPointer>
);

seam_core::seam!(
    /// `dsa_free(dsa_area *area, dsa_pointer dp)` — free a prior allocation.
    pub fn dsa_free_ptr(area: *mut DsaArea, dp: DsaPointer) -> PgResult<()>
);

seam_core::seam!(
    /// `dsa_get_address(dsa_area *area, dsa_pointer dp)` — the backend-local
    /// address for `dp` (the C `void *`), carried as the `u64` it resolves to;
    /// `0` for `InvalidDsaPointer` (C `NULL`). `Err` carries the C
    /// `ereport(ERROR)` for a reference to a freed segment.
    pub fn dsa_get_address_ptr(area: *mut DsaArea, dp: DsaPointer) -> PgResult<u64>
);
