//! Seam declarations for the `backend-utils-mmgr-dsa` unit
//! (`utils/mmgr/dsa.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. A live `dsa_area` is a backend-local handle into
//! the DSA substrate's own structures, so it crosses the seam as the raw
//! `*mut DsaArea` pointer the C code holds — never dereferenced by consumers.

use types_storage::{dsa_handle, DsaArea};
use types_error::PgResult;

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
