//! Seam declarations for the `backend-postmaster-auxprocess` unit
//! (`postmaster/auxprocess.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `AuxiliaryProcessMainCommon()` (auxprocess.c) — common initialization
    /// for auxiliary processes: PGPROC slot, BaseInit, procsignal slot,
    /// backend status. Paths inside (e.g. allocation) can `ereport(ERROR)`.
    pub fn auxiliary_process_main_common() -> types_error::PgResult<()>
);
