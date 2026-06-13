//! Seam declarations for the `backend-utils-resowner` unit
//! (`utils/resowner/resowner.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. (The query-lifecycle model replaces most of
//! resowner with RAII guards — see `docs/query-lifecycle-raii.md`; this seam
//! covers the auxiliary-process bulk release that aux-process error recovery
//! still drives by name.)

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c) — release all
    /// resources held by `AuxProcessResourceOwner`. Called from auxiliary
    /// processes' error-recovery cleanup with `isCommit = false`. `Err`
    /// carries any `ereport` from a release callback.
    pub fn release_aux_process_resources(is_commit: bool) -> types_error::PgResult<()>
);
