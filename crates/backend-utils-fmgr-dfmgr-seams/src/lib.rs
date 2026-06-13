//! Seam declarations for the `backend-utils-fmgr-dfmgr` unit
//! (`utils/fmgr/dfmgr.c`): dynamic-library symbol loading.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `load_external_function(filename, funcname, signalNotFound, filehandle)`
    /// specialized to the archiver's use: load `filename` and resolve the
    /// `_PG_archive_module_init` symbol. The archiver passes
    /// `signalNotFound = false`, so a missing symbol yields `Ok(None)` (the C
    /// returns NULL) rather than an `ereport(ERROR)`; the library load itself
    /// can still `ereport(ERROR)` (carried on `Err`).
    pub fn load_archive_module_init(
        filename: &str,
    ) -> types_error::PgResult<Option<types_pgarch::ArchiveModuleInit>>
);
