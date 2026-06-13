//! Seam declarations for the `backend-utils-fmgr-dfmgr` unit
//! (`utils/fmgr/dfmgr.c`): dynamic loading of external C functions.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! NB: the `CFuncHash` cache (`lookup_C_func` / `record_C_func`) and the
//! `fetch_finfo_record` `api_version` validation are the *function manager's*
//! own logic and live in `backend-utils-fmgr-core`; this seam is only the
//! genuine dfmgr external — loading the symbol and its info record.

use types_core::Oid;
use types_error::PgResult;
use types_fmgr::LoadedExternalFunc;

seam_core::seam!(
    /// `load_external_function(probin, prosrc, true, &libraryhandle)` then
    /// `fetch_finfo_record(libraryhandle, prosrc)` (dfmgr.c / fmgr.c) — load the
    /// extension symbol and its `Pg_finfo_record`. Returns the `(user_fn,
    /// api_version)` pair the function manager caches and validates. Can
    /// `ereport(ERROR)` (missing library / symbol, no info function), carried on
    /// `Err`. `function_id` is the pg_proc OID (diagnostics only here; the
    /// caller owns the `CFuncHash` keyed by it).
    pub fn load_external_function(
        probin: &str,
        prosrc: &str,
        function_id: Oid,
    ) -> PgResult<LoadedExternalFunc>
);
