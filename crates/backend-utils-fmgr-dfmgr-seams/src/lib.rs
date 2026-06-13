//! Seam declarations for the dynamic-library loader (`utils/fmgr/dfmgr.c`) and
//! the `shmem_request_hook` (`miscinit.c` owns the hook pointer; the installed
//! hook body belongs to whatever extension/module registered it), plus the
//! output-plugin load + dispatch surface (`load_external_function` plus the
//! loaded plugin's `_PG_output_plugin_init` vtable) consumed by logical
//! decoding, and the archiver's `_PG_archive_module_init` loader. Calls panic
//! until the owners land.

#![allow(non_snake_case)]

use types_logical::CallbackInvocation;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::LoadedExternalFunc;

seam_core::seam!(
    /// `load_file(filename, restricted)` (`utils/fmgr/dfmgr.c`) — load and
    /// initialize a dynamically loadable module. `ereport(ERROR)`s on a missing
    /// or incompatible library.
    pub fn load_file(filename: &str, restricted: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `shmem_request_hook != NULL` — whether a `shmem_request_hook` is
    /// installed.
    pub fn shmem_request_hook_present() -> bool
);

seam_core::seam!(
    /// `shmem_request_hook()` — invoke the installed shared-memory request hook.
    pub fn shmem_request_hook() -> types_error::PgResult<()>
);

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

seam_core::seam!(
    /// `load_external_function(...)` + `plugin_init(callbacks)`; returns the
    /// callback-presence bitmask (one bit per `OutputPluginCallbacks` field,
    /// LSB = `startup_cb`). `ereport`s if `_PG_output_plugin_init` is missing.
    pub fn load_output_plugin(plugin: String) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// Run the actual output-plugin callback while the
    /// `output_plugin_error_callback` errcontext frame is on
    /// `error_context_stack`. Returns the bool the two filter callbacks
    /// produce (ignored by the rest). The plugin callback can `ereport`.
    pub fn invoke_output_plugin_callback(inv: CallbackInvocation) -> types_error::PgResult<bool>
);

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
