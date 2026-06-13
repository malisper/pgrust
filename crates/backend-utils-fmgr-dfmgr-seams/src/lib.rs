//! Seam declarations for the dynamic-library loader (`utils/fmgr/dfmgr.c`) and
//! the `shmem_request_hook` (`miscinit.c` owns the hook pointer; the installed
//! hook body belongs to whatever extension/module registered it), plus the
//! output-plugin load + dispatch surface (`load_external_function` plus the
//! loaded plugin's `_PG_output_plugin_init` vtable) consumed by logical
//! decoding. Calls panic until the owners land.

#![allow(non_snake_case)]

use types_logical::CallbackInvocation;

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
