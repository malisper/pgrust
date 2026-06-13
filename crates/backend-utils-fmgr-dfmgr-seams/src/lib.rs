//! Seam declarations for the output-plugin load + dispatch surface
//! (`utils/fmgr/dfmgr.c` `load_external_function` plus the loaded plugin's
//! `_PG_output_plugin_init` vtable), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_logical::CallbackInvocation;

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
