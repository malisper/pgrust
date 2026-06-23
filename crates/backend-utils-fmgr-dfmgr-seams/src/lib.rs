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
    /// If `plugin` names a registered BUILTIN (in-process ported) output plugin,
    /// return its static name (used to key the builtin vtable on the context);
    /// else `None` (a genuine OS-loaded `.so`). Consulted by
    /// `StartupDecodingContext` after `LoadOutputPlugin` so the per-change
    /// dispatch can reach the builtin vtable WITH the live `&mut ctx`.
    pub fn builtin_output_plugin_name(plugin: &str) -> Option<&'static str>
);

seam_core::seam!(
    /// Dispatch one output-plugin callback to the builtin plugin named
    /// `ctx.builtin_plugin`, passing the LIVE `ctx` (the plugin writes into
    /// `ctx->out` and reads/stows `ctx->output_plugin_private`). Returns the bool
    /// the two filter callbacks produce (ignored by the rest); the callback can
    /// `ereport`. The `*_cb_wrapper`s in logical.c call this (instead of the
    /// flattened `invoke_output_plugin_callback`) whenever `ctx.builtin_plugin`
    /// is set — i.e. always, in this build, since every loadable plugin is a
    /// builtin (no C ABI to dlopen a real `.so`).
    pub fn invoke_builtin_output_plugin_callback(
        ctx: &mut types_logical::LogicalDecodingContext,
        inv: &CallbackInvocation,
    ) -> types_error::PgResult<bool>
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

seam_core::seam!(
    /// Resolve a `(library, function)` pair against the in-process registry of
    /// shared libraries whose C bodies have been ported into the Rust backend
    /// (e.g. the `src/test/regress/regress.c` regression-support library, which
    /// cannot be `dlopen`ed because the Rust backend does not expose the C ABI).
    /// `library` is the simple, suffix-free library name (`$libdir/regress` →
    /// `regress`). Returns the loaded `(user_fn, api_version)` pair the function
    /// manager caches, or `None` when no such library/symbol is registered (the
    /// caller then falls through to the real OS dynamic loader). The registered
    /// `_PG_init`-equivalent or the symbol body can `ereport(ERROR)`.
    pub fn resolve_builtin_library_function(
        library: &str,
        function: &str,
    ) -> PgResult<Option<LoadedExternalFunc>>
);

seam_core::seam!(
    /// Whether `library` (the simple, suffix-free library name) names a shared
    /// library whose C body has been ported into the Rust backend and registered
    /// (see [`resolve_builtin_library_function`]). `load_file` consults this so a
    /// bare library load of such a module succeeds without touching the OS loader.
    pub fn builtin_library_present(library: &str) -> bool
);

// ===========================================================================
// Shared in-process ported-library registry.
//
// The mock dynamic-loader replaces `dlopen` for shared libraries whose C bodies
// have been ported into the Rust backend (which exposes no C ABI). More than one
// such module exists (`src/test/regress/regress.c`, `$libdir/plpgsql`, ...), and
// each lives in its own crate, so the two consult-seams above cannot be
// `::set` by any single owner. Instead this crate owns a shared registry that
// every ported module registers into from its own `init_seams()`; the dfmgr
// unit installs the two consult-seams once to read this table. (The seams stay
// declared above so the dfmgr consumer keeps calling them; their installed
// bodies — see `dfmgr::init_seams` — delegate here.)
// ===========================================================================

/// A ported shared library's symbol-resolution entry: its simple (suffix-free,
/// directory-free) name and the lookup that maps a symbol name to its ported
/// `(user_fn, api_version)` pair (the `PG_FUNCTION_INFO_V1`-exposed pair the OS
/// loader would otherwise hand fmgr). `None` from `lookup` means "no such symbol
/// in this library" (the C "could not find function in file" error).
#[derive(Clone)]
pub struct BuiltinLibraryEntry {
    /// The simple library name (`$libdir/regress` → `regress`).
    pub name: &'static str,
    /// Resolve a symbol of this library to its ported function pair.
    pub lookup: fn(&str) -> Option<LoadedExternalFunc>,
    /// The library's `_PG_init`-equivalent, run when the library is loaded
    /// (`internal_load_library` → `call_pg_init` in dfmgr.c). `None` for
    /// libraries with no load-time initialization. Must be idempotent: the
    /// builtin-library `load_file` fast path may invoke it on every `LOAD` of
    /// the module (the OS-loader dedup that gates the C `_PG_init` call does not
    /// apply to in-process modules).
    pub pg_init: Option<fn() -> types_error::PgResult<()>>,
}

static BUILTIN_LIBRARIES: std::sync::Mutex<Vec<BuiltinLibraryEntry>> =
    std::sync::Mutex::new(Vec::new());

/// Register a ported shared library with the in-process loader registry. Called
/// from each ported library crate's `init_seams()` (e.g. `backend-test-regress`,
/// `backend-pl-plpgsql-handler`). Idempotent per name: re-registering the same
/// library name replaces its entry (so a double `init_seams()` is harmless).
pub fn register_builtin_library(entry: BuiltinLibraryEntry) {
    let mut libs = BUILTIN_LIBRARIES.lock().unwrap();
    if let Some(existing) = libs.iter_mut().find(|e| e.name == entry.name) {
        *existing = entry;
    } else {
        libs.push(entry);
    }
}

/// Whether `library` is a registered ported library. Backs the installed body of
/// [`builtin_library_present`].
pub fn registry_library_present(library: &str) -> bool {
    BUILTIN_LIBRARIES
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.name == library)
}

/// Resolve `(library, function)` against the registry. `Ok(None)` when the
/// library is not registered (the dfmgr caller then falls through to the OS
/// loader) or when the registered library has no such symbol (the dfmgr caller
/// turns that into the "could not find function in file" error). Backs the
/// installed body of [`resolve_builtin_library_function`].
pub fn registry_resolve(library: &str, function: &str) -> Option<LoadedExternalFunc> {
    let libs = BUILTIN_LIBRARIES.lock().unwrap();
    let entry = libs.iter().find(|e| e.name == library)?;
    (entry.lookup)(function)
}

/// The `_PG_init`-equivalent registered for `library`, if any. Backs the
/// builtin-library `load_file` path's `call_pg_init` step (the loader runs a
/// loaded module's `_PG_init` — `internal_load_library` in dfmgr.c).
pub fn registry_pg_init(library: &str) -> Option<fn() -> types_error::PgResult<()>> {
    BUILTIN_LIBRARIES
        .lock()
        .unwrap()
        .iter()
        .find(|e| e.name == library)
        .and_then(|e| e.pg_init)
}

// ===========================================================================
// Builtin OUTPUT-PLUGIN registry (Phase 0 of the logical-decoding output
// substrate).
//
// `LoadOutputPlugin(plugin)` (logical.c) `dlopen`s `$libdir/<plugin>` and
// resolves `_PG_output_plugin_init`, which fills an `OutputPluginCallbacks`
// vtable. The Rust backend exposes no C ABI, so the test_decoding output
// plugin's C body is ported in-process (crate `contrib-test-decoding`) and
// registered here, mirroring the ported shared-library registry above. The
// dfmgr `load_output_plugin` installer consults this BEFORE the OS loader; when
// a builtin matches, `install_load_output_plugin` returns the registered
// callback-presence bitmask (the C "the symbol filled the vtable" result).
//
// Crucially, the per-change DISPATCH path also routes through here: the C
// invokes the plugin's function pointers with the LIVE `LogicalDecodingContext`
// (the plugin writes into `ctx->out`, reads `ctx->output_plugin_options`, stows
// `ctx->output_plugin_private`). The flattened `invoke_output_plugin_callback`
// seam can't carry `&mut ctx`, so the wrappers call
// `invoke_builtin_output_plugin_callback` (below) which dispatches to the
// registered vtable WITH the live context.
// ===========================================================================

/// The vtable a builtin output plugin registers — the in-process equivalent of
/// the function pointers `_PG_output_plugin_init` fills in. `init` is the
/// `_PG_output_plugin_init`-equivalent: it returns the callback-presence bitmask
/// (one bit per `OutputPluginCallbacks` field, LSB = `startup_cb`) so
/// `LoadOutputPlugin` can enforce the required-callback rules. `invoke` runs one
/// callback against the live context (the resolved args ride on `inv.args`); it
/// returns the bool the two filter callbacks produce (ignored by the rest).
#[derive(Clone)]
pub struct BuiltinOutputPlugin {
    /// The plugin name (`pg_create_logical_replication_slot`'s `plugin` arg,
    /// `slot->data.plugin`), e.g. `"test_decoding"`.
    pub name: &'static str,
    /// `_PG_output_plugin_init(cb)` → the callback-presence bitmask.
    pub init: fn() -> u32,
    /// Run one output-plugin callback against the live decoding context.
    pub invoke: fn(
        &mut types_logical::LogicalDecodingContext,
        &types_logical::CallbackInvocation,
    ) -> types_error::PgResult<bool>,
}

static BUILTIN_OUTPUT_PLUGINS: std::sync::Mutex<Vec<BuiltinOutputPlugin>> =
    std::sync::Mutex::new(Vec::new());

/// Register a builtin (in-process ported) output plugin. Called from the
/// plugin crate's `init_seams()` (e.g. `contrib-test-decoding`). Idempotent per
/// name: re-registering replaces the entry.
pub fn register_builtin_output_plugin(entry: BuiltinOutputPlugin) {
    let mut plugins = BUILTIN_OUTPUT_PLUGINS.lock().unwrap();
    if let Some(existing) = plugins.iter_mut().find(|e| e.name == entry.name) {
        *existing = entry;
    } else {
        plugins.push(entry);
    }
}

/// Resolve `plugin` to its registered builtin output-plugin vtable, or `None`
/// when no such builtin is registered (the dfmgr caller then falls through to
/// the OS loader). Backs `install_load_output_plugin`'s Phase-0 path.
pub fn resolve_builtin_output_plugin(plugin: &str) -> Option<BuiltinOutputPlugin> {
    BUILTIN_OUTPUT_PLUGINS
        .lock()
        .unwrap()
        .iter()
        .find(|e| e.name == plugin)
        .cloned()
}

/// Dispatch one output-plugin callback to the registered builtin `plugin`,
/// passing the LIVE `ctx`. Returns `Some(result)` when `plugin` is a registered
/// builtin (the result is the callback's bool / propagated `ereport`), or `None`
/// when it is not (the caller routes to the OS-loaded plugin instead). Backs the
/// installed body of [`invoke_builtin_output_plugin_callback`].
pub fn registry_invoke_builtin_output_plugin(
    plugin: &str,
    ctx: &mut types_logical::LogicalDecodingContext,
    inv: &types_logical::CallbackInvocation,
) -> Option<types_error::PgResult<bool>> {
    let invoke = {
        let plugins = BUILTIN_OUTPUT_PLUGINS.lock().unwrap();
        plugins.iter().find(|e| e.name == plugin).map(|e| e.invoke)
    };
    invoke.map(|f| f(ctx, inv))
}
