//! Seam declarations for the OS dynamic-loader edge (`<dlfcn.h>` /
//! `stat(2)`) that `src/backend/utils/fmgr/dfmgr.c` reaches.
//!
//! On non-Windows these are libc (`dlopen`/`dlsym`/`dlclose`/`stat`) called
//! directly from `dfmgr.c`; there is no separate PostgreSQL `.c` translation
//! unit for them. The platform runtime installs the real implementations; a
//! call panics loudly until then. The opaque `void *` library handle never
//! crosses the idiomatic surface — the runtime maps it to/from an integer
//! [`LibraryHandle`](types_dfmgr::LibraryHandle) token.

use types_dfmgr::{FileIdentity, LibraryHandle, LibraryOpen};
use types_error::PgResult;
use fmgr::LoadedExternalFunc;
use types_logical::CallbackInvocation;

seam_core::seam!(
    /// `stat(libname, &stat_buf)` — the device/inode identity used by
    /// `SAME_INODE` to detect the same file reached by a different path.
    /// A `stat` failure becomes the `errcode_for_file_access()`
    /// "could not access file" error.
    pub fn stat_identity(libname: &str) -> PgResult<FileIdentity>
);

seam_core::seam!(
    /// `dlopen(libname, RTLD_NOW | RTLD_GLOBAL)` followed by
    /// `dlsym(handle, PG_MAGIC_FUNCTION_NAME_STRING)` and, if present,
    /// invoking the magic function to read its `Pg_magic_struct`.
    ///
    /// Bundles the open with the magic-block lookup because both are pure OS
    /// interaction with no in-crate decision between them. Returns
    /// [`LibraryOpen::WithMagic`] when the magic block is found,
    /// [`LibraryOpen::MissingMagic`] (carrying the still-open handle, so the
    /// caller can `close_library` before erroring) when it is not. A `dlopen`
    /// failure becomes the "could not load library" error carrying the
    /// `dlerror` text.
    pub fn open_library(libname: &str) -> PgResult<LibraryOpen>
);

seam_core::seam!(
    /// `dlsym(handle, "_PG_init")` and, if the symbol is present, invoke it
    /// (`(*PG_init)()`). No-op when the library has no `_PG_init`. The init
    /// function may `ereport(ERROR)`, surfaced as `Err`.
    pub fn call_pg_init(handle: LibraryHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `dlclose(handle)` — invoked on the magic-mismatch / missing-magic
    /// failure paths of `internal_load_library` before the error is raised.
    pub fn close_library(handle: LibraryHandle)
);

seam_core::seam!(
    /// `dlsym(handle, funcname) != NULL` — whether `funcname` resolves in the
    /// given already-loaded library. Backs `lookup_external_function` and the
    /// symbol lookup of `load_external_function`. The resolved symbol address
    /// is not returned across the idiomatic surface; a typed caller
    /// re-resolves the symbol through its own subsystem's seam once it knows
    /// the signature.
    pub fn function_exists(handle: LibraryHandle, funcname: &str) -> bool
);

seam_core::seam!(
    /// `fetch_finfo_record(handle, prosrc)` (`fmgr.c`) — resolve `prosrc` in the
    /// already-loaded library `handle` and read its `Pg_finfo_record`,
    /// returning the `(user_fn, api_version)` the function manager caches. The
    /// symbol resolution and info-function call are OS-loader interaction; a
    /// missing symbol or info record `ereport(ERROR)`s, surfaced on `Err`. Used
    /// by `backend-utils-fmgr-dfmgr`'s installer of the `load_external_function`
    /// inward seam to compose dfmgr's library load with the fmgr finfo read.
    pub fn fetch_finfo_record(handle: LibraryHandle, prosrc: &str) -> PgResult<LoadedExternalFunc>
);

seam_core::seam!(
    /// `dlsym(handle, "_PG_output_plugin_init")` + `plugin_init(&callbacks)`
    /// (logical decoding's plugin contract) — invoke the loaded plugin's init
    /// vtable hook and return the callback-presence bitmask (one bit per
    /// `OutputPluginCallbacks` field, LSB = `startup_cb`). `ereport`s if
    /// `_PG_output_plugin_init` is missing. The function-pointer table the
    /// plugin fills lives in the OS-loaded library, so this is loader runtime,
    /// not dfmgr logic.
    pub fn plugin_init(handle: LibraryHandle) -> PgResult<u32>
);

seam_core::seam!(
    /// Invoke a loaded output plugin's callback (the function pointer the plugin
    /// registered in its `OutputPluginCallbacks` vtable) with the ctx output
    /// state the wrapper prepared. Returns the bool the two filter callbacks
    /// produce (ignored otherwise). The callback can `ereport`. Pure
    /// loaded-symbol dispatch — OS-loader runtime, not dfmgr logic.
    pub fn invoke_output_plugin_callback(inv: CallbackInvocation) -> PgResult<bool>
);
