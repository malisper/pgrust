//! OS dynamic-loader runtime — the `<dlfcn.h>` / `stat(2)` edge that
//! `src/backend/utils/fmgr/dfmgr.c` reaches through [`port_dynloader_seams`].
//!
//! There is no PostgreSQL `.c` translation unit for these calls: on a POSIX
//! target they are libc (`dlopen`/`dlsym`/`dlclose`/`stat`) invoked inline from
//! `dfmgr.c` (and `fmgr.c`'s `fetch_finfo_record`). This crate is their owner.
//!
//! ## What is, and is not, expressible in an idiomatic-Rust build
//!
//! `stat(2)` is a pure filesystem query with no C-ABI dependency, so
//! [`stat_identity`] is ported faithfully against libc here — it backs the
//! `SAME_INODE` "same file, different path" (symlink / hard-link) detection of
//! `internal_load_library`.
//!
//! The remaining seams (`open_library`, `call_pg_init`, `close_library`,
//! `function_exists`, `fetch_finfo_record`, `plugin_init`,
//! `invoke_output_plugin_callback`) all `dlopen` a real PostgreSQL extension
//! `.so` and resolve / invoke symbols across the C ABI (`PG_MAGIC_FUNCTION`,
//! `Pg_finfo_record`, `_PG_init`, `_PG_output_plugin_init`, registered
//! `PGFunction` pointers). The Rust backend exposes no C ABI and cannot
//! `dlopen` a `regress.so`-shaped module — extension libraries whose C bodies
//! the suite needs (e.g. `src/test/regress/regress.c`) are instead ported into
//! the in-process ported-library registry that `dfmgr` consults BEFORE reaching
//! this OS edge (see `backend-utils-fmgr-dfmgr`'s
//! `install_load_external_function` / `install_load_file`). For any other
//! library these seams `mirror-pg-and-panic`: they name the genuinely-missing
//! `dlopen` ABI surface loudly rather than silently doing nothing. The unit is
//! therefore intentionally a PARTIAL port (it is not marked complete in
//! `CATALOG.tsv`), exactly mirroring the `TD-DFMGR-DYNLOADER` design debt.

use backend_utils_error::ereport;
use types_dfmgr::{FileIdentity, LibraryHandle, LibraryOpen};
use types_error::{PgResult, ERROR};
use types_fmgr::LoadedExternalFunc;
use types_logical::CallbackInvocation;

/// `stat(libname, &stat_buf)` (`dfmgr.c` `internal_load_library`) — read the
/// file's device/inode so `SAME_INODE` can detect the same file reached by a
/// different path (symlink / hard link). A `stat` failure becomes the C
/// ```c
/// ereport(ERROR,
///         (errcode_for_file_access(),
///          errmsg("could not access file \"%s\": %m", libname)));
/// ```
fn stat_identity(libname: &str) -> PgResult<FileIdentity> {
    // The path is interpolated into a NUL-terminated C string for the syscall;
    // an embedded NUL can never name a real file, so it takes the same
    // "could not access file" error path (ENOENT-shaped) the C `stat` would.
    let cpath = match std::ffi::CString::new(libname) {
        Ok(c) => c,
        Err(_) => {
            return Err(ereport(ERROR)
                .with_saved_errno(libc::ENOENT)
                .errcode_for_file_access()
                .errmsg(format!("could not access file \"{libname}\": %m"))
                .into_error());
        }
    };

    // SAFETY: `stat_buf` is fully written by a successful `stat(2)`; we only
    // read `st_dev`/`st_ino` after checking the return value, exactly as C does.
    let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::stat(cpath.as_ptr(), &mut stat_buf) };
    if rc == -1 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        return Err(ereport(ERROR)
            .with_saved_errno(errno)
            .errcode_for_file_access()
            .errmsg(format!("could not access file \"{libname}\": %m"))
            .into_error());
    }

    Ok(FileIdentity {
        device: stat_buf.st_dev,
        inode: stat_buf.st_ino,
    })
}

/// Diagnostic shared by every dlopen-based seam: there is no C ABI to
/// `dlopen`/`dlsym` across, so resolving a real `.so` is unreachable in this
/// build. Libraries whose bodies are ported into the in-process registry are
/// intercepted by `dfmgr` before reaching here, so a call that arrives means a
/// genuine OS extension `.so` was requested.
fn no_c_abi(what: &str, libname: &str) -> ! {
    panic!(
        "port-dynloader: {what} of OS extension library \"{libname}\" is \
         unreachable in the idiomatic-Rust backend (no C ABI to dlopen/dlsym); \
         libraries whose C bodies are ported are served by dfmgr's in-process \
         ported-library registry before reaching the OS loader \
         (TD-DFMGR-DYNLOADER)"
    );
}

/// Install this unit's inward seams. `stat_identity` is the real libc body;
/// the dlopen-based OS-edge seams `mirror-pg-and-panic` (see module docs).
pub fn init_seams() {
    port_dynloader_seams::stat_identity::set(stat_identity);

    port_dynloader_seams::open_library::set(|libname: &str| -> PgResult<LibraryOpen> {
        no_c_abi("open_library (dlopen)", libname)
    });
    port_dynloader_seams::call_pg_init::set(|_handle: LibraryHandle| -> PgResult<()> {
        no_c_abi("call_pg_init (_PG_init)", "<loaded handle>")
    });
    port_dynloader_seams::close_library::set(|_handle: LibraryHandle| {
        no_c_abi("close_library (dlclose)", "<loaded handle>")
    });
    port_dynloader_seams::function_exists::set(
        |_handle: LibraryHandle, funcname: &str| -> bool {
            no_c_abi("function_exists (dlsym)", funcname)
        },
    );
    port_dynloader_seams::fetch_finfo_record::set(
        |_handle: LibraryHandle, prosrc: &str| -> PgResult<LoadedExternalFunc> {
            no_c_abi("fetch_finfo_record (Pg_finfo_record)", prosrc)
        },
    );
    port_dynloader_seams::plugin_init::set(|_handle: LibraryHandle| -> PgResult<u32> {
        no_c_abi("plugin_init (_PG_output_plugin_init)", "<loaded handle>")
    });
    port_dynloader_seams::invoke_output_plugin_callback::set(
        |_inv: CallbackInvocation| -> PgResult<bool> {
            no_c_abi("invoke_output_plugin_callback", "<loaded handle>")
        },
    );
}
