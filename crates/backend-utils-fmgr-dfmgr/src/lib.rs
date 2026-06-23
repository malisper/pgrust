//! Dynamic function manager (`src/backend/utils/fmgr/dfmgr.c`).
//!
//! Loads dynamic-link libraries, validates their `Pg_magic_struct` ABI block,
//! de-duplicates by inode (`SAME_INODE`), expands `$libdir`/search-path
//! library names, maintains the process-lifetime rendezvous-variable table,
//! and (de)serializes the loaded-library list for parallel workers.
//!
//! Process-global state `dfmgr.c` keeps in file-scope statics — the loaded-
//! files list (`file_list`/`file_tail`) and the rendezvous-variable hash table
//! — is per-backend, so it lives in `thread_local!`s here (AGENTS.md backend-
//! global rule). The C list is `malloc`'d for the life of the process, so the
//! Rust analog stores owned `String` filenames that survive memory-context
//! resets, not context-allocated memory. `Dynamic_library_path` is this unit's
//! own GUC string variable; it too is a backend-local `thread_local!`.
//!
//! Genuinely-external edges go through their owner's seam crate: the OS
//! dynamic loader (`port-dynloader-seams`: `stat`/`dlopen`/`dlsym`/`dlclose`),
//! `common/path.c` (`common-path-seams`: `canonicalize_path`/
//! `is_absolute_path`), and `storage/file/fd.c`
//! (`backend-storage-file-fd-seams`: `pg_file_exists`). `pkglib_path` is read
//! from its owner `backend-utils-init-small` (globals.c) directly. The opaque
//! OS library handle is an integer [`LibraryHandle`] token, never a pointer.

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, PgString};
use types_core::fmgr::{PgAbiValues, FMGR_ABI_EXTRA};
use types_dfmgr::{FileIdentity, LibraryHandle, LibraryOpen, LoadedModule, LoadedModuleDetails};
use types_dfmgr::Pg_magic_struct;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_UNDEFINED_FUNCTION,
};

use port_dynloader_seams as loader;

/// Platform shared-library suffix (`DLSUFFIX`): the C build sets this per
/// platform — `.dylib` on macOS/Darwin, `.so` elsewhere (Makefile.global's
/// `DLSUFFIX`). Used only on the real OS-loader path
/// ([`expand_dynamic_library_name`]), where the appended suffix must match the
/// platform's actual library files. The in-process ported-library registry does
/// NOT use it: a registry key is a bare library name, so a suffixed probin is
/// reduced to its key by stripping any known suffix uniformly (see
/// [`KNOWN_DLSUFFIXES`] / [`simple_library_name`]).
#[cfg(target_os = "macos")]
const DLSUFFIX: &str = ".dylib";
#[cfg(not(target_os = "macos"))]
const DLSUFFIX: &str = ".so";

/// Shared-library suffixes that a `probin` may carry on any platform. The
/// in-process ported-library registry treats a `probin` purely as a key, so a
/// fully-qualified `.../regress.dylib` (macOS), `.../regress.so` (ELF) or
/// `.../regress.dll` (Windows) all reduce to the bare `regress` key regardless
/// of the platform this backend is built for. This deliberately does NOT depend
/// on the build platform's `DLSUFFIX`: the file is never opened, the name is
/// just looked up, so there is no platform divergence to honor here.
const KNOWN_DLSUFFIXES: [&str; 3] = [".so", ".dylib", ".dll"];

/* =========================================================================
 * Process(backend)-global loader state — dfmgr.c's file-scope statics.
 * ========================================================================= */

thread_local! {
    /// `file_list` / `file_tail` — the ordered list of loaded libraries, in
    /// load order (`get_first_loaded_module` / `get_next_loaded_module` walk
    /// the C singly-linked list in this order). The C list is `malloc`'d for
    /// the life of the process, so the filenames are owned `String`s, not
    /// context memory.
    static FILE_LIST: RefCell<Vec<LoadedModule>> = const { RefCell::new(Vec::new()) };

    /// Names of in-process ported ("builtin") libraries whose `_PG_init`-equivalent
    /// has already run this backend. The OS-loader `FILE_LIST` dedup gates
    /// `call_pg_init` for real `.so`s; builtin modules take the `install_load_file`
    /// fast path before that dedup, so their one-time `_PG_init` is tracked here.
    static BUILTIN_INITED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };

    /// `rendezvousHash` — `varName` → shared value token (`0` == NULL). C's
    /// value is a `void *`; idiomatically it is an integer token two
    /// cooperating libraries agree on. Process-lifetime: entries are never
    /// removed once created.
    static RENDEZVOUS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());

    /// `char *Dynamic_library_path;` (`dfmgr.c`) — the `dynamic_library_path`
    /// GUC string, owned by this unit. Default `"$libdir"` matches the GUC's
    /// boot value; the GUC assign hook overwrites it via
    /// [`set_dynamic_library_path`].
    static DYNAMIC_LIBRARY_PATH: RefCell<String> = RefCell::new(String::from("$libdir"));
}

/// Read `Dynamic_library_path` (`dynamic_library_path` GUC).
pub fn dynamic_library_path() -> String {
    DYNAMIC_LIBRARY_PATH.with(|p| p.borrow().clone())
}

/// Set `Dynamic_library_path` (the GUC assign hook).
pub fn set_dynamic_library_path(value: &str) {
    DYNAMIC_LIBRARY_PATH.with(|p| *p.borrow_mut() = value.to_owned());
}

/// `pkglib_path` (`globals.c`) — full path to the library directory; the
/// `$libdir` macro expands to it. Read from its owner `backend-utils-init-
/// small` and decoded from the C `char[MAXPGPATH]` NUL-terminated form.
fn pkglib_path() -> String {
    let bytes = backend_utils_init_small::globals::pkglib_path();
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).into_owned()
}

/* =========================================================================
 * load_external_function / load_file / lookup_external_function.
 * ========================================================================= */

/// `load_external_function(filename, funcname, signalNotFound, filehandle)`.
///
/// Loads the (possibly already loaded) library and reports whether `funcname`
/// resolves in it. On success the resolving library's [`LibraryHandle`] is
/// returned (the C `*filehandle` out-parameter). If the function is not found
/// and `signal_not_found` is true, raises `ERRCODE_UNDEFINED_FUNCTION`; errors
/// loading the library are raised regardless of `signal_not_found`.
pub fn load_external_function(
    mcx: Mcx<'_>,
    filename: &str,
    funcname: &str,
    signal_not_found: bool,
) -> PgResult<LibraryHandle> {
    // For extensions with hardcoded '$libdir/' library names, strip the prefix
    // to allow the library search path to be used. Done only for simple names
    // (e.g. "$libdir/foo"), not nested paths ("$libdir/foo/bar"); nested paths
    // are left untouched (expand_dynamic_library_name expands the '$libdir'
    // macro for them directly).
    let filename = strip_simple_libdir_prefix(filename);

    // Expand the possibly-abbreviated filename to an exact path name.
    let fullname = expand_dynamic_library_name(mcx, filename)?;

    // Load the shared library, unless we already did.
    let lib_handle = internal_load_library(&fullname)?;

    // Look up the function within the library.
    let retval = loader::function_exists::call(lib_handle, funcname);

    if !retval && signal_not_found {
        return Err(PgError::error(format!(
            "could not find function \"{funcname}\" in file \"{fullname}\""
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    Ok(lib_handle)
}

/// `load_file(filename, restricted)` — load a shared library without looking
/// up any particular function. When `restricted`, only libraries in the
/// presumed-secure directory `$libdir/plugins` may be referenced.
pub fn load_file(mcx: Mcx<'_>, filename: &str, restricted: bool) -> PgResult<()> {
    // Apply security restriction if requested.
    if restricted {
        check_restricted_library_name(filename)?;
    }

    // Expand the possibly-abbreviated filename to an exact path name.
    let fullname = expand_dynamic_library_name(mcx, filename)?;

    // Load the shared library, unless we already did.
    internal_load_library(&fullname)?;
    Ok(())
}

/// `lookup_external_function(filehandle, funcname)` — whether `funcname`
/// resolves in an already-loaded library. (`dlsym` cannot fail-with-error
/// here, matching the C signature that returns a bare `void *`.)
pub fn lookup_external_function(filehandle: LibraryHandle, funcname: &str) -> bool {
    loader::function_exists::call(filehandle, funcname)
}

/// `internal_load_library(libname)` — load the named library file unless
/// already loaded, returning its handle. `libname` is expected to be an exact
/// path name for the library file.
///
/// NB: there is presently no way to unload a dynamically loaded file.
fn internal_load_library(libname: &str) -> PgResult<LibraryHandle> {
    // Scan the list of loaded FILES to see if the file has been loaded.
    if let Some(handle) = FILE_LIST.with(|list| {
        list.borrow()
            .iter()
            .find(|m| m.filename == libname)
            .map(|m| m.handle)
    }) {
        return Ok(handle);
    }

    // Check for same files - different paths (i.e. symlink or hard link).
    let stat_buf = loader::stat_identity::call(libname)?;
    if let Some(handle) = FILE_LIST.with(|list| {
        list.borrow()
            .iter()
            .find(|m| same_inode(&stat_buf, m))
            .map(|m| m.handle)
    }) {
        return Ok(handle);
    }

    // File not loaded yet: open it (dlopen + magic-function lookup).
    let (handle, magic) = match loader::open_library::call(libname)? {
        LibraryOpen::WithMagic { handle, magic } => {
            // Check ABI compatibility fields. The C compares `len` against
            // sizeof(Pg_magic_struct) and the abi_fields with memcmp.
            if magic.len as usize != magic_struct_len()
                || magic.abi_fields != PgAbiValues::server()
            {
                // Copy the data block before unlinking the library (already
                // owned here), close the library, then issue the complaint.
                loader::close_library::call(handle);
                return Err(incompatible_module_error(libname, &magic.abi_fields));
            }
            (handle, magic)
        }
        LibraryOpen::MissingMagic { handle } => {
            // Try to close the library, then complain.
            loader::close_library::call(handle);
            return Err(PgError::error(format!(
                "incompatible library \"{libname}\": missing magic block"
            ))
            .with_hint(
                "Extension libraries are required to use the PG_MODULE_MAGIC macro.",
            ));
        }
    };

    // If the library has a _PG_init() function, call it.
    loader::call_pg_init::call(handle)?;

    // OK to link it into the list. The C `malloc` of the list entry is the
    // OOM `ereport(ERROR)` surface; make the spine growth fallible to match.
    let module = LoadedModule {
        identity: stat_buf,
        handle,
        magic,
        filename: libname.to_owned(),
    };
    FILE_LIST.with(|list| {
        let mut list = list.borrow_mut();
        list.try_reserve(1)
            .map_err(|_| PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY))?;
        list.push(module);
        Ok::<(), PgError>(())
    })?;

    Ok(handle)
}

/* =========================================================================
 * Magic-block compatibility error reporting.
 * ========================================================================= */

/// The serialized size a real `Pg_magic_struct` reports in its `len` field
/// (`sizeof(Pg_magic_struct)` on the 64-bit build platform).
///
/// The idiomatic owned struct (with `Option<String>` fields) has a different
/// in-memory size, so the ABI-defined on-disk size is computed explicitly to
/// match the value a real extension module bakes into its magic block:
///
/// * `int len`                  — 4 bytes at offset 0.
/// * `Pg_abi_values abi_fields` — 5*4 + 32 = 52 bytes at offset 4 (no padding
///   after `len`: the first member of `Pg_abi_values` is a 4-byte `int`).
/// * `const char *name`         — 8 bytes at offset 56.
/// * `const char *version`      — 8 bytes at offset 64.
///
/// Total = 72; no tail padding (the trailing pointer leaves 8-byte alignment).
const fn magic_struct_len() -> usize {
    let len_field = 4; // int len
    let abi_fields = 5 * 4 + 32; // 5 * int32 + char abi_extra[32]
    let name_ptr = 8; // const char *name
    let version_ptr = 8; // const char *version
    len_field + abi_fields + name_ptr + version_ptr
}

/// `incompatible_module_error(libname, module_magic_data)` — build the precise
/// "incompatible library" error for a magic block that failed the ABI check.
///
/// In C this is `pg_noreturn` and `ereport(ERROR)`s directly; here it returns
/// the [`PgError`] for the caller to raise via `?` / `return Err`.
///
/// XXX this code has to be adjusted any time the set of fields in a magic
/// block change!
pub fn incompatible_module_error(libname: &str, module_magic_data: &PgAbiValues) -> PgError {
    let magic_data = PgAbiValues::server();

    // If the version doesn't match, just report that, because the rest of the
    // block might not even have the fields we expect.
    if magic_data.version != module_magic_data.version {
        let library_version = if module_magic_data.version >= 1000 {
            format!("{}", module_magic_data.version / 100)
        } else {
            format!(
                "{}.{}",
                module_magic_data.version / 100,
                module_magic_data.version % 100
            )
        };
        return PgError::error(format!(
            "incompatible library \"{libname}\": version mismatch"
        ))
        .with_detail(format!(
            "Server is version {}, library is version {}.",
            magic_data.version / 100,
            library_version
        ));
    }

    // Similarly, if the ABI extra field doesn't match, error out. Other fields
    // below might also mismatch, but that isn't useful information if you're
    // using the wrong product altogether.
    //
    // C compares with `strcmp(module_magic_data->abi_extra, magic_data.abi_extra)`,
    // i.e. C-string semantics that stop at the first NUL — bytes after the
    // terminator do not participate. Mirror that exactly by comparing the
    // NUL-terminated prefixes rather than the full 32-byte arrays.
    if abi_extra_cstr(&module_magic_data.abi_extra) != abi_extra_cstr(&magic_data.abi_extra) {
        return PgError::error(format!(
            "incompatible library \"{libname}\": ABI mismatch"
        ))
        .with_detail(format!(
            "Server has ABI \"{}\", library has \"{}\".",
            abi_extra_string(&magic_data.abi_extra),
            abi_extra_string(&module_magic_data.abi_extra)
        ));
    }

    // Otherwise, spell out which fields don't agree. C builds the detail in a
    // StringInfo (palloc); here the per-field lines are assembled in a
    // transient memory context (an error-path diagnostic — best-effort: on
    // OOM the detail is dropped and the bare message is reported).
    let detail = build_field_mismatch_detail(&magic_data, module_magic_data).unwrap_or_default();

    let error = PgError::error(format!(
        "incompatible library \"{libname}\": magic block mismatch"
    ));
    if detail.is_empty() {
        error
    } else {
        error.with_detail(detail)
    }
}

/// Build the per-field "Server has X = .., library has .." detail string in a
/// transient memory context, mirroring the C `ereport` detail assembly. The
/// lines are joined with `'\n'`. Returns `Err` on OOM (the diagnostic is
/// best-effort; the caller falls back to no detail).
fn build_field_mismatch_detail(
    magic_data: &PgAbiValues,
    module_magic_data: &PgAbiValues,
) -> PgResult<String> {
    let ctx = mcx::MemoryContext::new("incompatible_module_error");
    let mcx = ctx.mcx();

    let mut details: PgString = PgString::new_in(mcx);
    let mut any = false;

    let append = |details: &mut PgString, line: &str| -> PgResult<()> {
        if !details.is_empty() {
            details.try_push('\n')?;
        }
        details.try_push_str(line)
    };

    if module_magic_data.funcmaxargs != magic_data.funcmaxargs {
        append(
            &mut details,
            &format!(
                "Server has {} = {}, library has {}.",
                "FUNC_MAX_ARGS", magic_data.funcmaxargs, module_magic_data.funcmaxargs
            ),
        )?;
        any = true;
    }
    if module_magic_data.indexmaxkeys != magic_data.indexmaxkeys {
        append(
            &mut details,
            &format!(
                "Server has {} = {}, library has {}.",
                "INDEX_MAX_KEYS", magic_data.indexmaxkeys, module_magic_data.indexmaxkeys
            ),
        )?;
        any = true;
    }
    if module_magic_data.namedatalen != magic_data.namedatalen {
        append(
            &mut details,
            &format!(
                "Server has {} = {}, library has {}.",
                "NAMEDATALEN", magic_data.namedatalen, module_magic_data.namedatalen
            ),
        )?;
        any = true;
    }
    if module_magic_data.float8byval != magic_data.float8byval {
        append(
            &mut details,
            &format!(
                "Server has {} = {}, library has {}.",
                "FLOAT8PASSBYVAL",
                bool_text(magic_data.float8byval),
                bool_text(module_magic_data.float8byval)
            ),
        )?;
        any = true;
    }

    if !any {
        append(
            &mut details,
            "Magic block has unexpected length or padding difference.",
        )?;
    }

    Ok(details.as_str().to_owned())
}

/// `check_module_magic` — validate a magic block's `len`/`abi_fields`,
/// returning `Ok` if compatible or the error `internal_load_library` would
/// raise. (The C code inlines this; exposing it keeps the comparison
/// testable.)
pub fn check_module_magic(libname: &str, module_magic_data: &Pg_magic_struct) -> PgResult<()> {
    if module_magic_data.len as usize != magic_struct_len()
        || module_magic_data.abi_fields != PgAbiValues::server()
    {
        return Err(incompatible_module_error(
            libname,
            &module_magic_data.abi_fields,
        ));
    }
    Ok(())
}

/* =========================================================================
 * Loaded-module iteration and details.
 * ========================================================================= */

/// `get_first_loaded_module()` — the index of the first loaded module, if any.
/// (The C iterator returns a `DynamicFileList *`; here it is the list index.)
pub fn get_first_loaded_module() -> Option<usize> {
    FILE_LIST.with(|list| {
        if list.borrow().is_empty() {
            None
        } else {
            Some(0)
        }
    })
}

/// `get_next_loaded_module(dfptr)` — the index of the module after `index` in
/// load order (`dfptr->next`).
pub fn get_next_loaded_module(index: usize) -> Option<usize> {
    FILE_LIST.with(|list| {
        let next = index + 1;
        if next < list.borrow().len() {
            Some(next)
        } else {
            None
        }
    })
}

/// `get_loaded_module_details(dfptr, ...)` — `library_path`, `module_name`,
/// `module_version` for the module at `index`.
pub fn get_loaded_module_details(index: usize) -> Option<LoadedModuleDetails> {
    FILE_LIST.with(|list| {
        list.borrow().get(index).map(|m| LoadedModuleDetails {
            library_path: m.filename.clone(),
            module_name: m.magic.name.clone(),
            module_version: m.magic.version.clone(),
        })
    })
}

/* =========================================================================
 * Rendezvous variables.
 * ========================================================================= */

/// `find_rendezvous_variable(varName)` — read the current value (`*rv`) of the
/// process-lifetime rendezvous variable named `varName`, creating it (as
/// `0` == NULL) on first use.
pub fn find_rendezvous_variable(var_name: &str) -> u64 {
    RENDEZVOUS.with(|h| *h.borrow_mut().entry(var_name.to_owned()).or_insert(0))
}

/// Set the value of the rendezvous variable named `var_name`, creating it on
/// first use. Idiomatic counterpart of writing through the `void **` that
/// `find_rendezvous_variable` returns in C.
pub fn set_rendezvous_variable(var_name: &str, value: u64) {
    RENDEZVOUS.with(|h| {
        h.borrow_mut().insert(var_name.to_owned(), value);
    });
}

/* =========================================================================
 * Library-state (de)serialization for parallel workers.
 * ========================================================================= */

/// `EstimateLibraryStateSpace()` — bytes needed to serialize the loaded-files
/// list (`Size size = 1`, plus `strlen(filename) + 1` per loaded file).
pub fn estimate_library_state_space() -> usize {
    FILE_LIST.with(|list| {
        let mut size: usize = 1;
        for m in list.borrow().iter() {
            size = size.saturating_add(m.filename.len() + 1);
        }
        size
    })
}

/// `SerializeLibraryState(maxsize, start_address)` — write each filename as a
/// NUL-terminated string, then a final NUL byte, mirroring the C `strlcpy`
/// loop and its `Assert(len < maxsize)`.
pub fn serialize_library_state(maxsize: usize, start_address: &mut [u8]) -> PgResult<()> {
    let required = estimate_library_state_space();
    if maxsize < required || start_address.len() < required {
        return Err(PgError::error("library state buffer is too small"));
    }
    FILE_LIST.with(|list| {
        let mut offset = 0;
        for m in list.borrow().iter() {
            let bytes = m.filename.as_bytes();
            start_address[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
            start_address[offset] = 0;
            offset += 1;
        }
        start_address[offset] = 0;
    });
    Ok(())
}

/// `RestoreLibraryState(start_address)` — load every library named in the
/// NUL-separated, double-NUL-terminated blob the serializing backend wrote.
pub fn restore_library_state(start_address: &[u8]) -> PgResult<()> {
    let mut pos = 0;
    while pos < start_address.len() && start_address[pos] != 0 {
        let end = start_address[pos..]
            .iter()
            .position(|&b| b == 0)
            .map(|i| pos + i)
            .ok_or_else(|| PgError::error("unterminated library state entry"))?;
        let name = std::str::from_utf8(&start_address[pos..end])
            .map_err(|_| PgError::error("library state entry is not valid UTF-8"))?
            .to_owned();
        internal_load_library(&name)?;
        pos = end + 1;
    }
    Ok(())
}

/* =========================================================================
 * Path-name expansion and search-path resolution.
 * ========================================================================= */

/// `expand_dynamic_library_name(name)`.
///
/// If `name` contains a slash, check whether the `$libdir`-expanded file
/// exists and return it; else search `Dynamic_library_path`. On failure,
/// append `DLSUFFIX` and try again. If all fails, return the original name
/// as-is (the ensuing load attempt will fail with a suitable message). The
/// result is always a fresh, context-allocated string.
fn expand_dynamic_library_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgString<'mcx>> {
    debug_assert!(!name.is_empty());

    let have_slash = first_dir_separator(name).is_some();
    let pkglib = pkglib_path();
    let dyn_path = dynamic_library_path();

    if !have_slash {
        if let Some(full) =
            find_in_path(mcx, name, &dyn_path, "dynamic_library_path", "$libdir", &pkglib)?
        {
            return Ok(full);
        }
    } else {
        let full = substitute_path_macro(mcx, name, "$libdir", &pkglib)?;
        if loader_pg_file_exists(full.as_str())? {
            return Ok(full);
        }
    }

    let new = format!("{name}{DLSUFFIX}");

    if !have_slash {
        if let Some(full) =
            find_in_path(mcx, &new, &dyn_path, "dynamic_library_path", "$libdir", &pkglib)?
        {
            return Ok(full);
        }
    } else {
        let full = substitute_path_macro(mcx, &new, "$libdir", &pkglib)?;
        if loader_pg_file_exists(full.as_str())? {
            return Ok(full);
        }
    }

    // If we can't find the file, just return the string as-is. The ensuing
    // load attempt will fail and report a suitable message.
    PgString::from_str_in(name, mcx)
}

/// `check_restricted_library_name(name)` — must begin with `"$libdir/plugins/"`
/// and have no directory separator after that (sufficient to prevent ".."
/// style attacks).
pub fn check_restricted_library_name(name: &str) -> PgResult<()> {
    const PREFIX: &str = "$libdir/plugins/";
    let bad = match name.strip_prefix(PREFIX) {
        Some(rest) => first_dir_separator(rest).is_some(),
        None => true,
    };
    if bad {
        return Err(
            PgError::error(format!("access to library \"{name}\" is not allowed"))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        );
    }
    Ok(())
}

/// `substitute_path_macro(str, macro, value)` — substitute a `$macro`
/// appearing at the very start of `str` (the only place macros are
/// recognized). The result is always a fresh, context-allocated string.
pub fn substitute_path_macro<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    macro_: &str,
    value: &str,
) -> PgResult<PgString<'mcx>> {
    debug_assert!(macro_.as_bytes().first() == Some(&b'$'));

    // Currently, we only recognize $macro at the start of the string.
    if s.as_bytes().first() != Some(&b'$') {
        return PgString::from_str_in(s, mcx);
    }

    let sep = first_dir_separator(s).unwrap_or(s.len());

    if macro_.len() != sep || &s[..sep] != macro_ {
        return Err(
            PgError::error(format!("invalid macro name in path: {s}"))
                .with_sqlstate(ERRCODE_INVALID_NAME),
        );
    }

    PgString::from_str_in(&format!("{value}{}", &s[sep..]), mcx)
}

/// `find_in_path(basename, path, path_param, macro, macro_val)`.
///
/// Search for `basename` in the colon-separated `path`, applying the
/// `$macro` → `macro_val` substitution and `canonicalize_path` to each
/// component and requiring each to be absolute. Returns the full path of the
/// first existing match, or `None`.
pub fn find_in_path<'mcx>(
    mcx: Mcx<'mcx>,
    basename: &str,
    path: &str,
    path_param: &str,
    macro_: &str,
    macro_val: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    debug_assert!(first_dir_separator(basename).is_none());

    // If the path variable is empty, don't do a path search.
    if path.is_empty() {
        return Ok(None);
    }

    // Iterate the path's ':'-separated components, mirroring the C loop's index
    // arithmetic. `start` is the C `p` offset.
    let mut start = 0;
    loop {
        // `piece = first_path_var_separator(p)`. A separator at the very start
        // of the current component (`piece == p`) is a zero-length component.
        // A *trailing* ':' does not trigger this: after it, the offset advances
        // to the terminating end where the next scan finds no separator
        // (`piece == NULL`); that empty component is processed below and
        // rejected by the absolute-path check, exactly as C does.
        let sep = first_path_var_separator(&path[start..]);
        if sep == Some(0) {
            return Err(PgError::error(format!(
                "zero-length component in parameter \"{path_param}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_NAME));
        }

        // `len = piece ? piece - p : strlen(p)`.
        let len = sep.unwrap_or(path.len() - start);
        let end = start + len;

        let piece = &path[start..end];
        let mangled = substitute_path_macro(mcx, piece, macro_, macro_val)?;
        let mangled = common_path_seams::canonicalize_path::call(mangled.as_str().to_owned());

        // Only absolute paths.
        if !common_path_seams::is_absolute_path::call(&mangled) {
            return Err(PgError::error(format!(
                "component in parameter \"{path_param}\" is not an absolute path"
            ))
            .with_sqlstate(ERRCODE_INVALID_NAME));
        }

        let full = PgString::from_str_in(&format!("{mangled}/{basename}"), mcx)?;

        if loader_pg_file_exists(full.as_str())? {
            return Ok(Some(full));
        }

        // `if (p[len] == '\0') break; else p += len + 1;`
        if end >= path.len() {
            break;
        }
        start = end + 1;
    }

    Ok(None)
}

/// `pg_file_exists(name)` (`storage/file/fd.c`) through its owner's seam.
fn loader_pg_file_exists(name: &str) -> PgResult<bool> {
    backend_storage_file_fd_seams::pg_file_exists::call(name)
}

/* =========================================================================
 * Pure helpers ported from common/path.c (dependency-free byte scans).
 * ========================================================================= */

/// `strncmp(filename, "$libdir/", 8) == 0` + simple-name check from
/// `load_external_function`: strip a leading `"$libdir/"` only when the
/// remainder is a simple name (no further directory separator).
fn strip_simple_libdir_prefix(filename: &str) -> &str {
    const PREFIX: &str = "$libdir/";
    if let Some(rest) = filename.strip_prefix(PREFIX) {
        if first_dir_separator(rest).is_none() {
            return rest;
        }
    }
    filename
}

/// `first_dir_separator(filename)` (`common/path.c`, non-Windows): the byte
/// offset of the first `'/'`, or `None`.
fn first_dir_separator(filename: &str) -> Option<usize> {
    filename.bytes().position(|b| b == b'/')
}

/// `first_path_var_separator(pathlist)` (`common/path.c`, non-Windows): the
/// byte offset of the first `':'`, or `None`.
fn first_path_var_separator(pathlist: &str) -> Option<usize> {
    pathlist.bytes().position(|b| b == b':')
}

/// `SAME_INODE(stat_buf, *file_scanner)` (non-Windows): same inode and device.
fn same_inode(identity: &FileIdentity, module: &LoadedModule) -> bool {
    identity.inode == module.identity.inode && identity.device == module.identity.device
}

/// The NUL-terminated prefix of an `abi_extra` byte array, modeling C-string
/// (`strcmp`) semantics: everything up to but not including the first NUL.
/// `incompatible_module_error` compares two of these to mirror the C
/// `strcmp(module_magic_data->abi_extra, magic_data.abi_extra)`, which ignores
/// any bytes following the terminator.
fn abi_extra_cstr(bytes: &[u8; 32]) -> &[u8] {
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    &bytes[..nul]
}

/// Render the NUL-terminated `abi_extra` byte array as a string for error
/// messages (`strcmp`/`%s` of `abi_extra` in `incompatible_module_error`).
fn abi_extra_string(bytes: &[u8; 32]) -> String {
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..nul]).into_owned()
}

/// `value ? "true" : "false"` for the `FLOAT8PASSBYVAL` detail line.
fn bool_text(value: i32) -> &'static str {
    if value != 0 {
        "true"
    } else {
        "false"
    }
}

/// Sanity reference: `FMGR_ABI_EXTRA` renders as "PostgreSQL".
#[doc(hidden)]
pub fn fmgr_abi_extra() -> String {
    abi_extra_string(&FMGR_ABI_EXTRA)
}

/* =========================================================================
 * Inward seam installers — `backend-utils-fmgr-dfmgr-seams`.
 *
 * This unit owns `crates/backend-utils-fmgr-dfmgr-seams` (X = the dfmgr unit,
 * maps to dfmgr.c). Its three declarations compose this crate's own
 * `load_external_function` with the OS-loader runtime for the parts that live
 * in a dlopen'd library (fmgr.c's `fetch_finfo_record`, a plugin's
 * `_PG_output_plugin_init` vtable, and a plugin's registered callback function
 * pointers). Each installer is a thin marshal+delegate: it runs dfmgr's own
 * load and hands the loaded-symbol work to `port-dynloader-seams`.
 * ========================================================================= */

/// The simple, suffix-free library name a `probin`/`filename` reduces to for the
/// in-process ported-library registry (see
/// [`backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function`]). Strips
/// any directory prefix (`$libdir/regress` → `regress`, `/path/to/regress.so` →
/// `regress`) and any known shared-library suffix (`.so`/`.dylib`/`.dll`),
/// platform-independently — the registry key is just a name, never a file, so
/// `regress`, `regress.so` and `regress.dylib` are the same key on every
/// platform. Returns `None` for a name that still has interior structure that
/// the registry never carries (it only registers bare library names).
fn simple_library_name(name: &str) -> Option<&str> {
    // Drop everything up to and including the last directory separator.
    let base = match first_dir_separator(name) {
        Some(_) => name.rsplit('/').next().unwrap_or(name),
        None => name,
    };
    // Drop a trailing known shared-library suffix, whatever the platform: the
    // name is a registry key, not a file to open, so no platform divergence.
    let base = KNOWN_DLSUFFIXES
        .iter()
        .find_map(|sfx| base.strip_suffix(sfx))
        .unwrap_or(base);
    if base.is_empty() {
        None
    } else {
        Some(base)
    }
}

/// Installer for `backend_utils_fmgr_dfmgr_seams::load_external_function`:
/// `load_external_function(probin, prosrc, true, &handle)` (dfmgr.c) then
/// `fetch_finfo_record(handle, prosrc)` (fmgr.c, via the loader runtime).
///
/// Before touching the OS dynamic loader, the in-process registry of shared
/// libraries whose C bodies are ported into the Rust backend is consulted: such
/// modules (e.g. `src/test/regress/regress.c`) cannot be `dlopen`ed because the
/// Rust backend exposes no C ABI, so their functions resolve directly to the
/// ported `PGFunction`. A registered library whose requested symbol is absent
/// is treated like the C "function not found in file" error (it would never
/// have a real `.so` to fall back to).
fn install_load_external_function(
    probin: &str,
    prosrc: &str,
    _function_id: types_core::Oid,
) -> PgResult<types_fmgr::LoadedExternalFunc> {
    if let Some(library) = simple_library_name(probin) {
        if backend_utils_fmgr_dfmgr_seams::builtin_library_present::call(library) {
            return match backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function::call(
                library, prosrc,
            )? {
                Some(loaded) => Ok(loaded),
                None => Err(PgError::error(format!(
                    "could not find function \"{prosrc}\" in file \"{probin}\""
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)),
            };
        }
    }

    let ctx = mcx::MemoryContext::new("load_external_function");
    // C: user_fn = load_external_function(filename, funcname, true, &libraryhandle);
    let handle = load_external_function(ctx.mcx(), probin, prosrc, true)?;
    // C: inforec = fetch_finfo_record(libraryhandle, prosrc);
    loader::fetch_finfo_record::call(handle, prosrc)
}

/// Installer for `backend_utils_fmgr_dfmgr_seams::load_output_plugin`:
/// `load_file`-shaped library load of the plugin, then the plugin's
/// `_PG_output_plugin_init` vtable hook (loader runtime), returning the
/// callback-presence bitmask.
///
/// Phase 0: the in-process registry of ported BUILTIN output plugins is
/// consulted BEFORE the OS loader. The Rust backend exposes no C ABI, so an
/// output plugin whose C body is ported in-process (e.g. `test_decoding`) cannot
/// be `dlopen`ed; instead it registers a `BuiltinOutputPlugin` vtable here. A
/// builtin match runs its `_PG_output_plugin_init`-equivalent and returns the
/// callback-presence bitmask, exactly as the OS path would after filling the
/// real `OutputPluginCallbacks` vtable.
fn install_load_output_plugin(plugin: String) -> PgResult<u32> {
    if let Some(builtin) =
        backend_utils_fmgr_dfmgr_seams::resolve_builtin_output_plugin(&plugin)
    {
        return Ok((builtin.init)());
    }
    let ctx = mcx::MemoryContext::new("load_output_plugin");
    // C (LoadOutputPlugin): load_external_function(plugin, "_PG_output_plugin_init",
    //    false, NULL) loads the library; the symbol+vtable init is loader runtime.
    let handle = load_external_function(ctx.mcx(), &plugin, "_PG_output_plugin_init", false)?;
    loader::plugin_init::call(handle)
}

/// Installer for `backend_utils_fmgr_dfmgr_seams::invoke_output_plugin_callback`:
/// pure loaded-symbol dispatch, delegated wholly to the loader runtime.
fn install_invoke_output_plugin_callback(
    inv: types_logical::CallbackInvocation,
) -> PgResult<bool> {
    loader::invoke_output_plugin_callback::call(inv)
}

/// Installer for `backend_utils_fmgr_dfmgr_seams::invoke_builtin_output_plugin_callback`:
/// dispatch one callback to the builtin output plugin keyed by `ctx.builtin_plugin`,
/// passing the LIVE `ctx`. Falls back to the flattened OS-loader seam if the
/// context is somehow not flagged as a builtin (unreachable in this build —
/// there is no C ABI to load a real `.so`).
fn install_invoke_builtin_output_plugin_callback(
    ctx: &mut types_logical::LogicalDecodingContext,
    inv: &types_logical::CallbackInvocation,
) -> PgResult<bool> {
    if let Some(plugin) = ctx.builtin_plugin {
        if let Some(result) =
            backend_utils_fmgr_dfmgr_seams::registry_invoke_builtin_output_plugin(plugin, ctx, inv)
        {
            return result;
        }
    }
    // Not a registered builtin: route to the OS-loaded plugin (graceful ERROR in
    // this build, since open_library always fails).
    loader::invoke_output_plugin_callback::call(types_logical::CallbackInvocation {
        callback_name: inv.callback_name,
        report_location: inv.report_location,
        accept_writes: inv.accept_writes,
        write_xid: inv.write_xid,
        write_location: inv.write_location,
        end_xact: inv.end_xact,
        args: clone_callback_args(&inv.args),
    })
}

/// Clone the resolved callback args (the flattened OS-loader seam takes them by
/// value). The args are small projected values; the heavy tuple data is resolved
/// lazily by the plugin via the reorderbuffer resolver seams.
fn clone_callback_args(
    args: &types_logical::OutputPluginCallbackArgs,
) -> types_logical::OutputPluginCallbackArgs {
    use types_logical::OutputPluginCallbackArgs as A;
    match args {
        A::Startup { is_init } => A::Startup { is_init: *is_init },
        A::Shutdown => A::Shutdown,
        A::Begin { txn } => A::Begin { txn: *txn },
        A::Commit { txn, commit_lsn } => A::Commit { txn: *txn, commit_lsn: *commit_lsn },
        A::BeginPrepare { txn } => A::BeginPrepare { txn: *txn },
        A::Prepare { txn, prepare_lsn } => A::Prepare { txn: *txn, prepare_lsn: *prepare_lsn },
        A::CommitPrepared { txn, commit_lsn } => {
            A::CommitPrepared { txn: *txn, commit_lsn: *commit_lsn }
        }
        A::RollbackPrepared { txn, prepare_end_lsn, prepare_time } => A::RollbackPrepared {
            txn: *txn,
            prepare_end_lsn: *prepare_end_lsn,
            prepare_time: *prepare_time,
        },
        A::Change { txn, relation, change } => {
            A::Change { txn: *txn, relation: *relation, change: *change }
        }
        A::Truncate { txn, nrelations, relations, change } => A::Truncate {
            txn: *txn,
            nrelations: *nrelations,
            relations: *relations,
            change: *change,
        },
        A::Message {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => A::Message {
            txn: *txn,
            message_lsn: *message_lsn,
            transactional: *transactional,
            prefix: *prefix,
            message_size: *message_size,
            message: *message,
        },
        A::FilterPrepare { xid, gid } => A::FilterPrepare { xid: *xid, gid: gid.clone() },
        A::FilterByOrigin { origin_id } => A::FilterByOrigin { origin_id: *origin_id },
        A::StreamStart { txn } => A::StreamStart { txn: *txn },
        A::StreamStop { txn } => A::StreamStop { txn: *txn },
        A::StreamAbort { txn, abort_lsn } => A::StreamAbort { txn: *txn, abort_lsn: *abort_lsn },
        A::StreamPrepare { txn, prepare_lsn } => {
            A::StreamPrepare { txn: *txn, prepare_lsn: *prepare_lsn }
        }
        A::StreamCommit { txn, commit_lsn } => {
            A::StreamCommit { txn: *txn, commit_lsn: *commit_lsn }
        }
        A::StreamChange { txn, relation, change } => {
            A::StreamChange { txn: *txn, relation: *relation, change: *change }
        }
        A::StreamMessage {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => A::StreamMessage {
            txn: *txn,
            message_lsn: *message_lsn,
            transactional: *transactional,
            prefix: *prefix,
            message_size: *message_size,
            message: *message,
        },
        A::StreamTruncate { txn, nrelations, relations, change } => A::StreamTruncate {
            txn: *txn,
            nrelations: *nrelations,
            relations: *relations,
            change: *change,
        },
    }
}

/// Installer for `backend_utils_fmgr_dfmgr_seams::load_file`: supply the
/// MemoryContext inside the installer (exactly like `load_external_function`
/// / `load_output_plugin`) so the marshaled arity-2 seam shape stays intact.
fn install_load_file(filename: &str, restricted: bool) -> PgResult<()> {
    // A library whose C body is ported into the Rust backend is already "loaded"
    // (its symbols live in-process); a bare load succeeds without the OS loader.
    // The OS loader would still run the module's `_PG_init` on first load
    // (`internal_load_library` → `call_pg_init`); mirror that for the in-process
    // module the first time it is loaded this backend, so e.g. `LOAD 'plpgsql'`
    // runs plpgsql's `_PG_init` (custom-GUC registration + `MarkGUCPrefixReserved`).
    if let Some(library) = simple_library_name(filename) {
        if backend_utils_fmgr_dfmgr_seams::builtin_library_present::call(library) {
            if let Some(pg_init) = backend_utils_fmgr_dfmgr_seams::registry_pg_init(library) {
                let already_inited = BUILTIN_INITED.with(|set| {
                    let mut set = set.borrow_mut();
                    if set.iter().any(|n| *n == library) {
                        true
                    } else {
                        set.push(library.to_owned());
                        false
                    }
                });
                if !already_inited {
                    pg_init()?;
                }
            }
            return Ok(());
        }
    }
    let ctx = mcx::MemoryContext::new("load_file");
    load_file(ctx.mcx(), filename, restricted)
}

/// Install this crate's owned inward seams (`backend-utils-fmgr-dfmgr-seams`).
pub fn init_seams() {
    backend_utils_fmgr_dfmgr_seams::load_file::set(install_load_file);
    backend_utils_fmgr_dfmgr_seams::load_external_function::set(install_load_external_function);

    // LOAD '<file>' (utility.c) reaches dfmgr's `load_file(filename, restricted)`
    // through the tcop utility out-seam. C passes a non-NULL `filename`; route it
    // through the same registry-aware installer so a ported builtin library
    // (e.g. `$libdir/regress`) loads in-process without the OS dynamic loader.
    backend_tcop_utility_out_seams::load_file::set(|filename, restricted| {
        let filename = filename.ok_or_else(|| {
            PgError::error("LOAD: missing library name").with_sqlstate(ERRCODE_INVALID_NAME)
        })?;
        install_load_file(filename, restricted)
    });

    // Install the two ported-library consult-seams to delegate to the shared
    // registry the seams crate owns. Each ported library (regress, plpgsql, ...)
    // registers itself there from its own init_seams via `register_builtin_library`;
    // a single owner can install these OnceLock seams, which the rest of dfmgr
    // (`install_load_external_function` / `install_load_file`) already calls.
    backend_utils_fmgr_dfmgr_seams::builtin_library_present::set(
        backend_utils_fmgr_dfmgr_seams::registry_library_present,
    );
    backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function::set(|library, function| {
        Ok(backend_utils_fmgr_dfmgr_seams::registry_resolve(library, function))
    });

    backend_utils_fmgr_dfmgr_seams::load_output_plugin::set(install_load_output_plugin);
    backend_utils_fmgr_dfmgr_seams::invoke_output_plugin_callback::set(
        install_invoke_output_plugin_callback,
    );

    // Phase-0 builtin output-plugin dispatch: the name-resolver (so
    // StartupDecodingContext can flag `ctx.builtin_plugin`) and the live-ctx
    // dispatch (so the `*_cb_wrapper`s reach the ported plugin's vtable WITH the
    // live `&mut ctx`). Both delegate to the registry the seams crate owns; each
    // builtin plugin (e.g. `contrib-test-decoding`) registers itself there from
    // its own init_seams via `register_builtin_output_plugin`.
    backend_utils_fmgr_dfmgr_seams::builtin_output_plugin_name::set(|plugin| {
        backend_utils_fmgr_dfmgr_seams::resolve_builtin_output_plugin(plugin).map(|e| e.name)
    });
    backend_utils_fmgr_dfmgr_seams::invoke_builtin_output_plugin_callback::set(
        install_invoke_builtin_output_plugin_callback,
    );

    // Parallel-worker transfer of the loaded-library list. The bodies are owned
    // here; the seam decls live in parallel-rt-seams. The DSM chunk is a packed
    // sequence of NUL-terminated filenames ending in a final empty (double-NUL)
    // entry, so it is self-delimiting on the restore side.
    {
        use backend_access_transam_parallel_rt_seams as rt;
        rt::estimate_library_state_space::set(|| Ok(estimate_library_state_space()));
        rt::serialize_library_state::set(|len, space| {
            // SAFETY: `space` is the start of a `len`-byte chunk shm_toc_allocate
            // reserved for the library state (EstimateLibraryStateSpace sized
            // it); the leader writes the whole chunk here. The audited
            // DSM-pointer primitive (cf. backend-utils-misc-guc).
            let buf = unsafe { core::slice::from_raw_parts_mut(space as *mut u8, len) };
            serialize_library_state(len, buf)
        });
        rt::restore_library_state::set(|space| {
            // The blob ends at the final empty entry (a NUL at an entry
            // boundary). Walk the NUL-terminated names to find that terminator,
            // bounding the slice handed to the owner. SAFETY: `space` points at
            // the library-state chunk the leader serialized; the embedded
            // double-NUL terminator bounds the read.
            let mut total = 0usize;
            loop {
                let first = unsafe { *((space + total) as *const u8) };
                if first == 0 {
                    // Final empty entry — include its terminator byte.
                    total += 1;
                    break;
                }
                // Skip this name's bytes plus its NUL terminator.
                let mut i = total;
                loop {
                    let b = unsafe { *((space + i) as *const u8) };
                    i += 1;
                    if b == 0 {
                        break;
                    }
                }
                total = i;
            }
            let buf = unsafe { core::slice::from_raw_parts(space as *const u8, total) };
            restore_library_state(buf)
        });

        // LookupParallelWorkerFunction's external-library branch (parallel.c:1672):
        //   return (parallel_worker_main_type)
        //       load_external_function(libraryname, funcname, true, NULL);
        // Reached only for a non-"postgres" library (the in-core parallel plans
        // all use "postgres"/ParallelQueryMain). `load_external_function` with
        // signal_not_found=true loads the library and raises the C
        // ERRCODE_UNDEFINED_FUNCTION "could not find function" error if the
        // symbol is absent — exactly mirroring `signalNotFound=true`.
        //
        // In this tree the loader does not carry a callable function-pointer
        // address across `function_exists` (port_dynloader_seams: "The resolved
        // symbol address itself is not returned here"), and `invoke_entrypoint`
        // dispatches only the in-core `ParallelQueryMain` token, so a resolved
        // external symbol still cannot be turned into an invokable
        // `parallel_worker_main_type`. The faithful, non-fabricated outcome for
        // such an external entry point is therefore the same lookup-miss error.
        rt::load_external_function::set(|libraryname, funcname| {
            let ctx = mcx::MemoryContext::new("load_external_function");
            // C: load_external_function(libraryname, funcname, true, NULL).
            // Loads the library; raises ERRCODE_UNDEFINED_FUNCTION on a missing
            // symbol (signal_not_found=true). On a missing library it raises the
            // library-load error regardless, as in C.
            let _handle = load_external_function(ctx.mcx(), libraryname, funcname, true)?;
            // The symbol resolved, but no invokable parallel-worker entry point
            // can be produced for an external library in this build (no C
            // function-pointer dispatch). Raise the identical lookup-miss error.
            Err(PgError::error(format!(
                "could not find function \"{funcname}\" in file \"{libraryname}\""
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION))
        });
    }

    // `char *Dynamic_library_path;` (`dfmgr.c`) is this unit's own GUC string
    // variable (`guc_tables.c` binds `&Dynamic_library_path` with no
    // check/assign/show hook). C reads the value straight out of the GUC slot;
    // install accessors so the GUC machinery (SET / boot-value assignment)
    // reads and writes this unit's backing `thread_local`. The variable is
    // never NULL here (boot value `"$libdir"`), so `get` always yields `Some`
    // and `set` substitutes the default for the C NULL case.
    backend_utils_misc_guc_tables::vars::Dynamic_library_path.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: || Some(dynamic_library_path()),
            set: |v| set_dynamic_library_path(&v.unwrap_or_default()),
        },
    );
}

#[cfg(test)]
mod tests;
