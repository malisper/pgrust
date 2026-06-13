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

/// Platform shared-library suffix (`DLSUFFIX`), `.so` on the build platforms.
const DLSUFFIX: &str = ".so";

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
    if module_magic_data.abi_extra != magic_data.abi_extra {
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
 * Seam installation.
 *
 * This unit installs no seams of its own: the OS dynamic loader
 * (port-dynloader-seams), common/path.c (common-path-seams), and
 * storage/file/fd.c (backend-storage-file-fd-seams) are owned by other (not-
 * yet-ported) units, and `Dynamic_library_path` is a backend-local accessor,
 * not a cross-cycle seam. No ported crate calls into dfmgr yet, so there is no
 * `backend-utils-fmgr-dfmgr-seams` crate.
 * ========================================================================= */

/// Install this crate's seams. There are none yet; kept for the `seams-init`
/// aggregator contract.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
