// probin is a registry KEY, never a file: no C ABI exists to dlopen (ratified
// no-dlopen carve, docs/design/carve-ratifications.md §2). The registry is the
// listing of $libdir itself — "<pkglib_path>/<name>DLSUFFIX" exists for every
// registered name — and every other path is a real stat(), so a file under
// $libdir/plugins/ stays the administrator's opt-in exactly as in C.

use std::cell::RefCell;
use std::ffi::CString;
use std::sync::Mutex;

use ::elog::{ereport, message_level_is_interesting};
use ::fmgr::PGFunction;
use ::types_error::{
    ErrorLocation, PgError, PgResult, DEBUG3, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_NAME, ERRCODE_UNDEFINED_FILE, ERRCODE_UNDEFINED_FUNCTION, ERROR,
};

pub const DLSUFFIX: &str = if cfg!(target_os = "macos") {
    ".dylib"
} else if cfg!(windows) {
    ".dll"
} else {
    ".so"
};

const LIBDIR_MACRO: &str = "$libdir";
const PLUGINS_PREFIX: &str = "$libdir/plugins/";

guc_tables::session_guc_string!(
    DYNAMIC_LIBRARY_PATH,
    dynamic_library_path_get,
    dynamic_library_path_set,
    Some("$libdir")
);

#[derive(Clone, Copy)]
pub struct BuiltinLibraryEntry {
    pub name: &'static str,
    pub lookup: fn(&str) -> Option<PGFunction>,
    // _PG_init: runs on the library's first load in a session (dfmgr.c
    // internal_load_library).
    pub pg_init: Option<fn() -> PgResult<()>>,
}

static BUILTIN_LIBRARIES: Mutex<Vec<BuiltinLibraryEntry>> = Mutex::new(Vec::new());

#[derive(Clone)]
struct LoadedFile {
    filename: String,
    entry: BuiltinLibraryEntry,
}

// file_list is a process static in C (dfmgr.c:59-60): a forked backend
// inherits the postmaster's shared_preload_libraries loads, so they list in
// pg_get_loaded_modules and a later LOAD of one never re-runs _PG_init.
#[derive(Clone, Default)]
pub struct FileList(Vec<LoadedFile>);

// The DynamicFileList walk (get_first_loaded_module/get_next_loaded_module +
// get_loaded_module_details, dfmgr.c:427-453): this backend's loaded
// libraries in load order, each with the filename it was opened under.
pub struct LoadedModule {
    pub library_path: String,
    pub module_name: &'static str,
}

// SerializeLibraryState (dfmgr.c:722-739): file_list's filenames in load
// order, the chunk a leader hands its parallel workers (parallel.c:383;
// EstimateLibraryStateSpace, dfmgr.c:702-716, only sizes that chunk).
#[derive(Clone, Default)]
pub struct SerializedLibraryState(Vec<String>);

#[derive(Default)]
struct FileListState {
    list: Vec<LoadedFile>,
    // How much of `list` is the fork-inherited postmaster prefix
    // (file_list_inherit): a retained worker thread truncates back to it
    // where C's worker would be a fresh fork (file_list_reset_to_inherited).
    inherited: usize,
}

thread_local! {
    // file_list (dfmgr.c): per-backend record of loaded files (malloc'd in C,
    // outliving every context), so _PG_init runs once per session.
    static FILE_LIST: RefCell<FileListState> =
        const { RefCell::new(FileListState { list: Vec::new(), inherited: 0 }) };
}

pub fn register_builtin_library(entry: BuiltinLibraryEntry) {
    let mut libs = BUILTIN_LIBRARIES.lock().unwrap();
    match libs.iter_mut().find(|e| e.name == entry.name) {
        Some(existing) => *existing = entry,
        None => libs.push(entry),
    }
}

fn registered(key: &str) -> Option<BuiltinLibraryEntry> {
    BUILTIN_LIBRARIES.lock().unwrap().iter().find(|e| e.name == key).copied()
}

fn pkglib_path() -> String {
    let buf = init_small::globals::pkglib_path();
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

fn is_directory(path: &str) -> bool {
    let cpath = CString::new(path).unwrap_or_default();
    let mut st = vfs::FileInfo::zeroed();
    vfs::stat(&cpath, &mut st) == 0 && st.is_dir()
}

// The registry's $libdir listing, read the way stat(2) reads a path: "//",
// "/./" and "x/.." collapse, ".." only backs out of an existing directory
// ($libdir itself counts), and a trailing "/" or "/." names a directory.
fn registered_at(full: &str) -> Option<BuiltinLibraryEntry> {
    let pkglib = pg_path::canonicalize_path(&pkglib_path());
    if pkglib.is_empty() || full.ends_with('/') || full.ends_with("/.") {
        return None;
    }
    let mut dir = String::new();
    for comp in full.split('/') {
        if comp == ".." {
            let d = pg_path::canonicalize_path(&dir);
            if d != pkglib && !is_directory(&d) {
                return None;
            }
        }
        dir.push_str(comp);
        dir.push('/');
    }
    let canonical = pg_path::canonicalize_path(full);
    let key = canonical
        .strip_prefix(pkglib.as_str())?
        .strip_prefix('/')?
        .strip_suffix(DLSUFFIX)?;
    if pg_path::first_dir_separator(key).is_some() {
        return None;
    }
    registered(key)
}

fn file_exists(full: &str) -> PgResult<bool> {
    if registered_at(full).is_some() {
        return Ok(true);
    }
    fd::pg_file_exists(full)
}

fn loc(line: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/utils/fmgr/dfmgr.c", line, funcname)
}

#[cold]
#[inline(never)]
fn invalid_name_error(message: String, line: i32, funcname: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error(message)
            .with_sqlstate(ERRCODE_INVALID_NAME)
            .with_error_location(loc(line, funcname)),
    )
}

#[cold]
#[inline(never)]
fn file_miss_error(libname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "could not access file \"{libname}\": No such file or directory"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FILE)
        .with_error_location(loc(212, "internal_load_library")),
    )
}

#[cold]
#[inline(never)]
fn file_access_error(libname: &str, errnum: i32) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .with_saved_errno(errnum)
            .errcode_for_file_access()
            .errmsg(format!("could not access file \"{libname}\": %m"))
            .into_error()
            .with_error_location(loc(212, "internal_load_library")),
    )
}

pub fn substitute_path_macro(s: &str, macro_name: &str, value: &str) -> PgResult<String> {
    if !s.starts_with('$') {
        return Ok(s.to_owned());
    }
    let sep = pg_path::first_dir_separator(s).unwrap_or(s.len());
    if macro_name.len() != sep || !s.starts_with(macro_name) {
        return Err(invalid_name_error(
            format!("invalid macro name in path: {s}"),
            552,
            "substitute_path_macro",
        ));
    }
    Ok(format!("{value}{}", &s[sep..]))
}

pub fn find_in_path(
    basename: &str,
    path: &str,
    path_param: &str,
    macro_name: &str,
    macro_val: &str,
) -> PgResult<Option<String>> {
    if path.is_empty() {
        return Ok(None);
    }
    let mut p = path;
    loop {
        let len = match pg_path::first_path_var_separator(p) {
            Some(0) => {
                return Err(invalid_name_error(
                    format!("zero-length component in parameter \"{path_param}\""),
                    604,
                    "find_in_path",
                ))
            }
            Some(i) => i,
            None => p.len(),
        };
        let mangled =
            pg_path::canonicalize_path(&substitute_path_macro(&p[..len], macro_name, macro_val)?);
        if !pg_path::is_absolute_path(&mangled) {
            return Err(invalid_name_error(
                format!("component in parameter \"{path_param}\" is not an absolute path"),
                623,
                "find_in_path",
            ));
        }
        let full = format!("{mangled}/{basename}");
        if message_level_is_interesting(DEBUG3) {
            ereport(DEBUG3)
                .errmsg_internal(format!("find_in_path: trying \"{full}\""))
                .finish(loc(631, "find_in_path"))?;
        }
        if file_exists(&full)? {
            return Ok(Some(full));
        }
        if len == p.len() {
            return Ok(None);
        }
        p = &p[len + 1..];
    }
}

fn expand_dynamic_library_name(name: &str) -> PgResult<String> {
    let have_slash = pg_path::first_dir_separator(name).is_some();
    let pkglib = pkglib_path();
    let path = dynamic_library_path_get().unwrap_or_default();
    for candidate in [name.to_owned(), format!("{name}{DLSUFFIX}")] {
        if !have_slash {
            if let Some(full) =
                find_in_path(&candidate, &path, "dynamic_library_path", LIBDIR_MACRO, &pkglib)?
            {
                return Ok(full);
            }
        } else {
            let full = substitute_path_macro(&candidate, LIBDIR_MACRO, &pkglib)?;
            if file_exists(&full)? {
                return Ok(full);
            }
        }
    }
    Ok(name.to_owned())
}

pub fn check_restricted_library_name(name: &str) -> PgResult<()> {
    if !name.starts_with(PLUGINS_PREFIX)
        || pg_path::first_dir_separator(&name[PLUGINS_PREFIX.len()..]).is_some()
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("access to library \"{name}\" is not allowed"))
            .finish(loc(525, "check_restricted_library_name"));
    }
    Ok(())
}

// A filename already in file_list needs no stat (C's first scan). Otherwise
// a path outside the registry's $libdir view is stat()ed like C; an existing
// file cannot be dlopen'd (carve §2), so its basename keys the registry. The
// entry is the module identity (C's SAME_INODE scan).
fn internal_load_library(libname: &str) -> PgResult<BuiltinLibraryEntry> {
    let known = FILE_LIST
        .with(|s| s.borrow().list.iter().find(|f| f.filename == libname).map(|f| f.entry));
    if let Some(entry) = known {
        return Ok(entry);
    }
    let entry = match registered_at(libname) {
        Some(e) => e,
        None => {
            let cpath = CString::new(libname).unwrap_or_default();
            let mut st = vfs::FileInfo::zeroed();
            if vfs::stat(&cpath, &mut st) != 0 {
                return Err(file_access_error(libname, vfs::get_errno()));
            }
            let base = &libname[pg_path::last_dir_separator(libname).map_or(0, |i| i + 1)..];
            let key = base.strip_suffix(DLSUFFIX).unwrap_or(base);
            registered(key).ok_or_else(|| file_miss_error(libname))?
        }
    };
    // Linked into file_list only after _PG_init succeeds, as in C.
    let already = FILE_LIST.with(|s| s.borrow().list.iter().any(|f| f.entry.name == entry.name));
    if !already {
        if let Some(init) = entry.pg_init {
            init()?;
        }
        FILE_LIST.with(|s| {
            s.borrow_mut().list.push(LoadedFile { filename: libname.to_owned(), entry })
        });
    }
    Ok(entry)
}

pub fn load_file(filename: &str, restricted: bool) -> PgResult<()> {
    if restricted {
        check_restricted_library_name(filename)?;
    }
    let fullname = expand_dynamic_library_name(filename)?;
    internal_load_library(&fullname)?;
    Ok(())
}

// file_list membership for THIS backend (dfmgr.c: a library is "loaded" in
// a process once internal_load_library linked it in — inherited from the
// postmaster under shared_preload_libraries, or added by a session LOAD /
// session_preload_libraries). Hook-only modules whose hooks are process-wide
// gate on this to reproduce C's per-process hook installation.
pub fn is_loaded(module_name: &str) -> bool {
    FILE_LIST.with(|s| s.borrow().list.iter().any(|f| f.entry.name == module_name))
}

pub fn loaded_modules() -> Vec<LoadedModule> {
    FILE_LIST.with(|s| {
        s.borrow()
            .list
            .iter()
            .map(|f| LoadedModule { library_path: f.filename.clone(), module_name: f.entry.name })
            .collect()
    })
}

// The postmaster's file_list, captured on its thread for a child thread's
// fork-inherited globals (launch_backend).
pub fn file_list_snapshot() -> FileList {
    FILE_LIST.with(|s| FileList(s.borrow().list.clone()))
}

pub fn file_list_inherit(list: &FileList) {
    FILE_LIST.with(|s| {
        *s.borrow_mut() = FileListState { list: list.0.clone(), inherited: list.0.len() }
    });
    let inits = BACKEND_INITS.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for f in &list.0 {
        if let Some((_, init)) = inits.iter().find(|(n, _)| *n == f.entry.name) {
            let _ = init();
        }
    }
}

// A preloaded library whose _PG_init state is per-thread here (C's fork
// hands the postmaster's copy to every child): re-run in each inheriting
// backend by file_list_inherit.
static BACKEND_INITS: Mutex<Vec<(&'static str, fn() -> PgResult<()>)>> = Mutex::new(Vec::new());

pub fn register_backend_init(name: &'static str, init: fn() -> PgResult<()>) {
    let mut v = BACKEND_INITS.lock().unwrap_or_else(|e| e.into_inner());
    if !v.iter().any(|(n, _)| *n == name) {
        v.push((name, init));
    }
}

// A C parallel worker is a fresh fork: its file_list starts as the
// postmaster's (parallel.c:1459-1462 then RestoreLibraryState on top). A
// retained worker thread drops what earlier tasks loaded, so a library one
// leader LOADed never stays "loaded" (is_loaded, hook gating,
// pg_get_loaded_modules) under the next leader's task.
pub fn file_list_reset_to_inherited() {
    FILE_LIST.with(|s| {
        let mut st = s.borrow_mut();
        let n = st.inherited;
        st.list.truncate(n);
    });
}

// SerializeLibraryState (dfmgr.c:722-739): every loaded filename, in
// file_list order.
pub fn serialize_library_state() -> SerializedLibraryState {
    FILE_LIST.with(|s| {
        SerializedLibraryState(s.borrow().list.iter().map(|f| f.filename.clone()).collect())
    })
}

// RestoreLibraryState (dfmgr.c:745-752): load every library the serializing
// backend had loaded — internal_load_library on the recorded filename, so an
// inherited (postmaster) entry is found in file_list and skipped, and each
// new one runs its _PG_init before being linked in.
pub fn restore_library_state(state: &SerializedLibraryState) -> PgResult<()> {
    for filename in &state.0 {
        internal_load_library(filename)?;
    }
    Ok(())
}

pub fn load_external_function(
    filename: &str,
    funcname: &str,
    signal_not_found: bool,
) -> PgResult<Option<PGFunction>> {
    let filename = match filename.strip_prefix("$libdir/") {
        Some(simple) if pg_path::first_dir_separator(simple).is_none() => simple,
        _ => filename,
    };
    let fullname = expand_dynamic_library_name(filename)?;
    let entry = internal_load_library(&fullname)?;
    match (entry.lookup)(funcname) {
        Some(f) => Ok(Some(f)),
        None if signal_not_found => Err(Box::new(
            PgError::error(format!(
                "could not find function \"{funcname}\" in file \"{fullname}\""
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
            .with_error_location(loc(131, "load_external_function")),
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_error::SqlState;

    fn set_pkglib(dir: &str) {
        let mut buf = [0u8; pg_path::MAXPGPATH];
        buf[..dir.len()].copy_from_slice(dir.as_bytes());
        init_small::globals::set_pkglib_path(buf);
    }

    fn setup() -> String {
        register_builtin_library(BuiltinLibraryEntry { name: "tlib", lookup: |_| None, pg_init: None });
        let pkglib = format!("/nonexistent-pkglib-{}", std::process::id());
        set_pkglib(&pkglib);
        dynamic_library_path_set(Some("$libdir".to_owned()));
        pkglib
    }

    fn loaded_module_names() -> Vec<&'static str> {
        loaded_modules().into_iter().map(|m| m.module_name).collect()
    }

    #[track_caller]
    fn assert_err<T>(r: PgResult<T>, state: SqlState, msg: &str) {
        let e = r.err().expect("expected an error");
        assert_eq!((e.sqlstate(), e.message()), (state, msg));
    }

    #[test]
    fn bare_and_libdir_names_resolve() {
        let pkglib = setup();
        let with_suffix = format!("tlib{DLSUFFIX}");
        let libdir_with_suffix = format!("$libdir/tlib{DLSUFFIX}");
        for name in ["tlib", "$libdir/tlib", &with_suffix, &libdir_with_suffix] {
            load_file(name, false).unwrap_or_else(|e| panic!("{name}: {}", e.message()));
        }
        assert_eq!(loaded_module_names(), vec!["tlib"]);
        assert_err(
            load_external_function("$libdir/tlib", "nosuchsymbol", true),
            ERRCODE_UNDEFINED_FUNCTION,
            &format!("could not find function \"nosuchsymbol\" in file \"{pkglib}/tlib{DLSUFFIX}\""),
        );
        assert!(load_external_function("$libdir/tlib", "nosuchsymbol", false).unwrap().is_none());
    }

    #[test]
    fn directory_component_and_foreign_suffix_are_file_misses() {
        setup();
        let foreign = if DLSUFFIX == ".so" { "tlib.dylib" } else { "tlib.so" };
        for name in ["/nonexistent/dir/tlib", "sub/tlib", foreign, "$libdir/plugins/tlib", "$libdir/sub/tlib"] {
            assert_err(
                load_file(name, false),
                ERRCODE_UNDEFINED_FILE,
                &format!("could not access file \"{name}\": No such file or directory"),
            );
        }
        assert_err(
            load_external_function("$libdir/sub/tlib", "f", true),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$libdir/sub/tlib\": No such file or directory",
        );
        assert_err(
            load_external_function("$libdir/nosuchlib", "f", true),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"nosuchlib\": No such file or directory",
        );
        assert!(loaded_module_names().is_empty());
    }

    #[test]
    fn restricted_load_needs_a_real_plugins_file() {
        setup();
        assert_err(
            load_file("$libdir/plugins/tlib", true),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$libdir/plugins/tlib\": No such file or directory",
        );
        for name in ["tlib", "$libdir/tlib", "$libdir/plugins/../tlib", "$libdir/plugins/a/b", "/tmp/tlib"] {
            assert_err(
                load_file(name, true),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                &format!("access to library \"{name}\" is not allowed"),
            );
        }
        assert!(loaded_module_names().is_empty());

        let pkglib = std::env::temp_dir().join(format!("dfmgr-pkglib-{}", std::process::id()));
        let plugins = pkglib.join("plugins");
        std::fs::create_dir_all(&plugins).unwrap();
        std::fs::write(plugins.join(format!("tlib{DLSUFFIX}")), b"").unwrap();
        set_pkglib(pkglib.to_str().unwrap());
        load_file("$libdir/plugins/tlib", true).unwrap();
        assert_eq!(loaded_module_names(), vec!["tlib"]);
        assert_err(
            load_file("$libdir/plugins/other", true),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$libdir/plugins/other\": No such file or directory",
        );
        std::fs::remove_dir_all(&pkglib).unwrap();
    }

    #[test]
    fn non_canonical_spellings_resolve_like_stat() {
        let pkglib = setup();
        let base = pkglib.trim_start_matches('/').to_owned();
        let up_and_back = format!("$libdir/../{base}/tlib");
        let abs_double = format!("{pkglib}//tlib");
        let dot_suffix = format!("$libdir/.//tlib{DLSUFFIX}");
        for name in ["$libdir//tlib", "$libdir/./tlib", &up_and_back, &abs_double, &dot_suffix] {
            load_file(name, false).unwrap_or_else(|e| panic!("{name}: {}", e.message()));
        }
        assert_eq!(loaded_module_names(), vec!["tlib"]);
        assert_err(
            load_external_function("$libdir//tlib", "nosuchsymbol", true),
            ERRCODE_UNDEFINED_FUNCTION,
            &format!("could not find function \"nosuchsymbol\" in file \"{pkglib}//tlib{DLSUFFIX}\""),
        );
        let trailing_slash = format!("$libdir/tlib{DLSUFFIX}/");
        let trailing_dot = format!("$libdir/tlib{DLSUFFIX}/.");
        let through_file = format!("$libdir/tlib{DLSUFFIX}/../tlib");
        for name in ["$libdir/plugins/../tlib", "$libdir/tlib/../tlib", &trailing_slash, &trailing_dot, &through_file] {
            assert_err(
                load_file(name, false),
                ERRCODE_UNDEFINED_FILE,
                &format!("could not access file \"{name}\": No such file or directory"),
            );
        }

        let real = std::env::temp_dir().join(format!("dfmgr-canon-{}", std::process::id()));
        std::fs::create_dir_all(real.join("plugins")).unwrap();
        set_pkglib(real.to_str().unwrap());
        load_file("$libdir/plugins/../tlib", false).unwrap();
        load_file("$libdir/plugins/./..//tlib", false).unwrap();
        assert_err(
            load_file("$libdir/plugins/nodir/../tlib", false),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$libdir/plugins/nodir/../tlib\": No such file or directory",
        );
        std::fs::remove_dir_all(&real).unwrap();
    }

    #[test]
    fn recorded_filename_loads_without_stat() {
        setup();
        let pkglib = std::env::temp_dir().join(format!("dfmgr-filelist-{}", std::process::id()));
        std::fs::create_dir_all(pkglib.join("plugins")).unwrap();
        let file = pkglib.join("plugins").join(format!("tlib{DLSUFFIX}"));
        std::fs::write(&file, b"").unwrap();
        set_pkglib(pkglib.to_str().unwrap());
        load_file("$libdir/plugins/tlib", true).unwrap();
        std::fs::remove_file(&file).unwrap();
        load_file(file.to_str().unwrap(), false).unwrap();
        assert_err(
            load_file("$libdir/plugins/tlib", false),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$libdir/plugins/tlib\": No such file or directory",
        );
        assert_eq!(loaded_module_names(), vec!["tlib"]);
        std::fs::remove_dir_all(&pkglib).unwrap();
    }

    #[test]
    fn dynamic_library_path_is_validated() {
        setup();
        for (path, state, msg) in [
            ("$foo", ERRCODE_INVALID_NAME, "invalid macro name in path: $foo"),
            (":/tmp", ERRCODE_INVALID_NAME, "zero-length component in parameter \"dynamic_library_path\""),
            ("/tmp::$libdir", ERRCODE_INVALID_NAME, "zero-length component in parameter \"dynamic_library_path\""),
            ("relative/dir", ERRCODE_INVALID_NAME, "component in parameter \"dynamic_library_path\" is not an absolute path"),
            ("$libdir:", ERRCODE_INVALID_NAME, "component in parameter \"dynamic_library_path\" is not an absolute path"),
            ("", ERRCODE_UNDEFINED_FILE, "could not access file \"tlib\": No such file or directory"),
            ("$libdir/sub", ERRCODE_UNDEFINED_FILE, "could not access file \"tlib\": No such file or directory"),
        ] {
            dynamic_library_path_set(Some(path.to_owned()));
            assert_err(load_file("tlib", false), state, msg);
        }
        dynamic_library_path_set(Some("/tmp:$libdir".to_owned()));
        load_file("tlib", false).unwrap();
        assert_err(load_file("$foo/bar", false), ERRCODE_INVALID_NAME, "invalid macro name in path: $foo/bar");
        assert_err(load_file("$libdirx/tlib", false), ERRCODE_INVALID_NAME, "invalid macro name in path: $libdirx/tlib");
        assert_err(
            load_file("$foo", false),
            ERRCODE_UNDEFINED_FILE,
            "could not access file \"$foo\": No such file or directory",
        );
    }

    #[test]
    fn loaded_module_carries_the_opened_filename() {
        let pkglib = setup();
        load_file("tlib", false).unwrap();
        let m = loaded_modules();
        assert_eq!(m.len(), 1);
        assert_eq!(
            (m[0].module_name, m[0].library_path.as_str()),
            ("tlib", format!("{pkglib}/tlib{DLSUFFIX}").as_str())
        );
    }

    #[test]
    fn inherited_file_list_runs_backend_init_in_the_child() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static INITS: AtomicUsize = AtomicUsize::new(0);
        let pkglib = setup();
        register_builtin_library(BuiltinLibraryEntry { name: "tbinit", lookup: |_| None, pg_init: None });
        register_backend_init("tbinit", || {
            INITS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        load_file("tbinit", false).unwrap();
        assert_eq!(INITS.load(Ordering::SeqCst), 0);
        let snapshot = file_list_snapshot();
        std::thread::spawn(move || {
            set_pkglib(&pkglib);
            dynamic_library_path_set(Some("$libdir".to_owned()));
            file_list_inherit(&snapshot);
            assert_eq!(INITS.load(Ordering::SeqCst), 1);
            load_file("tbinit", false).unwrap();
            assert_eq!(INITS.load(Ordering::SeqCst), 1);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn inherited_file_list_skips_pg_init_in_the_child() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static INITS: AtomicUsize = AtomicUsize::new(0);
        let pkglib = setup();
        register_builtin_library(BuiltinLibraryEntry {
            name: "tinh",
            lookup: |_| None,
            pg_init: Some(|| {
                INITS.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        });
        load_file("tinh", false).unwrap();
        assert_eq!(INITS.load(Ordering::SeqCst), 1);
        let snapshot = file_list_snapshot();
        std::thread::spawn(move || {
            set_pkglib(&pkglib);
            dynamic_library_path_set(Some("$libdir".to_owned()));
            assert!(loaded_modules().is_empty());
            file_list_inherit(&snapshot);
            assert_eq!(loaded_module_names(), vec!["tinh"]);
            // C: the backend finds the postmaster's entry in file_list, so
            // LOAD of a preloaded library neither stats nor re-inits it.
            load_file("tinh", false).unwrap();
            load_file("$libdir/tinh", false).unwrap();
            assert_eq!(INITS.load(Ordering::SeqCst), 1);
            assert_eq!(loaded_module_names(), vec!["tinh"]);
        })
        .join()
        .unwrap();
        assert_eq!(loaded_module_names(), vec!["tinh"]);
    }

    // SerializeLibraryState/RestoreLibraryState (dfmgr.c:722-752) across a
    // leader thread and a worker thread: the worker ends with the leader's
    // list in load order, runs _PG_init once for each library it did not
    // inherit, and skips the ones it did.
    #[test]
    fn library_state_restores_into_a_worker_thread() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static INITS_PRE: AtomicUsize = AtomicUsize::new(0);
        static INITS_SES: AtomicUsize = AtomicUsize::new(0);
        let pkglib = setup();
        register_builtin_library(BuiltinLibraryEntry {
            name: "tpre",
            lookup: |_| None,
            pg_init: Some(|| {
                INITS_PRE.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        });
        register_builtin_library(BuiltinLibraryEntry {
            name: "tses",
            lookup: |_| None,
            pg_init: Some(|| {
                INITS_SES.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        });
        // Postmaster: shared_preload_libraries=tpre; the leader inherits it
        // and LOADs tses; the worker inherits tpre only.
        load_file("tpre", false).unwrap();
        let postmaster = file_list_snapshot();
        file_list_inherit(&postmaster);
        load_file("tses", false).unwrap();
        load_file("tlib", false).unwrap();
        assert_eq!(loaded_module_names(), vec!["tpre", "tses", "tlib"]);
        let state = serialize_library_state();
        assert_eq!(
            state.0,
            vec![
                format!("{pkglib}/tpre{DLSUFFIX}"),
                format!("{pkglib}/tses{DLSUFFIX}"),
                format!("{pkglib}/tlib{DLSUFFIX}")
            ]
        );
        let (pre, ses) = (INITS_PRE.load(Ordering::SeqCst), INITS_SES.load(Ordering::SeqCst));
        std::thread::spawn(move || {
            set_pkglib(&pkglib);
            dynamic_library_path_set(Some("$libdir".to_owned()));
            file_list_inherit(&postmaster);
            assert_eq!(loaded_module_names(), vec!["tpre"]);
            restore_library_state(&state).unwrap();
            assert_eq!(loaded_module_names(), vec!["tpre", "tses", "tlib"]);
            assert_eq!(INITS_PRE.load(Ordering::SeqCst), pre);
            assert_eq!(INITS_SES.load(Ordering::SeqCst), ses + 1);
            // Restoring again is a no-op (every filename is already in
            // file_list: C's first scan).
            restore_library_state(&state).unwrap();
            assert_eq!(loaded_module_names(), vec!["tpre", "tses", "tlib"]);
            assert_eq!(INITS_SES.load(Ordering::SeqCst), ses + 1);
            // A retained worker thread claimed by another leader starts from
            // the postmaster's list again, like C's fresh fork.
            file_list_reset_to_inherited();
            assert_eq!(loaded_module_names(), vec!["tpre"]);
            assert!(!is_loaded("tses"));
            restore_library_state(&SerializedLibraryState(vec![format!(
                "{pkglib}/tlib{DLSUFFIX}"
            )]))
            .unwrap();
            assert_eq!(loaded_module_names(), vec!["tpre", "tlib"]);
            assert_eq!(INITS_SES.load(Ordering::SeqCst), ses + 1);
        })
        .join()
        .unwrap();
        assert_eq!(loaded_module_names(), vec!["tpre", "tses", "tlib"]);
    }
}
