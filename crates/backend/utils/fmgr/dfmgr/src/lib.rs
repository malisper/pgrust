// probin is a registry KEY, never a file: no C ABI exists to dlopen, so an
// unregistered library must stay loud with C's file-access error.

use std::cell::RefCell;
use std::sync::Mutex;

use ::fmgr::PGFunction;
use ::types_error::{PgError, PgResult, ERRCODE_UNDEFINED_FILE, ERRCODE_UNDEFINED_FUNCTION};

pub struct BuiltinLibraryEntry {
    pub name: &'static str,
    pub lookup: fn(&str) -> Option<PGFunction>,
    // _PG_init: runs on the library's first load in a session (dfmgr.c
    // internal_load_library).
    pub pg_init: Option<fn() -> PgResult<()>>,
}

static BUILTIN_LIBRARIES: Mutex<Vec<BuiltinLibraryEntry>> = Mutex::new(Vec::new());

const KNOWN_DLSUFFIXES: [&str; 3] = [".so", ".dylib", ".dll"];

// Key is platform-independent (any suffix strips): looked up, never opened.
pub fn simple_library_name(name: &str) -> Option<&str> {
    let base = name.rsplit('/').next().unwrap_or(name);
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

pub fn register_builtin_library(entry: BuiltinLibraryEntry) {
    let mut libs = BUILTIN_LIBRARIES.lock().unwrap();
    match libs.iter_mut().find(|e| e.name == entry.name) {
        Some(existing) => *existing = entry,
        None => libs.push(entry),
    }
}

pub fn library_present(filename: &str) -> bool {
    match simple_library_name(filename) {
        Some(key) => BUILTIN_LIBRARIES.lock().unwrap().iter().any(|e| e.name == key),
        None => false,
    }
}

thread_local! {
    // file_list (dfmgr.c): per-backend record of loaded libraries, so
    // _PG_init runs once per session.
    static LOADED_LIBRARIES: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

// load_file (dfmgr.c) for the LOAD command: resolve against the builtin
// registry (no dlopen exists) and run _PG_init on first load. An unregistered
// name is C's stat() miss.
pub fn load_file(filename: &str) -> PgResult<()> {
    let entry = simple_library_name(filename).and_then(|key| {
        let libs = BUILTIN_LIBRARIES.lock().unwrap();
        libs.iter().find(|e| e.name == key).map(|e| (e.name, e.pg_init))
    });
    let Some((name, pg_init)) = entry else {
        return Err(Box::new(
            PgError::error(format!(
                "could not access file \"{filename}\": No such file or directory"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FILE),
        ));
    };
    // Marked loaded only after _PG_init succeeds, mirroring C's unlink of the
    // file_list entry on load failure.
    let already = LOADED_LIBRARIES.with(|s| s.borrow().contains(&name));
    if !already {
        if let Some(init) = pg_init {
            init()?;
        }
        LOADED_LIBRARIES.with(|s| s.borrow_mut().push(name));
    }
    Ok(())
}

fn registry_resolve(key: &str, funcname: &str) -> Option<Option<PGFunction>> {
    let libs = BUILTIN_LIBRARIES.lock().unwrap();
    let entry = libs.iter().find(|e| e.name == key)?;
    Some((entry.lookup)(funcname))
}

// Unregistered library = C's stat() miss; registered-but-missing symbol = the
// C lookup miss, suppressed when !signal_not_found.
pub fn load_external_function(
    filename: &str,
    funcname: &str,
    signal_not_found: bool,
) -> PgResult<Option<PGFunction>> {
    let resolved = simple_library_name(filename).and_then(|key| registry_resolve(key, funcname));
    match resolved {
        Some(Some(f)) => Ok(Some(f)),
        Some(None) => {
            if signal_not_found {
                Err(Box::new(
                    PgError::error(format!(
                        "could not find function \"{funcname}\" in file \"{filename}\""
                    ))
                    .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
                ))
            } else {
                Ok(None)
            }
        }
        None => Err(Box::new(
            PgError::error(format!(
                "could not access file \"{filename}\": No such file or directory"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FILE),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_names() {
        assert_eq!(simple_library_name("$libdir/regress"), Some("regress"));
        assert_eq!(simple_library_name("/a/b/regress.so"), Some("regress"));
        assert_eq!(simple_library_name("regress.dylib"), Some("regress"));
        assert_eq!(simple_library_name("regress"), Some("regress"));
        assert_eq!(simple_library_name(""), None);
        assert_eq!(simple_library_name("dir/"), None);
    }

    #[test]
    fn unknown_library_is_file_error() {
        let err = load_external_function("nosuchfile", "f", true).unwrap_err();
        assert!(err.message().contains("could not access file \"nosuchfile\""));
    }

    #[test]
    fn missing_symbol() {
        register_builtin_library(BuiltinLibraryEntry { name: "tlib", lookup: |_| None, pg_init: None });
        let err = load_external_function("$libdir/tlib", "nosuchsymbol", true).unwrap_err();
        assert!(err
            .message()
            .contains("could not find function \"nosuchsymbol\" in file \"$libdir/tlib\""));
        assert!(load_external_function("$libdir/tlib", "nosuchsymbol", false)
            .unwrap()
            .is_none());
    }
}
