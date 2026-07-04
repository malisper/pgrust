// dfmgr.c, in-process ported-library slice: probin is a registry KEY, never a
// file. The Rust backend exposes no C ABI, so libraries whose C bodies are
// ported in-process (regress, ...) resolve here; there is NO dlopen — a probin
// that names no registered library raises C's file-access error.

use std::sync::Mutex;

use ::fmgr::PGFunction;
use ::types_error::{PgError, PgResult, ERRCODE_UNDEFINED_FILE, ERRCODE_UNDEFINED_FUNCTION};

pub struct BuiltinLibraryEntry {
    pub name: &'static str,
    pub lookup: fn(&str) -> Option<PGFunction>,
}

static BUILTIN_LIBRARIES: Mutex<Vec<BuiltinLibraryEntry>> = Mutex::new(Vec::new());

const KNOWN_DLSUFFIXES: [&str; 3] = [".so", ".dylib", ".dll"];

// `$libdir/regress`, `/abs/path/regress.so`, `regress.dylib` → `regress`.
// The key is platform-independent: the name is looked up, never opened.
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

// Idempotent per name (a double init_seams is harmless).
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

fn registry_resolve(key: &str, funcname: &str) -> Option<Option<PGFunction>> {
    let libs = BUILTIN_LIBRARIES.lock().unwrap();
    let entry = libs.iter().find(|e| e.name == key)?;
    Some((entry.lookup)(funcname))
}

// load_external_function(filename, funcname, signalNotFound, filehandle)
// (dfmgr.c) over the registry. An unregistered library is the C stat() miss
// (`could not access file`); a registered library that lacks the symbol is the
// C lookup miss (`could not find function`), suppressed when !signal_not_found.
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
        register_builtin_library(BuiltinLibraryEntry { name: "tlib", lookup: |_| None });
        let err = load_external_function("$libdir/tlib", "nosuchsymbol", true).unwrap_err();
        assert!(err
            .message()
            .contains("could not find function \"nosuchsymbol\" in file \"$libdir/tlib\""));
        assert!(load_external_function("$libdir/tlib", "nosuchsymbol", false)
            .unwrap()
            .is_none());
    }
}
