#[cfg(test)]
mod tests;

use std::path::{Component, Path, PathBuf};

use conffiles_seams::ConfFilesInDir;
use elog::ereport;
use types_error::{ErrorLevel, PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

// is_absolute_path (port/path.c), POSIX target.
fn is_absolute_path(location: &str) -> bool {
    location.starts_with('/')
}

// Lexical canonicalize_path (port/path.c): collapses . / .. / duplicate
// separators without touching the filesystem, so not-yet-existing paths work.
fn canonicalize_path_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
            Component::RootDir | Component::Prefix(_) => out.push(component.as_os_str()),
        }
    }
    out
}

pub fn absolute_config_location(location: &str, calling_file: Option<&Path>) -> PathBuf {
    if is_absolute_path(location) {
        return PathBuf::from(location);
    }
    let base = match calling_file {
        Some(calling_file) => calling_file
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default(),
        None => PathBuf::from(
            init_small::globals::DataDir().expect("DataDir must be set (C Assert(DataDir))"),
        ),
    };
    canonicalize_path_lexically(&base.join(location))
}

// ereport(elevel) then *err_msg + NULL return: >= ERROR throws, below emits
// and records the soft message.
fn resolve(
    elevel: ErrorLevel,
    error: PgError,
    err_msg: impl Into<String>,
) -> PgResult<ConfFilesInDir> {
    if elevel >= ERROR {
        Err(error.into())
    } else {
        if elog::message_level_is_interesting(elevel) {
            elog::emit_error_report_for(&error);
        }
        Ok(ConfFilesInDir {
            filenames: Vec::new(),
            err_msg: Some(err_msg.into()),
        })
    }
}

fn io_error(elevel: ErrorLevel, error: &std::io::Error, message: String) -> PgError {
    let mut builder = ereport(elevel);
    if let Some(errno) = error.raw_os_error() {
        builder = builder.with_saved_errno(errno).errcode_for_file_access();
    }
    builder.errmsg(message).into_error()
}

pub fn get_conf_files_in_dir(
    includedir: &str,
    calling_file: Option<&Path>,
    elevel: ErrorLevel,
) -> PgResult<ConfFilesInDir> {
    // An all-blank (including empty) name would read the containing directory.
    if includedir
        .bytes()
        .all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
        let error = ereport(elevel)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "empty configuration directory name: \"{includedir}\""
            ))
            .into_error();
        return resolve(elevel, error, "empty configuration directory name");
    }

    let directory = absolute_config_location(includedir, calling_file);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) => {
            let pg_error = io_error(
                elevel,
                &error,
                format!(
                    "could not open configuration directory \"{}\": %m",
                    directory.display()
                ),
            );
            return resolve(
                elevel,
                pg_error,
                format!("could not open directory \"{}\"", directory.display()),
            );
        }
    };

    let mut filenames: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            // ReadDir failure is ereport(ERROR) regardless of elevel (fd.c).
            Err(error) => {
                return Err(io_error(
                    ERROR,
                    &error,
                    format!("could not read directory \"{}\": %m", directory.display()),
                )
                .into())
            }
        };

        // Only *.conf, at least 6 bytes, not dot-prefixed ("."/"..", hidden,
        // backup, editor debris).
        let name = entry.file_name();
        let name_bytes = name.as_encoded_bytes();
        if name_bytes.len() < 6 || name_bytes[0] == b'.' || !name_bytes.ends_with(b".conf") {
            continue;
        }

        let filename = canonicalize_path_lexically(&directory.join(&name));
        // get_dirent_type(filename, de, look_through_symlinks=true, elevel).
        match std::fs::metadata(&filename) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => filenames.push(filename),
            Err(error) => {
                let pg_error = io_error(
                    elevel,
                    &error,
                    format!("could not stat file \"{}\": %m", filename.display()),
                );
                return resolve(
                    elevel,
                    pg_error,
                    format!("could not stat file \"{}\"", filename.display()),
                );
            }
        }
    }

    // qsort(pg_qsort_strcmp): byte-wise path order.
    filenames.sort_by(|a, b| {
        a.as_os_str()
            .as_encoded_bytes()
            .cmp(b.as_os_str().as_encoded_bytes())
    });

    Ok(ConfFilesInDir {
        filenames,
        err_msg: None,
    })
}

pub fn init_seams() {
    conffiles_seams::absolute_config_location::set(|location, calling_file| {
        absolute_config_location(&location, calling_file.as_deref())
    });
    conffiles_seams::get_conf_files_in_dir::set(|includedir, calling_file, elevel| {
        get_conf_files_in_dir(&includedir, calling_file.as_deref(), elevel)
    });
}
