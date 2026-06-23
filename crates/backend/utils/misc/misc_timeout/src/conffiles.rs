//! Port of `src/backend/utils/misc/conffiles.c` — utilities for handling
//! configuration files (used by GUC processing and authentication parsing).
//!
//! `AbsoluteConfigLocation`/`GetConfFilesInDir` are the two public functions
//! `guc-file.l` (and `hba.c`) call; this crate owns the
//! `backend-utils-misc-conffiles-seams` declarations and installs them from
//! `init_seams`. `DataDir` is a `globals.c` backend-global reached through the
//! `backend-utils-init-small-seams::data_dir` seam. `AllocateDir`/`ReadDir`/
//! `get_dirent_type` become ordinary `std::fs` directory iteration; the C
//! `qsort(pg_qsort_strcmp)` becomes a byte-wise sort of the path strings.

use std::path::{Component, Path, PathBuf};

use utils_error::{ereport, PgError, PgResult};
use ::init_small_seams::data_dir;
use ::conffiles_seams::ConfFilesInDir;
use types_error::{ErrorLevel, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OUT_OF_MEMORY, ERROR};

/// `AbsoluteConfigLocation(location, calling_file)` — given a possibly-relative
/// configuration file or directory location, return an absolute one.
///
/// A relative location is taken relative to the directory holding the calling
/// file, or to `DataDir` if there is no calling file.
pub fn absolute_config_location(location: &str, calling_file: Option<&Path>) -> PathBuf {
    if is_absolute_path(location) {
        // C: pstrdup(location)
        return PathBuf::from(location);
    }

    // C: build abs_path, then canonicalize_path(abs_path).
    let base = match calling_file {
        // C: strlcpy(abs_path, calling_file); get_parent_directory(abs_path);
        // join_path_components(abs_path, abs_path, location).
        Some(calling_file) => calling_file
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default(),
        // C: Assert(DataDir); join_path_components(abs_path, DataDir, location).
        None => PathBuf::from(data_dir::call().expect("DataDir must be set (C Assert(DataDir))")),
    };
    canonicalize_path_lexically(&base.join(location))
}

/// `GetConfFilesInDir(includedir, calling_file, elevel, &num, &err_msg)` —
/// return the `*.conf` files in `includedir`, sorted alphabetically (byte
/// order, mirroring `pg_qsort_strcmp`).
///
/// On a directory-access error, at/above `ERROR` the C `ereport(elevel)`
/// `longjmp`s (a hard `Err`); below it the C stores `*err_msg` and returns
/// `NULL`, which is the empty result carrying `err_msg`.
pub fn get_conf_files_in_dir(
    includedir: &str,
    calling_file: Option<&Path>,
    elevel: ErrorLevel,
) -> PgResult<ConfFilesInDir> {
    // C: reject an all-blank (including empty) name, which would otherwise read
    // the containing directory and recurse on the same file(s).
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

    // C: d = AllocateDir(directory); if (d == NULL) ereport + err_msg.
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

    // C: char **filenames grown in blocks of 32 via palloc/repalloc.
    let mut filenames: Vec<PathBuf> = Vec::new();

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                let pg_error = io_error(elevel, &error, "could not read configuration directory");
                return resolve(elevel, pg_error, "could not read configuration directory");
            }
        };

        // C: only names ending in ".conf"; reject names shorter than 6 bytes
        // (so a bare ".conf" with empty stem is excluded) and names starting
        // with '.' (covers "." / ".." / hidden / backup / editor debris).
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.len() < 6 || name.starts_with('.') || !name.ends_with(".conf") {
            continue;
        }

        // C: join_path_components(filename, directory, de->d_name);
        // canonicalize_path(filename); get_dirent_type(); skip dirs, error on
        // stat failure.
        let filename = canonicalize_path_lexically(&directory.join(name.as_ref()));
        match std::fs::metadata(&filename) {
            // C: PGFILETYPE_DIR -> not added.
            Ok(metadata) if metadata.is_dir() => {}
            // C: any other type -> add to array (fallible growth, so a huge
            // directory cannot abort the process on OOM).
            Ok(_) => {
                if filenames.try_reserve(1).is_err() {
                    return Err(out_of_memory("could not stat configuration files"));
                }
                filenames.push(filename);
            }
            // C: PGFILETYPE_ERROR (stat failed) -> pfree(filenames); err_msg.
            Err(error) => {
                let pg_error = io_error(
                    elevel,
                    &error,
                    format!("could not stat file \"{}\"", filename.display()),
                );
                return resolve(
                    elevel,
                    pg_error,
                    format!("could not stat file \"{}\"", filename.display()),
                );
            }
        }
    }

    // C: qsort(filenames, ..., pg_qsort_strcmp) — byte-wise ordering.
    filenames.sort_by(|left, right| {
        left.to_string_lossy()
            .as_bytes()
            .cmp(right.to_string_lossy().as_bytes())
    });

    Ok(ConfFilesInDir {
        filenames,
        err_msg: None,
    })
}

/// Mirror `ereport(elevel)` followed by `*err_msg = ...; return NULL`: at or
/// above `ERROR` the C `longjmp`s (hard `Err`); below it returns `NULL` with the
/// soft message (the empty result carrying `err_msg`).
fn resolve(
    elevel: ErrorLevel,
    error: PgError,
    err_msg: impl Into<String>,
) -> PgResult<ConfFilesInDir> {
    if elevel.0 >= ERROR.0 {
        Err(error)
    } else {
        Ok(ConfFilesInDir {
            filenames: Vec::new(),
            err_msg: Some(err_msg.into()),
        })
    }
}

/// Build a file-access `ereport` carrying the OS errno so
/// `errcode_for_file_access` and the `%m` substitution behave as in C.
fn io_error(elevel: ErrorLevel, error: &std::io::Error, message: impl Into<String>) -> PgError {
    let mut builder = ereport(elevel);
    if let Some(errno) = error.raw_os_error() {
        builder = builder.with_saved_errno(errno).errcode_for_file_access();
    }
    builder.errmsg(message).into_error()
}

fn out_of_memory(message: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg(message.to_owned())
        .into_error()
}

/// `is_absolute_path(location)` (`port/path.c`) for the POSIX target: a path is
/// absolute iff it begins with `/`.
fn is_absolute_path(location: &str) -> bool {
    location.starts_with('/')
}

/// Lexical equivalent of `canonicalize_path()`: collapse `.` and `..` and
/// duplicate separators without touching the filesystem (so it works on
/// not-yet-existing paths, exactly like the C string routine).
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
