//! Owner of the `read_tz_file` seam (`port-path-seams`).
//!
//! This is the file-access half of `ParseTzFile` (`utils/misc/tzparser.c`): the
//! C bundle
//!
//! ```c
//! get_share_path(my_exec_path, share_path);
//! snprintf(file_path, ..., "%s/timezonesets/%s", share_path, filename);
//! tzFile = AllocateFile(file_path, "r");
//! if (!tzFile) { ...probe the directory with AllocateDir... }
//! while (!feof(tzFile)) { fgets(tzbuf, sizeof(tzbuf), tzFile); ... }
//! ```
//!
//! The caller (`tzparser::parse_tz_file`) has already enforced that `filename`
//! is all-alpha, so it cannot contain a path separator and cannot escape the
//! `timezonesets` directory. We resolve the install's `share` directory via
//! `get_share_path(my_exec_path)` (`common/path.c`, already installed) exactly
//! as `pg_tzdir` does for the tzdb, then read the file's lines.

use types_misc_more2::{TzFileOpenError, TzFileResult};

/// `MAXPGPATH` (`pg_config_manual.h`).
const MAXPGPATH: usize = 1024;

/// `sizeof(tzbuf)` in `ParseTzFile` — the per-line buffer is `char tzbuf[1024]`,
/// so a line of `sizeof(tzbuf) - 1 == 1023` bytes (excluding the terminating
/// NUL) is rejected as "too long".
const TZBUF_MAX: usize = 1024 - 1;

/// Read `my_exec_path` (a NUL-padded `[u8; MAXPGPATH]` global) as a string,
/// mirroring `backend_timezone_pgtz`'s `my_exec_path_str`.
fn my_exec_path_str() -> String {
    let buf = backend_utils_init_small::globals::my_exec_path();
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// `read_tz_file(filename)` — the file-access body of `ParseTzFile`.
///
/// `filename` is a bare, all-alpha name (validated by the caller). Returns the
/// file's lines (without trailing newlines), or a classified open/read failure
/// so the caller can reproduce the C diagnostic selection.
pub fn read_tz_file(filename: &str) -> TzFileResult {
    let my_exec_path = my_exec_path_str();
    // get_share_path(my_exec_path, share_path);
    let share_path = common_path_seams::get_share_path::call(&my_exec_path);

    // snprintf(file_path, ..., "%s/timezonesets/%s", share_path, filename);
    // (snprintf into a MAXPGPATH buffer silently truncates; mirror that.)
    let mut file_path = format!("{share_path}/timezonesets/{filename}");
    if file_path.len() >= MAXPGPATH {
        file_path.truncate(MAXPGPATH - 1);
    }

    // tzFile = AllocateFile(file_path, "r");
    // std::fs is inert on wasm64-unknown-unknown; read via the host VFS there.
    #[cfg(not(target_family = "wasm"))]
    let read_result = std::fs::read(&file_path);
    #[cfg(target_family = "wasm")]
    let read_result = wasm_libc_shim::fscompat::read(&file_path);
    let contents = match read_result {
        Ok(bytes) => bytes,
        Err(err) => {
            // The open failed. C now checks whether the *directory* itself is
            // the problem (an incomplete/unreadable installation), because that
            // is likely the first place a broken install is noticed.
            //
            //   snprintf(file_path, ..., "%s/timezonesets", share_path);
            //   tzdir = AllocateDir(file_path);
            //   if (tzdir == NULL) { errmsg("could not open directory \"%s\"...
            let mut dir_path = format!("{share_path}/timezonesets");
            if dir_path.len() >= MAXPGPATH {
                dir_path.truncate(MAXPGPATH - 1);
            }
            // AllocateDir == opendir; NULL == could not open the directory.
            #[cfg(not(target_family = "wasm"))]
            let dir_err = std::fs::read_dir(&dir_path).is_err();
            // wasm64: probe the directory via the host VFS (std::fs is inert).
            #[cfg(target_family = "wasm")]
            let dir_err = {
                use wasm_libc_shim::osfd::OsStrExt as _;
                wasm_libc_shim::osfile::WasmReadDir::open(
                    std::path::Path::new(&dir_path).as_os_str().as_bytes(),
                )
                .is_err()
            };
            if dir_err {
                return TzFileResult::Open(TzFileOpenError::DirectoryMissing {
                    // C: errmsg("could not open directory \"%s\": %m", file_path)
                    // — file_path here is the timezonesets directory path.
                    dir_error: dir_path,
                    // C: errhint("...the file \"%s\"...", my_exec_path).
                    exec_path: my_exec_path,
                });
            }
            // FreeDir(tzdir); errno = save_errno;
            //
            // Directory is fine; classify by the original open errno. C:
            //   if (errno != ENOENT || depth > 0)
            //       errmsg("could not read time zone file \"%s\": %m", filename);
            // The depth>0 distinction is the caller's; here we report ENOENT as
            // FileNotFound (the caller suppresses the message at depth 0) and
            // any other error as FileUnreadable.
            return if err.kind() == std::io::ErrorKind::NotFound {
                TzFileResult::Open(TzFileOpenError::FileNotFound)
            } else {
                TzFileResult::Open(TzFileOpenError::FileUnreadable {
                    file_error: filename.to_string(),
                })
            };
        }
    };

    // while (!feof(tzFile)) { fgets(tzbuf, sizeof(tzbuf), tzFile); ... }
    //
    // fgets reads up to sizeof(tzbuf)-1 bytes per line (newline-terminated);
    // strlen(tzbuf) == sizeof(tzbuf)-1 means the line was too long for tzbuf.
    // We split on '\n' and check each line's byte length against TZBUF_MAX.
    // A read error mid-file is the C ferror path; std::fs::read either fully
    // succeeded or returned Err above, so no separate ferror branch is reached.
    let mut lines = Vec::new();
    let mut lineno: i32 = 0;
    for raw_line in contents.split_inclusive(|&b| b == b'\n') {
        lineno += 1;
        // C compares against the buffer-full condition: a chunk that fills tzbuf
        // without a terminating newline (i.e. >= TZBUF_MAX bytes before any
        // newline). fgets would have stopped at TZBUF_MAX-1 chars.
        let had_newline = raw_line.last() == Some(&b'\n');
        let body: &[u8] = if had_newline {
            &raw_line[..raw_line.len() - 1]
        } else {
            raw_line
        };
        if !had_newline && body.len() >= TZBUF_MAX {
            return TzFileResult::LineTooLong { lineno };
        }
        // Drop a trailing '\r' so CRLF files parse like the C (which strips it
        // via the leading-whitespace skip + trailing tokenization). Keeping the
        // newline-trimmed line; the parser trims leading whitespace itself.
        let body = if body.last() == Some(&b'\r') {
            &body[..body.len() - 1]
        } else {
            body
        };
        lines.push(String::from_utf8_lossy(body).into_owned());
    }

    TzFileResult::Lines(lines)
}
