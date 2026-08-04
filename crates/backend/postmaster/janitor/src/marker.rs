//! The adoption-guard marker file (spec D1 item 4).
//!
//! A one-line file in PGDATA recording the ACKNOWLEDGED value of
//! `pgrust.ephemeral_db_prefix`. It is what makes "newly configured prefix"
//! distinguishable from "normal restart with leftover ephemerals": with a
//! matching marker the startup sweep proceeds; without one (absent or a
//! different prefix) and with live non-template matches, the janitor starts
//! PAUSED. Written on `pgrust_janitor_unpause()` and on a first start where
//! nothing matches. This is the first pgrust-only PGDATA state file; it
//! follows the port's durable-small-state-file convention (write a temp
//! sibling, then `fd::durable_rename`, which fsyncs the file and the parent
//! directory — the replorigin/walsummarizer recipe), NOT the non-durable
//! postmaster.pid pattern.
//!
//! Paths are PGDATA-relative: the process cwd IS PGDATA (the postmaster
//! chdirs there and every backend is a thread of that one process —
//! miscinit/lockfile.rs documents the invariant).

use types_error::{PgError, PgResult, ERROR};

/// PGDATA-relative marker path. Contents: the acknowledged prefix, one
/// trailing newline.
pub const MARKER_FILE: &str = "pgrust_janitor.prefix";
const MARKER_TMP: &str = "pgrust_janitor.prefix.tmp";

/// The adoption-guard verdict for a configured prefix against the marker
/// content (`None` = file absent). Pure; unit-tested below.
#[derive(Debug, PartialEq, Eq)]
pub enum Guard {
    /// Marker present and records exactly this prefix: normal operation
    /// (a restart with leftover ephemerals is NOT a guard event).
    Acknowledged,
    /// Marker absent or recording a different prefix. Whether the janitor
    /// actually pauses additionally requires surviving non-template matches
    /// (main_loop.rs): with none, the janitor writes the marker and adopts.
    Unacknowledged { recorded: Option<String> },
}

pub fn decode(content: Option<&str>, prefix: &str) -> Guard {
    match content {
        Some(raw) => {
            // Exactly one trailing newline is the write format; tolerate a
            // missing one (hand-created file) but nothing fancier.
            let recorded = raw.strip_suffix('\n').unwrap_or(raw);
            if recorded == prefix {
                Guard::Acknowledged
            } else {
                Guard::Unacknowledged {
                    recorded: Some(recorded.to_string()),
                }
            }
        }
        None => Guard::Unacknowledged { recorded: None },
    }
}

/// A marker larger than this is not ours (the content is one prefix line;
/// a prefix is at most NAMEDATALEN-1 bytes).
const MARKER_MAX: usize = 4096;

fn file_error(op: &str, path: &str, errno: i32) -> Box<PgError> {
    let mut e = PgError::new(ERROR, format!("could not {op} \"{path}\""));
    e.saved_errno = Some(errno);
    Box::new(e)
}

/// Read the marker. `Ok(None)` = absent. Non-ENOENT I/O errors and non-UTF-8
/// content propagate: the guard must never be silently defeated by an
/// unreadable marker. All I/O rides the fd/vfd choke (determinism law),
/// never raw std::fs.
pub fn read() -> PgResult<Option<String>> {
    let raw = fd::OpenTransientFile(MARKER_FILE, libc::O_RDONLY)?;
    if raw < 0 {
        if fd::get_errno() == libc::ENOENT {
            return Ok(None);
        }
        return Err(file_error(
            "open adoption-guard marker",
            MARKER_FILE,
            fd::get_errno(),
        ));
    }
    let mut bytes: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        let n = fd::pg_pread(raw, &mut chunk, bytes.len() as i64);
        if n < 0 {
            let errno = fd::get_errno();
            fd::CloseTransientFile(raw);
            return Err(file_error("read adoption-guard marker", MARKER_FILE, errno));
        }
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..n as usize]);
        if bytes.len() > MARKER_MAX {
            fd::CloseTransientFile(raw);
            return Err(Box::new(PgError::new(
                ERROR,
                format!("adoption-guard marker \"{MARKER_FILE}\" is implausibly large"),
            )));
        }
    }
    fd::CloseTransientFile(raw);
    match String::from_utf8(bytes) {
        Ok(s) => Ok(Some(s)),
        Err(_) => Err(Box::new(PgError::new(
            ERROR,
            format!("adoption-guard marker \"{MARKER_FILE}\" is not valid UTF-8"),
        ))),
    }
}

/// Durably record `prefix` as acknowledged: temp sibling + durable_rename
/// (fsync file, fsync PGDATA). Must complete before any deferred sweep runs
/// (builtins.rs orders marker-write before clearing the pause flag). The
/// write recipe is the replorigin state-file shape: pg_unlink any stale
/// temp, OpenTransientFile(O_CREAT|O_EXCL|O_WRONLY), pg_pwrite, close,
/// durable_rename — fd/vfd choke throughout.
pub fn write(prefix: &str) -> PgResult<()> {
    if fd::pg_unlink(MARKER_TMP) < 0 && fd::get_errno() != libc::ENOENT {
        return Err(file_error(
            "remove stale marker temp file",
            MARKER_TMP,
            fd::get_errno(),
        ));
    }
    let raw = fd::OpenTransientFile(MARKER_TMP, libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY)?;
    if raw < 0 {
        return Err(file_error(
            "create marker temp file",
            MARKER_TMP,
            fd::get_errno(),
        ));
    }
    let content = format!("{prefix}\n");
    let n = fd::pg_pwrite(raw, content.as_bytes(), 0);
    if n != content.len() as isize {
        let errno = if fd::get_errno() == 0 {
            libc::ENOSPC
        } else {
            fd::get_errno()
        };
        fd::CloseTransientFile(raw);
        return Err(file_error("write marker temp file", MARKER_TMP, errno));
    }
    if fd::CloseTransientFile(raw) != 0 {
        return Err(file_error(
            "close marker temp file",
            MARKER_TMP,
            fd::get_errno(),
        ));
    }
    if fd::durable_rename(MARKER_TMP, MARKER_FILE, ERROR)? != 0 {
        // Only reachable below ERROR elevel; keep the invariant explicit.
        return Err(Box::new(PgError::new(
            ERROR,
            format!("could not durably rename \"{MARKER_TMP}\" to \"{MARKER_FILE}\""),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_marker_is_unacknowledged() {
        assert_eq!(
            decode(None, "tv_"),
            Guard::Unacknowledged { recorded: None }
        );
    }

    #[test]
    fn matching_marker_acknowledges() {
        assert_eq!(decode(Some("tv_\n"), "tv_"), Guard::Acknowledged);
        // Tolerated hand-written variant without the newline.
        assert_eq!(decode(Some("tv_"), "tv_"), Guard::Acknowledged);
    }

    #[test]
    fn different_prefix_is_unacknowledged_and_reports_it() {
        assert_eq!(
            decode(Some("prod\n"), "tv_"),
            Guard::Unacknowledged {
                recorded: Some("prod".to_string())
            }
        );
        // Prefix-of-prefix is still different: the guard compares exactly.
        assert_eq!(
            decode(Some("tv\n"), "tv_"),
            Guard::Unacknowledged {
                recorded: Some("tv".to_string())
            }
        );
        // Extra content beyond one trailing newline never sneaks past.
        assert_eq!(
            decode(Some("tv_\n\n"), "tv_"),
            Guard::Unacknowledged {
                recorded: Some("tv_\n".to_string())
            }
        );
    }
}
