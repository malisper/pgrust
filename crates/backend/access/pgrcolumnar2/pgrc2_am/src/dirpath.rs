//! O-7 directory path derivation + the vfs-only directory operations
//! (spec §12: the table directory `pgrc2_<relfilenumber>` is a SIBLING of
//! where the relation's main fork lives — tablespace and temp-namespace
//! semantics ride `relpath::GetRelationPath` unchanged).
//!
//! All I/O goes through the sanctioned `vfs::` choke (determinism-ledger
//! law: zero rows owed). Directory contents are FLAT by construction
//! (`pgrc2_format::dirlayout` names only); recursive trees never exist, so
//! removal is unlink-all-entries + rmdir.

use std::ffi::CString;

use pgrc2_format::dirlayout::table_dir_name;
use types_core::{ForkNumber, ProcNumber};
use types_error::{PgError, PgResult};
use types_storage::storage::RelFileLocator;

/// The per-table pgrcolumnar2 directory path for a relation's locator.
pub fn table_dir_path(locator: RelFileLocator, backend: ProcNumber) -> String {
    let main = relpath::GetRelationPath(locator, backend, ForkNumber::MAIN_FORKNUM);
    let dir_name = table_dir_name(locator.relNumber as u64);
    match main.rfind('/') {
        Some(i) => format!("{}/{}", &main[..i], dir_name),
        // A bare relative main path (global tablespace shapes still carry
        // "global/<rel>", so this arm is defensive only).
        None => dir_name,
    }
}

fn cpath(path: &str) -> PgResult<CString> {
    CString::new(path).map_err(|_| io_err("path contains NUL", path, 0))
}

fn io_err(what: &str, path: &str, errno: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("pgrcolumnar2: {what}: {path} (errno {errno})"))
            .with_sqlstate(types_error::ERRCODE_IO_ERROR),
    )
}

/// Does the directory exist? (stat; ENOENT → false).
pub fn dir_exists(dir: &str) -> PgResult<bool> {
    let c = cpath(dir)?;
    let mut info = vfs::FileInfo::zeroed();
    if vfs::stat(&c, &mut info) != 0 {
        let errno = vfs::get_errno();
        if errno == libc::ENOENT {
            return Ok(false);
        }
        return Err(io_err("stat failed", dir, errno));
    }
    Ok(info.is_dir())
}

/// Create the table directory if absent; returns true iff THIS call created
/// it. Durability: the parent directory is fsynced after a create, so the
/// dirent survives a crash (the publish path re-fsyncs the table dir for
/// its own contents).
pub fn mkdir_if_absent(dir: &str) -> PgResult<bool> {
    let c = cpath(dir)?;
    if vfs::mkdir(&c, 0o700) != 0 {
        let errno = vfs::get_errno();
        if errno == libc::EEXIST {
            return Ok(false);
        }
        return Err(io_err("mkdir failed", dir, errno));
    }
    fsync_parent(dir)?;
    Ok(true)
}

fn fsync_parent(dir: &str) -> PgResult<()> {
    let parent = match dir.rfind('/') {
        Some(i) if i > 0 => &dir[..i],
        _ => ".",
    };
    fsync_dir(parent)
}

fn fsync_dir(path: &str) -> PgResult<()> {
    let c = cpath(path)?;
    let fd = vfs::open(&c, libc::O_RDONLY | libc::O_CLOEXEC, 0);
    if fd < 0 {
        return Err(io_err("open for dir fsync failed", path, vfs::get_errno()));
    }
    let rc = vfs::fsync(fd);
    let errno = vfs::get_errno();
    vfs::close(fd);
    if rc != 0 {
        return Err(io_err("dir fsync failed", path, errno));
    }
    Ok(())
}

/// Remove the table directory and everything in it (flat by construction).
/// ENOENT anywhere is fine: deletion is idempotent (commit-time replay
/// after a partial removal must succeed — the pendingDeletes posture).
pub fn remove_table_dir(dir: &str) -> PgResult<()> {
    let c = cpath(dir)?;
    if !unlink_entries(dir, &c)? {
        return Ok(());
    }
    if vfs::rmdir(&c) != 0 {
        let errno = vfs::get_errno();
        if errno != libc::ENOENT {
            return Err(io_err("rmdir failed", dir, errno));
        }
    }
    fsync_parent(dir)
}

/// Remove every file INSIDE the directory, keeping the directory itself —
/// the nontransactional-truncate face (callers guarantee safety, as with
/// heap's `RelationTruncate`).
pub fn remove_dir_contents(dir: &str) -> PgResult<()> {
    let c = cpath(dir)?;
    if !unlink_entries(dir, &c)? {
        return Ok(());
    }
    fsync_dir(dir)
}

/// Unlink every entry of a flat directory. `Ok(false)` = the directory does
/// not exist (nothing to do); ENOENT on individual entries is tolerated.
fn unlink_entries(dir: &str, c: &CString) -> PgResult<bool> {
    let iter = match vfs::read_dir(c) {
        Ok(iter) => iter,
        Err(errno) => {
            if errno == libc::ENOENT {
                return Ok(false);
            }
            return Err(io_err("dir scan failed", dir, errno));
        }
    };
    for entry in iter {
        let name = match entry {
            Ok(n) => n,
            Err(errno) => return Err(io_err("dir scan entry failed", dir, errno)),
        };
        if name == "." || name == ".." {
            continue;
        }
        let p = cpath(&format!("{dir}/{name}"))?;
        if vfs::unlink(&p) != 0 {
            let errno = vfs::get_errno();
            if errno != libc::ENOENT {
                return Err(io_err("unlink failed", &format!("{dir}/{name}"), errno));
            }
        }
    }
    Ok(true)
}
