//! The writer's file-system seam.
//!
//! Every byte this crate persists goes through [`WriteVfs`]:
//!
//! - [`RealVfs`] calls the sanctioned `vfs::` free-function shims (the
//!   determinism-lint choke point — this crate owes zero ledger rows). Under
//!   `--cfg pgrust_sim` those shims dispatch to the tree's SimVfs, so M3-K's
//!   crash batteries (sector tearing, seeded dirent subsets) compose under
//!   this writer with no writer changes.
//! - [`MemVfs`] is the in-crate deterministic model used by this crate's own
//!   kill-9-shaped crash matrix in DEFAULT CI (no cfg): per-file
//!   volatile/durable content, per-directory volatile/durable dirents
//!   (namespace ops are volatile until the parent dir is fsync'd — the
//!   dirent-loss model), an op log for ordering witnesses, and a
//!   crash-at-op-N injector. [`MemVfs::crash_and_revive`] drops everything
//!   not durable, exactly a `kill -9` at an op boundary.
//!
//! Method names deliberately avoid the determinism lint's name-based
//! patterns (`sync_all`/`sync_data`/`metadata`/`write_all_at`): the lint
//! flags those on ANY receiver.

use crate::{WriteError, WriteResult};
use std::collections::BTreeMap;
use std::ffi::CString;

/// Opaque file handle: `raw` is the vfs-minted descriptor (RealVfs), `key`
/// the MemVfs open-table key; `path` rides for error reporting + op logs.
#[derive(Debug)]
pub struct WvFd {
    raw: i32,
    key: u64,
    path: String,
}

impl WvFd {
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// The writer-side Vfs contract. `&mut self` throughout: the writer is
/// single-threaded per table (R1–R6 worker confinement), so no interior
/// mutability and no sync types are needed.
pub trait WriteVfs {
    /// Create-or-truncate `path` for read/write.
    fn create_rw(&mut self, path: &str) -> WriteResult<WvFd>;
    /// Open an EXISTING file for read/write (publish-time part_no patch).
    fn open_rw(&mut self, path: &str) -> WriteResult<WvFd>;
    fn pwrite_at(&mut self, fd: &WvFd, off: u64, bytes: &[u8]) -> WriteResult<()>;
    fn fsync_file(&mut self, fd: &WvFd) -> WriteResult<()>;
    fn close_file(&mut self, fd: WvFd) -> WriteResult<()>;
    fn read_full(&mut self, path: &str) -> WriteResult<Vec<u8>>;
    fn rename_path(&mut self, from: &str, to: &str) -> WriteResult<()>;
    fn unlink_path(&mut self, path: &str) -> WriteResult<()>;
    fn fsync_dir(&mut self, dir: &str) -> WriteResult<()>;
    /// Deterministic (sorted) names, no "." / "..".
    fn list_dir(&mut self, dir: &str) -> WriteResult<Vec<String>>;
    fn mkdir_path(&mut self, dir: &str) -> WriteResult<()>;
    fn exists_path(&mut self, path: &str) -> WriteResult<bool>;
}

// ---------------------------------------------------------------------------
// RealVfs — the production impl over the vfs:: shims
// ---------------------------------------------------------------------------

/// Production impl. ZST; all state is the kernel's (or SimVfs's under
/// `--cfg pgrust_sim`).
#[derive(Debug, Default, Clone, Copy)]
pub struct RealVfs;

fn cpath(path: &str, op: &'static str) -> WriteResult<CString> {
    CString::new(path).map_err(|_| WriteError::Io {
        op,
        path: path.to_string(),
        errno: 0,
    })
}

fn io_err(op: &'static str, path: &str) -> WriteError {
    WriteError::Io {
        op,
        path: path.to_string(),
        errno: vfs::get_errno(),
    }
}

impl RealVfs {
    fn open_flags(&mut self, path: &str, flags: libc::c_int, op: &'static str) -> WriteResult<WvFd> {
        let c = cpath(path, op)?;
        let raw = vfs::open(&c, flags, 0o600 as libc::mode_t);
        if raw < 0 {
            return Err(io_err(op, path));
        }
        Ok(WvFd {
            raw,
            key: 0,
            path: path.to_string(),
        })
    }
}

impl WriteVfs for RealVfs {
    fn create_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        self.open_flags(
            path,
            libc::O_CREAT | libc::O_RDWR | libc::O_TRUNC,
            "create_rw",
        )
    }

    fn open_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        self.open_flags(path, libc::O_RDWR, "open_rw")
    }

    fn pwrite_at(&mut self, fd: &WvFd, off: u64, bytes: &[u8]) -> WriteResult<()> {
        let mut done = 0usize;
        while done < bytes.len() {
            let n = vfs::pwrite(fd.raw, &bytes[done..], (off + done as u64) as libc::off_t);
            if n < 0 {
                if vfs::get_errno() == libc::EINTR {
                    continue;
                }
                return Err(io_err("pwrite", &fd.path));
            }
            if n == 0 {
                return Err(io_err("pwrite(short)", &fd.path));
            }
            done += n as usize;
        }
        // GL-BULKWRITE-1 (#686): a sealed part is up to 256 MiB of dirty
        // page cache in one call; account it against the dirty-page
        // governor (no-op unless armed). WHAT is written is untouched —
        // parts stay byte-identical; only writeback timing changes.
        write_governor::note_write(fd.raw, off, bytes.len() as u64);
        Ok(())
    }

    fn fsync_file(&mut self, fd: &WvFd) -> WriteResult<()> {
        // EINTR retry (the cbstore segfile posture).
        while vfs::fsync(fd.raw) < 0 {
            if vfs::get_errno() != libc::EINTR {
                return Err(io_err("fsync", &fd.path));
            }
        }
        Ok(())
    }

    fn close_file(&mut self, fd: WvFd) -> WriteResult<()> {
        // GL-BULKWRITE-1 (#686): kick + forget any pending governor window
        // before the descriptor number can be reused.
        write_governor::note_close(fd.raw);
        if vfs::close(fd.raw) < 0 {
            return Err(io_err("close", &fd.path));
        }
        Ok(())
    }

    fn read_full(&mut self, path: &str) -> WriteResult<Vec<u8>> {
        let fd = self.open_flags(path, libc::O_RDONLY, "open_ro")?;
        let mut out = Vec::new();
        let mut buf = [0u8; 64 * 1024];
        let mut off: u64 = 0;
        loop {
            let n = vfs::pread(fd.raw, &mut buf, off as libc::off_t);
            if n < 0 {
                if vfs::get_errno() == libc::EINTR {
                    continue;
                }
                let e = io_err("pread", path);
                let _ = self.close_file(fd);
                return Err(e);
            }
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n as usize]);
            off += n as u64;
        }
        self.close_file(fd)?;
        Ok(out)
    }

    fn rename_path(&mut self, from: &str, to: &str) -> WriteResult<()> {
        let cf = cpath(from, "rename")?;
        let ct = cpath(to, "rename")?;
        if vfs::rename(&cf, &ct) < 0 {
            return Err(io_err("rename", from));
        }
        Ok(())
    }

    fn unlink_path(&mut self, path: &str) -> WriteResult<()> {
        let c = cpath(path, "unlink")?;
        if vfs::unlink(&c) < 0 {
            return Err(io_err("unlink", path));
        }
        Ok(())
    }

    fn fsync_dir(&mut self, dir: &str) -> WriteResult<()> {
        let c = cpath(dir, "fsync_dir")?;
        let raw = vfs::open(&c, libc::O_RDONLY, 0);
        if raw < 0 {
            return Err(io_err("fsync_dir(open)", dir));
        }
        while vfs::fsync(raw) < 0 {
            let e = vfs::get_errno();
            if e == libc::EINTR {
                continue;
            }
            // Some filesystems refuse dir fsync; tolerate exactly the
            // fd.c/fsync_fname_ext posture (EBADF/EINVAL non-fatal).
            if e == libc::EBADF || e == libc::EINVAL {
                break;
            }
            vfs::close(raw);
            return Err(WriteError::Io {
                op: "fsync_dir",
                path: dir.to_string(),
                errno: e,
            });
        }
        if vfs::close(raw) < 0 {
            return Err(io_err("fsync_dir(close)", dir));
        }
        Ok(())
    }

    fn list_dir(&mut self, dir: &str) -> WriteResult<Vec<String>> {
        let c = cpath(dir, "list_dir")?;
        let iter = vfs::read_dir(&c).map_err(|errno| WriteError::Io {
            op: "list_dir",
            path: dir.to_string(),
            errno,
        })?;
        let mut names: Vec<String> = Vec::new();
        for entry in iter {
            names.push(entry.map_err(|errno| WriteError::Io {
                op: "list_dir(entry)",
                path: dir.to_string(),
                errno,
            })?);
        }
        // PosixVfs order is directory order; sort for determinism (SimVfs is
        // already BTree-ordered — sorting is idempotent there).
        names.sort();
        Ok(names)
    }

    fn mkdir_path(&mut self, dir: &str) -> WriteResult<()> {
        let c = cpath(dir, "mkdir")?;
        if vfs::mkdir(&c, 0o700 as libc::mode_t) < 0 {
            return Err(io_err("mkdir", dir));
        }
        Ok(())
    }

    fn exists_path(&mut self, path: &str) -> WriteResult<bool> {
        let c = cpath(path, "stat")?;
        let mut info = vfs::FileInfo::default();
        if vfs::stat(&c, &mut info) == 0 {
            return Ok(true);
        }
        if vfs::get_errno() == libc::ENOENT {
            return Ok(false);
        }
        Err(io_err("stat", path))
    }
}

// ---------------------------------------------------------------------------
// MemVfs — the deterministic crash-model impl (default-CI test vehicle)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct MemFile {
    cur: Vec<u8>,
    durable: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
struct MemDir {
    /// name → inode.
    cur: BTreeMap<String, u64>,
    durable: BTreeMap<String, u64>,
}

/// Deterministic in-memory file system with a kill-9-shaped durability
/// model:
///
/// - file WRITES are volatile until `fsync_file` (content law);
/// - namespace ops (create/rename/unlink dirents) are volatile until the
///   parent directory's `fsync_dir` (dirent law — the SimVfs rule-3 model);
/// - [`MemVfs::crash_and_revive`] discards every volatile state: dirents
///   revert to the durable set, surviving files revert to durable content,
///   files with no durable dirent vanish, open fds die;
/// - `crash_at_op(n)` kills the universe BEFORE the nth subsequent op
///   applies — the crash-matrix injector (every op boundary is a kill -9
///   point);
/// - every op appends a readable line to [`MemVfs::ops`] — the ordering
///   witness the publish-ordering tests string-match (the sim_dirsync
///   op-trace pattern).
#[derive(Debug, Default)]
pub struct MemVfs {
    files: BTreeMap<u64, MemFile>,
    dirs: BTreeMap<String, MemDir>,
    open: BTreeMap<u64, u64>,
    next_ino: u64,
    next_fd: u64,
    ops: Vec<String>,
    op_count: u64,
    crash_at: Option<u64>,
    killed: bool,
}

fn split_path(path: &str) -> WriteResult<(&str, &str)> {
    match path.rsplit_once('/') {
        Some((dir, name)) if !dir.is_empty() && !name.is_empty() => Ok((dir, name)),
        _ => Err(WriteError::Contract {
            detail: "MemVfs paths are dir/name shaped",
        }),
    }
}

impl MemVfs {
    pub fn new() -> MemVfs {
        MemVfs::default()
    }

    /// The op log (readable lines, in op order).
    pub fn ops(&self) -> &[String] {
        &self.ops
    }

    pub fn op_count(&self) -> u64 {
        self.op_count
    }

    /// Arm a crash BEFORE the `n`th subsequent op applies (1-based: `1`
    /// kills the very next op).
    pub fn crash_at_op(&mut self, n: u64) {
        self.crash_at = Some(self.op_count + n);
    }

    pub fn killed(&self) -> bool {
        self.killed
    }

    /// kill -9 at this boundary: drop ALL volatile state, revive the
    /// universe for recovery.
    pub fn crash_and_revive(&mut self) {
        for d in self.dirs.values_mut() {
            d.cur = d.durable.clone();
        }
        let live: std::collections::BTreeSet<u64> = self
            .dirs
            .values()
            .flat_map(|d| d.cur.values().copied())
            .collect();
        self.files.retain(|ino, _| live.contains(ino));
        for f in self.files.values_mut() {
            f.cur = f.durable.clone();
        }
        self.open.clear();
        self.killed = false;
        self.crash_at = None;
        self.ops.push("CRASH".to_string());
    }

    fn tick(&mut self, line: String) -> WriteResult<()> {
        if self.killed {
            return Err(WriteError::Io {
                op: "killed",
                path: line,
                errno: 0,
            });
        }
        self.op_count += 1;
        if self.crash_at == Some(self.op_count) {
            self.killed = true;
            return Err(WriteError::Io {
                op: "crash",
                path: line,
                errno: 0,
            });
        }
        self.ops.push(line);
        Ok(())
    }

    fn ino_of(&self, path: &str) -> WriteResult<Option<u64>> {
        let (dir, name) = split_path(path)?;
        Ok(self.dirs.get(dir).and_then(|d| d.cur.get(name)).copied())
    }
}

impl WriteVfs for MemVfs {
    fn create_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        self.tick(format!("create {path}"))?;
        let (dir, name) = split_path(path)?;
        let d = self.dirs.get_mut(dir).ok_or(WriteError::Io {
            op: "create_rw",
            path: path.to_string(),
            errno: libc::ENOENT,
        })?;
        let ino = match d.cur.get(name) {
            Some(&ino) => {
                // O_TRUNC semantics.
                self.files.get_mut(&ino).expect("live ino").cur.clear();
                ino
            }
            None => {
                self.next_ino += 1;
                let ino = self.next_ino;
                d.cur.insert(name.to_string(), ino);
                self.files.insert(ino, MemFile::default());
                ino
            }
        };
        self.next_fd += 1;
        self.open.insert(self.next_fd, ino);
        Ok(WvFd {
            raw: -1,
            key: self.next_fd,
            path: path.to_string(),
        })
    }

    fn open_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        self.tick(format!("open {path}"))?;
        let ino = self.ino_of(path)?.ok_or(WriteError::Io {
            op: "open_rw",
            path: path.to_string(),
            errno: libc::ENOENT,
        })?;
        self.next_fd += 1;
        self.open.insert(self.next_fd, ino);
        Ok(WvFd {
            raw: -1,
            key: self.next_fd,
            path: path.to_string(),
        })
    }

    fn pwrite_at(&mut self, fd: &WvFd, off: u64, bytes: &[u8]) -> WriteResult<()> {
        self.tick(format!("pwrite {} off={} len={}", fd.path, off, bytes.len()))?;
        let ino = *self.open.get(&fd.key).ok_or(WriteError::Io {
            op: "pwrite",
            path: fd.path.clone(),
            errno: libc::EBADF,
        })?;
        let f = self.files.get_mut(&ino).expect("open ino live");
        let end = off as usize + bytes.len();
        if f.cur.len() < end {
            f.cur.resize(end, 0);
        }
        f.cur[off as usize..end].copy_from_slice(bytes);
        Ok(())
    }

    fn fsync_file(&mut self, fd: &WvFd) -> WriteResult<()> {
        self.tick(format!("fsync {}", fd.path))?;
        let ino = *self.open.get(&fd.key).ok_or(WriteError::Io {
            op: "fsync",
            path: fd.path.clone(),
            errno: libc::EBADF,
        })?;
        let f = self.files.get_mut(&ino).expect("open ino live");
        f.durable = f.cur.clone();
        Ok(())
    }

    fn close_file(&mut self, fd: WvFd) -> WriteResult<()> {
        self.tick(format!("close {}", fd.path))?;
        self.open.remove(&fd.key);
        Ok(())
    }

    fn read_full(&mut self, path: &str) -> WriteResult<Vec<u8>> {
        // Reads are not durability ops; still logged for completeness.
        self.tick(format!("read {path}"))?;
        let ino = self.ino_of(path)?.ok_or(WriteError::Io {
            op: "read_full",
            path: path.to_string(),
            errno: libc::ENOENT,
        })?;
        Ok(self.files.get(&ino).expect("live ino").cur.clone())
    }

    fn rename_path(&mut self, from: &str, to: &str) -> WriteResult<()> {
        self.tick(format!("rename {from} -> {to}"))?;
        let (fdir, fname) = split_path(from)?;
        let (tdir, tname) = split_path(to)?;
        let ino = self
            .dirs
            .get_mut(fdir)
            .and_then(|d| d.cur.remove(fname))
            .ok_or(WriteError::Io {
                op: "rename",
                path: from.to_string(),
                errno: libc::ENOENT,
            })?;
        let td = self.dirs.get_mut(tdir).ok_or(WriteError::Io {
            op: "rename",
            path: to.to_string(),
            errno: libc::ENOENT,
        })?;
        td.cur.insert(tname.to_string(), ino);
        Ok(())
    }

    fn unlink_path(&mut self, path: &str) -> WriteResult<()> {
        self.tick(format!("unlink {path}"))?;
        let (dir, name) = split_path(path)?;
        let removed = self.dirs.get_mut(dir).and_then(|d| d.cur.remove(name));
        if removed.is_none() {
            return Err(WriteError::Io {
                op: "unlink",
                path: path.to_string(),
                errno: libc::ENOENT,
            });
        }
        Ok(())
    }

    fn fsync_dir(&mut self, dir: &str) -> WriteResult<()> {
        self.tick(format!("fsyncdir {dir}"))?;
        let d = self.dirs.get_mut(dir).ok_or(WriteError::Io {
            op: "fsync_dir",
            path: dir.to_string(),
            errno: libc::ENOENT,
        })?;
        d.durable = d.cur.clone();
        Ok(())
    }

    fn list_dir(&mut self, dir: &str) -> WriteResult<Vec<String>> {
        self.tick(format!("listdir {dir}"))?;
        let d = self.dirs.get(dir).ok_or(WriteError::Io {
            op: "list_dir",
            path: dir.to_string(),
            errno: libc::ENOENT,
        })?;
        Ok(d.cur.keys().cloned().collect())
    }

    fn mkdir_path(&mut self, dir: &str) -> WriteResult<()> {
        self.tick(format!("mkdir {dir}"))?;
        self.dirs.entry(dir.to_string()).or_default();
        Ok(())
    }

    fn exists_path(&mut self, path: &str) -> WriteResult<bool> {
        self.tick(format!("stat {path}"))?;
        Ok(self.ino_of(path)?.is_some())
    }
}
