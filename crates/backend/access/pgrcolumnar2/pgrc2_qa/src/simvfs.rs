//! SimVfs — the fault-injecting [`WriteVfs`] behind the M3-K crash
//! batteries (chunk row: "SimVfs crash batteries (dirent-loss model) over
//! seal/publish/manifest").
//!
//! ## The durability model (a strict superset of MemVfs's)
//!
//! MemVfs (pgrc2_write) models a kill -9 as all-or-nothing at fsync grain:
//! un-fsynced file content and un-fsynced dirents vanish WHOLESALE. Real
//! filesystems are worse: writeback persists un-fsynced state in arbitrary
//! sector-granular pieces and directory ops in arbitrary subsets/orders.
//! SimVfs models exactly that adversary, deterministically from a seed:
//!
//! - **Content law**: `fsync_file` makes the file's current content durable
//!   (POSIX). Writes since the last fsync are recorded as a per-file op log
//!   (`Write{off,len}` / `Trunc`). At crash, each pending op survives
//!   PARTIALLY: every 512-byte sector a `Write` touched gets its own
//!   survival coin (the torn-sector model); a `Trunc` gets one coin. A
//!   sector persisted beyond the durable length implies length persistence
//!   with zero-fill of any gap (writeback extends files in page units —
//!   zeros are what the disk holds for the unwritten middle).
//! - **Dirent law**: namespace ops (create/rename/unlink) are recorded per
//!   parent directory and are volatile until `fsync_dir`. At crash, each
//!   pending op gets a survival coin and the survivors REPLAY IN ORDER
//!   against the durable dirent set (subset + implied reorder; an op whose
//!   precursor did not survive — a rename of a name that never persisted —
//!   is skipped). A rename is atomic even under crash (journal replay
//!   yields before-or-after, never both names).
//! - **Files with no surviving dirent are gone**; open fds die; an fsync'd
//!   file's content is NEVER torn (that is the definition of fsync).
//!
//! `crash_at_op(n)` + the op log mirror MemVfs exactly, so the batteries
//! sweep EVERY vfs-op boundary of a publish window and then explore S
//! adversarial persistence outcomes at each — boundary sweep × seed sweep.
//!
//! `Clone` is intentional: the sweep forks the universe at a boundary and
//! explores every (crash point × seed) from the same prefix.
//!
//! ## The WvFd seam workaround (A/D-lane report item)
//!
//! `pgrc2_write::wvfs::WvFd` has NO public constructor, so [`WriteVfs`] is
//! not externally implementable as frozen — an M3-K charter gap (the M3-D
//! crate doc promises "M3-K's crash batteries … compose under this writer
//! with no writer changes"). Workaround, no product edit: SimVfs mints its
//! `WvFd`s through a private shadow `MemVfs` (whose content is ignored) and
//! resolves fds BY PATH. That is sound because the writer's file protocol
//! opens each path sequentially (never two live fds on one path) — enforced
//! here with a loud `Contract` refusal, so any future violation is caught,
//! never silently mismodeled. Filed as a seam issue; the clean fix is a
//! `WvFd` constructor (or a sealed-factory method) in the A/D surface.

use pgrc2_write::wvfs::{MemVfs, WriteVfs, WvFd};
use pgrc2_write::{WriteError, WriteResult};
use std::collections::{BTreeMap, BTreeSet};

use crate::XorShift;

pub const SECTOR: usize = 512;

#[derive(Debug, Clone)]
enum FileOp {
    Write { off: u64, len: usize },
    Trunc,
}

#[derive(Debug, Default, Clone)]
struct SimFile {
    cur: Vec<u8>,
    durable: Vec<u8>,
    /// Content ops since the last fsync, in order.
    pending: Vec<FileOp>,
}

#[derive(Debug, Clone)]
enum NsOp {
    Link { name: String, ino: u64 },
    Unlink { name: String },
    Rename { from: String, to: String },
}

#[derive(Debug, Default, Clone)]
struct SimDir {
    cur: BTreeMap<String, u64>,
    durable: BTreeMap<String, u64>,
    /// Namespace ops since the last dir fsync, in order.
    pending: Vec<NsOp>,
}

/// The fault-injecting VFS. See the module doc for the model.
#[derive(Debug, Default)]
pub struct SimVfs {
    files: BTreeMap<u64, SimFile>,
    dirs: BTreeMap<String, SimDir>,
    /// Live fds, PATH-keyed (see the WvFd seam workaround note).
    open: BTreeMap<String, u64>,
    next_ino: u64,
    ops: Vec<String>,
    op_count: u64,
    crash_at: Option<u64>,
    killed: bool,
    /// WvFd mint (content ignored; never crashes; not cloned).
    shadow: MemVfs,
}

impl Clone for SimVfs {
    fn clone(&self) -> SimVfs {
        SimVfs {
            files: self.files.clone(),
            dirs: self.dirs.clone(),
            open: self.open.clone(),
            next_ino: self.next_ino,
            ops: self.ops.clone(),
            op_count: self.op_count,
            crash_at: self.crash_at,
            killed: self.killed,
            shadow: MemVfs::new(),
        }
    }
}

fn split_path(path: &str) -> WriteResult<(&str, &str)> {
    match path.rsplit_once('/') {
        Some((dir, name)) if !dir.is_empty() && !name.is_empty() => Ok((dir, name)),
        _ => Err(WriteError::Contract {
            detail: "SimVfs paths are dir/name shaped",
        }),
    }
}

impl SimVfs {
    pub fn new() -> SimVfs {
        SimVfs::default()
    }

    /// The op log (readable lines, in op order) — the ordering witness.
    pub fn ops(&self) -> &[String] {
        &self.ops
    }

    pub fn op_count(&self) -> u64 {
        self.op_count
    }

    /// Arm a crash BEFORE the `n`th subsequent op applies (1-based; the
    /// MemVfs convention).
    pub fn crash_at_op(&mut self, n: u64) {
        self.crash_at = Some(self.op_count + n);
    }

    pub fn killed(&self) -> bool {
        self.killed
    }

    /// Mint a WvFd for `path` through the shadow MemVfs (see module doc).
    fn mint_fd(&mut self, path: &str) -> WriteResult<WvFd> {
        let (dir, _) = split_path(path)?;
        self.shadow.mkdir_path(dir)?;
        self.shadow.create_rw(path)
    }

    /// kill -9 with the ADVERSARIAL persistence outcome drawn from `rng`:
    /// pending namespace ops survive per-op, pending content ops survive
    /// per-sector; everything fsync'd is exactly durable. Revives the
    /// universe for recovery.
    pub fn crash_and_revive(&mut self, rng: &mut XorShift) {
        // --- dirent law: per-op coins, replayed in order -------------------
        for d in self.dirs.values_mut() {
            let mut durable = std::mem::take(&mut d.durable);
            for op in d.pending.drain(..) {
                if !rng.coin() {
                    continue;
                }
                match op {
                    NsOp::Link { name, ino } => {
                        durable.insert(name, ino);
                    }
                    NsOp::Unlink { name } => {
                        durable.remove(&name);
                    }
                    NsOp::Rename { from, to } => {
                        // Atomic: applies only if the source name persisted.
                        if let Some(ino) = durable.remove(&from) {
                            durable.insert(to, ino);
                        }
                    }
                }
            }
            d.durable = durable;
            d.cur = d.durable.clone();
        }
        // --- content law: per-sector coins over the pending op log ---------
        let live: BTreeSet<u64> = self
            .dirs
            .values()
            .flat_map(|d| d.cur.values().copied())
            .collect();
        self.files.retain(|ino, _| live.contains(ino));
        for f in self.files.values_mut() {
            let mut base = std::mem::take(&mut f.durable);
            for op in f.pending.drain(..) {
                match op {
                    FileOp::Trunc => {
                        if rng.coin() {
                            base.clear();
                        }
                    }
                    FileOp::Write { off, len } => {
                        let mut s = off as usize;
                        let end = off as usize + len;
                        while s < end {
                            let sec_end = ((s / SECTOR) + 1) * SECTOR;
                            let e = sec_end.min(end);
                            if rng.coin() {
                                // This sector's bytes hit disk. Length
                                // extension zero-fills any gap.
                                if base.len() < e {
                                    base.resize(e, 0);
                                }
                                if f.cur.len() >= e {
                                    base[s..e].copy_from_slice(&f.cur[s..e]);
                                }
                            }
                            s = e;
                        }
                    }
                }
            }
            f.durable = base;
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

    fn resolve_fd(&self, fd: &WvFd, op: &'static str) -> WriteResult<u64> {
        self.open.get(fd.path()).copied().ok_or(WriteError::Io {
            op,
            path: fd.path().to_string(),
            errno: libc::EBADF,
        })
    }

    fn register_open(&mut self, path: &str, ino: u64) -> WriteResult<()> {
        if self.open.contains_key(path) {
            // The path-keyed fd model's guard (see module doc): the writer
            // protocol never holds two live fds on one path.
            return Err(WriteError::Contract {
                detail: "SimVfs: second live fd on one path",
            });
        }
        self.open.insert(path.to_string(), ino);
        Ok(())
    }

    /// Snapshot every file in `dir` (name → current content) WITHOUT
    /// ticking the op log — checker plumbing, not simulated I/O.
    pub fn snapshot_dir(&self, dir: &str) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        if let Some(d) = self.dirs.get(dir) {
            for (name, ino) in &d.cur {
                if let Some(f) = self.files.get(ino) {
                    out.insert(name.clone(), f.cur.clone());
                }
            }
        }
        out
    }
}

impl WriteVfs for SimVfs {
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
                // O_TRUNC on an existing binding: a content op.
                let f = self.files.get_mut(&ino).expect("live ino");
                f.cur.clear();
                f.pending.push(FileOp::Trunc);
                ino
            }
            None => {
                self.next_ino += 1;
                let ino = self.next_ino;
                d.cur.insert(name.to_string(), ino);
                d.pending.push(NsOp::Link {
                    name: name.to_string(),
                    ino,
                });
                self.files.insert(ino, SimFile::default());
                ino
            }
        };
        self.register_open(path, ino)?;
        self.mint_fd(path)
    }

    fn open_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        self.tick(format!("open {path}"))?;
        let ino = self.ino_of(path)?.ok_or(WriteError::Io {
            op: "open_rw",
            path: path.to_string(),
            errno: libc::ENOENT,
        })?;
        self.register_open(path, ino)?;
        self.mint_fd(path)
    }

    fn pwrite_at(&mut self, fd: &WvFd, off: u64, bytes: &[u8]) -> WriteResult<()> {
        self.tick(format!(
            "pwrite {} off={} len={}",
            fd.path(),
            off,
            bytes.len()
        ))?;
        let ino = self.resolve_fd(fd, "pwrite")?;
        let f = self.files.get_mut(&ino).expect("open ino live");
        let end = off as usize + bytes.len();
        if f.cur.len() < end {
            f.cur.resize(end, 0);
        }
        f.cur[off as usize..end].copy_from_slice(bytes);
        f.pending.push(FileOp::Write {
            off,
            len: bytes.len(),
        });
        Ok(())
    }

    fn fsync_file(&mut self, fd: &WvFd) -> WriteResult<()> {
        self.tick(format!("fsync {}", fd.path()))?;
        let ino = self.resolve_fd(fd, "fsync")?;
        let f = self.files.get_mut(&ino).expect("open ino live");
        f.durable = f.cur.clone();
        f.pending.clear();
        Ok(())
    }

    fn close_file(&mut self, fd: WvFd) -> WriteResult<()> {
        self.tick(format!("close {}", fd.path()))?;
        self.open.remove(fd.path());
        Ok(())
    }

    fn read_full(&mut self, path: &str) -> WriteResult<Vec<u8>> {
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
        if fdir != tdir {
            return Err(WriteError::Contract {
                detail: "SimVfs models same-directory renames only",
            });
        }
        let d = self.dirs.get_mut(fdir).ok_or(WriteError::Io {
            op: "rename",
            path: from.to_string(),
            errno: libc::ENOENT,
        })?;
        let ino = d.cur.remove(fname).ok_or(WriteError::Io {
            op: "rename",
            path: from.to_string(),
            errno: libc::ENOENT,
        })?;
        d.cur.insert(tname.to_string(), ino);
        d.pending.push(NsOp::Rename {
            from: fname.to_string(),
            to: tname.to_string(),
        });
        Ok(())
    }

    fn unlink_path(&mut self, path: &str) -> WriteResult<()> {
        self.tick(format!("unlink {path}"))?;
        let (dir, name) = split_path(path)?;
        let d = self.dirs.get_mut(dir).ok_or(WriteError::Io {
            op: "unlink",
            path: path.to_string(),
            errno: libc::ENOENT,
        })?;
        if d.cur.remove(name).is_none() {
            return Err(WriteError::Io {
                op: "unlink",
                path: path.to_string(),
                errno: libc::ENOENT,
            });
        }
        d.pending.push(NsOp::Unlink {
            name: name.to_string(),
        });
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
        d.pending.clear();
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
