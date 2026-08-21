//! The reader's I/O seam: positioned reads over part files and whole-file
//! reads over table-directory files, all through the `vfs` choke (the
//! sanctioned file surface — SimVfs-composable for M3-K's crash batteries,
//! zero determinism-ledger rows).
//!
//! [`PartIo`] exists so the open path, fault accounting, and every test can
//! run over injected in-memory parts ([`MemPartIo`]) with controlled
//! identity facts; [`VfsPartIo`] is the production implementation.

use core::ffi::CStr;
use std::ffi::CString;

use pgrc2_format::FormatError;

use crate::{ReadError, ReadResult};

/// Positioned-read access to one (immutable, sealed) part file plus the
/// physical identity facts spec §11 keys on. Implementations are read-only
/// and internally immutable; `&self` reads are safe from any thread.
pub trait PartIo: Send + Sync {
    /// File length in bytes (identity word 3).
    fn len(&self) -> u64;
    /// `(st_dev, st_ino)` (identity words 1–2). In-memory implementations
    /// return synthetic facts — tests use them to model copies and reuse.
    fn dev_ino(&self) -> (u64, u64);
    /// Fill `buf` from `off`, exactly. Short data (EOF inside the range) is
    /// a typed `Truncated` at `at`; an I/O failure is `ReadError::Io`.
    fn pread_exact(&self, off: u64, buf: &mut [u8], at: &'static str) -> ReadResult<()>;
    /// Advisory readahead hint over `[off, off+len)` — MAY no-op, never
    /// fails, never changes what a later read returns (the cold-readahead
    /// claim-hook face; C3 inherit). Default no-op keeps every in-memory
    /// implementation honest by construction.
    fn advise_willneed(&self, _off: u64, _len: u64) {}
}

// ---------------------------------------------------------------------------
// production: vfs-backed positioned reads
// ---------------------------------------------------------------------------

/// Production [`PartIo`] over a vfs file descriptor (O_RDONLY). Identity
/// facts are gathered by ONE fstat at open, before any content read — the
/// stat-before-read discipline: a concurrent replace makes lookups fail
/// spurious-miss, never stale-hit (spec §11).
pub struct VfsPartIo {
    fd: libc::c_int,
    dev: u64,
    ino: u64,
    len: u64,
}

impl VfsPartIo {
    /// Open `path` read-only and gather identity facts.
    pub fn open(path: &CStr) -> ReadResult<VfsPartIo> {
        let fd = vfs::open(path, libc::O_RDONLY | libc::O_CLOEXEC, 0);
        if fd < 0 {
            return Err(ReadError::Io {
                at: "part open",
                errno: vfs::get_errno(),
            });
        }
        let mut info = vfs::FileInfo::zeroed();
        if vfs::fstat(fd, &mut info) != 0 {
            let errno = vfs::get_errno();
            vfs::close(fd);
            return Err(ReadError::Io {
                at: "part fstat",
                errno,
            });
        }
        Ok(VfsPartIo {
            fd,
            dev: info.dev,
            ino: info.ino,
            len: info.size.max(0) as u64,
        })
    }
}

impl Drop for VfsPartIo {
    fn drop(&mut self) {
        vfs::close(self.fd);
    }
}

impl PartIo for VfsPartIo {
    fn len(&self) -> u64 {
        self.len
    }

    fn dev_ino(&self) -> (u64, u64) {
        (self.dev, self.ino)
    }

    fn advise_willneed(&self, off: u64, len: u64) {
        // Advisory through the vfs choke (posix_fadvise WILLNEED on Linux,
        // F_RDADVISE on macOS, SimVfs benign-gated). Errors are ignored —
        // a failed hint costs speed, never rows.
        if len == 0 || off > libc::off_t::MAX as u64 || len > libc::off_t::MAX as u64 {
            return;
        }
        let _ = vfs::fadvise_willneed(self.fd, off as libc::off_t, len as libc::off_t);
    }

    fn pread_exact(&self, off: u64, buf: &mut [u8], at: &'static str) -> ReadResult<()> {
        // [coldopen] Global demand-read census (cache-independent — counts
        // syscalls, not misses): the open/first-touch attribution currency.
        PREAD_CALLS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        PREAD_BYTES.fetch_add(buf.len() as u64, core::sync::atomic::Ordering::Relaxed);
        let mut done = 0usize;
        while done < buf.len() {
            let n = vfs::pread(
                self.fd,
                &mut buf[done..],
                (off + done as u64) as libc::off_t,
            );
            if n < 0 {
                let errno = vfs::get_errno();
                if errno == libc::EINTR {
                    continue;
                }
                return Err(ReadError::Io { at, errno });
            }
            if n == 0 {
                // EOF inside the requested range: the file is shorter than
                // the structure claims — truncation, typed.
                return Err(ReadError::Format(FormatError::Truncated { at }));
            }
            done += n as usize;
        }
        Ok(())
    }
}

/// [coldopen] Process-global positioned-read census over the production
/// VfsPartIo (instrumentation, never state): syscall count + requested
/// bytes. Cache-independent — the honest currency for attributing a cold
/// first-statement's demand reads (COLDFACE/open-probe consumers read the
/// deltas).
pub static PREAD_CALLS: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);
pub static PREAD_BYTES: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);

/// Snapshot the read census as (calls, bytes).
pub fn pread_census() -> (u64, u64) {
    (
        PREAD_CALLS.load(core::sync::atomic::Ordering::Relaxed),
        PREAD_BYTES.load(core::sync::atomic::Ordering::Relaxed),
    )
}

// ---------------------------------------------------------------------------
// tests/tools: in-memory parts with controlled identity
// ---------------------------------------------------------------------------

/// In-memory [`PartIo`]: the byte image plus synthetic identity facts.
/// Two instances over the same bytes with equal `(dev, ino)` model the same
/// physical file; different `ino` models a copy (new identity, spec §11).
pub struct MemPartIo {
    bytes: std::sync::Arc<[u8]>,
    dev: u64,
    ino: u64,
}

impl MemPartIo {
    pub fn new(bytes: Vec<u8>, dev: u64, ino: u64) -> MemPartIo {
        MemPartIo {
            bytes: bytes.into(),
            dev,
            ino,
        }
    }

    /// Share the same byte image under the same identity (a second open of
    /// the same file).
    pub fn reopen(&self) -> MemPartIo {
        MemPartIo {
            bytes: self.bytes.clone(),
            dev: self.dev,
            ino: self.ino,
        }
    }
}

impl PartIo for MemPartIo {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn dev_ino(&self) -> (u64, u64) {
        (self.dev, self.ino)
    }

    fn pread_exact(&self, off: u64, buf: &mut [u8], at: &'static str) -> ReadResult<()> {
        let off = off as usize;
        let end = off
            .checked_add(buf.len())
            .ok_or(ReadError::Format(FormatError::Bounds { at }))?;
        if end > self.bytes.len() {
            return Err(ReadError::Format(FormatError::Truncated { at }));
        }
        buf.copy_from_slice(&self.bytes[off..end]);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// table-directory reads (manifest walk, spec §12/§13)
// ---------------------------------------------------------------------------

/// Whole-file reads inside one table directory (`pgrc2_<relfilenumber>/`).
/// Manifest generations and the commit pointer are small (spec §13); the
/// walk reads them whole. `Ok(None)` = the file does not exist (ENOENT) —
/// a first-class answer (absent `CURRENT` means "no committed publish"),
/// never an error.
pub trait TableDirIo {
    fn read_file(&self, name: &str) -> ReadResult<Option<Vec<u8>>>;
}

/// Production [`TableDirIo`] over a directory path, through vfs.
pub struct VfsTableDir {
    dir: String,
}

impl VfsTableDir {
    /// `dir` is the table directory path (no trailing slash).
    pub fn new(dir: String) -> VfsTableDir {
        VfsTableDir { dir }
    }

    fn join(&self, name: &str) -> ReadResult<CString> {
        CString::new(format!("{}/{}", self.dir, name)).map_err(|_| {
            ReadError::Format(FormatError::Corrupt {
                at: "table dir path NUL",
            })
        })
    }
}

impl TableDirIo for VfsTableDir {
    fn read_file(&self, name: &str) -> ReadResult<Option<Vec<u8>>> {
        let path = self.join(name)?;
        let fd = vfs::open(&path, libc::O_RDONLY | libc::O_CLOEXEC, 0);
        if fd < 0 {
            let errno = vfs::get_errno();
            if errno == libc::ENOENT {
                return Ok(None);
            }
            return Err(ReadError::Io {
                at: "dir file open",
                errno,
            });
        }
        let mut info = vfs::FileInfo::zeroed();
        if vfs::fstat(fd, &mut info) != 0 {
            let errno = vfs::get_errno();
            vfs::close(fd);
            return Err(ReadError::Io {
                at: "dir file fstat",
                errno,
            });
        }
        let len = info.size.max(0) as usize;
        let mut buf = vec![0u8; len];
        let mut done = 0usize;
        while done < len {
            let n = vfs::pread(fd, &mut buf[done..], done as libc::off_t);
            if n < 0 {
                let errno = vfs::get_errno();
                if errno == libc::EINTR {
                    continue;
                }
                vfs::close(fd);
                return Err(ReadError::Io {
                    at: "dir file read",
                    errno,
                });
            }
            if n == 0 {
                // Shrank under us; sealed dir files never shrink — refuse.
                vfs::close(fd);
                return Err(ReadError::Format(FormatError::Truncated {
                    at: "dir file",
                }));
            }
            done += n as usize;
        }
        vfs::close(fd);
        Ok(Some(buf))
    }
}

/// In-memory [`TableDirIo`] for tests: a name → bytes map.
#[derive(Default)]
pub struct MemTableDir {
    pub files: std::collections::BTreeMap<String, Vec<u8>>,
}

impl MemTableDir {
    pub fn new() -> MemTableDir {
        MemTableDir::default()
    }

    pub fn put(&mut self, name: &str, bytes: Vec<u8>) {
        self.files.insert(name.to_string(), bytes);
    }
}

impl TableDirIo for MemTableDir {
    fn read_file(&self, name: &str) -> ReadResult<Option<Vec<u8>>> {
        Ok(self.files.get(name).cloned())
    }
}
