//! Leaf-seam implementations for the base-backup file walk.
//!
//! `basebackup.c` makes a handful of OS-level / global-read calls directly: the
//! `lstat`/`readlink` stat primitives, `geteuid`/`getegid`/`time(NULL)` for the
//! injected `sendFileWithContent` stat, the `pg_file_create_mode` /
//! `pg_dir_create_mode` file-perm globals (`common/file_perm.c`), and the
//! `tarCreateHeader` member-header writer (`src/port/tar.c`). These have no
//! ported owner of their own, so the base-backup consumer (their only caller)
//! implements them here and installs them from [`crate::init_seams`].
//!
//! `read_link` is owned by `backend-storage-file-fd` (the OS-readlink primitive
//! is fd-coupled there); the rest are installed below.

use backend_backup_basebackup_seams::{self as bbseam, LstatInfo, TarError, TarHeader, TAR_BLOCK_SIZE};
use backend_utils_error::ereport;
use types_error::{PgResult, ERROR};

use crate::here;

// ---------------------------------------------------------------------------
// install
// ---------------------------------------------------------------------------

/// Install the base-backup leaf seams whose owners are this consumer.
pub(crate) fn init_leaf_seams() {
    bbseam::lstat_file::set(lstat_file);
    bbseam::tar_create_header::set(tar_create_header);
    bbseam::geteuid::set(geteuid);
    bbseam::getegid::set(getegid);
    bbseam::time_now::set(time_now);
    bbseam::pg_file_create_mode::set(pg_file_create_mode);
    bbseam::pg_dir_create_mode::set(pg_dir_create_mode);
}

// ---------------------------------------------------------------------------
// lstat
// ---------------------------------------------------------------------------

/// `lstat(path, &statbuf)` — stat a path without following symlinks.
///
/// `Ok(Some(info))` on success, `Ok(None)` when `errno == ENOENT` (the caller
/// renders its own path-specific error), and `Err` (the C
/// `errcode_for_file_access()` `ereport(ERROR, "could not stat file")`) for any
/// other failure.
fn lstat_file(path: &str) -> PgResult<Option<LstatInfo>> {
    let cpath = match std::ffi::CString::new(path) {
        Ok(c) => c,
        // A NUL in the path can't name a real file: treat as ENOENT (vanished).
        Err(_) => return Ok(None),
    };
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; st is a valid out-param.
    let sret = unsafe { libc::lstat(cpath.as_ptr(), &mut st) };
    if sret != 0 {
        let errno = errno_now();
        if errno == libc::ENOENT {
            return Ok(None);
        }
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(alloc_format(path))
            .into_error()
            .with_error_location(here("lstat_file")));
    }
    Ok(Some(LstatInfo {
        size: st.st_size as i64,
        mode: st.st_mode as u32,
        uid: st.st_uid as u32,
        gid: st.st_gid as u32,
        mtime: st.st_mtime as i64,
    }))
}

/// `errmsg("could not stat file \"%s\": %m", path)` text (the `%m` is supplied
/// by `errcode_for_file_access`).
fn alloc_format(path: &str) -> String {
    format!("could not stat file \"{path}\"")
}

// ---------------------------------------------------------------------------
// geteuid / getegid / time
// ---------------------------------------------------------------------------

/// `geteuid()` — effective user id.
fn geteuid() -> u32 {
    // SAFETY: geteuid never fails and has no preconditions.
    unsafe { libc::geteuid() as u32 }
}

/// `getegid()` — effective group id.
fn getegid() -> u32 {
    // SAFETY: getegid never fails and has no preconditions.
    unsafe { libc::getegid() as u32 }
}

/// `time(NULL)` — current wall-clock time in seconds.
fn time_now() -> i64 {
    // SAFETY: time(NULL) is sound with a null out-param; it returns the time_t.
    unsafe { libc::time(std::ptr::null_mut()) as i64 }
}

// ---------------------------------------------------------------------------
// file-perm globals (common/file_perm.c)
// ---------------------------------------------------------------------------

/// `pg_file_create_mode` global — reads the fd owner's file-perm global, which
/// `checkDataDir`/`SetDataDirectoryCreatePerm` seeds at startup.
fn pg_file_create_mode() -> u32 {
    backend_storage_file_fd::vfd_core::pg_file_create_mode()
}

/// `pg_dir_create_mode` global — reads the fd owner's directory-perm global.
fn pg_dir_create_mode() -> u32 {
    backend_storage_file_fd::vfd_core::pg_dir_create_mode()
}

// ---------------------------------------------------------------------------
// tarCreateHeader (src/port/tar.c)
// ---------------------------------------------------------------------------

// pgtar.h offset constants (the 512-byte ustar member header layout).
const TAR_OFFSET_NAME: usize = 0; // 100 byte string
const TAR_OFFSET_MODE: usize = 100; // 8 byte tar number, excludes S_IFMT
const TAR_OFFSET_UID: usize = 108; // 8 byte tar number
const TAR_OFFSET_GID: usize = 116; // 8 byte tar number
const TAR_OFFSET_SIZE: usize = 124; // 12 byte tar number
const TAR_OFFSET_MTIME: usize = 136; // 12 byte tar number
const TAR_OFFSET_CHECKSUM: usize = 148; // 8 byte tar number
const TAR_OFFSET_TYPEFLAG: usize = 156; // 1 byte file type
const TAR_OFFSET_LINKNAME: usize = 157; // 100 byte string
const TAR_OFFSET_MAGIC: usize = 257; // "ustar" + terminating zero
const TAR_OFFSET_VERSION: usize = 263; // "00"
const TAR_OFFSET_UNAME: usize = 265; // 32 byte string
const TAR_OFFSET_GNAME: usize = 297; // 32 byte string
const TAR_OFFSET_DEVMAJOR: usize = 329; // 8 byte tar number
const TAR_OFFSET_DEVMINOR: usize = 337; // 8 byte tar number

const TAR_FILETYPE_PLAIN: u8 = b'0';
const TAR_FILETYPE_SYMLINK: u8 = b'2';
const TAR_FILETYPE_DIRECTORY: u8 = b'5';

// sys/stat.h file-type bits used by tarCreateHeader (S_ISDIR).
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;

fn s_isdir(mode: u32) -> bool {
    (mode & S_IFMT) == S_IFDIR
}

/// `tarCreateHeader(h, filename, linktarget, size, mode, uid, gid, mtime)`
/// (`src/port/tar.c`) — render a 512-byte tar member header.
fn tar_create_header(
    filename: &str,
    linktarget: Option<&str>,
    size: i64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: i64,
) -> TarHeader {
    if filename.len() > 99 {
        return TarHeader {
            rc: TarError::NameTooLong,
            bytes: [0u8; TAR_BLOCK_SIZE],
        };
    }

    if let Some(lt) = linktarget {
        if lt.len() > 99 {
            return TarHeader {
                rc: TarError::SymlinkTooLong,
                bytes: [0u8; TAR_BLOCK_SIZE],
            };
        }
    }

    let mut h = [0u8; TAR_BLOCK_SIZE];

    // Name 100
    strlcpy(&mut h, TAR_OFFSET_NAME, filename, 100);
    if linktarget.is_some() || s_isdir(mode) {
        // We only support symbolic links to directories, and this is indicated
        // in the tar format by adding a slash at the end of the name, the same
        // as for regular directories.
        let flen = filename.len().min(99);
        h[flen] = b'/';
        h[flen + 1] = b'\0';
    }

    // Mode 8 - this doesn't include the file type bits (S_IFMT)
    print_tar_number(&mut h, TAR_OFFSET_MODE, 8, (mode & 0o7777) as u64);

    // User ID 8
    print_tar_number(&mut h, TAR_OFFSET_UID, 8, uid as u64);

    // Group 8
    print_tar_number(&mut h, TAR_OFFSET_GID, 8, gid as u64);

    // File size 12
    if linktarget.is_some() || s_isdir(mode) {
        // Symbolic link or directory has size zero
        print_tar_number(&mut h, TAR_OFFSET_SIZE, 12, 0);
    } else {
        print_tar_number(&mut h, TAR_OFFSET_SIZE, 12, size as u64);
    }

    // Mod Time 12
    print_tar_number(&mut h, TAR_OFFSET_MTIME, 12, mtime as u64);

    // Checksum 8 cannot be calculated until we've filled all other fields

    if let Some(lt) = linktarget {
        // Type - Symbolic link
        h[TAR_OFFSET_TYPEFLAG] = TAR_FILETYPE_SYMLINK;
        // Link Name 100
        strlcpy(&mut h, TAR_OFFSET_LINKNAME, lt, 100);
    } else if s_isdir(mode) {
        // Type - directory
        h[TAR_OFFSET_TYPEFLAG] = TAR_FILETYPE_DIRECTORY;
    } else {
        // Type - regular file
        h[TAR_OFFSET_TYPEFLAG] = TAR_FILETYPE_PLAIN;
    }

    // Magic 6
    strcpy(&mut h, TAR_OFFSET_MAGIC, "ustar");

    // Version 2
    h[TAR_OFFSET_VERSION] = b'0';
    h[TAR_OFFSET_VERSION + 1] = b'0';

    // User 32 (XXX: Do we need to care about setting correct username?)
    strlcpy(&mut h, TAR_OFFSET_UNAME, "postgres", 32);

    // Group 32 (XXX: Do we need to care about setting correct group name?)
    strlcpy(&mut h, TAR_OFFSET_GNAME, "postgres", 32);

    // Major Dev 8
    print_tar_number(&mut h, TAR_OFFSET_DEVMAJOR, 8, 0);

    // Minor Dev 8
    print_tar_number(&mut h, TAR_OFFSET_DEVMINOR, 8, 0);

    // Prefix 155 - not used, leave as nulls

    // Finally, compute and insert the checksum
    let sum = tar_checksum(&h);
    print_tar_number(&mut h, TAR_OFFSET_CHECKSUM, 8, sum as u64);

    TarHeader {
        rc: TarError::Ok,
        bytes: h,
    }
}

/// `print_tar_number(s, len, val)` (tar.c) — render `val` into `len` bytes at
/// `off`, octal-with-trailing-space when it fits, else base-256.
fn print_tar_number(h: &mut [u8; TAR_BLOCK_SIZE], off: usize, len: usize, mut val: u64) {
    if val < (1u64 << ((len - 1) * 3)) {
        // Use octal with trailing space
        let mut i = len;
        i -= 1;
        h[off + i] = b' ';
        while i > 0 {
            i -= 1;
            h[off + i] = ((val & 7) as u8) + b'0';
            val >>= 3;
        }
    } else {
        // Use base-256 with leading \200
        h[off] = 0o200;
        let mut i = len;
        while i > 1 {
            i -= 1;
            h[off + i] = (val & 255) as u8;
            val >>= 8;
        }
    }
}

/// `tarChecksum(header)` (tar.c) — the simple unsigned-byte sum, treating the
/// 8-byte checksum field (offset 148..156) as 8 spaces.
fn tar_checksum(header: &[u8; TAR_BLOCK_SIZE]) -> i32 {
    let mut sum: i32 = 8 * (b' ' as i32); // presumed value for checksum field
    for (i, &b) in header.iter().enumerate() {
        if i < 148 || i >= 156 {
            sum += 0xFF & (b as i32);
        }
    }
    sum
}

/// `strlcpy(&h[off], src, size)` — copy at most `size - 1` bytes of `src`,
/// always NUL-terminating within the `size`-byte field (the rest of `h` is
/// already zeroed). Matches BSD `strlcpy` truncation behaviour.
fn strlcpy(h: &mut [u8; TAR_BLOCK_SIZE], off: usize, src: &str, size: usize) {
    let bytes = src.as_bytes();
    let n = bytes.len().min(size - 1);
    h[off..off + n].copy_from_slice(&bytes[..n]);
    h[off + n] = b'\0';
}

/// `strcpy(&h[off], src)` — copy `src` plus its NUL terminator (used only for
/// the fixed-size "ustar" magic that is known to fit).
fn strcpy(h: &mut [u8; TAR_BLOCK_SIZE], off: usize, src: &str) {
    let bytes = src.as_bytes();
    h[off..off + bytes.len()].copy_from_slice(bytes);
    h[off + bytes.len()] = b'\0';
}

// ---------------------------------------------------------------------------
// errno
// ---------------------------------------------------------------------------

/// The current `errno` value.
fn errno_now() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
