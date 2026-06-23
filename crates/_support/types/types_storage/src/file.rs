//! Storage/file type vocabulary (`storage/fd.h`, `common/relpath.h`,
//! `common/file_utils.h`), trimmed to what the `storage/file/fd.c` port and its
//! consumers need.
//!
//! A [`File`] is an *index* into the VFD cache, not a kernel handle (`fd.c`'s
//! `typedef int File`); it is a newtype over the C `int` so it is not
//! interchangeable with a raw integer. The directory-entry record [`DirEnt`]
//! carries only the field PostgreSQL ever reads from `struct dirent`
//! (`d_name`). The remaining aliases/constants are the `FileCopyMethod` /
//! `DataDirSyncMethod` / `FileExtendMethod` enums, the `io_direct` flag bits,
//! and the temp-file / tablespace path constants. [`PGAlignedBlock`] is the
//! `BLCKSZ`-sized I/O buffer block embedded by callers such as `buffile.c`.

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use ::types_core::BLCKSZ;

/// `File` (`storage/fd.h`) — `typedef int File`. A virtual file descriptor: an
/// index into fd.c's VFD cache, NOT an OS file descriptor. A value `> 0` is a
/// valid VFD; `<= 0` signals "no file"/error in the fd.c APIs. `0` is the
/// LRU/free-list header (never a usable VFD). Modeled as a newtype over the C
/// `int` so it is not interchangeable with a raw integer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct File(pub i32);

/// `Dir` — a live directory iterator opened with `AllocateDir`, identified by a
/// stable integer handle (an index into the allocated-descriptor table).
pub type Dir = i32;

/// A directory entry returned by `ReadDir` / `ReadDirExtended`.
///
/// C hands back a `struct dirent *`; the only field PostgreSQL ever reads is
/// `d_name`, so the idiomatic record carries just the owned name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEnt {
    /// `dirent.d_name` — the entry's file name.
    pub d_name: String,
}

/// `PGAlignedBlock` (`c.h`) — a `union { char data[BLCKSZ]; double force_align_d;
/// int64 force_align_i64; }` used as a `BLCKSZ`-aligned I/O buffer. The
/// alignment exists so the kernel can DMA into it; in the owned port the
/// buffer is a heap `Vec<u8>` of exactly `BLCKSZ` bytes (the Rust allocator's
/// 16-byte alignment is sufficient for the buffered read/write the callers
/// perform). Only the `data` arm is ever read or written.
#[derive(Clone, Debug)]
pub struct PGAlignedBlock {
    /// `char data[BLCKSZ]` — the block payload (always exactly `BLCKSZ` bytes).
    pub data: Vec<u8>,
}

impl Default for PGAlignedBlock {
    fn default() -> Self {
        PGAlignedBlock {
            data: vec![0u8; BLCKSZ],
        }
    }
}

// --- file_utils.h enums (carried as i32 aliases, the bits32/GUC-int use) ---

/// `enum FileCopyMethod` (`common/file_utils.h`).
pub type FileCopyMethod = i32;
/// `enum DataDirSyncMethod` (`common/file_utils.h`).
pub type DataDirSyncMethod = i32;
/// `FileExtendMethod` (`storage/fd.h`).
pub type FileExtendMethod = i32;

pub const FILE_COPY_METHOD_COPY: FileCopyMethod = 0;
pub const FILE_COPY_METHOD_CLONE: FileCopyMethod = 1;

pub const DATA_DIR_SYNC_METHOD_FSYNC: DataDirSyncMethod = 0;
pub const DATA_DIR_SYNC_METHOD_SYNCFS: DataDirSyncMethod = 1;

pub const FILE_EXTEND_METHOD_POSIX_FALLOCATE: FileExtendMethod = 0;
pub const FILE_EXTEND_METHOD_WRITE_ZEROS: FileExtendMethod = 1;
pub const DEFAULT_FILE_EXTEND_METHOD: FileExtendMethod = FILE_EXTEND_METHOD_POSIX_FALLOCATE;

// --- io_direct flag bits (`storage/fd.h`) ---
pub const IO_DIRECT_DATA: i32 = 0x01;
pub const IO_DIRECT_WAL: i32 = 0x02;
pub const IO_DIRECT_WAL_INIT: i32 = 0x04;

// --- fd.c compile-time constants ---
/// `NUM_RESERVED_FDS` (`fd.c`).
pub const NUM_RESERVED_FDS: i32 = 10;
/// `FD_MINFREE` (`fd.c`).
pub const FD_MINFREE: i32 = 48;

// --- temp-file / tablespace path constants (`common/relpath.h`, `fd.h`) ---
/// `PG_TEMP_FILE_PREFIX` (`storage/fd.h`).
pub const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";
/// `PG_TEMP_FILES_DIR` (`storage/fd.h`).
pub const PG_TEMP_FILES_DIR: &str = "pgsql_tmp";
/// `OIDCHARS` (`common/relpath.h`) — max chars printed by `%u` for an OID.
pub const OIDCHARS: usize = 10;
/// `FORKNAMECHARS` (`common/relpath.h`) — max chars for a fork name.
pub const FORKNAMECHARS: usize = 4;
/// `TABLESPACE_VERSION_DIRECTORY` — `"PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO`.
/// For PostgreSQL 18.3: `PG_MAJORVERSION == "18"`, `CATALOG_VERSION_NO == 202506291`.
pub const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";
/// `PG_TBLSPC_DIR` (`common/relpath.h`) — tablespace path relative to `$PGDATA`.
pub const PG_TBLSPC_DIR: &str = "pg_tblspc";
