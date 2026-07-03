use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use ::types_core::BLCKSZ;

// fd.c's `typedef int File`: an index into the VFD cache, not a kernel fd.
// > 0 is a valid VFD; 0 is the LRU/free-list header.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct File(pub i32);

pub type Dir = i32;

// C's `struct dirent *`: d_name is the only field PostgreSQL reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEnt {
    pub d_name: String, // std String justified: cold directory walks, no query context
}

#[derive(Clone, Debug)]
pub struct PGAlignedBlock {
    pub data: Vec<u8>,
}

impl Default for PGAlignedBlock {
    fn default() -> Self {
        PGAlignedBlock {
            data: vec![0u8; BLCKSZ],
        }
    }
}

pub type FileCopyMethod = i32;
pub type DataDirSyncMethod = i32;
pub type FileExtendMethod = i32;

pub const FILE_COPY_METHOD_COPY: FileCopyMethod = 0;
pub const FILE_COPY_METHOD_CLONE: FileCopyMethod = 1;

pub const DATA_DIR_SYNC_METHOD_FSYNC: DataDirSyncMethod = 0;
pub const DATA_DIR_SYNC_METHOD_SYNCFS: DataDirSyncMethod = 1;

pub const FILE_EXTEND_METHOD_POSIX_FALLOCATE: FileExtendMethod = 0;
pub const FILE_EXTEND_METHOD_WRITE_ZEROS: FileExtendMethod = 1;
pub const DEFAULT_FILE_EXTEND_METHOD: FileExtendMethod = FILE_EXTEND_METHOD_POSIX_FALLOCATE;

pub const IO_DIRECT_DATA: i32 = 0x01;
pub const IO_DIRECT_WAL: i32 = 0x02;
pub const IO_DIRECT_WAL_INIT: i32 = 0x04;

pub const NUM_RESERVED_FDS: i32 = 10;
pub const FD_MINFREE: i32 = 48;

pub const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";
pub const PG_TEMP_FILES_DIR: &str = "pgsql_tmp";
pub const OIDCHARS: usize = 10;
pub const FORKNAMECHARS: usize = 4;
// "PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO for 18.3.
pub const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";
pub const PG_TBLSPC_DIR: &str = "pg_tblspc";
