//! File-set vocabulary (`storage/fileset.h`, `storage/sharedfileset.h`): the
//! shmem-resident descriptors that coordinate a set of BufFile-backed temp
//! files across a parallel-query backend group. Only the data shapes live
//! here; the create/attach/open protocol (`storage/file/fileset.c`,
//! `sharedfileset.c`) is reached through that owner's seam crate.

use crate::storage::Spinlock;
use types_core::{uint32, Oid};

/// `FileSet` (`storage/fileset.h`) — names a group of temporary files shared
/// by a set of backends, keyed by the creating PID and a per-PID counter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct FileSet {
    /// `pid_t creator_pid` — PID of the creating process.
    pub creator_pid: i32,
    /// `uint32 number` — per-PID identifier.
    pub number: uint32,
    /// `int ntablespaces` — number of tablespaces to use.
    pub ntablespaces: i32,
    /// `Oid tablespaces[8]` — OIDs of tablespaces to use (`FILESET_MAX_TABLESPACES`).
    pub tablespaces: [Oid; 8],
}

/// `FILESET_MAX_TABLESPACES` (`storage/fileset.h`).
pub const FILESET_MAX_TABLESPACES: usize = 8;

/// `SharedFileSet` (`storage/sharedfileset.h`) — a [`FileSet`] plus the
/// reference-count bookkeeping that lets attaching backends clean up after the
/// creator detaches. Embedded directly in DSM-resident structs (e.g.
/// `ParallelHashJoinState`), so it carries a real spinlock and is neither
/// `Copy` nor `Clone`.
#[derive(Debug)]
#[repr(C)]
pub struct SharedFileSet {
    /// `FileSet fs`.
    pub fs: FileSet,
    /// `slock_t mutex` — protects the reference count.
    pub mutex: Spinlock,
    /// `int refcnt` — number of attached backends.
    pub refcnt: i32,
}
