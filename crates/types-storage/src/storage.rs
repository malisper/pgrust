//! Trimmed copy of the src-idiomatic `types::storage` module: the LWLock
//! handle and its supporting pieces.

use types_core::{uint16, uint32, ProcNumber, INVALID_PROC_NUMBER};

/// `LWLockMode` (`storage/lwlock.h`).
pub type LWLockMode = u32;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_WAIT_UNTIL_FREE: LWLockMode = 2;

/// `pg_atomic_uint32` (`port/atomics.h`) — a shmem-resident atomic word.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/// `proclist_head` (`storage/proclist_types.h`) — head/tail pgprocno indexes of
/// a doubly-linked PGPROC list; `INVALID_PROC_NUMBER` at the ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct proclist_head {
    pub head: ProcNumber,
    pub tail: ProcNumber,
}

impl Default for proclist_head {
    fn default() -> Self {
        Self {
            head: INVALID_PROC_NUMBER,
            tail: INVALID_PROC_NUMBER,
        }
    }
}

/// `LWLock` (`storage/lwlock.h`): tranche id, atomic lock state, and the list
/// of waiting PGPROCs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: proclist_head,
}

/// `NUM_INDIVIDUAL_LWLOCKS` — generated from `lwlocklist.h`.
pub const NUM_INDIVIDUAL_LWLOCKS: i32 = 54;

/// `DynamicSharedMemoryControlLock` (`lwlocklist.h`): offset of the DSM
/// control lock in `MainLWLockArray` (`&MainLWLockArray[34].lock`).
pub const DYNAMIC_SHARED_MEMORY_CONTROL_LOCK: usize = 34;

/// `dsm_handle` (`storage/dsm_impl.h`) — a "name" for a dynamic shared memory
/// segment.
pub type dsm_handle = uint32;

/// `DSM_HANDLE_INVALID` (`(dsm_handle) 0`).
pub const DSM_HANDLE_INVALID: dsm_handle = 0;

/// `PGShmemHeader` (`storage/pg_shmem.h`) — standard header for all Postgres
/// shared memory segments, resident at the start of the main segment.
/// `repr(C)` because it lives in real shared memory.
#[repr(C)]
pub struct PGShmemHeader {
    /// `magic` — magic # to identify Postgres segments.
    pub magic: i32,
    /// `creatorPID` — PID of creating process (set but unread).
    pub creatorPID: libc::pid_t,
    /// `totalsize` — total size of segment.
    pub totalsize: usize,
    /// `freeoffset` — offset to first free space.
    pub freeoffset: usize,
    /// `dsm_control` — ID of dynamic shared memory control segment.
    pub dsm_control: dsm_handle,
    /// `index` — pointer to ShmemIndex table.
    pub index: *mut core::ffi::c_void,
    /// `device` — device data directory is on (non-Windows only).
    pub device: libc::dev_t,
    /// `inode` — inode number of data directory (non-Windows only).
    pub inode: libc::ino_t,
}

/// `PGShmemMagic` (`storage/pg_shmem.h`).
pub const PGShmemMagic: i32 = 679834894;

// `BuiltinTrancheIds` (`storage/lwlock.h`) — the chain from
// `LWTRANCHE_XACT_BUFFER = NUM_INDIVIDUAL_LWLOCKS` down to the tranche the
// pgstat ports consume (`LWTRANCHE_PGSTATS_DATA`).
pub const LWTRANCHE_XACT_BUFFER: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LWTRANCHE_COMMITTS_BUFFER: i32 = LWTRANCHE_XACT_BUFFER + 1;
pub const LWTRANCHE_SUBTRANS_BUFFER: i32 = LWTRANCHE_COMMITTS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_BUFFER: i32 = LWTRANCHE_SUBTRANS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_BUFFER: i32 = LWTRANCHE_MULTIXACTOFFSET_BUFFER + 1;
pub const LWTRANCHE_NOTIFY_BUFFER: i32 = LWTRANCHE_MULTIXACTMEMBER_BUFFER + 1;
pub const LWTRANCHE_SERIAL_BUFFER: i32 = LWTRANCHE_NOTIFY_BUFFER + 1;
pub const LWTRANCHE_WAL_INSERT: i32 = LWTRANCHE_SERIAL_BUFFER + 1;
pub const LWTRANCHE_BUFFER_CONTENT: i32 = LWTRANCHE_WAL_INSERT + 1;
pub const LWTRANCHE_REPLICATION_ORIGIN_STATE: i32 = LWTRANCHE_BUFFER_CONTENT + 1;
pub const LWTRANCHE_REPLICATION_SLOT_IO: i32 = LWTRANCHE_REPLICATION_ORIGIN_STATE + 1;
pub const LWTRANCHE_LOCK_FASTPATH: i32 = LWTRANCHE_REPLICATION_SLOT_IO + 1;
pub const LWTRANCHE_BUFFER_MAPPING: i32 = LWTRANCHE_LOCK_FASTPATH + 1;
pub const LWTRANCHE_LOCK_MANAGER: i32 = LWTRANCHE_BUFFER_MAPPING + 1;
pub const LWTRANCHE_PREDICATE_LOCK_MANAGER: i32 = LWTRANCHE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_HASH_JOIN: i32 = LWTRANCHE_PREDICATE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_BTREE_SCAN: i32 = LWTRANCHE_PARALLEL_HASH_JOIN + 1;
pub const LWTRANCHE_PARALLEL_QUERY_DSA: i32 = LWTRANCHE_PARALLEL_BTREE_SCAN + 1;
pub const LWTRANCHE_PER_SESSION_DSA: i32 = LWTRANCHE_PARALLEL_QUERY_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPE: i32 = LWTRANCHE_PER_SESSION_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPMOD: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPE + 1;
pub const LWTRANCHE_SHARED_TUPLESTORE: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPMOD + 1;
pub const LWTRANCHE_SHARED_TIDBITMAP: i32 = LWTRANCHE_SHARED_TUPLESTORE + 1;
pub const LWTRANCHE_PARALLEL_APPEND: i32 = LWTRANCHE_SHARED_TIDBITMAP + 1;
pub const LWTRANCHE_PER_XACT_PREDICATE_LIST: i32 = LWTRANCHE_PARALLEL_APPEND + 1;
pub const LWTRANCHE_PGSTATS_DSA: i32 = LWTRANCHE_PER_XACT_PREDICATE_LIST + 1;
pub const LWTRANCHE_PGSTATS_HASH: i32 = LWTRANCHE_PGSTATS_DSA + 1;
pub const LWTRANCHE_PGSTATS_DATA: i32 = LWTRANCHE_PGSTATS_HASH + 1;
