//! Trimmed copy of the src-idiomatic `types::storage` module: the LWLock
//! handle and its supporting pieces.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use types_core::{uint16, uint32, Oid, ProcNumber, RelFileNumber, uint8, INVALID_PROC_NUMBER};

/// `enum LWLockMode` (`storage/lwlock.h:112`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum LWLockMode {
    LW_EXCLUSIVE = 0,
    LW_SHARED = 1,
    /// A special mode used in `PGPROC->lwWaitMode`, when waiting for lock to
    /// become free. Not to be used as `LWLockAcquire` argument.
    LW_WAIT_UNTIL_FREE = 2,
}

pub use LWLockMode::*;

/// `pg_atomic_uint32` (`port/atomics.h`) — a shmem-resident atomic word,
/// concurrently read and CAS'd by every backend. A real atomic; like the C
/// struct (whose copy would tear concurrent state) it is neither `Copy` nor
/// `Clone`, and identity (not value) is its equality.
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct pg_atomic_uint32 {
    pub value: AtomicU32,
}

impl pg_atomic_uint32 {
    /// `pg_atomic_init_u32(ptr, val)`.
    pub const fn new(value: uint32) -> Self {
        Self {
            value: AtomicU32::new(value),
        }
    }

    /// `pg_atomic_read_u32(ptr)`.
    pub fn read(&self) -> uint32 {
        self.value.load(Ordering::Relaxed)
    }
}

/// `pg_atomic_uint64` (`port/atomics.h`) — a shmem-resident atomic 8-byte
/// word; see [`pg_atomic_uint32`] for why it is not `Copy`/`Clone`.
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct pg_atomic_uint64 {
    pub value: AtomicU64,
}

impl pg_atomic_uint64 {
    /// `pg_atomic_init_u64(ptr, val)`.
    pub const fn new(value: types_core::uint64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    /// `pg_atomic_read_u64(ptr)`.
    pub fn read(&self) -> types_core::uint64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// `LWLockWaitState` (`storage/lwlock.h`) — the `PGPROC.lwWaiting` state byte
/// set and read by the LWLock wait-list machinery.
pub type LWLockWaitState = uint8;
/// not currently waiting / woken up
pub const LW_WS_NOT_WAITING: LWLockWaitState = 0;
/// currently waiting
pub const LW_WS_WAITING: LWLockWaitState = 1;
/// removed from waitlist, but not yet signaled
pub const LW_WS_PENDING_WAKEUP: LWLockWaitState = 2;

/// `proclist_node` (`storage/proclist_types.h`) — a node in a doubly-linked
/// list of PGPROCs identified by pgprocno. A node not in any list has
/// `next == prev == 0`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct proclist_node {
    /// pgprocno of the next PGPROC
    pub next: ProcNumber,
    /// pgprocno of the prev PGPROC
    pub prev: ProcNumber,
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
/// of waiting PGPROCs. Shmem-resident and concurrently accessed, so (like its
/// atomic `state`) it is neither `Copy` nor `Clone` — a copied lock would be a
/// different lock.
#[derive(Debug, Default)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: proclist_head,
}

/// `LWLOCK_PADDED_SIZE` (`storage/lwlock.h`) — `PG_CACHE_LINE_SIZE`.
pub const LWLOCK_PADDED_SIZE: usize = 128;

/// `LWLockPadded` (`storage/lwlock.h`) — in C a union of an `LWLock` with a
/// pad to `LWLOCK_PADDED_SIZE`, so each lock in an array sits on its own
/// cache line. The alignment attribute reproduces both the size and the
/// placement guarantee.
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct LWLockPadded {
    pub lock: LWLock,
}

const _: () = assert!(core::mem::size_of::<LWLockPadded>() == LWLOCK_PADDED_SIZE);

/// `MAX_BACKENDS_BITS` / `MAX_BACKENDS` (`storage/procnumber.h`).
pub const MAX_BACKENDS_BITS: i32 = 18;
pub const MAX_BACKENDS: uint32 = (1_u32 << MAX_BACKENDS_BITS) - 1;

/// `ProcSignalReason` (`storage/procsignal.h`) — reasons for signaling a
/// Postgres child process over the multiplexed SIGUSR1 channel.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalReason {
    PROCSIG_CATCHUP_INTERRUPT = 0,
    PROCSIG_NOTIFY_INTERRUPT = 1,
    PROCSIG_PARALLEL_MESSAGE = 2,
    PROCSIG_WALSND_INIT_STOPPING = 3,
    PROCSIG_BARRIER = 4,
    PROCSIG_LOG_MEMORY_CONTEXT = 5,
    PROCSIG_PARALLEL_APPLY_MESSAGE = 6,
    PROCSIG_RECOVERY_CONFLICT_DATABASE = 7,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE = 8,
    PROCSIG_RECOVERY_CONFLICT_LOCK = 9,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT = 10,
    PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT = 11,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN = 12,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK = 13,
}

pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK;
/// `NUM_PROCSIGNALS` (`storage/procsignal.h`).
pub const NUM_PROCSIGNALS: usize = PROCSIG_RECOVERY_CONFLICT_LAST as usize + 1;

/// `ProcSignalBarrierType` (`storage/procsignal.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalBarrierType {
    /// ask smgr to close files
    PROCSIGNAL_BARRIER_SMGRRELEASE = 0,
}

/// `MAX_IO_WORKERS` (`storage/proc.h`).
pub const MAX_IO_WORKERS: i32 = 32;
/// `NUM_AUXILIARY_PROCS` (`storage/proc.h`): extra PGPROC/ProcSignal slots
/// for auxiliary processes.
pub const NUM_AUXILIARY_PROCS: i32 = 6 + MAX_IO_WORKERS;

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

// Fixed-partition layout of the main LWLock array (`storage/lwlock.h`).
pub const NUM_BUFFER_PARTITIONS: i32 = 128;
pub const LOG2_NUM_LOCK_PARTITIONS: i32 = 4;
pub const NUM_LOCK_PARTITIONS: i32 = 1 << LOG2_NUM_LOCK_PARTITIONS;
pub const LOG2_NUM_PREDICATELOCK_PARTITIONS: i32 = 4;
pub const NUM_PREDICATELOCK_PARTITIONS: i32 = 1 << LOG2_NUM_PREDICATELOCK_PARTITIONS;
pub const BUFFER_MAPPING_LWLOCK_OFFSET: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LOCK_MANAGER_LWLOCK_OFFSET: i32 = BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS;
pub const PREDICATELOCK_MANAGER_LWLOCK_OFFSET: i32 =
    LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS;
pub const NUM_FIXED_LWLOCKS: i32 =
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS;

// `BuiltinTrancheIds` (`storage/lwlock.h`) — the full chain from
// `LWTRANCHE_XACT_BUFFER = NUM_INDIVIDUAL_LWLOCKS` to
// `LWTRANCHE_FIRST_USER_DEFINED`.
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
pub const LWTRANCHE_LAUNCHER_DSA: i32 = LWTRANCHE_PGSTATS_DATA + 1;
pub const LWTRANCHE_LAUNCHER_HASH: i32 = LWTRANCHE_LAUNCHER_DSA + 1;
pub const LWTRANCHE_DSM_REGISTRY_DSA: i32 = LWTRANCHE_LAUNCHER_HASH + 1;
pub const LWTRANCHE_DSM_REGISTRY_HASH: i32 = LWTRANCHE_DSM_REGISTRY_DSA + 1;
pub const LWTRANCHE_COMMITTS_SLRU: i32 = LWTRANCHE_DSM_REGISTRY_HASH + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_SLRU: i32 = LWTRANCHE_COMMITTS_SLRU + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_SLRU: i32 = LWTRANCHE_MULTIXACTMEMBER_SLRU + 1;
pub const LWTRANCHE_NOTIFY_SLRU: i32 = LWTRANCHE_MULTIXACTOFFSET_SLRU + 1;
pub const LWTRANCHE_SERIAL_SLRU: i32 = LWTRANCHE_NOTIFY_SLRU + 1;
pub const LWTRANCHE_SUBTRANS_SLRU: i32 = LWTRANCHE_SERIAL_SLRU + 1;
pub const LWTRANCHE_XACT_SLRU: i32 = LWTRANCHE_SUBTRANS_SLRU + 1;
pub const LWTRANCHE_PARALLEL_VACUUM_DSA: i32 = LWTRANCHE_XACT_SLRU + 1;
pub const LWTRANCHE_AIO_URING_COMPLETION: i32 = LWTRANCHE_PARALLEL_VACUUM_DSA + 1;
pub const LWTRANCHE_FIRST_USER_DEFINED: i32 = LWTRANCHE_AIO_URING_COMPLETION + 1;

/// `LOCKMODE` (`storage/lockdefs.h`) — a relation/object lock level.
pub type LOCKMODE = i32;
pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;

/// `RelFileLocator` (`storage/relfilelocator.h`) — the physical identity of a
/// relation: tablespace, database, and relfilenumber.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RelFileLocator {
    /// `spcOid` — tablespace.
    pub spcOid: Oid,
    /// `dbOid` — database.
    pub dbOid: Oid,
    /// `relNumber` — relation storage number.
    pub relNumber: RelFileNumber,
}

/// `RelFileLocatorEquals(locator1, locator2)` (`storage/relfilelocator.h`).
#[inline]
pub fn RelFileLocatorEquals(a: &RelFileLocator, b: &RelFileLocator) -> bool {
    a == b
}
