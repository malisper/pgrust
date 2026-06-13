//! Trimmed copy of the src-idiomatic `types::storage` module: the LWLock
//! handle and its supporting pieces.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};

use types_core::{uint16, uint32, uint64, uint8, Oid, ProcNumber, RelFileNumber, Size, TransactionId, INVALID_PROC_NUMBER};

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

impl Default for LWLockMode {
    /// C's zero value (`LW_EXCLUSIVE`), for zero-initialized shmem images.
    fn default() -> Self {
        LW_EXCLUSIVE
    }
}

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

    /// `pg_atomic_write_u64(ptr, val)`.
    pub fn write(&self, value: types_core::uint64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

/// A PostgreSQL spinlock word (`slock_t`, `storage/s_lock.h`).
///
/// Acquired with an atomic test-and-set ([`Spinlock::tas`]) and released with
/// a fence-ordered store of zero ([`Spinlock::unlock`]). `#[repr(transparent)]`
/// over an `AtomicI32` so the in-memory layout matches the `int`-width
/// `slock_t`. The word-level primitives live here (like the `pg_atomic_*`
/// types above) so shmem-resident structs can embed the lock word; the
/// contended-acquire backoff loop (`s_lock.c`) lives in the
/// `backend-storage-lmgr-s-lock` crate.
#[repr(transparent)]
#[derive(Debug, Default)]
pub struct Spinlock {
    word: AtomicI32,
}

impl Spinlock {
    /// A new, free spinlock.
    pub const fn new() -> Self {
        Self {
            word: AtomicI32::new(0),
        }
    }

    /// `S_INIT_LOCK`/`S_UNLOCK` — store zero, releasing the lock.
    ///
    /// `Release` ordering keeps loads and stores issued before the unlock
    /// from being reordered past it, matching PostgreSQL's `S_UNLOCK` fence
    /// requirement (`__sync_lock_release` semantics).
    pub fn unlock(&self) {
        self.word.store(0, Ordering::Release);
    }

    /// `S_LOCK_FREE(lock)` — true when `*lock == 0`.
    pub fn is_free(&self) -> bool {
        self.word.load(Ordering::Relaxed) == 0
    }

    /// `tas(lock)` — atomically set the word to 1 and return the previous
    /// value (0 if the lock was free and is now ours, nonzero if held).
    ///
    /// `Acquire` ordering keeps loads and stores issued after the TAS from
    /// being reordered before it, matching PostgreSQL's `TAS` fence
    /// requirement (`__sync_lock_test_and_set` semantics).
    pub fn tas(&self) -> i32 {
        self.word.swap(1, Ordering::Acquire)
    }

    /// `TAS_SPIN(lock)` — `*(lock) ? 1 : TAS(lock)`.
    ///
    /// On x86_64 and aarch64 it is a win to do a non-locking read of the word
    /// before attempting the (more expensive) atomic TAS while spinning.
    pub fn tas_spin(&self) -> i32 {
        if self.word.load(Ordering::Relaxed) != 0 {
            1
        } else {
            self.tas()
        }
    }
}

/// `enum LWLockWaitState` (`storage/lwlock.h:28`) — the `PGPROC.lwWaiting`
/// state set and read by the LWLock wait-list machinery.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum LWLockWaitState {
    /// not currently waiting / woken up
    LW_WS_NOT_WAITING = 0,
    /// currently waiting
    LW_WS_WAITING = 1,
    /// removed from waitlist, but not yet signalled
    LW_WS_PENDING_WAKEUP = 2,
}

pub use LWLockWaitState::*;

impl Default for LWLockWaitState {
    /// C's zero value (`LW_WS_NOT_WAITING`), for zero-initialized PGPROCs.
    fn default() -> Self {
        LW_WS_NOT_WAITING
    }
}

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

/// The `waiters` field of an [`LWLock`]: a `proclist_head` that, per
/// lwlock.c's protocol, is mutated only while the wait-list spinlock bit
/// (`LW_FLAG_LOCKED`) is held in the lock's `state` word. Backends share
/// `&LWLock` handles, so the head lives in an `UnsafeCell`; the runtime
/// exclusion that makes `ptr()` access sound is the `LW_FLAG_LOCKED` bit,
/// exactly as in C.
#[derive(Debug, Default)]
pub struct LWLockWaitList {
    cell: UnsafeCell<proclist_head>,
}

// SAFETY: cross-thread access is serialized by the owning LWLock's
// LW_FLAG_LOCKED spinlock bit (lwlock.c's wait-list protocol).
unsafe impl Sync for LWLockWaitList {}

impl LWLockWaitList {
    pub const fn new(head: proclist_head) -> Self {
        Self {
            cell: UnsafeCell::new(head),
        }
    }

    /// Raw pointer to the list head. Dereferencing requires holding the
    /// owning lock's `LW_FLAG_LOCKED` bit (or otherwise having exclusive
    /// access, e.g. single-threaded initialization).
    pub fn ptr(&self) -> *mut proclist_head {
        self.cell.get()
    }

    /// Exclusive-access view (used by `LWLockInitialize`, which legitimately
    /// holds `&mut LWLock`).
    pub fn get_mut(&mut self) -> &mut proclist_head {
        self.cell.get_mut()
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
    pub waiters: LWLockWaitList,
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

/// `DSMRegistryLock` (`lwlocklist.h`, `PG_LWLOCK(50, DSMRegistry)`): offset of
/// the DSM-registry lock in `MainLWLockArray` (`&MainLWLockArray[50].lock`).
pub const DSM_REGISTRY_LOCK: usize = 50;
/// `ProcArrayLock` (`lwlocklist.h`): `PG_LWLOCK(4, ProcArray)`.
pub const PROC_ARRAY_LOCK: usize = 4;
/// `ReplicationSlotAllocationLock` — `PG_LWLOCK(36, ReplicationSlotAllocation)`.
pub const REPLICATION_SLOT_ALLOCATION_LOCK: usize = 36;
/// `ReplicationSlotControlLock` — `PG_LWLOCK(37, ReplicationSlotControl)`.
pub const REPLICATION_SLOT_CONTROL_LOCK: usize = 37;

/// `dsm_handle` (`storage/dsm_impl.h`) — a "name" for a dynamic shared memory
/// segment.
pub type dsm_handle = uint32;

/// `DSM_HANDLE_INVALID` (`(dsm_handle) 0`).
pub const DSM_HANDLE_INVALID: dsm_handle = 0;

/// `dsa_handle` (`utils/dsa.h`, `typedef dsm_handle dsa_handle`) — a "name" for
/// a DSA area that can be passed between cooperating backends.
pub type dsa_handle = dsm_handle;

/// `dsa_pointer` (`utils/dsa.h`) — a relative pointer within a DSA area
/// (`uint64` on 64-bit pointer width).
pub type dsa_pointer = uint64;

/// `InvalidDsaPointer` (`utils/dsa.h`) — `((dsa_pointer) 0)`.
pub const INVALID_DSA_POINTER: dsa_pointer = 0;

/// `dshash_table_handle` (`lib/dshash.h`, `typedef dsa_pointer
/// dshash_table_handle`) — a handle to a dshash table passed between backends.
pub type dshash_table_handle = dsa_pointer;

/// `dsa_area` (`utils/dsa.h`) — opaque backend-local handle to a DSA area. The
/// area's internals are owned by the `dsa.c` substrate; consumers only hold
/// and pass the pointer, so the body stays opaque.
#[repr(C)]
pub struct DsaArea {
    _private: [u8; 0],
}

/// `dshash_table` (`lib/dshash.h`) — opaque backend-local handle to a dshash
/// table. The table's internals are owned by the `dshash.c` substrate;
/// consumers only hold and pass the pointer, so the body stays opaque.
#[repr(C)]
pub struct DshashTable {
    _private: [u8; 0],
}

/// Which built-in key-handling helper set a [`DshashParameters`] selects. The C
/// `dshash_parameters` carries raw `compare`/`hash`/`copy` function pointers,
/// but "function pointers can't be shared between backends" (`dshash.h`), so
/// every backend supplies the same set by value; the only set the DSM registry
/// uses is the NUL-terminated-string helpers (`dshash_strcmp`/`dshash_strhash`/
/// `dshash_strcpy`), which `dshash.c` owns. This selector names that set
/// without crossing the seam with the foreign function pointers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DshashKeyKind {
    /// `dshash_strcmp` / `dshash_strhash` / `dshash_strcpy` — fixed-width
    /// NUL-terminated string keys occupying the first `key_size` bytes of the
    /// entry.
    String,
}

/// `dshash_parameters` (`lib/dshash.h`) — the parameters to create or attach a
/// dshash table. `tranche_id` is only consulted on create. The compare/hash/
/// copy function pointers are conveyed by [`DshashKeyKind`] (see its docs).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DshashParameters {
    /// `key_size` — size of the key (initial bytes of the entry).
    pub key_size: Size,
    /// `entry_size` — total size of an entry.
    pub entry_size: Size,
    /// The built-in key-helper set (`compare_function`/`hash_function`/
    /// `copy_function`).
    pub key_kind: DshashKeyKind,
    /// `tranche_id` — the LWLock tranche for the table's partition locks.
    pub tranche_id: i32,
}

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

/// `VirtualTransactionId` (`storage/lock.h`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: types_core::LocalTransactionId,
}

impl VirtualTransactionId {
    /// `SetInvalidVirtualTransactionId(vxid)`.
    pub const fn invalid() -> Self {
        Self {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    /// `VirtualTransactionIdIsValid(vxid)` —
    /// `LocalTransactionIdIsValid((vxid).localTransactionId)`.
    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }
}

// ---------------------------------------------------------------------------
// `storage/standby.h` / `storage/procarray.h` running-xacts vocabulary.
// ---------------------------------------------------------------------------

/// `subxids_array_status` (`storage/standby.h`).
pub type subxids_array_status = u32;
pub const SUBXIDS_IN_ARRAY: subxids_array_status = 0;
pub const SUBXIDS_MISSING: subxids_array_status = 1;
pub const SUBXIDS_IN_SUBTRANS: subxids_array_status = 2;

/// `RunningTransactionsData` (`storage/standby.h`). The C `xids` pointer
/// (length `xcnt + subxcnt`) is context-allocated (C builds it in
/// TopMemoryContext / the current context), so it is a `PgVec` carrying its
/// allocator lifetime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunningTransactionsData<'mcx> {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_status: subxids_array_status,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: mcx::PgVec<'mcx, TransactionId>,
}

/// Handle to the LWLocks `GetRunningTransactionData` (`procarray.c`) holds
/// while its caller's callback runs: the C contract "returns with
/// ProcArrayLock and XidGenLock held" becomes a with-locks callback shape.
/// The owner releases every lock still held when the callback returns —
/// success and error path alike — so no lock is ever held across `?` without
/// a guard.
pub trait RunningTransactionLocksHeld {
    /// `LWLockRelease(ProcArrayLock)` before the callback finishes — the
    /// hot-standby (`wal_level < logical`) path in `LogStandbySnapshot`.
    /// `Err` carries the C `elog(ERROR, "lock ... is not held")`.
    fn release_proc_array_lock(&mut self) -> types_error::PgResult<()>;
}

/// `xl_standby_lock` (`storage/standbydefs.h`): one logged
/// AccessExclusiveLock — 12 bytes, no padding.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct xl_standby_lock {
    /// xid of the holding transaction.
    pub xid: TransactionId,
    /// `InvalidOid` when locking a shared relation.
    pub dbOid: Oid,
    pub relOid: Oid,
}

/// `shm_toc_estimator` (`storage/shm_toc.h`) — transient sizing accumulator
/// for `shm_toc_estimate`; lives in backend-local memory, not the segment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct shm_toc_estimator {
    /// `Size space_for_chunks`.
    pub space_for_chunks: Size,
    /// `Size number_of_keys`.
    pub number_of_keys: Size,
}

/// `PrefetchBufferResult` (`storage/bufmgr.h`) — the result of
/// `PrefetchBuffer`/`PrefetchSharedBuffer`: a buffer the block was already
/// found in (`InvalidBuffer` when none), and whether an I/O was initiated.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrefetchBufferResult {
    /// `Buffer recent_buffer` — the block's buffer if already cached.
    pub recent_buffer: types_core::Buffer,
    /// `bool initiated_io` — whether a prefetch was started.
    pub initiated_io: bool,
}
