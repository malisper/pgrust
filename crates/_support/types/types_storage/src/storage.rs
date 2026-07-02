use core::cell::UnsafeCell;
use core::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, AtomicU8, Ordering,
};

use ::types_core::{
    uint16, uint32, uint64, uint8, LocalTransactionId, Oid, ProcNumber, RelFileNumber, Size,
    TransactionId, XLogRecPtr, XidStatus, INVALID_PROC_NUMBER,
};

use crate::ilist::dlist_head;
use crate::latch::Latch;
use crate::lock::{LOCKMASK, LOCKMODE, LOCKTAG};

pub use ::types_core::{Buffer, BufferIsValid, InvalidBuffer};

pub type LocationIndex = uint16;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum LWLockMode {
    #[default]
    LW_EXCLUSIVE = 0,
    LW_SHARED = 1,
    // PGPROC->lwWaitMode only, never an LWLockAcquire argument.
    LW_WAIT_UNTIL_FREE = 2,
}

pub use LWLockMode::*;

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct pg_atomic_uint32 {
    pub value: AtomicU32,
}

impl pg_atomic_uint32 {
    pub const fn new(value: uint32) -> Self {
        Self {
            value: AtomicU32::new(value),
        }
    }

    pub fn read(&self) -> uint32 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct pg_atomic_uint64 {
    pub value: AtomicU64,
}

impl pg_atomic_uint64 {
    pub const fn new(value: uint64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    pub fn read(&self) -> uint64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn write(&self, value: uint64) {
        self.value.store(value, Ordering::Relaxed);
    }

    pub fn read_membarrier(&self) -> uint64 {
        self.value.load(Ordering::SeqCst)
    }

    pub fn write_membarrier(&self, value: uint64) {
        self.value.store(value, Ordering::SeqCst);
    }

    pub fn fetch_add(&self, add_: uint64) -> uint64 {
        self.value.fetch_add(add_, Ordering::SeqCst)
    }

    pub fn monotonic_advance(&self, target: uint64) -> uint64 {
        let mut currval = self.value.load(Ordering::SeqCst);
        while currval < target {
            match self.value.compare_exchange_weak(
                currval,
                target,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return target,
                Err(observed) => currval = observed,
            }
        }
        currval
    }
}

#[repr(transparent)]
#[derive(Debug, Default)]
pub struct Spinlock {
    word: AtomicI32,
}

impl Spinlock {
    pub const fn new() -> Self {
        Self {
            word: AtomicI32::new(0),
        }
    }

    pub fn unlock(&self) {
        self.word.store(0, Ordering::Release);
    }

    pub fn is_free(&self) -> bool {
        self.word.load(Ordering::Relaxed) == 0
    }

    pub fn tas(&self) -> i32 {
        self.word.swap(1, Ordering::Acquire)
    }

    // Non-locking read before the TAS while spinning (C's TAS_SPIN).
    pub fn tas_spin(&self) -> i32 {
        if self.word.load(Ordering::Relaxed) != 0 {
            1
        } else {
            self.tas()
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum LWLockWaitState {
    #[default]
    LW_WS_NOT_WAITING = 0,
    LW_WS_WAITING = 1,
    LW_WS_PENDING_WAKEUP = 2,
}

pub use LWLockWaitState::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct proclist_node {
    pub next: ProcNumber,
    pub prev: ProcNumber,
}

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

// Mutated only while the owning lock's LW_FLAG_LOCKED bit is held (lwlock.c's
// wait-list protocol) — that runtime exclusion is what makes ptr() sound.
#[derive(Debug, Default)]
pub struct LWLockWaitList {
    cell: UnsafeCell<proclist_head>,
}

// SAFETY: cross-thread access is serialized by the owning LWLock's
// LW_FLAG_LOCKED spinlock bit.
unsafe impl Sync for LWLockWaitList {}

impl LWLockWaitList {
    pub const fn new(head: proclist_head) -> Self {
        Self {
            cell: UnsafeCell::new(head),
        }
    }

    pub fn ptr(&self) -> *mut proclist_head {
        self.cell.get()
    }

    pub fn get_mut(&mut self) -> &mut proclist_head {
        self.cell.get_mut()
    }
}

#[repr(C)]
#[derive(Debug, Default)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: LWLockWaitList,
}

// SAFETY: shmem-resident; `state` is atomic and `waiters` is serialized by the
// LW_FLAG_LOCKED bit in `state`.
unsafe impl Sync for LWLock {}

pub const LWLOCK_PADDED_SIZE: usize = 128;

#[repr(C, align(128))]
#[derive(Debug, Default)]
pub struct LWLockPadded {
    pub lock: LWLock,
}

const _: () = assert!(core::mem::size_of::<LWLockPadded>() == LWLOCK_PADDED_SIZE);

pub const MAX_BACKENDS_BITS: i32 = 18;
pub const MAX_BACKENDS: uint32 = (1_u32 << MAX_BACKENDS_BITS) - 1;

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
pub const NUM_PROCSIGNALS: usize = PROCSIG_RECOVERY_CONFLICT_LAST as usize + 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalBarrierType {
    PROCSIGNAL_BARRIER_SMGRRELEASE = 0,
}

pub const MAX_IO_WORKERS: i32 = 32;
pub const NUM_AUXILIARY_PROCS: i32 = 6 + MAX_IO_WORKERS;

// lwlocklist.h: PG_LWLOCK ids run 1..=53; NUM_INDIVIDUAL_LWLOCKS = last + 1.
pub const NUM_INDIVIDUAL_LWLOCKS: i32 = 54;

pub const SHMEM_INDEX_LOCK: usize = 1;
pub const OID_GEN_LOCK: usize = 2;
pub const XID_GEN_LOCK: usize = 3;
pub const PROC_ARRAY_LOCK: usize = 4;
pub const SINVAL_READ_LOCK: usize = 5;
pub const SINVAL_WRITE_LOCK: usize = 6;
pub const MULTI_XACT_GEN_LOCK: usize = 13;
pub const REL_CACHE_INIT_LOCK: usize = 16;
pub const BTREE_VACUUM_LOCK: usize = 20;
pub const NOTIFY_QUEUE_LOCK: usize = 27;
pub const SYNC_REP_LOCK: usize = 32;
pub const DYNAMIC_SHARED_MEMORY_CONTROL_LOCK: usize = 34;
pub const REPLICATION_SLOT_ALLOCATION_LOCK: usize = 36;
pub const REPLICATION_SLOT_CONTROL_LOCK: usize = 37;
pub const COMMIT_TS_LOCK: usize = 39;
pub const MULTI_XACT_TRUNCATION_LOCK: usize = 41;
pub const XACT_TRUNCATION_LOCK: usize = 44;
pub const WRAP_LIMITS_VACUUM_LOCK: usize = 46;
pub const NOTIFY_QUEUE_TAIL_LOCK: usize = 47;
pub const WAIT_EVENT_CUSTOM_LOCK: usize = 48;
pub const DSM_REGISTRY_LOCK: usize = 50;

pub const SERIALIZABLE_XACT_HASH_LOCK: i32 = 28;
pub const SERIALIZABLE_FINISHED_LIST_LOCK: i32 = 29;
pub const SERIALIZABLE_PREDICATE_LIST_LOCK: i32 = 30;
pub const SERIAL_CONTROL_LOCK: i32 = 52;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HugePagesStatus {
    HUGE_PAGES_OFF = 0,
    HUGE_PAGES_ON = 1,
    HUGE_PAGES_TRY = 2,
    HUGE_PAGES_UNKNOWN = 3,
}

pub type dsm_handle = uint32;
pub const DSM_HANDLE_INVALID: dsm_handle = 0;
pub type dsa_handle = dsm_handle;
pub type dsa_pointer = uint64;
pub const INVALID_DSA_POINTER: dsa_pointer = 0;
pub type dshash_table_handle = dsa_pointer;

#[repr(C)]
pub struct DsaArea {
    _private: [u8; 0],
}

#[repr(C)]
pub struct DshashTable {
    _private: [u8; 0],
}

// dshash.h: "function pointers can't be shared between backends" — this names
// the built-in compare/hash/copy set instead of carrying the C pointers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DshashKeyKind {
    String,
    Binary,
    Record,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DshashParameters {
    pub key_size: Size,
    pub entry_size: Size,
    pub key_kind: DshashKeyKind,
    pub tranche_id: i32,
}

#[repr(C)]
pub struct PGShmemHeader {
    pub magic: i32,
    pub creatorPID: libc::pid_t,
    pub totalsize: usize,
    pub freeoffset: usize,
    pub dsm_control: dsm_handle,
    pub index: *mut core::ffi::c_void,
    pub device: libc::dev_t,
    pub inode: libc::ino_t,
}

pub const PGShmemMagic: i32 = 679834894;

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

// Three Oids, no padding — relfilelocator.h requires that for hashtable keys.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

impl RelFileLocator {
    pub const fn new(spcOid: Oid, dbOid: Oid, relNumber: RelFileNumber) -> Self {
        Self {
            spcOid,
            dbOid,
            relNumber,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let word = |off: usize| -> Option<uint32> {
            Some(uint32::from_ne_bytes(data.get(off..off + 4)?.try_into().ok()?))
        };
        Some(Self {
            spcOid: word(0)?,
            dbOid: word(4)?,
            relNumber: word(8)?,
        })
    }

    pub const fn spc_oid(&self) -> Oid {
        self.spcOid
    }

    pub const fn db_oid(&self) -> Oid {
        self.dbOid
    }

    pub const fn rel_number(&self) -> RelFileNumber {
        self.relNumber
    }
}

const _: () = assert!(core::mem::size_of::<RelFileLocator>() == 12);

#[inline]
pub fn RelFileLocatorEquals(a: &RelFileLocator, b: &RelFileLocator) -> bool {
    a == b
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum ReadBufferMode {
    Normal = 0,
    ZeroAndLock,
    ZeroAndCleanupLock,
    ZeroOnError,
    NormalNoLog,
}

pub use ::types_core::VirtualTransactionId;

pub type subxids_array_status = u32;
pub const SUBXIDS_IN_ARRAY: subxids_array_status = 0;
pub const SUBXIDS_MISSING: subxids_array_status = 1;
pub const SUBXIDS_IN_SUBTRANS: subxids_array_status = 2;

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

// GetRunningTransactionData's "returns with ProcArrayLock and XidGenLock held"
// contract as a with-locks callback; the owner releases whatever is still held
// on both success and error exits.
pub trait RunningTransactionLocksHeld {
    fn release_proc_array_lock(&mut self) -> types_error::PgResult<()>;
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct xl_standby_lock {
    pub xid: TransactionId,
    pub dbOid: Oid,
    pub relOid: Oid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrefetchBufferResult {
    pub recent_buffer: Buffer,
    pub initiated_io: bool,
}

pub const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XidCacheStatus {
    pub count: uint8,
    pub overflowed: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct XidCache {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS],
}

impl Default for XidCache {
    fn default() -> Self {
        Self {
            xids: [0; PGPROC_MAX_CACHED_SUBXIDS],
        }
    }
}

pub type ProcWaitStatus = u32;
pub const PROC_WAIT_STATUS_OK: ProcWaitStatus = 0;
pub const PROC_WAIT_STATUS_WAITING: ProcWaitStatus = 1;
pub const PROC_WAIT_STATUS_ERROR: ProcWaitStatus = 2;

pub const PROC_IS_AUTOVACUUM: uint8 = 0x01;
pub const PROC_IN_VACUUM: uint8 = 0x02;
pub const PROC_IN_SAFE_IC: uint8 = 0x04;
pub const PROC_VACUUM_FOR_WRAPAROUND: uint8 = 0x08;
pub const PROC_IN_LOGICAL_DECODING: uint8 = 0x10;
pub const PROC_AFFECTS_ALL_HORIZONS: uint8 = 0x20;
pub const PROC_VACUUM_STATE_MASK: uint8 =
    PROC_IN_VACUUM | PROC_IN_SAFE_IC | PROC_VACUUM_FOR_WRAPAROUND;
pub const PROC_XMIN_FLAGS: uint8 = PROC_IN_VACUUM | PROC_IN_SAFE_IC;

pub const DELAY_CHKPT_START: i32 = 1 << 0;
pub const DELAY_CHKPT_COMPLETE: i32 = 1 << 1;

pub const NUM_SPECIAL_WORKER_PROCS: i32 = 2;

pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: i32 = 1024;
pub const FP_LOCK_SLOTS_PER_GROUP: i32 = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PGSemaphoreData {
    pub semId: i32,
    pub semNum: i32,
}

// C keeps the pair separately assignable (not a VirtualTransactionId) because
// it is not atomically assignable as a whole.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PGProcVxid {
    pub procNumber: ProcNumber,
    pub lxid: LocalTransactionId,
}

impl Default for PGProcVxid {
    fn default() -> Self {
        Self {
            procNumber: INVALID_PROC_NUMBER,
            lxid: 0,
        }
    }
}

// Shmem-resident, always reached by pointer in C — never Copy/Clone.
// C-pointer fields carry identities, not invented boxes: `procgloballist` is
// the FreeListId naming one of PROC_HDR's four heads, `waitLock`/`waitProcLock`
// carry the awaited lock's LOCKTAG / holder ProcNumber, `lockGroupLeader` the
// leader's ProcNumber.
#[derive(Debug)]
pub struct PGPROC {
    pub links: dlist_node,
    pub procgloballist: Option<FreeListId>,
    pub sem: Option<Box<PGSemaphoreData>>,
    pub waitStatus: ProcWaitStatus,
    pub procLatch: Latch,
    pub xid: TransactionId,
    pub xmin: TransactionId,
    pub pid: i32,
    pub pgxactoff: i32,
    pub vxid: PGProcVxid,
    pub databaseId: Oid,
    pub roleId: Oid,
    pub tempNamespaceId: Oid,
    pub isRegularBackend: bool,
    pub recoveryConflictPending: bool,
    pub lwWaiting: uint8,
    pub lwWaitMode: uint8,
    pub lwWaitLink: proclist_node,
    pub cvWaitLink: proclist_node,
    pub waitLock: Option<LOCKTAG>,
    pub waitProcLock: Option<ProcNumber>,
    pub waitLockMode: LOCKMODE,
    pub heldLocks: LOCKMASK,
    pub waitStart: pg_atomic_uint64,
    pub delayChkptFlags: i32,
    pub statusFlags: uint8,
    pub waitLSN: XLogRecPtr,
    pub syncRepState: i32,
    pub syncRepLinks: proclist_node,
    pub myProcLocks: [dlist_head; NUM_LOCK_PARTITIONS as usize],
    pub subxidStatus: XidCacheStatus,
    pub subxids: XidCache,
    pub procArrayGroupMember: bool,
    pub procArrayGroupNext: pg_atomic_uint32,
    pub procArrayGroupMemberXid: TransactionId,
    pub wait_event_info: uint32,
    pub clogGroupMember: bool,
    pub clogGroupNext: pg_atomic_uint32,
    pub clogGroupMemberXid: TransactionId,
    pub clogGroupMemberXidStatus: XidStatus,
    pub clogGroupMemberPage: i64,
    pub clogGroupMemberLsn: XLogRecPtr,
    pub fpInfoLock: LWLock,
    pub fpLockBits: Vec<uint64>,
    pub fpRelId: Vec<Oid>,
    pub fpVXIDLock: bool,
    pub fpLocalTransactionId: LocalTransactionId,
    pub lockGroupLeader: Option<ProcNumber>,
    pub lockGroupMembers: ProcFreeList,
    pub lockGroupLink: dlist_node,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreeListId {
    Regular,
    Autovac,
    Bgworker,
    Walsender,
}

// The intrusive shmem dlist of PGPROCs realized over the index-addressed
// allProcs arena as an ordered ProcNumber list.
#[derive(Clone, Debug, Default)]
pub struct ProcFreeList {
    pub members: VecDeque<ProcNumber>,
}

impl ProcFreeList {
    pub const fn new() -> Self {
        Self {
            members: VecDeque::new(),
        }
    }

    pub fn push_tail(&mut self, procno: ProcNumber) {
        self.members.push_back(procno);
    }

    pub fn push_head(&mut self, procno: ProcNumber) {
        self.members.push_front(procno);
    }

    pub fn pop_head(&mut self) -> Option<ProcNumber> {
        self.members.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn remove(&mut self, procno: ProcNumber) {
        if let Some(pos) = self.members.iter().position(|&p| p == procno) {
            self.members.remove(pos);
        }
    }
}

#[derive(Debug)]
pub struct PROC_HDR {
    pub allProcs: Vec<PGPROC>,
    pub xids: Vec<TransactionId>,
    pub subxidStates: Vec<XidCacheStatus>,
    pub statusFlags: Vec<uint8>,
    pub allProcCount: uint32,
    pub freeProcs: ProcFreeList,
    pub autovacFreeProcs: ProcFreeList,
    pub bgworkerFreeProcs: ProcFreeList,
    pub walsenderFreeProcs: ProcFreeList,
    pub procArrayGroupFirst: pg_atomic_uint32,
    pub clogGroupFirst: pg_atomic_uint32,
    pub walwriterProc: ProcNumber,
    pub checkpointerProc: ProcNumber,
    pub spins_per_delay: i32,
    pub startupBufferPinWaitBufId: i32,
}

impl PGPROC {
    // Mirrors InitProcGlobal's MemSet(0) over the fresh PGPROC array.
    pub fn new_zeroed() -> Self {
        Self {
            links: dlist_node::new(),
            procgloballist: None,
            sem: None,
            waitStatus: PROC_WAIT_STATUS_OK,
            procLatch: Latch {
                is_set: AtomicI32::new(0),
                maybe_sleeping: AtomicI32::new(0),
                is_shared: AtomicBool::new(false),
                owner_pid: AtomicI32::new(0),
            },
            xid: 0,
            xmin: 0,
            pid: 0,
            pgxactoff: 0,
            vxid: PGProcVxid {
                procNumber: 0,
                lxid: 0,
            },
            databaseId: 0,
            roleId: 0,
            tempNamespaceId: 0,
            isRegularBackend: false,
            recoveryConflictPending: false,
            lwWaiting: 0,
            lwWaitMode: 0,
            lwWaitLink: proclist_node::default(),
            cvWaitLink: proclist_node::default(),
            waitLock: None,
            waitProcLock: None,
            waitLockMode: 0,
            heldLocks: 0,
            waitStart: pg_atomic_uint64::new(0),
            delayChkptFlags: 0,
            statusFlags: 0,
            waitLSN: 0,
            syncRepState: 0,
            syncRepLinks: proclist_node { next: 0, prev: 0 },
            myProcLocks: core::array::from_fn(|_| dlist_head::new()),
            subxidStatus: XidCacheStatus {
                count: 0,
                overflowed: false,
            },
            subxids: XidCache::default(),
            procArrayGroupMember: false,
            procArrayGroupNext: pg_atomic_uint32::new(0),
            procArrayGroupMemberXid: 0,
            wait_event_info: 0,
            clogGroupMember: false,
            clogGroupNext: pg_atomic_uint32::new(0),
            clogGroupMemberXid: 0,
            clogGroupMemberXidStatus: 0,
            clogGroupMemberPage: 0,
            clogGroupMemberLsn: 0,
            fpInfoLock: LWLock::default(),
            fpLockBits: Vec::new(),
            fpRelId: Vec::new(),
            fpVXIDLock: false,
            fpLocalTransactionId: 0,
            lockGroupLeader: None,
            lockGroupMembers: ProcFreeList::new(),
            lockGroupLink: dlist_node::new(),
        }
    }
}

impl PROC_HDR {
    pub fn new_zeroed() -> Self {
        Self {
            allProcs: Vec::new(),
            xids: Vec::new(),
            subxidStates: Vec::new(),
            statusFlags: Vec::new(),
            allProcCount: 0,
            freeProcs: ProcFreeList::new(),
            autovacFreeProcs: ProcFreeList::new(),
            bgworkerFreeProcs: ProcFreeList::new(),
            walsenderFreeProcs: ProcFreeList::new(),
            procArrayGroupFirst: pg_atomic_uint32::new(INVALID_PROC_NUMBER as u32),
            clogGroupFirst: pg_atomic_uint32::new(INVALID_PROC_NUMBER as u32),
            walwriterProc: INVALID_PROC_NUMBER,
            checkpointerProc: INVALID_PROC_NUMBER,
            spins_per_delay: 0,
            startupBufferPinWaitBufId: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lwlock_layout_and_fixed_partitions() {
        assert!(core::mem::size_of::<LWLock>() <= LWLOCK_PADDED_SIZE);
        assert_eq!(BUFFER_MAPPING_LWLOCK_OFFSET, 54);
        assert_eq!(LOCK_MANAGER_LWLOCK_OFFSET, 182);
        assert_eq!(PREDICATELOCK_MANAGER_LWLOCK_OFFSET, 198);
        assert_eq!(NUM_FIXED_LWLOCKS, 214);
        assert_eq!(LWTRANCHE_FIRST_USER_DEFINED, 95);
        assert_eq!(LWTRANCHE_AIO_URING_COMPLETION, 94);
    }

    #[test]
    fn proc_constants_match_headers() {
        assert_eq!(MAX_BACKENDS, 0x3FFFF);
        assert_eq!(NUM_AUXILIARY_PROCS, 38);
        assert_eq!(NUM_PROCSIGNALS, 14);
        assert_eq!(PROC_VACUUM_STATE_MASK, 0x0E);
        assert_eq!(PROC_XMIN_FLAGS, 0x06);
    }

    #[test]
    fn monotonic_advance_matches_c_loop() {
        let v = pg_atomic_uint64::new(5);
        assert_eq!(v.monotonic_advance(3), 5);
        assert_eq!(v.monotonic_advance(9), 9);
        assert_eq!(v.read(), 9);
    }

    #[test]
    fn spinlock_tas_shape() {
        let s = Spinlock::new();
        assert!(s.is_free());
        assert_eq!(s.tas(), 0);
        assert_eq!(s.tas_spin(), 1);
        s.unlock();
        assert!(s.is_free());
    }
}
