use core::ffi::{c_char, c_int, c_uint, c_void};

use crate::types::{
    uint16, uint32, uint64, uint8, BlockNumber, ForkNumber, OffsetNumber, Oid, ProcNumber,
    RelFileNumber, Size, TransactionId, XLogRecPtr, BLCKSZ, INVALID_OID, INVALID_PROC_NUMBER,
};
pub use crate::wal::RelFileLocator;
use crate::xact::LocalTransactionId;

pub const InvalidBlockNumber: BlockNumber = 0xffff_ffff;
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const FirstOffsetNumber: OffsetNumber = 1;
pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / core::mem::size_of::<ItemIdData>()) as u16;
pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = InvalidBlockNumber;
pub const MAIN_FORKNUM: ForkNumber = 0;
pub const FSM_FORKNUM: ForkNumber = 1;
pub const VISIBILITYMAP_FORKNUM: ForkNumber = 2;
pub const INIT_FORKNUM: ForkNumber = 3;
pub const MAX_FORKNUM: ForkNumber = INIT_FORKNUM;
pub const InvalidForkNumber: ForkNumber = -1;
pub const InvalidRelFileNumber: RelFileNumber = INVALID_OID;

// --- Tablespace / relation-path constants (common/relpath.h) ---
/// `TABLESPACE_VERSION_DIRECTORY` -- `"PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO`.
/// For PostgreSQL 18.3, `PG_MAJORVERSION == "18"` and
/// `CATALOG_VERSION_NO == 202506291`.
pub const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";
/// `PG_TBLSPC_DIR` -- tablespace path relative to `$PGDATA`.
pub const PG_TBLSPC_DIR: &str = "pg_tblspc";
/// `PG_TBLSPC_DIR_SLASH` -- `"pg_tblspc/"` (used for string comparisons).
pub const PG_TBLSPC_DIR_SLASH: &str = "pg_tblspc/";
/// `OIDCHARS` -- max chars printed by `%u` for an OID in a relation path.
pub const OIDCHARS: usize = 10;
/// `FORKNAMECHARS` -- max chars for a fork name.
pub const FORKNAMECHARS: usize = 4;

pub type File = c_int;
pub type FileCopyMethod = c_int;
pub type DataDirSyncMethod = c_int;
pub type FileExtendMethod = c_int;

pub const FILE_COPY_METHOD_COPY: FileCopyMethod = 0;
pub const FILE_COPY_METHOD_CLONE: FileCopyMethod = 1;
pub const DATA_DIR_SYNC_METHOD_FSYNC: DataDirSyncMethod = 0;
pub const DATA_DIR_SYNC_METHOD_SYNCFS: DataDirSyncMethod = 1;
pub const FILE_EXTEND_METHOD_POSIX_FALLOCATE: FileExtendMethod = 0;
pub const FILE_EXTEND_METHOD_WRITE_ZEROS: FileExtendMethod = 1;
pub const DEFAULT_FILE_EXTEND_METHOD: FileExtendMethod = FILE_EXTEND_METHOD_POSIX_FALLOCATE;
pub const IO_DIRECT_DATA: c_int = 0x01;
pub const IO_DIRECT_WAL: c_int = 0x02;
pub const IO_DIRECT_WAL_INIT: c_int = 0x04;
pub const NUM_RESERVED_FDS: c_int = 10;
pub const FD_MINFREE: c_int = 48;
pub const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";

pub type dsm_handle = uint32;
pub type pid_t = i32;
pub type dev_t = i32;
pub type ino_t = u64;
pub type slock_t = c_int;
pub type LWLockMode = c_uint;
pub type LocationIndex = uint16;
pub type ItemOffset = uint16;
pub type ItemLength = uint16;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

impl VirtualTransactionId {
    pub const fn invalid() -> Self {
        Self {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }
}

pub type subxids_array_status = c_uint;
pub const SUBXIDS_IN_ARRAY: subxids_array_status = 0;
pub const SUBXIDS_MISSING: subxids_array_status = 1;
pub const SUBXIDS_IN_SUBTRANS: subxids_array_status = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunningTransactionsData {
    pub xcnt: c_int,
    pub subxcnt: c_int,
    pub subxid_status: subxids_array_status,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: *mut TransactionId,
}

pub const LP_UNUSED: c_uint = 0;
pub const LP_NORMAL: c_uint = 1;
pub const LP_REDIRECT: c_uint = 2;
pub const LP_DEAD: c_uint = 3;

pub const PD_HAS_FREE_LINES: uint16 = 0x0001;
pub const PD_PAGE_FULL: uint16 = 0x0002;
pub const PD_ALL_VISIBLE: uint16 = 0x0004;
pub const PD_VALID_FLAG_BITS: uint16 = 0x0007;
pub const PG_PAGE_LAYOUT_VERSION: uint8 = 4;
pub const PG_DATA_CHECKSUM_VERSION: uint16 = 1;
pub const SizeOfPageHeaderData: Size = core::mem::offset_of!(PageHeaderData, pd_linp);
pub const PAI_OVERWRITE: c_int = 1 << 0;
pub const PAI_IS_HEAP: c_int = 1 << 1;
pub const PIV_LOG_WARNING: c_int = 1 << 0;
pub const PIV_LOG_LOG: c_int = 1 << 1;
pub const PIV_IGNORE_CHECKSUM_FAILURE: c_int = 1 << 2;
pub const MaxHeapTuplesPerPage: c_int =
    ((BLCKSZ - SizeOfPageHeaderData) / (24 + core::mem::size_of::<ItemIdData>())) as c_int;
pub const MaxIndexTuplesPerPage: c_int = MaxHeapTuplesPerPage;

pub const DEFAULT_SPINS_PER_DELAY: c_int = 100;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_WAIT_UNTIL_FREE: LWLockMode = 2;

/// `LWLockWaitState` (storage/lwlock.h) — the `PGPROC.lwWaiting` state byte set
/// and read by the LWLock wait-list machinery. Stored as a `uint8` in PGPROC.
pub type LWLockWaitState = uint8;
/// not currently waiting / woken up
pub const LW_WS_NOT_WAITING: LWLockWaitState = 0;
/// currently waiting
pub const LW_WS_WAITING: LWLockWaitState = 1;
/// removed from waitlist, but not yet signaled
pub const LW_WS_PENDING_WAKEUP: LWLockWaitState = 2;

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
pub const NUM_PROCSIGNALS: usize =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK as usize + 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalBarrierType {
    PROCSIGNAL_BARRIER_SMGRRELEASE = 0,
}

pub const MAX_BACKENDS_BITS: c_int = 18;
pub const MAX_BACKENDS: uint32 = (1_u32 << MAX_BACKENDS_BITS) - 1;

pub const NUM_INDIVIDUAL_LWLOCKS: c_int = 54;
pub const NUM_BUFFER_PARTITIONS: c_int = 128;
pub const LOG2_NUM_LOCK_PARTITIONS: c_int = 4;
pub const NUM_LOCK_PARTITIONS: c_int = 1 << LOG2_NUM_LOCK_PARTITIONS;
pub const LOG2_NUM_PREDICATELOCK_PARTITIONS: c_int = 4;
pub const NUM_PREDICATELOCK_PARTITIONS: c_int = 1 << LOG2_NUM_PREDICATELOCK_PARTITIONS;
pub const BUFFER_MAPPING_LWLOCK_OFFSET: c_int = NUM_INDIVIDUAL_LWLOCKS;
pub const LOCK_MANAGER_LWLOCK_OFFSET: c_int = BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS;
pub const PREDICATELOCK_MANAGER_LWLOCK_OFFSET: c_int =
    LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS;
pub const NUM_FIXED_LWLOCKS: c_int =
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS;
pub const LWLOCK_PADDED_SIZE: usize = 128;

pub const LWTRANCHE_XACT_BUFFER: c_int = NUM_INDIVIDUAL_LWLOCKS;
pub const LWTRANCHE_COMMITTS_BUFFER: c_int = LWTRANCHE_XACT_BUFFER + 1;
pub const LWTRANCHE_SUBTRANS_BUFFER: c_int = LWTRANCHE_COMMITTS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_BUFFER: c_int = LWTRANCHE_SUBTRANS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_BUFFER: c_int = LWTRANCHE_MULTIXACTOFFSET_BUFFER + 1;
pub const LWTRANCHE_NOTIFY_BUFFER: c_int = LWTRANCHE_MULTIXACTMEMBER_BUFFER + 1;
pub const LWTRANCHE_SERIAL_BUFFER: c_int = LWTRANCHE_NOTIFY_BUFFER + 1;
pub const LWTRANCHE_WAL_INSERT: c_int = LWTRANCHE_SERIAL_BUFFER + 1;
pub const LWTRANCHE_BUFFER_CONTENT: c_int = LWTRANCHE_WAL_INSERT + 1;
pub const LWTRANCHE_REPLICATION_ORIGIN_STATE: c_int = LWTRANCHE_BUFFER_CONTENT + 1;
pub const LWTRANCHE_REPLICATION_SLOT_IO: c_int = LWTRANCHE_REPLICATION_ORIGIN_STATE + 1;
pub const LWTRANCHE_LOCK_FASTPATH: c_int = LWTRANCHE_REPLICATION_SLOT_IO + 1;
pub const LWTRANCHE_BUFFER_MAPPING: c_int = LWTRANCHE_LOCK_FASTPATH + 1;
pub const LWTRANCHE_LOCK_MANAGER: c_int = LWTRANCHE_BUFFER_MAPPING + 1;
pub const LWTRANCHE_PREDICATE_LOCK_MANAGER: c_int = LWTRANCHE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_HASH_JOIN: c_int = LWTRANCHE_PREDICATE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_BTREE_SCAN: c_int = LWTRANCHE_PARALLEL_HASH_JOIN + 1;
pub const LWTRANCHE_PARALLEL_QUERY_DSA: c_int = LWTRANCHE_PARALLEL_BTREE_SCAN + 1;
pub const LWTRANCHE_PER_SESSION_DSA: c_int = LWTRANCHE_PARALLEL_QUERY_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPE: c_int = LWTRANCHE_PER_SESSION_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPMOD: c_int = LWTRANCHE_PER_SESSION_RECORD_TYPE + 1;
pub const LWTRANCHE_SHARED_TUPLESTORE: c_int = LWTRANCHE_PER_SESSION_RECORD_TYPMOD + 1;
pub const LWTRANCHE_SHARED_TIDBITMAP: c_int = LWTRANCHE_SHARED_TUPLESTORE + 1;
pub const LWTRANCHE_PARALLEL_APPEND: c_int = LWTRANCHE_SHARED_TIDBITMAP + 1;
pub const LWTRANCHE_PER_XACT_PREDICATE_LIST: c_int = LWTRANCHE_PARALLEL_APPEND + 1;
pub const LWTRANCHE_PGSTATS_DSA: c_int = LWTRANCHE_PER_XACT_PREDICATE_LIST + 1;
pub const LWTRANCHE_PGSTATS_HASH: c_int = LWTRANCHE_PGSTATS_DSA + 1;
pub const LWTRANCHE_PGSTATS_DATA: c_int = LWTRANCHE_PGSTATS_HASH + 1;
pub const LWTRANCHE_LAUNCHER_DSA: c_int = LWTRANCHE_PGSTATS_DATA + 1;
pub const LWTRANCHE_LAUNCHER_HASH: c_int = LWTRANCHE_LAUNCHER_DSA + 1;
pub const LWTRANCHE_DSM_REGISTRY_DSA: c_int = LWTRANCHE_LAUNCHER_HASH + 1;
pub const LWTRANCHE_DSM_REGISTRY_HASH: c_int = LWTRANCHE_DSM_REGISTRY_DSA + 1;
pub const LWTRANCHE_COMMITTS_SLRU: c_int = LWTRANCHE_DSM_REGISTRY_HASH + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_SLRU: c_int = LWTRANCHE_COMMITTS_SLRU + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_SLRU: c_int = LWTRANCHE_MULTIXACTMEMBER_SLRU + 1;
pub const LWTRANCHE_NOTIFY_SLRU: c_int = LWTRANCHE_MULTIXACTOFFSET_SLRU + 1;
pub const LWTRANCHE_SERIAL_SLRU: c_int = LWTRANCHE_NOTIFY_SLRU + 1;
pub const LWTRANCHE_SUBTRANS_SLRU: c_int = LWTRANCHE_SERIAL_SLRU + 1;
pub const LWTRANCHE_XACT_SLRU: c_int = LWTRANCHE_SUBTRANS_SLRU + 1;
pub const LWTRANCHE_PARALLEL_VACUUM_DSA: c_int = LWTRANCHE_XACT_SLRU + 1;
pub const LWTRANCHE_AIO_URING_COMPLETION: c_int = LWTRANCHE_PARALLEL_VACUUM_DSA + 1;
pub const LWTRANCHE_FIRST_USER_DEFINED: c_int = LWTRANCHE_AIO_URING_COMPLETION + 1;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSet {
    pub creator_pid: pid_t,
    pub number: uint32,
    pub ntablespaces: c_int,
    pub tablespaces: [Oid; 8],
}

impl Default for FileSet {
    fn default() -> Self {
        Self {
            creator_pid: 0,
            number: 0,
            ntablespaces: 0,
            tablespaces: [0; 8],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedFileSet {
    pub fs: FileSet,
    pub mutex: slock_t,
    pub refcnt: c_int,
}

#[repr(C)]
pub struct BufFile {
    _private: [u8; 0],
}

/// Metadata describing a worker's materialized logical tape, passed through
/// shared memory to the leader so it can reconstruct the tape via
/// `LogicalTapeImport`.
///
/// ABI-exact, on-disk/shared-memory layout: must match the C `TapeShare`
/// (single `int64`) from `src/include/utils/logtape.h` byte-for-byte.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TapeShare {
    /// Location of the materialized tape's first block.
    pub firstblocknumber: i64,
}

/// On-disk block trailer stored at the end of every BLCKSZ logical-tape block.
///
/// The first block of a tape has `prev == -1`.  The last block of a tape stores
/// the number of valid bytes on the block, inverted, in `next`; therefore
/// `next < 0` indicates the last block.
///
/// ABI-exact, on-disk layout: must match the C `TapeBlockTrailer` (two `int64`)
/// from `src/backend/utils/sort/logtape.c` byte-for-byte.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TapeBlockTrailer {
    /// previous block on this tape, or -1 on first block
    pub prev: i64,
    /// next block on this tape, or # of valid bytes on last block (if < 0)
    pub next: i64,
}

#[repr(C)]
pub struct dsm_segment {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/// `proclist_node` (storage/proclist_types.h) — a node in a doubly-linked list
/// of PGPROCs identified by pgprocno. The link fields hold the 0-based PGPROC
/// indexes of the next/prev process, or `INVALID_PROC_NUMBER` at the ends. A
/// node not in any list has `next == prev == 0`. In PostgreSQL this is embedded
/// in each `PGPROC` (e.g. the `lwWaitLink` member).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct proclist_node {
    /// pgprocno of the next PGPROC
    pub next: ProcNumber,
    /// pgprocno of the prev PGPROC
    pub prev: ProcNumber,
}

impl Default for proclist_node {
    fn default() -> Self {
        Self { next: 0, prev: 0 }
    }
}

#[repr(C)]
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

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: proclist_head,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union LWLockPadded {
    pub lock: LWLock,
    pub pad: [c_char; LWLOCK_PADDED_SIZE],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NamedLWLockTranche {
    pub trancheId: c_int,
    pub trancheName: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LWLockHandle {
    pub lock: *mut LWLock,
    pub mode: LWLockMode,
}

/// `Barrier` (storage/barrier.h) — a phased synchronization barrier used by
/// parallel hash join (`build_barrier`, `batch_barrier`, the grow barriers).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Barrier {
    pub mutex: slock_t,
    /// phase counter
    pub phase: c_int,
    /// the number of participants attached
    pub participants: c_int,
    /// the number of participants that have arrived
    pub arrived: c_int,
    /// highest phase elected
    pub elected: c_int,
    /// used only for assertions
    pub static_party: bool,
    pub condition_variable: crate::execnodes::ConditionVariable,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct shm_toc_entry {
    pub key: uint64,
    pub offset: Size,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct shm_toc {
    pub toc_magic: uint64,
    pub toc_mutex: slock_t,
    pub toc_total_bytes: Size,
    pub toc_allocated_bytes: Size,
    pub toc_nentry: uint32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/// `shm_mq` (storage/shm_mq.c) — the single-reader, single-writer shared-memory
/// message queue header.  This struct is SHMEM-resident: it lives at the start
/// of a caller-provided shared-memory (DSM) region, and the variable-length ring
/// buffer follows the header at `mq_ring` (a C `FLEXIBLE_ARRAY_MEMBER`, modelled
/// here as a zero-length array so the header size/offsets match the C layout).
///
/// `mq_bytes_read` / `mq_bytes_written` are accessed atomically (8-byte loads
/// and stores); `mq_mutex` is a real `slock_t` spinlock; `mq_receiver` /
/// `mq_sender` are `PGPROC *` shmem addresses.
#[repr(C)]
pub struct shm_mq {
    /// `slock_t mq_mutex` — spinlock guarding the sender/receiver pointers.
    pub mq_mutex: slock_t,
    /// `PGPROC *mq_receiver` — set once by the receiver.
    pub mq_receiver: *mut PGPROC,
    /// `PGPROC *mq_sender` — set once by the sender.
    pub mq_sender: *mut PGPROC,
    /// `pg_atomic_uint64 mq_bytes_read` — total bytes the receiver has consumed.
    pub mq_bytes_read: crate::relscan::pg_atomic_uint64,
    /// `pg_atomic_uint64 mq_bytes_written` — total bytes the sender has written.
    pub mq_bytes_written: crate::relscan::pg_atomic_uint64,
    /// `Size mq_ring_size` — usable ring-buffer length (immutable after create).
    pub mq_ring_size: Size,
    /// `bool mq_detached` — set false→true by either side; needs no lock.
    pub mq_detached: bool,
    /// `uint8 mq_ring_offset` — padding from `mq_ring` to the MAXALIGN'd ring.
    pub mq_ring_offset: uint8,
    /// `char mq_ring[FLEXIBLE_ARRAY_MEMBER]` — start of the ring buffer.
    pub mq_ring: [c_char; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpinDelayStatus {
    spins: c_int,
    delays: c_int,
    cur_delay: c_int,
    file: *const c_char,
    line: c_int,
    func: *const c_char,
}

impl SpinDelayStatus {
    /// Build a status value from raw PostgreSQL location pointers.
    ///
    /// # Safety
    ///
    /// `file` and `func`, when non-null, must point to valid nul-terminated
    /// strings for as long as the status may be used. PostgreSQL normally
    /// supplies static `__FILE__` and `__func__` strings.
    pub const unsafe fn from_raw_parts(
        file: *const c_char,
        line: c_int,
        func: *const c_char,
    ) -> Self {
        Self {
            spins: 0,
            delays: 0,
            cur_delay: 0,
            file,
            line,
            func,
        }
    }

    pub const fn spins(&self) -> c_int {
        self.spins
    }

    pub const fn delays(&self) -> c_int {
        self.delays
    }

    pub const fn cur_delay(&self) -> c_int {
        self.cur_delay
    }

    pub const fn file(&self) -> *const c_char {
        self.file
    }

    pub const fn line(&self) -> c_int {
        self.line
    }

    pub const fn func(&self) -> *const c_char {
        self.func
    }

    pub fn set_spins(&mut self, spins: c_int) {
        self.spins = spins;
    }

    pub fn set_delays(&mut self, delays: c_int) {
        self.delays = delays;
    }

    pub fn set_cur_delay(&mut self, cur_delay: c_int) {
        self.cur_delay = cur_delay;
    }
}

pub const PGShmemMagic: i32 = 679_834_894;
pub const SHMEM_INDEX_KEYSIZE: usize = 48;
pub const SHMEM_INDEX_SIZE: usize = 64;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemIdData {
    raw: uint32,
}

impl ItemIdData {
    pub const fn new(lp_off: ItemOffset, lp_flags: c_uint, lp_len: ItemLength) -> Self {
        Self {
            raw: (lp_off as uint32 & 0x7fff)
                | ((lp_flags as uint32 & 0x0003) << 15)
                | ((lp_len as uint32 & 0x7fff) << 17),
        }
    }

    pub const fn lp_off(&self) -> ItemOffset {
        (self.raw & 0x7fff) as ItemOffset
    }

    pub const fn lp_flags(&self) -> c_uint {
        ((self.raw >> 15) & 0x0003) as c_uint
    }

    pub const fn lp_len(&self) -> ItemLength {
        ((self.raw >> 17) & 0x7fff) as ItemLength
    }

    pub fn set_unused(&mut self) {
        *self = Self::new(0, LP_UNUSED, 0);
    }

    pub fn set_normal(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, LP_NORMAL, len);
    }

    /// Update the item's offset and length without changing its lp_flags field.
    ///
    /// Mirrors bufpage.c:1483-1485, where PageIndexTupleOverwrite writes
    /// `tupid->lp_off` and `tupid->lp_len` directly while preserving the
    /// existing lp_flags (e.g. LP_DEAD).
    pub fn set_storage(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, self.lp_flags(), len);
    }

    pub fn set_redirect(&mut self, link: OffsetNumber) {
        *self = Self::new(link, LP_REDIRECT, 0);
    }

    pub fn set_dead(&mut self) {
        *self = Self::new(0, LP_DEAD, 0);
    }

    pub fn mark_dead(&mut self) {
        *self = Self::new(self.lp_off(), LP_DEAD, self.lp_len());
    }

    pub const fn is_used(&self) -> bool {
        self.lp_flags() != LP_UNUSED
    }

    pub const fn is_normal(&self) -> bool {
        self.lp_flags() == LP_NORMAL
    }

    pub const fn is_redirected(&self) -> bool {
        self.lp_flags() == LP_REDIRECT
    }

    pub const fn is_dead(&self) -> bool {
        self.lp_flags() == LP_DEAD
    }

    pub const fn has_storage(&self) -> bool {
        self.lp_len() != 0
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PageXLogRecPtr {
    pub xlogid: uint32,
    pub xrecoff: uint32,
}

impl PageXLogRecPtr {
    pub const fn from_lsn(lsn: XLogRecPtr) -> Self {
        Self {
            xlogid: (lsn >> 32) as uint32,
            xrecoff: lsn as uint32,
        }
    }

    pub const fn lsn(&self) -> XLogRecPtr {
        ((self.xlogid as XLogRecPtr) << 32) | self.xrecoff as XLogRecPtr
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageHeaderData {
    pub pd_lsn: PageXLogRecPtr,
    pub pd_checksum: uint16,
    pub pd_flags: uint16,
    pub pd_lower: LocationIndex,
    pub pd_upper: LocationIndex,
    pub pd_special: LocationIndex,
    pub pd_pagesize_version: uint16,
    pub pd_prune_xid: TransactionId,
    pub pd_linp: [ItemIdData; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PGShmemHeader {
    pub magic: i32,
    pub creatorPID: pid_t,
    pub totalsize: Size,
    pub freeoffset: Size,
    pub dsm_control: dsm_handle,
    pub index: *mut c_void,
    pub device: dev_t,
    pub inode: ino_t,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShmemIndexEnt {
    pub key: [c_char; SHMEM_INDEX_KEYSIZE],
    pub location: *mut c_void,
    pub size: Size,
    pub allocated_size: Size,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalCatcacheMsg {
    pub id: i8,
    pub dbId: Oid,
    pub hashValue: uint32,
}

pub const SHAREDINVALCATALOG_ID: i8 = -1;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalCatalogMsg {
    pub id: i8,
    pub dbId: Oid,
    pub catId: Oid,
}

pub const SHAREDINVALRELCACHE_ID: i8 = -2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalRelcacheMsg {
    pub id: i8,
    pub dbId: Oid,
    pub relId: Oid,
}

pub const SHAREDINVALSMGR_ID: i8 = -3;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalSmgrMsg {
    pub id: i8,
    pub backend_hi: i8,
    pub backend_lo: uint16,
    pub rlocator: RelFileLocator,
}

pub const SHAREDINVALRELMAP_ID: i8 = -4;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalRelmapMsg {
    pub id: i8,
    pub dbId: Oid,
}

pub const SHAREDINVALSNAPSHOT_ID: i8 = -5;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalSnapshotMsg {
    pub id: i8,
    pub dbId: Oid,
    pub relId: Oid,
}

pub const SHAREDINVALRELSYNC_ID: i8 = -6;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedInvalRelSyncMsg {
    pub id: i8,
    pub dbId: Oid,
    pub relid: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union SharedInvalidationMessage {
    pub id: i8,
    pub cc: SharedInvalCatcacheMsg,
    pub cat: SharedInvalCatalogMsg,
    pub rc: SharedInvalRelcacheMsg,
    pub sm: SharedInvalSmgrMsg,
    pub rm: SharedInvalRelmapMsg,
    pub sn: SharedInvalSnapshotMsg,
    pub rs: SharedInvalRelSyncMsg,
}

// ---------------------------------------------------------------------------
// Storage substrate shared ABI vocabulary
//
// These mirror the C definitions in src/include/storage/{relfilelocator.h,
// smgr.h, lock.h, buf_internals.h}. They are placed here because they cross
// the C boundary (SMgrRelationData is referenced by the buffer manager, the
// catalog, AIO, etc.) and must keep an identical layout.
// ---------------------------------------------------------------------------

/// `RelFileLocatorBackend` (relfilelocator.h): a `RelFileLocator` plus the
/// owning backend (`InvalidBackendId` / shared `INVALID_PROC_NUMBER` for
/// permanent relations, a real `ProcNumber` for backend-local temp rels).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

impl RelFileLocatorBackend {
    pub const fn new(locator: RelFileLocator, backend: ProcNumber) -> Self {
        Self { locator, backend }
    }

    /// True for a backend-local temporary relation (`RelFileLocatorBackendIsTemp`).
    pub const fn is_temp(&self) -> bool {
        self.backend != INVALID_PROC_NUMBER
    }
}

/// Number of forks (`MAX_FORKNUM + 1`); used to size `smgr_cached_nblocks`.
pub const SMGR_NFORKS: usize = (MAX_FORKNUM + 1) as usize;

/// `SMgrId` enum from smgr.h. Only the magnetic-disk manager exists today.
pub type SMgrId = c_uint;
pub const SMGR_INVALID: SMgrId = 0xffff_ffff;
pub const SMGR_MD: SMgrId = 0;

/// `SMgrRelationData` (smgr.h). The lower-layer private state (`md_*`) is
/// intentionally opaque here; the smgr crate keeps it in its own owned table
/// rather than as raw `*mut _MdfdVec` arrays, so this struct only carries the
/// boundary-visible fields. `repr(C)` preserves the field order for any C
/// caller that inspects `smgr_rlocator`/`smgr_cached_nblocks`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SMgrRelationData {
    pub smgr_rlocator: RelFileLocatorBackend,
    pub smgr_targblock: BlockNumber,
    pub smgr_cached_nblocks: [BlockNumber; SMGR_NFORKS],
    pub smgr_which: c_int,
}

pub type SMgrRelation = *mut SMgrRelationData;

// --- Lock manager vocabulary (lock.h / lockdefs.h) -------------------------
// `LOCKMODE` and the AccessShareLock..AccessExclusiveLock constants live in
// the access module of this crate; we reuse them here.
use crate::access::LOCKMODE;

pub type LOCKMASK = c_int;
pub type LOCKMETHODID = uint16;

/// Highest defined lock mode (`AccessExclusiveLock`).
pub const MaxLockMode: LOCKMODE = 8;

pub const DEFAULT_LOCKMETHOD: LOCKMETHODID = 1;
pub const USER_LOCKMETHOD: LOCKMETHODID = 2;

pub type LockTagType = u8;
pub const LOCKTAG_RELATION: LockTagType = 0;
pub const LOCKTAG_RELATION_EXTEND: LockTagType = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: LockTagType = 2;
pub const LOCKTAG_PAGE: LockTagType = 3;
pub const LOCKTAG_TUPLE: LockTagType = 4;
pub const LOCKTAG_TRANSACTION: LockTagType = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: LockTagType = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: LockTagType = 7;
pub const LOCKTAG_OBJECT: LockTagType = 8;
pub const LOCKTAG_USERLOCK: LockTagType = 9;
pub const LOCKTAG_ADVISORY: LockTagType = 10;
pub const LOCKTAG_APPLY_TRANSACTION: LockTagType = 11;

/// `LOCKTAG_LAST_TYPE` (lock.h): the highest-numbered lock tag type.
pub const LOCKTAG_LAST_TYPE: LockTagType = LOCKTAG_APPLY_TRANSACTION;

/// `XLTW_Oper` (lmgr.h): identifies the operation that needs to wait for
/// another transaction in `XactLockTableWait`, used to set up the verbose
/// error-context callback.
pub type XLTW_Oper = c_uint;
pub const XLTW_None: XLTW_Oper = 0;
pub const XLTW_Update: XLTW_Oper = 1;
pub const XLTW_Delete: XLTW_Oper = 2;
pub const XLTW_Lock: XLTW_Oper = 3;
pub const XLTW_LockUpdated: XLTW_Oper = 4;
pub const XLTW_InsertIndex: XLTW_Oper = 5;
pub const XLTW_InsertIndexUnique: XLTW_Oper = 6;
pub const XLTW_FetchUpdated: XLTW_Oper = 7;
pub const XLTW_RecheckExclusionConstr: XLTW_Oper = 8;

/// `LOCKTAG` (lock.h): the 16-byte key identifying any lockable object.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

pub type LockAcquireResult = c_uint;
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

/// `MAX_LOCKMODES` (lock.h): upper bound on the number of lock modes; sizes the
/// `requested[]`/`granted[]` arrays in `LOCK`. Index 0 is the unused NoLock
/// slot, so valid modes run 1..=MaxLockMode.
pub const MAX_LOCKMODES: usize = 10;

// `dlist_head` / `dclist_head` (ilist.h) are defined canonically in `crate::guc`
// (the full ilist type family lives there); re-used here so the LOCK/PROCLOCK
// shmem structs embed the identical `repr(C)` layout without a duplicate
// definition (which would make the crate-root glob re-export ambiguous).
use crate::guc::{dclist_head, dlist_head};

/// `PROCLOCKTAG` (lock.h): the key for the PROCLOCK hash. `myLock` and `myProc`
/// are absolute shmem addresses (a `LOCK *` and a `PGPROC *`); the tag need only
/// be unique for the PROCLOCK's lifespan. "NB: we assume this struct contains no
/// padding" — two pointers, so it is 16 bytes with no padding, matching C.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PROCLOCKTAG {
    pub myLock: *mut LOCK,
    pub myProc: *mut core::ffi::c_void,
}

/// `LOCK` (lock.h): the per-locked-object record stored in `LockMethodLockHash`.
/// Lives in shared memory; `repr(C)` with exact PostgreSQL field order so the
/// dynahash-allocated shmem bytes can be reinterpreted as this struct.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LOCK {
    /// hash key
    pub tag: LOCKTAG,
    /// bitmask for lock types already granted
    pub grantMask: LOCKMASK,
    /// bitmask for lock types awaited
    pub waitMask: LOCKMASK,
    /// list of PROCLOCK objects assoc. with lock
    pub procLocks: dlist_head,
    /// list of PGPROC objects waiting on lock
    pub waitProcs: dclist_head,
    /// counts of requested locks
    pub requested: [c_int; MAX_LOCKMODES],
    /// total of requested[] array
    pub nRequested: c_int,
    /// counts of granted locks
    pub granted: [c_int; MAX_LOCKMODES],
    /// total of granted[] array
    pub nGranted: c_int,
}

/// `PROCLOCK` (lock.h): per-lock-per-holder record stored in
/// `LockMethodProcLockHash`. Lives in shared memory; `repr(C)` with exact field
/// order. `groupLeader` is a `PGPROC *` shmem address.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PROCLOCK {
    /// unique identifier of proclock object
    pub tag: PROCLOCKTAG,
    /// proc's lock group leader, or proc itself
    pub groupLeader: *mut core::ffi::c_void,
    /// bitmask for lock types currently held
    pub holdMask: LOCKMASK,
    /// bitmask for lock types to be released
    pub releaseMask: LOCKMASK,
    /// list link in LOCK's list of proclocks
    pub lockLink: crate::guc::dlist_node,
    /// list link in PGPROC's list of proclocks
    pub procLink: crate::guc::dlist_node,
}

// --- Buffer manager vocabulary (buf_internals.h / buf.h) -------------------
// `InvalidBuffer` (== 0) is defined in the executor module of this crate.

/// `BufferTag` (buf_internals.h): identifies the block held in a buffer.
/// Mirrors the canonical PostgreSQL field order. Note this differs from the
/// convenience `BufferTag` in pgrust-pg-traits, which carries split Oids.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct buftag {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
    pub forkNum: ForkNumber,
    pub blockNum: BlockNumber,
}

/// `BUF_FLAG_MASK` / `BUF_USAGECOUNT_*` packing for `BufferDesc.state`.
pub const BUF_REFCOUNT_BITS: u32 = 18;
/// `BUF_REFCOUNT_ONE` (buf_internals.h): one shared pin = the lowest bit.
pub const BUF_REFCOUNT_ONE: u32 = 1;
pub const BUF_USAGECOUNT_ONE: u32 = 1 << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_MASK: u32 = 0x003C_0000;
pub const BUF_REFCOUNT_MASK: u32 = (1 << BUF_REFCOUNT_BITS) - 1;
pub const BUF_FLAG_MASK: u32 = 0xFFC0_0000;
pub const BM_MAX_USAGE_COUNT: u32 = 5;

pub const BM_LOCKED: u32 = 1 << 22;
pub const BM_DIRTY: u32 = 1 << 23;
pub const BM_VALID: u32 = 1 << 24;
pub const BM_TAG_VALID: u32 = 1 << 25;
pub const BM_IO_IN_PROGRESS: u32 = 1 << 26;
pub const BM_IO_ERROR: u32 = 1 << 27;
pub const BM_JUST_DIRTIED: u32 = 1 << 28;
pub const BM_PIN_COUNT_WAITER: u32 = 1 << 29;
pub const BM_CHECKPOINT_NEEDED: u32 = 1 << 30;
pub const BM_PERMANENT: u32 = 1 << 31;

/// `LockBuffer` content-lock modes (bufmgr.h).
pub const BUFFER_LOCK_UNLOCK: c_int = 0;
pub const BUFFER_LOCK_SHARE: c_int = 1;
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

/// `PgAioWaitRef` (aio_types.h): a reference to an in-flight async I/O handle.
/// Three `uint32`s (the generation is split to avoid int64 alignment). Carried
/// in `BufferDesc` for exact shmem layout parity; the AIO machinery itself is
/// deferred.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgAioWaitRef {
    pub aio_index: uint32,
    pub generation_upper: uint32,
    pub generation_lower: uint32,
}

/// `BufferDesc` (buf_internals.h): the per-buffer header. `repr(C)` and
/// field-for-field with C so the shmem-resident descriptor array is binary
/// compatible with a co-resident C backend's `GetBufferDescriptor(i)`. `state`
/// is the packed `pg_atomic_uint32` (flags | usagecount | shared refcount).
///
/// `io_wref` and `content_lock` are carried for layout fidelity even though the
/// AIO and content-lock machinery are facaded/deferred in the buffer manager
/// port: omitting them would shrink the struct and desynchronize the array
/// stride from C (`sizeof(BufferDescPadded) == 64`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BufferDesc {
    /// `BufferTag tag` — ID of page contained in buffer; valid when BM_TAG_VALID.
    pub tag: buftag,
    /// `int buf_id` — buffer's index number (from 0); never changes.
    pub buf_id: c_int,
    /// `pg_atomic_uint32 state` — flags | usagecount | shared refcount.
    pub state: pg_atomic_uint32,
    /// `int wait_backend_pgprocno` — backend of pin-count waiter.
    pub wait_backend_pgprocno: c_int,
    /// `int freeNext` — link in freelist chain (protected by
    /// `buffer_strategy_lock`, not the header lock).
    pub freeNext: c_int,
    /// `PgAioWaitRef io_wref` — set iff AIO is in progress.
    pub io_wref: PgAioWaitRef,
    /// `LWLock content_lock` — to lock access to buffer contents.
    pub content_lock: LWLock,
}

/// `BUFFERDESC_PAD_TO_SIZE` (buf_internals.h): on 64-bit, `BufferDescPadded` is
/// padded to a 64-byte cache line so the descriptor array is cache-line aligned
/// and false-sharing-free, exactly as C arranges `BufferDescriptors`.
pub const BUFFERDESC_PAD_TO_SIZE: usize = 64;

/// `BufferDescPadded` (buf_internals.h): a union of `BufferDesc` with a
/// 64-byte pad, giving the descriptor array a 64-byte stride that matches C.
#[repr(C)]
#[derive(Clone, Copy)]
pub union BufferDescPadded {
    pub bufferdesc: BufferDesc,
    pub pad: [c_char; BUFFERDESC_PAD_TO_SIZE],
}

// ---------------------------------------------------------------------------
// PGPROC / PROC_HDR — per-process shared memory data structures (storage/proc.h)
//
// These are the shmem-resident structures owned by `proc.c` (ported in the
// `backend-storage-lmgr-proc` crate). They are `repr(C)` with the exact
// PostgreSQL field order so the `ShmemInitStruct`-allocated bytes can be
// reinterpreted as these structs and shared coherently across backends.
// ---------------------------------------------------------------------------

/// `PGPROC_MAX_CACHED_SUBXIDS` (proc.h): per-backend advertised subxid cache size.
pub const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

/// `XidCacheStatus` (proc.h): the subxid-cache status mirrored into
/// `PROC_HDR->subxidStates[]`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XidCacheStatus {
    /// number of cached subxids, never more than `PGPROC_MAX_CACHED_SUBXIDS`
    pub count: uint8,
    /// has `PGPROC->subxids` overflowed
    pub overflowed: bool,
}

/// `struct XidCache` (proc.h): per-backend cache of subtransaction XIDs.
#[repr(C)]
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

/// `ProcWaitStatus` (proc.h): result of joining/waiting on a lock's wait queue.
/// `OK` = lock granted, `WAITING` = on the queue (must `ProcSleep`), `ERROR` =
/// deadlock detected or `dontWait`.
pub type ProcWaitStatus = c_uint;
pub const PROC_WAIT_STATUS_OK: ProcWaitStatus = 0;
pub const PROC_WAIT_STATUS_WAITING: ProcWaitStatus = 1;
pub const PROC_WAIT_STATUS_ERROR: ProcWaitStatus = 2;

/// Flags for `PGPROC->statusFlags` and `PROC_HDR->statusFlags[]` (proc.h).
pub const PROC_IS_AUTOVACUUM: uint8 = 0x01;
pub const PROC_IN_VACUUM: uint8 = 0x02;
pub const PROC_IN_SAFE_IC: uint8 = 0x04;
pub const PROC_VACUUM_FOR_WRAPAROUND: uint8 = 0x08;
pub const PROC_IN_LOGICAL_DECODING: uint8 = 0x10;
pub const PROC_AFFECTS_ALL_HORIZONS: uint8 = 0x20;
/// `PROC_VACUUM_STATE_MASK` (proc.h): flags reset at EOXact.
pub const PROC_VACUUM_STATE_MASK: uint8 =
    PROC_IN_VACUUM | PROC_IN_SAFE_IC | PROC_VACUUM_FOR_WRAPAROUND;
/// `PROC_XMIN_FLAGS` (proc.h): flags affecting how the proc's Xmin is interpreted.
pub const PROC_XMIN_FLAGS: uint8 = PROC_IN_VACUUM | PROC_IN_SAFE_IC;

/// `DELAY_CHKPT_START` / `DELAY_CHKPT_COMPLETE` (proc.h): `PGPROC.delayChkptFlags`.
pub const DELAY_CHKPT_START: c_int = 1 << 0;
pub const DELAY_CHKPT_COMPLETE: c_int = 1 << 1;

/// `NUM_SPECIAL_WORKER_PROCS` (proc.h): extra PGPROCs for "special worker"
/// processes (autovacuum launcher + slotsync worker).
pub const NUM_SPECIAL_WORKER_PROCS: c_int = 2;
/// `MAX_IO_WORKERS` (proc.h).
pub const MAX_IO_WORKERS: c_int = 32;
/// `NUM_AUXILIARY_PROCS` (proc.h): extra PGPROCs for auxiliary processes.
pub const NUM_AUXILIARY_PROCS: c_int = 6 + MAX_IO_WORKERS;

/// `FP_LOCK_GROUPS_PER_BACKEND_MAX` / `FP_LOCK_SLOTS_PER_GROUP` (proc.h).
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: c_int = 1024;
pub const FP_LOCK_SLOTS_PER_GROUP: c_int = 16;

/// The inner anonymous `vxid` struct of `PGPROC`: the currently-running
/// top-level transaction's virtual xid, kept as two separately-assignable parts.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PGProcVxid {
    /// For regular backends, equal to `GetNumberFromPGProc(proc)`. For prepared
    /// xacts, ID of the original backend. For unused entries, `INVALID_PROC_NUMBER`.
    pub procNumber: ProcNumber,
    /// local id of the top-level transaction, or `InvalidLocalTransactionId`.
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

/// `PGPROC` (storage/proc.h): the per-backend shared-memory slot. `repr(C)` with
/// the exact PostgreSQL field order. `sem` is the per-process semaphore, kept as
/// a raw pointer matching C's `PGSemaphore` typedef (`PGSemaphoreData *`).
/// `fpLockBits` / `fpRelId` point into the separately-allocated fast-path array
/// (variable length, so not embedded directly).
#[repr(C)]
pub struct PGPROC {
    /// list link if process is in a list
    pub links: crate::guc::dlist_node,
    /// procglobal list that owns this PGPROC
    pub procgloballist: *mut dlist_head,

    /// ONE semaphore to sleep on (`PGSemaphore` = `PGSemaphoreData *` in C)
    pub sem: *mut c_void,
    pub waitStatus: ProcWaitStatus,

    /// generic latch for process
    pub procLatch: crate::net::Latch,

    /// id of top-level transaction currently being executed (mirrored in
    /// `ProcGlobal->xids[pgxactoff]`)
    pub xid: TransactionId,
    /// minimal running XID as it was when we were starting our xact
    pub xmin: TransactionId,

    /// Backend's process ID; 0 if prepared xact
    pub pid: c_int,
    /// offset into the dense `ProcGlobal` arrays
    pub pgxactoff: c_int,

    /// currently-running top-level transaction's virtual xid
    pub vxid: PGProcVxid,

    /// OID of database this backend is using
    pub databaseId: Oid,
    /// OID of role using this backend
    pub roleId: Oid,
    /// OID of temp schema this backend is using
    pub tempNamespaceId: Oid,
    /// true if it's a regular backend
    pub isRegularBackend: bool,

    /// hot-standby: a conflict signal has been sent for the current transaction
    pub recoveryConflictPending: bool,

    /// see `LWLockWaitState` (lwlock.h)
    pub lwWaiting: uint8,
    /// lwlock mode being waited for
    pub lwWaitMode: uint8,
    /// position in LW lock wait list
    pub lwWaitLink: proclist_node,

    /// position in CV wait list
    pub cvWaitLink: proclist_node,

    /// Lock object we're sleeping on (NULL if not waiting)
    pub waitLock: *mut LOCK,
    /// Per-holder info for awaited lock (NULL if not waiting)
    pub waitProcLock: *mut PROCLOCK,
    /// type of lock we're waiting for
    pub waitLockMode: crate::access::LOCKMODE,
    /// bitmask for lock types already held on this object by this backend
    pub heldLocks: LOCKMASK,
    /// time at which wait for lock acquisition started
    pub waitStart: crate::relscan::pg_atomic_uint64,

    /// for `DELAY_CHKPT_*` flags
    pub delayChkptFlags: c_int,

    /// this backend's status flags (mirrored in `ProcGlobal->statusFlags[pgxactoff]`)
    pub statusFlags: uint8,

    /// waiting for this LSN or higher (sync rep)
    pub waitLSN: XLogRecPtr,
    /// wait state for sync rep
    pub syncRepState: c_int,
    /// list link if process is in syncrep queue
    pub syncRepLinks: crate::guc::dlist_node,

    /// PROCLOCK lists, one per lock partition
    pub myProcLocks: [dlist_head; NUM_LOCK_PARTITIONS as usize],

    /// mirrored with `ProcGlobal->subxidStates[i]`
    pub subxidStatus: XidCacheStatus,
    /// cache for subtransaction XIDs
    pub subxids: XidCache,

    /// true, if member of ProcArray group waiting for XID clear
    pub procArrayGroupMember: bool,
    /// next ProcArray group member waiting for XID clear
    pub procArrayGroupNext: pg_atomic_uint32,
    /// latest xid among the transaction's main XID and subtransactions
    pub procArrayGroupMemberXid: TransactionId,

    /// proc's wait information
    pub wait_event_info: uint32,

    /// true, if member of clog group
    pub clogGroupMember: bool,
    /// next clog group member
    pub clogGroupNext: pg_atomic_uint32,
    /// transaction id of clog group member
    pub clogGroupMemberXid: TransactionId,
    /// transaction status of clog group member
    pub clogGroupMemberXidStatus: crate::xact::XidStatus,
    /// clog page corresponding to clog group member's xid
    pub clogGroupMemberPage: i64,
    /// WAL location of commit record for clog group member
    pub clogGroupMemberLsn: XLogRecPtr,

    /// protects per-backend fast-path state
    pub fpInfoLock: LWLock,
    /// lock modes held for each fast-path slot
    pub fpLockBits: *mut uint64,
    /// slots for rel oids
    pub fpRelId: *mut Oid,
    /// are we holding a fast-path VXID lock?
    pub fpVXIDLock: bool,
    /// lxid for fast-path VXID lock
    pub fpLocalTransactionId: LocalTransactionId,

    /// lock group leader, if I'm a member
    pub lockGroupLeader: *mut PGPROC,
    /// list of members, if I'm a leader
    pub lockGroupMembers: dlist_head,
    /// my member link, if I'm a member
    pub lockGroupLink: crate::guc::dlist_node,
}

/// `PROC_HDR` (storage/proc.h): the single cluster-wide process-table header.
/// `repr(C)` with exact PostgreSQL field order. The dense `xids` / `subxidStates`
/// / `statusFlags` arrays are indexed by `PGPROC->pgxactoff`.
#[repr(C)]
pub struct PROC_HDR {
    /// Array of PGPROC structures (not including dummies for prepared txns)
    pub allProcs: *mut PGPROC,
    /// Array mirroring `PGPROC.xid` for each PGPROC currently in the procarray
    pub xids: *mut TransactionId,
    /// Array mirroring `PGPROC.subxidStatus`
    pub subxidStates: *mut XidCacheStatus,
    /// Array mirroring `PGPROC.statusFlags`
    pub statusFlags: *mut uint8,
    /// Length of `allProcs` array
    pub allProcCount: uint32,
    /// Head of list of free PGPROC structures
    pub freeProcs: dlist_head,
    /// Head of list of autovacuum & special worker free PGPROC structures
    pub autovacFreeProcs: dlist_head,
    /// Head of list of bgworker free PGPROC structures
    pub bgworkerFreeProcs: dlist_head,
    /// Head of list of walsender free PGPROC structures
    pub walsenderFreeProcs: dlist_head,
    /// First pgproc waiting for group XID clear
    pub procArrayGroupFirst: pg_atomic_uint32,
    /// First pgproc waiting for group transaction status update
    pub clogGroupFirst: pg_atomic_uint32,
    /// Current slot number of the WAL writer (only one at a time)
    pub walwriterProc: ProcNumber,
    /// Current slot number of the checkpointer (only one at a time)
    pub checkpointerProc: ProcNumber,
    /// Current shared estimate of appropriate spins_per_delay value
    pub spins_per_delay: c_int,
    /// Buffer id of the buffer that Startup process waits for pin on, or -1
    pub startupBufferPinWaitBufId: c_int,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn shmem_layout_matches_generated_macos_shape() {
        assert_eq!(size_of::<PGShmemHeader>(), 56);
        assert_eq!(align_of::<PGShmemHeader>(), 8);
        assert_eq!(offset_of!(PGShmemHeader, magic), 0);
        assert_eq!(offset_of!(PGShmemHeader, creatorPID), 4);
        assert_eq!(offset_of!(PGShmemHeader, totalsize), 8);
        assert_eq!(offset_of!(PGShmemHeader, freeoffset), 16);
        assert_eq!(offset_of!(PGShmemHeader, dsm_control), 24);
        assert_eq!(offset_of!(PGShmemHeader, index), 32);
        assert_eq!(offset_of!(PGShmemHeader, device), 40);
        assert_eq!(offset_of!(PGShmemHeader, inode), 48);
        assert_eq!(size_of::<ShmemIndexEnt>(), 72);
        assert_eq!(offset_of!(ShmemIndexEnt, key), 0);
        assert_eq!(offset_of!(ShmemIndexEnt, location), 48);
        assert_eq!(offset_of!(ShmemIndexEnt, size), 56);
        assert_eq!(offset_of!(ShmemIndexEnt, allocated_size), 64);
        assert_eq!(size_of::<SpinDelayStatus>(), 40);
        assert_eq!(align_of::<SpinDelayStatus>(), 8);
        assert_eq!(offset_of!(SpinDelayStatus, spins), 0);
        assert_eq!(offset_of!(SpinDelayStatus, delays), 4);
        assert_eq!(offset_of!(SpinDelayStatus, cur_delay), 8);
        assert_eq!(offset_of!(SpinDelayStatus, file), 16);
        assert_eq!(offset_of!(SpinDelayStatus, line), 24);
        assert_eq!(offset_of!(SpinDelayStatus, func), 32);
        assert_eq!(size_of::<shm_toc_entry>(), 16);
        assert_eq!(align_of::<shm_toc_entry>(), 8);
        assert_eq!(offset_of!(shm_toc_entry, key), 0);
        assert_eq!(offset_of!(shm_toc_entry, offset), 8);
        assert_eq!(size_of::<shm_toc>(), 40);
        assert_eq!(align_of::<shm_toc>(), 8);
        assert_eq!(offset_of!(shm_toc, toc_magic), 0);
        assert_eq!(offset_of!(shm_toc, toc_mutex), 8);
        assert_eq!(offset_of!(shm_toc, toc_total_bytes), 16);
        assert_eq!(offset_of!(shm_toc, toc_allocated_bytes), 24);
        assert_eq!(offset_of!(shm_toc, toc_nentry), 32);
        assert_eq!(size_of::<shm_toc_estimator>(), 16);
        assert_eq!(align_of::<shm_toc_estimator>(), 8);
        assert_eq!(offset_of!(shm_toc_estimator, space_for_chunks), 0);
        assert_eq!(offset_of!(shm_toc_estimator, number_of_keys), 8);
        assert_eq!(size_of::<shm_mq>(), 56);
        assert_eq!(align_of::<shm_mq>(), 8);
        assert_eq!(offset_of!(shm_mq, mq_mutex), 0);
        assert_eq!(offset_of!(shm_mq, mq_receiver), 8);
        assert_eq!(offset_of!(shm_mq, mq_sender), 16);
        assert_eq!(offset_of!(shm_mq, mq_bytes_read), 24);
        assert_eq!(offset_of!(shm_mq, mq_bytes_written), 32);
        assert_eq!(offset_of!(shm_mq, mq_ring_size), 40);
        assert_eq!(offset_of!(shm_mq, mq_detached), 48);
        assert_eq!(offset_of!(shm_mq, mq_ring_offset), 49);
        assert_eq!(offset_of!(shm_mq, mq_ring), 50);
        assert_eq!(size_of::<pg_atomic_uint32>(), 4);
        assert_eq!(align_of::<pg_atomic_uint32>(), 4);
        assert_eq!(size_of::<proclist_head>(), 8);
        assert_eq!(offset_of!(proclist_head, head), 0);
        assert_eq!(offset_of!(proclist_head, tail), 4);
        assert_eq!(size_of::<proclist_node>(), 8);
        assert_eq!(offset_of!(proclist_node, next), 0);
        assert_eq!(offset_of!(proclist_node, prev), 4);
        assert_eq!(size_of::<LWLock>(), 16);
        assert_eq!(align_of::<LWLock>(), 4);
        assert_eq!(offset_of!(LWLock, tranche), 0);
        assert_eq!(offset_of!(LWLock, state), 4);
        assert_eq!(offset_of!(LWLock, waiters), 8);
        assert_eq!(size_of::<LWLockPadded>(), 128);
        assert_eq!(align_of::<LWLockPadded>(), 4);
        assert_eq!(size_of::<NamedLWLockTranche>(), 16);
        assert_eq!(align_of::<NamedLWLockTranche>(), 8);
        assert_eq!(offset_of!(NamedLWLockTranche, trancheId), 0);
        assert_eq!(offset_of!(NamedLWLockTranche, trancheName), 8);
        assert_eq!(size_of::<LWLockHandle>(), 16);
        assert_eq!(align_of::<LWLockHandle>(), 8);
        // Lock-manager shmem structs (lock.h). Offsets verified against C.
        assert_eq!(size_of::<dlist_head>(), 16);
        assert_eq!(size_of::<dclist_head>(), 24);
        assert_eq!(size_of::<LOCKTAG>(), 16);
        assert_eq!(size_of::<LOCK>(), 152);
        assert_eq!(align_of::<LOCK>(), 8);
        assert_eq!(offset_of!(LOCK, tag), 0);
        assert_eq!(offset_of!(LOCK, grantMask), 16);
        assert_eq!(offset_of!(LOCK, waitMask), 20);
        assert_eq!(offset_of!(LOCK, procLocks), 24);
        assert_eq!(offset_of!(LOCK, waitProcs), 40);
        assert_eq!(offset_of!(LOCK, requested), 64);
        assert_eq!(offset_of!(LOCK, nRequested), 104);
        assert_eq!(offset_of!(LOCK, granted), 108);
        assert_eq!(offset_of!(LOCK, nGranted), 148);
        assert_eq!(size_of::<PROCLOCKTAG>(), 16);
        assert_eq!(offset_of!(PROCLOCKTAG, myLock), 0);
        assert_eq!(offset_of!(PROCLOCKTAG, myProc), 8);
        assert_eq!(size_of::<PROCLOCK>(), 64);
        assert_eq!(align_of::<PROCLOCK>(), 8);
        assert_eq!(offset_of!(PROCLOCK, tag), 0);
        assert_eq!(offset_of!(PROCLOCK, groupLeader), 16);
        assert_eq!(offset_of!(PROCLOCK, holdMask), 24);
        assert_eq!(offset_of!(PROCLOCK, releaseMask), 28);
        assert_eq!(offset_of!(PROCLOCK, lockLink), 32);
        assert_eq!(offset_of!(PROCLOCK, procLink), 48);
        assert_eq!(size_of::<FileSet>(), 44);
        assert_eq!(align_of::<FileSet>(), 4);
        assert_eq!(offset_of!(FileSet, creator_pid), 0);
        assert_eq!(offset_of!(FileSet, number), 4);
        assert_eq!(offset_of!(FileSet, ntablespaces), 8);
        assert_eq!(offset_of!(FileSet, tablespaces), 12);
        assert_eq!(size_of::<SharedFileSet>(), 52);
        assert_eq!(align_of::<SharedFileSet>(), 4);
        assert_eq!(offset_of!(SharedFileSet, fs), 0);
        assert_eq!(offset_of!(SharedFileSet, mutex), 44);
        assert_eq!(offset_of!(SharedFileSet, refcnt), 48);

        // Logical-tape on-disk / shared-memory structs (logtape.h, logtape.c).
        assert_eq!(size_of::<TapeShare>(), 8);
        assert_eq!(align_of::<TapeShare>(), 8);
        assert_eq!(offset_of!(TapeShare, firstblocknumber), 0);
        assert_eq!(size_of::<TapeBlockTrailer>(), 16);
        assert_eq!(align_of::<TapeBlockTrailer>(), 8);
        assert_eq!(offset_of!(TapeBlockTrailer, prev), 0);
        assert_eq!(offset_of!(TapeBlockTrailer, next), 8);

        // PGPROC / PROC_HDR (proc.h). The small fixed-size mirror structs and
        // header field offsets are checked against C; PGPROC's overall size is
        // platform-dependent (LWLock/Latch padding), so only its repr-C
        // alignment and the header layout are asserted here.
        assert_eq!(size_of::<XidCacheStatus>(), 2);
        assert_eq!(align_of::<XidCacheStatus>(), 1);
        assert_eq!(offset_of!(XidCacheStatus, count), 0);
        assert_eq!(offset_of!(XidCacheStatus, overflowed), 1);
        assert_eq!(size_of::<XidCache>(), 4 * PGPROC_MAX_CACHED_SUBXIDS);
        assert_eq!(size_of::<PGProcVxid>(), 8);
        assert_eq!(offset_of!(PGProcVxid, procNumber), 0);
        assert_eq!(offset_of!(PGProcVxid, lxid), 4);
        // PROC_HDR: pointers then a uint32 count then four 16-byte dlist heads.
        assert_eq!(offset_of!(PROC_HDR, allProcs), 0);
        assert_eq!(offset_of!(PROC_HDR, xids), 8);
        assert_eq!(offset_of!(PROC_HDR, subxidStates), 16);
        assert_eq!(offset_of!(PROC_HDR, statusFlags), 24);
        assert_eq!(offset_of!(PROC_HDR, allProcCount), 32);
        assert_eq!(offset_of!(PROC_HDR, freeProcs), 40);
        assert_eq!(offset_of!(PROC_HDR, autovacFreeProcs), 56);
        assert_eq!(offset_of!(PROC_HDR, bgworkerFreeProcs), 72);
        assert_eq!(offset_of!(PROC_HDR, walsenderFreeProcs), 88);
        // PGPROC starts with the dlist_node links + the owning-list pointer.
        assert_eq!(offset_of!(PGPROC, links), 0);
        assert_eq!(offset_of!(PGPROC, procgloballist), 16);
        assert_eq!(offset_of!(PGPROC, sem), 24);
        assert_eq!(offset_of!(PGPROC, waitStatus), 32);

        // Buffer descriptor shmem structs (buf_internals.h). Offsets verified
        // against C: tag@0, buf_id@20, state@24, wait_backend_pgprocno@28,
        // freeNext@32, io_wref@36, content_lock@48, sizeof==64; the padded union
        // gives the descriptor array a 64-byte (cache-line) stride.
        assert_eq!(size_of::<PgAioWaitRef>(), 12);
        assert_eq!(size_of::<buftag>(), 20);
        assert_eq!(offset_of!(BufferDesc, tag), 0);
        assert_eq!(offset_of!(BufferDesc, buf_id), 20);
        assert_eq!(offset_of!(BufferDesc, state), 24);
        assert_eq!(offset_of!(BufferDesc, wait_backend_pgprocno), 28);
        assert_eq!(offset_of!(BufferDesc, freeNext), 32);
        assert_eq!(offset_of!(BufferDesc, io_wref), 36);
        assert_eq!(offset_of!(BufferDesc, content_lock), 48);
        assert_eq!(size_of::<BufferDesc>(), 64);
        assert_eq!(size_of::<BufferDescPadded>(), BUFFERDESC_PAD_TO_SIZE);
        assert_eq!(size_of::<BufferDescPadded>(), 64);
        assert_eq!(align_of::<BufferDescPadded>(), align_of::<BufferDesc>());
    }
}
