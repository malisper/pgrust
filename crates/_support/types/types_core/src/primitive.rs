pub type PgWChar = u32;
pub type Oid = u32;
pub type BlockNumber = u32;
pub type TransactionId = u32;
pub type MultiXactId = TransactionId;
pub type MultiXactOffset = uint32;
pub type XLogRecPtr = u64;
pub type TimeLineID = u32;
pub type TimestampTz = i64;
pub type pg_time_t = i64;
pub type Size = usize;
pub type AttrNumber = i16;
pub type Index = u32;
pub type ParseLoc = i32;
pub const InvalidAttrNumber: AttrNumber = 0;
pub type RegProcedure = Oid;
pub type Cost = f64;
pub type Cardinality = f64;
pub type Selectivity = f64;
pub type ProtocolVersion = uint32;
pub type ProcNumber = i32;
pub type LocalTransactionId = u32;
pub type uint8 = u8;
pub type uint16 = u16;
pub type uint32 = u32;
pub type uint64 = u64;
pub type int64 = i64;
pub type bits32 = uint32;
pub type RmgrId = uint8;

// rmgrlist.h order: entry order fixes the WAL-visible numeric ids.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RmgrIds {
    RM_XLOG_ID = 0,
    RM_XACT_ID,
    RM_SMGR_ID,
    RM_CLOG_ID,
    RM_DBASE_ID,
    RM_TBLSPC_ID,
    RM_MULTIXACT_ID,
    RM_RELMAP_ID,
    RM_STANDBY_ID,
    RM_HEAP2_ID,
    RM_HEAP_ID,
    RM_BTREE_ID,
    RM_HASH_ID,
    RM_GIN_ID,
    RM_GIST_ID,
    RM_SEQ_ID,
    RM_SPGIST_ID,
    RM_BRIN_ID,
    RM_COMMIT_TS_ID,
    RM_REPLORIGIN_ID,
    RM_GENERIC_ID,
    RM_LOGICALMSG_ID,
    RM_NEXT_ID,
}
pub type XLogSegNo = uint64;
pub type pg_crc32c = uint32;
pub type RelFileNumber = Oid;
pub type OffsetNumber = uint16;
pub type RepOriginId = uint16;
pub type pid_t = i32;
pub type sig_atomic_t = i32;

pub const BLCKSZ: usize = 8192;
pub const BITS_PER_BYTE: i32 = 8;
pub const InvalidOid: Oid = 0;
pub const INVALID_OID: Oid = InvalidOid;

pub const InvalidRepOriginId: RepOriginId = 0;

#[inline]
pub const fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const MaxBlockNumber: BlockNumber = 0xFFFF_FFFE;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ForkNumber {
    InvalidForkNumber = -1,
    MAIN_FORKNUM = 0,
    FSM_FORKNUM = 1,
    VISIBILITYMAP_FORKNUM = 2,
    INIT_FORKNUM = 3,
}

pub use ForkNumber::*;

impl ForkNumber {
    pub const fn from_i32(value: i32) -> Option<ForkNumber> {
        match value {
            -1 => Some(ForkNumber::InvalidForkNumber),
            0 => Some(ForkNumber::MAIN_FORKNUM),
            1 => Some(ForkNumber::FSM_FORKNUM),
            2 => Some(ForkNumber::VISIBILITYMAP_FORKNUM),
            3 => Some(ForkNumber::INIT_FORKNUM),
            _ => None,
        }
    }
}

impl Default for ForkNumber {
    fn default() -> Self {
        ForkNumber::MAIN_FORKNUM
    }
}

/// Buffer-pool slot index: positive = shared, negative = local, 0 = invalid.
pub type Buffer = i32;

pub const InvalidBuffer: Buffer = 0;

#[inline]
pub const fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

pub const MAX_FORKNUM: ForkNumber = ForkNumber::INIT_FORKNUM;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;
pub const FUNC_MAX_ARGS: usize = 100;
pub const MAXPGPATH: usize = 1024;

pub type pgsocket = core::ffi::c_int;

pub const PGINVALID_SOCKET: pgsocket = -1;

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;

pub const PG_DIR_MODE_OWNER: i32 = 0o700;
pub const USE_POSTGRES_DATES: i32 = 0;
pub const USE_ISO_DATES: i32 = 1;
pub const USE_SQL_DATES: i32 = 2;
pub const USE_GERMAN_DATES: i32 = 3;
pub const DATEORDER_YMD: i32 = 0;
pub const DATEORDER_DMY: i32 = 1;
pub const DATEORDER_MDY: i32 = 2;
pub const INTSTYLE_POSTGRES: i32 = 0;

// `GlobalVisState` handle; `id == 0` is C's NULL. Homed here to break the
// storage -> snapshot -> vacuum -> storage cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct GlobalVisStateHandle {
    pub id: u64,
}
impl GlobalVisStateHandle {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
    pub const fn is_none(self) -> bool {
        self.id == 0
    }
}
