//! PostgreSQL scalar type vocabulary — the foundational width-preserving
//! aliases, constants, and the `OidIsValid` helper.
//!
//! This is the idiomatic replacement for the source crate's `types.rs` glob
//! module (`pub use types::*`). Widths are preserved exactly; `core::ffi::c_int`
//! becomes `i32` since they are identical. `Datum` lives in [`crate::datum`].

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
/// `Index` (`c.h`) — index into an array (e.g. a range-table index).
pub type Index = u32;
/// `ParseLoc` (`nodes/nodes.h`) — a token/parse location, or -1 if unknown.
pub type ParseLoc = i32;
/// `InvalidAttrNumber` (`access/attnum.h`).
pub const InvalidAttrNumber: AttrNumber = 0;
/// `RegProcedure` (`c.h`) — a pg_proc OID ("RegProcedure is the preferred
/// name in C code" for `regproc`).
pub type RegProcedure = Oid;
/// `Cost` (`nodes.h`) — abstract plan cost, a `double`.
pub type Cost = f64;
/// `Cardinality` (`nodes.h`) — estimated row count, a `double`.
pub type Cardinality = f64;
/// `Selectivity` (`nodes.h`) — fraction-of-rows estimate, a `double`.
pub type Selectivity = f64;
pub type ProtocolVersion = uint32;
/// `ProcNumber` (`procnumber.h`) — was C `int`.
pub type ProcNumber = i32;
/// `LocalTransactionId` (`c.h`) — a `uint32`.
pub type LocalTransactionId = u32;
pub type uint8 = u8;
pub type uint16 = u16;
pub type uint32 = u32;
pub type uint64 = u64;
pub type int64 = i64;
/// `bits32` (`c.h`) — a `>= 32` bit unsigned bitmask container.
pub type bits32 = uint32;
pub type RmgrId = uint8;
pub type XLogSegNo = uint64;
pub type pg_crc32c = uint32;
pub type RelFileNumber = Oid;
pub type OffsetNumber = uint16;
/// `RepOriginId` (`xlogdefs.h`) — a `uint16`.
pub type RepOriginId = uint16;
/// POSIX `pid_t`, used as an `int`-width process id throughout PostgreSQL.
pub type pid_t = i32;
/// Platform `sig_atomic_t` (`int` on every supported target).
pub type sig_atomic_t = i32;

pub const BLCKSZ: usize = 8192;
/// `BITS_PER_BYTE` (`c.h`).
pub const BITS_PER_BYTE: i32 = 8;
pub const InvalidOid: Oid = 0;
pub const INVALID_OID: Oid = InvalidOid;


/// `InvalidRepOriginId` — `#define InvalidRepOriginId 0` (`origin.h`).
pub const InvalidRepOriginId: RepOriginId = 0;

/// `OidIsValid(oid)` — `(oid) != InvalidOid` (`c.h`).
#[inline]
pub const fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `InvalidBlockNumber` (`storage/block.h`) — `0xFFFFFFFF`.
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `MaxBlockNumber` (`storage/block.h`) — `0xFFFFFFFE`.
pub const MaxBlockNumber: BlockNumber = 0xFFFF_FFFE;

/// `enum ForkNumber` (`common/relpath.h`).
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
    /// The C `ForkNumber` int as stored in WAL record bodies; `None` for a
    /// value outside the enum (impossible for well-formed WAL).
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
    /// C's zero value (`MAIN_FORKNUM`), for zero-initialized decoded blocks.
    fn default() -> Self {
        ForkNumber::MAIN_FORKNUM
    }
}

/// `Buffer` (`storage/buf.h`) — a buffer-pool slot index (a C `int`):
/// positive = shared buffer, negative = local buffer, 0 = invalid.
pub type Buffer = i32;

/// `InvalidBuffer` (`storage/buf.h`).
pub const InvalidBuffer: Buffer = 0;

/// `BufferIsValid(buffer)` (`storage/bufmgr.h`) — `(buffer) != InvalidBuffer`
/// (after the Assert on the valid range).
#[inline]
pub const fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `InvalidRelFileNumber` (`storage/relfilelocator.h`) — `InvalidOid`.
pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

/// `MAX_FORKNUM` (`common/relpath.h`) — `INIT_FORKNUM`.
pub const MAX_FORKNUM: ForkNumber = ForkNumber::INIT_FORKNUM;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;
/// `FUNC_MAX_ARGS` (`pg_config_manual.h`) — maximum function arguments.
pub const FUNC_MAX_ARGS: usize = 100;
pub const MAXPGPATH: usize = 1024;

/// `pgsocket` (`port.h`) — a socket file descriptor (`int` on Unix).
pub type pgsocket = core::ffi::c_int;

/// `PGINVALID_SOCKET` (`port.h`) — the invalid-socket sentinel (`-1` on Unix).
pub const PGINVALID_SOCKET: pgsocket = -1;

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;

/// `PG_DIR_MODE_OWNER` — `0700`, was C `int` (`common/file_perm.h`).
pub const PG_DIR_MODE_OWNER: i32 = 0o700;
/// `USE_ISO_DATES` (`miscadmin.h`).
pub const USE_ISO_DATES: i32 = 1;
/// `DATEORDER_MDY` (`miscadmin.h`).
pub const DATEORDER_MDY: i32 = 2;
/// `INTSTYLE_POSTGRES` (`miscadmin.h`).
pub const INTSTYLE_POSTGRES: i32 = 0;
