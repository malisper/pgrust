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
pub type Timestamp = i64;
pub type TimeOffset = i64;
pub type fsec_t = i32;
pub type DateADT = i32;
pub type TimeADT = i64;
pub type Size = usize;
pub type NodeTag = u32;
pub type AttrNumber = i16;
/// `Index` (`c.h`) — index into an array (e.g. a range-table index).
pub type Index = u32;
/// `InvalidAttrNumber` (`access/attnum.h`).
pub const InvalidAttrNumber: AttrNumber = 0;
/// `Cost` (`nodes.h`) — abstract plan cost, a `double`.
pub type Cost = f64;
/// `Cardinality` (`nodes.h`) — estimated row count, a `double`.
pub type Cardinality = f64;
/// `Selectivity` (`nodes.h`) — fraction-of-rows estimate, a `double`.
pub type Selectivity = f64;
pub type ProtocolVersion = uint32;
/// `ProcNumber` (`procnumber.h`) — was C `int`.
pub type ProcNumber = i32;
pub type uint8 = u8;
pub type uint16 = u16;
pub type uint32 = u32;
pub type uint64 = u64;
pub type int64 = i64;
pub type RmgrId = uint8;
pub type XLogSegNo = uint64;
pub type RepOriginId = uint16;
pub type pg_crc32c = uint32;
pub type RelFileNumber = Oid;
/// `RegProcedure` (`postgres_ext.h`) — a pg_proc OID.
pub type RegProcedure = Oid;
/// `ForkNumber` (`relpath.h`) — was C `int`.
pub type ForkNumber = i32;
/// `Buffer` (`buf.h`) — shared/local buffer id, was C `int`.
pub type Buffer = i32;
pub type OffsetNumber = uint16;

pub const BLCKSZ: usize = 8192;
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

/// `InvalidForkNumber` (`common/relpath.h`).
pub const InvalidForkNumber: ForkNumber = -1;
/// `MAIN_FORKNUM` (`common/relpath.h`).
pub const MAIN_FORKNUM: ForkNumber = 0;
/// `FSM_FORKNUM` (`common/relpath.h`).
pub const FSM_FORKNUM: ForkNumber = 1;
/// `VISIBILITYMAP_FORKNUM` (`common/relpath.h`).
pub const VISIBILITYMAP_FORKNUM: ForkNumber = 2;
/// `INIT_FORKNUM` (`common/relpath.h`).
pub const INIT_FORKNUM: ForkNumber = 3;
/// `MAX_FORKNUM` (`common/relpath.h`) — `INIT_FORKNUM`.
pub const MAX_FORKNUM: ForkNumber = INIT_FORKNUM;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;
pub const MAXPGPATH: usize = 1024;
/// `PG_DIR_MODE_OWNER` — `0700`, was C `int`.
pub const PG_DIR_MODE_OWNER: i32 = 0o700;
pub const USE_ISO_DATES: i32 = 1;
pub const DATEORDER_MDY: i32 = 2;
pub const INTSTYLE_POSTGRES: i32 = 0;

/// `pgsocket` (`port.h`) — a socket file descriptor (`int` on Unix).
pub type pgsocket = core::ffi::c_int;

/// `PGINVALID_SOCKET` (`port.h`) — the invalid-socket sentinel (`-1` on Unix).
pub const PGINVALID_SOCKET: pgsocket = -1;

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;
