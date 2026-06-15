use core::ffi::c_int;

pub type PgWChar = u32;
pub type Oid = u32;
pub type Datum = usize;
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
/// `Cost` (`nodes.h`) — abstract plan cost, a `double`.
pub type Cost = f64;
/// `Cardinality` (`nodes.h`) — estimated row count, a `double`.
pub type Cardinality = f64;
/// `Selectivity` (`nodes.h`) — fraction-of-rows estimate, a `double`.
pub type Selectivity = f64;
pub type ProtocolVersion = uint32;
pub type ProcNumber = c_int;
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
pub type ForkNumber = c_int;
pub type Buffer = c_int;
pub type OffsetNumber = uint16;

pub const BLCKSZ: usize = 8192;
pub const InvalidOid: Oid = 0;
pub const INVALID_OID: Oid = InvalidOid;

/// `InvalidRepOriginId` -- `#define InvalidRepOriginId 0` (`origin.h`).
pub const InvalidRepOriginId: RepOriginId = 0;

/// `OidIsValid(oid)` -- `(oid) != InvalidOid` (`c.h`).
#[inline]
pub const fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}
pub const INVALID_PROC_NUMBER: ProcNumber = -1;
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;
pub const MAXPGPATH: usize = 1024;
pub const PG_DIR_MODE_OWNER: c_int = 0o700;
pub const USE_ISO_DATES: c_int = 1;
pub const DATEORDER_MDY: c_int = 2;
pub const INTSTYLE_POSTGRES: c_int = 0;
