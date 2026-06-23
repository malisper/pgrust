//! ABI structs and constants for the transaction-status SLRU managers:
//! clog, subtrans, commit_ts, and multixact.
//!
//! On-disk / xlog record layouts are `#[repr(C)]` with compile-time layout
//! asserts so they match the PostgreSQL 18.3 C structs exactly.

use crate::types::{Oid, RepOriginId, TimestampTz, TransactionId};

// Re-export the canonical clog status vocabulary defined in `xact` so callers
// of this module have it in scope without re-declaring (avoids glob conflicts).
pub use crate::xact::{
    XidStatus, TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED,
    TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_SUB_COMMITTED,
};

// ---------------------------------------------------------------------------
// MultiXact base types (c.h)
// ---------------------------------------------------------------------------

/// `typedef TransactionId MultiXactId;` (c.h).
pub type MultiXactId = TransactionId;
/// `typedef uint32 MultiXactOffset;` (c.h).
pub type MultiXactOffset = u32;

// ---------------------------------------------------------------------------
// clog (access/clog.h)
// ---------------------------------------------------------------------------

/// clog XLOG opcodes (`info & XLOG_RECORD_TYPE_MASK`).
pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

/// `struct xl_clog_truncate` (clog.h) -- WAL record for CLOG truncation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_clog_truncate {
    pub pageno: i64,
    pub oldestXact: TransactionId,
    pub oldestXactDb: Oid,
}

// ---------------------------------------------------------------------------
// commit_ts (access/commit_ts.h)
// ---------------------------------------------------------------------------

/// commit_ts XLOG opcodes.
pub const COMMIT_TS_ZEROPAGE: u8 = 0x00;
pub const COMMIT_TS_TRUNCATE: u8 = 0x10;

/// `struct xl_commit_ts_set` (commit_ts.h). The subxact Xids follow `mainxid`
/// in the WAL stream (flexible-array tail); only the fixed header is modeled.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_commit_ts_set {
    pub timestamp: TimestampTz,
    pub nodeid: RepOriginId,
    pub mainxid: TransactionId,
}

/// `SizeOfCommitTsSet = offsetof(xl_commit_ts_set, mainxid) + sizeof(TransactionId)`.
pub const SizeOfCommitTsSet: usize =
    core::mem::offset_of!(xl_commit_ts_set, mainxid) + core::mem::size_of::<TransactionId>();

/// `struct xl_commit_ts_truncate` (commit_ts.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_commit_ts_truncate {
    pub pageno: i64,
    pub oldestXid: TransactionId,
}

/// `SizeOfCommitTsTruncate = offsetof(xl_commit_ts_truncate, oldestXid) + sizeof(TransactionId)`.
pub const SizeOfCommitTsTruncate: usize =
    core::mem::offset_of!(xl_commit_ts_truncate, oldestXid) + core::mem::size_of::<TransactionId>();

// ---------------------------------------------------------------------------
// multixact (access/multixact.h)
// ---------------------------------------------------------------------------

pub const InvalidMultiXactId: MultiXactId = 0;
pub const FirstMultiXactId: MultiXactId = 1;
pub const MaxMultiXactId: MultiXactId = 0xFFFF_FFFF;
pub const MaxMultiXactOffset: MultiXactOffset = 0xFFFF_FFFF;

#[inline]
pub const fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

/// `typedef enum MultiXactStatus` (multixact.h). The first four values are tuple
/// lock modes; the last two are update/delete modes.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiXactStatus {
    ForKeyShare = 0x00,
    ForShare = 0x01,
    ForNoKeyUpdate = 0x02,
    ForUpdate = 0x03,
    /// an update that doesn't touch "key" columns
    NoKeyUpdate = 0x04,
    /// other updates, and delete
    Update = 0x05,
}

pub const MaxMultiXactStatus: u32 = MultiXactStatus::Update as u32;

/// `ISUPDATE_from_mxstatus(status)` -- does a status correspond to a tuple update?
#[inline]
pub const fn ISUPDATE_from_mxstatus(status: u32) -> bool {
    status > MultiXactStatus::ForUpdate as u32
}

/// `struct MultiXactMember` (multixact.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: u32,
}

/// multixact XLOG opcodes.
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

/// `struct xl_multixact_create` (multixact.h). Members follow as a
/// flexible-array tail; only the fixed header is modeled here.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_multixact_create {
    /// new MultiXact's ID
    pub mid: MultiXactId,
    /// its starting offset in members file
    pub moff: MultiXactOffset,
    /// number of member XIDs
    pub nmembers: i32,
}

/// `SizeOfMultiXactCreate = offsetof(xl_multixact_create, members)`.
pub const SizeOfMultiXactCreate: usize = core::mem::size_of::<xl_multixact_create>();

/// `struct xl_multixact_truncate` (multixact.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_multixact_truncate {
    pub oldestMultiDB: Oid,
    /// just for completeness' sake -- start of truncated multixact-offset range
    pub startTruncOff: MultiXactId,
    pub endTruncOff: MultiXactId,
    /// to-be-truncated range of multixact members
    pub startTruncMemb: MultiXactOffset,
    pub endTruncMemb: MultiXactOffset,
}

/// `SizeOfMultiXactTruncate = sizeof(xl_multixact_truncate)`.
pub const SizeOfMultiXactTruncate: usize = core::mem::size_of::<xl_multixact_truncate>();

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn transam_status_abi_layouts_match_postgres() {
        // xl_clog_truncate { int64 pageno; TransactionId oldestXact; Oid oldestXactDb; }
        assert_eq!(size_of::<xl_clog_truncate>(), 16);
        assert_eq!(align_of::<xl_clog_truncate>(), 8);
        assert_eq!(offset_of!(xl_clog_truncate, pageno), 0);
        assert_eq!(offset_of!(xl_clog_truncate, oldestXact), 8);
        assert_eq!(offset_of!(xl_clog_truncate, oldestXactDb), 12);

        // xl_commit_ts_set { TimestampTz timestamp; RepOriginId nodeid; TransactionId mainxid; }
        assert_eq!(offset_of!(xl_commit_ts_set, timestamp), 0);
        assert_eq!(offset_of!(xl_commit_ts_set, nodeid), 8);
        // TransactionId (4-byte align) after a 2-byte nodeid -> padding to offset 12
        assert_eq!(offset_of!(xl_commit_ts_set, mainxid), 12);
        assert_eq!(SizeOfCommitTsSet, 16);

        // xl_commit_ts_truncate { int64 pageno; TransactionId oldestXid; }
        assert_eq!(offset_of!(xl_commit_ts_truncate, pageno), 0);
        assert_eq!(offset_of!(xl_commit_ts_truncate, oldestXid), 8);
        assert_eq!(SizeOfCommitTsTruncate, 12);

        // MultiXactMember { TransactionId xid; MultiXactStatus status; }
        assert_eq!(size_of::<MultiXactMember>(), 8);
        assert_eq!(offset_of!(MultiXactMember, xid), 0);
        assert_eq!(offset_of!(MultiXactMember, status), 4);

        // xl_multixact_create header { MultiXactId mid; MultiXactOffset moff; int32 nmembers; }
        assert_eq!(offset_of!(xl_multixact_create, mid), 0);
        assert_eq!(offset_of!(xl_multixact_create, moff), 4);
        assert_eq!(offset_of!(xl_multixact_create, nmembers), 8);
        assert_eq!(SizeOfMultiXactCreate, 12);

        // xl_multixact_truncate { Oid; MultiXactId x2; MultiXactOffset x2; }
        assert_eq!(offset_of!(xl_multixact_truncate, oldestMultiDB), 0);
        assert_eq!(offset_of!(xl_multixact_truncate, startTruncOff), 4);
        assert_eq!(offset_of!(xl_multixact_truncate, endTruncOff), 8);
        assert_eq!(offset_of!(xl_multixact_truncate, startTruncMemb), 12);
        assert_eq!(offset_of!(xl_multixact_truncate, endTruncMemb), 16);
        assert_eq!(SizeOfMultiXactTruncate, 20);

        // sanity: base type widths
        assert_eq!(size_of::<MultiXactId>(), 4);
        assert_eq!(size_of::<MultiXactOffset>(), 4);
        assert_eq!(size_of::<RepOriginId>(), 2);
    }
}
