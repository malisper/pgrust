//! Multixact rmgr WAL record bodies and member vocabulary
//! (`access/multixact.h`), trimmed to the fields ports consume so far.

use crate::bytes::{i32_at, u32_at};
use ::types_core::{MultiXactId, MultiXactOffset, Oid, TransactionId};

/// `MultiXactStatus` (`access/multixact.h`) — the lock/update mode of one
/// multixact member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum MultiXactStatus {
    /// `MultiXactStatusForKeyShare = 0x00`.
    ForKeyShare = 0x00,
    /// `MultiXactStatusForShare = 0x01`.
    ForShare = 0x01,
    /// `MultiXactStatusForNoKeyUpdate = 0x02`.
    ForNoKeyUpdate = 0x02,
    /// `MultiXactStatusForUpdate = 0x03`.
    ForUpdate = 0x03,
    /// `MultiXactStatusNoKeyUpdate = 0x04`.
    NoKeyUpdate = 0x04,
    /// `MultiXactStatusUpdate = 0x05`.
    Update = 0x05,
}

impl MultiXactStatus {
    /// Decode the C enum's `int` representation; `None` for values outside
    /// the enum (C reads them anyway and callers print "unknown").
    pub const fn from_i32(v: i32) -> Option<Self> {
        match v {
            0x00 => Some(Self::ForKeyShare),
            0x01 => Some(Self::ForShare),
            0x02 => Some(Self::ForNoKeyUpdate),
            0x03 => Some(Self::ForUpdate),
            0x04 => Some(Self::NoKeyUpdate),
            0x05 => Some(Self::Update),
            _ => None,
        }
    }

    /// The C `int` discriminant (`0x00..=0x05`).
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    /// `ISUPDATE_from_mxstatus(status)` (`access/multixact.h`) — true for the
    /// `NoKeyUpdate`/`Update` modes that carry an updating xid.
    pub const fn is_update(self) -> bool {
        (self as i32) > (Self::ForUpdate as i32)
    }
}

/// `MaxMultiXactStatus` (`access/multixact.h`) — `MultiXactStatusUpdate`.
pub const MAX_MULTI_XACT_STATUS: i32 = MultiXactStatus::Update as i32;

// ---------------------------------------------------------------------------
// MultiXact rmgr opcodes (`info & XLOG_MULTIXACT_*`, access/multixact.h).
// ---------------------------------------------------------------------------

/// `XLOG_MULTIXACT_ZERO_OFF_PAGE` — a new offsets page was zeroed.
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
/// `XLOG_MULTIXACT_ZERO_MEM_PAGE` — a new members page was zeroed.
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
/// `XLOG_MULTIXACT_CREATE_ID` — a new MultiXactId was created.
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
/// `XLOG_MULTIXACT_TRUNCATE_ID` — SLRU segments were truncated.
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

/// `MultiXactMember`: `{TransactionId xid; MultiXactStatus status;}`.
/// `status` is `None` where the on-record `int` is outside the enum.
#[derive(Clone, Copy, Debug)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: Option<MultiXactStatus>,
}

/// `sizeof(MultiXactMember)` — xid plus the enum stored as `int`.
pub const SIZEOF_MULTI_XACT_MEMBER: usize = 8;

impl MultiXactMember {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xid: u32_at(rec, 0),
            status: MultiXactStatus::from_i32(i32_at(rec, 4)),
        }
    }

    /// Serialize one `MultiXactMember` (`xid` + `status` as a C `int`).
    pub fn to_bytes(&self) -> [u8; SIZEOF_MULTI_XACT_MEMBER] {
        let mut out = [0u8; SIZEOF_MULTI_XACT_MEMBER];
        out[0..4].copy_from_slice(&self.xid.to_ne_bytes());
        let status = self.status.map(|s| s.as_i32()).unwrap_or(0);
        out[4..8].copy_from_slice(&status.to_ne_bytes());
        out
    }
}

/// A `MultiXactMember[]` borrowed from a record body.
#[derive(Clone, Copy, Debug)]
pub struct MultiXactMembers<'a> {
    bytes: &'a [u8],
}

impl<'a> MultiXactMembers<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Member `i`; panics past the end of the bytes.
    pub fn get(&self, i: usize) -> MultiXactMember {
        MultiXactMember::from_bytes(&self.bytes[i * SIZEOF_MULTI_XACT_MEMBER..])
    }
}

/// `xl_multixact_create`: `{MultiXactId mid; MultiXactOffset moff;
/// int32 nmembers; MultiXactMember members[FLEXIBLE_ARRAY_MEMBER];}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_multixact_create {
    pub mid: MultiXactId,
    pub moff: MultiXactOffset,
    pub nmembers: i32,
}

/// `SizeOfMultiXactCreate` — `offsetof(xl_multixact_create, members)`.
pub const SIZE_OF_MULTI_XACT_CREATE: usize = 12;

impl xl_multixact_create {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            mid: u32_at(rec, 0),
            moff: u32_at(rec, 4),
            nmembers: i32_at(rec, 8),
        }
    }

    /// The trailing `members` array.
    pub fn members(rec: &[u8]) -> MultiXactMembers<'_> {
        MultiXactMembers::from_bytes(&rec[SIZE_OF_MULTI_XACT_CREATE..])
    }

    /// Serialize the fixed header (`mid` + `moff` + `nmembers`); callers append
    /// the `members[]` bodies.
    pub fn to_bytes(&self) -> [u8; SIZE_OF_MULTI_XACT_CREATE] {
        let mut out = [0u8; SIZE_OF_MULTI_XACT_CREATE];
        out[0..4].copy_from_slice(&self.mid.to_ne_bytes());
        out[4..8].copy_from_slice(&self.moff.to_ne_bytes());
        out[8..12].copy_from_slice(&self.nmembers.to_ne_bytes());
        out
    }
}

/// `xl_multixact_truncate`: the to-be-truncated offset and member ranges.
#[derive(Clone, Copy, Debug)]
pub struct xl_multixact_truncate {
    pub oldestMultiDB: Oid,
    pub startTruncOff: MultiXactId,
    pub endTruncOff: MultiXactId,
    pub startTruncMemb: MultiXactOffset,
    pub endTruncMemb: MultiXactOffset,
}

/// `SizeOfMultiXactTruncate` — `sizeof(xl_multixact_truncate)` (five `uint32`).
pub const SIZE_OF_MULTI_XACT_TRUNCATE: usize = 20;

impl xl_multixact_truncate {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            oldestMultiDB: u32_at(rec, 0),
            startTruncOff: u32_at(rec, 4),
            endTruncOff: u32_at(rec, 8),
            startTruncMemb: u32_at(rec, 12),
            endTruncMemb: u32_at(rec, 16),
        }
    }

    /// Serialize the record body.
    pub fn to_bytes(&self) -> [u8; SIZE_OF_MULTI_XACT_TRUNCATE] {
        let mut out = [0u8; SIZE_OF_MULTI_XACT_TRUNCATE];
        out[0..4].copy_from_slice(&self.oldestMultiDB.to_ne_bytes());
        out[4..8].copy_from_slice(&self.startTruncOff.to_ne_bytes());
        out[8..12].copy_from_slice(&self.endTruncOff.to_ne_bytes());
        out[12..16].copy_from_slice(&self.startTruncMemb.to_ne_bytes());
        out[16..20].copy_from_slice(&self.endTruncMemb.to_ne_bytes());
        out
    }
}
