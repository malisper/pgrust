//! Standby rmgr WAL record bodies (`storage/standbydefs.h`).

use crate::bytes::{bool_at, i32_at, u32_at};
use ::types_core::{Oid, TransactionId};
use ::types_storage::sinval::SharedInvalMessages;

/// `xl_standby_lock` (`storage/lockdefs.h`):
/// `{TransactionId xid; Oid dbOid; Oid relOid;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_standby_lock {
    pub xid: TransactionId,
    pub dbOid: Oid,
    pub relOid: Oid,
}

/// `sizeof(xl_standby_lock)`.
pub const SIZEOF_XL_STANDBY_LOCK: usize = 12;

impl xl_standby_lock {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xid: u32_at(rec, 0),
            dbOid: u32_at(rec, 4),
            relOid: u32_at(rec, 8),
        }
    }
}

/// An `xl_standby_lock[]` borrowed from a record body.
#[derive(Clone, Copy, Debug)]
pub struct StandbyLocks<'a> {
    bytes: &'a [u8],
}

impl<'a> StandbyLocks<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Lock `i`; panics past the end of the bytes.
    pub fn get(&self, i: usize) -> xl_standby_lock {
        xl_standby_lock::from_bytes(&self.bytes[i * SIZEOF_XL_STANDBY_LOCK..])
    }
}

/// `xl_standby_locks`: `{int nlocks; xl_standby_lock locks[];}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_standby_locks {
    pub nlocks: i32,
}

impl xl_standby_locks {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nlocks: i32_at(rec, 0) }
    }

    /// The trailing `locks` array (4-aligned after `nlocks`).
    pub fn locks(rec: &[u8]) -> StandbyLocks<'_> {
        StandbyLocks::from_bytes(&rec[4..])
    }
}

/// `xl_running_xacts`: `{int xcnt; int subxcnt; bool subxid_overflow;
/// TransactionId nextXid; TransactionId oldestRunningXid;
/// TransactionId latestCompletedXid; TransactionId xids[];}` — `xids` holds
/// the `xcnt` xacts then the `subxcnt` subxacts, 4-aligned at 24.
#[derive(Clone, Copy, Debug)]
pub struct xl_running_xacts {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_overflow: bool,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
}

impl xl_running_xacts {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xcnt: i32_at(rec, 0),
            subxcnt: i32_at(rec, 4),
            subxid_overflow: bool_at(rec, 8),
            nextXid: u32_at(rec, 12),
            oldestRunningXid: u32_at(rec, 16),
            latestCompletedXid: u32_at(rec, 20),
        }
    }

    /// `xids[i]` of the trailing array; panics past the end of the record.
    pub fn xid(rec: &[u8], i: usize) -> TransactionId {
        u32_at(rec, 24 + i * 4)
    }
}

/// `xl_invalidations`: `{Oid dbId; Oid tsId; bool relcacheInitFileInval;
/// int nmsgs; SharedInvalidationMessage msgs[];}` — `msgs` 4-aligned at 16.
#[derive(Clone, Copy, Debug)]
pub struct xl_invalidations {
    pub dbId: Oid,
    pub tsId: Oid,
    pub relcacheInitFileInval: bool,
    pub nmsgs: i32,
}

impl xl_invalidations {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            dbId: u32_at(rec, 0),
            tsId: u32_at(rec, 4),
            relcacheInitFileInval: bool_at(rec, 8),
            nmsgs: i32_at(rec, 12),
        }
    }

    /// The trailing `msgs` array.
    pub fn msgs(rec: &[u8]) -> SharedInvalMessages<'_> {
        SharedInvalMessages::from_bytes(&rec[16..])
    }
}
