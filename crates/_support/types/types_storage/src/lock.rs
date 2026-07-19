use ::types_core::{uint16, uint32, uint8, Oid, ProcNumber, TimestampTz, TransactionId};

use crate::ilist::{dlist_head, dlist_node};
use crate::storage::proclist_head;

pub type LOCKMODE = i32;

pub type LOCKMASK = i32;

pub const fn LOCKBIT_ON(lockmode: LOCKMODE) -> LOCKMASK {
    1 << lockmode
}

pub const fn LOCKBIT_OFF(lockmode: LOCKMODE) -> LOCKMASK {
    !(1 << lockmode)
}

pub type LOCKMETHODID = uint16;

pub const MAX_LOCKMODES: usize = 10;

pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;
pub const MaxLockMode: LOCKMODE = AccessExclusiveLock;

pub const InplaceUpdateTupleLock: LOCKMODE = ExclusiveLock;

pub const DEFAULT_LOCKMETHOD: uint8 = 1;
pub const USER_LOCKMETHOD: uint8 = 2;

pub const LOCKTAG_RELATION: uint8 = 0;
pub const LOCKTAG_RELATION_EXTEND: uint8 = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: uint8 = 2;
pub const LOCKTAG_PAGE: uint8 = 3;
pub const LOCKTAG_TUPLE: uint8 = 4;
pub const LOCKTAG_TRANSACTION: uint8 = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: uint8 = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: uint8 = 7;
pub const LOCKTAG_OBJECT: uint8 = 8;
pub const LOCKTAG_USERLOCK: uint8 = 9;
pub const LOCKTAG_ADVISORY: uint8 = 10;
pub const LOCKTAG_APPLY_TRANSACTION: uint8 = 11;
pub const LOCKTAG_LAST_TYPE: uint8 = LOCKTAG_APPLY_TRANSACTION;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum XLTW_Oper {
    None = 0,
    Update = 1,
    Delete = 2,
    Lock = 3,
    LockUpdated = 4,
    InsertIndex = 5,
    InsertIndexUnique = 6,
    FetchUpdated = 7,
    RecheckExclusionConstr = 8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

const _: () = assert!(core::mem::size_of::<LOCKTAG>() == 16);

impl LOCKTAG {
    #[inline]
    pub fn as_hash_words(&self) -> (u64, u64) {
        let w0 = self.locktag_field1 as u64 | ((self.locktag_field2 as u64) << 32);
        let w1 = self.locktag_field3 as u64
            | ((self.locktag_field4 as u64) << 32)
            | ((self.locktag_type as u64) << 48)
            | ((self.locktag_lockmethodid as u64) << 56);
        (w0, w1)
    }
}

// Two word writes, not seven field writes: LOCALLOCK map probes are
// per-statement hot (docs/benchmarks/lock.md).
impl core::hash::Hash for LOCKTAG {
    #[inline]
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        let (w0, w1) = self.as_hash_words();
        state.write_u64(w0);
        state.write_u64(w1);
    }
}

// Constructors mirror lock.h's SET_LOCKTAG_* field-for-field; these tags key
// the shared lock hash, so the encodings must match C exactly.
impl LOCKTAG {
    #[inline]
    pub fn relation(dbid: Oid, relid: Oid) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: relid,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_RELATION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn relation_extend(dbid: Oid, relid: Oid) -> Self {
        LOCKTAG {
            locktag_type: LOCKTAG_RELATION_EXTEND,
            ..Self::relation(dbid, relid)
        }
    }

    #[inline]
    pub fn database_frozen_ids(dbid: Oid) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_DATABASE_FROZEN_IDS,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn page(dbid: Oid, relid: Oid, blocknum: uint32) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: relid,
            locktag_field3: blocknum,
            locktag_field4: 0,
            locktag_type: LOCKTAG_PAGE,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn tuple(dbid: Oid, relid: Oid, blocknum: uint32, offnum: uint16) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: relid,
            locktag_field3: blocknum,
            locktag_field4: offnum,
            locktag_type: LOCKTAG_TUPLE,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn speculative_insertion(xid: TransactionId, token: uint32) -> Self {
        LOCKTAG {
            locktag_field1: xid,
            locktag_field2: token,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_SPECULATIVE_TOKEN,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn object(dbid: Oid, classid: Oid, objid: Oid, objsubid: uint16) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: classid,
            locktag_field3: objid,
            locktag_field4: objsubid,
            locktag_type: LOCKTAG_OBJECT,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    #[inline]
    pub fn apply_transaction(dbid: Oid, suboid: Oid, xid: TransactionId, objid: uint16) -> Self {
        LOCKTAG {
            locktag_field1: dbid,
            locktag_field2: suboid,
            locktag_field3: xid,
            locktag_field4: objid,
            locktag_type: LOCKTAG_APPLY_TRANSACTION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    pub fn advisory(id1: uint32, id2: uint32, id3: uint32, id4: uint16) -> Self {
        LOCKTAG {
            locktag_field1: id1,
            locktag_field2: id2,
            locktag_field3: id3,
            locktag_field4: id4,
            locktag_type: LOCKTAG_ADVISORY,
            locktag_lockmethodid: USER_LOCKMETHOD,
        }
    }

    pub fn transaction(xid: uint32) -> Self {
        LOCKTAG {
            locktag_field1: xid,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_TRANSACTION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    pub fn virtualtransaction(proc_number: uint32, local_transaction_id: uint32) -> Self {
        LOCKTAG {
            locktag_field1: proc_number,
            locktag_field2: local_transaction_id,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_VIRTUALTRANSACTION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LockInstanceData {
    pub locktag: LOCKTAG,
    pub holdMask: LOCKMASK,
    pub waitLockMode: LOCKMODE,
    pub vxid: crate::storage::VirtualTransactionId,
    pub waitStart: TimestampTz,
    pub pid: i32,
    pub leaderPid: i32,
    pub fastpath: bool,
}

// The scalar projection of one SIREAD predicate lock crossing the predicate
// seam for pg_lock_status (lockfuncs.c).
#[derive(Clone, Debug)]
pub struct PredLockStatusRow {
    pub locktypename: alloc::string::String,
    pub database: u32,
    pub relation: u32,
    pub has_page: bool,
    pub page: u32,
    pub has_tuple: bool,
    pub tuple: u16,
    pub proc_number: i32,
    pub local_xid: u32,
    pub pid: i32,
}

// VirtualXactLock's examination of the target's fpInfoLock-guarded fast-path
// VXID state; decides lock.c's next step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualXactExamineOutcome {
    Ended,
    StillRunningNoWait,
    Proceed { xid: TransactionId },
}

pub type LockAcquireResult = i32;
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum DeadLockState {
    NotYetChecked = 0,
    NoDeadLock = 1,
    SoftDeadLock = 2,
    HardDeadLock = 3,
    BlockedByAutoVacuum = 4,
}

// C's pointers into lock.c's static const tables, carried as 'static borrows.
#[derive(Clone, Copy, Debug)]
pub struct LockMethodData {
    pub numLockModes: i32,
    pub conflictTab: &'static [LOCKMASK],
    pub lockModeNames: &'static [&'static str],
    pub trace_flag: bool,
}

pub type LockMethod = &'static LockMethodData;

// Shmem-resident dynahash entry; every field is protected by the lock
// partition assigned by LockTagHashCode(tag) ([PART]). waitProcs threads
// ProcNumbers through PGPROC.links; procLocks threads PROCLOCK.lockLink.
#[repr(C)]
#[derive(Debug)]
pub struct LOCK {
    pub tag: LOCKTAG,
    pub grantMask: LOCKMASK,
    pub waitMask: LOCKMASK,
    pub procLocks: dlist_head,
    pub waitProcs: ProcWaitQueue,
    pub requested: [i32; MAX_LOCKMODES],
    pub nRequested: i32,
    pub granted: [i32; MAX_LOCKMODES],
    pub nGranted: i32,
}

impl LOCK {
    pub fn lock_method(&self) -> LOCKMETHODID {
        self.tag.locktag_lockmethodid as LOCKMETHODID
    }
}

// C's dclist_head over PGPROC.links, realized over ProcNumbers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ProcWaitQueue {
    pub list: proclist_head,
    pub count: u32,
}

impl ProcWaitQueue {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// The PROCLOCK hash key. C keys on {LOCK*, PGPROC*}; the PGPROC identity here
// is its ProcNumber. `pad` participates in the 16-byte blob key and MUST be
// zero. myLock stays a raw pointer: dynahash LOCK entries are address-stable
// for the life of the PROCLOCK (guaranteed by nRequested accounting).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PROCLOCKTAG {
    pub myLock: *mut LOCK,
    pub myProc: ProcNumber,
    pub pad: u32,
}

// 64-bit layout pin (8-byte myLock pointer). On wasm32 (ILP32) the pointer
// is 4 bytes and the blob key is 12 bytes; callers key on
// size_of::<PROCLOCKTAG>() so the key stays internally consistent.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<PROCLOCKTAG>() == 16);

impl PROCLOCKTAG {
    pub fn new(my_lock: *mut LOCK, my_proc: ProcNumber) -> Self {
        Self {
            myLock: my_lock,
            myProc: my_proc,
            pad: 0,
        }
    }
}

// Shmem-resident dynahash entry, [PART] like LOCK. groupLeader is a PGPROC
// identity (C stores the pointer). releaseMask is per-backend scratch used
// only by the owning backend within LockReleaseAll/PostPrepare_Locks.
#[repr(C)]
#[derive(Debug)]
pub struct PROCLOCK {
    pub tag: PROCLOCKTAG,
    pub groupLeader: ProcNumber,
    pub holdMask: LOCKMASK,
    pub releaseMask: LOCKMASK,
    pub lockLink: dlist_node,
    pub procLink: dlist_node,
}

const _: () = {
    assert!(!core::mem::needs_drop::<LOCK>());
    assert!(!core::mem::needs_drop::<PROCLOCK>());
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LOCALLOCKTAG {
    pub lock: LOCKTAG,
    pub mode: LOCKMODE,
}

impl core::hash::Hash for LOCALLOCKTAG {
    #[inline]
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        let (w0, w1) = self.lock.as_hash_words();
        state.write_u64(w0);
        state.write_u64(w1);
        state.write_u32(self.mode as u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_mode_bits_match_lock_h() {
        assert_eq!(LOCKBIT_ON(AccessExclusiveLock), 1 << 8);
        assert_eq!(LOCKBIT_OFF(AccessShareLock), !(1 << 1));
        assert_eq!(LOCKTAG_LAST_TYPE, 11);
    }

    #[test]
    fn shared_lock_shapes_are_shmem_resident() {
        assert!(!core::mem::needs_drop::<LOCK>());
        assert!(!core::mem::needs_drop::<PROCLOCK>());
        assert_eq!(core::mem::size_of::<PROCLOCKTAG>(), 16);
    }
}
