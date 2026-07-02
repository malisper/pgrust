use ::types_core::Oid;

// storage/lockdefs.h lock modes; hosted here until the lmgr unit lands, as
// rel.h itself hosts LockRelId/LockInfoData for the same convenience.
pub type LOCKMODE = i32;

pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;
pub const MaxLockMode: LOCKMODE = 8;

pub const InplaceUpdateTupleLock: LOCKMODE = ExclusiveLock;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LockRelId {
    pub relId: Oid,
    pub dbId: Oid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}
