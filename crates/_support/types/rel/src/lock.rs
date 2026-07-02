use ::types_core::Oid;

// LOCKMODE's home is types_storage::lock (C: storage/lockdefs.h); re-exported
// here because rel.h-shaped code names the modes through utils/rel.h.
pub use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, InplaceUpdateTupleLock, MaxLockMode,
    NoLock, RowExclusiveLock, RowShareLock, ShareLock, ShareRowExclusiveLock,
    ShareUpdateExclusiveLock, LOCKMODE,
};

// dbId is InvalidOid for a shared/global relation (utils/rel.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LockRelId {
    pub relId: Oid,
    pub dbId: Oid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}
