//! Seam declarations for the `backend-storage-lmgr-lmgr` unit
//! (`storage/lmgr/lmgr.c`: relation/object lock wrappers), plus the
//! [`LockGuard`] value the lock acquisitions return.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Per `docs/query-lifecycle-raii.md`, lock acquisition returns a guard, not
//! a bare `()` paired with an ambient release call. A held lock is released
//! exactly one of three ways:
//!
//! * [`LockGuard::release`] — the C explicit `UnlockRelationOid` /
//!   `UnlockDatabaseObject` (retry loops), surfacing lmgr's error channel;
//! * [`LockGuard::keep`] — C parity for the common case where the function
//!   returns with the lock held until transaction end. Interim shape: once
//!   `TxnResources` (query-lifecycle doc) lands, `keep()` becomes moving the
//!   guard into the transaction owner;
//! * `Drop` — the abort path: an error unwinding past the guard releases the
//!   lock, which is what C's transaction-abort resowner sweep would do.

use types_core::{Oid, TransactionId, VirtualTransactionId};
use types_error::PgResult;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `CheckRelationLockedByMe(relation, lockmode, orstronger)` (lmgr.c):
    /// does this backend hold `lockmode` (or, with `orstronger`, any
    /// stronger lock) on the relation? Infallible (pure local lock-table
    /// lookup). Open relations cross as their `Oid`.
    pub fn check_relation_locked_by_me(
        relation: Oid,
        lockmode: LOCKMODE,
        orstronger: bool,
    ) -> bool
);

seam_core::seam!(
    /// `LockRelationOid(relid, lockmode)` (lmgr.c): lock a relation by OID
    /// and accept pending invalidation messages on lock acquisition. Can
    /// `ereport(ERROR)` (deadlock, cancel), carried on `Err`. On success the
    /// lock is held by the returned guard.
    pub fn lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<LockGuard>
);

seam_core::seam!(
    /// `ConditionalLockRelationOid(relid, lockmode)` (lmgr.c): as above but
    /// fail (return `Ok(None)`, the C `false`) instead of waiting.
    pub fn conditional_lock_relation_oid(
        relid: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<LockGuard>>
);

seam_core::seam!(
    /// `LockDatabaseObject(classid, objid, objsubid, lockmode)` (lmgr.c):
    /// lock a general database object; also accepts pending invalidation
    /// messages. Can `ereport(ERROR)`, carried on `Err`. On success the lock
    /// is held by the returned guard.
    pub fn lock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<LockGuard>
);

seam_core::seam!(
    /// `LockSharedObject(classid, objid, objsubid, lockmode)` (lmgr.c): lock a
    /// shared-catalog object (a global object visible from every database);
    /// also accepts pending invalidation messages. Can `ereport(ERROR)`,
    /// carried on `Err`. On success the lock is held by the returned guard
    /// (released at transaction end via `keep`, the C default).
    pub fn lock_shared_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<LockGuard>
);

seam_core::seam!(
    /// `UnlockRelationOid(relid, lockmode)` (lmgr.c). [`LockGuard`] plumbing
    /// — consumers go through the guard, never call this directly. C can
    /// `elog(WARNING/ERROR)` on a lock-table inconsistency, carried on `Err`.
    pub fn unlock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockDatabaseObject(classid, objid, objsubid, lockmode)` (lmgr.c).
    /// [`LockGuard`] plumbing — consumers go through the guard, never call
    /// this directly.
    pub fn unlock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

/// What a [`LockGuard`] holds — the lmgr-level identity of the lock, enough
/// to delegate the matching `Unlock*` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockTag {
    /// A `LockRelationOid`-acquired lock.
    Relation { relid: Oid, lockmode: LOCKMODE },
    /// A `LockDatabaseObject`-acquired lock.
    Object {
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    },
}

/// A held heavyweight lock (see the module docs for the release contract).
/// Constructed by the lmgr owner when installing the `lock_*` seams.
#[derive(Debug)]
pub struct LockGuard(Option<LockTag>);

impl LockGuard {
    /// Guard for a lock just acquired via `LockRelationOid`.
    pub fn relation(relid: Oid, lockmode: LOCKMODE) -> Self {
        LockGuard(Some(LockTag::Relation { relid, lockmode }))
    }

    /// Guard for a lock just acquired via `LockDatabaseObject`.
    pub fn database_object(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) -> Self {
        LockGuard(Some(LockTag::Object { classid, objid, objsubid, lockmode }))
    }

    /// Explicit early release — the C `UnlockRelationOid` /
    /// `UnlockDatabaseObject` call, surfacing its error channel.
    pub fn release(mut self) -> PgResult<()> {
        match self.0.take() {
            Some(tag) => unlock(tag),
            None => Ok(()),
        }
    }

    /// Return with the lock held until transaction end (the C default: lmgr
    /// locks are transaction-scoped and the function simply returns).
    /// Interim shape until `TxnResources` exists to move the guard into.
    pub fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for LockGuard {
    /// The abort path: release on unwind, mirroring C's transaction-abort
    /// lock release. The unlock error channel cannot propagate from `drop`;
    /// an inconsistency here is the C `elog(WARNING)` lost until the lmgr
    /// owner lands and the guard moves into `TxnResources`.
    fn drop(&mut self) {
        if let Some(tag) = self.0.take() {
            let _ = unlock(tag);
        }
    }
}

/// Delegate to the matching `Unlock*` seam.
fn unlock(tag: LockTag) -> PgResult<()> {
    match tag {
        LockTag::Relation { relid, lockmode } => unlock_relation_oid::call(relid, lockmode),
        LockTag::Object { classid, objid, objsubid, lockmode } => {
            unlock_database_object::call(classid, objid, objsubid, lockmode)
        }
    }
}

seam_core::seam!(
    /// `LockRelationForExtension(rel, ExclusiveLock)` (lmgr.c): take the
    /// relation-extension lock. On success the lock is held by the returned
    /// guard; releasing the guard is `UnlockRelationForExtension`. Acquisition
    /// can `ereport(ERROR)`, carried on `Err`.
    pub fn lock_relation_for_extension<'mcx>(
        rel: &types_rel::Relation<'mcx>,
    ) -> PgResult<RelationExtensionLockGuard>
);

seam_core::seam!(
    /// `UnlockRelationForExtension(rel, ExclusiveLock)` (lmgr.c) — the release
    /// half, reached only through [`RelationExtensionLockGuard`].
    pub fn unlock_relation_for_extension(relid: Oid) -> PgResult<()>
);

/// A held relation-extension lock. Releasing it (explicitly or on unwind)
/// delegates to `UnlockRelationForExtension`. Constructed by the lmgr owner
/// when installing `lock_relation_for_extension`.
#[derive(Debug)]
pub struct RelationExtensionLockGuard(Option<Oid>);

impl RelationExtensionLockGuard {
    /// Guard for a lock just acquired on the relation with this OID.
    pub fn new(relid: Oid) -> Self {
        RelationExtensionLockGuard(Some(relid))
    }

    /// Explicit early release — the C `UnlockRelationForExtension`.
    pub fn release(mut self) -> PgResult<()> {
        match self.0.take() {
            Some(relid) => unlock_relation_for_extension::call(relid),
            None => Ok(()),
        }
    }
}

impl Drop for RelationExtensionLockGuard {
    fn drop(&mut self) {
        if let Some(relid) = self.0.take() {
            let _ = unlock_relation_for_extension::call(relid);
        }
    }
}

seam_core::seam!(
    /// `XactLockTableInsert(xid)` — take ExclusiveLock on the transaction
    /// XID. Lock acquisition can `ereport(ERROR)` (out of shared memory).
    pub fn xact_lock_table_insert(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `XactLockTableDelete(xid)` — release the subtransaction XID lock.
    pub fn xact_lock_table_delete(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `VirtualXactLockTableInsert(vxid)` — lock our virtual transaction id
    /// before advertising it in the proc array.
    pub fn virtual_xact_lock_table_insert(vxid: VirtualTransactionId) -> PgResult<()>
);
