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

extern crate alloc;

seam_core::seam!(
    /// `GetLockNameFromTagType(uint16 locktag_type)` (lmgr.c): the name of a
    /// heavyweight lock type (`LockTagTypeNames[locktag_type]`, or
    /// `"unknown wait event"` for an out-of-range value). Returns a `'static`
    /// lock-method name owned by lmgr.c.
    pub fn get_lock_name_from_tag_type(locktag_type: types_core::uint16) -> &'static str
);

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

seam_core::seam!(
    /// `UnlockSharedObject(classid, objid, objsubid, lockmode)` (lmgr.c):
    /// release a lock on a shared-catalog object. Used by the
    /// `get_object_address` retry loop to drop the lock taken on a now-stale
    /// shared object before re-resolving. Can `elog(WARNING/ERROR)` on a
    /// lock-table inconsistency, carried on `Err`.
    pub fn unlock_shared_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `LockApplyTransactionForSession(suboid, xid, objid, lockmode)` (lmgr.c):
    /// take a *session-level* lock on a transaction being applied on a logical
    /// replication subscriber (the parallel-apply deadlock-detection STREAM and
    /// XACT locks). `MyDatabaseId` is read internally by the owner.
    ///
    /// These are deliberately session-scoped and held across the streaming
    /// protocol's state machine — the leader holds the stream lock for the
    /// whole streamed transaction while parallel-apply workers block on it, and
    /// the matching `Unlock*` is an explicit call later in the protocol, never
    /// a function-scoped `Drop`. They are therefore explicit lock/unlock seams
    /// mirroring the C control flow, not [`LockGuard`]s; release on
    /// proc/session exit is the lmgr owner's responsibility. Can
    /// `ereport(ERROR)` (deadlock, cancel), carried on `Err`.
    pub fn lock_apply_transaction_for_session(
        suboid: Oid,
        xid: types_core::TransactionId,
        objid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockApplyTransactionForSession(suboid, xid, objid, lockmode)`
    /// (lmgr.c): release the matching session-level apply-transaction lock.
    /// `MyDatabaseId` is read internally by the owner. Explicit counterpart to
    /// [`lock_apply_transaction_for_session`]; can `elog(WARNING/ERROR)` on a
    /// lock-table inconsistency, carried on `Err`.
    pub fn unlock_apply_transaction_for_session(
        suboid: Oid,
        xid: types_core::TransactionId,
        objid: u16,
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
    /// `DescribeLockTag(buf, tag)` (lmgr.c) — render a `LOCKTAG` to a human
    /// description for the deadlock report. C appends to a `StringInfo`; the
    /// seam returns the rendered `String` (the detector appends it itself).
    pub fn describe_lock_tag(tag: types_storage::lock::LOCKTAG) -> alloc::string::String
);

seam_core::seam!(
    /// `CheckRelationOidLockedByMe(relid, lockmode, orstronger)` (lmgr.c).
    pub fn check_relation_oid_locked_by_me(
        relid: Oid,
        lockmode: LOCKMODE,
        orstronger: bool,
    ) -> bool
);

seam_core::seam!(
    /// `LockTuple(relation, tid, lockmode)` (lmgr.c): acquire a heavyweight
    /// tuple-tag lock (used for the in-place-update tuple lock during UPDATE
    /// of a relation that needs it). This lock is held until transaction end
    /// and released by the transaction's resource owner, so it is taken
    /// imperatively rather than as a scope guard (mirroring the C).
    pub fn lock_tuple(
        relid: Oid,
        tid: types_tuple::heaptuple::ItemPointerData,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockTuple(relation, tid, lockmode)` (lmgr.c): release the
    /// heavyweight tuple-tag lock taken by [`lock_tuple`].
    pub fn unlock_tuple(
        relid: Oid,
        tid: types_tuple::heaptuple::ItemPointerData,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ConditionalLockTuple(relation, tid, lockmode, logLockFailure)` (lmgr.c)
    /// — try to acquire the heavyweight tuple-tag lock without blocking;
    /// returns whether it was obtained. Used by `heap_lock_tuple`'s
    /// `heap_acquire_tuplock` under the Skip / Error wait policies. `Err`
    /// carries the lock-manager `ereport(ERROR)` surface.
    pub fn conditional_lock_tuple(
        relid: Oid,
        tid: types_tuple::heaptuple::ItemPointerData,
        lockmode: LOCKMODE,
        log_lock_failure: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ConditionalXactLockTableWait(xid, logLockFailure)` (lmgr.c) — never
    /// blocks; returns `true` when `xid` has finished (lock obtained then
    /// released), `false` if it would have to wait. Used by the heap-AM
    /// non-blocking lock-wait paths. `Err` carries the lock-manager
    /// `ereport(ERROR)` surface.
    pub fn conditional_xact_lock_table_wait(
        xid: TransactionId,
        log_lock_failure: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `XactLockTableWait(xid, rel, ctid, oper)` (lmgr.c) — block until `xid`
    /// commits or aborts, attaching the operation's error context. The seam
    /// crosses the address (relation name + ctid block/offset) and `oper` the
    /// owner needs to build `XactLockTableWaitInfo` for the error-context
    /// callback. Used by the heap-AM lock-wait paths. `Err` carries the wait
    /// `ereport(ERROR)` surface.
    pub fn xact_lock_table_wait(
        xid: TransactionId,
        rel_name: alloc::string::String,
        ctid: types_tuple::heaptuple::ItemPointerData,
        oper: types_storage::lock::XLTW_Oper,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `LockSharedObjectForSession(classid, objid, objsubid, lockmode)`
    /// (lmgr.c): take a *session-level* lock on a shared-catalog object, held
    /// until explicitly released (not at transaction end). Used by
    /// `dbase_redo`'s hot-standby `XLOG_DBASE_DROP` path to lock the database
    /// while resolving recovery conflicts. Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn lock_shared_object_for_session(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockSharedObjectForSession(classid, objid, objsubid, lockmode)`
    /// (lmgr.c): release the session-level shared-object lock taken by
    /// [`lock_shared_object_for_session`]. Can `elog(WARNING/ERROR)` on a
    /// lock-table inconsistency, carried on `Err`.
    pub fn unlock_shared_object_for_session(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `LockDatabaseFrozenIds(lockmode)` (lmgr.c): take the per-database
    /// `LOCKTAG_DATABASE_FROZEN_IDS` lock so only one backend per database runs
    /// `vac_update_datfrozenxid()`, avoiding races that would move
    /// `datfrozenxid`/`datminmxid` backward. `MyDatabaseId` supplies the tag.
    /// `Err` carries the lock-manager `ereport(ERROR)` surface.
    pub fn lock_database_frozen_ids(lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    /// `SpeculativeInsertionWait(xid, token)` (lmgr.c): wait for the given
    /// speculative insertion to be confirmed or aborted (a `ShareLock`
    /// acquire+immediate-release on the speculative-insertion lock tag).
    /// Reached from `check_exclusion_or_unique_constraint` (execIndexing.c)
    /// when a conflicting in-progress speculative insertion is found. `Err`
    /// carries the lock-manager `ereport(ERROR)` surface.
    pub fn speculative_insertion_wait(xid: TransactionId, token: u32) -> PgResult<()>
);
