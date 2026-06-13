//! Seam declarations for the `backend-storage-lmgr-lmgr` unit
//! (`storage/lmgr/lmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid`.

seam_core::seam!(
    /// `CheckRelationLockedByMe(relation, lockmode, orstronger)` (lmgr.c):
    /// does this backend hold `lockmode` (or, with `orstronger`, any
    /// stronger lock) on the relation? Infallible (pure local lock-table
    /// lookup).
    pub fn check_relation_locked_by_me(
        relation: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
        orstronger: bool,
    ) -> bool
);

seam_core::seam!(
    /// `LockSharedObject(classId, objId, objSubId, lockmode)` (lmgr.c) — take a
    /// lock on a non-relation shared object (e.g. a `pg_database` row). Can
    /// `ereport(ERROR)` on deadlock/lock-table exhaustion, carried on `Err`.
    pub fn lock_shared_object(
        classid: types_core::primitive::Oid,
        objid: types_core::primitive::Oid,
        objsubid: u16,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `UnlockSharedObject(classId, objId, objSubId, lockmode)` (lmgr.c).
    pub fn unlock_shared_object(
        classid: types_core::primitive::Oid,
        objid: types_core::primitive::Oid,
        objsubid: u16,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);
