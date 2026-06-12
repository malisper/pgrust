//! Seam declarations for the `backend-storage-lmgr-lmgr` unit
//! (`storage/lmgr/lmgr.c`: relation/object lock wrappers).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_tuple::access::LOCKMODE;

seam_core::seam!(
    /// `LockRelationOid(relid, lockmode)` (lmgr.c): lock a relation by OID
    /// and accept pending invalidation messages on lock acquisition. Can
    /// `ereport(ERROR)` (deadlock, cancel), carried on `Err`.
    pub fn lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    /// `ConditionalLockRelationOid(relid, lockmode)` (lmgr.c): as above but
    /// fail (return `false`) instead of waiting.
    pub fn conditional_lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<bool>
);

seam_core::seam!(
    /// `UnlockRelationOid(relid, lockmode)` (lmgr.c). C can
    /// `elog(WARNING/ERROR)` on a lock-table inconsistency, carried on `Err`.
    pub fn unlock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    /// `LockDatabaseObject(classid, objid, objsubid, lockmode)` (lmgr.c):
    /// lock a general database object; also accepts pending invalidation
    /// messages. Can `ereport(ERROR)`, carried on `Err`.
    pub fn lock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockDatabaseObject(classid, objid, objsubid, lockmode)` (lmgr.c).
    pub fn unlock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);
