use types_core::Oid;
use types_error::PgResult;
use types_rel::LOCKMODE;

seam_core::seam!(
    pub fn lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    pub fn unlock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    pub fn conditional_lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<bool>
);

seam_core::seam!(
    pub fn check_relation_locked_by_me(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool
);

seam_core::seam!(
    // LockDatabaseObject(classid, objid, objsubid, lockmode) (lmgr.c).
    pub fn lock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    // UnlockDatabaseObject(classid, objid, objsubid, lockmode) (lmgr.c).
    pub fn unlock_database_object(
        classid: Oid,
        objid: Oid,
        objsubid: u16,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    // DescribeLockTag(&buf, tag) marshaled to the built text (lmgr.c).
    pub fn describe_lock_tag(tag: types_storage::lock::LOCKTAG) -> std::string::String
);
