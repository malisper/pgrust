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
    pub fn check_relation_locked_by_me(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool
);
