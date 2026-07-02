use types_core::Oid;
use types_error::PgResult;
use types_rel::LOCKMODE;

// lmgr locks are transaction-scoped (released explicitly or at xact end),
// never scope-tied, so acquisition returns no guard. Err = the C
// ereport(ERROR) surface (deadlock, cancel; unlock: lock-table inconsistency).
seam_core::seam!(
    pub fn lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

// UnlockRelationOid re-derives the same lock tag SetLocktagRelationOid built
// for the lock, identical to UnlockRelationId on rd_lockInfo.lockRelId.
seam_core::seam!(
    pub fn unlock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam_core::seam!(
    pub fn check_relation_locked_by_me(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool
);
