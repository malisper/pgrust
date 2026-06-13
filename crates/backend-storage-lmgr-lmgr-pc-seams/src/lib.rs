//! plancache's slice of relation locking (`storage/lmgr/lmgr.c`). plancache
//! takes transient, non-conflicting locks on relation OIDs during
//! planner/executor lock acquisition and releases them on the revalidation
//! race path; the lock manager owns the bookkeeping. The owning unit installs
//! these; until then a call panics loudly.

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `LockRelationOid(relid, lockmode)`.
    pub fn lock_relation_oid(relid: Oid, lockmode: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockRelationOid(relid, lockmode)`.
    pub fn unlock_relation_oid(relid: Oid, lockmode: i32) -> PgResult<()>
);
