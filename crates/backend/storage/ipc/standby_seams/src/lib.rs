use types_core::TransactionId;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

seam_core::seam!(
    // LogStandbyInvalidations(nmsgs, msgs, relcacheInitFileInval) (standby.c).
    pub fn log_standby_invalidations<'a>(
        msgs: &'a [SharedInvalidationMessage],
        relcache_init_file_inval: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn standby_release_lock_tree<'a>(xid: TransactionId, subxids: &'a [TransactionId])
);

seam_core::seam!(
    // LogAccessExclusiveLockPrepare() (standby.c).
    pub fn log_access_exclusive_lock_prepare() -> types_error::PgResult<()>
);

seam_core::seam!(
    // LogAccessExclusiveLock(dbOid, relOid) (standby.c).
    pub fn log_access_exclusive_lock(db_oid: types_core::Oid, rel_oid: types_core::Oid) -> types_error::PgResult<()>
);
