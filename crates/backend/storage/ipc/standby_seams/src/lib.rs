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
