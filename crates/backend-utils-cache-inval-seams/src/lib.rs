//! Seam declarations for the `backend-utils-cache-inval` unit
//! (`utils/cache/inval.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

seam_core::seam!(
    /// `ProcessCommittedInvalidationMessages(msgs, nmsgs,
    /// RelcacheInitFileInval, dbid, tsid)` — apply invalidation messages from
    /// a committed transaction during WAL replay.
    pub fn process_committed_invalidation_messages(
        msgs: &[SharedInvalidationMessage],
        relcache_init_file_inval: bool,
        dbid: Oid,
        tsid: Oid,
    ) -> PgResult<()>
);
