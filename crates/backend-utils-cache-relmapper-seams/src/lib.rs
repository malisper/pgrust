//! Seam declarations for the `backend-utils-cache-relmapper` unit
//! (`utils/cache/relmapper.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `AtCCI_RelationMap()` — make pending relation-map changes visible to
    /// this transaction.
    pub fn at_cci_relation_map()
);

seam_core::seam!(
    /// `AtEOXact_RelationMap(isCommit, isParallelWorker)` — commit/discard
    /// relation-map updates; the commit path writes WAL and can
    /// `ereport(ERROR)`.
    pub fn at_eoxact_relation_map(is_commit: bool, is_parallel_worker: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_RelationMap()` — errors out if the transaction changed the
    /// map (not supported under 2PC).
    pub fn at_prepare_relation_map() -> PgResult<()>
);
