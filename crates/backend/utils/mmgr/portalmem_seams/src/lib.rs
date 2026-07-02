use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    // PreCommit_Portals(isPrepare): true if any portal converted (loop again).
    pub fn pre_commit_portals(is_prepare: bool) -> PgResult<bool>
);

seam_core::seam!(
    pub fn at_abort_portals() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_cleanup_portals() -> PgResult<()>
);

seam_core::seam!(
    // C also passes parentXactOwner; owner-relinking dissolves into resowner.
    pub fn at_subcommit_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
        parent_level: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_subabort_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_subcleanup_portals(my_subid: SubTransactionId) -> PgResult<()>
);
