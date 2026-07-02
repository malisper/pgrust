use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_multixact()
);

seam_core::seam!(
    pub fn at_prepare_multixact() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_multixact(xid: TransactionId)
);

seam_core::seam!(
    // GetMultiXactIdMembers; members surface through the callback so no
    // allocator crosses the seam. Returns C's nmembers (-1 = none/invalid).
    pub fn get_multi_xact_id_members(
        multi: types_core::MultiXactId,
        from_pgupgrade: bool,
        is_lock_only: bool,
        consume: &mut dyn FnMut(&[types_storage::multixact::MultiXactMember]),
    ) -> PgResult<i32>
);
