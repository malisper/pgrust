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

seam_core::seam!(
    // MultiXactIdIsRunning(multi, isLockOnly) (multixact.c).
    pub fn multi_xact_id_is_running(
        multi: types_core::MultiXactId,
        is_lock_only: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // MultiXactIdCreateFromMembers(nmembers, members) (multixact.c).
    pub fn multi_xact_id_create_from_members(
        members: &mut [types_storage::multixact::MultiXactMember],
    ) -> PgResult<types_core::MultiXactId>
);

seam_core::seam!(
    // MultiXactIdCreate(xid1, status1, xid2, status2) (multixact.c).
    pub fn multi_xact_id_create(
        xid1: TransactionId,
        status1: types_storage::multixact::MultiXactStatus,
        xid2: TransactionId,
        status2: types_storage::multixact::MultiXactStatus,
    ) -> PgResult<types_core::MultiXactId>
);

seam_core::seam!(
    // MultiXactIdExpand(multi, xid, status) (multixact.c).
    pub fn multi_xact_id_expand(
        multi: types_core::MultiXactId,
        xid: TransactionId,
        status: types_storage::multixact::MultiXactStatus,
    ) -> PgResult<types_core::MultiXactId>
);

seam_core::seam!(
    // StartupMultiXact() (multixact.c).
    pub fn startup_multixact() -> PgResult<()>
);

seam_core::seam!(
    // TrimMultiXact() (multixact.c).
    pub fn trim_multixact() -> PgResult<()>
);

seam_core::seam!(
    // CheckPointMultiXact() (multixact.c).
    pub fn check_point_multixact() -> PgResult<()>
);

seam_core::seam!(
    // MultiXactSetNextMXact(nextMulti, nextMultiOffset) (multixact.c).
    pub fn multixact_set_next_mxact(
        next_multi: types_core::MultiXactId,
        next_multi_offset: types_core::MultiXactOffset,
    )
);

seam_core::seam!(
    // SetMultiXactIdLimit(oldestMulti, oldestMultiDB, is_startup) (multixact.c).
    pub fn set_multixact_id_limit(
        oldest_multi: types_core::MultiXactId,
        oldest_multi_db: types_core::Oid,
        is_startup: bool,
    )
);

seam_core::seam!(
    // MultiXactAdvanceNextMXact(nextMulti, nextMultiOffset) (multixact.c).
    pub fn multixact_advance_next_mxact(
        next_multi: types_core::MultiXactId,
        next_multi_offset: types_core::MultiXactOffset,
    )
);

seam_core::seam!(
    // MultiXactAdvanceOldest(oldestMulti, oldestMultiDB) (multixact.c).
    pub fn multixact_advance_oldest(
        oldest_multi: types_core::MultiXactId,
        oldest_multi_db: types_core::Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // MultiXactGetCheckptMulti(is_shutdown, ...) (multixact.c):
    // (nextMulti, nextMultiOffset, oldestMulti, oldestMultiDB).
    pub fn multixact_get_checkpt_multi(
        is_shutdown: bool,
    ) -> (
        types_core::MultiXactId,
        types_core::MultiXactOffset,
        types_core::MultiXactId,
        types_core::Oid,
    )
);

seam_core::seam!(
    // MultiXactIdSetOldestMember (multixact.c): per-backend pre-DML bookkeeping.
    pub fn multi_xact_id_set_oldest_member() -> PgResult<()>
);
