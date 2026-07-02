use types_error::PgResult;

// The ResourceOwnerCreate/Release/Delete choreography of xact.c, one seam per
// C site cluster; the owner unit realizes these over its RAII owner values
// (commit() consumes / Drop aborts — docs/no-drop.md).

seam_core::seam!(
    // AtStart_ResourceOwner (xact.c): create "TopTransaction" owner, publish
    // Top/Cur/CurrentResourceOwner.
    pub fn at_start_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    // AtSubStart_ResourceOwner: child "SubTransaction" owner, publish Cur/Current.
    pub fn at_substart_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerRelease(TopTransactionResourceOwner, BEFORE_LOCKS, isCommit, true).
    pub fn release_transaction_owner_before_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerRelease(TopTransactionResourceOwner, LOCKS, isCommit, true)
    // then (AFTER_LOCKS, isCommit, true).
    pub fn release_transaction_owner_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerRelease(s->curTransactionOwner, BEFORE_LOCKS, isCommit, false).
    pub fn release_subxact_owner_before_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerRelease(s->curTransactionOwner, LOCKS, isCommit, false)
    // then (AFTER_LOCKS, isCommit, false).
    pub fn release_subxact_owner_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerDelete(TopTransactionResourceOwner) + clear Top/Cur globals.
    pub fn delete_transaction_owner() -> PgResult<()>
);

seam_core::seam!(
    // Current/CurTransactionResourceOwner = parent's owner;
    // ResourceOwnerDelete(s->curTransactionOwner).
    pub fn cleanup_subxact_owner() -> PgResult<()>
);

seam_core::seam!(
    // CurrentResourceOwner = NULL.
    pub fn reset_current_resource_owner()
);

seam_core::seam!(
    // CurrentResourceOwner = s->curTransactionOwner (AtSubAbort_ResourceOwner).
    pub fn set_current_to_cur_transaction()
);

seam_core::seam!(
    // AssignTransactionId's owner swap: CurrentResourceOwner = the levels_up-th
    // ancestor of CurTransactionResourceOwner; returns an opaque token for the
    // previous CurrentResourceOwner.
    pub fn swap_current_to_cur_transaction_ancestor(levels_up: u32) -> usize
);

seam_core::seam!(
    pub fn restore_current_resource_owner(token: usize)
);

seam_core::seam!(
    // CurrentResourceOwner (resowner global); NULL-token when none is set.
    pub fn current_resource_owner() -> types_resowner::ResourceOwner
);

seam_core::seam!(
    pub fn resource_owner_enlarge(owner: types_resowner::ResourceOwner) -> PgResult<()>
);

seam_core::seam!(
    // ResourceOwnerRememberSnapshot (snapmgr.c wrapper over ResourceOwnerRemember).
    pub fn resource_owner_remember_snapshot(
        owner: types_resowner::ResourceOwner,
        snapshot: std::rc::Rc<types_snapshot::SnapshotData<'static>>,
    )
);

seam_core::seam!(
    pub fn resource_owner_forget_snapshot(
        owner: types_resowner::ResourceOwner,
        snapshot: std::rc::Rc<types_snapshot::SnapshotData<'static>>,
    )
);

seam_core::seam!(
    // ResourceOwnerRememberLock; the LOCALLOCK* is marshaled to its table key.
    pub fn resource_owner_remember_lock(
        owner: types_resowner::ResourceOwner,
        tag: types_storage::lock::LOCALLOCKTAG,
    )
);

seam_core::seam!(
    pub fn resource_owner_forget_lock(
        owner: types_resowner::ResourceOwner,
        tag: types_storage::lock::LOCALLOCKTAG,
    )
);

seam_core::seam!(
    pub fn resource_owner_get_parent(
        owner: types_resowner::ResourceOwner,
    ) -> types_resowner::ResourceOwner
);
