use types_error::PgResult;

// The ResourceOwnerCreate/Release/Delete choreography of xact.c, one seam per
// C site cluster; the owner unit realizes these over its RAII owner values
// (commit() consumes / Drop aborts — docs/no-drop.md).

seam_core::seam!(
    // CurrentResourceOwner (resowner global); NULL-token when none is set.
    pub fn current_resource_owner() -> types_resowner::ResourceOwner
);

seam_core::seam!(
    pub fn set_current_resource_owner(owner: types_resowner::ResourceOwner)
);

seam_core::seam!(
    pub fn top_transaction_resource_owner() -> types_resowner::ResourceOwner
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

