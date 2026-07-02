// Portal-facing snapmgr.c slice (snapmgr_seams is concurrently owned by
// another port; this slice keeps portalmem's wiring race-free).

use std::rc::Rc;

use types_resowner::ResourceOwner;
use types_snapshot::SnapshotData;

seam_core::seam!(
    // UnregisterSnapshotFromOwner(snapshot, owner) (snapmgr.c).
    pub fn unregister_snapshot_from_owner(
        snapshot: Rc<SnapshotData<'static>>,
        owner: ResourceOwner,
    )
);

seam_core::seam!(
    // ActiveSnapshotSet() (snapmgr.c).
    pub fn active_snapshot_set() -> bool
);

seam_core::seam!(
    // PopActiveSnapshot() (snapmgr.c); elog(ERROR)s on an empty stack.
    pub fn pop_active_snapshot() -> types_error::PgResult<()>
);
