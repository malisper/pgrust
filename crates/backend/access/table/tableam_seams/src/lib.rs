use std::rc::Rc;

use mcx::Mcx;
use tableam_vocab::VacuumParams;
use types_error::PgResult;
use types_rel::{Relation, RelationData};
use types_snapshot::SnapshotData;
use types_storage::buf::BufferAccessStrategy;
use types_tuple::ItemPointerData;

seam_core::seam!(
    pub fn table_relation_vacuum<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        rel: &'a RelationData<'mcx>,
        params: &'a VacuumParams,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<()>
);

seam_core::seam!(
    // currtid_internal (tid.c): scan-open + fetch + scan-close, bundled.
    pub fn table_tid_get_latest<'mcx>(
        mcx: Mcx<'mcx>,
        rel: Relation<'mcx>,
        snapshot: Rc<SnapshotData<'static>>,
        tid: ItemPointerData,
    ) -> PgResult<ItemPointerData>
);

seam_core::seam!(
    // get_table_am_oid (amcmds.c); tableamapi.c's GUC check hook reaches up
    // into commands (guc <- tableam <- amcmds is not a crate edge).
    pub fn get_table_am_oid(amname: &str, missing_ok: bool) -> PgResult<types_core::Oid>
);
