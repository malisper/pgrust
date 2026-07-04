use mcx::Mcx;
use tableam_vocab::BulkInsertStateData;
use types_core::{CommandId, Oid};
use types_error::PgResult;
use types_rel::Relation;
use types_slot::SlotData;
use types_tuple::TupleDescData;

/// DR_transientrel marshal shape (matview.c); logic lives in commands_matview.
pub struct TransientRelState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub transientoid: Oid,
    pub rel: Option<Relation<'mcx>>,
    pub output_cid: CommandId,
    pub ti_options: i32,
    pub bistate: Option<BulkInsertStateData>,
}

impl<'mcx> TransientRelState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, transientoid: Oid) -> Self {
        TransientRelState {
            mcx,
            transientoid,
            rel: None,
            output_cid: 0,
            ti_options: 0,
            bistate: None,
        }
    }
}

seam_core::seam!(
    pub fn transientrel_startup<'mcx>(
        state: &mut TransientRelState<'mcx>,
        operation: i32,
        typeinfo: &TupleDescData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn transientrel_receive<'mcx>(
        state: &mut TransientRelState<'mcx>,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn transientrel_shutdown<'mcx>(state: &mut TransientRelState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    pub fn matview_maintenance_is_enabled() -> bool
);

seam_core::seam!(
    pub fn exec_refresh_mat_view<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &types_nodes::rawnodes::RefreshMatViewStmt<'mcx>,
        query_string: &str,
        qc: Option<&mut types_portal::QueryCompletion>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn refresh_mat_view_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        matview_oid: Oid,
        is_create: bool,
        skip_data: bool,
        concurrent: bool,
        query_string: &str,
        qc: Option<&mut types_portal::QueryCompletion>,
    ) -> PgResult<()>
);
