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
