use mcx::Mcx;
use tableam_vocab::BulkInsertStateData;
use types_core::{CommandId, InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::Node;
use types_rel::Relation;
use types_slot::SlotData;
use types_tuple::TupleDescData;

/// DR_intorel marshal shape: `into` is an IntoClause node handle; the rest is
/// filled by intorel_startup (logic lives in commands_createas, not here).
pub struct IntoRelState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub into: Node<'mcx>,
    pub rel: Option<Relation<'mcx>>,
    pub reladdr: Oid,
    pub output_cid: CommandId,
    pub ti_options: i32,
    pub bistate: Option<BulkInsertStateData>,
}

impl<'mcx> IntoRelState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, into: Node<'mcx>) -> Self {
        IntoRelState {
            mcx,
            into,
            rel: None,
            reladdr: InvalidOid,
            output_cid: 0,
            ti_options: 0,
            bistate: None,
        }
    }
}

seam_core::seam!(
    pub fn intorel_startup<'mcx>(
        state: &mut IntoRelState<'mcx>,
        operation: i32,
        typeinfo: &TupleDescData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn intorel_receive<'mcx>(
        state: &mut IntoRelState<'mcx>,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn intorel_shutdown<'mcx>(state: &mut IntoRelState<'mcx>) -> PgResult<()>
);
