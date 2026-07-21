use mcx::Mcx;
use tableam_vocab::{BulkInsertStateData, WriteMultiInsertBuffer};
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
    /// W1 multi-insert buffer (tableam::write_buffer); None = per-tuple path.
    pub mibuf: Option<WriteMultiInsertBuffer<'mcx>>,
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
            mibuf: None,
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

seam_core::seam!(
    // GetIntoRelEFlags projected to its one input (prepare cannot dep
    // createas: createas deps prepare for ExecuteQuery).
    pub fn get_into_rel_eflags(skip_data: bool) -> i32
);

seam_core::seam!(
    // CreateTableAsRelExists for EXPLAIN's CTAS arm (explain cannot dep
    // createas: createas reaches explain via postgres/tablecmds).
    pub fn create_table_as_rel_exists<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &types_nodes::rawnodes::CreateTableAsStmt<'mcx>,
    ) -> PgResult<bool>
);
