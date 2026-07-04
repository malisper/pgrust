use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_rel::Relation;

seam_core::seam!(
    pub fn check_cmd_replica_identity<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        cmd: CmdType,
    ) -> PgResult<()>
);
