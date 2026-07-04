use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // deparse_expression (ruleutils.c) for tablecmds' partition-key error
    // names; ruleutils -> indexcmds -> tablecmds, so this edge is a seam.
    pub fn deparse_expression<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: types_nodes::Node<'mcx>,
        relid: Oid,
    ) -> PgResult<String>
);
