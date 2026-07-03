use mcx::Mcx;
use parser_small1::ParseState;
use types_error::PgResult;
use types_nodes::Node;

seam_core::seam!(
    pub fn assign_expr_collations<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a ParseState<'p, 'mcx>,
        expr: Node<'mcx>,
    ) -> PgResult<()>
);
