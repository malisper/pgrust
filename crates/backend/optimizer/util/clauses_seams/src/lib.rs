use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::Node;

seam_core::seam!(
    // clauses::fold::eval_const_expressions for callers a direct dep would
    // cycle (typcache domain-constraint load rides adt_rangetypes->typcache).
    pub fn eval_const_expressions<'mcx>(
        mcx: Mcx<'mcx>,
        node: Node<'mcx>,
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    pub fn evaluate_expr<'mcx>(
        mcx: Mcx<'mcx>,
        expr: Node<'mcx>,
        result_type: Oid,
        result_typmod: i32,
        result_collation: Oid,
    ) -> PgResult<Node<'mcx>>
);
