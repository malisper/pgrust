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

seam_core::seam!(
    // inline_function's parser-dependent middle (clauses.c body parse/gate +
    // functions.c prepare_sql_fn_parse_info/check_sql_fn_retval): returns the
    // SUBSTITUTED body expression, not yet re-simplified; None = C's `goto
    // fail` decline. Installed by sql_functions (a clauses->parser dep cycles).
    pub fn inline_sql_function<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
        result_type: Oid,
        result_collid: Oid,
        input_collid: Oid,
        args: &'a types_nodes::NodeList<'mcx>,
    ) -> PgResult<Option<Node<'mcx>>>
);
