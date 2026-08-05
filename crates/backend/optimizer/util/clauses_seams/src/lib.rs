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
    // recheck_cast_function_args' parser tail (clauses.c:
    // enforce_generic_type_consistency + make_fn_arguments over a NULL
    // pstate); installed by parse_func (a clauses->parser dep cycles).
    pub fn recheck_cast_function_args<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        args: types_nodes::NodeList<'mcx>,
        actual_arg_types: &'a [Oid],
        declared_arg_types: &'a [Oid],
        result_type: Oid,
        prorettype: Oid,
    ) -> PgResult<types_nodes::NodeList<'mcx>>
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

seam_core::seam!(
    // inline_set_returning_function's parser-dependent middle (clauses.c body
    // fetch/parse/rewrite + functions.c check_sql_fn_retval + the
    // substitute_actual_srf_parameters pass); None = C's `goto fail` decline.
    // Installed by sql_functions (a clauses->parser dep cycles); rte is the
    // gate-cleared RTE_FUNCTION RangeTblEntry node.
    pub fn inline_set_returning_sql_body<'mcx>(
        mcx: Mcx<'mcx>,
        rte: Node<'mcx>,
        prokind: i8,
    ) -> PgResult<Option<&'mcx types_nodes::parsenodes::Query<'mcx>>>
);

seam_core::seam!(
    // recheck_cast_function_args' parser leg (parse_func make_fn_arguments,
    // null pstate); installed by parse_func — a clauses->parse_func dep
    // cycles via catalog_namespace->catalog_indexing->execindexing.
    pub fn make_fn_arguments_nullstate<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        args: &'a types_nodes::NodeList<'mcx>,
        actual_arg_types: &'a [Oid],
        declared_arg_types: &'a [Oid],
    ) -> PgResult<types_nodes::NodeList<'mcx>>
);

seam_core::seam!(
    // find_simplified_clause's duplicate-evaluation gate (rangetypes.c):
    // C declines when the elemExpr is volatile, contains a subplan, or costs
    // more than 10*cpu_operator_cost to evaluate. Installed by the planner
    // crate (clauses volatility walk + costsize cost walk) — an
    // adt_rangetypes->clauses dep cycles via typcache.
    pub fn expr_safe_to_evaluate_twice<'mcx>(
        node: Node<'mcx>,
    ) -> PgResult<bool>
);
