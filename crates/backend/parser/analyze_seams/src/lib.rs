use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::rawnodes::RawStmt;
use types_portal::QueryEnvHandle;

seam_core::seam!(
    pub fn parse_analyze_fixedparams<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: &'a RawStmt<'mcx>,
        source_text: &'a str,
        param_types: &'a [Oid],
        query_env: QueryEnvHandle,
    ) -> PgResult<Query<'mcx>>
);

seam_core::seam!(
    // C pg_analyze_and_rewrite_withcb with sql_fn_parser_setup: named/positional
    // SQL-function parameter hooks (empty string = unnamed).
    pub fn parse_analyze_sql_fn<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: &'a RawStmt<'mcx>,
        source_text: &'a str,
        fname: &'a str,
        argtypes: &'a [Oid],
        argnames: &'a [&'a str],
        query_env: QueryEnvHandle,
    ) -> PgResult<Query<'mcx>>
);

seam_core::seam!(
    // C's Oid **paramTypes / int *numParams out-params come back as the
    // second tuple element (the resolved parameter types).
    pub fn parse_analyze_varparams<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: &'a RawStmt<'mcx>,
        source_text: &'a str,
        param_types: &'a [Oid],
        query_env: QueryEnvHandle,
    ) -> PgResult<(Query<'mcx>, mcx::PgVec<'mcx, Oid>)>
);

seam_core::seam!(
    pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool
);

seam_core::seam!(
    // C parse_sub_analyze (analyze.c); parentCTE rides as a Node option.
    pub fn parse_sub_analyze<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: types_nodes::Node<'mcx>,
        parent_parse_state: &'a parser_small1::ParseState<'a, 'mcx>,
        parent_cte: Option<types_nodes::Node<'mcx>>,
        locked_from_parent: bool,
        resolve_unknowns: bool,
    ) -> PgResult<Query<'mcx>>
);
