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
    pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool
);
