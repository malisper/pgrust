use mcx::Mcx;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_portal::ParamListHandle;

seam_core::seam!(
    pub fn planner<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse: Query<'mcx>,
        query_string: &'a str,
        cursor_options: i32,
        bound_params: ParamListHandle,
    ) -> PgResult<PlannedStmt<'mcx>>
);
