use mcx::Mcx;
use parser_small1::ParseState;
use types_core::ParseLoc;
use types_error::PgResult;
use types_nodes::NodeList;

seam_core::seam!(
    // GetNSItemByRangeTablePosn + expandNSItemVars (parse_relation.c); the Var
    // list only (colnames dropped). Seam because clauses -> coerce closes a
    // cycle on a direct dep.
    pub fn expand_nsitem_vars_at<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a ParseState<'p, 'mcx>,
        varno: i32,
        sublevels_up: i32,
        location: ParseLoc,
    ) -> PgResult<NodeList<'mcx>>
);
