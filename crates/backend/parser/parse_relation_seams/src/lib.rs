use mcx::Mcx;
use parser_small1::ParseState;
use types_core::ParseLoc;
use types_error::PgResult;
use types_nodes::NodeList;

seam_core::seam!(
    pub fn expand_nsitem_vars_at<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a ParseState<'p, 'mcx>,
        varno: i32,
        sublevels_up: i32,
        location: ParseLoc,
    ) -> PgResult<NodeList<'mcx>>
);
