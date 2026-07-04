use mcx::Mcx;
use parser_small1::ParseState;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::NodeList;

seam_core::seam!(
    pub fn transform_agg_order_distinct<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a mut ParseState<'p, 'mcx>,
        tlist: &'a mut NodeList<'mcx>,
        agg_order: &'a NodeList<'mcx>,
        agg_distinct: bool,
    ) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>, mcx::PgVec<'mcx, Oid>)>
);

seam_core::seam!(
    pub fn transform_agg_within_group<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a mut ParseState<'p, 'mcx>,
        tlist: &'a NodeList<'mcx>,
        agg_order: &'a NodeList<'mcx>,
    ) -> PgResult<NodeList<'mcx>>
);
