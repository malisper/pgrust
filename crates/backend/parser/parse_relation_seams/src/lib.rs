use mcx::Mcx;
use parser_small1::ParseState;
use types_error::PgResult;
use types_nodes::{NodeList, Var};

// upstream 9108fed3eda9 (18.5): Fix parsing of parenthesised OLD/NEW in RETURNING list.
// The whole-row Var travels intact so the impl can resolve its nsitem by
// (varno, varlevelsup, varreturningtype) via GetNSItemByVar.
seam_core::seam!(
    pub fn expand_nsitem_vars_at<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &'a ParseState<'p, 'mcx>,
        var: &'a Var<'mcx>,
    ) -> PgResult<NodeList<'mcx>>
);
