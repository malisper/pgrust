//! Seam declarations for the `backend-parser-agg` unit (`parser/parse_agg.c`).
//!
//! `transformGroupingFunc` is consumed across a cycle by `parse_expr.c`
//! (`backend-parser-parse-expr`), which `backend-parser-agg` depends on via
//! `backend-parser-parse-expr-seams`. The remaining entry points
//! (`parseCheckAggregates`, `expand_grouping_sets`, and the
//! `build_aggregate_*fn_expr` / `resolve_aggregate_transtype` /
//! `get_aggregate_argtypes` / `agg_args_support_sendreceive` planner-executor
//! helpers) are exposed here so their (currently unported) consumers can reach
//! them; the owner installs them all from its `init_seams()`.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Aggref, Expr};
use types_nodes::copy_query::Query;
use types_nodes::parsestmt::ParseState;

seam_core::seam!(
    /// `transformGroupingFunc(pstate, p)` (parse_agg.c) — transform a
    /// `GROUPING(...)` expression. `p` is the raw `GROUPING(...)` node
    /// (`Node::Expr(Expr::GroupingFunc)`); the result is the analyzed
    /// `Expr::GroupingFunc`. Consumed across the cycle by parse_expr.c.
    pub fn transform_grouping_func<'mcx>(
        pstate: &mut ParseState<'mcx>,
        p: Node<'mcx>,
    ) -> PgResult<Expr>
);

seam_core::seam!(
    /// `parseCheckAggregates(pstate, qry)` (parse_agg.c) — final aggregate /
    /// grouping checks; rewrites grouped vars in the targetlist and HAVING.
    /// Allocates the RTE_GROUP RTE/nsitem and rewritten nodes in `mcx`.
    pub fn parse_check_aggregates<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        qry: &mut Query<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `expand_grouping_sets(groupingSets, groupDistinct, limit)` (parse_agg.c)
    /// — expand a groupingSets clause to a flat list of integer grouping sets,
    /// sorted by length. `None` when the expansion exceeds `limit`.
    pub fn expand_grouping_sets<'mcx>(
        mcx: Mcx<'mcx>,
        grouping_sets: &[Node<'mcx>],
        group_distinct: bool,
        limit: i32,
    ) -> PgResult<Option<PgVec<'mcx, PgVec<'mcx, i32>>>>
);

seam_core::seam!(
    /// `get_aggregate_argtypes(aggref, inputTypes)` (parse_agg.c) — the actual
    /// argument-type OIDs of an aggregate call.
    pub fn get_aggregate_argtypes<'mcx>(
        mcx: Mcx<'mcx>,
        aggref: &Aggref,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `resolve_aggregate_transtype(aggfuncid, aggtranstype, inputTypes,
    /// numArguments)` (parse_agg.c). `mcx` is the scratch context for the
    /// `get_func_signature` fetch in the polymorphic path.
    pub fn resolve_aggregate_transtype<'mcx>(
        mcx: Mcx<'mcx>,
        aggfuncid: Oid,
        aggtranstype: Oid,
        input_types: &[Oid],
        num_arguments: i32,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `agg_args_support_sendreceive(aggref)` (parse_agg.c).
    pub fn agg_args_support_sendreceive(aggref: &Aggref) -> PgResult<bool>
);

seam_core::seam!(
    /// `build_aggregate_transfn_expr(...)` (parse_agg.c). Returns
    /// `(transfnexpr, invtransfnexpr)`; `invtransfnexpr` is `None` when
    /// `build_invtrans` is false or `invtransfn_oid` is invalid.
    pub fn build_aggregate_transfn_expr(
        agg_input_types: &[Oid],
        agg_num_inputs: i32,
        agg_num_direct_inputs: i32,
        agg_variadic: bool,
        agg_state_type: Oid,
        agg_input_collation: Oid,
        transfn_oid: Oid,
        invtransfn_oid: Oid,
        build_invtrans: bool,
    ) -> PgResult<(Expr, Option<Expr>)>
);

seam_core::seam!(
    /// `build_aggregate_serialfn_expr(serialfn_oid, &serialfnexpr)`
    /// (parse_agg.c).
    pub fn build_aggregate_serialfn_expr(serialfn_oid: Oid) -> PgResult<Expr>
);

seam_core::seam!(
    /// `build_aggregate_deserialfn_expr(deserialfn_oid, &deserialfnexpr)`
    /// (parse_agg.c).
    pub fn build_aggregate_deserialfn_expr(deserialfn_oid: Oid) -> PgResult<Expr>
);

seam_core::seam!(
    /// `build_aggregate_finalfn_expr(...)` (parse_agg.c).
    pub fn build_aggregate_finalfn_expr(
        agg_input_types: &[Oid],
        num_finalfn_inputs: i32,
        agg_state_type: Oid,
        agg_result_type: Oid,
        agg_input_collation: Oid,
        finalfn_oid: Oid,
    ) -> PgResult<Expr>
);

seam_core::seam!(
    /// `transformAggregateCall(pstate, agg, args, aggorder, agg_distinct)`
    /// (parse_agg.c) — finish transformation of an aggregate call, filling the
    /// `Aggref`'s `args`/`aggorder`/`aggdistinct`/`aggdirectargs`/`aggargtypes`
    /// and running the placement/level checks. `args` is the already-transformed
    /// plain argument list; `aggorder` is the raw ORDER BY (`SortBy` nodes).
    /// Consumed by parse_func.c (unported).
    pub fn transform_aggregate_call<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        agg: &mut Aggref,
        args: PgVec<'mcx, Expr>,
        aggorder: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
        agg_distinct: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `transformWindowFuncCall(pstate, wfunc, windef)` (parse_agg.c) — finish
    /// transformation of a window function call: link it to the right
    /// `WindowDef` and mark `p_hasWindowFuncs`. Consumed by parse_func.c
    /// (unported).
    pub fn transform_window_func_call<'mcx>(
        pstate: &mut ParseState<'mcx>,
        wfunc: &mut types_nodes::primnodes::WindowFunc,
        windef: &types_nodes::rawnodes::WindowDef<'mcx>,
    ) -> PgResult<()>
);
