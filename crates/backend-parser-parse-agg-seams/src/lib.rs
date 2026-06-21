//! Seam declarations for the `backend-parser-parse-agg` unit
//! (`parser/parse_agg.c`, part of the unported `backend-parser-medium2` unit).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic).

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::ParseState;
use types_nodes::primnodes::{Aggref, Expr};

seam_core::seam!(
    /// `transformAggregateCall(pstate, agg, args, aggorder, agg_distinct)`
    /// (parse_agg.c): finish building an `Aggref` — wrap the (already
    /// type-coerced) plain argument expressions and any `ORDER BY` items into
    /// the aggregate's `aggdirectargs`/`args`/`aggorder`/`aggdistinct` and
    /// `aggargtypes`, set `agglevelsup`, and record the aggregate in `pstate`
    /// (`p_hasAggs`, level checks). The C mutates `*agg` and `*pstate` in place;
    /// the owned model takes the freshly-built `Aggref` plus the raw `args`
    /// (plain exprs) and `aggorder` (`SortBy` nodes) by value and returns the
    /// finished `Aggref`. `Err` carries the aggregate-placement `ereport(ERROR)`
    /// surface.
    pub fn transform_aggregate_call<'mcx>(
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        agg: types_nodes::primnodes::Aggref<'static>,
        args: std::vec::Vec<types_nodes::primnodes::Expr<'static>>,
        aggorder: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
        agg_distinct: bool,
    ) -> types_error::PgResult<types_nodes::primnodes::Aggref<'static>>
);

seam_core::seam!(
    /// `transformWindowFuncCall(pstate, wfunc, windef)` (parse_agg.c): finish
    /// building a `WindowFunc` — find or create the matching window clause in
    /// `pstate->p_windowdefs`, set `wfunc->winref`, and record the window
    /// function in `pstate` (`p_hasWindowFuncs`, level checks). The C mutates
    /// `*wfunc` and `*pstate` in place; the owned model takes the built
    /// `WindowFunc` and the `WindowDef` by value and returns the finished
    /// `WindowFunc`. `Err` carries the window-placement `ereport(ERROR)`
    /// surface.
    pub fn transform_window_func_call<'mcx>(
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        wfunc: types_nodes::primnodes::WindowFunc<'static>,
        windef: types_nodes::rawnodes::WindowDef<'mcx>,
    ) -> types_error::PgResult<types_nodes::primnodes::WindowFunc<'static>>
);

seam_core::seam!(
    /// `transformGroupingFunc(pstate, p)` (parse_agg.c) — transform a
    /// `GROUPING(...)` expression. `p` is the raw `GROUPING(...)` node
    /// (`Node::Expr(Expr::GroupingFunc)`); the result is the analyzed
    /// `Expr::GroupingFunc`. Consumed across the cycle by parse_expr.c.
    pub fn transform_grouping_func<'mcx>(
        pstate: &mut ParseState<'mcx>,
        p: Node<'mcx>,
    ) -> PgResult<Expr<'static>>
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
    pub fn get_aggregate_argtypes<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        aggref: &Aggref<'a>,
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
    pub fn agg_args_support_sendreceive(aggref: &Aggref<'static>) -> PgResult<bool>
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
    ) -> PgResult<(Expr<'static>, Option<Expr<'static>>)>
);

seam_core::seam!(
    /// `build_aggregate_serialfn_expr(serialfn_oid, &serialfnexpr)`
    /// (parse_agg.c).
    pub fn build_aggregate_serialfn_expr(serialfn_oid: Oid) -> PgResult<Expr<'static>>
);

seam_core::seam!(
    /// `build_aggregate_deserialfn_expr(deserialfn_oid, &deserialfnexpr)`
    /// (parse_agg.c).
    pub fn build_aggregate_deserialfn_expr(deserialfn_oid: Oid) -> PgResult<Expr<'static>>
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
    ) -> PgResult<Expr<'static>>
);
