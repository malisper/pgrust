//! `optimizer/plan/planagg.c` — special planning for MIN/MAX aggregates.
//!
//! `preprocess_minmax_aggregates` is called from `grouping_planner` whenever the
//! query `hasAggs`. It checks whether the query is a plain (un-grouped,
//! single-table, CTE-free) aggregate query whose aggregates are *all* MIN/MAX,
//! and if so tries to replace each `MIN(x)`/`MAX(x)` with an indexscan that
//! reads just the first row (`SELECT x ... ORDER BY x LIMIT 1`). When it
//! succeeds it adds a `MinMaxAggPath` to the `UPPERREL_GROUP_AGG` upperrel,
//! where it competes with (and usually beats) the regular aggregate plan.
//!
//! ## Crate split
//!
//! The reject/early-out logic and `can_minmax_aggs` (which examines the
//! `AggInfo` list `preprocess_aggrefs` built, decides whether every aggregate
//! is MIN/MAX, and builds the `MinMaxAggInfo` candidate list) live HERE.
//! `preprocess_minmax_aggregates` returns `Some(candidates)` when the query is
//! all-MIN/MAX and optimizable, else `None` (the common `count`/`sum`/`avg`
//! path: the regular aggregate plan is used).
//!
//! The *index-path* construction — `build_minmax_path` per aggregate (clone the
//! root into a subroot, `query_planner` a `SELECT col … ORDER BY col LIMIT 1`,
//! keep the cheapest IndexScan-backed presorted path), the per-agg
//! `SS_make_initplan_output_param`, and `create_minmaxagg_path` /
//! `add_path(UPPERREL_GROUP_AGG)` — lives in the planner crate
//! (`backend-optimizer-plan-planner`), which owns `grouping_planner` and already
//! depends on `query_planner`/pathnode/init-subselect. Routing it there avoids
//! pulling those (and a dependency cycle) into this leaf unit while staying
//! faithful to C, where every planagg.c function is reachable from
//! `grouping_planner`'s translation-unit neighbourhood. `create_minmaxagg_plan`
//! (the createplan leg that turns the `MinMaxAggPath` into a `Result` with one
//! InitPlan per aggregate) lives in the createplan crate.

#![allow(non_snake_case)]

extern crate alloc;

use ::mcx::Mcx;
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use ::types_error::PgResult;
use ::nodes::copy_query::Query;
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::{Aggref, Expr};
use pathnodes::{MinMaxAggInfo, PlannerInfo};

/// `preprocess_minmax_aggregates(root)` (planagg.c:73).
///
/// `parse` is `root->parse` (resolved by the caller). For a query that is not a
/// plain all-MIN/MAX aggregate query this returns having populated nothing; the
/// regular aggregate plan is then used.
pub fn preprocess_minmax_aggregates<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &Query<'mcx>,
) -> PgResult<Option<alloc::vec::Vec<MinMaxAggInfo>>> {
    /* minmax_aggs list should be empty at this point */
    debug_assert!(root.minmax_aggs.is_empty());

    /* Nothing to do if query has no aggregates */
    if !parse.hasAggs {
        return Ok(None);
    }

    debug_assert!(parse.setOperations.is_none()); /* shouldn't get here if a setop */
    debug_assert!(parse.rowMarks.is_empty()); /* nor if FOR UPDATE */

    /*
     * Reject unoptimizable cases.
     *
     * We don't handle GROUP BY or windowing, because our current
     * implementations of grouping require looking at all the rows anyway, and
     * so there's not much point in optimizing MIN/MAX.
     */
    if !parse.groupClause.is_empty()
        || parse.groupingSets.len() > 1
        || parse.hasWindowFuncs
    {
        return Ok(None);
    }

    /*
     * Reject if query contains any CTEs; there's no way to build an indexscan
     * on one so we couldn't succeed here.
     */
    if !parse.cteList.is_empty() {
        return Ok(None);
    }

    /*
     * We also restrict the query to reference exactly one table, since join
     * conditions can't be handled reasonably. The single table could be buried
     * in several levels of FromExpr due to subqueries; chase down through the
     * FromExpr layers to a single RangeTblRef.
     *
     * jtnode = parse->jointree;  (the Query holds the top FromExpr by box, not
     * as a Node, so peel it first, then continue the `while IsA(FromExpr)` walk
     * over the inner Node-typed subtrees.)
     */
    let Some(top) = parse.jointree.as_deref() else {
        return Ok(None);
    };
    // while (IsA(jtnode, FromExpr)) { if list_length(fromlist) != 1 return; jtnode = linitial; }
    if top.fromlist.len() != 1 {
        return Ok(None);
    }
    let mut cur: &Node<'mcx> = top.fromlist[0].as_ref();
    loop {
        match cur.node_tag() {
            ntag::T_FromExpr => {
                let f = cur.expect_fromexpr();
                if f.fromlist.len() != 1 {
                    return Ok(None);
                }
                cur = f.fromlist[0].as_ref();
            }
            _ => break,
        }
    }

    // if (!IsA(jtnode, RangeTblRef)) return;
    let rtindex = match cur.node_tag() {
        ntag::T_RangeTblRef => cur.expect_rangetblref().rtindex,
        _ => return Ok(None),
    };

    // rte = planner_rt_fetch(rtr->rtindex, root); (1-based RT index)
    let rte = &parse.rtable[(rtindex - 1) as usize];
    use ::nodes::parsenodes::RTEKind;
    match rte.rtekind {
        RTEKind::RTE_RELATION => { /* ordinary relation, ok */ }
        RTEKind::RTE_SUBQUERY if rte.inh => { /* flattened UNION ALL subquery, ok */ }
        _ => return Ok(None),
    }

    /*
     * Examine all the aggregates and verify all are MIN/MAX aggregates. Stop
     * as soon as we find one that isn't. On success this returns the
     * `MinMaxAggInfo` candidate list (`aggfnoid`/`aggsortop`/`target` filled,
     * paths still unbuilt); on failure (any non-MIN/MAX aggregate) `None`.
     *
     * The remaining planagg.c steps — `build_minmax_path` per aggregate, the
     * `SS_make_initplan_output_param` per-agg output Param, and the
     * `create_minmaxagg_path` / `add_path(UPPERREL_GROUP_AGG)` — need
     * `query_planner` on a cloned subroot, which would pull a planner-crate
     * dependency cycle into this unit. Those run in the planner crate (which owns
     * `grouping_planner` and already depends on `query_planner`), keyed off this
     * candidate list. This split mirrors C: planagg.c's functions all live in the
     * planner's translation-unit neighbourhood; only `can_minmax_aggs` (pure
     * classification over `root->agginfos`) lives here.
     */
    can_minmax_aggs(mcx, root)
}

/// `can_minmax_aggs(root, &context)` (planagg.c:237) — examine the `AggInfo`
/// list `preprocess_aggrefs` built and check whether every aggregate is a
/// MIN/MAX aggregate, building the `MinMaxAggInfo` candidate list as it goes.
/// Returns `None` as soon as a non-MIN/MAX aggregate is found (the common case
/// for `count`/`sum`/`avg`), else `Some(list)`.
///
/// Each built `MinMaxAggInfo` has `aggfnoid` / `aggsortop` set and `target` =
/// the [`::pathnodes::NodeId`] of the aggregate argument expression, interned
/// into the OUTER `root`'s `node_arena` (so `build_minmax_path` and the setrefs
/// Aggref→Param replacement can both read it). `path` / `param` / `subroot_idx`
/// are left at their `Default` (the planner-crate path-build step fills them).
fn can_minmax_aggs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Option<alloc::vec::Vec<MinMaxAggInfo>>> {
    let mut context: alloc::vec::Vec<MinMaxAggInfo> = alloc::vec::Vec::new();
    // foreach(lc, root->agginfos)
    let agginfo_ids: alloc::vec::Vec<::pathnodes::NodeId> = root.agginfos.clone();
    for agginfo_id in agginfo_ids {
        // aggref = linitial_node(Aggref, agginfo->aggrefs);
        let aggref_id = root.agg_info(agginfo_id).aggrefs[0];
        let aggref: &Aggref = match root.node(aggref_id) {
            Expr::Aggref(a) => a,
            _ => unreachable!("AggInfo.aggrefs handle resolves to Expr::Aggref"),
        };
        debug_assert!(aggref.agglevelsup == 0);

        // if (list_length(aggref->args) != 1) return false; /* not MIN/MAX */
        if aggref.args.len() != 1 {
            return Ok(None);
        }

        // ORDER BY makes it an ordered-set agg or changes MIN/MAX semantics: punt.
        if !aggref.aggorder.is_empty() {
            return Ok(None);
        }

        // FILTER: punt for now.
        if aggref.aggfilter.is_some() {
            return Ok(None);
        }

        // aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
        let aggfnoid = aggref.aggfnoid;
        let aggsortop = fetch_agg_sort_op(mcx, aggfnoid)?;
        if !OidIsValid(aggsortop) {
            return Ok(None); /* not a MIN/MAX aggregate */
        }

        // curTarget = (TargetEntry *) linitial(aggref->args);
        // The Aggref's args are an owned TargetEntry<'static> list; read the
        // first arg's expr by value to inspect it.
        let cur_expr: Expr = {
            let aggref: &Aggref = match root.node(aggref_id) {
                Expr::Aggref(a) => a,
                _ => unreachable!(),
            };
            // TargetEntry.expr is Option<Box<Expr>>.
            match aggref.args[0].expr.as_deref() {
                Some(e) => e.clone_in(mcx)?,
                None => return Ok(None),
            }
        };

        // if (contain_mutable_functions(curTarget->expr)) return false;
        if clauses::contain_mutable_functions(Some(&cur_expr))? {
            return Ok(None); /* not potentially indexable */
        }

        // if (type_is_rowtype(exprType(curTarget->expr))) return false;
        let exprtype = nodes_core::nodefuncs::expr_type(Some(&cur_expr))?;
        if lsyscache_seams::type_is_rowtype::call(exprtype)? {
            return Ok(None); /* IS NOT NULL would have weird semantics */
        }

        // A target carrying a SubPlan (e.g. `max((SELECT ...))`) can never be
        // satisfied by an index scan, so the C path-build would abandon the
        // optimization for it. C reaches that abandonment by *trying*
        // build_minmax_path and getting no indexed path; we short-circuit the
        // same outcome here — and avoid building a subquery whose ORDER BY /
        // tlist embeds a SubPlan (the owned model can't re-plan that into a
        // presorted index path). Bounded restriction with identical net result:
        // the regular Agg plan is used.
        if clauses::contain_subplans(Some(&cur_expr))? {
            return Ok(None);
        }

        // mminfo = makeNode(MinMaxAggInfo); fill aggfnoid/aggsortop/target. Intern
        // the target expr into the OUTER root's node_arena and keep its NodeId.
        let target_id = root.alloc_node(cur_expr);
        let mut mminfo = MinMaxAggInfo::default();
        mminfo.aggfnoid = aggfnoid;
        mminfo.aggsortop = aggsortop;
        mminfo.target = target_id;
        // path = NULL; pathcost = 0; param = NULL (left at Default).

        context.push(mminfo);
    }
    Ok(Some(context))
}

/// `fetch_agg_sort_op(aggfnoid)` (planagg.c:499) — `SearchSysCache1(AGGFNOID)` +
/// `GETSTRUCT(Form_pg_aggregate)->aggsortop`. Reuses the installed
/// `agg_form_by_oid` syscache projection (the same `AGGFNOID` lookup), reading
/// its `aggsortop`. `InvalidOid` on a cache miss (C: `!HeapTupleIsValid`).
fn fetch_agg_sort_op<'mcx>(mcx: Mcx<'mcx>, aggfnoid: Oid) -> PgResult<Oid> {
    match syscache_seams::agg_form_by_oid::call(mcx, aggfnoid)? {
        Some(form) => Ok(form.aggsortop),
        None => Ok(InvalidOid),
    }
}
