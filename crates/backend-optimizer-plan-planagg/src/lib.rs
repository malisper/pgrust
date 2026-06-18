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
//! ## Bounded port
//!
//! The reject/early-out logic and `can_minmax_aggs` (which examines the
//! `AggInfo` list `preprocess_aggrefs` built and decides whether every
//! aggregate is MIN/MAX) are ported faithfully. For a query whose aggregates
//! are NOT all MIN/MAX — e.g. `count(*)`, `sum(x)` — `can_minmax_aggs` returns
//! `false` and `preprocess_minmax_aggregates` returns having done nothing: the
//! regular aggregate plan is used. This is the common path and is fully
//! supported here.
//!
//! The *index-path* construction (`build_minmax_path`, the `MinMaxAggInfo`
//! arena/`MinMaxAggPath` machinery, the per-aggregate subroot) is the
//! cross-root subquery-planner / path-arena keystone (the same one
//! `prepunion`/`planagg`'s subroot logic bottoms out on): `MinMaxAggInfo` is
//! not yet an arena node here, and there is no per-aggregate `subquery_planner`
//! path subroot. So once an aggregate is identified as a genuine MIN/MAX
//! candidate, building its path is a faithful loud panic. MIN/MAX queries
//! therefore do not (yet) get the indexscan optimization; that is a planner
//! keystone, not part of this bounded unit.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Aggref, Expr};
use types_pathnodes::PlannerInfo;

/// `preprocess_minmax_aggregates(root)` (planagg.c:73).
///
/// `parse` is `root->parse` (resolved by the caller). For a query that is not a
/// plain all-MIN/MAX aggregate query this returns having populated nothing; the
/// regular aggregate plan is then used.
pub fn preprocess_minmax_aggregates<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &Query<'mcx>,
) -> PgResult<()> {
    /* minmax_aggs list should be empty at this point */
    debug_assert!(root.minmax_aggs.is_empty());

    /* Nothing to do if query has no aggregates */
    if !parse.hasAggs {
        return Ok(());
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
        return Ok(());
    }

    /*
     * Reject if query contains any CTEs; there's no way to build an indexscan
     * on one so we couldn't succeed here.
     */
    if !parse.cteList.is_empty() {
        return Ok(());
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
        return Ok(());
    };
    // while (IsA(jtnode, FromExpr)) { if list_length(fromlist) != 1 return; jtnode = linitial; }
    if top.fromlist.len() != 1 {
        return Ok(());
    }
    let mut cur: &Node<'mcx> = top.fromlist[0].as_ref();
    loop {
        match cur {
            Node::FromExpr(f) => {
                if f.fromlist.len() != 1 {
                    return Ok(());
                }
                cur = f.fromlist[0].as_ref();
            }
            _ => break,
        }
    }

    // if (!IsA(jtnode, RangeTblRef)) return;
    let rtindex = match cur {
        Node::RangeTblRef(rtr) => rtr.rtindex,
        _ => return Ok(()),
    };

    // rte = planner_rt_fetch(rtr->rtindex, root); (1-based RT index)
    let rte = &parse.rtable[(rtindex - 1) as usize];
    use types_nodes::parsenodes::RTEKind;
    match rte.rtekind {
        RTEKind::RTE_RELATION => { /* ordinary relation, ok */ }
        RTEKind::RTE_SUBQUERY if rte.inh => { /* flattened UNION ALL subquery, ok */ }
        _ => return Ok(()),
    }

    /*
     * Examine all the aggregates and verify all are MIN/MAX aggregates. Stop
     * as soon as we find one that isn't.
     */
    if !can_minmax_aggs(mcx, root)? {
        return Ok(());
    }

    /*
     * OK, there is at least the possibility of performing the optimization.
     * Building an access path for each aggregate (build_minmax_path) and the
     * MinMaxAggPath construction is the cross-root subquery-planner / path-arena
     * keystone — not modeled here. A genuine all-MIN/MAX query reaches this
     * point; faithfully fail so the regular aggregate plan is not silently
     * skipped.
     */
    Err(minmax_path_keystone())
}

/// `can_minmax_aggs(root, &context)` (planagg.c:237) — examine the `AggInfo`
/// list `preprocess_aggrefs` built and check whether every aggregate is a
/// MIN/MAX aggregate. Returns `false` as soon as a non-MIN/MAX aggregate is
/// found (the common case for `count`/`sum`/`avg`).
///
/// In C this also builds the `MinMaxAggInfo` `*context` list as it goes; here,
/// because the `MinMaxAggInfo` arena node and the path machinery are the
/// unported keystone, we only perform the *classification* (which is what
/// decides the return value) and let `preprocess_minmax_aggregates` panic at the
/// path-build step if every aggregate qualifies.
fn can_minmax_aggs<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo) -> PgResult<bool> {
    // foreach(lc, root->agginfos)
    let agginfo_ids: alloc::vec::Vec<types_pathnodes::NodeId> = root.agginfos.clone();
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
            return Ok(false);
        }

        // ORDER BY makes it an ordered-set agg or changes MIN/MAX semantics: punt.
        if !aggref.aggorder.is_empty() {
            return Ok(false);
        }

        // FILTER: punt for now.
        if aggref.aggfilter.is_some() {
            return Ok(false);
        }

        // aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
        let aggfnoid = aggref.aggfnoid;
        let aggsortop = fetch_agg_sort_op(mcx, aggfnoid)?;
        if !OidIsValid(aggsortop) {
            return Ok(false); /* not a MIN/MAX aggregate */
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
                None => return Ok(false),
            }
        };

        // if (contain_mutable_functions(curTarget->expr)) return false;
        if backend_optimizer_util_clauses::contain_mutable_functions(Some(&cur_expr))? {
            return Ok(false); /* not potentially indexable */
        }

        // if (type_is_rowtype(exprType(curTarget->expr))) return false;
        let exprtype = backend_nodes_core::nodefuncs::expr_type(Some(&cur_expr))?;
        if backend_utils_cache_lsyscache_seams::type_is_rowtype::call(exprtype)? {
            return Ok(false); /* IS NOT NULL would have weird semantics */
        }

        // This aggregate is a genuine MIN/MAX candidate. In C we would build a
        // MinMaxAggInfo and continue; the caller's path-build step is the
        // keystone, so we keep going (other aggs might still disqualify the
        // query) but the caller will panic if *all* aggs qualify.
    }
    Ok(true)
}

/// `fetch_agg_sort_op(aggfnoid)` (planagg.c:499) — `SearchSysCache1(AGGFNOID)` +
/// `GETSTRUCT(Form_pg_aggregate)->aggsortop`. Reuses the installed
/// `agg_form_by_oid` syscache projection (the same `AGGFNOID` lookup), reading
/// its `aggsortop`. `InvalidOid` on a cache miss (C: `!HeapTupleIsValid`).
fn fetch_agg_sort_op<'mcx>(mcx: Mcx<'mcx>, aggfnoid: Oid) -> PgResult<Oid> {
    match backend_utils_cache_syscache_seams::agg_form_by_oid::call(mcx, aggfnoid)? {
        Some(form) => Ok(form.aggsortop),
        None => Ok(InvalidOid),
    }
}

/// The cross-root subquery-planner / path-arena keystone the MIN/MAX
/// index-path optimization bottoms out on (`build_minmax_path` +
/// `MinMaxAggInfo` arena node + `MinMaxAggPath`). Reached only when every
/// aggregate in a plain single-table query is a genuine MIN/MAX aggregate.
fn minmax_path_keystone() -> types_error::PgError {
    types_error::PgError::error(String::from(
        "preprocess_minmax_aggregates: build_minmax_path / MinMaxAggPath \
         index-path construction (planagg.c) is the cross-root subquery-planner \
         path-arena keystone — MinMaxAggInfo is not yet an arena node and there \
         is no per-aggregate subroot. MIN/MAX over an indexable single-table \
         query cannot yet use the indexscan shortcut.",
    ))
}
