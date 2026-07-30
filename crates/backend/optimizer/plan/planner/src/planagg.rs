//! planagg.c: MIN/MAX aggregate -> ordered-index-scan InitPlan rewrite.

use mcx::{alloc_leak_in, PgVec};
use types_error::{PgError, PgResult};
use types_nodes::list::NodeList;
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::{FromExpr, NullTest, NullTestType};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{MinMaxAggInfo, UPPERREL_GROUP_AGG};

use crate::pathnode::{add_path, create_pathtarget};
use crate::planmain::query_planner;
use crate::run::PlannerRun;

pub fn preprocess_minmax_aggregates<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let parse = run.parse();
    debug_assert!(run.root.minmax_aggs.is_empty());
    if !parse.hasAggs {
        return Ok(());
    }
    debug_assert!(parse.setOperations.is_none());
    debug_assert!(parse.rowMarks.is_nil());

    if !parse.groupClause.is_nil() || parse.groupingSets.len() > 1 || parse.hasWindowFuncs {
        return Ok(());
    }
    if !parse.cteList.is_nil() {
        return Ok(());
    }

    let top = parse.jointree.expect("jointree is a FromExpr");
    if top.fromlist.len() != 1 {
        return Ok(());
    }
    let mut jtnode = top.fromlist.nth(0);
    while jtnode.node_tag() == NodeTag::T_FromExpr {
        let f = jtnode.as_from_expr().unwrap();
        if f.fromlist.len() != 1 {
            return Ok(());
        }
        jtnode = f.fromlist.nth(0);
    }
    let Some(rtr) = jtnode.as_range_tbl_ref() else {
        return Ok(());
    };
    // planner_rt_fetch pre-query_planner: simple_rte_array is unbuilt, read
    // the parse rtable directly.
    let rte = parse
        .rtable
        .nth(rtr.rtindex as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    match rte.rtekind {
        RTEKind::RTE_RELATION => {}
        RTEKind::RTE_SUBQUERY if rte.inh => {}
        _ => return Ok(()),
    }

    let Some(mut aggs_list) = can_minmax_aggs(run)? else {
        return Ok(());
    };

    for i in 0..aggs_list.len() {
        let aggsortop = aggs_list[i].aggsortop;
        let (eqop, reverse) =
            match lsyscache::amop::get_equality_op_for_ordering_op(aggsortop)? {
                Some((eqop, reverse)) if eqop != 0 => (eqop, reverse),
                _ => {
                    return Err(could_not_find_eqop(aggsortop));
                }
            };
        // NULLS FIRST is likelier available under a reverse-sort operator, so
        // try reverse's polarity first (planagg.c).
        if build_minmax_path(run, &mut aggs_list[i], eqop, aggsortop, reverse, reverse)? {
            continue;
        }
        if build_minmax_path(run, &mut aggs_list[i], eqop, aggsortop, reverse, !reverse)? {
            continue;
        }
        return Ok(());
    }

    for i in 0..aggs_list.len() {
        let target = *run.root.expr_node(aggs_list[i].target);
        let (ty, _) = crate::costsize::expr_type_typmod(target);
        let coll = crate::pathkeys::expr_collation(target);
        // SS_make_initplan_output_param (subselect.c).
        let (_, prm_node) = crate::subselect::generate_new_exec_param(run, ty, -1, coll)?;
        aggs_list[i].param = run.intern_expr(prm_node);
    }

    let grouped_rel = crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_GROUP_AGG);
    let target = create_pathtarget(run, run.processed_tlist())?;
    let quals: PgVec<'mcx, types_pathnodes::NodeId> = {
        let mut v = PgVec::new_in(run.mcx);
        if let Some(h) = parse.havingQual {
            for hc in h.as_list().expect("preprocessed havingQual is a list") {
                v.push(run.intern_expr(hc));
            }
        }
        v
    };
    let mm_path = crate::pathnode::create_minmaxagg_path(run, grouped_rel, target, aggs_list, quals)?;
    add_path(run, grouped_rel, mm_path);
    Ok(())
}

fn can_minmax_aggs<'mcx>(
    run: &mut PlannerRun<'mcx>,
) -> PgResult<Option<PgVec<'mcx, MinMaxAggInfo>>> {
    let mut list: PgVec<'mcx, MinMaxAggInfo> = PgVec::new_in(run.mcx);
    let agginfo_ids = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.agginfos);
    for aid in agginfo_ids.iter() {
        let aggref_id = run.root.agg_info(*aid).aggrefs[0];
        let aggref = run
            .root
            .expr_node(aggref_id)
            .as_aggref()
            .expect("AggInfo.aggrefs holds Aggrefs");
        debug_assert_eq!(aggref.agglevelsup, 0);
        if aggref.args.len() != 1 {
            return Ok(None);
        }
        // ORDER BY changes MIN/MAX semantics when the opclass sees distinct
        // values as equal; it also rejects ordered-set aggs (planagg.c).
        if !aggref.aggorder.is_nil() {
            return Ok(None);
        }
        if aggref.aggfilter.is_some() {
            return Ok(None);
        }
        let aggsortop = fetch_agg_sort_op(aggref.aggfnoid)?;
        if aggsortop == 0 {
            return Ok(None);
        }
        let expr = aggref
            .args
            .nth(0)
            .as_target_entry()
            .expect("Aggref.args holds TargetEntries")
            .expr;
        if clauses::contain_mutable_functions(expr)? {
            return Ok(None);
        }
        let (ty, _) = crate::costsize::expr_type_typmod(expr);
        if lsyscache::type_is_rowtype(ty)? {
            return Ok(None);
        }
        list.push(MinMaxAggInfo {
            aggfnoid: aggref.aggfnoid,
            aggsortop,
            target: run.intern_expr(expr),
            ..MinMaxAggInfo::default()
        });
    }
    Ok(Some(list))
}

fn build_minmax_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mminfo: &mut MinMaxAggInfo,
    eqop: types_core::Oid,
    sortop: types_core::Oid,
    reverse_sort: bool,
    nulls_first: bool,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let target = *run.root.expr_node(mminfo.target);
    // Deep copy per C (planagg.c:353 copyObject): the probe planning
    // scribbles in place (reduce_outer_joins' nullingrels strip, RTE jointype
    // flips) and the main planning re-plans the same subquery RTEs; a shared
    // tree leaves the second pass with stripped quals under an unreduced
    // jointree ("wrong varnullingrels" in setrefs).
    let deep = rewrite_manip::copy_query_node(mcx, run.parse())?;
    // IncrementVarSublevelsUp((Node *) parse, 1, 1) (planagg.c): the clone
    // becomes a subquery, so outer references (a correlated minmax rewrite)
    // now live one level higher. At query_level 1 no Var can carry
    // varlevelsup > 0, so the walk is skipped to keep that path unchanged.
    if run.root.query_level != 1 {
        rewrite_manip::IncrementVarSublevelsUp(deep, 1, 1)?;
    }
    let mut subparse =
        crate::subselect::query_cells_copy(mcx, deep.as_query().expect("Query round trip"))?;

    // C copyObject's mminfo->target into the tle and the NullTest; the outer
    // node must stay outside the probe's scribble scope. The out/read round
    // trip covers post-preprocess nodes (the target can be a SubPlan:
    // "sublinks within outer-level aggregates", aggregates.sql).
    let tle_target = rewrite_manip::copy_node(mcx, target)?;
    let ntest_target = rewrite_manip::copy_node(mcx, target)?;
    let tle = Node::mk_target_entry(mcx, tle_target, 1, Some("agg_target"), false)?;
    // assignSortGroupRef: the single fresh tle takes ref 1.
    // SAFETY: freshly built node; no other reference is live.
    unsafe {
        tle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .expect("TargetEntry node");
    subparse.targetList = NodeList::make1(mcx, tle)?;

    subparse.havingQual = None;
    subparse.distinctClause = NodeList::nil();
    subparse.hasDistinctOn = false;
    subparse.hasAggs = false;

    let ntest = Node::mk(
        mcx,
        NullTest {
            arg: Some(ntest_target),
            nulltesttype: NullTestType::IS_NOT_NULL,
            argisrow: false,
            location: -1,
        },
    )?;
    let old_f = subparse.jointree.expect("jointree is a FromExpr");
    let mut quals = match old_f.quals {
        Some(q) => q.as_list().expect("preprocessed quals are a list").clone_in(mcx)?,
        None => NodeList::nil(),
    };
    if !quals.iter().any(|n| types_nodes::equal(n, ntest)) {
        let mut with_ntest = NodeList::make1(mcx, ntest)?;
        for q in &quals {
            with_ntest.lappend(mcx, q)?;
        }
        quals = with_ntest;
    }
    subparse.jointree = Some(alloc_leak_in(
        mcx,
        FromExpr {
            fromlist: old_f.fromlist.clone_in(mcx)?,
            quals: Some(Node::mk_list(mcx, quals)?),
        },
    )?);

    let sortcl = Node::mk(
        mcx,
        types_nodes::parsenodes::SortGroupClause {
            tleSortGroupRef: 1,
            eqop,
            sortop,
            reverse_sort,
            nulls_first,
            hashable: false,
        },
    )?;
    subparse.sortClause = NodeList::make1(mcx, sortcl)?;

    subparse.limitOffset = None;
    subparse.limitCount = Some(Node::mk_const(
        mcx,
        types_core::catalog::INT8OID,
        -1,
        0,
        8,
        datum::Datum::from_i64(1),
        false,
        true,
    )?);

    let sealed: &'mcx types_nodes::parsenodes::Query<'mcx> = alloc_leak_in(mcx, subparse)?;

    run.push_minmax_root()?;
    // append_rel_list carries over from the outer root (planagg.c:353-354's
    // copyObject): a pulled-up UNION ALL target keeps its appendrel there.
    run.root.parse = run.intern_query(sealed);
    run.processed_tlist = Some(&sealed.targetList);
    run.root.tuple_fraction = 1.0;
    run.root.limit_tuples = 1.0;

    let final_rel = query_planner(run, minmax_qp_callback)?;

    // SS_identify_outer_params ran at push_minmax_root (run.rs).
    crate::subselect::ss_charge_for_initplans(run, final_rel)?;

    let final_rows = run.root.rel(final_rel).rows;
    let path_fraction = if final_rows > 1.0 { 1.0 / final_rows } else { 1.0 };

    let pathlist = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(final_rel).pathlist);
    let query_pathkeys =
        crate::relnode::pgvec_clone_shallow(mcx, &run.root.query_pathkeys);
    let sorted_path = crate::pathkeys::get_cheapest_fractional_path_for_pathkeys(
        run,
        &pathlist,
        &query_pathkeys,
        path_fraction,
    );
    let Some(sorted_path) = sorted_path else {
        run.pop_root_to_minmax_subroot();
        return Ok(false);
    };

    let proj_target = create_pathtarget(run, run.processed_tlist())?;
    let sorted_path =
        crate::pathnode::apply_projection_to_path(run, final_rel, sorted_path, proj_target)?;

    // Matches compare_fractional_path_costs (planagg.c note).
    let (startup, total) = {
        let p = run.root.path(sorted_path).base();
        (p.startup_cost, p.total_cost)
    };
    let path_cost = startup + path_fraction * (total - startup);

    let subroot_idx = run.pop_root_to_minmax_subroot();
    mminfo.pathcost = path_cost;
    mminfo.subroot_idx = Some(subroot_idx);
    mminfo.subroot_path = Some(sorted_path);
    Ok(true)
}

fn minmax_qp_callback<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    run.root.group_pathkeys = PgVec::new_in(run.mcx);
    run.root.num_groupby_pathkeys = 0;
    run.root.window_pathkeys = PgVec::new_in(run.mcx);
    run.root.distinct_pathkeys = PgVec::new_in(run.mcx);
    let parse = run.parse();
    run.root.sort_pathkeys =
        crate::pathkeys::make_pathkeys_for_sortclauses(run, &parse.sortClause, &parse.targetList)?;
    run.root.query_pathkeys =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.sort_pathkeys);
    Ok(())
}

fn fetch_agg_sort_op(aggfnoid: types_core::Oid) -> PgResult<types_core::Oid> {
    Ok(match syscache_seams::lookup_pg_aggregate_shape::call(aggfnoid)? {
        Some(form) => form.aggsortop,
        None => 0,
    })
}

pub(crate) use types_pathnodes::run::subroot_path_base;

#[track_caller]
#[cold]
#[inline(never)]
fn could_not_find_eqop(aggsortop: types_core::Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "could not find equality operator for ordering operator {aggsortop}"
    )))
}
