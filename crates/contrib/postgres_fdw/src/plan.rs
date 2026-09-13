use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{Oid, BLCKSZ};
use types_error::{
    PgError, PgResult,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::list::{IntList, NodeList};
use types_nodes::{FdwKind, FdwRoutine, Node};
use types_pathnodes::relids::{relids_is_empty, relids_is_subset, relids_overlap};
use types_pathnodes::{
    EcId, EmId, PathId, PathKey, PathNode, PtId, RelId, RinfoId, COMPARE_LT,
    RELOPT_BASEREL, RELOPT_JOINREL, RELOPT_OTHER_JOINREL, RELOPT_OTHER_MEMBER_REL,
    RELOPT_OTHER_UPPER_REL, RELOPT_UPPER_REL, UPPERREL_FINAL, UPPERREL_GROUP_AGG,
    UPPERREL_ORDERED,
};
use types_tuple::{SizeofHeapTupleHeader, MAXALIGN};

use executils::EStateData;
use nodeforeignscan::{FdwExecRoutine, ForeignScanState};
use planner::fdwplan::{FdwPlanRoutine, UpperPathExtra};
use planner::run::PlannerRun;

use crate::deparse;
use crate::relinfo::{attach_fpinfo, fpinfo, fpinfo_opt, PgFdwRelationInfo};

const DEFAULT_FDW_STARTUP_COST: f64 = 100.0;
const DEFAULT_FDW_TUPLE_COST: f64 = 0.2;
/// If no remote estimates, assume a sort costs 20% extra (postgres_fdw.c:64).
const DEFAULT_FDW_SORT_MULTIPLIER: f64 = 1.2;

/// PgFdwPathExtraData (postgres_fdw.c:295-302): the extra information
/// estimate_path_cost_size gets for a path that performs the final sort
/// and/or the LIMIT restriction remotely.
pub(crate) struct PgFdwPathExtraData {
    pub target: PtId,
    pub has_final_sort: bool,
    pub has_limit: bool,
    pub limit_tuples: f64,
    pub count_est: i64,
    pub offset_est: i64,
}

// IS_SIMPLE_REL (pathnodes.h).
fn is_simple_rel(run: &PlannerRun<'_>, rel: RelId) -> bool {
    matches!(run.root.rel(rel).reloptkind, RELOPT_BASEREL | RELOPT_OTHER_MEMBER_REL)
}

// IS_UPPER_REL (pathnodes.h:873).
fn is_upper_rel(run: &PlannerRun<'_>, rel: RelId) -> bool {
    matches!(run.root.rel(rel).reloptkind, RELOPT_UPPER_REL | RELOPT_OTHER_UPPER_REL)
}

// IS_OTHER_REL (pathnodes.h).
fn is_other_rel(run: &PlannerRun<'_>, rel: RelId) -> bool {
    matches!(
        run.root.rel(rel).reloptkind,
        RELOPT_OTHER_MEMBER_REL | RELOPT_OTHER_JOINREL | RELOPT_OTHER_UPPER_REL
    )
}

// ---------- handler ----------

fn fc_postgres_fdw_handler(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    static ROUTINE: FdwRoutine = FdwRoutine::new(FdwKind::PostgresFdw);
    Ok(Datum::from_usize(&ROUTINE as *const FdwRoutine as usize))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "postgres_fdw_handler" => fc_postgres_fdw_handler,
        "postgres_fdw_validator" => crate::option::fc_postgres_fdw_validator,
        "postgres_fdw_get_connections" => crate::connection::fc_postgres_fdw_get_connections,
        "postgres_fdw_get_connections_1_2" => {
            crate::connection::fc_postgres_fdw_get_connections_1_2
        }
        "postgres_fdw_disconnect" => crate::connection::fc_postgres_fdw_disconnect,
        "postgres_fdw_disconnect_all" => crate::connection::fc_postgres_fdw_disconnect_all,
        _ => return None,
    })
}

// ---------- option application ----------

fn apply_server_options<'mcx>(
    fp: &mut PgFdwRelationInfo<'mcx>,
    mcx: Mcx<'mcx>,
    server: &foreigncmds::foreign::ForeignServer<'mcx>,
) -> PgResult<()> {
    let mut opts: Vec<(String, String)> = Vec::new();
    for o in server.options.iter() {
        opts.push((o.name.to_string(), o.require_value()?.to_string()));
    }
    for (name, value) in opts {
        match name.as_str() {
            "use_remote_estimate" => fp.use_remote_estimate = parse_bool(&value)?,
            "fdw_startup_cost" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_real(&value, 0) {
                    fp.fdw_startup_cost = v;
                }
            }
            "fdw_tuple_cost" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_real(&value, 0) {
                    fp.fdw_tuple_cost = v;
                }
            }
            "extensions" => {
                fp.shippable_extensions = crate::option::extract_extension_list(mcx, &value, false)?;
            }
            "fetch_size" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(&value, 0) {
                    fp.fetch_size = v;
                }
            }
            "async_capable" => fp.async_capable = parse_bool(&value)?,
            _ => {}
        }
    }
    Ok(())
}

fn apply_table_options<'mcx>(
    fp: &mut PgFdwRelationInfo<'mcx>,
    table: &foreigncmds::foreign::ForeignTable<'mcx>,
) -> PgResult<()> {
    let mut opts: Vec<(String, String)> = Vec::new();
    for o in table.options.iter() {
        opts.push((o.name.to_string(), o.require_value()?.to_string()));
    }
    for (name, value) in opts {
        match name.as_str() {
            "use_remote_estimate" => fp.use_remote_estimate = parse_bool(&value)?,
            "fetch_size" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(&value, 0) {
                    fp.fetch_size = v;
                }
            }
            "async_capable" => fp.async_capable = parse_bool(&value)?,
            _ => {}
        }
    }
    Ok(())
}

fn parse_bool(value: &str) -> PgResult<bool> {
    // defGetBoolean's string forms (commands_define); options are already
    // validator-checked, so this only meets true/false/on/off/1/0/yes/no.
    Ok(matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "on" | "1" | "yes" | "y"
    ))
}

// ---------- GetForeignRelSize ----------

fn postgres_get_foreign_rel_size<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    foreigntableid: Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut fp = PgFdwRelationInfo::new(mcx);
    fp.pushdown_safe = true;
    let table = foreigncmds::foreign::GetForeignTable(mcx, foreigntableid)?;
    fp.serverid = table.serverid;
    let server = foreigncmds::foreign::GetForeignServer(mcx, table.serverid)?;

    fp.use_remote_estimate = false;
    fp.fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
    fp.fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
    fp.fetch_size = 100;
    fp.async_capable = false;
    apply_server_options(&mut fp, mcx, &server)?;
    apply_table_options(&mut fp, &table)?;

    // classifyConditions over baserestrictinfo.
    let baserestrict: Vec<RinfoId> =
        run.root.rel(rel_id).baserestrictinfo.iter().copied().collect();
    let mut remote_conds = PgVec::new_in(mcx);
    let mut local_conds = PgVec::new_in(mcx);
    attach_fpinfo(mcx, run.root.rel_mut(rel_id), fp)?;
    deparse::classify_conditions(run, rel_id, &baserestrict, &mut remote_conds, &mut local_conds)?;

    // attrs_used: reltarget exprs + local_conds clauses.
    let varno = run.root.rel(rel_id).relid as i32;
    let mut attrs_used = types_nodes::Bitmapset::empty();
    let expr_ids: Vec<types_pathnodes::NodeId> =
        run.pathtarget(run.rel_reltarget_id(rel_id)).exprs.iter().copied().collect();
    for id in expr_ids {
        vars::pull_varattnos(mcx, *run.root.expr_node(id), varno, &mut attrs_used)?;
    }
    for &ri in local_conds.iter() {
        let clause = run.root.rinfo(ri).clause;
        vars::pull_varattnos(mcx, *run.root.expr_node(clause), varno, &mut attrs_used)?;
    }

    let varrelid = run.root.rel(rel_id).relid as i32;
    let local_slice: Vec<RinfoId> = local_conds.iter().copied().collect();
    let local_conds_sel = planner::clausesel::clauselist_selectivity(
        run,
        &local_slice,
        varrelid,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut local_cost = types_pathnodes::QualCost::default();
    for &ri in &local_slice {
        let clause = run.root.rinfo(ri).clause;
        let node = *run.root.expr_node(clause);
        let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
        local_cost.startup += c.startup;
        local_cost.per_tuple += c.per_tuple;
    }

    {
        let fpc = fpinfo(run.root.rel(rel_id));
        let mut fpm = fpc.borrow_mut();
        fpm.remote_conds = remote_conds;
        fpm.local_conds = local_conds;
        fpm.attrs_used = attrs_used;
        fpm.local_conds_sel = local_conds_sel;
        fpm.local_conds_cost = local_cost;
        fpm.retrieved_rows = -1.0;
        fpm.rel_startup_cost = -1.0;
        fpm.rel_total_cost = -1.0;
    }

    let use_remote_estimate = fpinfo(run.root.rel(rel_id)).borrow().use_remote_estimate;
    if use_remote_estimate {
        let (rows, width, disabled_nodes, startup_cost, total_cost) =
            estimate_path_cost_size(run, rel_id, &[], &[], None)?;
        {
            let fpc = fpinfo(run.root.rel(rel_id));
            let mut fpm = fpc.borrow_mut();
            fpm.rows = rows;
            fpm.width = width;
            fpm.disabled_nodes = disabled_nodes;
            fpm.startup_cost = startup_cost;
            fpm.total_cost = total_cost;
        }
        run.root.rel_mut(rel_id).rows = rows;
        let pt = run.rel_reltarget_id(rel_id);
        run.root.pathtarget_mut(pt).width = width;
    } else {
        // 10-page hack for never-ANALYZEd tables (plancat.c parity).
        {
            let r = run.root.rel(rel_id);
            if r.tuples < 0.0 {
                let width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
                let tuple_den = width as i64 + MAXALIGN(SizeofHeapTupleHeader) as i64;
                // C integer division: baserel->tuples = (10 * BLCKSZ) / (...).
                let tuples = ((10 * BLCKSZ as i64) / tuple_den) as f64;
                let r = run.root.rel_mut(rel_id);
                r.pages = 10;
                r.tuples = tuples;
            }
        }
        planner::costsize::set_baserel_size_estimates(run, rel_id)?;

        let (rows, width, disabled_nodes, startup_cost, total_cost) =
            estimate_path_cost_size(run, rel_id, &[], &[], None)?;
        {
            let fpc = fpinfo(run.root.rel(rel_id));
            let mut fpm = fpc.borrow_mut();
            fpm.rows = rows;
            fpm.width = width;
            fpm.disabled_nodes = disabled_nodes;
            fpm.startup_cost = startup_cost;
            fpm.total_cost = total_cost;
        }
    }

    {
        let fpc = fpinfo(run.root.rel(rel_id));
        let mut fpm = fpc.borrow_mut();
        fpm.relation_name = mcx_str(mcx, &format!("{varno}"))?;
        fpm.relation_index = varno;
    }
    Ok(())
}

// estimate_path_cost_size (postgres_fdw.c:3106): the remote-EXPLAIN lane when
// use_remote_estimate, else the local lane (seqscan-shaped for base rels,
// cached input costs for joins/groupings); both feed the cached rel_* costs
// and the FDW startup/transfer tail. `pathkeys` costs a remotely-sorted
// output; `fpextra` a path performing the final sort / LIMIT remotely.
// Returns (rows, width, disabled_nodes, startup_cost, total_cost).
pub(crate) fn estimate_path_cost_size<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    param_join_conds: &[RinfoId],
    pathkeys: &[PathKey],
    fpextra: Option<&PgFdwPathExtraData>,
) -> PgResult<(f64, i32, i32, f64, f64)> {
    let mcx = run.mcx;
    let use_remote_estimate = fpinfo(run.root.rel(rel_id)).borrow().use_remote_estimate;
    let disabled_nodes = 0;
    let mut rows;
    let width;
    let mut retrieved_rows;
    let mut startup_cost;
    let mut total_cost;

    if use_remote_estimate {
        let mut remote_param_join_conds = PgVec::new_in(mcx);
        let mut local_param_join_conds = PgVec::new_in(mcx);
        crate::deparse::classify_conditions(
            run,
            rel_id,
            param_join_conds,
            &mut remote_param_join_conds,
            &mut local_param_join_conds,
        )?;

        let mut remote_conds: Vec<RinfoId> = remote_param_join_conds.iter().copied().collect();
        remote_conds
            .extend(fpinfo(run.root.rel(rel_id)).borrow().remote_conds.iter().copied());

        let fdw_scan_tlist = if matches!(
            run.root.rel(rel_id).reloptkind,
            types_pathnodes::RELOPT_JOINREL
                | types_pathnodes::RELOPT_OTHER_JOINREL
                | types_pathnodes::RELOPT_UPPER_REL
                | types_pathnodes::RELOPT_OTHER_UPPER_REL
        ) {
            build_tlist_to_deparse(run, rel_id)?
        } else {
            NodeList::nil()
        };
        let (query, _retrieved_attrs, _) = crate::deparse::deparse_select_stmt_for_rel(
            run,
            rel_id,
            &fdw_scan_tlist,
            &remote_conds,
            pathkeys,
            fpextra.is_some_and(|f| f.has_final_sort),
            fpextra.is_some_and(|f| f.has_limit),
            None,
        )?;
        let mut sql = String::with_capacity(query.as_str().len() + 8);
        sql.push_str("EXPLAIN ");
        sql.push_str(query.as_str());

        let (serverid, local_conds_sel, local_conds_cost) = {
            let fp = fpinfo(run.root.rel(rel_id)).borrow();
            (fp.serverid, fp.local_conds_sel, fp.local_conds_cost)
        };
        let userid = {
            let u = run.root.rel(rel_id).userid;
            if u != types_core::InvalidOid { u } else { miscinit::GetUserId() }
        };
        let user = foreigncmds::foreign::GetUserMapping(mcx, userid, serverid)?;
        let conn_key = crate::connection::get_connection(mcx, &user, false)?;
        let (r_rows, r_width, r_startup, r_total) = get_remote_estimate(conn_key, &sql)?;
        crate::connection::release_connection(conn_key);

        retrieved_rows = r_rows;
        width = r_width;
        startup_cost = r_startup;
        total_cost = r_total;

        let varrelid = run.root.rel(rel_id).relid as i32;
        let local_ids: Vec<RinfoId> = local_param_join_conds.iter().copied().collect();
        let mut local_sel = planner::clausesel::clauselist_selectivity(
            run,
            &local_ids,
            varrelid,
            types_pathnodes::JOIN_INNER,
            None,
        )?;
        local_sel *= local_conds_sel;
        rows = planner::costsize::clamp_row_est(r_rows * local_sel);

        startup_cost += local_conds_cost.startup;
        total_cost += local_conds_cost.per_tuple * retrieved_rows;
        let mut local_cost = types_pathnodes::QualCost::default();
        for &ri in &local_ids {
            let clause = run.root.rinfo(ri).clause;
            let node = *run.root.expr_node(clause);
            let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
            local_cost.startup += c.startup;
            local_cost.per_tuple += c.per_tuple;
        }
        startup_cost += local_cost.startup;
        total_cost += local_cost.per_tuple * retrieved_rows;

        let rt_cost = run.pathtarget(run.rel_reltarget_id(rel_id)).cost;
        startup_cost += rt_cost.startup;
        total_cost += rt_cost.startup;
        total_cost += rt_cost.per_tuple * rows;
        if matches!(
            run.root.rel(rel_id).reloptkind,
            types_pathnodes::RELOPT_UPPER_REL | types_pathnodes::RELOPT_OTHER_UPPER_REL
        ) {
            // Some tlist expressions (grouping/aggs) run remotely; back out
            // their local eval cost.
            let mut tlist_cost = types_pathnodes::QualCost::default();
            for tle_node in &fdw_scan_tlist {
                let tle = tle_node.as_target_entry().expect("TargetEntry");
                let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), tle.expr)?;
                tlist_cost.startup += c.startup;
                tlist_cost.per_tuple += c.per_tuple;
            }
            startup_cost -= tlist_cost.startup;
            total_cost -= tlist_cost.startup;
            total_cost -= tlist_cost.per_tuple * rows;
        }
    } else {
        debug_assert!(param_join_conds.is_empty());
        let mut run_cost;
        let (cached_startup, cached_total, cached_retrieved, cached_rows, cached_width) = {
            let fp = fpinfo(run.root.rel(rel_id)).borrow();
            (fp.rel_startup_cost, fp.rel_total_cost, fp.retrieved_rows, fp.rows, fp.width)
        };
        if cached_startup >= 0.0 && cached_total >= 0.0 {
            debug_assert!(cached_retrieved >= 0.0);
            rows = cached_rows;
            retrieved_rows = cached_retrieved;
            width = cached_width;
            startup_cost = cached_startup;
            run_cost = cached_total - cached_startup;
            // Costing a scan/join with additional post-scan/join-processing
            // steps: the cached costs predate apply_scanjoin_target_to_paths'
            // final target, so add its eval costs now (postgres_fdw.c:3229).
            if let Some(f) = fpextra {
                if !is_upper_rel(run, rel_id) {
                    // Shouldn't get here unless we have LIMIT.
                    debug_assert!(f.has_limit);
                    debug_assert!(matches!(
                        run.root.rel(rel_id).reloptkind,
                        RELOPT_BASEREL | RELOPT_JOINREL
                    ));
                    let rt_cost = run.pathtarget(run.rel_reltarget_id(rel_id)).cost;
                    startup_cost += rt_cost.startup;
                    run_cost += rt_cost.per_tuple * rows;
                }
            }
        } else if matches!(
            run.root.rel(rel_id).reloptkind,
            types_pathnodes::RELOPT_JOINREL | types_pathnodes::RELOPT_OTHER_JOINREL
        ) {
            // Join: children's cached (pre-network) costs + qual application.
            let (outerrel, innerrel, local_conds_sel, local_conds_cost, joinclause_sel) = {
                let fp = fpinfo(run.root.rel(rel_id)).borrow();
                (
                    fp.outerrel.expect("join fpinfo outerrel"),
                    fp.innerrel.expect("join fpinfo innerrel"),
                    fp.local_conds_sel,
                    fp.local_conds_cost,
                    fp.joinclause_sel,
                )
            };
            rows = run.root.rel(rel_id).rows;
            width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
            let (o_rows, o_start, o_total) = {
                let fp = fpinfo(run.root.rel(outerrel)).borrow();
                (fp.rows, fp.rel_startup_cost, fp.rel_total_cost)
            };
            let (i_rows, i_start, i_total) = {
                let fp = fpinfo(run.root.rel(innerrel)).borrow();
                (fp.rows, fp.rel_startup_cost, fp.rel_total_cost)
            };
            let mut nrows = i_rows * o_rows;
            retrieved_rows = planner::costsize::clamp_row_est(rows / local_conds_sel)
                .min(nrows);

            let qual_cost = |run: &mut PlannerRun<'mcx>,
                             ids: &[RinfoId]|
             -> PgResult<types_pathnodes::QualCost> {
                let mut c = types_pathnodes::QualCost::default();
                for &ri in ids {
                    let clause = run.root.rinfo(ri).clause;
                    let node = *run.root.expr_node(clause);
                    let q = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
                    c.startup += q.startup;
                    c.per_tuple += q.per_tuple;
                }
                Ok(c)
            };
            let (remote_ids, join_ids): (Vec<RinfoId>, Vec<RinfoId>) = {
                let fp = fpinfo(run.root.rel(rel_id)).borrow();
                (fp.remote_conds.iter().copied().collect(), fp.joinclauses.iter().copied().collect())
            };
            let remote_conds_cost = qual_cost(run, &remote_ids)?;
            let join_cost = qual_cost(run, &join_ids)?;

            startup_cost = i_start + o_start;
            startup_cost += join_cost.startup;
            startup_cost += remote_conds_cost.startup;
            startup_cost += local_conds_cost.startup;

            let mut rc = (i_total - i_start) + (o_total - o_start);
            rc += nrows * join_cost.per_tuple;
            nrows = planner::costsize::clamp_row_est(nrows * joinclause_sel);
            rc += nrows * remote_conds_cost.per_tuple;
            rc += local_conds_cost.per_tuple * retrieved_rows;

            let rt_cost = run.pathtarget(run.rel_reltarget_id(rel_id)).cost;
            startup_cost += rt_cost.startup;
            rc += rt_cost.per_tuple * rows;
            run_cost = rc;
        } else if matches!(
            run.root.rel(rel_id).reloptkind,
            types_pathnodes::RELOPT_UPPER_REL | types_pathnodes::RELOPT_OTHER_UPPER_REL
        ) {
            // Grouping/aggregation atop the (cached) input rel costs; a
            // blend of cost_agg's sorted and hashed models, per C.
            let (outerrel, local_conds_sel, local_conds_cost) = {
                let fp = fpinfo(run.root.rel(rel_id)).borrow();
                (
                    fp.outerrel.expect("upper fpinfo outerrel"),
                    fp.local_conds_sel,
                    fp.local_conds_cost,
                )
            };
            let (input_rows, o_start, o_total) = {
                let ofp = fpinfo(run.root.rel(outerrel)).borrow();
                (ofp.rows, ofp.rel_startup_cost, ofp.rel_total_cost)
            };
            let mut aggcosts = types_pathnodes::AggClauseCosts::default();
            if run.parse().hasAggs {
                planner::prepagg::get_agg_clause_costs(
                    run,
                    types_pathnodes::AGGSPLIT_SIMPLE,
                    &mut aggcosts,
                )?;
            }
            let num_group_cols = run.root.processed_groupClause.len();
            let grouped_tlist =
                fpinfo(run.root.rel(rel_id)).borrow().grouped_tlist.clone_in(mcx)?;
            let clauses = planner::relnode::pgvec_clone_shallow(
                mcx,
                &run.root.processed_groupClause,
            );
            let group_exprs =
                types_pathnodes::run::sortgrouplist_exprs(run, &clauses, &grouped_tlist);
            let num_groups =
                planner::selfuncs::estimate_num_groups(run, &group_exprs, input_rows)?;

            if run.root.hasHavingQual {
                let remote_ids: Vec<RinfoId> = fpinfo(run.root.rel(rel_id))
                    .borrow()
                    .remote_conds
                    .iter()
                    .copied()
                    .collect();
                let remote_sel = planner::clausesel::clauselist_selectivity(
                    run,
                    &remote_ids,
                    0,
                    types_pathnodes::JOIN_INNER,
                    None,
                )?;
                retrieved_rows = planner::costsize::clamp_row_est(num_groups * remote_sel);
                rows = planner::costsize::clamp_row_est(retrieved_rows * local_conds_sel);
            } else {
                rows = num_groups;
                retrieved_rows = num_groups;
            }
            width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;

            let o_rt_cost = run.pathtarget(run.rel_reltarget_id(outerrel)).cost;
            startup_cost = o_start;
            startup_cost += o_rt_cost.startup;
            startup_cost += aggcosts.transCost.startup;
            startup_cost += aggcosts.transCost.per_tuple * input_rows;
            startup_cost += aggcosts.finalCost.startup;
            startup_cost +=
                (planner::costsize::gucs::cpu_operator_cost() * num_group_cols as f64)
                    * input_rows;

            let mut rc = o_total - o_start;
            rc += o_rt_cost.per_tuple * input_rows;
            rc += aggcosts.finalCost.per_tuple * num_groups;
            rc += planner::costsize::gucs::cpu_tuple_cost() * num_groups;

            if run.root.hasHavingQual {
                let remote_ids: Vec<RinfoId> = fpinfo(run.root.rel(rel_id))
                    .borrow()
                    .remote_conds
                    .iter()
                    .copied()
                    .collect();
                let mut remote_cost = types_pathnodes::QualCost::default();
                for &ri in &remote_ids {
                    let clause = run.root.rinfo(ri).clause;
                    let node = *run.root.expr_node(clause);
                    let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
                    remote_cost.startup += c.startup;
                    remote_cost.per_tuple += c.per_tuple;
                }
                startup_cost += remote_cost.startup;
                rc += remote_cost.per_tuple * num_groups;
                startup_cost += local_conds_cost.startup;
                rc += local_conds_cost.per_tuple * retrieved_rows;
            }

            let rt_cost = run.pathtarget(run.rel_reltarget_id(rel_id)).cost;
            startup_cost += rt_cost.startup;
            rc += rt_cost.per_tuple * rows;
            run_cost = rc;
        } else {
            let local_conds_sel = fpinfo(run.root.rel(rel_id)).borrow().local_conds_sel;
            let r = run.root.rel(rel_id);
            rows = r.rows;
            width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
            let tuples = r.tuples;
            let pages = r.pages;
            let baserestrict_startup = r.baserestrictcost.startup;
            let baserestrict_per_tuple = r.baserestrictcost.per_tuple;

            let mut rr = planner::costsize::clamp_row_est(rows / local_conds_sel.max(1e-9));
            if rr > tuples {
                rr = tuples;
            }
            retrieved_rows = rr;
            startup_cost = 0.0;
            let mut rc = planner::costsize::gucs::seq_page_cost() * pages as f64;
            startup_cost += baserestrict_startup;
            let cpu_per_tuple =
                planner::costsize::gucs::cpu_tuple_cost() + baserestrict_per_tuple;
            rc += cpu_per_tuple * tuples;

            let rt_cost = run.pathtarget(run.rel_reltarget_id(rel_id)).cost;
            startup_cost += rt_cost.startup;
            rc += rt_cost.per_tuple * rows;
            run_cost = rc;
        }

        // Without remote estimates, we have no real way to estimate the cost
        // of generating sorted output; estimate a value high enough that we
        // won't pick the sorted path when the ordering isn't locally useful,
        // but low enough to err on the side of pushing down the ORDER BY
        // when it is (postgres_fdw.c:3452-3474).
        if !pathkeys.is_empty() {
            if is_upper_rel(run, rel_id) {
                debug_assert!(
                    run.root.rel(rel_id).reloptkind == RELOPT_UPPER_REL
                        && fpinfo(run.root.rel(rel_id)).borrow().stage == UPPERREL_GROUP_AGG
                );
                let limit_tuples =
                    fpextra.expect("a sorted grouping path carries fpextra").limit_tuples;
                adjust_foreign_grouping_path_cost(
                    run,
                    pathkeys,
                    retrieved_rows,
                    width,
                    limit_tuples,
                    &mut startup_cost,
                    &mut run_cost,
                );
            } else {
                startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
                run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
            }
        }

        total_cost = startup_cost + run_cost;

        // Adjust the cost estimates if we have LIMIT.
        if let Some(f) = fpextra {
            if f.has_limit {
                planner::pathnode::adjust_limit_rows_costs(
                    &mut rows,
                    &mut startup_cost,
                    &mut total_cost,
                    f.offset_est,
                    f.count_est,
                );
                retrieved_rows = rows;
            }
        }
    }

    // If this includes the final sort step, the given target, which will be
    // applied to the resulting path, might have different expressions from
    // the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
    // eval costs (postgres_fdw.c:3496-3505).
    if let Some(f) = fpextra {
        let reltarget = run.rel_reltarget_id(rel_id);
        if f.has_final_sort && f.target != reltarget {
            let oldcost = run.pathtarget(reltarget).cost;
            let newcost = run.pathtarget(f.target).cost;
            startup_cost += newcost.startup - oldcost.startup;
            total_cost += newcost.startup - oldcost.startup;
            total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
        }
    }

    // Cache the retrieved rows and cost estimates for scans, joins, or
    // groupings without any parameterization, pathkeys, or additional
    // post-scan/join-processing steps, before adding the costs for
    // transferring data from the foreign server (postgres_fdw.c:3518).
    if pathkeys.is_empty() && param_join_conds.is_empty() && fpextra.is_none() {
        let fpc = fpinfo(run.root.rel(rel_id));
        let mut fpm = fpc.borrow_mut();
        fpm.retrieved_rows = retrieved_rows;
        fpm.rel_startup_cost = startup_cost;
        fpm.rel_total_cost = total_cost;
    }

    let (fdw_startup_cost, fdw_tuple_cost) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.fdw_startup_cost, fp.fdw_tuple_cost)
    };
    startup_cost += fdw_startup_cost;
    total_cost += fdw_startup_cost;
    total_cost += fdw_tuple_cost * retrieved_rows;
    total_cost += planner::costsize::gucs::cpu_tuple_cost() * retrieved_rows;

    // If we have LIMIT, prefer performing the restriction remotely: the core
    // code doesn't account for the extra fetches a local LIMIT causes (see
    // create_limit_path()), so without remote estimates the two would cost
    // the same; tweak the remote restriction's cost so it wins when the
    // LIMIT is a useful one (postgres_fdw.c:3548-3564).
    if let Some(f) = fpextra {
        let fp_rows = fpinfo(run.root.rel(rel_id)).borrow().rows;
        if !use_remote_estimate && f.has_limit && f.limit_tuples > 0.0 && f.limit_tuples < fp_rows
        {
            debug_assert!(fp_rows > 0.0);
            total_cost -= (total_cost - startup_cost) * 0.05 * (fp_rows - f.limit_tuples) / fp_rows;
        }
    }

    Ok((rows, width, disabled_nodes, startup_cost, total_cost))
}

// adjust_foreign_grouping_path_cost (postgres_fdw.c:3659): the cost of
// generating properly-sorted output from a foreign grouping path. C never
// writes its p_disabled_nodes argument, so it is not threaded through.
fn adjust_foreign_grouping_path_cost(
    run: &PlannerRun<'_>,
    pathkeys: &[PathKey],
    retrieved_rows: f64,
    width: i32,
    limit_tuples: f64,
    p_startup_cost: &mut f64,
    p_run_cost: &mut f64,
) {
    // If the GROUP BY clause isn't sort-able, the plan chosen by the remote
    // side is unlikely to generate properly-sorted output, so it would need
    // an explicit sort; adjust the given costs with cost_sort(). Likewise if
    // it is sort-able but isn't a superset of the given pathkeys. Otherwise
    // apply the same heuristic as for the scan or join case.
    let grouping_is_sortable = run.root.processed_groupClause.iter().all(|&id| {
        run.root.expr_node(id).as_sort_group_clause().expect("group clause cell").sortop != 0
    });
    if !grouping_is_sortable
        || !types_pathnodes::pathkeys_contained_in(pathkeys, &run.root.group_pathkeys)
    {
        let (_disabled_nodes, startup_cost, total_cost) = planner::costsize::cost_sort_shape(
            0,
            *p_startup_cost + *p_run_cost,
            retrieved_rows,
            width,
            0.0,
            init_small::globals::work_mem(),
            limit_tuples,
        );
        *p_startup_cost = startup_cost;
        *p_run_cost = total_cost - startup_cost;
    } else {
        // The default extra cost seems too large for foreign-grouping cases;
        // add 1/4th of that default.
        let sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER - 1.0) * 0.25;
        *p_startup_cost *= sort_multiplier;
        *p_run_cost *= sort_multiplier;
    }
}

// find_em_for_rel (postgres_fdw.c:7851): an EquivalenceClass member whose
// expression uses only Vars from the given rel (none from the hidden
// subquery rels) and is shippable; child members are considered.
pub(crate) fn find_em_for_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: EcId,
    rel: RelId,
) -> PgResult<Option<EmId>> {
    let ems = planner::equivclass::ec_members_for_relids(run, ec, &run.root.rel(rel).relids);
    for em in ems.iter().copied() {
        let (usable, expr_id) = {
            let m = run.root.em(em);
            let rel_relids = &run.root.rel(rel).relids;
            let fp = fpinfo(run.root.rel(rel)).borrow();
            // Note we require !bms_is_empty, else we'd accept constant
            // expressions which are not suitable for the purpose.
            (
                relids_is_subset(&m.em_relids, rel_relids)
                    && !relids_is_empty(&m.em_relids)
                    && !relids_overlap(&m.em_relids, &fp.hidden_subquery_rels),
                m.em_expr,
            )
        };
        if usable && deparse::is_foreign_expr(run, rel, *run.root.expr_node(expr_id))? {
            return Ok(Some(em));
        }
    }
    Ok(None)
}

// find_em_for_rel_target (postgres_fdw.c:7886): an EquivalenceClass member
// that is computed as a sort column in the given rel's reltarget and is
// shippable. Caller separately verifies that the pathkey's ordering
// operator is shippable.
pub(crate) fn find_em_for_rel_target<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: EcId,
    rel: RelId,
) -> PgResult<Option<EmId>> {
    let target = run.rel_reltarget_id(rel);
    let sort_clause = &run.parse().sortClause;
    let nexprs = run.pathtarget(target).exprs.len();
    for i in 0..nexprs {
        let (expr_id, sgref) = {
            let t = run.pathtarget(target);
            (t.exprs[i], t.sortgrouprefs.get(i).copied().unwrap_or(0))
        };
        // Ignore non-sort expressions (get_sortgroupref_clause_noerr over
        // parse->sortClause).
        if sgref == 0
            || !sort_clause.iter().any(|c| {
                c.as_sort_group_clause().expect("sortClause holds SortGroupClause").tleSortGroupRef
                    == sgref
            })
        {
            continue;
        }
        // We ignore binary-compatible relabeling on both ends.
        let mut expr = *run.root.expr_node(expr_id);
        while let Some(rt) = expr.as_relabel_type() {
            expr = rt.arg;
        }
        // Locate an EquivalenceClass member matching this expr, if any.
        // Ignore child members (they never live in ec_members).
        let members: Vec<EmId> = run.root.ec(ec).ec_members.iter().copied().collect();
        for em in members {
            let (is_const, is_child, em_expr_id) = {
                let m = run.root.em(em);
                (m.em_is_const, m.em_is_child, m.em_expr)
            };
            // Don't match constants.
            if is_const {
                continue;
            }
            debug_assert!(!is_child);
            let full_expr = *run.root.expr_node(em_expr_id);
            let mut em_expr = full_expr;
            while let Some(rt) = em_expr.as_relabel_type() {
                em_expr = rt.arg;
            }
            // Match if same expression (after stripping relabel).
            if !types_nodes::equal::equal(em_expr, expr) {
                continue;
            }
            // Check that expression (including relabels!) is shippable.
            if deparse::is_foreign_expr(run, rel, full_expr)? {
                return Ok(Some(em));
            }
        }
    }
    Ok(None)
}

// get_useful_ecs_for_relation (postgres_fdw.c:814): the EquivalenceClasses
// that might be useful as sort orderings for a merge join against `rel`.
fn get_useful_ecs_for_relation<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
) -> PgResult<Vec<EcId>> {
    let mcx = run.mcx;
    let mut useful_eclass_list: Vec<EcId> = Vec::new();

    // First, consider whether any active EC is potentially useful for a
    // merge join against this relation.
    if run.root.rel(rel).has_eclass_joins {
        for i in 0..run.root.eq_classes.len() {
            let cur_ec = EcId(i as u32);
            // C's root->eq_classes holds canonical ECs only.
            if run.root.ec(cur_ec).ec_merged.is_some() {
                continue;
            }
            if planner::equivclass::eclass_useful_for_merging(run, cur_ec, rel) {
                useful_eclass_list.push(cur_ec);
            }
        }
    }

    // Next, consider whether there are any non-EC derivable join clauses
    // that are merge-joinable. If the joininfo list is empty, exit quickly.
    if run.root.rel(rel).joininfo.is_empty() {
        return Ok(useful_eclass_list);
    }

    // If this is a child rel, we must use the topmost parent rel to search.
    let relids = if is_other_rel(run, rel) {
        debug_assert!(!relids_is_empty(&run.root.rel(rel).top_parent_relids));
        planner::relnode::relids_copy(mcx, &run.root.rel(rel).top_parent_relids)
    } else {
        planner::relnode::relids_copy(mcx, &run.root.rel(rel).relids)
    };

    // Check each join clause in turn.
    let joininfo: Vec<RinfoId> = run.root.rel(rel).joininfo.iter().copied().collect();
    for ri in joininfo {
        // Consider only mergejoinable clauses.
        if run.root.rinfo(ri).mergeopfamilies.is_empty() {
            continue;
        }
        // Make sure we've got canonical ECs.
        planner::pathkeys::update_mergeclause_eclasses(run, ri)?;

        // mergeopfamilies != NIL guarantees left_ec and right_ec are set.
        // Identify which side of this merge-joinable clause contains columns
        // from the relation produced by this RelOptInfo; test for overlap,
        // not containment (either side may carry extra relations). It is
        // even possible that relids overlaps neither side (A LEFT JOIN B ON
        // A.x = B.x AND A.x = 1 puts A.x = 1 in B's joininfo); skip then.
        let (left_ec, right_ec) = {
            let r = run.root.rinfo(ri);
            (
                r.left_ec.expect("mergeclause left_ec set"),
                r.right_ec.expect("mergeclause right_ec set"),
            )
        };
        if relids_overlap(&relids, &run.root.ec(right_ec).ec_relids) {
            if !useful_eclass_list.contains(&right_ec) {
                useful_eclass_list.push(right_ec);
            }
        } else if relids_overlap(&relids, &run.root.ec(left_ec).ec_relids)
            && !useful_eclass_list.contains(&left_ec)
        {
            useful_eclass_list.push(left_ec);
        }
    }

    Ok(useful_eclass_list)
}

// get_useful_pathkeys_for_relation (postgres_fdw.c:910): which orderings of
// a relation might be useful — the query's own pathkeys (to avoid a local
// sort) and, with remote estimates, single-key orderings that could enable
// a merge join.
fn get_useful_pathkeys_for_relation<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
) -> PgResult<Vec<PgVec<'mcx, PathKey>>> {
    let mcx = run.mcx;
    let mut useful_pathkeys_list: Vec<PgVec<'mcx, PathKey>> = Vec::new();

    // Pushing the query_pathkeys to the remote server is always worth
    // considering, because it might let us avoid a local sort.
    fpinfo(run.root.rel(rel)).borrow_mut().qp_is_pushdown_safe = false;
    if !run.root.query_pathkeys.is_empty() {
        let query_pathkeys: Vec<PathKey> = run.root.query_pathkeys.iter().copied().collect();
        let mut query_pathkeys_ok = true;
        for pathkey in &query_pathkeys {
            // The planner and executor have no clever strategy for taking
            // data sorted by a prefix of the query's pathkeys and getting it
            // sorted by all of them: unless we can push down all of the
            // query pathkeys, forget it.
            if !deparse::is_foreign_pathkey(run, rel, pathkey)? {
                query_pathkeys_ok = false;
                break;
            }
        }
        if query_pathkeys_ok {
            let mut keys = PgVec::new_in(mcx);
            keys.extend(query_pathkeys);
            useful_pathkeys_list.push(keys);
            fpinfo(run.root.rel(rel)).borrow_mut().qp_is_pushdown_safe = true;
        }
    }

    // Even without remote estimates, having the remote side do the sort
    // generally won't be worse than doing it locally. What follows —
    // pathkeys that seem promising for possible merge joins — is more
    // speculative, so bail out if we can't use remote estimates.
    if !fpinfo(run.root.rel(rel)).borrow().use_remote_estimate {
        return Ok(useful_pathkeys_list);
    }

    // Get the list of interesting EquivalenceClasses.
    let useful_eclass_list = get_useful_ecs_for_relation(run, rel)?;

    // Extract unique EC for query, if any, so we don't consider it again.
    let query_ec = if run.root.query_pathkeys.len() == 1 {
        run.root.query_pathkeys[0].pk_eclass
    } else {
        None
    };

    // As a heuristic, the only pathkeys we consider here are those of length
    // one: each one generates a round-trip to the remote side.
    for cur_ec in useful_eclass_list {
        // If redundant with what we did above, skip it.
        if Some(cur_ec) == query_ec {
            continue;
        }
        // Can't push down the sort if the EC's opfamily is not shippable.
        let opfamily = run.root.ec(cur_ec).ec_opfamilies[0];
        let shippable = {
            let fp = fpinfo(run.root.rel(rel)).borrow();
            crate::shippable::is_shippable(
                mcx,
                opfamily,
                types_core::catalog::OPERATOR_FAMILY_RELATION_ID,
                fp.serverid(),
                &fp.shippable_extensions,
            )?
        };
        if !shippable {
            continue;
        }
        // If no pushable expression for this rel, skip it.
        if find_em_for_rel(run, cur_ec, rel)?.is_none() {
            continue;
        }
        // Looks like we can generate a pathkey, so let's do it.
        let pathkey = planner::pathkeys::make_canonical_pathkey(
            run,
            cur_ec,
            opfamily,
            COMPARE_LT,
            false,
        );
        let mut keys = PgVec::new_in(mcx);
        keys.push(pathkey);
        useful_pathkeys_list.push(keys);
    }

    Ok(useful_pathkeys_list)
}

// add_paths_with_pathkeys_for_rel (postgres_fdw.c:6120): one ForeignPath per
// useful set of pathkeys, sorted remotely. `epq_path` is the EPQ-capable
// local join path of a join rel (it must be at least as well sorted as the
// path itself, in case it gets used as input to a mergejoin).
fn add_paths_with_pathkeys_for_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    epq_path: Option<PathId>,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    let mcx = run.mcx;
    let useful_pathkeys_list = get_useful_pathkeys_for_relation(run, rel)?;

    // Before creating sorted paths, arrange for the passed-in EPQ path, if
    // any, to return columns needed by the parent ForeignScan node so that
    // they will propagate up through Sort nodes injected below, if necessary
    // (postgres_fdw.c:6133-6172).
    let mut epq_path = epq_path;
    if let Some(epq) = epq_path {
        if !useful_pathkeys_list.is_empty() {
            let epq_target = run
                .root
                .path(epq)
                .base()
                .pathtarget_id
                .expect("EPQ path has a pathtarget");
            // copy_pathtarget.
            let mut target = {
                let src = run.pathtarget(epq_target);
                let mut t = types_pathnodes::PathTarget::new(mcx);
                t.exprs.extend(src.exprs.iter().copied());
                t.sortgrouprefs.extend(src.sortgrouprefs.iter().copied());
                t.cost = src.cost;
                t.width = src.width;
                t.has_volatile_expr = src.has_volatile_expr;
                t
            };
            let old_len = target.exprs.len();
            // add_new_columns_to_pathtarget(pull_var_clause(...)): Vars
            // required for evaluating PHVs in the tlist, then for the local
            // conditions.
            let mut needed: NodeList<'mcx> = NodeList::nil();
            let expr_ids: Vec<types_pathnodes::NodeId> = target.exprs.iter().copied().collect();
            for id in expr_ids {
                let node = *run.root.expr_node(id);
                for v in &vars::pull_var_clause(mcx, node, vars::PVC_RECURSE_PLACEHOLDERS)? {
                    needed.lappend(mcx, v)?;
                }
            }
            let local: Vec<RinfoId> =
                fpinfo(run.root.rel(rel)).borrow().local_conds.iter().copied().collect();
            for ri in local {
                let clause = *run.root.expr_node(run.root.rinfo(ri).clause);
                for v in &vars::pull_var_clause(mcx, clause, vars::PVC_RECURSE_PLACEHOLDERS)? {
                    needed.lappend(mcx, v)?;
                }
            }
            for v in &needed {
                let present = target
                    .exprs
                    .iter()
                    .any(|&e| types_nodes::equal::equal(*run.root.expr_node(e), v));
                if !present {
                    // add_column_to_pathtarget(target, expr, 0).
                    target.exprs.push(run.root.alloc_expr_node(v));
                    if !target.sortgrouprefs.is_empty() {
                        target.sortgrouprefs.push(0);
                    }
                }
            }
            // If we have added any new columns, adjust the tlist of the EPQ
            // path with a ProjectionPath (no set_pathtarget_cost_width: the
            // plan only executes EPQ checks). The EPQ path is a join path,
            // so it is projection-capable; its parallel safety already
            // folds in rel->consider_parallel and its own target's exprs,
            // and the added Vars are parallel safe.
            if target.exprs.len() > old_len {
                debug_assert!(planner::pathnode::is_projection_capable_pathtype(
                    run.root.path(epq).base().pathtype
                ));
                let parallel_safe = run.root.path(epq).base().parallel_safe;
                let target_id = run.root.alloc_pathtarget(target);
                let projected = planner::pathnode::create_projection_path(
                    run,
                    rel,
                    epq,
                    target_id,
                    parallel_safe,
                );
                epq_path = Some(run.root.alloc_path(projected));
            }
        }
    }

    // Create one path for each set of pathkeys we found above.
    for useful_pathkeys in useful_pathkeys_list {
        let (rows, _width, disabled_nodes, startup_cost, total_cost) =
            estimate_path_cost_size(run, rel, &[], &useful_pathkeys, None)?;

        // The EPQ path must be at least as well sorted as the path itself,
        // in case it gets used as input to a mergejoin.
        let mut sorted_epq_path = epq_path;
        if let Some(epq) = sorted_epq_path {
            if !types_pathnodes::pathkeys_contained_in(
                &useful_pathkeys,
                &run.root.path(epq).base().pathkeys,
            ) {
                let mut keys = PgVec::new_in(mcx);
                keys.extend(useful_pathkeys.iter().copied());
                sorted_epq_path =
                    Some(planner::pathnode::create_sort_path(run, rel, epq, keys, -1.0));
            }
        }

        let lateral_relids =
            planner::relnode::relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        let path = if is_simple_rel(run, rel) {
            planner::pathnode::create_foreignscan_path(
                run,
                rel,
                None,
                rows,
                disabled_nodes,
                startup_cost,
                total_cost,
                useful_pathkeys,
                &lateral_relids,
                sorted_epq_path,
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
            )?
        } else {
            let mut fdw_restrictinfo = PgVec::new_in(mcx);
            fdw_restrictinfo.extend(restrictlist.iter().copied());
            planner::pathnode::create_foreign_join_path(
                run,
                rel,
                None,
                rows,
                disabled_nodes,
                startup_cost,
                total_cost,
                useful_pathkeys,
                &lateral_relids,
                sorted_epq_path,
                fdw_restrictinfo,
                PgVec::new_in(mcx),
            )?
        };
        planner::pathnode::add_path(run, rel, path);
    }
    Ok(())
}

// get_remote_estimate: run EXPLAIN remotely and scrape the top plan line's
// "(cost=%lf..%lf rows=%lf width=%d)".
fn get_remote_estimate(conn_key: Oid, sql: &str) -> PgResult<(f64, i32, f64, f64)> {
    let res = crate::connection::exec_query(conn_key, sql)?;
    if res.status != pgclient::ExecStatus::TuplesOk {
        return Err(crate::connection::remote_error(&res, Some(sql)));
    }
    let line: &str = res
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.as_deref())
        .and_then(|b| core::str::from_utf8(b).ok())
        .unwrap_or("");
    let parsed = line.rfind('(').and_then(|p| parse_explain_costs(&line[p..]));
    match parsed {
        Some((startup, total, rows, width)) => Ok((rows, width, startup, total)),
        None => Err(Box::new(PgError::error(format!(
            "could not interpret EXPLAIN output: \"{line}\""
        )))),
    }
}

fn parse_explain_costs(tail: &str) -> Option<(f64, f64, f64, i32)> {
    let s = tail.strip_prefix("(cost=")?;
    let i = s.find("..")?;
    let startup: f64 = s[..i].parse().ok()?;
    let s = &s[i + 2..];
    let i = s.find(" rows=")?;
    let total: f64 = s[..i].parse().ok()?;
    let s = &s[i + 6..];
    let i = s.find(" width=")?;
    let rows: f64 = s[..i].parse().ok()?;
    let s = &s[i + 7..];
    let i = s.find(')')?;
    let width: i32 = s[..i].parse().ok()?;
    Some((startup, total, rows, width))
}

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied verbatim from a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// ---------- GetForeignPaths ----------

fn postgres_get_foreign_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    _foreigntableid: Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let (rows, disabled_nodes, startup_cost, total_cost) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.rows, fp.disabled_nodes, fp.startup_cost, fp.total_cost)
    };
    let required_outer =
        planner::relnode::relids_copy(mcx, &run.root.rel(rel_id).lateral_relids);
    let path = planner::pathnode::create_foreignscan_path(
        run,
        rel_id,
        None,
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        PgVec::new_in(mcx),
        &required_outer,
        None,
        PgVec::new_in(mcx),
        PgVec::new_in(mcx),
    )?;
    planner::pathnode::add_path(run, rel_id, path);

    // Add paths with pathkeys (postgres_fdw.c:1050).
    add_paths_with_pathkeys_for_rel(run, rel_id, None, &[])?;

    // Without remote estimates there is no way to cost join clauses; stop.
    if !fpinfo(run.root.rel(rel_id)).borrow().use_remote_estimate {
        return Ok(());
    }

    use types_pathnodes::relids::{
        relids_del_member, relids_equal, relids_is_empty, relids_union,
    };

    // Collect candidate outer parameterizations as ParamPathInfos, first from
    // generic join clauses, then from EquivalenceClasses.
    let mut ppi_outers: Vec<types_pathnodes::Relids<'mcx>> = Vec::new();
    let joininfo: Vec<RinfoId> = run.root.rel(rel_id).joininfo.iter().copied().collect();
    for ri in joininfo {
        if !planner::indxpath::join_clause_is_movable_to(run, ri, rel_id) {
            continue;
        }
        let clause_node = *run.root.expr_node(run.root.rinfo(ri).clause);
        if !deparse::is_foreign_expr(run, rel_id, clause_node)? {
            continue;
        }
        let required_outer = {
            let rinfo = run.root.rinfo(ri);
            let rel = run.root.rel(rel_id);
            let u = relids_union(mcx, &rinfo.clause_relids, &rel.lateral_relids);
            relids_del_member(mcx, &u, rel.relid as i32)
        };
        if relids_is_empty(&required_outer) {
            continue;
        }
        let ppi = planner::pathnode::get_baserel_parampathinfo(run, rel_id, &required_outer)?
            .expect("non-empty required_outer yields a PPI");
        if !ppi_outers.iter().any(|o| relids_equal(o, &ppi.ppi_req_outer)) {
            ppi_outers.push(ppi.ppi_req_outer.clone());
        }
    }

    if run.root.rel(rel_id).has_eclass_joins {
        // Repeatedly scan the eclass list for members of this rel, generating
        // implied equalities per member (ec_member_matches_foreign).
        let mut already_used: Vec<types_pathnodes::NodeId> = Vec::new();
        loop {
            let mut current: Option<types_pathnodes::NodeId> = None;
            let prohibited =
                planner::relnode::relids_copy(mcx, &run.root.rel(rel_id).lateral_referencers);
            let clauses = planner::equivclass::generate_implied_equalities_for_column(
                run,
                rel_id,
                |run, _rel, _ec, em| {
                    let expr = run.root.em(em).em_expr;
                    if let Some(cur) = current {
                        return Ok(types_nodes::equal::equal(
                            *run.root.expr_node(expr),
                            *run.root.expr_node(cur),
                        ));
                    }
                    if already_used.iter().any(|&u| {
                        types_nodes::equal::equal(
                            *run.root.expr_node(expr),
                            *run.root.expr_node(u),
                        )
                    }) {
                        return Ok(false);
                    }
                    current = Some(expr);
                    Ok(true)
                },
                &prohibited,
            )?;
            let Some(cur) = current else {
                break;
            };
            for ri in clauses.iter().copied() {
                if !planner::indxpath::join_clause_is_movable_to(run, ri, rel_id) {
                    continue;
                }
                let clause_node = *run.root.expr_node(run.root.rinfo(ri).clause);
                if !deparse::is_foreign_expr(run, rel_id, clause_node)? {
                    continue;
                }
                let required_outer = {
                    let rinfo = run.root.rinfo(ri);
                    let rel = run.root.rel(rel_id);
                    let u = relids_union(mcx, &rinfo.clause_relids, &rel.lateral_relids);
                    relids_del_member(mcx, &u, rel.relid as i32)
                };
                if relids_is_empty(&required_outer) {
                    continue;
                }
                let ppi =
                    planner::pathnode::get_baserel_parampathinfo(run, rel_id, &required_outer)?
                        .expect("non-empty required_outer yields a PPI");
                if !ppi_outers.iter().any(|o| relids_equal(o, &ppi.ppi_req_outer)) {
                    ppi_outers.push(ppi.ppi_req_outer.clone());
                }
            }
            already_used.push(cur);
        }
    }

    // Build a parameterized path per useful outer relation, costed remotely.
    for req_outer in ppi_outers {
        let clauses: Vec<RinfoId> = {
            let rel = run.root.rel(rel_id);
            let i = rel
                .ppilist
                .iter()
                .position(|p| relids_equal(&p.ppi_req_outer, &req_outer))
                .expect("PPI cached above");
            rel.ppilist[i].ppi_clauses.iter().copied().collect()
        };
        let (rows, _width, disabled_nodes, startup_cost, total_cost) =
            estimate_path_cost_size(run, rel_id, &clauses, &[], None)?;
        {
            let rel = run.root.rel_mut(rel_id);
            if let Some(p) = rel
                .ppilist
                .iter_mut()
                .find(|p| relids_equal(&p.ppi_req_outer, &req_outer))
            {
                p.ppi_rows = rows;
            }
        }
        let path = planner::pathnode::create_foreignscan_path(
            run,
            rel_id,
            None,
            rows,
            disabled_nodes,
            startup_cost,
            total_cost,
            PgVec::new_in(mcx),
            &req_outer,
            None,
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        )?;
        planner::pathnode::add_path(run, rel_id, path);
    }
    Ok(())
}

// ---------- GetForeignJoinPaths ----------

fn is_outer_join(jointype: types_pathnodes::JoinType) -> bool {
    matches!(
        jointype,
        types_pathnodes::JOIN_LEFT
            | types_pathnodes::JOIN_FULL
            | types_pathnodes::JOIN_RIGHT
            | types_pathnodes::JOIN_ANTI
            | types_pathnodes::JOIN_RIGHT_ANTI
    )
}

// semijoin_target_ok: the reltarget must not reference inner-rel Vars (the
// inner side deparses to EXISTS and can't be projected).
fn semijoin_target_ok<'mcx>(
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    innerrel: RelId,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let expr_ids: Vec<types_pathnodes::NodeId> = {
        let rel = run.root.rel(joinrel);
        run.pathtarget(rel.pathtarget_id.expect("rel has reltarget")).exprs.iter().copied().collect()
    };
    for id in expr_ids {
        let node = *run.root.expr_node(id);
        let vs = vars::pull_var_clause(mcx, node, vars::PVC_INCLUDE_PLACEHOLDERS)?;
        for v in &vs {
            if let Some(var) = v.as_var() {
                if types_pathnodes::relids::relids_is_member(
                    var.varno,
                    &run.root.rel(innerrel).relids,
                ) {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

// merge_fdw_options (join arm; fpinfo_i always present here).
fn merge_fdw_options<'mcx>(
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
) {
    let fpc = fpinfo(run.root.rel(joinrel));
    let mut fp = fpc.borrow_mut();
    let fo = fpinfo(run.root.rel(outerrel)).borrow();
    let fi = fpinfo(run.root.rel(innerrel)).borrow();
    fp.serverid = fo.serverid;
    fp.fdw_startup_cost = fo.fdw_startup_cost;
    fp.fdw_tuple_cost = fo.fdw_tuple_cost;
    fp.shippable_extensions.clear();
    fp.shippable_extensions.extend(fo.shippable_extensions.iter().copied());
    fp.use_remote_estimate = fo.use_remote_estimate || fi.use_remote_estimate;
    fp.fetch_size = fo.fetch_size.max(fi.fetch_size);
    fp.async_capable = fo.async_capable || fi.async_capable;
}

#[allow(clippy::too_many_arguments)]
fn foreign_join_ok<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    jointype: types_pathnodes::JoinType,
    outerrel: RelId,
    innerrel: RelId,
    restrictlist: &[RinfoId],
) -> PgResult<bool> {
    use types_pathnodes::relids::{
        relids_copy, relids_is_empty, relids_is_subset, relids_union,
    };
    use types_pathnodes::{JOIN_FULL, JOIN_LEFT, JOIN_RIGHT, JOIN_SEMI};
    let mcx = run.mcx;

    if !matches!(
        jointype,
        types_pathnodes::JOIN_INNER | JOIN_LEFT | JOIN_RIGHT | JOIN_FULL | JOIN_SEMI
    ) {
        return Ok(false);
    }
    if jointype == JOIN_SEMI && !semijoin_target_ok(run, joinrel, innerrel)? {
        return Ok(false);
    }

    {
        let Some(fo) = fpinfo_opt(run.root.rel(outerrel)) else { return Ok(false) };
        let Some(fi) = fpinfo_opt(run.root.rel(innerrel)) else { return Ok(false) };
        if !fo.borrow().pushdown_safe || !fi.borrow().pushdown_safe {
            return Ok(false);
        }
        if !fo.borrow().local_conds.is_empty() || !fi.borrow().local_conds.is_empty() {
            return Ok(false);
        }
    }

    // Merge FDW options before shippability checks (shippable_extensions).
    merge_fdw_options(run, joinrel, outerrel, innerrel);

    // Split restrictlist into join quals and pushed-down (other) quals.
    let joinrelids = relids_copy(mcx, &run.root.rel(joinrel).relids);
    let mut joinclauses: Vec<RinfoId> = Vec::new();
    let mut remote_conds: Vec<RinfoId> = Vec::new();
    let mut local_conds: Vec<RinfoId> = Vec::new();
    for &ri in restrictlist {
        let clause_node = *run.root.expr_node(run.root.rinfo(ri).clause);
        let is_remote = deparse::is_foreign_expr(run, joinrel, clause_node)?;
        if is_outer_join(jointype)
            && !planner::joinrels::rinfo_is_pushed_down(run, ri, &joinrelids)
        {
            if !is_remote {
                return Ok(false);
            }
            joinclauses.push(ri);
        } else if is_remote {
            remote_conds.push(ri);
        } else {
            local_conds.push(ri);
        }
    }

    // A PlaceHolderVar that must be evaluated inside this join tree defeats
    // deparseExplicitTargetList; one evaluated at the top is fine.
    {
        let relids = if run.root.rel(joinrel).reloptkind
            == types_pathnodes::RELOPT_OTHER_JOINREL
        {
            relids_copy(mcx, &run.root.rel(joinrel).top_parent_relids)
        } else {
            relids_copy(mcx, &run.root.rel(joinrel).relids)
        };
        let ph_ids: Vec<_> = run.root.placeholder_list.iter().copied().collect();
        for ph in ph_ids {
            let phinfo = run.root.phinfo(ph);
            if relids_is_subset(&phinfo.ph_eval_at, &relids)
                && !relids_is_subset(&relids, &phinfo.ph_eval_at)
            {
                return Ok(false);
            }
        }
    }

    let (o_remote, o_hidden, o_lower, o_name): (Vec<RinfoId>, _, _, &str) = {
        let fo = fpinfo(run.root.rel(outerrel)).borrow();
        (
            fo.remote_conds.iter().copied().collect(),
            relids_copy(mcx, &fo.hidden_subquery_rels),
            relids_copy(mcx, &fo.lower_subquery_rels),
            fo.relation_name,
        )
    };
    let (i_remote, i_hidden, i_lower, i_name): (Vec<RinfoId>, _, _, &str) = {
        let fi = fpinfo(run.root.rel(innerrel)).borrow();
        (
            fi.remote_conds.iter().copied().collect(),
            relids_copy(mcx, &fi.hidden_subquery_rels),
            relids_copy(mcx, &fi.lower_subquery_rels),
            fi.relation_name,
        )
    };

    let mut make_outerrel_subquery = false;
    let mut make_innerrel_subquery = false;
    let mut lower_subquery_rels = relids_union(mcx, &o_lower, &i_lower);
    let mut hidden_subquery_rels = relids_union(mcx, &o_hidden, &i_hidden);

    // Pull up child remote conds into join clauses / other clauses.
    match jointype {
        types_pathnodes::JOIN_INNER => {
            remote_conds.extend(i_remote.iter().copied());
            remote_conds.extend(o_remote.iter().copied());
        }
        JOIN_LEFT => {
            if relids_is_empty(&i_hidden) {
                joinclauses.extend(i_remote.iter().copied());
            }
            if relids_is_empty(&o_hidden) {
                remote_conds.extend(o_remote.iter().copied());
            }
        }
        JOIN_RIGHT => {
            if relids_is_empty(&o_hidden) {
                joinclauses.extend(o_remote.iter().copied());
            }
            if relids_is_empty(&i_hidden) {
                remote_conds.extend(i_remote.iter().copied());
            }
        }
        JOIN_SEMI => {
            joinclauses.extend(i_remote.iter().copied());
            joinclauses.extend(remote_conds.iter().copied());
            remote_conds = o_remote.clone();
            hidden_subquery_rels = relids_union(
                mcx,
                &hidden_subquery_rels,
                &run.root.rel(innerrel).relids,
            );
        }
        JOIN_FULL => {
            if !o_remote.is_empty() {
                make_outerrel_subquery = true;
                lower_subquery_rels = relids_union(
                    mcx,
                    &lower_subquery_rels,
                    &run.root.rel(outerrel).relids,
                );
            }
            if !i_remote.is_empty() {
                make_innerrel_subquery = true;
                lower_subquery_rels = relids_union(
                    mcx,
                    &lower_subquery_rels,
                    &run.root.rel(innerrel).relids,
                );
            }
        }
        _ => unreachable!(),
    }

    if jointype == types_pathnodes::JOIN_INNER {
        debug_assert!(joinclauses.is_empty());
        joinclauses = core::mem::take(&mut remote_conds);
    } else if matches!(jointype, JOIN_LEFT | JOIN_RIGHT | JOIN_FULL) {
        if !relids_is_empty(&o_hidden) {
            make_outerrel_subquery = true;
            lower_subquery_rels =
                relids_union(mcx, &lower_subquery_rels, &run.root.rel(outerrel).relids);
        }
        if !relids_is_empty(&i_hidden) {
            make_innerrel_subquery = true;
            lower_subquery_rels =
                relids_union(mcx, &lower_subquery_rels, &run.root.rel(innerrel).relids);
        }
    }

    let relation_name = mcx_str(
        mcx,
        &format!("({o_name}) {} JOIN ({i_name})", deparse::jointype_name(jointype)?),
    )?;
    let relation_index =
        run.parse().rtable.len() as i32 + run.root.join_rel_list.len() as i32;

    {
        let fpc = fpinfo(run.root.rel(joinrel));
        let mut fp = fpc.borrow_mut();
        fp.joinclauses = PgVec::new_in(mcx);
        fp.joinclauses.extend(joinclauses.iter().copied());
        fp.remote_conds = PgVec::new_in(mcx);
        fp.remote_conds.extend(remote_conds.iter().copied());
        fp.local_conds = PgVec::new_in(mcx);
        fp.local_conds.extend(local_conds.iter().copied());
        fp.outerrel = Some(outerrel);
        fp.innerrel = Some(innerrel);
        fp.jointype = jointype;
        fp.make_outerrel_subquery = make_outerrel_subquery;
        fp.make_innerrel_subquery = make_innerrel_subquery;
        fp.lower_subquery_rels = lower_subquery_rels;
        fp.hidden_subquery_rels = hidden_subquery_rels;
        fp.pushdown_safe = true;
        fp.retrieved_rows = -1.0;
        fp.rel_startup_cost = -1.0;
        fp.rel_total_cost = -1.0;
        fp.relation_name = relation_name;
        debug_assert_eq!(fp.relation_index, 0);
        fp.relation_index = relation_index;
    }
    Ok(true)
}

// postgresGetForeignJoinPaths (postgres_fdw.c:6362).
fn postgres_get_foreign_join_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: types_pathnodes::JoinType,
    sjinfo: &types_pathnodes::SpecialJoinInfo<'mcx>,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    let mcx = run.mcx;
    if run.root.rel(joinrel).fdw_state.is_some() {
        return Ok(());
    }
    if !types_pathnodes::relids::relids_is_empty(&run.root.rel(joinrel).lateral_relids) {
        return Ok(());
    }
    let mut fp = PgFdwRelationInfo::new(mcx);
    fp.pushdown_safe = false;
    attach_fpinfo(mcx, run.root.rel_mut(joinrel), fp)?;

    // If there is a possibility that EvalPlanQual will be executed, we need
    // to be able to reconstruct the row using scans of the base relations.
    // GetExistingLocalJoinPath will find a suitable path for this purpose in
    // the path list of the joinrel, if one exists. We must be careful to
    // call it before adding any ForeignPath, since the ForeignPath might
    // dominate the only suitable local path available. We also do it before
    // calling foreign_join_ok(), since that function updates fpinfo and
    // marks it as pushable if the join is found to be pushable.
    let epq_path: Option<PathId> = {
        use types_nodes::CmdType;
        let ct = run.parse().commandType;
        if ct == CmdType::CMD_DELETE || ct == CmdType::CMD_UPDATE || !run.root.rowMarks.is_empty()
        {
            let Some(p) = planner::pathnode::get_existing_local_join_path(run, joinrel) else {
                // elog(DEBUG3, "could not push down foreign join because a
                // local path suitable for EPQ checks was not found")
                return Ok(());
            };
            Some(p)
        } else {
            None
        }
    };

    if !foreign_join_ok(run, joinrel, jointype, outerrel, innerrel, restrictlist)? {
        return Ok(());
    }

    let local_ids: Vec<RinfoId> =
        fpinfo(run.root.rel(joinrel)).borrow().local_conds.iter().copied().collect();
    let local_conds_sel = planner::clausesel::clauselist_selectivity(
        run,
        &local_ids,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut local_cost = types_pathnodes::QualCost::default();
    for &ri in &local_ids {
        let clause = run.root.rinfo(ri).clause;
        let node = *run.root.expr_node(clause);
        let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
        local_cost.startup += c.startup;
        local_cost.per_tuple += c.per_tuple;
    }
    let use_remote_estimate = fpinfo(run.root.rel(joinrel)).borrow().use_remote_estimate;
    let joinclause_sel = if !use_remote_estimate {
        let join_ids: Vec<RinfoId> =
            fpinfo(run.root.rel(joinrel)).borrow().joinclauses.iter().copied().collect();
        let fp_jointype = fpinfo(run.root.rel(joinrel)).borrow().jointype;
        planner::clausesel::clauselist_selectivity(run, &join_ids, 0, fp_jointype, Some(sjinfo))?
    } else {
        0.0
    };
    {
        let fpc = fpinfo(run.root.rel(joinrel));
        let mut fpm = fpc.borrow_mut();
        fpm.local_conds_sel = local_conds_sel;
        fpm.local_conds_cost = local_cost;
        if !use_remote_estimate {
            fpm.joinclause_sel = joinclause_sel;
        }
    }

    let (rows, width, disabled_nodes, startup_cost, total_cost) =
        estimate_path_cost_size(run, joinrel, &[], &[], None)?;
    run.root.rel_mut(joinrel).rows = rows;
    let pt = run.rel_reltarget_id(joinrel);
    run.root.pathtarget_mut(pt).width = width;
    {
        let fpc = fpinfo(run.root.rel(joinrel));
        let mut fpm = fpc.borrow_mut();
        fpm.rows = rows;
        fpm.width = width;
        fpm.disabled_nodes = disabled_nodes;
        fpm.startup_cost = startup_cost;
        fpm.total_cost = total_cost;
    }

    let required_outer = types_pathnodes::relids::relids_empty();
    let mut fdw_restrictinfo = PgVec::new_in(mcx);
    fdw_restrictinfo.extend(restrictlist.iter().copied());
    let path = planner::pathnode::create_foreign_join_path(
        run,
        joinrel,
        None,
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        PgVec::new_in(mcx),
        &required_outer,
        epq_path,
        fdw_restrictinfo,
        PgVec::new_in(mcx),
    )?;
    planner::pathnode::add_path(run, joinrel, path);

    // Consider pathkeys for the join relation (postgres_fdw.c:6496).
    add_paths_with_pathkeys_for_rel(run, joinrel, epq_path, restrictlist)?;
    Ok(())
}

// ---------- GetForeignUpperPaths (GROUP_AGG / ORDERED / FINAL) ----------

// postgresGetForeignUpperPaths (postgres_fdw.c:6748).
fn postgres_get_foreign_upper_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    stage: types_pathnodes::UpperRelationKind,
    input_rel: RelId,
    output_rel: RelId,
    extra: &UpperPathExtra<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    // If input rel is not safe to pushdown, then simply return as we cannot
    // perform any post-join operations on the foreign server.
    {
        let Some(ifp) = fpinfo_opt(run.root.rel(input_rel)) else { return Ok(()) };
        if !ifp.borrow().pushdown_safe {
            return Ok(());
        }
    }
    // Ignore stages we don't support; and skip any duplicate calls.
    if (stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED && stage != UPPERREL_FINAL)
        || run.root.rel(output_rel).fdw_state.is_some()
    {
        return Ok(());
    }
    let mut fp = PgFdwRelationInfo::new(mcx);
    fp.pushdown_safe = false;
    fp.stage = stage;
    attach_fpinfo(mcx, run.root.rel_mut(output_rel), fp)?;

    match stage {
        UPPERREL_GROUP_AGG => {
            let having_qual = match extra {
                UpperPathExtra::GroupAgg { having_qual } => *having_qual,
                _ => None,
            };
            add_foreign_grouping_paths(run, input_rel, output_rel, having_qual)
        }
        UPPERREL_ORDERED => add_foreign_ordered_paths(run, input_rel, output_rel),
        UPPERREL_FINAL => {
            let (limit_needed, limit_tuples, count_est, offset_est) = match extra {
                UpperPathExtra::Final { limit_needed, limit_tuples, count_est, offset_est } => {
                    (*limit_needed, *limit_tuples, *count_est, *offset_est)
                }
                _ => (false, -1.0, 0, 0),
            };
            add_foreign_final_paths(
                run,
                input_rel,
                output_rel,
                limit_needed,
                limit_tuples,
                count_est,
                offset_est,
            )
        }
        _ => Err(Box::new(PgError::error(format!("unexpected upper relation: {stage}")))),
    }
}

// merge_fdw_options with fpinfo_i = NULL (postgres_fdw.c:6688): the upper
// rel takes the input rel's server and FDW options.
fn copy_fdw_options(run: &PlannerRun<'_>, dst: RelId, src: RelId) {
    let fpc = fpinfo(run.root.rel(dst));
    let mut fp = fpc.borrow_mut();
    let fo = fpinfo(run.root.rel(src)).borrow();
    fp.serverid = fo.serverid;
    fp.fdw_startup_cost = fo.fdw_startup_cost;
    fp.fdw_tuple_cost = fo.fdw_tuple_cost;
    fp.shippable_extensions.clear();
    fp.shippable_extensions.extend(fo.shippable_extensions.iter().copied());
    fp.use_remote_estimate = fo.use_remote_estimate;
    fp.fetch_size = fo.fetch_size;
    fp.async_capable = fo.async_capable;
}

// FdwPathPrivateIndex (postgres_fdw.c:277-283): [has_final_sort, has_limit]
// as Boolean nodes in the ForeignPath's fdw_private.
fn make_path_private<'mcx>(
    run: &mut PlannerRun<'mcx>,
    has_final_sort: bool,
    has_limit: bool,
) -> PgResult<PgVec<'mcx, types_pathnodes::NodeId>> {
    let mcx = run.mcx;
    let mut fdw_private = PgVec::new_in(mcx);
    fdw_private.push(run.root.alloc_expr_node(Node::mk_boolean(mcx, has_final_sort)?));
    fdw_private.push(run.root.alloc_expr_node(Node::mk_boolean(mcx, has_limit)?));
    Ok(fdw_private)
}

// add_foreign_ordered_paths (postgres_fdw.c:6897): a foreign path that
// performs the final sort remotely, added to the ordered_rel.
fn add_foreign_ordered_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    ordered_rel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    // Shouldn't get here unless the query has ORDER BY.
    debug_assert!(!parse.sortClause.is_nil());

    // We don't support cases where there are any SRFs in the targetlist.
    if parse.hasTargetSRFs {
        return Ok(());
    }

    // Save the input_rel as outerrel in fpinfo; copy the foreign server, FDW
    // options etc. from the input relation's fpinfo.
    fpinfo(run.root.rel(ordered_rel)).borrow_mut().outerrel = Some(input_rel);
    copy_fdw_options(run, ordered_rel, input_rel);

    // If the input_rel is a base or join relation, we would already have
    // considered pushing down the final sort to the remote server when
    // creating pre-sorted foreign paths for that relation, because the
    // query_pathkeys is set to the root->sort_pathkeys in that case (see
    // standard_qp_callback()).
    let input_kind = run.root.rel(input_rel).reloptkind;
    if input_kind == RELOPT_BASEREL || input_kind == RELOPT_JOINREL {
        debug_assert!(types_pathnodes::pathkeys_contained_in(
            &run.root.query_pathkeys,
            &run.root.sort_pathkeys
        ) && run.root.query_pathkeys.len() == run.root.sort_pathkeys.len());
        // Safe to push down if the query_pathkeys is safe to push down.
        let qp_safe = fpinfo(run.root.rel(input_rel)).borrow().qp_is_pushdown_safe;
        fpinfo(run.root.rel(ordered_rel)).borrow_mut().pushdown_safe = qp_safe;
        return Ok(());
    }

    // The input_rel should be a grouping relation.
    debug_assert!(
        input_kind == RELOPT_UPPER_REL
            && fpinfo(run.root.rel(input_rel)).borrow().stage == UPPERREL_GROUP_AGG
    );

    // We try to create a path below by extending a simple foreign path for
    // the underlying grouping relation to perform the final sort remotely,
    // which is stored into the fdw_private list of the resulting path.

    // Assess if it is safe to push down the final sort.
    let sort_pathkeys: Vec<PathKey> = run.root.sort_pathkeys.iter().copied().collect();
    for pathkey in &sort_pathkeys {
        let pathkey_ec = pathkey.pk_eclass.expect("canonical pathkey has an eclass");
        // is_foreign_expr would detect volatile expressions as well, but
        // checking ec_has_volatile here saves some cycles.
        if run.root.ec(pathkey_ec).ec_has_volatile {
            return Ok(());
        }
        // Can't push down the sort if pathkey's opfamily is not shippable.
        let shippable = {
            let fp = fpinfo(run.root.rel(ordered_rel)).borrow();
            crate::shippable::is_shippable(
                mcx,
                pathkey.pk_opfamily,
                types_core::catalog::OPERATOR_FAMILY_RELATION_ID,
                fp.serverid(),
                &fp.shippable_extensions,
            )?
        };
        if !shippable {
            return Ok(());
        }
        // The EC must contain a shippable EM that is computed in input_rel's
        // reltarget, else we can't push down the sort.
        if find_em_for_rel_target(run, pathkey_ec, input_rel)?.is_none() {
            return Ok(());
        }
    }

    // Safe to push down.
    fpinfo(run.root.rel(ordered_rel)).borrow_mut().pushdown_safe = true;

    // Construct PgFdwPathExtraData.
    let target = run.root.upper_targets[UPPERREL_ORDERED as usize]
        .expect("upper_targets[UPPERREL_ORDERED] is set before create_ordered_paths");
    let fpextra = PgFdwPathExtraData {
        target,
        has_final_sort: true,
        has_limit: false,
        limit_tuples: 0.0,
        count_est: 0,
        offset_est: 0,
    };

    // Estimate the costs of performing the final sort remotely.
    let (rows, _width, disabled_nodes, startup_cost, total_cost) =
        estimate_path_cost_size(run, input_rel, &[], &sort_pathkeys, Some(&fpextra))?;

    // Build the fdw_private list that will be used by postgresGetForeignPlan.
    let fdw_private = make_path_private(run, true, false)?;

    // Create foreign ordering path (parent = input_rel, as C).
    let mut pathkeys = PgVec::new_in(mcx);
    pathkeys.extend(sort_pathkeys);
    let ordered_path = planner::pathnode::create_foreign_upper_path(
        run,
        input_rel,
        Some(target),
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        pathkeys,
        None,
        PgVec::new_in(mcx),
        fdw_private,
    )?;

    // and add it to the ordered_rel.
    planner::pathnode::add_path(run, ordered_rel, ordered_path);
    Ok(())
}

// add_foreign_final_paths (postgres_fdw.c:7035): foreign paths performing
// the final processing (FOR UPDATE/SHARE, LIMIT/OFFSET, possibly the final
// sort) remotely, added to the final_rel. The FinalPathExtraData fields
// arrive unpacked.
#[allow(clippy::too_many_arguments)]
fn add_foreign_final_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    final_rel: RelId,
    limit_needed: bool,
    limit_tuples: f64,
    count_est: i64,
    offset_est: i64,
) -> PgResult<()> {
    use types_nodes::nodes_enums::LimitOption;
    use types_nodes::CmdType;
    let mcx = run.mcx;
    let parse = run.parse();

    // Currently, we only support this for SELECT commands.
    if parse.commandType != CmdType::CMD_SELECT {
        return Ok(());
    }

    // No work if there is no FOR UPDATE/SHARE clause and if there is no need
    // to add a LIMIT node.
    if parse.rowMarks.is_nil() && !limit_needed {
        return Ok(());
    }

    // We don't support cases where there are any SRFs in the targetlist.
    if parse.hasTargetSRFs {
        return Ok(());
    }

    // Save the input_rel as outerrel in fpinfo; copy the foreign server, FDW
    // options etc. from the input relation's fpinfo.
    fpinfo(run.root.rel(final_rel)).borrow_mut().outerrel = Some(input_rel);
    copy_fdw_options(run, final_rel, input_rel);

    // If there is no need to add a LIMIT node, there might be a ForeignPath
    // in the input_rel's pathlist that implements all behavior of the query.
    // Note: we would already have accounted for the query's FOR UPDATE/SHARE
    // (if any) before we get here.
    if !limit_needed {
        debug_assert!(!parse.rowMarks.is_nil());

        // Grouping and aggregation are not supported with FOR UPDATE/SHARE,
        // so the input_rel should be a base, join, or ordered relation; and
        // if it's an ordered relation, its input relation should be a base
        // or join relation.
        debug_assert!({
            let kind = run.root.rel(input_rel).reloptkind;
            kind == RELOPT_BASEREL
                || kind == RELOPT_JOINREL
                || (kind == RELOPT_UPPER_REL && {
                    let ifp = fpinfo(run.root.rel(input_rel)).borrow();
                    ifp.stage == UPPERREL_ORDERED
                        && matches!(
                            run.root.rel(ifp.outerrel.expect("ordered rel has outerrel")).reloptkind,
                            RELOPT_BASEREL | RELOPT_JOINREL
                        )
                })
        });

        let paths: Vec<PathId> = run.root.rel(input_rel).pathlist.iter().copied().collect();
        for path in paths {
            // apply_scanjoin_target_to_paths() uses create_projection_path()
            // to adjust each of its input paths if needed, whereas
            // create_ordered_paths() uses apply_projection_to_path() to do
            // that. So the former might have put a ProjectionPath on top of
            // the ForeignPath; look through ProjectionPath and see if the
            // path underneath it is ForeignPath.
            let is_foreign = match run.root.path(path) {
                PathNode::ForeignPath(_) => true,
                PathNode::ProjectionPath(pp) => pp
                    .subpath
                    .is_some_and(|sub| matches!(run.root.path(sub), PathNode::ForeignPath(_))),
                _ => false,
            };
            if !is_foreign {
                continue;
            }
            // Create foreign final path; this gets rid of a no-longer-needed
            // outer plan (if any), which makes the EXPLAIN output look
            // cleaner.
            let (parent, target, rows, disabled_nodes, startup_cost, total_cost, pathkeys) = {
                let b = run.root.path(path).base();
                let mut keys = PgVec::new_in(mcx);
                keys.extend(b.pathkeys.iter().copied());
                (
                    b.parent,
                    b.pathtarget_id,
                    b.rows,
                    b.disabled_nodes,
                    b.startup_cost,
                    b.total_cost,
                    keys,
                )
            };
            let final_path = planner::pathnode::create_foreign_upper_path(
                run,
                parent,
                target,
                rows,
                disabled_nodes,
                startup_cost,
                total_cost,
                pathkeys,
                None,
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
            )?;

            // and add it to the final_rel.
            planner::pathnode::add_path(run, final_rel, final_path);

            // Safe to push down.
            fpinfo(run.root.rel(final_rel)).borrow_mut().pushdown_safe = true;
            return Ok(());
        }

        // If we get here it means no ForeignPaths; since we would already
        // have considered pushing down all operations for the query to the
        // remote server, give up on it.
        return Ok(());
    }

    debug_assert!(limit_needed);

    // If the input_rel is an ordered relation, replace the input_rel with
    // its input relation.
    let mut input_rel = input_rel;
    let mut has_final_sort = false;
    let mut pathkeys: Vec<PathKey> = Vec::new();
    if run.root.rel(input_rel).reloptkind == RELOPT_UPPER_REL
        && fpinfo(run.root.rel(input_rel)).borrow().stage == UPPERREL_ORDERED
    {
        input_rel = fpinfo(run.root.rel(input_rel))
            .borrow()
            .outerrel
            .expect("ordered rel has outerrel");
        has_final_sort = true;
        pathkeys = run.root.sort_pathkeys.iter().copied().collect();
    }

    // The input_rel should be a base, join, or grouping relation.
    debug_assert!({
        let kind = run.root.rel(input_rel).reloptkind;
        kind == RELOPT_BASEREL
            || kind == RELOPT_JOINREL
            || (kind == RELOPT_UPPER_REL
                && fpinfo(run.root.rel(input_rel)).borrow().stage == UPPERREL_GROUP_AGG)
    });

    // We try to create a path below by extending a simple foreign path for
    // the underlying base, join, or grouping relation to perform the final
    // sort (if has_final_sort) and the LIMIT restriction remotely, which is
    // stored into the fdw_private list of the resulting path. (We
    // re-estimate the costs of sorting the underlying relation, if
    // has_final_sort.)

    // Assess if it is safe to push down the LIMIT and OFFSET to the remote
    // server: not if the underlying relation has any local conditions.
    if !fpinfo(run.root.rel(input_rel)).borrow().local_conds.is_empty() {
        return Ok(());
    }

    // If the query has FETCH FIRST .. WITH TIES, 1) it must have ORDER BY as
    // well, and 2) ORDER BY must already have been determined to be safe to
    // push down. The FETCH clause would then be safe to push down with ORDER
    // BY if the remote server is v13 or later, but without a remote-version
    // check the remote query could fail entirely; disable pushing it.
    if parse.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
        return Ok(());
    }

    // Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
    // not safe to remote (is_foreign_expr(NULL) is true).
    if let Some(offset) = parse.limitOffset {
        if !deparse::is_foreign_expr(run, input_rel, offset)? {
            return Ok(());
        }
    }
    if let Some(count) = parse.limitCount {
        if !deparse::is_foreign_expr(run, input_rel, count)? {
            return Ok(());
        }
    }

    // Safe to push down.
    fpinfo(run.root.rel(final_rel)).borrow_mut().pushdown_safe = true;

    // Construct PgFdwPathExtraData.
    let target = run.root.upper_targets[UPPERREL_FINAL as usize]
        .expect("upper_targets[UPPERREL_FINAL] is set before the final rel");
    let fpextra = PgFdwPathExtraData {
        target,
        has_final_sort,
        has_limit: limit_needed,
        limit_tuples,
        count_est,
        offset_est,
    };

    // Estimate the costs of performing the final sort and the LIMIT
    // restriction remotely. If has_final_sort is false, we wouldn't need to
    // execute EXPLAIN anymore if use_remote_estimate, since the costs can be
    // roughly estimated using the costs we already have for the underlying
    // relation, in the same way as when use_remote_estimate is false. Since
    // it's pretty expensive to execute EXPLAIN, force use_remote_estimate to
    // false in that case.
    let save_use_remote_estimate = if !fpextra.has_final_sort {
        let fpc = fpinfo(run.root.rel(input_rel));
        let mut ifp = fpc.borrow_mut();
        let saved = ifp.use_remote_estimate;
        ifp.use_remote_estimate = false;
        Some(saved)
    } else {
        None
    };
    let estimate = estimate_path_cost_size(run, input_rel, &[], &pathkeys, Some(&fpextra));
    if let Some(saved) = save_use_remote_estimate {
        fpinfo(run.root.rel(input_rel)).borrow_mut().use_remote_estimate = saved;
    }
    let (rows, _width, disabled_nodes, startup_cost, total_cost) = estimate?;

    // Build the fdw_private list that will be used by postgresGetForeignPlan.
    let fdw_private = make_path_private(run, has_final_sort, limit_needed)?;

    // Create foreign final path (parent = input_rel, as C); this gets rid of
    // a no-longer-needed outer plan (if any), which makes the EXPLAIN output
    // look cleaner.
    let mut path_pathkeys = PgVec::new_in(mcx);
    path_pathkeys.extend(pathkeys);
    let final_path = planner::pathnode::create_foreign_upper_path(
        run,
        input_rel,
        Some(target),
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        path_pathkeys,
        None,
        PgVec::new_in(mcx),
        fdw_private,
    )?;

    // and add it to the final_rel.
    planner::pathnode::add_path(run, final_rel, final_path);
    Ok(())
}

fn add_foreign_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    grouped_rel: RelId,
    having_qual: Option<Node<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    {
        let parse = run.parse();
        if parse.groupClause.is_nil()
            && parse.groupingSets.is_nil()
            && !parse.hasAggs
            && !run.root.hasHavingQual
        {
            return Ok(());
        }
    }

    // Save input_rel as outerrel; copy FDW options (merge_fdw_options with
    // fpinfo_i = NULL).
    fpinfo(run.root.rel(grouped_rel)).borrow_mut().outerrel = Some(input_rel);
    copy_fdw_options(run, grouped_rel, input_rel);

    if !foreign_grouping_ok(run, grouped_rel, having_qual)? {
        return Ok(());
    }

    let local_ids: Vec<RinfoId> =
        fpinfo(run.root.rel(grouped_rel)).borrow().local_conds.iter().copied().collect();
    let local_conds_sel = planner::clausesel::clauselist_selectivity(
        run,
        &local_ids,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut local_cost = types_pathnodes::QualCost::default();
    for &ri in &local_ids {
        let clause = run.root.rinfo(ri).clause;
        let node = *run.root.expr_node(clause);
        let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
        local_cost.startup += c.startup;
        local_cost.per_tuple += c.per_tuple;
    }
    {
        let fpc = fpinfo(run.root.rel(grouped_rel));
        let mut fpm = fpc.borrow_mut();
        fpm.local_conds_sel = local_conds_sel;
        fpm.local_conds_cost = local_cost;
    }

    let (rows, width, disabled_nodes, startup_cost, total_cost) =
        estimate_path_cost_size(run, grouped_rel, &[], &[], None)?;
    {
        let fpc = fpinfo(run.root.rel(grouped_rel));
        let mut fpm = fpc.borrow_mut();
        fpm.rows = rows;
        fpm.width = width;
        fpm.disabled_nodes = disabled_nodes;
        fpm.startup_cost = startup_cost;
        fpm.total_cost = total_cost;
    }

    let path = planner::pathnode::create_foreign_upper_path(
        run,
        grouped_rel,
        None,
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        PgVec::new_in(mcx),
        None,
        PgVec::new_in(mcx),
        PgVec::new_in(mcx),
    )?;
    planner::pathnode::add_path(run, grouped_rel, path);
    Ok(())
}

fn foreign_grouping_ok<'mcx>(
    run: &mut PlannerRun<'mcx>,
    grouped_rel: RelId,
    having_qual: Option<Node<'mcx>>,
) -> PgResult<bool> {
    let mcx = run.mcx;
    if !run.parse().groupingSets.is_nil() {
        return Ok(false);
    }
    let outerrel = fpinfo(run.root.rel(grouped_rel)).borrow().outerrel.expect("outerrel set");
    if !fpinfo(run.root.rel(outerrel)).borrow().local_conds.is_empty() {
        return Ok(false);
    }

    let mut tlist: NodeList<'mcx> = NodeList::nil();
    let add_flat = |mcx: mcx::Mcx<'mcx>,
                    tlist: &mut NodeList<'mcx>,
                    expr: Node<'mcx>|
     -> PgResult<()> {
        for existing in &*tlist {
            let tle = existing.as_target_entry().expect("TargetEntry");
            if types_nodes::equal::equal(tle.expr, expr) {
                return Ok(());
            }
        }
        let resno = (tlist.len() + 1) as i16;
        tlist.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr,
                    resno,
                    resname: None,
                    ressortgroupref: 0,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?,
        )?;
        Ok(())
    };

    let (expr_ids, sgrefs): (Vec<types_pathnodes::NodeId>, Vec<u32>) = {
        let pt = run.pathtarget(run.rel_reltarget_id(grouped_rel));
        (pt.exprs.iter().copied().collect(), pt.sortgrouprefs.iter().copied().collect())
    };
    for (i, id) in expr_ids.iter().enumerate() {
        let expr = *run.root.expr_node(*id);
        let sgref = sgrefs.get(i).copied().unwrap_or(0);
        let in_group_by = sgref != 0
            && run.parse().groupClause.iter().any(|n| {
                n.as_variant::<types_nodes::parsenodes::SortGroupClause>()
                    .is_some_and(|s| s.tleSortGroupRef == sgref)
            });
        if in_group_by {
            if !deparse::is_foreign_expr(run, grouped_rel, expr)? {
                return Ok(false);
            }
            if deparse::is_foreign_param(run, grouped_rel, expr) {
                return Ok(false);
            }
            // Duplicate GROUP BY entries with distinct sortgrouprefs are
            // preserved (no dedup, unlike add_to_flat_tlist).
            let resno = (tlist.len() + 1) as i16;
            tlist.lappend(
                mcx,
                Node::mk(
                    mcx,
                    types_nodes::primnodes::TargetEntry {
                        expr,
                        resno,
                        resname: None,
                        ressortgroupref: sgref,
                        resorigtbl: 0,
                        resorigcol: 0,
                        resjunk: false,
                    },
                )?,
            )?;
        } else if deparse::is_foreign_expr(run, grouped_rel, expr)?
            && !deparse::is_foreign_param(run, grouped_rel, expr)
        {
            add_flat(mcx, &mut tlist, expr)?;
        } else {
            let aggvars = vars::pull_var_clause(mcx, expr, vars::PVC_INCLUDE_AGGREGATES)?;
            for v in &aggvars {
                if !deparse::is_foreign_expr(run, grouped_rel, v)? {
                    return Ok(false);
                }
            }
            for v in &aggvars {
                if v.node_tag() == types_nodes::NodeTag::T_Aggref {
                    add_flat(mcx, &mut tlist, v)?;
                }
            }
        }
    }

    // HAVING: classify into remote (HAVING pushed) and local conds.
    let mut remote_conds: Vec<RinfoId> = Vec::new();
    let mut local_conds: Vec<RinfoId> = Vec::new();
    if let Some(h) = having_qual {
        let clauses: Vec<Node<'mcx>> =
            h.as_list().expect("preprocessed havingQual is a list").iter().collect();
        for expr in clauses {
            let relids = planner::relnode::relids_copy(mcx, &run.root.rel(grouped_rel).relids);
            let sec = run.root.qual_security_level;
            let ri = planner::initsplan::make_restrictinfo(
                run,
                expr,
                true,
                false,
                false,
                false,
                sec,
                relids,
                planner::relnode::relids_empty(),
                planner::relnode::relids_empty(),
            )?;
            if deparse::is_foreign_expr(run, grouped_rel, expr)? {
                remote_conds.push(ri);
            } else {
                local_conds.push(ri);
            }
        }
    }

    // Vars/aggregates inside local HAVING conds must still be shippable.
    for &ri in &local_conds {
        let clause = *run.root.expr_node(run.root.rinfo(ri).clause);
        let aggvars = vars::pull_var_clause(mcx, clause, vars::PVC_INCLUDE_AGGREGATES)?;
        for v in &aggvars {
            if v.node_tag() == types_nodes::NodeTag::T_Aggref {
                if !deparse::is_foreign_expr(run, grouped_rel, v)? {
                    return Ok(false);
                }
                add_flat(mcx, &mut tlist, v)?;
            }
        }
    }

    let relation_name = {
        let o_name = fpinfo(run.root.rel(outerrel)).borrow().relation_name;
        mcx_str(mcx, &format!("Aggregate on ({o_name})"))?
    };

    {
        let fpc = fpinfo(run.root.rel(grouped_rel));
        let mut fp = fpc.borrow_mut();
        fp.remote_conds = PgVec::new_in(mcx);
        fp.remote_conds.extend(remote_conds.iter().copied());
        fp.local_conds = PgVec::new_in(mcx);
        fp.local_conds.extend(local_conds.iter().copied());
        fp.grouped_tlist = tlist;
        fp.pushdown_safe = true;
        fp.retrieved_rows = -1.0;
        fp.rel_startup_cost = -1.0;
        fp.rel_total_cost = -1.0;
        fp.relation_name = relation_name;
    }
    Ok(true)
}

// ---------- GetForeignPlan ----------

fn postgres_get_foreign_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    _foreigntableid: Oid,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: PgVec<'mcx, RinfoId>,
    mut outer_plan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let reloptkind = run.root.rel(rel_id).reloptkind;

    // Get FDW private data created by postgresGetForeignUpperPaths(), if
    // any (FdwPathPrivateIndex), and the path's pathkeys.
    let (has_final_sort, has_limit, path_pathkeys) = {
        let PathNode::ForeignPath(fp) = run.root.path(best_path) else {
            return Err(Box::new(PgError::error("postgresGetForeignPlan: not a ForeignPath")));
        };
        let flag = |id: types_pathnodes::NodeId| -> bool {
            run.root.expr_node(id).as_boolean().expect("FdwPathPrivate flag is a Boolean").boolval
        };
        let (fs, hl) = if fp.fdw_private.is_empty() {
            (false, false)
        } else {
            (flag(fp.fdw_private[0]), flag(fp.fdw_private[1]))
        };
        let keys: Vec<PathKey> = fp.path.pathkeys.iter().copied().collect();
        (fs, hl, keys)
    };
    let is_join = matches!(
        reloptkind,
        types_pathnodes::RELOPT_JOINREL
            | types_pathnodes::RELOPT_OTHER_JOINREL
            | types_pathnodes::RELOPT_UPPER_REL
            | types_pathnodes::RELOPT_OTHER_UPPER_REL
    );

    let scan_relid;
    let mut remote_rinfos: Vec<RinfoId> = Vec::new();
    let mut local_exprs: NodeList<'mcx> = NodeList::nil();
    let mut remote_recheck: NodeList<'mcx> = NodeList::nil();
    let mut fdw_scan_tlist: NodeList<'mcx> = NodeList::nil();
    let fdw_recheck_quals: NodeList<'mcx>;

    if !is_join {
        scan_relid = run.root.rel(rel_id).relid;
        // Split scan_clauses into remote and local (extract_actual_clauses
        // shape plus the remote/local decision).
        let (remote_set, local_set): (Vec<RinfoId>, Vec<RinfoId>) = {
            let fp = fpinfo(run.root.rel(rel_id)).borrow();
            (fp.remote_conds.iter().copied().collect(), fp.local_conds.iter().copied().collect())
        };
        for &ri in scan_clauses.iter() {
            if run.root.rinfo(ri).pseudoconstant {
                continue;
            }
            let clause = run.root.rinfo(ri).clause;
            let clause_node = *run.root.expr_node(clause);
            if remote_set.contains(&ri) {
                remote_rinfos.push(ri);
                remote_recheck.lappend(mcx, clause_node)?;
            } else if local_set.contains(&ri) {
                local_exprs.lappend(mcx, clause_node)?;
            } else if deparse::is_foreign_expr(run, rel_id, clause_node)? {
                remote_rinfos.push(ri);
                remote_recheck.lappend(mcx, clause_node)?;
            } else {
                local_exprs.lappend(mcx, clause_node)?;
            }
        }
        // Base-relation EPQ recheck reruns all the remote quals.
        fdw_recheck_quals = remote_recheck;
    } else {
        // Join relation: conditions come from the fpinfo, not scan_clauses.
        scan_relid = 0;
        debug_assert!(scan_clauses.is_empty());
        let (remote, local): (Vec<RinfoId>, Vec<RinfoId>) = {
            let fp = fpinfo(run.root.rel(rel_id)).borrow();
            (fp.remote_conds.iter().copied().collect(), fp.local_conds.iter().copied().collect())
        };
        for &ri in &remote {
            remote_rinfos.push(ri);
            let clause = run.root.rinfo(ri).clause;
            remote_recheck.lappend(mcx, *run.root.expr_node(clause))?;
        }
        for &ri in &local {
            let clause = run.root.rinfo(ri).clause;
            local_exprs.lappend(mcx, *run.root.expr_node(clause))?;
        }
        // EPQ recheck is handled by the local join alternative (outer_plan).
        fdw_recheck_quals = NodeList::nil();
        fdw_scan_tlist = build_tlist_to_deparse(run, rel_id)?;

        // Ensure that the outer plan produces a tuple whose descriptor
        // matches our scan tuple slot. Also, remove the local conditions
        // from the outer plan's quals, lest they be evaluated twice, once by
        // the local plan and once by the scan.
        if let Some(op) = outer_plan {
            // Right now, we only consider grouping and aggregation beyond
            // joins. Queries involving aggregates or grouping do not require
            // the EPQ mechanism, hence should not have an outer plan here.
            debug_assert!(!matches!(
                reloptkind,
                types_pathnodes::RELOPT_UPPER_REL | types_pathnodes::RELOPT_OTHER_UPPER_REL
            ));
            // First, update the plan's qual list if possible. In some cases
            // the quals might be enforced below the topmost plan level, in
            // which case we'll fail to remove them; it's not worth working
            // harder than this.
            for qual in local_exprs.iter() {
                let new_qual =
                    list_delete_equal(mcx, &op.as_plan().expect("plan node").qual, qual)?;
                // SAFETY: the outer plan was built by create_plan_recurse for
                // this ForeignScan alone (exclusive plan-tree ownership).
                unsafe { op.with_plan_mut(|p| p.qual = new_qual) }.expect("plan node");
                // For an inner join the local conditions of the foreign scan
                // plan can be part of the joinquals as well. (They might also
                // be in the mergequals or hashquals, but we can't touch those
                // without breaking the plan.)
                let join_inner = match op.node_tag() {
                    types_nodes::NodeTag::T_NestLoop => {
                        op.as_nest_loop().map(|j| (j.join.jointype, &j.join.joinqual))
                    }
                    types_nodes::NodeTag::T_MergeJoin => {
                        op.as_merge_join().map(|j| (j.join.jointype, &j.join.joinqual))
                    }
                    types_nodes::NodeTag::T_HashJoin => {
                        op.as_hash_join().map(|j| (j.join.jointype, &j.join.joinqual))
                    }
                    _ => None,
                };
                if let Some((jt, joinqual)) = join_inner {
                    if jt == types_nodes::jointype::JoinType::JOIN_INNER {
                        let new_joinqual = list_delete_equal(mcx, joinqual, qual)?;
                        // SAFETY: as above.
                        unsafe {
                            match op.node_tag() {
                                types_nodes::NodeTag::T_NestLoop => op
                                    .with_mut::<types_nodes::plannodes::NestLoop, _>(|j| {
                                        j.join.joinqual = new_joinqual
                                    }),
                                types_nodes::NodeTag::T_MergeJoin => op
                                    .with_mut::<types_nodes::plannodes::MergeJoin, _>(|j| {
                                        j.join.joinqual = new_joinqual
                                    }),
                                _ => op.with_mut::<types_nodes::plannodes::HashJoin, _>(|j| {
                                    j.join.joinqual = new_joinqual
                                }),
                            }
                        }
                        .expect("join plan node");
                    }
                }
            }
            // Now fix the subplan's tlist --- this might result in inserting
            // a Result node atop the plan tree.
            let parallel_safe = run.root.path(best_path).base().parallel_safe;
            outer_plan = Some(planner::createplan::change_plan_targetlist(
                mcx,
                op,
                fdw_scan_tlist.clone_in(mcx)?,
                parallel_safe,
            )?);
        }
    }

    // Deparse the remote SELECT, collecting params.
    let (sql, retrieved_attrs, params) = deparse::deparse_select_stmt_for_rel(
        run,
        rel_id,
        &fdw_scan_tlist,
        &remote_rinfos,
        &path_pathkeys,
        has_final_sort,
        has_limit,
        Some(PgVec::new_in(mcx)),
    )?;

    // Remember final_remote_exprs for possible use by direct modify.
    {
        let fpc = fpinfo(run.root.rel(rel_id));
        let mut fpm = fpc.borrow_mut();
        fpm.final_remote_exprs.clear();
        for &ri in &remote_rinfos {
            fpm.final_remote_exprs.push(run.root.rinfo(ri).clause);
        }
    }

    // fdw_exprs = params_list (Vars/Params to transmit).
    let mut fdw_exprs: NodeList<'mcx> = NodeList::nil();
    if let Some(params) = params {
        for p in params.iter() {
            fdw_exprs.lappend(mcx, *p)?;
        }
    }

    // fdw_private = [ SELECT sql, retrieved_attrs (Int list), fetch_size
    //                 (, relation_name for join/upper) ].
    let (fetch_size, relation_name) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.fetch_size, fp.relation_name)
    };
    let mut fdw_private: NodeList<'mcx> = NodeList::nil();
    fdw_private.lappend(mcx, Node::mk_string(mcx, mcx_str(mcx, sql.as_str())?)?)?;
    let mut ra: IntList<'mcx> = IntList::nil();
    for &a in retrieved_attrs.iter() {
        ra.lappend(mcx, a)?;
    }
    fdw_private.lappend(mcx, Node::mk_int_list(mcx, ra)?)?;
    fdw_private.lappend(mcx, Node::mk_integer(mcx, fetch_size)?)?;
    if is_join {
        fdw_private.lappend(mcx, Node::mk_string(mcx, relation_name)?)?;
    }

    planner::createplan::make_foreignscan(
        mcx,
        tlist,
        local_exprs,
        scan_relid,
        fdw_exprs,
        fdw_private,
        fdw_scan_tlist,
        fdw_recheck_quals,
        outer_plan,
    )
}

// list_delete (list.c): the list without the first cell equal() to `datum`.
fn list_delete_equal<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    datum: Node<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut out: NodeList<'mcx> = NodeList::nil();
    let mut deleted = false;
    for n in list.iter() {
        if !deleted && types_nodes::equal::equal(n, datum) {
            deleted = true;
            continue;
        }
        out.lappend(mcx, n)?;
    }
    Ok(out)
}

// build_tlist_to_deparse (deparse.c): Vars needed from the foreign server —
// reltarget exprs plus local_conds Vars, deduplicated (add_to_flat_tlist).
// Upper rels return the grouped_tlist built by foreign_grouping_ok.
fn build_tlist_to_deparse<'mcx>(
    run: &PlannerRun<'mcx>,
    rel_id: RelId,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    if matches!(
        run.root.rel(rel_id).reloptkind,
        types_pathnodes::RELOPT_UPPER_REL | types_pathnodes::RELOPT_OTHER_UPPER_REL
    ) {
        return fpinfo(run.root.rel(rel_id)).borrow().grouped_tlist.clone_in(mcx).map_err(Into::into);
    }
    let mut tlist: NodeList<'mcx> = NodeList::nil();
    let add_vars = |vars: NodeList<'mcx>, tlist: &mut NodeList<'mcx>| -> PgResult<()> {
        for v in &vars {
            let mut found = false;
            for existing in &*tlist {
                let tle = existing.as_target_entry().expect("TargetEntry");
                if types_nodes::equal::equal(tle.expr, v) {
                    found = true;
                    break;
                }
            }
            if !found {
                let resno = (tlist.len() + 1) as i16;
                tlist.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::TargetEntry {
                            expr: v,
                            resno,
                            resname: None,
                            ressortgroupref: 0,
                            resorigtbl: 0,
                            resorigcol: 0,
                            resjunk: false,
                        },
                    )?,
                )?;
            }
        }
        Ok(())
    };
    let expr_ids: Vec<types_pathnodes::NodeId> = {
        let rel = run.root.rel(rel_id);
        run.pathtarget(rel.pathtarget_id.expect("rel has reltarget")).exprs.iter().copied().collect()
    };
    for id in expr_ids {
        let node = *run.root.expr_node(id);
        let vars = vars::pull_var_clause(mcx, node, vars::PVC_RECURSE_PLACEHOLDERS)?;
        add_vars(vars, &mut tlist)?;
    }
    let local: Vec<RinfoId> =
        fpinfo(run.root.rel(rel_id)).borrow().local_conds.iter().copied().collect();
    for ri in local {
        let clause = *run.root.expr_node(run.root.rinfo(ri).clause);
        let vars = vars::pull_var_clause(mcx, clause, vars::PVC_RECURSE_PLACEHOLDERS)?;
        add_vars(vars, &mut tlist)?;
    }
    Ok(tlist)
}

// postgresExplainForeignScan: "Relations" for join/upper scans (RT indexes
// in fpinfo->relation_name translated to names; the reference name is
// EXPLAIN's deduplicated es->rtable_names entry, else the RTE's eref
// aliasname — postgres_fdw.c postgresExplainForeignScan), then "Remote SQL"
// when VERBOSE.
fn explain_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    flags: types_nodes::FdwExplainFlags<'_>,
    emit: &mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
) -> PgResult<()> {
    // Direct modify (postgresExplainDirectModify): only "Remote SQL".
    if node.plan.operation != types_nodes::CmdType::CMD_SELECT {
        if flags.verbose {
            if let Some(sql) = node.plan.fdw_private.iter().next().and_then(|n| n.as_string())
            {
                emit("Remote SQL", types_nodes::FdwExplainProp::Text(sql.sval))?;
            }
        }
        return Ok(());
    }
    const FDW_SCAN_PRIVATE_RELATIONS: usize = 3;
    let fdw_private = &node.plan.fdw_private;
    if fdw_private.len() > FDW_SCAN_PRIVATE_RELATIONS {
        let raw = fdw_private
            .iter()
            .nth(FDW_SCAN_PRIVATE_RELATIONS)
            .and_then(|n| n.as_string())
            .expect("fdw_private[3] is the relations string")
            .sval;
        let mcx = estate.es_query_cxt;
        // setrefs offsets RT indexes but not this string; recover the offset
        // from the minimum rti in the string vs fs_base_relids.
        let mut minrti = i32::MAX;
        {
            let mut it = raw.char_indices().peekable();
            while let Some((i, c)) = it.next() {
                if c.is_ascii_digit() {
                    let mut end = i + 1;
                    while let Some(&(j, cj)) = it.peek() {
                        if cj.is_ascii_digit() {
                            it.next();
                            end = j + 1;
                        } else {
                            break;
                        }
                    }
                    let rti: i32 = raw[i..end].parse().expect("digits");
                    minrti = minrti.min(rti);
                }
            }
        }
        let rtoffset = node.plan.fs_base_relids.next_member(-1) - minrti;
        let mut out = String::new();
        let mut chars = raw.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c.is_ascii_digit() {
                let mut end = i + 1;
                while let Some(&(j, cj)) = chars.peek() {
                    if cj.is_ascii_digit() {
                        chars.next();
                        end = j + 1;
                    } else {
                        break;
                    }
                }
                let rti: i32 = raw[i..end].parse().expect("digits");
                let rti = rti + rtoffset;
                debug_assert!(node.plan.fs_base_relids.is_member(rti));
                let rte = estate.es_range_table[(rti - 1) as usize];
                let relname = lsyscache::get_rel_name(mcx, rte.relid)?
                    .expect("relation exists");
                use crate::deparse::Push as _;
                let mut piece = mcx::PgString::new_in(mcx);
                if flags.verbose {
                    let nsp = lsyscache::get_namespace_name_or_temp(
                        mcx,
                        lsyscache::get_rel_namespace(rte.relid)?,
                    )?
                    .expect("namespace exists");
                    deparse::append_quoted_identifier(&mut piece, mcx, nsp.as_str())?;
                    piece.push('.');
                }
                deparse::append_quoted_identifier(&mut piece, mcx, relname.as_str())?;
                let refname = flags
                    .rtable_names
                    .get((rti - 1) as usize)
                    .copied()
                    .flatten()
                    .or_else(|| rte.eref.and_then(|e| e.aliasname))
                    .unwrap_or("");
                if !refname.is_empty() && refname != relname.as_str() {
                    piece.push(' ');
                    deparse::append_quoted_identifier(&mut piece, mcx, refname)?;
                }
                out.push_str(piece.as_str());
            } else {
                out.push(c);
            }
        }
        emit("Relations", types_nodes::FdwExplainProp::Text(&out))?;
    }
    if !flags.verbose {
        return Ok(());
    }
    // fdw_private[FdwScanPrivateSelectSql] = the remote SELECT.
    if let Some(sql) = node.plan.fdw_private.iter().next().and_then(|n| n.as_string()) {
        emit("Remote SQL", types_nodes::FdwExplainProp::Text(sql.sval))?;
    }
    Ok(())
}

static PLAN_ROUTINE: FdwPlanRoutine = FdwPlanRoutine {
    get_foreign_rel_size: postgres_get_foreign_rel_size,
    get_foreign_paths: postgres_get_foreign_paths,
    get_foreign_plan: postgres_get_foreign_plan,
    get_foreign_join_paths: Some(postgres_get_foreign_join_paths),
    get_foreign_upper_paths: Some(postgres_get_foreign_upper_paths),
    add_foreign_update_targets: Some(crate::modify::add_foreign_update_targets),
    plan_direct_modify: Some(crate::modify::plan_direct_modify),
    plan_foreign_modify: Some(crate::modify::plan_foreign_modify),
    is_foreign_path_async_capable: Some(postgres_is_foreign_path_async_capable),
};

static MODIFY_ROUTINE: nodemodifytable::FdwModifyRoutine = nodemodifytable::FdwModifyRoutine {
    begin: crate::modify::begin_foreign_modify,
    exec_insert: crate::modify::exec_foreign_insert,
    exec_update: crate::modify::exec_foreign_update,
    exec_delete: crate::modify::exec_foreign_delete,
    end: crate::modify::end_foreign_modify,
    explain: Some(crate::modify::explain_foreign_modify),
    flush: Some(crate::modify::flush_foreign_modify),
    begin_insert: Some(crate::modify::begin_foreign_insert),
};

// postgresIsForeignPathAsyncCapable.
fn postgres_is_foreign_path_async_capable<'mcx>(
    run: &PlannerRun<'mcx>,
    path_id: PathId,
) -> bool {
    let rel = run.root.rel(run.root.path(path_id).base().parent);
    fpinfo(rel).borrow().async_capable
}

static EXEC_ROUTINE: FdwExecRoutine = FdwExecRoutine {
    begin: crate::exec::begin_foreign_scan,
    iterate: crate::exec::iterate_foreign_scan,
    rescan: crate::exec::rescan_foreign_scan,
    end: crate::exec::end_foreign_scan,
    explain: Some(explain_foreign_scan),
    begin_direct: Some(crate::modify::begin_direct_modify),
    iterate_direct: Some(crate::modify::iterate_direct_modify),
    end_direct: Some(crate::modify::end_direct_modify),
    async_request: Some(crate::exec::foreign_async_request),
    async_configure_wait: Some(crate::exec::foreign_async_configure_wait),
    async_notify: Some(crate::exec::foreign_async_notify),
    recheck: Some(crate::exec::recheck_foreign_scan),
};

// _PG_init (option.c:588-599): runs when the library is loaded (the first
// C-language function of the extension resolved in this backend): define
// postgres_fdw.application_name, then reserve the prefix.
fn pg_init() -> PgResult<()> {
    // Unlike application_name GUC, no GUC_IS_NAME flag nor check_hook, so the
    // value may exceed NAMEDATALEN and hold non-ASCII (the remote truncates
    // and sanitizes it).
    guc::DefineCustomStringVariable(
        "postgres_fdw.application_name",
        Some("Sets the application name to be used on the remote server."),
        None,
        None,
        types_guc::PGC_USERSET,
        0,
    )?;
    guc::MarkGUCPrefixReserved("postgres_fdw");
    Ok(())
}

pub fn install() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: crate::LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
    planner::fdwplan::install_fdw_plan_routine(FdwKind::PostgresFdw, &PLAN_ROUTINE);
    nodeforeignscan::install_fdw_exec_routine(FdwKind::PostgresFdw, &EXEC_ROUTINE);
    nodemodifytable::install_fdw_modify_routine(FdwKind::PostgresFdw, &MODIFY_ROUTINE);
    static UPDATABLE_FN: foreigncmds::foreign::FdwUpdatableFn =
        crate::modify::is_foreign_rel_updatable;
    foreigncmds::foreign::install_fdw_updatable(FdwKind::PostgresFdw, &UPDATABLE_FN);
}
