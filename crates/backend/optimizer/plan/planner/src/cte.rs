//! SS_process_ctes (subselect.c) + set_cte_pathlist (allpaths.c), materialize-only.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::list::{IntList, NodeList};
use types_nodes::parsenodes::{CTEMaterialize, Query, RangeTblEntry};
use types_nodes::primnodes::{SubLinkType, SubPlan};
use types_nodes::Node;
use types_pathnodes::{PlannerInfo, RelId};

use crate::createplan::create_plan;
use crate::pathnode::{add_path, get_cheapest_fractional_path};
use crate::planmain::fetch_final_rel;
use crate::run::PlannerRun;

// DIVERGENCE from C: every referenced CTE is force-materialized — the
// single-reference inline arm (inline_cte) is skipped so the CTE Scan plan
// shape always holds; explicit hints that would change that are loud.
pub fn ss_process_ctes<'mcx>(run: &mut PlannerRun<'mcx>, parse: &Query<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(run.root.cte_plan_ids.is_empty());

    for cte_node in &parse.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctename = cte.ctename.expect("CTE has a name");
        let ctequery = cte
            .ctequery
            .and_then(|n| n.as_query())
            .expect("CTE ctequery is an analyzed Query");

        if cte.cterefcount == 0 && ctequery.commandType == types_nodes::CmdType::CMD_SELECT {
            run.root.cte_plan_ids.push(-1);
            continue;
        }
        if cte.ctematerialized != CTEMaterialize::CTEMaterializeDefault {
            panic!(
                "SS_process_ctes (subselect.c): MATERIALIZED/NOT MATERIALIZED hint on \
                 \"{ctename}\"; CTE inline lane"
            );
        }
        if cte.cterecursive {
            panic!("SS_process_ctes (subselect.c): WITH RECURSIVE \"{ctename}\"; M2 recursive-CTE lane");
        }
        if ctequery.commandType != types_nodes::CmdType::CMD_SELECT {
            panic!("SS_process_ctes (subselect.c): data-modifying CTE \"{ctename}\"; M2 DML-CTE lane");
        }

        let subquery = crate::subselect::query_cells_copy(mcx, ctequery)?;

        debug_assert!(run.root.plan_params.is_empty());
        run.push_root()?;
        crate::subquery::subquery_planner(run, subquery, 0.0, None)?;
        let final_rel = fetch_final_rel(run);
        let best_path = get_cheapest_fractional_path(run, final_rel, 0.0);
        let plan = create_plan(run, best_path)?;
        run.pop_root_to_subroot();
        if !run.root.plan_params.is_empty() {
            panic!("SS_process_ctes (subselect.c): unexpected outer reference in CTE query");
        }

        let (first_col_type, first_col_typmod, first_col_collation) =
            crate::subselect::get_first_col_type(plan);
        let paramid = assign_special_exec_param(run)?;

        run.glob.subplans.lappend(mcx, plan)?;
        let plan_id = run.glob.subplans.len() as i32;
        debug_assert_eq!(run.subroots.len(), run.glob.subplans.len());

        let mut splan = SubPlan {
            subLinkType: SubLinkType::CTE_SUBLINK,
            testexpr: None,
            paramIds: IntList::nil(),
            plan_id,
            plan_name: Some(str_in(mcx, &format!("CTE {ctename}"))?),
            firstColType: first_col_type,
            firstColTypmod: first_col_typmod,
            firstColCollation: first_col_collation,
            useHashTable: false,
            unknownEqFalse: false,
            parallel_safe: false,
            setParam: IntList::make1(mcx, paramid)?,
            parParam: IntList::nil(),
            args: NodeList::nil(),
            startup_cost: 0.0,
            per_call_cost: 0.0,
        };
        crate::subselect::cost_subplan(&mut splan, plan);
        let splan_node = Node::mk(mcx, splan)?;
        let splan_id = run.intern_expr(splan_node);
        run.root.init_plans.push(splan_id);
        run.root.cte_plan_ids.push(plan_id);
    }
    Ok(())
}

// SS_assign_special_exec_param (paramassign.c).
fn assign_special_exec_param(run: &mut PlannerRun<'_>) -> PgResult<i32> {
    let paramid = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(run.mcx, 0)?;
    Ok(paramid)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// set_cte_pathlist (allpaths.c); pathkeys stay empty (convert_subquery_pathkeys
// unported — a sorted CTE output loses its order hint, cost-only divergence).
pub fn set_cte_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    let (plan_id, _) = cte_plan_id_and_param(run, rte);
    let cteplan = run.glob.subplans.nth((plan_id - 1) as usize);
    let plan_rows = cteplan.as_plan().expect("plan node").plan_rows;
    crate::costsize::set_cte_size_estimates(run, rel, plan_rows)?;
    debug_assert!(run.root.rel(rel).lateral_relids.is_none());
    let path = crate::pathnode::create_ctescan_path(run, rel)?;
    add_path(run, rel, path);
    Ok(())
}

pub(crate) fn cte_plan_id_and_param(run: &PlannerRun<'_>, rte: &RangeTblEntry<'_>) -> (i32, i32) {
    assert!(
        !rte.self_reference,
        "set_worktable_pathlist (allpaths.c): recursive self-reference; M2 recursive-CTE lane"
    );
    let ctename = rte.ctename.expect("CTE RTE has ctename");
    let levelsup = rte.ctelevelsup as usize;
    // Parent roots intern their parse Query only after preprocessing, so an
    // up-level reference cannot resolve yet.
    assert!(
        levelsup == 0,
        "set_cte_pathlist (allpaths.c): ctelevelsup {levelsup} for CTE \"{ctename}\" \
         (reference from a sub-query level); M2 nested-CTE lane"
    );
    let cteroot: &PlannerInfo<'_> = &run.root;
    let parse = run.queries[cteroot.parse.0 as usize];
    let ndx = parse
        .cteList
        .iter()
        .position(|c| {
            c.as_common_table_expr().expect("cteList cell").ctename == Some(ctename)
        })
        .unwrap_or_else(|| panic!("could not find CTE \"{ctename}\""));
    assert!(
        ndx < cteroot.cte_plan_ids.len(),
        "could not find plan for CTE \"{ctename}\""
    );
    let plan_id = cteroot.cte_plan_ids[ndx];
    assert!(plan_id > 0, "no plan was made for CTE \"{ctename}\"");

    let cte_param = cteroot
        .init_plans
        .iter()
        .find_map(|&ipid| {
            let sp = cteroot
                .expr_node(ipid)
                .as_sub_plan()
                .expect("init_plans holds SubPlan nodes");
            (sp.plan_id == plan_id).then(|| sp.setParam.nth(0))
        })
        .unwrap_or_else(|| panic!("could not find plan for CTE \"{ctename}\""));
    (plan_id, cte_param)
}
