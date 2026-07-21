//! CTE planning: SS_process_ctes, inline_cte, {cte,worktable} pathlists.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::parsenodes::{CTEMaterialize, Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{SubLinkType, SubPlan};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{PlannerInfo, RelId};

use crate::createplan::create_plan;
use crate::pathnode::{add_path, get_cheapest_fractional_path};
use crate::planmain::fetch_final_rel;
use crate::run::PlannerRun;

pub fn ss_process_ctes<'mcx>(run: &mut PlannerRun<'mcx>, parse: &Query<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(run.root.cte_plan_ids.is_empty());
    // Child levels read this cteList mid-preprocessing (pre-seal): snapshot
    // it here, cells shared, positions == cte_plan_ids index.
    run.root.cte_list = parse.cteList.clone_in(mcx)?;

    for cte_node in &parse.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctename = cte.ctename.expect("CTE has a name");
        let ctequery_node = cte.ctequery.expect("CTE has a ctequery");
        let ctequery = ctequery_node.as_query().expect("CTE ctequery is an analyzed Query");
        let cmd_type = ctequery.commandType;

        if cte.cterefcount == 0 && cmd_type == types_nodes::CmdType::CMD_SELECT {
            run.root.cte_plan_ids.push(-1);
            continue;
        }

        if (cte.ctematerialized == CTEMaterialize::CTEMaterializeNever
            || (cte.ctematerialized == CTEMaterialize::CTEMaterializeDefault
                && cte.cterefcount == 1))
            && !cte.cterecursive
            && cmd_type == types_nodes::CmdType::CMD_SELECT
            && !contain_dml(ctequery_node)?
            && (cte.cterefcount <= 1 || !contain_outer_selfref(ctequery_node)?)
            && !clauses::contain_volatile_functions(ctequery_node)?
        {
            inline_cte(run, parse, ctename, ctequery_node, cte.cterefcount)?;
            run.root.cte_plan_ids.push(-1);
            continue;
        }

        let subquery =
            mcx::leak_in(mcx::alloc_in(mcx, crate::subselect::query_cells_copy(mcx, ctequery)?)?);

        debug_assert!(run.root.plan_params.is_empty());
        run.push_root()?;
        crate::subquery::subquery_planner(run, subquery, cte.cterecursive, 0.0, None)?;
        let final_rel = fetch_final_rel(run);
        let best_path = get_cheapest_fractional_path(run, final_rel, 0.0);
        let plan = create_plan(run, best_path)?;
        let pathkey_descs = crate::pathkeys::extract_subquery_pathkey_descs(run, best_path);
        let tlist = crate::pathkeys::extract_subquery_tlist(run, best_path);
        run.pop_root_to_subroot();
        if !run.root.plan_params.is_empty() {
            panic!("SS_process_ctes (subselect.c): unexpected outer reference in CTE query");
        }

        let (first_col_type, first_col_typmod, first_col_collation) =
            crate::subselect::get_first_col_type(plan);
        let paramid = assign_special_exec_param(run)?;

        run.glob.subplans.lappend(mcx, plan)?;
        let plan_id = run.glob.subplans.len() as i32;
        run.cte_subpath_infos.push(types_pathnodes::run::CteSubpathInfo {
            plan_id,
            pathkey_descs,
            tlist,
        });
        // >= not ==: ancestors' parked subroots may be in flight (build_subplan).
        debug_assert!(run.subroots.len() >= run.glob.subplans.len());

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

fn contain_dml(node: Node<'_>) -> PgResult<bool> {
    struct W;
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(q) = node.as_query() {
                if q.commandType != types_nodes::CmdType::CMD_SELECT || !q.rowMarks.is_nil() {
                    return Ok(true);
                }
                return nodes_core::query_tree_walker(q, self, 0);
            }
            nodes_core::expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            if q.commandType != types_nodes::CmdType::CMD_SELECT || !q.rowMarks.is_nil() {
                return Ok(true);
            }
            nodes_core::query_tree_walker(q, self, 0)
        }
    }
    nodes_core::NodeWalker::visit(&mut W, node)
}

fn contain_outer_selfref(node: Node<'_>) -> PgResult<bool> {
    struct W {
        depth: u32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(rte) = node.as_range_tbl_entry() {
                return Ok(rte.rtekind == RTEKind::RTE_CTE
                    && rte.self_reference
                    && rte.ctelevelsup >= self.depth);
            }
            if let Some(q) = node.as_query() {
                return self.visit_query_ref(q);
            }
            nodes_core::expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.depth += 1;
            let r =
                nodes_core::query_tree_walker(q, self, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
            self.depth -= 1;
            Ok(r)
        }
    }
    debug_assert_eq!(node.node_tag(), NodeTag::T_Query);
    let mut w = W { depth: 0 };
    nodes_core::NodeWalker::visit(&mut w, node)
}

struct InlineCteWalker<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    ctename: &'a str,
    levelsup: i64,
    ctequery: Node<'mcx>,
    // cterefcount == 1: source is dead after single inlining, so the tree
    // moves; multi-reference NOT MATERIALIZED deep-copies per reference.
    share: bool,
}

impl<'a, 'mcx> nodes_core::NodeWalker<'mcx> for InlineCteWalker<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(q) = node.as_query() {
            return self.visit_query_ref(q);
        }
        if let Some(rte) = node.as_range_tbl_entry() {
            if rte.rtekind == RTEKind::RTE_CTE
                && rte.ctename == Some(self.ctename)
                && rte.ctelevelsup as i64 == self.levelsup
            {
                let newquery_node = if self.share {
                    self.ctequery
                } else {
                    let s = outfuncs::nodeToString(self.mcx, self.ctequery)?;
                    readfuncs::stringToNode(self.mcx, s.as_str())?
                };
                if self.levelsup > 0 {
                    rewrite_manip::IncrementVarSublevelsUp(
                        newquery_node,
                        self.levelsup as i32,
                        1,
                    )?;
                }
                let newquery = newquery_node.as_query().expect("CTE ctequery is a Query");
                // FOR UPDATE never extends into CTEs, so no rowmark pushdown.
                // SAFETY: the parse tree is planner-owned; no borrow of this
                // RTE is read across the write.
                unsafe {
                    node.with_mut::<RangeTblEntry, _>(|r| {
                        r.rtekind = RTEKind::RTE_SUBQUERY;
                        r.subquery = Some(newquery);
                        r.security_barrier = false;
                        r.ctename = None;
                        r.ctelevelsup = 0;
                        r.self_reference = false;
                        r.coltypes = OidList::nil();
                        r.coltypmods = IntList::nil();
                        r.colcollations = OidList::nil();
                    })
                }
                .expect("RangeTblEntry");
            }
            return Ok(false);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    // Visit RTEs after their contents so the walk never descends into the
    // freshly inlined subquery.
    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.levelsup += 1;
        let r = nodes_core::query_tree_walker(q, self, nodes_core::QTW_EXAMINE_RTES_AFTER)?;
        self.levelsup -= 1;
        Ok(r)
    }
}

// The outer Query is a pre-seal local: its fields walk here in
// query_tree_walker order; nested (sealed) queries take the generic walker.
fn inline_cte<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    ctename: &str,
    ctequery: Node<'mcx>,
    cterefcount: i32,
) -> PgResult<()> {
    let mut w = InlineCteWalker {
        mcx: run.mcx,
        ctename,
        levelsup: 0,
        ctequery,
        share: cterefcount == 1,
    };
    let visit_all = |w: &mut InlineCteWalker<'_, 'mcx>,
                     list: &NodeList<'mcx>|
     -> PgResult<()> {
        for n in list {
            nodes_core::NodeWalker::visit(w, n)?;
        }
        Ok(())
    };
    let visit_opt =
        |w: &mut InlineCteWalker<'_, 'mcx>, n: Option<Node<'mcx>>| -> PgResult<()> {
            if let Some(n) = n {
                nodes_core::NodeWalker::visit(w, n)?;
            }
            Ok(())
        };
    visit_all(&mut w, &parse.targetList)?;
    visit_all(&mut w, &parse.withCheckOptions)?;
    visit_opt(&mut w, parse.onConflict)?;
    visit_all(&mut w, &parse.mergeActionList)?;
    visit_opt(&mut w, parse.mergeJoinCondition)?;
    visit_all(&mut w, &parse.returningList)?;
    if let Some(jt) = parse.jointree {
        visit_all(&mut w, &jt.fromlist)?;
        visit_opt(&mut w, jt.quals)?;
    }
    visit_opt(&mut w, parse.setOperations)?;
    visit_opt(&mut w, parse.havingQual)?;
    visit_opt(&mut w, parse.limitOffset)?;
    visit_opt(&mut w, parse.limitCount)?;
    for wc_node in &parse.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        visit_opt(&mut w, wc.startOffset)?;
        visit_opt(&mut w, wc.endOffset)?;
    }
    visit_all(&mut w, &parse.cteList)?;
    nodes_core::range_table_walker(&parse.rtable, &mut w, nodes_core::QTW_EXAMINE_RTES_AFTER)?;
    Ok(())
}

pub(crate) fn assign_special_exec_param(run: &mut PlannerRun<'_>) -> PgResult<i32> {
    let paramid = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(run.mcx, 0)?;
    Ok(paramid)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

pub fn set_cte_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    let (plan_id, cte_param) = resolve_cte_plan(run, rte);
    // createplan can't re-run the resolve: the suspended_roots chain this
    // level walked is unwound before its plan is created.
    run.root.cte_scan_params.push(types_pathnodes::CteScanParam {
        rti: rti as u32,
        plan_id,
        cte_param,
    });
    let cteplan = run.glob.subplans.nth((plan_id - 1) as usize);
    let plan_rows = cteplan.as_plan().expect("plan node").plan_rows;
    crate::costsize::set_cte_size_estimates(run, rel, plan_rows)?;
    let mcx = run.mcx;
    let pathkeys = match run
        .cte_subpath_infos
        .iter()
        .position(|i| i.plan_id == plan_id)
    {
        Some(ix) => {
            let descs = core::mem::replace(
                &mut run.cte_subpath_infos[ix].pathkey_descs,
                mcx::PgVec::new_in(mcx),
            );
            let tlist = core::mem::replace(
                &mut run.cte_subpath_infos[ix].tlist,
                mcx::PgVec::new_in(mcx),
            );
            let pks = crate::pathkeys::convert_subquery_pathkeys(run, rel, &descs, &tlist)?;
            run.cte_subpath_infos[ix].pathkey_descs = descs;
            run.cte_subpath_infos[ix].tlist = tlist;
            pks
        }
        None => mcx::PgVec::new_in(mcx),
    };
    debug_assert!(crate::relnode::relids_is_unset(&run.root.rel(rel).lateral_relids));
    let path = crate::pathnode::create_ctescan_path(run, rel, pathkeys)?;
    add_path(run, rel, path);
    Ok(())
}

// wt_param_id is stashed on this level's root because the suspended_roots
// chain to the cteroot (ctelevelsup-1 up) is gone by createplan time.
pub fn set_worktable_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    debug_assert!(rte.self_reference);
    let ctename = rte.ctename.expect("CTE RTE has ctename");
    let levelsup = rte.ctelevelsup as usize;
    assert!(levelsup > 0, "bad levelsup for CTE \"{ctename}\"");
    let up = levelsup - 1;
    let (wt_param, nr_path) = if up == 0 {
        (run.root.wt_param_id, run.root.non_recursive_path)
    } else {
        assert!(
            up <= run.suspended_roots.len(),
            "bad levelsup for CTE \"{ctename}\""
        );
        let cteroot: &PlannerInfo<'_> =
            &run.suspended_roots[run.suspended_roots.len() - up].root;
        (cteroot.wt_param_id, cteroot.non_recursive_path)
    };
    assert!(wt_param >= 0, "could not find param ID for CTE \"{ctename}\"");
    let nr_rows = {
        let pid = nr_path.unwrap_or_else(|| panic!("could not find path for CTE \"{ctename}\""));
        if up == 0 {
            run.root.path(pid).base().rows
        } else {
            run.suspended_roots[run.suspended_roots.len() - up].root.path(pid).base().rows
        }
    };
    run.root.self_ref_wt_param = wt_param;

    crate::costsize::set_cte_size_estimates(run, rel, nr_rows)?;
    debug_assert!(crate::relnode::relids_is_unset(&run.root.rel(rel).lateral_relids));
    let path = crate::pathnode::create_worktablescan_path(run, rel)?;
    add_path(run, rel, path);
    Ok(())
}

// levelsup steps up suspended_roots (one push_root per C parent_root link).
fn resolve_cte_plan(run: &PlannerRun<'_>, rte: &RangeTblEntry<'_>) -> (i32, i32) {
    debug_assert!(!rte.self_reference);
    let ctename = rte.ctename.expect("CTE RTE has ctename");
    let levelsup = rte.ctelevelsup as usize;
    let cteroot: &PlannerInfo<'_> = if levelsup == 0 {
        &run.root
    } else {
        assert!(
            levelsup <= run.suspended_roots.len(),
            "bad levelsup for CTE \"{ctename}\""
        );
        &run.suspended_roots[run.suspended_roots.len() - levelsup].root
    };
    let ndx = cteroot
        .cte_list
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

pub(crate) fn cte_plan_id_and_param(run: &PlannerRun<'_>, rti: usize) -> (i32, i32) {
    run.root
        .cte_scan_params
        .iter()
        .find(|p| p.rti == rti as u32)
        .map(|p| (p.plan_id, p.cte_param))
        .unwrap_or_else(|| {
            panic!("create_ctescan_plan (createplan.c): rti {rti} has no resolved CTE plan")
        })
}
