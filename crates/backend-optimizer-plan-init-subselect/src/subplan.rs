//! SUBPLAN BUILDING (subselect.c) — `make_subplan` / `build_subplan` and their
//! helpers, plus `SS_process_ctes`, the EXISTS-query simplifier /
//! EXISTS→ANY converter, and the `SS_make_initplan_*` entry points.
//!
//! # Model reconciliation (read before editing)
//!
//! The C lower planner (`subquery_planner` → `fetch_upper_rel` →
//! `get_cheapest_fractional_path` → `create_plan`) is reached through the single
//! outward seam [`plan_sublink_subquery`](backend_optimizer_plan_init_subselect_ext_seams::plan_sublink_subquery),
//! which hands back the finished `(subroot, plan, subpath, subquery_id)` quad.
//! `cost_subplan` / `materialize_finished_plan` / `ExecMaterializesOutput` are
//! likewise outward seams (their owners — costsize.c / createplan.c / execAmi.c
//! — are unported over the owned `SubPlan`/`Plan` model). `hash_ok_operator`'s
//! `SearchSysCache1(OPEROID)` projection rides the `oper_canhash_code` seam.
//!
//! The owned-value model: a `SubPlan` is built as `SubPlan<'static>` and stored
//! in the `node_arena` as an `Expr::SubPlan` (for initplans, its `NodeId` is
//! pushed onto `root.init_plans`). `glob.subplans`/`subroots`/`subpaths` carry
//! [`PlanId`] handles into the [`PlannerRun`] subplan store; the 1-based
//! `splan.plan_id == glob.subplans.len()` after interning. `multiexpr_params`
//! holds, per sublink-id, a `Vec<NodeId>` of the replacement `Param` nodes.

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    Expr, OpExpr, Param, ParamKind, SubLinkType, SubPlan, SubPlanExpr,
};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PathId, PlannerInfo};

use backend_nodes_core::makefuncs::{
    make_ands_explicit, make_ands_implicit, make_null_const, make_opclause, make_target_entry,
    make_var_from_target_entry,
};
use backend_nodes_nodeFuncs_seams::expr_type_info;
use backend_optimizer_plan_init_subselect_ext_seams as initext;
use backend_optimizer_util_clauses::grounded::{
    contain_exec_param, contain_subplans, contain_volatile_functions,
};
use backend_utils_cache_lsyscache_seams as lsyscache;

use crate::correlation::SS_process_sublinks;

/// `VOIDOID` (pg_type.h).
const VOIDOID: Oid = types_core::catalog::VOIDOID;
/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = types_core::catalog::BOOLOID;
/// `RECORDOID` (pg_type.h) — the pseudo-type for an anonymous record.
const RECORDOID: Oid = 2249;
/// `INT8OID`.
const INT8OID: Oid = types_core::catalog::INT8OID;
/// `InvalidOid`.
const INVALID_OID: Oid = 0;
/// `ARRAY_EQ_OP` (pg_operator.h) — `anyarray = anyarray`.
const ARRAY_EQ_OP: Oid = 1070;
/// `RECORD_EQ_OP` (pg_operator.h) — `record = record`.
const RECORD_EQ_OP: Oid = 2988;

/// `MAXALIGN(len)` — round up to the maximum alignment boundary (8 bytes).
#[inline]
fn maxalign(len: i64) -> i64 {
    (len + 7) & !7
}

/// `SizeofHeapTupleHeader` (access/htup_details.h) = `offsetof(HeapTupleHeaderData,
/// t_bits)` = 23 bytes.
const SIZEOF_HEAP_TUPLE_HEADER: i64 = 23;

/// `get_hash_memory_limit()` (nodeHash.c) — the hash-table memory budget in
/// bytes. `hash_mem_multiplier * work_mem`; with the defaults (4.0 × 4 MB) this
/// is 16 MB. Read by `subplan_is_hashable`/`subpath_is_hashable`. The GUCs live
/// in unported owners; mirror the default product as the local read.
#[inline]
fn get_hash_memory_limit() -> f64 {
    // work_mem default 4096 kB; hash_mem_multiplier default 2.0 (PG 18).
    // get_hash_memory_limit returns bytes: (work_mem * 1024) * hash_mem_multiplier.
    (4096.0 * 1024.0) * 2.0
}

/// `elog(ERROR, ...)` shorthand.
fn elog_error(msg: impl Into<alloc::string::String>) -> PgError {
    PgError::error(msg)
}

// ===========================================================================
// get_first_col_type
// ===========================================================================

/// `get_first_col_type(plan, &coltype, &coltypmod, &colcollation)` (subselect.c):
/// the datatype/typmod/collation of the first column of the plan's output.
///
/// The plan node is an owned [`Node`] embedding a `Plan` base; its targetlist is
/// `Option<PgVec<TargetEntry>>`.
fn get_first_col_type(plan: &Node<'_>) -> PgResult<(Oid, i32, Oid)> {
    let head = plan.plan_head();
    // In cases such as EXISTS, tlist might be empty; arbitrarily use VOID.
    if let Some(tlist) = head.targetlist.as_ref() {
        if let Some(tent) = tlist.first() {
            if !tent.resjunk {
                let expr = tent
                    .expr
                    .as_deref()
                    .expect("get_first_col_type: TargetEntry has no expr");
                let info = expr_type_info::call(expr)?;
                return Ok((info.typid, info.typmod, info.collation));
            }
        }
    }
    Ok((VOIDOID, -1, INVALID_OID))
}

// ===========================================================================
// make_subplan
// ===========================================================================

/// `make_subplan(root, orig_subquery, subLinkType, subLinkId, testexpr,
/// isTopQual)` (subselect.c): convert a SubLink into a SubPlan (or InitPlan
/// replacement expression).
///
/// `orig_subquery` is the SubLink's embedded owned sub-`Query`
/// (`Option<PgBox<Query>>`); C `copyObject`s it before planning.
#[allow(clippy::too_many_arguments)]
pub fn make_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    orig_subquery: Option<PgBox<'static, types_nodes::copy_query::Query<'static>>>,
    sub_link_type: SubLinkType,
    sub_link_id: i32,
    testexpr: Option<Expr>,
    is_top_qual: bool,
) -> PgResult<Expr> {
    let orig_subquery = orig_subquery.expect("make_subplan: SubLink has no subselect");

    // Copy the source Query node. The owned tree gives us a borrow; clone the
    // embedded Query into the run's context so the lower planner can scribble.
    let mut subquery = orig_subquery.clone_in(mcx)?;

    // If it's an EXISTS subplan, we might be able to simplify it.
    let mut simple_exists = false;
    if sub_link_type == SubLinkType::Exists {
        simple_exists = simplify_EXISTS_query(root, mcx, &mut subquery)?;
    }

    // Choose the tuple_fraction for the lower planner.
    let tuple_fraction = if sub_link_type == SubLinkType::Exists {
        1.0 // just like a LIMIT 1
    } else if sub_link_type == SubLinkType::All || sub_link_type == SubLinkType::Any {
        0.5 // 50%
    } else {
        0.0 // default behavior
    };

    // plan_params should not be in use in current query level.
    debug_assert!(root.plan_params.is_empty());

    // Generate Paths for the subquery + best path + plan, via the seam.
    let sub_result = initext::plan_sublink_subquery::call(
        root,
        run,
        subquery,
        false, // hasRecursion
        tuple_fraction,
    )?;
    let initext::SublinkPlanResult {
        subroot,
        plan,
        subpath,
        subquery_id: _,
    } = sub_result;

    // Isolate the params needed by this specific subplan.
    let plan_params = core::mem::take(&mut root.plan_params);

    // And convert to SubPlan or InitPlan format.
    let mut result = build_subplan(
        mcx,
        root,
        run,
        plan,
        subpath,
        subroot,
        plan_params,
        sub_link_type,
        sub_link_id,
        testexpr,
        None, // testexpr_paramids = NIL
        is_top_qual,
    )?;

    // If it's a correlated EXISTS with an unimportant targetlist, try to
    // transform it to a hashable ANY and generate the alternative plan.
    if simple_exists && matches!(result, Expr::SubPlan(_)) {
        // Make a second copy of the original subquery and re-simplify.
        let mut subquery2 = orig_subquery.clone_in(mcx)?;
        let resimplified = simplify_EXISTS_query(root, mcx, &mut subquery2)?;
        debug_assert!(resimplified);

        // See if it can be converted to an ANY query.
        let converted =
            convert_EXISTS_to_ANY(root, run, mcx, subquery2)?;
        if let Some((subquery2, newtestexpr, param_ids)) = converted {
            // Generate Paths for the ANY subquery; we'll need all rows.
            let sub_result2 = initext::plan_sublink_subquery::call(
                root,
                run,
                subquery2,
                false,
                0.0,
            )?;
            let initext::SublinkPlanResult {
                subroot: subroot2,
                plan: plan2,
                subpath: subpath2,
                subquery_id: _,
            } = sub_result2;

            // Isolate the params needed by this specific subplan.
            let plan_params2 = core::mem::take(&mut root.plan_params);

            // Now we can check if it'll fit in hash_mem. (subpath_is_hashable
            // works from the Path; we have only the finished plan here, so use
            // the plan-based test, which is identical in substance.)
            if subplan_is_hashable(&plan2) {
                // OK, finish: convert to SubPlan format with the precomputed
                // testexpr + paramIds (so build_subplan won't re-convert).
                let hashplan = build_subplan(
                    mcx,
                    root,
                    run,
                    plan2,
                    subpath2,
                    subroot2,
                    plan_params2,
                    SubLinkType::Any,
                    0,
                    Some(newtestexpr),
                    Some(param_ids),
                    true,
                )?;

                // Check we got what we expected: a SubPlan with useHashTable.
                let hashplan = match hashplan {
                    Expr::SubPlan(s) => s,
                    _ => {
                        return Err(elog_error(
                            "convert_EXISTS_to_ANY did not yield a hashable SubPlan",
                        ))
                    }
                };
                debug_assert!(hashplan.0.parParam.is_empty());
                debug_assert!(hashplan.0.useHashTable);

                // Leave it to setrefs.c to decide which plan to use. The
                // `AlternativeSubPlan.subplans` field holds `PgBox<'mcx, SubPlan>`
                // (Mcx-allocated); re-box each `Box<SubPlan<'static>>` into mcx.
                let mut subplans: Vec<PgBox<'mcx, SubPlan<'mcx>>> = Vec::new();
                if let Expr::SubPlan(s) = result {
                    subplans.push(alloc_in(mcx, subplan_static_to_mcx(*s.0))?);
                } else {
                    unreachable!()
                }
                subplans.push(alloc_in(mcx, subplan_static_to_mcx(*hashplan.0))?);
                let alt = types_nodes::primnodes::AlternativeSubPlan { subplans };
                // Erase the 'mcx lifetime parameter to the Expr tree's notional
                // 'static (the data lives in the planner-run context); same
                // convention as `subplan_into_static`.
                let alt_static: types_nodes::primnodes::AlternativeSubPlan<'static> =
                    unsafe { core::mem::transmute(alt) };
                result = Expr::AlternativeSubPlan(
                    types_nodes::primnodes::AlternativeSubPlanExpr(alloc::boxed::Box::new(
                        alt_static,
                    )),
                );
                root.hasAlternativeSubPlans = true;
            }
        }
    }

    Ok(result)
}

/// Erase a `SubPlan<'mcx>`'s lifetime parameter to `'static` for embedding in
/// the lifetime-free `Expr::SubPlan` tree. The struct's `PgVec`/`PgString`/
/// `PgBox` children were allocated in the planner `mcx`; this transmutes only
/// the lifetime parameter, not the data — the exact convention used for
/// `SubLink.subselect` (`Query<'mcx>` -> `PgBox<'static, Query<'static>>`) and
/// `SubPlanExpr(Box<SubPlan<'static>>)` everywhere else in the repo.
#[inline]
fn subplan_into_static(s: SubPlan<'_>) -> SubPlan<'static> {
    // SAFETY: lifetime-parameter-only transmute of an owned value whose backing
    // allocations live in the planner-run context (which outlives the read-only
    // Expr tree's notional 'static lifetime). No data is moved or reinterpreted.
    unsafe { core::mem::transmute::<SubPlan<'_>, SubPlan<'static>>(s) }
}

/// Inverse of [`subplan_into_static`]: re-attach the `'mcx` lifetime to a
/// `SubPlan<'static>` extracted from an `Expr::SubPlan` so it can be re-boxed
/// into the `mcx`-bound `AlternativeSubPlan.subplans` Vec. Same
/// lifetime-parameter-only transmute convention.
#[inline]
fn subplan_static_to_mcx<'mcx>(s: SubPlan<'static>) -> SubPlan<'mcx> {
    // SAFETY: lifetime-parameter-only transmute; the data was originally
    // allocated in this same planner-run `mcx` (see `subplan_into_static`).
    unsafe { core::mem::transmute::<SubPlan<'static>, SubPlan<'mcx>>(s) }
}

// ===========================================================================
// build_subplan
// ===========================================================================

/// Outcome of the per-`subLinkType` dispatch in `build_subplan`: either a
/// non-SubPlan replacement expression (Param / Const / converted rowcompare
/// testexpr) or "the result is the SubPlan node itself" (built after interning).
enum ResultKind {
    Expr(Expr),
    SubPlanNode,
}

/// `build_subplan(...)` (subselect.c): build a SubPlan node given the raw
/// inputs. Returns either the SubPlan (`Expr::SubPlan`) or a replacement
/// expression if we decide to make it an InitPlan.
#[allow(clippy::too_many_arguments)]
fn build_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    mut plan: Node<'mcx>,
    subpath: Option<PathId>,
    subroot: PlannerInfo,
    plan_params: Vec<types_pathnodes::NodeId>,
    sub_link_type: SubLinkType,
    sub_link_id: i32,
    testexpr: Option<Expr>,
    testexpr_paramids: Option<PgVec<'mcx, i32>>,
    unknown_eq_false: bool,
) -> PgResult<Expr> {
    // Initialize the SubPlan node.
    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(&plan)?;
    let plan_parallel_safe = plan.plan_head().parallel_safe;

    let mut splan: SubPlan<'mcx> = SubPlan {
        subLinkType: sub_link_type,
        testexpr: None,
        paramIds: PgVec::new_in(mcx),
        plan_id: 0,
        plan_name: None,
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: unknown_eq_false,
        parallel_safe: plan_parallel_safe,
        setParam: PgVec::new_in(mcx),
        parParam: PgVec::new_in(mcx),
        args: PgVec::new_in(mcx),
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };

    // Make parParam and args lists of param IDs and expressions that the current
    // query level will pass to this child plan.
    for pitem_id in plan_params {
        let (item_id, param_id) = {
            let pitem = root.planner_param_item(pitem_id);
            (pitem.item, pitem.paramId)
        };
        let arg_expr = root.node(item_id).clone_in(mcx)?;
        let arg = match &arg_expr {
            Expr::PlaceHolderVar(_)
            | Expr::Aggref(_)
            | Expr::GroupingFunc(_)
            | Expr::ReturningExpr(_) => SS_process_sublinks(mcx, root, run, arg_expr, false)?,
            _ => arg_expr,
        };
        splan.parParam.push(param_id);
        splan.args.push(alloc_in(mcx, arg)?);
    }

    let is_init_plan: bool;
    let kind: ResultKind;

    if splan.parParam.is_empty() && sub_link_type == SubLinkType::Exists {
        debug_assert!(testexpr.is_none());
        let prm = backend_optimizer_util_paramassign::generate_new_exec_param(
            root, BOOLOID, -1, INVALID_OID,
        )?;
        splan.setParam.push(prm.paramid);
        is_init_plan = true;
        kind = ResultKind::Expr(Expr::Param(prm));
    } else if splan.parParam.is_empty() && sub_link_type == SubLinkType::Expr {
        debug_assert!(testexpr.is_none());
        let (ty, typmod, coll) = first_tlist_type(&plan)?;
        let prm =
            backend_optimizer_util_paramassign::generate_new_exec_param(root, ty, typmod, coll)?;
        splan.setParam.push(prm.paramid);
        is_init_plan = true;
        kind = ResultKind::Expr(Expr::Param(prm));
    } else if splan.parParam.is_empty() && sub_link_type == SubLinkType::Array {
        debug_assert!(testexpr.is_none());
        let (elem_ty, typmod, coll) = first_tlist_type(&plan)?;
        let arraytype = lsyscache::get_promoted_array_type::call(elem_ty)?;
        if arraytype == INVALID_OID {
            return Err(elog_error(alloc::format!(
                "could not find array type for datatype {elem_ty}"
            )));
        }
        let prm = backend_optimizer_util_paramassign::generate_new_exec_param(
            root, arraytype, typmod, coll,
        )?;
        splan.setParam.push(prm.paramid);
        is_init_plan = true;
        kind = ResultKind::Expr(Expr::Param(prm));
    } else if splan.parParam.is_empty() && sub_link_type == SubLinkType::RowCompare {
        let testexpr = testexpr.expect("ROWCOMPARE_SUBLINK requires a testexpr");
        let (params, ids) = generate_subquery_params(root, &plan)?;
        let r = convert_testexpr(mcx, testexpr, &params)?;
        for id in &ids {
            splan.paramIds.push(*id);
        }
        // splan->setParam = list_copy(splan->paramIds);
        for id in &ids {
            splan.setParam.push(*id);
        }
        is_init_plan = true;
        kind = ResultKind::Expr(r);
    } else if sub_link_type == SubLinkType::MultiExpr {
        debug_assert!(testexpr.is_none());
        let (params, ids) = generate_subquery_params(root, &plan)?;
        for id in &ids {
            splan.setParam.push(*id);
        }
        // Save the replacement Param nodes in the n'th cell of
        // root->multiexpr_params; intern each as a NodeId.
        let mut param_node_ids: Vec<types_pathnodes::NodeId> = Vec::new();
        for p in params {
            param_node_ids.push(root.alloc_node(p));
        }
        while root.multiexpr_params.len() < sub_link_id as usize {
            root.multiexpr_params.push(Vec::new());
        }
        debug_assert!(root.multiexpr_params[(sub_link_id - 1) as usize].is_empty());
        root.multiexpr_params[(sub_link_id - 1) as usize] = param_node_ids;

        if splan.parParam.is_empty() {
            is_init_plan = true;
            kind = ResultKind::Expr(Expr::Const(make_null_const(mcx, RECORDOID, -1, INVALID_OID)?));
        } else {
            is_init_plan = false;
            kind = ResultKind::SubPlanNode;
        }
    } else {
        // ALL / ANY (and any other) types.
        match (testexpr, &testexpr_paramids) {
            (te, Some(ids)) => {
                splan.testexpr = te.map(|t| alloc_in(mcx, t)).transpose()?;
                for id in ids {
                    splan.paramIds.push(*id);
                }
            }
            (Some(te), None) => {
                let (params, pids) = generate_subquery_params(root, &plan)?;
                for id in &pids {
                    splan.paramIds.push(*id);
                }
                let converted = convert_testexpr(mcx, te, &params)?;
                splan.testexpr = Some(alloc_in(mcx, converted)?);
            }
            (None, None) => {}
        }

        if sub_link_type == SubLinkType::Any
            && splan.parParam.is_empty()
            && subplan_is_hashable(&plan)
            && testexpr_is_hashable(splan.testexpr.as_deref(), &splan.paramIds)?
        {
            splan.useHashTable = true;
        } else if splan.parParam.is_empty()
            && enable_material()
            && !initext::exec_materializes_output::call(plan.node_tag())
        {
            plan = initext::materialize_finished_plan::call(plan)?;
        }

        is_init_plan = false;
        kind = ResultKind::SubPlanNode;
    }

    // ---- shared tail (C: add to glob lists / label / cost_subplan / return) --

    // Compute costs from the plan before handing it to the run.
    initext::cost_subplan::call(root, &mut splan, &plan)?;

    let subpath = subpath.unwrap_or(PathId(0));
    let interned = run.intern_subplan(plan, subroot, subpath);
    {
        let glob = root
            .glob
            .as_mut()
            .expect("build_subplan: root->glob is NULL");
        glob.subplans.push(interned);
        glob.subpaths.push(interned);
        glob.subroots.push(interned);
    }
    let plan_id = run.subplan_len() as i32;
    splan.plan_id = plan_id;

    if splan.parParam.is_empty() && !is_init_plan && !splan.useHashTable {
        let glob = root.glob.as_mut().unwrap();
        glob.rewind_plan_ids = relids_add_member(glob.rewind_plan_ids.take(), plan_id);
    }

    let label = if is_init_plan {
        alloc::format!("InitPlan {plan_id}")
    } else {
        alloc::format!("SubPlan {plan_id}")
    };
    splan.plan_name = Some(PgString::from_str_in(&label, mcx)?);

    let result = match kind {
        ResultKind::Expr(e) => {
            if is_init_plan {
                let nid = root.alloc_node(Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(
                    subplan_into_static(splan),
                ))));
                root.init_plans.push(nid);
            }
            e
        }
        ResultKind::SubPlanNode => {
            debug_assert!(!is_init_plan);
            Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(subplan_into_static(splan))))
        }
    };

    Ok(result)
}

// ===========================================================================
// generate_subquery_params / generate_subquery_vars
// ===========================================================================

/// First non-resjunk targetlist entry's `(type, typmod, collation)` —
/// `linitial(plan->targetlist)` projected by `exprType`/`exprTypmod`/
/// `exprCollation`. The C asserts `!te->resjunk`; here the EXPR/ARRAY arms call
/// this only after planning yields a real scalar tlist.
fn first_tlist_type(plan: &Node<'_>) -> PgResult<(Oid, i32, Oid)> {
    let head = plan.plan_head();
    let tlist = head
        .targetlist
        .as_ref()
        .expect("first_tlist_type: plan has no targetlist");
    let te = tlist.first().expect("first_tlist_type: empty targetlist");
    debug_assert!(!te.resjunk);
    let expr = te
        .expr
        .as_deref()
        .expect("first_tlist_type: TargetEntry has no expr");
    let info = expr_type_info::call(expr)?;
    Ok((info.typid, info.typmod, info.collation))
}

/// `generate_subquery_params(root, tlist, &paramIds)` (subselect.c): build a
/// list of `Param` exprs representing the output columns of a sublink's
/// sub-select, returning `(params, ids)`.
fn generate_subquery_params(
    root: &mut PlannerInfo,
    plan: &Node<'_>,
) -> PgResult<(Vec<Expr>, Vec<i32>)> {
    let head = plan.plan_head();
    let mut result: Vec<Expr> = Vec::new();
    let mut ids: Vec<i32> = Vec::new();
    if let Some(tlist) = head.targetlist.as_ref() {
        for tent in tlist.iter() {
            if tent.resjunk {
                continue;
            }
            let expr = tent
                .expr
                .as_deref()
                .expect("generate_subquery_params: TargetEntry has no expr");
            let info = expr_type_info::call(expr)?;
            let param = backend_optimizer_util_paramassign::generate_new_exec_param(
                root,
                info.typid,
                info.typmod,
                info.collation,
            )?;
            ids.push(param.paramid);
            result.push(Expr::Param(param));
        }
    }
    Ok((result, ids))
}

/// `generate_subquery_vars(root, tlist, varno)` (subselect.c): build a list of
/// `Var` exprs for the sub-select's output columns. Used only by the pull-up
/// family (out of scope here), but kept for completeness of the module API.
#[allow(dead_code)]
fn generate_subquery_vars<'mcx>(
    tlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    varno: types_core::primitive::Index,
) -> PgResult<Vec<Expr>> {
    let mut result: Vec<Expr> = Vec::new();
    for tent in tlist.iter() {
        if tent.resjunk {
            continue;
        }
        let var = make_var_from_target_entry(varno as i32, tent)?;
        result.push(Expr::Var(var));
    }
    Ok(result)
}

// ===========================================================================
// convert_testexpr
// ===========================================================================

/// `convert_testexpr(root, testexpr, subst_nodes)` (subselect.c): replace
/// PARAM_SUBLINK Params with the nodes from `subst_nodes`.
fn convert_testexpr<'mcx>(
    _mcx: Mcx<'mcx>,
    testexpr: Expr,
    subst_nodes: &[Expr],
) -> PgResult<Expr> {
    convert_testexpr_mutator(testexpr, subst_nodes)
}

/// `convert_testexpr_mutator(node, context)` (subselect.c).
fn convert_testexpr_mutator(node: Expr, subst_nodes: &[Expr]) -> PgResult<Expr> {
    if let Expr::Param(param) = &node {
        if param.paramkind == ParamKind::PARAM_SUBLINK {
            let id = param.paramid;
            if id <= 0 || (id as usize) > subst_nodes.len() {
                return Err(elog_error(alloc::format!(
                    "unexpected PARAM_SUBLINK ID: {id}"
                )));
            }
            // Copy the list item to avoid doubly-linked substructure.
            return Ok(subst_nodes[(id - 1) as usize].clone());
        }
    }
    if let Expr::SubLink(_) = &node {
        // A nested SubLink: do not recurse into it.
        return Ok(node);
    }
    let mut err: Option<PgError> = None;
    let result = backend_nodes_core::nodefuncs::expression_tree_mutator(node, &mut |child: Expr| {
        if err.is_some() {
            return child;
        }
        match convert_testexpr_mutator(child, subst_nodes) {
            Ok(c) => c,
            Err(e) => {
                err = Some(e);
                Expr::Const(backend_nodes_core::makefuncs::make_bool_const(true, false))
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

// ===========================================================================
// hashability checks
// ===========================================================================

/// `subplan_is_hashable(plan)` (subselect.c): can we implement an ANY subplan by
/// hashing? Estimated subquery result must fit in hash_mem.
fn subplan_is_hashable(plan: &Node<'_>) -> bool {
    let head = plan.plan_head();
    let subquery_size = head.plan_rows
        * ((maxalign(head.plan_width as i64) + maxalign(SIZEOF_HEAP_TUPLE_HEADER)) as f64);
    subquery_size <= get_hash_memory_limit()
}

/// `testexpr_is_hashable(testexpr, param_ids)` (subselect.c): is an ANY
/// SubLink's test expression hashable?
fn testexpr_is_hashable(testexpr: Option<&Expr>, param_ids: &[i32]) -> PgResult<bool> {
    match testexpr {
        Some(Expr::OpExpr(op)) => {
            if test_opexpr_is_hashable(op, param_ids)? {
                return Ok(true);
            }
        }
        Some(Expr::BoolExpr(b))
            if b.boolop == types_nodes::primnodes::BoolExprType::AND_EXPR =>
        {
            for andarg in b.args.iter() {
                match andarg {
                    Expr::OpExpr(op) => {
                        if !test_opexpr_is_hashable(op, param_ids)? {
                            return Ok(false);
                        }
                    }
                    _ => return Ok(false),
                }
            }
            return Ok(true);
        }
        _ => {}
    }
    Ok(false)
}

/// `test_opexpr_is_hashable(testexpr, param_ids)` (subselect.c).
fn test_opexpr_is_hashable(testexpr: &OpExpr, param_ids: &[i32]) -> PgResult<bool> {
    // The combining operator must be hashable and strict.
    if !hash_ok_operator(testexpr)? {
        return Ok(false);
    }
    // The left and right inputs must belong to outer / inner respectively.
    if testexpr.args.len() != 2 {
        return Ok(false);
    }
    if contain_exec_param(Some(&testexpr.args[0]), param_ids)? {
        return Ok(false);
    }
    if backend_optimizer_util_vars::var::contain_var_clause(&Node::Expr(testexpr.args[1].clone())) {
        return Ok(false);
    }
    Ok(true)
}

/// `hash_ok_operator(expr)` (subselect.c): check expression is hashable +
/// strict.
fn hash_ok_operator(expr: &OpExpr) -> PgResult<bool> {
    let opid = expr.opno;
    // Quick out if not a binary operator.
    if expr.args.len() != 2 {
        return Ok(false);
    }
    if opid == ARRAY_EQ_OP || opid == RECORD_EQ_OP {
        // Strict, but must check input type to ensure hashable.
        let leftarg = &expr.args[0];
        let info = expr_type_info::call(leftarg)?;
        lsyscache::op_hashjoinable::call(opid, info.typid).map_err(|e| e)
    } else {
        // Look up the operator properties: (oprcanhash, oprcode).
        let (oprcanhash, oprcode) = initext::oper_canhash_code::call(opid)?;
        if !oprcanhash || !lsyscache::func_strict::call(oprcode)? {
            return Ok(false);
        }
        Ok(true)
    }
}

/// `enable_material` (cost.h GUC) — default true. The GUC lives in an unported
/// owner; mirror the boot-time default (the planner only ever reads it).
fn enable_material() -> bool {
    crate::enable_material()
}

/// `relids_add_member` over the planner [`Relids`](types_pathnodes::Relids) set
/// (the `glob.rewindPlanIDs` member type) — the relnode seam.
fn relids_add_member(a: types_pathnodes::Relids, x: i32) -> types_pathnodes::Relids {
    backend_optimizer_util_relnode_seams::relids_add_member::call(a, x)
}

// ===========================================================================
// SS_process_ctes
// ===========================================================================

/// `SS_process_ctes(root)` (subselect.c): process a query's WITH list — ignore
/// unreferenced SELECT CTEs, inline eligible ones, or convert to initplans.
pub fn SS_process_ctes<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
) -> PgResult<()> {
    debug_assert!(root.cte_plan_ids.is_empty());

    // Snapshot the CTE list length; we resolve each CTE by index through the run
    // so the borrow of `root.parse`'s Query stays scoped per-iteration.
    let n_ctes = run.resolve(root.parse).cteList.len();

    for cte_idx in 0..n_ctes {
        // Project the per-CTE scalars we need (refcount, materialized, recursive,
        // commandType, name) by resolving the CTE node + its ctequery.
        let (cterefcount, ctematerialized, cterecursive, cmd_type, ctename) = {
            let parse = run.resolve(root.parse);
            let cte_node = &parse.cteList[cte_idx];
            let cte = match &**cte_node {
                Node::CommonTableExpr(c) => c,
                _ => return Err(elog_error("cteList element is not a CommonTableExpr")),
            };
            let ctequery = cte
                .ctequery
                .as_deref()
                .expect("CTE has no ctequery");
            let cmd_type = ctequery
                .as_query()
                .expect("CTE ctequery is not a Query")
                .commandType;
            let name = cte
                .ctename
                .as_ref()
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            (
                cte.cterefcount,
                cte.ctematerialized,
                cte.cterecursive,
                cmd_type,
                name,
            )
        };

        // Ignore SELECT CTEs that are not actually referenced anywhere.
        if cterefcount == 0 && cmd_type == types_nodes::nodes::CmdType::CMD_SELECT {
            root.cte_plan_ids.push(-1);
            continue;
        }

        // Consider inlining the CTE. Compute the eligibility predicate first
        // (the volatile-functions / DML / outer-selfref checks read the
        // ctequery; clone it as a Node for the read-only walks).
        let may_inline = (ctematerialized
            == types_nodes::rawnodes::CTEMaterializeNever
            || (ctematerialized == types_nodes::rawnodes::CTEMaterializeDefault
                && cterefcount == 1))
            && !cterecursive
            && cmd_type == types_nodes::nodes::CmdType::CMD_SELECT;

        let do_inline = if may_inline {
            // contain_dml / contain_outer_selfref / contain_volatile_functions
            // over the ctequery Query.
            let cte_query_node: Node<'mcx> = {
                let parse = run.resolve(root.parse);
                let cte = match &*parse.cteList[cte_idx] {
                    Node::CommonTableExpr(c) => c,
                    _ => unreachable!(),
                };
                let cq = cte.ctequery.as_deref().unwrap().as_query().unwrap();
                Node::Query(cq.clone_in(mcx)?)
            };
            let dml = contain_dml(&cte_query_node);
            let outer_selfref = if cterefcount <= 1 {
                false
            } else {
                contain_outer_selfref(&cte_query_node)
            };
            // contain_volatile_functions takes Option<&Expr>; the ctequery is a
            // Query, so wrap-and-walk via the Node-level engine: the C calls
            // contain_volatile_functions((Node *) cte->ctequery). The ported
            // clauses helper is over &Expr; the volatile check over a whole Query
            // is reached through eval-const's sibling. We replicate the C result
            // by walking the Query's expression trees for volatility using the
            // node walker + the per-Expr volatility helper.
            let volatile = query_contains_volatile(&cte_query_node)?;
            !dml && !outer_selfref && !volatile
        } else {
            false
        };

        if do_inline {
            inline_cte(mcx, root, run, cte_idx)?;
            root.cte_plan_ids.push(-1);
            continue;
        }

        // Otherwise: plan the CTE as a separately-planned initplan.
        //
        // Copy the source Query node.
        let subquery = {
            let parse = run.resolve(root.parse);
            let cte = match &*parse.cteList[cte_idx] {
                Node::CommonTableExpr(c) => c,
                _ => unreachable!(),
            };
            cte.ctequery
                .as_deref()
                .unwrap()
                .as_query()
                .unwrap()
                .clone_in(mcx)?
        };

        debug_assert!(root.plan_params.is_empty());

        // Generate Paths for the CTE query. Always plan for full retrieval.
        let sub_result = initext::plan_sublink_subquery::call(
            root, run, subquery, cterecursive, 0.0,
        )?;
        let initext::SublinkPlanResult {
            subroot,
            plan,
            subpath,
            subquery_id: _,
        } = sub_result;

        // It should not be possible for the CTE to have requested params here.
        if !root.plan_params.is_empty() {
            return Err(elog_error("unexpected outer reference in CTE query"));
        }

        // Make a SubPlan node for it (just enough unlike build_subplan).
        let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(&plan)?;
        let mut splan: SubPlan<'mcx> = SubPlan {
            subLinkType: SubLinkType::Cte,
            testexpr: None,
            paramIds: PgVec::new_in(mcx),
            plan_id: 0,
            plan_name: None,
            firstColType: first_col_type,
            firstColTypmod: first_col_typmod,
            firstColCollation: first_col_collation,
            useHashTable: false,
            unknownEqFalse: false,
            // CTE scans are not considered for parallelism.
            parallel_safe: false,
            setParam: PgVec::new_in(mcx),
            parParam: PgVec::new_in(mcx),
            args: PgVec::new_in(mcx),
            startup_cost: 0.0,
            per_call_cost: 0.0,
        };

        // Assign a param ID to represent the CTE's output.
        let paramid = backend_optimizer_util_paramassign::assign_special_exec_param(root)?;
        splan.setParam.push(paramid);

        // Compute costs from the plan before interning.
        initext::cost_subplan::call(root, &mut splan, &plan)?;

        // Add the subplan, its path, and its PlannerInfo to the global lists.
        let subpath = subpath.unwrap_or(PathId(0));
        let interned = run.intern_subplan(plan, subroot, subpath);
        {
            let glob = root
                .glob
                .as_mut()
                .expect("SS_process_ctes: root->glob is NULL");
            glob.subplans.push(interned);
            glob.subpaths.push(interned);
            glob.subroots.push(interned);
        }
        let plan_id = run.subplan_len() as i32;
        splan.plan_id = plan_id;

        // Label for EXPLAIN.
        splan.plan_name = Some(PgString::from_str_in(
            &alloc::format!("CTE {ctename}"),
            mcx,
        )?);

        // root->init_plans = lappend(root->init_plans, splan);
        let nid = root.alloc_node(Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(
            subplan_into_static(splan),
        ))));
        root.init_plans.push(nid);
        root.cte_plan_ids.push(plan_id);
    }
    Ok(())
}

/// Walk a Query `Node` for volatile functions in any of its expression trees
/// (`contain_volatile_functions((Node *) cte->ctequery)`). The ported
/// `contain_volatile_functions` helper is over `&Expr`; this drives it over the
/// whole query tree via the node walker, returning `true` on the first volatile
/// expression found.
fn query_contains_volatile(node: &Node<'_>) -> PgResult<bool> {
    let mut found = false;
    let mut err: Option<PgError> = None;
    let mut visit = |n: &Node| -> bool {
        if found || err.is_some() {
            return true;
        }
        if let Node::Expr(e) = n {
            match contain_volatile_functions(Some(e)) {
                Ok(true) => {
                    found = true;
                    return true;
                }
                Ok(false) => {}
                Err(e2) => {
                    err = Some(e2);
                    return true;
                }
            }
        }
        false
    };
    match node {
        Node::Query(q) => {
            backend_nodes_core::node_walker::query_tree_walker(q, &mut visit, 0);
        }
        other => {
            visit(other);
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    Ok(found)
}

// ===========================================================================
// contain_dml / contain_outer_selfref / inline_cte
// ===========================================================================

/// `contain_dml(node)` (subselect.c): is any subquery not a plain SELECT?
fn contain_dml(node: &Node<'_>) -> bool {
    contain_dml_walker(node)
}

/// `contain_dml_walker(node, context)` (subselect.c).
fn contain_dml_walker(node: &Node<'_>) -> bool {
    match node {
        Node::Query(query) => {
            if query.commandType != types_nodes::nodes::CmdType::CMD_SELECT
                || !query.rowMarks.is_empty()
            {
                return true;
            }
            // query_tree_walker(query, contain_dml_walker, context, 0)
            let mut aborted = false;
            let mut visit = |n: &Node| -> bool {
                if aborted {
                    return true;
                }
                if contain_dml_walker(n) {
                    aborted = true;
                    return true;
                }
                false
            };
            backend_nodes_core::node_walker::query_tree_walker(query, &mut visit, 0);
            aborted
        }
        Node::Expr(e) => {
            // expression_tree_walker(node, contain_dml_walker, context):
            // visit children only (a bare Expr can embed a SubLink whose
            // subselect is a Query). Walk via the Node-level engine, which
            // recurses Expr children (and into SubLink subqueries as Query).
            let mut aborted = false;
            let mut visit = |n: &Node| -> bool {
                if aborted {
                    return true;
                }
                if contain_dml_walker(n) {
                    aborted = true;
                    return true;
                }
                false
            };
            backend_nodes_core::node_walker::expression_tree_walker(&Node::Expr(e.clone()), &mut visit)
        }
        _ => false,
    }
}

/// `contain_outer_selfref(node)` (subselect.c): is there an external recursive
/// self-reference?
fn contain_outer_selfref(node: &Node<'_>) -> bool {
    debug_assert!(matches!(node, Node::Query(_)));
    let mut depth: u32 = 0;
    contain_outer_selfref_walker(node, &mut depth)
}

/// `contain_outer_selfref_walker(node, depth)` (subselect.c).
fn contain_outer_selfref_walker(node: &Node<'_>, depth: &mut u32) -> bool {
    match node {
        Node::RangeTblEntry(rte) => {
            // Check for a self-reference to a CTE above the search start.
            if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE
                && rte.self_reference
                && rte.ctelevelsup >= *depth
            {
                return true;
            }
            false
        }
        Node::Query(query) => {
            *depth += 1;
            // query_tree_walker(query, ..., QTW_EXAMINE_RTES_BEFORE). The repo's
            // query_tree_walker does not surface RTE nodes for the EXAMINE flag,
            // so we hand-visit each RTE before descending into its subqueries.
            let mut result = false;
            // Visit RTEs first (EXAMINE_RTES_BEFORE). The walker's RTE arm only
            // inspects rtekind / self_reference / ctelevelsup on an RTE_CTE, so
            // check those scalars inline (no Node wrapping needed).
            for rte in query.rtable.iter() {
                if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE
                    && rte.self_reference
                    && rte.ctelevelsup >= *depth
                {
                    result = true;
                    break;
                }
            }
            if !result {
                // Then the query's own expression trees + subquery RTEs.
                let mut visit = |n: &Node| -> bool {
                    if result {
                        return true;
                    }
                    if contain_outer_selfref_walker(n, depth) {
                        result = true;
                        return true;
                    }
                    false
                };
                backend_nodes_core::node_walker::query_tree_walker(
                    query,
                    &mut visit,
                    backend_nodes_core::node_walker::QTW_EXAMINE_RTES_BEFORE,
                );
            }
            *depth -= 1;
            result
        }
        Node::Expr(e) => {
            let mut result = false;
            let mut visit = |n: &Node| -> bool {
                if result {
                    return true;
                }
                if contain_outer_selfref_walker(n, depth) {
                    result = true;
                    return true;
                }
                false
            };
            backend_nodes_core::node_walker::expression_tree_walker(&Node::Expr(e.clone()), &mut visit);
            result
        }
        _ => false,
    }
}

/// `inline_cte_walker_context` (subselect.c).
struct InlineCteCtx<'mcx> {
    ctename: alloc::string::String,
    levelsup: i64,
    ctequery: types_nodes::copy_query::Query<'mcx>,
}

/// `inline_cte(root, cte)` (subselect.c): convert RTE_CTE references to the
/// given CTE into RTE_SUBQUERYs (mutating `root->parse` in place).
fn inline_cte<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    cte_idx: usize,
) -> PgResult<()> {
    // Extract the CTE's name and query (a deep copy of the ctequery to
    // substitute), then walk root->parse replacing matching RTEs.
    let (ctename, ctequery) = {
        let parse = run.resolve(root.parse);
        let cte = match &*parse.cteList[cte_idx] {
            Node::CommonTableExpr(c) => c,
            _ => return Err(elog_error("cteList element is not a CommonTableExpr")),
        };
        let name = cte
            .ctename
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let cq = cte
            .ctequery
            .as_deref()
            .expect("CTE has no ctequery")
            .as_query()
            .expect("CTE ctequery is not a Query")
            .clone_in(mcx)?;
        (name, cq)
    };

    let mut ctx = InlineCteCtx {
        ctename,
        // Start at levelsup = -1 because we'll immediately increment it.
        levelsup: -1,
        ctequery,
    };

    // (void) inline_cte_walker((Node *) root->parse, &context)
    let parse = run.resolve_mut(root.parse);
    inline_cte_walker_query(mcx, parse, &mut ctx)?;
    Ok(())
}

/// `inline_cte_walker(node, context)` (subselect.c), Query arm. Visits the
/// query's RTE nodes *after* their contents (QTW_EXAMINE_RTES_AFTER) so we don't
/// descend into the newly inlined CTE query.
fn inline_cte_walker_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut types_nodes::copy_query::Query<'mcx>,
    ctx: &mut InlineCteCtx<'mcx>,
) -> PgResult<()> {
    ctx.levelsup += 1;

    // Recurse into subqueries (in the RTEs) BEFORE rewriting this level's RTEs
    // (EXAMINE_RTES_AFTER): first descend into each RTE_SUBQUERY's sub-Query and
    // the query's own expression-borne sublinks, then rewrite the RTE shells.
    //
    // Descend into sub-queries reachable from this query's RTEs.
    for i in 0..query.rtable.len() {
        if query.rtable[i].rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY {
            if let Some(sub) = query.rtable[i].subquery.as_deref_mut() {
                inline_cte_walker_query(mcx, sub, ctx)?;
            }
        }
    }
    // Descend into sublinks embedded in this query's expression trees: any
    // SubLink's subselect is another Query level. Walk the query's expressions
    // mutably and recurse into SubLink subselects.
    inline_cte_walk_query_exprs(mcx, query, ctx)?;

    // Now rewrite this level's RTE_CTE references that match.
    for i in 0..query.rtable.len() {
        let rte = &mut query.rtable[i];
        if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE
            && rte
                .ctename
                .as_ref()
                .map(|s| s.as_str() == ctx.ctename)
                .unwrap_or(false)
            && (rte.ctelevelsup as i64) == ctx.levelsup
        {
            // Found a reference to replace. Generate a copy of the CTE query
            // with appropriate level adjustment for outer references.
            let mut newquery = ctx.ctequery.clone_in(mcx)?;
            if ctx.levelsup > 0 {
                let mut nq_node = Node::Query(newquery);
                backend_rewrite_core::increment::IncrementVarSublevelsUp(
                    &mut nq_node,
                    ctx.levelsup as i32,
                    1,
                )?;
                newquery = match nq_node {
                    Node::Query(q) => q,
                    _ => unreachable!(),
                };
            }
            // Convert the RTE_CTE RTE into an RTE_SUBQUERY.
            rte.rtekind = types_nodes::parsenodes::RTEKind::RTE_SUBQUERY;
            rte.subquery = Some(alloc_in(mcx, newquery)?);
            rte.security_barrier = false;
            // Zero out CTE-specific fields.
            rte.ctename = None;
            rte.ctelevelsup = 0;
            rte.self_reference = false;
            rte.coltypes = PgVec::new_in(mcx);
            rte.coltypmods = PgVec::new_in(mcx);
            rte.colcollations = PgVec::new_in(mcx);
        }
    }

    ctx.levelsup -= 1;
    Ok(())
}

/// Walk a Query's expression trees mutably and recurse into any SubLink
/// subselects (those are nested Query levels for `inline_cte_walker`). Mirrors
/// the expression-tree-walker arm of `inline_cte_walker` for the embedded-Query
/// SubLink children.
fn inline_cte_walk_query_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut types_nodes::copy_query::Query<'mcx>,
    ctx: &mut InlineCteCtx<'mcx>,
) -> PgResult<()> {
    // Collect a mutable visit over the query's expression children. We use the
    // mutable node walker; for each visited Expr that is a SubLink, recurse into
    // its subselect Query (a deeper level for inline_cte_walker).
    let mut err: Option<PgError> = None;
    let mut visit = |n: &mut Node| -> bool {
        if err.is_some() {
            return true;
        }
        if let Node::Expr(e) = n {
            if let Expr::SubLink(sl) = e {
                if let Some(subq) = sl.subselect.as_deref_mut() {
                    // SubLink.subselect is PgBox<'static, Query<'static>>; the
                    // levels walker mutates the embedded Query in place.
                    // SAFETY: the 'static notional lifetime matches the arena
                    // convention; treat as a Query<'mcx> for the recursion.
                    let subq: &mut types_nodes::copy_query::Query<'mcx> =
                        unsafe { core::mem::transmute(subq) };
                    if let Err(e2) = inline_cte_walker_query(mcx, subq, ctx) {
                        err = Some(e2);
                        return true;
                    }
                }
            }
        }
        false
    };
    backend_nodes_core::node_walker::query_tree_mutator(
        query,
        &mut visit,
        backend_nodes_core::node_walker::QTW_IGNORE_RANGE_TABLE,
    );
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ===========================================================================
// simplify_EXISTS_query
// ===========================================================================

/// `simplify_EXISTS_query(root, query)` (subselect.c): remove useless stuff in
/// an EXISTS's subquery. Returns true if it discarded the targetlist.
fn simplify_EXISTS_query<'mcx>(
    root: &mut PlannerInfo,
    mcx: Mcx<'mcx>,
    query: &mut types_nodes::copy_query::Query<'mcx>,
) -> PgResult<bool> {
    if query.commandType != types_nodes::nodes::CmdType::CMD_SELECT
        || query.setOperations.is_some()
        || query.hasAggs
        || !query.groupingSets.is_empty()
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasModifyingCTE
        || query.havingQual.is_some()
        || query.limitOffset.is_some()
        || !query.rowMarks.is_empty()
    {
        return Ok(false);
    }

    // LIMIT with a constant positive (or NULL) value doesn't affect EXISTS.
    if query.limitCount.is_some() {
        let limit_node = query.limitCount.take().unwrap();
        let limit_expr = match PgBox::into_inner(limit_node) {
            Node::Expr(e) => e,
            other => {
                return Err(elog_error(alloc::format!(
                    "simplify_EXISTS_query: limitCount is not an expression node: {:?}",
                    other.node_tag()
                )))
            }
        };
        // eval_const_expressions(root, query->limitCount).
        let folded = initext::eval_const_expressions_expr::call(root, limit_expr)?;
        // Might as well update the query if we simplified the clause.
        let keep = match &folded {
            Expr::Const(limit) => {
                debug_assert!(limit.consttype == INT8OID);
                // !constisnull && DatumGetInt64(constvalue) <= 0  → can't simplify
                !(!limit.constisnull && datum_get_int64(&limit.constvalue) <= 0)
            }
            _ => false,
        };
        if !keep {
            query.limitCount = Some(alloc_in(mcx, Node::Expr(folded))?);
            return Ok(false);
        }
        // We can drop the LIMIT.
        query.limitCount = None;
    }

    // Throw away the targetlist + GROUP / WINDOW / DISTINCT / ORDER BY.
    query.targetList = PgVec::new_in(mcx);
    query.groupClause = PgVec::new_in(mcx);
    query.windowClause = PgVec::new_in(mcx);
    query.distinctClause = PgVec::new_in(mcx);
    query.sortClause = PgVec::new_in(mcx);
    query.hasDistinctOn = false;

    // Remove the RTE_GROUP RTE and clear hasGroupRTE.
    for i in 0..query.rtable.len() {
        if query.rtable[i].rtekind == types_nodes::parsenodes::RTEKind::RTE_GROUP {
            debug_assert!(query.hasGroupRTE);
            query.rtable.remove(i);
            query.hasGroupRTE = false;
            break;
        }
    }

    Ok(true)
}

/// `DatumGetInt64(d)` over the repo's by-value `Datum`.
fn datum_get_int64(d: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> i64 {
    d.as_i64()
}

// ===========================================================================
// convert_EXISTS_to_ANY
// ===========================================================================

/// `contain_vars_of_level((Node *) list, levelsup)` over a slice of `Expr`
/// conjuncts: true if any element references a Var of the given level.
fn list_contain_vars_of_level(list: &[Expr], levelsup: i32) -> bool {
    list.iter()
        .any(|e| backend_optimizer_util_vars::var::contain_vars_of_level(&Node::Expr(e.clone()), levelsup))
}

/// `contain_aggs_of_level((Node *) list, levelsup)` over a slice of `Expr`.
fn list_contain_aggs_of_level(list: &[Expr], levelsup: i32) -> bool {
    list.iter()
        .any(|e| backend_rewrite_core::walkers::contain_aggs_of_level(&Node::Expr(e.clone()), levelsup))
}

/// `convert_EXISTS_to_ANY(root, subselect, &testexpr, &paramIds)` (subselect.c):
/// try to convert EXISTS to a hashable ANY sublink. On success returns
/// `Some((modified_subselect, testexpr, paramIds))`, else `None`.
fn convert_EXISTS_to_ANY<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    mcx: Mcx<'mcx>,
    mut subselect: types_nodes::copy_query::Query<'mcx>,
) -> PgResult<Option<(types_nodes::copy_query::Query<'mcx>, Expr, PgVec<'mcx, i32>)>> {
    // Query must not require a targetlist (caller already dealt with it).
    debug_assert!(subselect.targetList.is_empty());

    // Separate out the WHERE clause.
    let where_clause_node = subselect
        .jointree
        .as_mut()
        .expect("convert_EXISTS_to_ANY: subquery has no jointree")
        .quals
        .take();
    let where_clause: Option<Expr> = match where_clause_node {
        Some(n) => match PgBox::into_inner(n) {
            Node::Expr(e) => Some(e),
            _ => return Err(elog_error("convert_EXISTS_to_ANY: WHERE is not an Expr")),
        },
        None => None,
    };

    // The rest of the sub-select must not refer to any Vars of the parent query.
    if backend_optimizer_util_vars::var::contain_vars_of_level(
        &Node::Query(subselect.clone_in(mcx)?),
        1,
    ) {
        return Ok(None);
    }

    // We don't risk optimizing if the WHERE clause is volatile.
    if contain_volatile_functions(where_clause.as_ref())? {
        return Ok(None);
    }

    // Clean up the WHERE clause: eval_const_expressions, canonicalize_qual,
    // make_ands_implicit.
    let where_clause = match where_clause {
        Some(wc) => initext::eval_const_expressions_expr::call(root, wc)?,
        None => {
            // eval_const_expressions(NULL) is NULL → make_ands_implicit yields [].
            return Ok(None);
        }
    };
    let where_clause =
        backend_optimizer_prep_prepqual::canonicalize_qual(Some(where_clause), false)?;
    let where_list: Vec<Expr> = make_ands_implicit(where_clause);

    // Break the implicit-AND list into "outervar = innervar" hash clauses.
    let mut leftargs: Vec<Expr> = Vec::new();
    let mut rightargs: Vec<Expr> = Vec::new();
    let mut opids: Vec<Oid> = Vec::new();
    let mut opcollations: Vec<Oid> = Vec::new();
    let mut new_where: Vec<Expr> = Vec::new();

    for clause in where_list {
        let mut handled = false;
        if let Expr::OpExpr(op) = &clause {
            if hash_ok_operator(op)? {
                let leftarg = op.args[0].clone();
                let rightarg = op.args[1].clone();
                if backend_optimizer_util_vars::var::contain_vars_of_level(
                    &Node::Expr(leftarg.clone()),
                    1,
                ) {
                    leftargs.push(leftarg);
                    rightargs.push(rightarg);
                    opids.push(op.opno);
                    opcollations.push(op.inputcollid);
                    handled = true;
                } else if backend_optimizer_util_vars::var::contain_vars_of_level(
                    &Node::Expr(rightarg.clone()),
                    1,
                ) {
                    // Commute the clause to put the outer var on the left.
                    let comm = lsyscache::get_commutator::call(op.opno)?;
                    if comm != INVALID_OID {
                        // build a commuted OpExpr and re-check hashability
                        let mut commuted = op.clone();
                        commuted.opno = comm;
                        if hash_ok_operator(&commuted)? {
                            leftargs.push(rightarg);
                            rightargs.push(leftarg);
                            opids.push(comm);
                            opcollations.push(op.inputcollid);
                            handled = true;
                        } else {
                            return Ok(None);
                        }
                    } else {
                        // No commutator: no chance to optimize.
                        return Ok(None);
                    }
                }
            }
        }
        if !handled {
            new_where.push(clause);
        }
    }

    // If we didn't find anything we could convert, fail.
    if leftargs.is_empty() {
        return Ok(None);
    }

    // No parent Vars or Aggs in the stuff we put back into the child query.
    if list_contain_vars_of_level(&new_where, 1) || list_contain_vars_of_level(&rightargs, 1) {
        return Ok(None);
    }
    // root->parse->hasAggs guards the uplevel-Agg check (an uplevel Var could
    // have been optimized away by eval_const_expressions, leaving a bare Agg).
    if run.resolve(root.parse).hasAggs
        && (list_contain_aggs_of_level(&new_where, 1)
            || list_contain_aggs_of_level(&rightargs, 1))
    {
        return Ok(None);
    }

    // No child Vars in the stuff we pull up.
    if list_contain_vars_of_level(&leftargs, 0) {
        return Ok(None);
    }

    // No sublinks in the stuff we pull up.
    {
        // contain_subplans((Node *) leftargs): check each pulled-up left arg.
        let mut has = false;
        for l in &leftargs {
            if contain_subplans(Some(l))? {
                has = true;
                break;
            }
        }
        if has {
            return Ok(None);
        }
    }

    // Adjust the sublevelsup in the stuff we're pulling up.
    {
        let mut adjusted: Vec<Expr> = Vec::with_capacity(leftargs.len());
        for l in leftargs.drain(..) {
            let mut n = Node::Expr(l);
            backend_rewrite_core::increment::IncrementVarSublevelsUp(&mut n, -1, 1)?;
            adjusted.push(match n {
                Node::Expr(e) => e,
                _ => unreachable!(),
            });
        }
        leftargs = adjusted;
    }

    // Put back any child-level-only WHERE clauses.
    if !new_where.is_empty() {
        let explicit = make_ands_explicit(new_where);
        let jt = subselect.jointree.as_mut().unwrap();
        jt.quals = Some(alloc_in(mcx, Node::Expr(explicit))?);
    }

    // Build a new targetlist for the child + a testexpr for the parent.
    let mut tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    let mut testlist: Vec<Expr> = Vec::new();
    let mut paramids: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut resno: i32 = 1;
    for (((leftarg, rightarg), opid), opcollation) in leftargs
        .into_iter()
        .zip(rightargs.into_iter())
        .zip(opids.into_iter())
        .zip(opcollations.into_iter())
    {
        let info = expr_type_info::call(&rightarg)?;
        let param = backend_optimizer_util_paramassign::generate_new_exec_param(
            root,
            info.typid,
            info.typmod,
            info.collation,
        )?;
        tlist.push(make_target_entry(mcx, rightarg, resno as i16, None, false)?);
        resno += 1;
        testlist.push(make_opclause(
            opid,
            BOOLOID,
            false,
            leftarg,
            Some(Expr::Param(param.clone())),
            INVALID_OID,
            opcollation,
        ));
        paramids.push(param.paramid);
    }

    // Put everything where it should go.
    let mut tlist_pg: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> =
        PgVec::new_in(mcx);
    for te in tlist {
        tlist_pg.push(te);
    }
    subselect.targetList = tlist_pg;
    let testexpr = make_ands_explicit(testlist);

    Ok(Some((subselect, testexpr, paramids)))
}

// ===========================================================================
// SS_make_initplan_output_param / SS_make_initplan_from_plan
// ===========================================================================

/// `SS_make_initplan_output_param(root, resulttype, resulttypmod,
/// resultcollation)` (subselect.c): make a Param for an initPlan's output.
pub fn SS_make_initplan_output_param(
    root: &mut PlannerInfo,
    resulttype: Oid,
    resulttypmod: i32,
    resultcollation: Oid,
) -> PgResult<Param> {
    backend_optimizer_util_paramassign::generate_new_exec_param(
        root,
        resulttype,
        resulttypmod,
        resultcollation,
    )
}

/// `SS_make_initplan_from_plan(root, subroot, plan, prm)` (subselect.c): given a
/// plan tree, make it an InitPlan.
pub fn SS_make_initplan_from_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    subroot: PlannerInfo,
    plan: Node<'mcx>,
    prm: &Param,
) -> PgResult<()> {
    // Build the SubPlan node and fill in costs.
    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(&plan)?;
    let plan_parallel_safe = plan.plan_head().parallel_safe;

    let mut node: SubPlan<'mcx> = SubPlan {
        subLinkType: SubLinkType::Expr,
        testexpr: None,
        paramIds: PgVec::new_in(mcx),
        plan_id: 0,
        plan_name: None,
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: false,
        parallel_safe: plan_parallel_safe,
        setParam: PgVec::new_in(mcx),
        parParam: PgVec::new_in(mcx),
        args: PgVec::new_in(mcx),
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };
    node.setParam.push(prm.paramid);

    // Set costs of SubPlan using info from the plan tree (cost_subplan reads the
    // plan; do it before interning).
    initext::cost_subplan::call(root, &mut node, &plan)?;

    // Add the subplan and its PlannerInfo, plus a dummy path entry.
    let interned = run.intern_subplan(plan, subroot, PathId(0));
    {
        let glob = root
            .glob
            .as_mut()
            .expect("SS_make_initplan_from_plan: root->glob is NULL");
        glob.subplans.push(interned);
        glob.subpaths.push(interned);
        glob.subroots.push(interned);
    }
    let plan_id = run.subplan_len() as i32;
    node.plan_id = plan_id;
    node.plan_name = Some(PgString::from_str_in(
        &alloc::format!("InitPlan {plan_id}"),
        mcx,
    )?);

    let nid = root.alloc_node(Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(
        subplan_into_static(node),
    ))));
    root.init_plans.push(nid);
    Ok(())
}
