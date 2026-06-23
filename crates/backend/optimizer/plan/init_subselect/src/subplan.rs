//! SUBPLAN BUILDING (subselect.c) — `make_subplan` / `build_subplan` and their
//! helpers, plus `SS_process_ctes`, the EXISTS-query simplifier /
//! EXISTS→ANY converter, and the `SS_make_initplan_*` entry points.
//!
//! # Model reconciliation (read before editing)
//!
//! The C lower planner (`subquery_planner` → `fetch_upper_rel` →
//! `get_cheapest_fractional_path` → `create_plan`) is reached through the single
//! outward seam [`plan_sublink_subquery`](init_subselect_ext_seams::plan_sublink_subquery),
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
use ::types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use ::nodes::nodes::Node;
use ::nodes::primnodes::{
    Expr, OpExpr, Param, ParamKind, SubLinkType, SubPlan, SubPlanExpr,
};
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{PathId, PlannerInfo};

use ::nodes_core::makefuncs::{
    make_ands_explicit, make_ands_implicit, make_null_const, make_opclause, make_target_entry,
    make_var_from_target_entry,
};
use ::nodeFuncs_seams::expr_type_info;
use init_subselect_ext_seams as initext;
use ::clauses::grounded::{
    contain_exec_param, contain_subplans, contain_volatile_functions,
};
use lsyscache_seams as lsyscache;

use crate::correlation::SS_process_sublinks;

/// `VOIDOID` (pg_type.h).
const VOIDOID: Oid = ::types_core::catalog::VOIDOID;
/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = ::types_core::catalog::BOOLOID;
/// `RECORDOID` (pg_type.h) — the pseudo-type for an anonymous record.
const RECORDOID: Oid = 2249;
/// `INT8OID`.
const INT8OID: Oid = ::types_core::catalog::INT8OID;
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
    orig_subquery: Option<PgBox<'mcx, ::nodes::copy_query::Query<'mcx>>>,
    sub_link_type: SubLinkType,
    sub_link_id: i32,
    testexpr: Option<Expr<'mcx>>,
    is_top_qual: bool,
) -> PgResult<Expr<'mcx>> {
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
                    subplans.push(alloc_in(mcx, *s.0)?);
                } else {
                    unreachable!()
                }
                subplans.push(alloc_in(mcx, *hashplan.0)?);
                // The subplans Vec is already `'mcx`-branded (Mcx-allocated), so
                // build the `AlternativeSubPlan` at `'mcx` and keep `result` at
                // `'mcx` — no lifetime erasure needed (the prior forged-'static
                // transmute is removed by the Expr-'mcx campaign).
                let alt = ::nodes::primnodes::AlternativeSubPlan { subplans };
                result = Expr::AlternativeSubPlan(
                    ::nodes::primnodes::AlternativeSubPlanExpr(alloc::boxed::Box::new(alt)),
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

// ===========================================================================
// build_subplan
// ===========================================================================

/// Outcome of the per-`subLinkType` dispatch in `build_subplan`: either a
/// non-SubPlan replacement expression (Param / Const / converted rowcompare
/// testexpr) or "the result is the SubPlan node itself" (built after interning).
enum ResultKind<'mcx> {
    Expr(Expr<'mcx>),
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
    plan_params: Vec<::pathnodes::NodeId>,
    sub_link_type: SubLinkType,
    sub_link_id: i32,
    testexpr: Option<Expr<'mcx>>,
    testexpr_paramids: Option<PgVec<'mcx, i32>>,
    unknown_eq_false: bool,
) -> PgResult<Expr<'mcx>> {
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
        let prm = paramassign::generate_new_exec_param(
            root, BOOLOID, -1, INVALID_OID,
        )?;
        splan.setParam.push(prm.paramid);
        is_init_plan = true;
        kind = ResultKind::Expr(Expr::Param(prm));
    } else if splan.parParam.is_empty() && sub_link_type == SubLinkType::Expr {
        debug_assert!(testexpr.is_none());
        let (ty, typmod, coll) = first_tlist_type(&plan)?;
        let prm =
            paramassign::generate_new_exec_param(root, ty, typmod, coll)?;
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
        let prm = paramassign::generate_new_exec_param(
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
        let mut param_node_ids: Vec<::pathnodes::NodeId> = Vec::new();
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
            plan = initext::materialize_finished_plan::call(mcx, root, plan)?;
        }

        is_init_plan = false;
        kind = ResultKind::SubPlanNode;
    }

    // ---- shared tail (C: add to glob lists / label / cost_subplan / return) --

    // Compute costs from the plan before handing it to the run.
    initext::cost_subplan::call(root, &mut splan, &plan)?;

    let subpath = subpath.unwrap_or(PathId(0));
    let interned = run.intern_subplan(plan, subroot, subpath);
    // C: splan->plan_id = list_length(root->glob->subplans) AFTER appending. The
    // authoritative 1-based plan_id is `glob.subplans.len()`, NOT
    // `run.subplan_len()`: the two normally agree, but the lifted MIN/MAX-agg
    // optimization (build_minmax_agg_paths) can leave an *un-attached* subplan in
    // the run's value store (interned but not pushed onto glob.subplans, because
    // its MinMaxAggPath ultimately lost to a plain Agg). Counting glob.subplans
    // ignores those orphans, matching C exactly.
    let plan_id = {
        let glob = root
            .glob
            .as_mut()
            .expect("build_subplan: root->glob is NULL");
        glob.subplans.push(interned);
        glob.subpaths.push(interned);
        glob.subroots.push(interned);
        glob.subplans.len() as i32
    };
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
                // `alloc_node` interns the Expr into the planner node arena
                // (erasing the `'mcx` lifetime to the arena's notional 'static);
                // `splan` is already `'mcx`-branded, so pass it straight through.
                let nid = root.alloc_node(Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(splan))));
                root.init_plans.push(nid);
            }
            e
        }
        ResultKind::SubPlanNode => {
            debug_assert!(!is_init_plan);
            Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(splan)))
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
fn generate_subquery_params<'mcx>(
    root: &mut PlannerInfo,
    plan: &Node<'_>,
) -> PgResult<(Vec<Expr<'mcx>>, Vec<i32>)> {
    let head = plan.plan_head();
    let mut result: Vec<Expr<'mcx>> = Vec::new();
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
            let param = paramassign::generate_new_exec_param(
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
    tlist: &[::nodes::primnodes::TargetEntry<'mcx>],
    varno: ::types_core::primitive::Index,
) -> PgResult<Vec<Expr<'mcx>>> {
    let mut result: Vec<Expr<'mcx>> = Vec::new();
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
    testexpr: Expr<'mcx>,
    subst_nodes: &[Expr<'mcx>],
) -> PgResult<Expr<'mcx>> {
    convert_testexpr_mutator(testexpr, subst_nodes)
}

/// `convert_testexpr_mutator(node, context)` (subselect.c).
fn convert_testexpr_mutator<'mcx>(node: Expr<'mcx>, subst_nodes: &[Expr<'mcx>]) -> PgResult<Expr<'mcx>> {
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
    let result = ::nodes_core::nodefuncs::expression_tree_mutator(node, &mut |child: Expr| {
        if err.is_some() {
            return child;
        }
        match convert_testexpr_mutator(child, subst_nodes) {
            Ok(c) => c,
            Err(e) => {
                err = Some(e);
                Expr::Const(::nodes_core::makefuncs::make_bool_const(true, false))
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
            if b.boolop == ::nodes::primnodes::BoolExprType::AND_EXPR =>
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
    // Deep-copy the operand for the read-only walker via `Expr::clone_in` into a
    // transient scratch context (a derived `Expr::clone` panics on a
    // context-allocated child).
    let cx = ::mcx::MemoryContext::new("test_opexpr_is_hashable scratch");
    let arg1_node = Node::mk_expr(cx.mcx(), testexpr.args[1].clone_in(cx.mcx())?)?;
    if vars::var::contain_var_clause(&arg1_node) {
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

/// `relids_add_member` over the planner [`Relids`](::pathnodes::Relids) set
/// (the `glob.rewindPlanIDs` member type) — the relnode seam.
fn relids_add_member(a: ::pathnodes::Relids, x: i32) -> ::pathnodes::Relids {
    relnode_seams::relids_add_member::call(a, x)
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
            let cte = match cte_node.as_commontableexpr() {
                Some(c) => c,
                None => return Err(elog_error("cteList element is not a CommonTableExpr")),
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
        if cterefcount == 0 && cmd_type == ::nodes::nodes::CmdType::CMD_SELECT {
            root.cte_plan_ids.push(-1);
            continue;
        }

        // Consider inlining the CTE. Compute the eligibility predicate first
        // (the volatile-functions / DML / outer-selfref checks read the
        // ctequery; clone it as a Node for the read-only walks).
        let may_inline = (ctematerialized
            == ::nodes::rawnodes::CTEMaterializeNever
            || (ctematerialized == ::nodes::rawnodes::CTEMaterializeDefault
                && cterefcount == 1))
            && !cterecursive
            && cmd_type == ::nodes::nodes::CmdType::CMD_SELECT;

        let do_inline = if may_inline {
            // contain_dml / contain_outer_selfref / contain_volatile_functions
            // over the ctequery Query.
            let cte_query_node: Node<'mcx> = {
                let parse = run.resolve(root.parse);
                let cte = match parse.cteList[cte_idx].as_commontableexpr() {
                    Some(c) => c,
                    None => unreachable!(),
                };
                let cq = cte.ctequery.as_deref().unwrap().as_query().unwrap();
                Node::mk_query(mcx, cq.clone_in(mcx)?)?
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
            let cte = match parse.cteList[cte_idx].as_commontableexpr() {
                Some(c) => c,
                None => unreachable!(),
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
        let paramid = paramassign::assign_special_exec_param(root)?;
        splan.setParam.push(paramid);

        // Compute costs from the plan before interning.
        initext::cost_subplan::call(root, &mut splan, &plan)?;

        // Add the subplan, its path, and its PlannerInfo to the global lists.
        // plan_id = list_length(glob->subplans) after appending (C semantics);
        // count glob.subplans, not run.subplan_len(), so an un-attached MIN/MAX-agg
        // subplan in the run value store can't inflate the number.
        let subpath = subpath.unwrap_or(PathId(0));
        let interned = run.intern_subplan(plan, subroot, subpath);
        let plan_id = {
            let glob = root
                .glob
                .as_mut()
                .expect("SS_process_ctes: root->glob is NULL");
            glob.subplans.push(interned);
            glob.subpaths.push(interned);
            glob.subroots.push(interned);
            glob.subplans.len() as i32
        };
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
    query_contains_volatile_node(node)
}

/// Dispatch a `Node` reached during the volatile scan of a CTE query: a nested
/// `Query` (e.g. a FROM-subquery RTE handed up by `range_table_entry_walker`)
/// recurses via `query_tree_walker` — mirroring C's `IsA(node, Query)` arm of
/// `contain_volatile_functions_walker` — while every other Node is an `Expr` and
/// runs the per-Expr volatility check. Without the Query recursion a volatile
/// buried in an inner subquery RTE (e.g. `with x as (select * from (select
/// f1, random() from t) ss)`) would be missed, wrongly inlining the CTE.
fn query_contains_volatile_node(node: &Node<'_>) -> PgResult<bool> {
    if let Some(q) = node.as_query() {
        let mut found = false;
        let mut err: Option<PgError> = None;
        let mut visit = |n: &Node| -> bool {
            if found || err.is_some() {
                return true;
            }
            match query_contains_volatile_node(n) {
                Ok(true) => {
                    found = true;
                    true
                }
                Ok(false) => false,
                Err(e) => {
                    err = Some(e);
                    true
                }
            }
        };
        ::nodes_core::node_walker::query_tree_walker(q, &mut visit, 0);
        if let Some(e) = err {
            return Err(e);
        }
        return Ok(found);
    }
    if let Some(e) = node.as_expr() {
        return contain_volatile_functions(Some(e));
    }
    Ok(false)
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
    if let Some(query) = node.as_query() {
        if query.commandType != ::nodes::nodes::CmdType::CMD_SELECT
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
        ::nodes_core::node_walker::query_tree_walker(query, &mut visit, 0);
        return aborted;
    }
    if let Some(e) = node.as_expr() {
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
        // Deep-copy via `Expr::clone_in` into a transient scratch context for
        // the read-only walker (a derived `Expr::clone` panics on a
        // context-allocated child such as a SubLink).
        let cx = ::mcx::MemoryContext::new("contain_dml_walker scratch");
        let owned = match e.clone_in(cx.mcx()) {
            Ok(o) => o,
            Err(_) => return true,
        };
        let e_node = match Node::mk_expr(cx.mcx(), owned) {
            Ok(n) => n,
            Err(_) => return true,
        };
        return ::nodes_core::node_walker::expression_tree_walker(&e_node, &mut visit);
    }
    false
}

/// `contain_outer_selfref(node)` (subselect.c): is there an external recursive
/// self-reference?
fn contain_outer_selfref(node: &Node<'_>) -> bool {
    debug_assert!(node.node_tag() == ::nodes::nodes::ntag::T_Query);
    let mut depth: u32 = 0;
    contain_outer_selfref_walker(node, &mut depth)
}

/// `contain_outer_selfref_walker(node, depth)` (subselect.c).
fn contain_outer_selfref_walker(node: &Node<'_>, depth: &mut u32) -> bool {
    if let Some(rte) = node.as_rangetblentry() {
        // Check for a self-reference to a CTE above the search start.
        if rte.rtekind == ::nodes::parsenodes::RTEKind::RTE_CTE
            && rte.self_reference
            && rte.ctelevelsup >= *depth
        {
            return true;
        }
        return false;
    }
    if let Some(query) = node.as_query() {
        {
            *depth += 1;
            // query_tree_walker(query, ..., QTW_EXAMINE_RTES_BEFORE). The repo's
            // query_tree_walker does not surface RTE nodes for the EXAMINE flag,
            // so we hand-visit each RTE before descending into its subqueries.
            let mut result = false;
            // Visit RTEs first (EXAMINE_RTES_BEFORE). The walker's RTE arm only
            // inspects rtekind / self_reference / ctelevelsup on an RTE_CTE, so
            // check those scalars inline (no Node wrapping needed).
            for rte in query.rtable.iter() {
                if rte.rtekind == ::nodes::parsenodes::RTEKind::RTE_CTE
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
                ::nodes_core::node_walker::query_tree_walker(
                    query,
                    &mut visit,
                    ::nodes_core::node_walker::QTW_EXAMINE_RTES_BEFORE,
                );
            }
            *depth -= 1;
            return result;
        }
    }
    if let Some(e) = node.as_expr() {
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
        // Deep-copy via `Expr::clone_in` into a transient scratch context for
        // the read-only walker (a derived `Expr::clone` panics on a
        // context-allocated child).
        let cx = ::mcx::MemoryContext::new("contain_outer_selfref_walker scratch");
        if let Ok(owned) = e.clone_in(cx.mcx()) {
            if let Ok(e_node) = Node::mk_expr(cx.mcx(), owned) {
                ::nodes_core::node_walker::expression_tree_walker(&e_node, &mut visit);
            }
        }
        return result;
    }
    false
}

/// `inline_cte_walker_context` (subselect.c).
struct InlineCteCtx<'mcx> {
    ctename: alloc::string::String,
    levelsup: i64,
    ctequery: ::nodes::copy_query::Query<'mcx>,
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
        let cte = match parse.cteList[cte_idx].as_commontableexpr() {
            Some(c) => c,
            None => return Err(elog_error("cteList element is not a CommonTableExpr")),
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
    query: &mut ::nodes::copy_query::Query<'mcx>,
    ctx: &mut InlineCteCtx<'mcx>,
) -> PgResult<()> {
    ctx.levelsup += 1;

    // Recurse into subqueries (in the RTEs) BEFORE rewriting this level's RTEs
    // (EXAMINE_RTES_AFTER): first descend into each RTE_SUBQUERY's sub-Query and
    // the query's own expression-borne sublinks, then rewrite the RTE shells.
    //
    // Descend into sub-queries reachable from this query's RTEs.
    for i in 0..query.rtable.len() {
        if query.rtable[i].rtekind == ::nodes::parsenodes::RTEKind::RTE_SUBQUERY {
            if let Some(sub) = query.rtable[i].subquery.as_deref_mut() {
                inline_cte_walker_query(mcx, sub, ctx)?;
            }
        }
    }
    // Descend into sublinks embedded in this query's expression trees: any
    // SubLink's subselect is another Query level. Walk the query's expressions
    // mutably and recurse into SubLink subselects.
    inline_cte_walk_query_exprs(mcx, query, ctx)?;

    // Descend into the queries of *this level's* CTEs. C's query_tree_walker
    // does `WALK(query->cteList)`, and the CommonTableExpr walker arm recurses
    // into `cte->ctequery` — another Query level — so a reference to the CTE
    // being inlined that appears inside a *sibling* CTE's query gets rewritten
    // too (the `cterefcount==1` default-materialize inline path depends on it).
    for i in 0..query.cteList.len() {
        let cq_opt: Option<&mut ::nodes::copy_query::Query<'mcx>> = query.cteList[i]
            .as_commontableexpr_mut()
            .and_then(|c| c.ctequery.as_deref_mut())
            .and_then(|n| n.as_query_mut());
        if let Some(cq) = cq_opt {
            inline_cte_walker_query(mcx, cq, ctx)?;
        }
    }

    // Now rewrite this level's RTE_CTE references that match.
    for i in 0..query.rtable.len() {
        let rte = &mut query.rtable[i];
        if rte.rtekind == ::nodes::parsenodes::RTEKind::RTE_CTE
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
                let mut nq_node = Node::mk_query(mcx, newquery)?;
                rewrite_core::increment::IncrementVarSublevelsUp(
                    &mut nq_node,
                    ctx.levelsup as i32,
                    1,
                    mcx,
                )?;
                newquery = nq_node.into_query().expect("expected Query node");
            }
            // Convert the RTE_CTE RTE into an RTE_SUBQUERY.
            rte.rtekind = ::nodes::parsenodes::RTEKind::RTE_SUBQUERY;
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
    query: &mut ::nodes::copy_query::Query<'mcx>,
    ctx: &mut InlineCteCtx<'mcx>,
) -> PgResult<()> {
    // Walk this query's expression trees mutably. C's `inline_cte_walker` is a
    // `query_or_expression_tree_walker`: at every node it does the default
    // recursion into all children, and a SubLink's subselect (a nested Query
    // level) is reached because the default walker recurses into it. The mutator
    // we drive here calls `visit` on each top-level expression but does NOT, on
    // its own, descend into a node's grandchildren — so `visit` must itself
    // recurse into the node's expression children, otherwise a SubLink buried
    // inside an Aggref / FuncExpr / etc. is never seen. We therefore mirror the
    // `IncrementVarSublevelsUp_walker` shape: handle the SubLink arm (recurse
    // into its subselect as a new Query level), and for every other node recurse
    // into its expression children via `expression_tree_walker_mut`.
    let mut err: Option<PgError> = None;
    fn visit_node<'mcx>(
        n: &mut Node<'mcx>,
        ctx: &mut InlineCteCtx<'mcx>,
        err: &mut Option<PgError>,
        mcx: Mcx<'mcx>,
    ) -> bool {
        if err.is_some() {
            return true;
        }
        if let Some(Expr::SubLink(sl)) = n.as_expr_mut() {
            // Walk the subselect ourselves (a new Query level) and detach it so
            // the default `expression_tree_walker_mut` below does not descend
            // into it a second time at the wrong level; it still walks testexpr.
            let saved = sl.subselect.take();
            if let Some(mut sub_box) = saved {
                {
                    let subq: &mut ::nodes::copy_query::Query<'mcx> =
                        unsafe { core::mem::transmute(&mut *sub_box) };
                    if let Err(e2) = inline_cte_walker_query(mcx, subq, ctx) {
                        // restore before aborting
                        if let Some(Expr::SubLink(sl)) = n.as_expr_mut() {
                            sl.subselect = Some(sub_box);
                        }
                        *err = Some(e2);
                        return true;
                    }
                }
                if let Some(Expr::SubLink(sl)) = n.as_expr_mut() {
                    sl.subselect = Some(sub_box);
                }
            }
            // Recurse into the SubLink's testexpr children (same query level).
            return ::nodes_core::node_walker::expression_tree_walker_mut(
                n,
                &mut |c| visit_node(c, ctx, err, mcx),
                mcx,
            );
        }
        // Any other node: recurse into its expression children so SubLinks
        // nested inside (Aggref args, FuncExpr args, CaseExpr, ...) are reached.
        ::nodes_core::node_walker::expression_tree_walker_mut(
            n,
            &mut |c| visit_node(c, ctx, err, mcx),
            mcx,
        )
    }
    ::nodes_core::node_walker::query_tree_mutator(
        query,
        &mut |n| visit_node(n, ctx, &mut err, mcx),
        ::nodes_core::node_walker::QTW_IGNORE_RANGE_TABLE,
        mcx,
    );

    // C's `inline_cte_walker` is a `query_or_expression_tree_walker` that does
    // NOT set QTW_IGNORE_RANGE_TABLE, so `range_table_walker` also visits each
    // RTE's *expression* fields at the current query level: an RTE_VALUES's
    // `values_lists`, an RTE_FUNCTION's `functions`, and an RTE_TABLEFUNC's
    // `tablefunc`. A SubLink referencing the CTE can live there — e.g.
    // `WITH cte AS (...) VALUES ((SELECT ... FROM cte))`, where the sublink sits
    // in the top query's VALUES RTE rather than its targetList. The owned port
    // walks RTE_SUBQUERY sub-Queries explicitly (as new levels) and otherwise
    // ignores the range table here, so these same-level RTE expressions must be
    // walked too, or the buried SubLink's CteScan is never rewritten and a
    // surviving CTE reference trips "no plan was made for CTE".
    if err.is_none() {
        for i in 0..query.rtable.len() {
            match query.rtable[i].rtekind {
                ::nodes::parsenodes::RTEKind::RTE_VALUES => {
                    for j in 0..query.rtable[i].values_lists.len() {
                        if err.is_some() {
                            break;
                        }
                        let n: &mut Node<'mcx> = &mut query.rtable[i].values_lists[j];
                        visit_node(n, ctx, &mut err, mcx);
                    }
                }
                ::nodes::parsenodes::RTEKind::RTE_FUNCTION => {
                    for j in 0..query.rtable[i].functions.len() {
                        if err.is_some() {
                            break;
                        }
                        let n: &mut Node<'mcx> = &mut query.rtable[i].functions[j];
                        visit_node(n, ctx, &mut err, mcx);
                    }
                }
                ::nodes::parsenodes::RTEKind::RTE_TABLEFUNC => {
                    if let Some(tf) = query.rtable[i].tablefunc.as_mut() {
                        let n: &mut Node<'mcx> = &mut *tf;
                        visit_node(n, ctx, &mut err, mcx);
                    }
                }
                _ => {}
            }
        }
    }

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
    query: &mut ::nodes::copy_query::Query<'mcx>,
) -> PgResult<bool> {
    if query.commandType != ::nodes::nodes::CmdType::CMD_SELECT
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
        // `limitCount` is the concretely-typed `Option<PgBox<Expr>>` view.
        let limit_expr = PgBox::into_inner(limit_node);
        // eval_const_expressions(root, query->limitCount).
        let folded = initext::eval_const_expressions_expr::call(mcx, limit_expr)?;
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
            query.limitCount = Some(alloc_in(mcx, folded)?);
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
        if query.rtable[i].rtekind == ::nodes::parsenodes::RTEKind::RTE_GROUP {
            debug_assert!(query.hasGroupRTE);
            query.rtable.remove(i);
            query.hasGroupRTE = false;
            break;
        }
    }

    Ok(true)
}

/// `DatumGetInt64(d)` over the repo's by-value `Datum`.
fn datum_get_int64(d: &types_tuple::heaptuple::Datum<'_>) -> i64 {
    d.as_i64()
}

// ===========================================================================
// convert_EXISTS_to_ANY
// ===========================================================================

/// `contain_vars_of_level((Node *) list, levelsup)` over a slice of `Expr`
/// conjuncts: true if any element references a Var of the given level.
fn list_contain_vars_of_level(list: &[Expr], levelsup: i32) -> bool {
    // The read-only walker needs an owned `Node`; deep-copy each conjunct via
    // `Expr::clone_in` into a transient scratch context (a derived `Expr::clone`
    // panics on a context-allocated child such as a SubLink).
    let cx = ::mcx::MemoryContext::new("list_contain_vars_of_level scratch");
    list.iter().any(|e| {
        let Ok(owned) = e.clone_in(cx.mcx()) else { return false };
        match Node::mk_expr(cx.mcx(), owned) {
            Ok(n) => vars::var::contain_vars_of_level(&n, levelsup),
            Err(_) => false,
        }
    })
}

/// `contain_aggs_of_level((Node *) list, levelsup)` over a slice of `Expr`.
fn list_contain_aggs_of_level(list: &[Expr], levelsup: i32) -> bool {
    let cx = ::mcx::MemoryContext::new("list_contain_aggs_of_level scratch");
    list.iter().any(|e| {
        let Ok(owned) = e.clone_in(cx.mcx()) else { return false };
        match Node::mk_expr(cx.mcx(), owned) {
            Ok(n) => rewrite_core::walkers::contain_aggs_of_level(&n, levelsup),
            Err(_) => false,
        }
    })
}

/// `convert_EXISTS_to_ANY(root, subselect, &testexpr, &paramIds)` (subselect.c):
/// try to convert EXISTS to a hashable ANY sublink. On success returns
/// `Some((modified_subselect, testexpr, paramIds))`, else `None`.
fn convert_EXISTS_to_ANY<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    mcx: Mcx<'mcx>,
    mut subselect: ::nodes::copy_query::Query<'mcx>,
) -> PgResult<Option<(::nodes::copy_query::Query<'mcx>, Expr<'mcx>, PgVec<'mcx, i32>)>> {
    // Query must not require a targetlist (caller already dealt with it).
    debug_assert!(subselect.targetList.is_empty());

    // Separate out the WHERE clause.
    let where_clause_node = subselect
        .jointree
        .as_mut()
        .expect("convert_EXISTS_to_ANY: subquery has no jointree")
        .quals
        .take();
    let where_clause: Option<Expr<'mcx>> = match where_clause_node {
        Some(n) => match PgBox::into_inner(n).into_expr() {
            Some(e) => Some(e),
            None => return Err(elog_error("convert_EXISTS_to_ANY: WHERE is not an Expr")),
        },
        None => None,
    };

    // The rest of the sub-select must not refer to any Vars of the parent query.
    if vars::var::contain_vars_of_level(
        &Node::mk_query(mcx, subselect.clone_in(mcx)?)?,
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
        Some(wc) => initext::eval_const_expressions_expr::call(mcx, wc)?,
        None => {
            // eval_const_expressions(NULL) is NULL → make_ands_implicit yields [].
            return Ok(None);
        }
    };
    let where_clause =
        prepqual::canonicalize_qual(mcx, Some(where_clause), false)?;
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
                // Deep-copy the operands via `Expr::clone_in` (they are moved
                // into leftargs/rightargs and new nodes; a derived `Expr::clone`
                // panics on a context-allocated child).
                let leftarg = op.args[0].clone_in(mcx)?;
                let rightarg = op.args[1].clone_in(mcx)?;
                if vars::var::contain_vars_of_level(
                    &Node::mk_expr(mcx, leftarg.clone_in(mcx)?)?,
                    1,
                ) {
                    leftargs.push(leftarg);
                    rightargs.push(rightarg);
                    opids.push(op.opno);
                    opcollations.push(op.inputcollid);
                    handled = true;
                } else if vars::var::contain_vars_of_level(
                    &Node::mk_expr(mcx, rightarg.clone_in(mcx)?)?,
                    1,
                ) {
                    // Commute the clause to put the outer var on the left.
                    let comm = lsyscache::get_commutator::call(op.opno)?;
                    if comm != INVALID_OID {
                        // build a commuted OpExpr and re-check hashability.
                        // Deep-copy via `Expr::clone_in` (a derived `.clone()`
                        // panics on a context-allocated child operand).
                        let mut commuted = match clause.clone_in(mcx)? {
                            Expr::OpExpr(o) => o,
                            _ => unreachable!("clause matched Expr::OpExpr above"),
                        };
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
            let mut n = Node::mk_expr(mcx, l)?;
            rewrite_core::increment::IncrementVarSublevelsUp(&mut n, -1, 1, mcx)?;
            adjusted.push(n.into_expr().expect("expected Expr node"));
        }
        leftargs = adjusted;
    }

    // Put back any child-level-only WHERE clauses.
    if !new_where.is_empty() {
        let explicit = make_ands_explicit(new_where);
        let jt = subselect.jointree.as_mut().unwrap();
        jt.quals = Some(alloc_in(mcx, Node::mk_expr(mcx, explicit)?)?);
    }

    // Build a new targetlist for the child + a testexpr for the parent.
    let mut tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
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
        let param = paramassign::generate_new_exec_param(
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
    let mut tlist_pg: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
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
    paramassign::generate_new_exec_param(
        root,
        resulttype,
        resulttypmod,
        resultcollation,
    )
}

/// Shared core of `SS_make_initplan_from_plan`: build the `SubPlan` node from a
/// finished plan tree, intern the plan/subroot/subpath into the run's value
/// store, and return the `SubPlan` [`::pathnodes::NodeId`] in `root`'s
/// node_arena (WITHOUT appending it to `root.init_plans` OR to `glob.subplans`).
///
/// Public so planagg's `build_minmax_agg_paths` (planner crate) can pre-build the
/// per-aggregate InitPlan `SubPlan` at preprocess time, where it holds the
/// `&mut PlannerRun` the intern needs (`create_minmaxagg_plan` runs under
/// `create_plan`, which only has `&PlannerRun`). The caller stashes the returned
/// `(NodeId, PlanId)` on the `MinMaxAggInfo`; `create_minmaxagg_plan` then both
/// (a) appends the `Plan` tree to `glob.subplans` and assigns the SubPlan's
/// 1-based `plan_id` + `InitPlan N` name, and (b) appends the SubPlan to
/// `root.init_plans` — but only once the MinMaxAggPath has actually won. This
/// matches C, where `SS_make_initplan_from_plan` (which does the `lappend` and
/// `plan_id = list_length(glob->subplans)`) is called *from* `create_minmaxagg_plan`,
/// so a losing MIN/MAX optimization never reserves a `glob.subplans` slot and never
/// inflates a sibling InitPlan/SubPlan's `plan_id`.
pub fn build_initplan_subplan_node<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    subroot: PlannerInfo,
    plan: Node<'mcx>,
    prm: &Param,
) -> PgResult<(::pathnodes::NodeId, ::pathnodes::PlanId)> {
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

    // Intern the InitPlan `Plan` tree (+ its PlannerInfo and a dummy path entry)
    // into the run's value store, reserving its `PlanId` handle. We deliberately
    // do NOT push it onto `glob.subplans` here, and leave `plan_id`/`plan_name`
    // unset: that attach + numbering happens in `create_minmaxagg_plan` iff the
    // MinMaxAggPath wins (mirroring C, where the `lappend(glob->subplans, ...)` +
    // `plan_id = list_length(glob->subplans)` live inside `SS_make_initplan_from_plan`,
    // called from `create_minmaxagg_plan`). If the optimization loses, the run
    // value store simply holds an unreferenced entry — harmless, and crucially it
    // never occupies a slot in the numbered `glob.subplans` list, so it cannot
    // bump a sibling InitPlan/SubPlan's 1-based `plan_id`.
    let interned = run.intern_subplan(plan, subroot, PathId(0));

    Ok((
        root.alloc_node(Expr::SubPlan(SubPlanExpr(alloc::boxed::Box::new(
            subplan_into_static(node),
        )))),
        interned,
    ))
}

/// `SS_make_initplan_from_plan(root, subroot, plan, prm)` (subselect.c): given a
/// plan tree, make it an InitPlan and attach it to the current query level —
/// `glob->subplans = lappend(glob->subplans, plan); splan->plan_id =
/// list_length(glob->subplans); plan_name = "InitPlan N"; root->init_plans =
/// lappend(root->init_plans, splan)`. (Unlike the lifted MIN/MAX-agg path, this
/// caller knows the InitPlan is kept, so it attaches eagerly.)
pub fn SS_make_initplan_from_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    subroot: PlannerInfo,
    plan: Node<'mcx>,
    prm: &Param,
) -> PgResult<()> {
    let (nid, pid) = build_initplan_subplan_node(mcx, root, run, subroot, plan, prm)?;
    // Attach to glob.subplans + assign the 1-based plan_id / "InitPlan N" name.
    let plan_id = {
        let glob = root
            .glob
            .as_mut()
            .expect("SS_make_initplan_from_plan: root->glob is NULL");
        glob.subplans.push(pid);
        glob.subpaths.push(pid);
        glob.subroots.push(pid);
        glob.subplans.len() as i32
    };
    // The arena SubPlan is `Expr<'static>`; the name is allocated in the planner
    // `mcx` (which outlives the run), so erase its lifetime to match — the same
    // lifetime-only transmute the subplan store relies on (`subplan_into_static`).
    let plan_name: PgString<'static> = {
        let s = PgString::from_str_in(&alloc::format!("InitPlan {plan_id}"), mcx)?;
        // SAFETY: lifetime-parameter-only transmute of an owned PgString whose
        // backing bytes live in the planner memory context for the run's life.
        unsafe { core::mem::transmute::<PgString<'_>, PgString<'static>>(s) }
    };
    if let Expr::SubPlan(s) = root.node_mut(nid) {
        s.0.plan_id = plan_id;
        s.0.plan_name = Some(plan_name);
    }
    root.init_plans.push(nid);
    Ok(())
}

// ===========================================================================
// resolve_cte_subplan / resolve_worktable_param
//   (the SubPlan-init resolution legs of create_ctescan_plan /
//   create_worktablescan_plan, createplan.c:3884 / :4055). These dereference
//   the init SubPlans / `wt_param_id` built here by SS_process_ctes, so
//   subselect.c (this unit) owns them and installs them into createplan-seams.
// ===========================================================================

/// `create_ctescan_plan`'s CTE-`SubPlan` resolution leg (createplan.c:3884):
/// walk `cteroot->parse->cteList` to find the referenced CTE's index, read its
/// `plan_id` from `cteroot->cte_plan_ids`, locate the matching init `SubPlan` in
/// `cteroot->init_plans`, and return `(plan_id, cte_param_id)` where
/// `cte_param_id = linitial_int(ctesplan->setParam)`.
pub fn resolve_cte_subplan<'mcx>(
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    scan_relid: u32,
) -> PgResult<(i32, i32)> {
    debug_assert!(scan_relid > 0);
    let rte = ::pathnodes::planner_run::planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, ::nodes::parsenodes::RTEKind::RTE_CTE);
    debug_assert!(!rte.self_reference);

    let ctename = rte
        .ctename
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // levelsup = rte->ctelevelsup; cteroot = root; while (levelsup-- > 0) cteroot
    // = cteroot->parent_root.
    let mut cteroot: &PlannerInfo = root;
    let mut levelsup = rte.ctelevelsup;
    while levelsup > 0 {
        levelsup -= 1;
        cteroot = cteroot
            .parent_root
            .as_deref()
            .ok_or_else(|| elog_error(alloc::format!("bad levelsup for CTE \"{ctename}\"")))?;
    }

    // ndx = index of the matching CTE in cteroot->parse->cteList.
    let mut ndx: usize = 0;
    let mut found = false;
    {
        let parse = run.resolve(cteroot.parse);
        for cte_node in parse.cteList.iter() {
            let cte = match cte_node.as_commontableexpr() {
                Some(c) => c,
                None => return Err(elog_error("cteList element is not a CommonTableExpr")),
            };
            let this_name = cte.ctename.as_ref().map(|s| s.as_str()).unwrap_or("");
            if this_name == ctename {
                found = true;
                break;
            }
            ndx += 1;
        }
    }
    if !found {
        return Err(elog_error(alloc::format!(
            "could not find CTE \"{ctename}\""
        )));
    }
    if ndx >= cteroot.cte_plan_ids.len() {
        return Err(elog_error(alloc::format!(
            "could not find plan for CTE \"{ctename}\""
        )));
    }
    let plan_id = cteroot.cte_plan_ids[ndx];
    if plan_id <= 0 {
        return Err(elog_error(alloc::format!(
            "no plan was made for CTE \"{ctename}\""
        )));
    }

    // Find the init SubPlan whose plan_id matches; cte_param_id =
    // linitial_int(ctesplan->setParam).
    for &ipl in &cteroot.init_plans {
        if let Expr::SubPlan(splan) = cteroot.node(ipl) {
            if splan.0.plan_id == plan_id {
                let cte_param_id = *splan
                    .0
                    .setParam
                    .first()
                    .ok_or_else(|| elog_error("CTE SubPlan has empty setParam"))?;
                return Ok((plan_id, cte_param_id));
            }
        }
    }
    Err(elog_error(alloc::format!(
        "could not find plan for CTE \"{ctename}\""
    )))
}

/// `create_worktablescan_plan`'s work-table-`Param` resolution leg
/// (createplan.c:4055): walk to the plan level one below where the CTE comes
/// from and return its `cteroot->wt_param_id`.
pub fn resolve_worktable_param(
    root: &PlannerInfo,
    _run: &PlannerRun<'_>,
    scan_relid: u32,
) -> PgResult<i32> {
    debug_assert!(scan_relid > 0);
    let rte = ::pathnodes::planner_run::planner_rt_fetch(_run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, ::nodes::parsenodes::RTEKind::RTE_CTE);
    debug_assert!(rte.self_reference);

    let ctename = rte
        .ctename
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // C (createplan.c:4055):
    //   levelsup = rte->ctelevelsup;
    //   if (levelsup == 0)  /* shouldn't happen */
    //       elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
    //   levelsup--;
    //   cteroot = root;
    //   while (levelsup-- > 0) { cteroot = cteroot->parent_root; if (!cteroot) ... }
    //   return cteroot->wt_param_id;
    //
    // The work-table PARAM_EXEC id lives on the recursion-planning root (the
    // level processing the recursive UNION, one level below where the CTE comes
    // from). In this owned PlannerInfo model the recursive term's leaf subroot
    // does NOT retain its `parent_root` at create-plan time (it was taken back
    // out after the leaf was planned — PlannerInfo is not `Clone`). But the same
    // `wt_param_id` was stamped onto this leaf subroot when its access paths were
    // built (recursion_carry in subquery_planner_carried), so when the
    // parent_root walk cannot complete we read the stamped id off `root` itself —
    // it is exactly `cteroot->wt_param_id`. (cf. set_worktable_pathlist, which
    // makes the symmetric accommodation for `non_recursive_path->rows`.)
    let mut levelsup = rte.ctelevelsup;
    if levelsup == 0 {
        return Err(elog_error(alloc::format!(
            "bad levelsup for CTE \"{ctename}\""
        )));
    }
    levelsup -= 1;
    let mut cteroot: &PlannerInfo = root;
    while levelsup > 0 {
        levelsup -= 1;
        match cteroot.parent_root.as_deref() {
            Some(p) => cteroot = p,
            None => {
                // Owned-model fallback: parent_root chain is severed at
                // create-plan time. The work-table param id was stamped onto the
                // leaf subroot (`root.wt_param_id`); use it.
                if root.wt_param_id < 0 {
                    return Err(elog_error(alloc::format!(
                        "could not find param ID for CTE \"{ctename}\""
                    )));
                }
                return Ok(root.wt_param_id);
            }
        }
    }
    if cteroot.wt_param_id < 0 {
        return Err(elog_error(alloc::format!(
            "could not find param ID for CTE \"{ctename}\""
        )));
    }
    Ok(cteroot.wt_param_id)
}

/// `copyObject(list_nth(root->multiexpr_params[subqueryid-1], colno-1))`
/// (`fix_param_node`, setrefs.c:2136) — resolve a `PARAM_MULTIEXPR` `Param` to
/// its replacement expression. The `multiexpr_params` replacement `Param` nodes
/// are interned into `root`'s node arena here (SS_process_sublinks /
/// build_subplan), so subselect.c owns the lookup+copy. `subqueryid`/`colno` are
/// 1-based; the caller has already bounds-checked them.
pub fn resolve_multiexpr_param(
    root: &PlannerInfo,
    subqueryid: usize,
    colno: usize,
) -> PgResult<Expr<'static>> {
    let nid = root.multiexpr_params[subqueryid - 1][colno - 1];
    // copyObject of a replacement Param — a flat clone of the interned node.
    Ok(root.node(nid).clone())
}
