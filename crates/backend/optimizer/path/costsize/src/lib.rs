//! costsize.c: scan/join/agg/sort/window cost model + size estimates.
//! Callbacks into planner-hosted selectivity/plancat/index-AM logic ride
//! planner_seams (direct dep would cycle: planner -> costsize).

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    is_outer_join, pathkeys_contained_in, relids, tag16, HashPath, MergePath, MergeScanSelCache,
    NestPath, NodeId, PathId, PathKey, PathNode, QualCost, RelId, RinfoId, SemiAntiJoinFactors,
    SpecialJoinInfo, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, RTE_RELATION,
};

pub mod gucs;
pub mod runtime_model;
pub mod serial_model;

pub fn init_seams() {
    gucs::install();
}

use types_pathnodes::run::PlannerRun;

pub const MAXIMUM_ROWCOUNT: f64 = 1e100;
const MAX_ALLOC_SIZE: i64 = 0x3fffffff;

pub fn clamp_row_est(nrows: f64) -> f64 {
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round_ties_even()
    }
}

pub fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > MAX_ALLOC_SIZE {
        return MAX_ALLOC_SIZE as i32;
    }
    debug_assert!(tuple_width >= 0);
    tuple_width as i32
}

// get_tablespace_page_costs (spccache.c): reloptions unported, so every
// tablespace reads the GUC defaults (divergence owned by the spccache unit).
pub fn get_tablespace_page_costs(_spcid: u32) -> (f64, f64) {
    (gucs::random_page_cost(), gucs::seq_page_cost())
}

// cost_qual_eval (costsize.c) with the rinfo->eval_cost cache (lesson 10).
pub fn cost_qual_eval(run: &mut PlannerRun<'_>, quals: &[RinfoId]) -> PgResult<QualCost> {
    let mut total = QualCost::default();
    for &rid in quals {
        let cached = run.root.rinfo(rid).eval_cost;
        let cost = if cached.startup >= 0.0 {
            cached
        } else {
            if run.root.rinfo(rid).orclause.is_some() {
                panic!("cost_qual_eval_walker (costsize.c): orclause; M2 OR lane");
            }
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let mut cost = QualCost::default();
            cost_qual_eval_walker(Some(run), clause, &mut cost)?;
            if run.root.rinfo(rid).pseudoconstant {
                cost.startup += cost.per_tuple;
                cost.per_tuple = 0.0;
            }
            run.root.rinfo_mut(rid).eval_cost = cost;
            cost
        };
        total.startup += cost.startup;
        total.per_tuple += cost.per_tuple;
    }
    Ok(total)
}
pub fn cost_qual_eval_node<'mcx>(
    run: Option<&mut PlannerRun<'mcx>>,
    node: Node<'mcx>,
) -> PgResult<QualCost> {
    let mut cost = QualCost::default();
    cost_qual_eval_walker(run, node, &mut cost)?;
    Ok(cost)
}

fn cost_qual_eval_walker<'mcx>(
    mut run: Option<&mut PlannerRun<'mcx>>,
    node: Node<'mcx>,
    cost: &mut QualCost,
) -> PgResult<()> {
    match node.node_tag() {
        // SQLValueFunction: no explicit C case; childless leaf, no charge.
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_NextValueExpr => Ok(()),
        // C charges nothing for Aggref/WindowFunc themselves and does not
        // descend: their costs are get_agg_clause_costs'/cost_windowagg's job.
        NodeTag::T_Aggref | NodeTag::T_WindowFunc => Ok(()),
        NodeTag::T_GroupingFunc => {
            cost.per_tuple += gucs::cpu_operator_cost();
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            planner_seams::add_function_cost::call(f.funcid, cost)?;
            for arg in &f.args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // set_opfuncid memo write-back is unmodeled (walker.rs note).
            let opfuncid = if o.opfuncid != 0 {
                o.opfuncid
            } else {
                lsyscache::get_opcode(o.opno)?
            };
            planner_seams::add_function_cost::call(opfuncid, cost)?;
            for arg in &o.args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => cost_qual_eval_walker(run.as_deref_mut(), node.as_relabel_type().unwrap().arg, cost),
        // C: no charge for FieldSelect itself.
        NodeTag::T_FieldSelect => {
            cost_qual_eval_walker(run.as_deref_mut(), node.as_field_select().unwrap().arg, cost)
        }
        // C charges both I/O functions of the coercion.
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            let (infunc, _) = lsyscache::getTypeInputInfo(c.resulttype)?;
            planner_seams::add_function_cost::call(infunc, cost)?;
            let (outfunc, _) = lsyscache::getTypeOutputInfo(expr_type_typmod(c.arg).0)?;
            planner_seams::add_function_cost::call(outfunc, cost)?;
            cost_qual_eval_walker(run.as_deref_mut(), c.arg, cost)
        }
        NodeTag::T_CoerceToDomain => {
            cost_qual_eval_walker(run.as_deref_mut(), node.as_coerce_to_domain().unwrap().arg, cost)
        }
        // C charges the per-element expression once per estimated element,
        // then its fall-through walks both children generically as well.
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            if let Some(elemexpr) = a.elemexpr {
                let perelem = cost_qual_eval_node(run.as_deref_mut(), elemexpr)?;
                cost.startup += perelem.startup;
                if perelem.per_tuple > 0.0 {
                    cost.per_tuple += perelem.per_tuple
                        * planner_seams::estimate_array_length::call(run.as_deref_mut(), a.arg)?;
                }
                cost_qual_eval_walker(run.as_deref_mut(), elemexpr, cost)?;
            }
            cost_qual_eval_walker(run.as_deref_mut(), a.arg, cost)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            cost_qual_eval_walker(run.as_deref_mut(), node.as_convert_rowtype_expr().unwrap().arg, cost)
        }
        // Boolean connectives are free in C; NullTest is "cheap" (no charge).
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for arg in node.as_list().unwrap() {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            let opfuncid = if sa.opfuncid == 0 {
                lsyscache::get_opcode(sa.opno)?
            } else {
                sa.opfuncid
            };
            let arraynode = sa.args.nth(1);
            let mut sacosts = QualCost {
                startup: 0.0,
                per_tuple: 0.0,
            };
            planner_seams::add_function_cost::call(opfuncid, &mut sacosts)?;
            if sa.hashfuncid != 0 {
                // Hashed SAOP: build the table at startup, then one hash +
                // one comparison per tuple.
                let mut hcosts = QualCost { startup: 0.0, per_tuple: 0.0 };
                planner_seams::add_function_cost::call(sa.hashfuncid, &mut hcosts)?;
                cost.startup += sacosts.startup + hcosts.startup;
                cost.startup += planner_seams::estimate_array_length::call(
                    run.as_deref_mut(),
                    arraynode,
                )? * hcosts.per_tuple;
                cost.per_tuple += hcosts.per_tuple + sacosts.per_tuple;
            } else {
                // C: the operator runs against about half the array elements.
                cost.startup += sacosts.startup;
                cost.per_tuple += sacosts.per_tuple
                    * planner_seams::estimate_array_length::call(run.as_deref_mut(), arraynode)?
                    * 0.5;
            }
            for arg in sa.args.iter() {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            Ok(())
        }
        // SubscriptingRef carries no per-node charge in C; recurse.
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for e in sr.refupperindexpr.iter().flatten() {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            for e in sr.reflowerindexpr.iter().flatten() {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            if let Some(e) = sr.refexpr {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            if let Some(e) = sr.refassgnexpr {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => cost_qual_eval_walker(run.as_deref_mut(), arg, cost),
            None => Ok(()),
        },
        // C charges DistinctExpr like OpExpr; BooleanTest itself is free.
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid = if d.opfuncid != 0 {
                d.opfuncid
            } else {
                lsyscache::get_opcode(d.opno)?
            };
            planner_seams::add_function_cost::call(opfuncid, cost)?;
            for arg in &d.args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        // C's OpExpr/DistinctExpr/NullIfExpr arm charges the operator too.
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            let opfuncid = if d.opfuncid != 0 {
                d.opfuncid
            } else {
                lsyscache::get_opcode(d.opno)?
            };
            planner_seams::add_function_cost::call(opfuncid, cost)?;
            for arg in &d.args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => cost_qual_eval_walker(run.as_deref_mut(), arg, cost),
            None => Ok(()),
        },
        // No C case: falls to C's expression_tree_walker default.
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            cost_qual_eval_walker(run.as_deref_mut(), fs.arg, cost)?;
            for a in &fs.newvals {
                cost_qual_eval_walker(run.as_deref_mut(), a, cost)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        // C arbitrarily uses the first alternative's cost.
        NodeTag::T_AlternativeSubPlan => {
            let asp = node.as_alternative_sub_plan().unwrap();
            cost_qual_eval_walker(run.as_deref_mut(), asp.subplans.first().expect("alternatives"), cost)
        }
        // The SubPlan's own costs, precomputed by cost_subplan; C does not
        // descend into the testexpr (already included) or args.
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            cost.startup += sp.startup_cost;
            cost.per_tuple += sp.per_call_cost;
            Ok(())
        }
        // C's default arm: CASE itself is free, children are charged.
        NodeTag::T_CaseTestExpr => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                cost_qual_eval_walker(run.as_deref_mut(), a, cost)?;
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                cost_qual_eval_walker(run.as_deref_mut(), cw.expr.expect("CaseWhen.expr"), cost)?;
                cost_qual_eval_walker(run.as_deref_mut(), cw.result.expect("CaseWhen.result"), cost)?;
            }
            match c.defresult {
                Some(d) => cost_qual_eval_walker(run.as_deref_mut(), d, cost),
                None => Ok(()),
            }
        }
        NodeTag::T_CoerceToDomainValue => Ok(()),
        // C's default arm: a childless leaf, no cost.
        NodeTag::T_CurrentOfExpr => Ok(()),
        // C's default arm: COALESCE/GREATEST/LEAST are free, children charged.
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        // C conservatively charges every column's comparison operator.
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for opno in &rc.opnos {
                planner_seams::add_function_cost::call(lsyscache::get_opcode(opno)?, cost)?;
            }
            for arg in rc.largs.iter().chain(rc.rargs.iter()) {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        // C charges MinMaxExpr a flat cpu_operator_cost ("cost 1").
        NodeTag::T_MinMaxExpr => {
            cost.per_tuple += gucs::cpu_operator_cost();
            for arg in &node.as_min_max_expr().unwrap().args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            Ok(())
        }
        // C charges JsonExpr cpu_operator_cost; the constructor/predicate
        // nodes are free themselves, children charged (default arm).
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            cost.per_tuple += gucs::cpu_operator_cost();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            for v in &j.passing_values {
                cost_qual_eval_walker(run.as_deref_mut(), v, cost)?;
            }
            Ok(())
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for arg in &c.args {
                cost_qual_eval_walker(run.as_deref_mut(), arg, cost)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                cost_qual_eval_walker(run.as_deref_mut(), e, cost)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => {
            match node.as_json_is_predicate().unwrap().expr {
                Some(e) => cost_qual_eval_walker(run.as_deref_mut(), e, cost),
                None => Ok(()),
            }
        }
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => cost_qual_eval_walker(run.as_deref_mut(), e, cost),
            None => Ok(()),
        },
        // No C case: both fall to C's expression_tree_walker default.
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter().chain(x.args.iter()) {
                cost_qual_eval_walker(run.as_deref_mut(), a, cost)?;
            }
            Ok(())
        }
        NodeTag::T_TableFunc => {
            let tf = node.as_table_func().unwrap();
            for a in tf.ns_uris.iter().chain(tf.colvalexprs.iter().flatten()).chain(tf.passingvalexprs.iter())
            {
                cost_qual_eval_walker(run.as_deref_mut(), a, cost)?;
            }
            for a in tf.colexprs.iter().chain(tf.coldefexprs.iter()).flatten() {
                cost_qual_eval_walker(run.as_deref_mut(), a, cost)?;
            }
            if let Some(d) = tf.docexpr {
                cost_qual_eval_walker(run.as_deref_mut(), d, cost)?;
            }
            if let Some(r) = tf.rowexpr {
                cost_qual_eval_walker(run.as_deref_mut(), r, cost)?;
            }
            Ok(())
        }
        // No C case: falls to C's expression_tree_walker default.
        NodeTag::T_PlaceHolderVar => {
            cost_qual_eval_walker(run.as_deref_mut(), node.as_place_holder_var().unwrap().phexpr, cost)
        }
        // No C case: falls to C's expression_tree_walker default.
        NodeTag::T_ReturningExpr => {
            cost_qual_eval_walker(run.as_deref_mut(), node.as_returning_expr().unwrap().retexpr, cost)
        }
        other => panic!("cost_qual_eval_walker (costsize.c): {other:?}; M2 expression lane"),
    }
}
fn get_restriction_qual_cost(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    path_id: types_pathnodes::PathId,
) -> PgResult<QualCost> {
    let Some(ppi) = run.root.path(path_id).base().param_info.as_deref() else {
        return Ok(run.root.rel(rel).baserestrictcost);
    };
    let clauses = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &ppi.ppi_clauses);
    let mut qc = cost_qual_eval(run, &clauses)?;
    qc.startup += run.root.rel(rel).baserestrictcost.startup;
    qc.per_tuple += run.root.rel(rel).baserestrictcost.per_tuple;
    Ok(qc)
}

// get_parameterized_baserel_size (costsize.c).
pub fn get_parameterized_baserel_size<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    param_clauses: &[types_pathnodes::RinfoId],
) -> PgResult<f64> {
    let mut allclauses: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    for &r in param_clauses {
        allclauses.push(r);
    }
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        allclauses.push(run.root.rel(rel).baserestrictinfo[i]);
    }
    let relid = run.root.rel(rel).relid as i32;
    let selec = planner_seams::clauselist_selectivity::call(
        run,
        &allclauses,
        relid,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut nrows = clamp_row_est(run.root.rel(rel).tuples * selec);
    if nrows > run.root.rel(rel).rows {
        nrows = run.root.rel(rel).rows;
    }
    Ok(nrows)
}
// get_parallel_divisor (costsize.c).
pub fn get_parallel_divisor(parallel_workers: i32) -> f64 {
    let mut parallel_divisor = parallel_workers as f64;
    if gucs::parallel_leader_participation() {
        let leader_contribution = 1.0 - (0.3 * parallel_workers as f64);
        if leader_contribution > 0.0 {
            parallel_divisor += leader_contribution;
        }
    }
    parallel_divisor
}

// compute_gather_rows (costsize.c).
pub fn compute_gather_rows(rows: f64, parallel_workers: i32) -> f64 {
    debug_assert!(parallel_workers > 0);
    clamp_row_est(rows * get_parallel_divisor(parallel_workers))
}


// Gather setup price: pgrcolumnar-fed parallel plans pay the measured
// thread-native startup (consts::DEFAULT_PGRCOLUMNAR_PARALLEL_SETUP_COST
// provenance), heap plans keep C's parallel_setup_cost. The Gather may sit
// on an upper rel (grouped rel over a partial agg), whose amflags are
// empty — scan the simple-rel array instead: any pgrcolumnar baserel in the
// (sub)query prices the whole plan's Gathers.
fn gather_setup_cost(run: &PlannerRun<'_>) -> f64 {
    if pgrcolumnar_feeds_plan(run) {
        // Stage-4 pool arming (guc_tables::lane_pool): a session that set
        // `pgrust.lane_parallel_pool` is asking for the parallel shape —
        // drop the provisional pre-pool surcharge back to the regular knob
        // so the forced-DOP partial paths actually win.
        if guc_tables::lane_pool::lane_parallel_pool_armed() {
            gucs::parallel_setup_cost()
        } else {
            gucs::pgrcolumnar_parallel_setup_cost()
        }
    } else {
        gucs::parallel_setup_cost()
    }
}

// Per-tuple Gather transfer price: pgrcolumnar-fed plans pay the measured P0b
// chunked-transport rate (consts::DEFAULT_PGRCOLUMNAR_PARALLEL_TUPLE_COST
// provenance), heap plans keep C's parallel_tuple_cost. Same selector as
// gather_setup_cost.
fn gather_tuple_cost(run: &PlannerRun<'_>) -> f64 {
    if pgrcolumnar_feeds_plan(run) {
        // Armed pool sessions price Gather transfer at the regular rate too:
        // the measured pgrcolumnar chunked-transport rate (0.005/tuple) is cheap
        // enough that at high forced DOP the planner starts preferring
        // ship-every-row-to-the-leader plans (HashAggregate ABOVE Gather)
        // over the partial-agg shape the pool exists for. Heap semantics for
        // armed pgrcolumnar plans; the measured rate stays for unarmed costing.
        if guc_tables::lane_pool::lane_parallel_pool_armed() {
            gucs::parallel_tuple_cost()
        } else {
            gucs::pgrcolumnar_parallel_tuple_cost()
        }
    } else {
        gucs::parallel_tuple_cost()
    }
}

pub fn pgrcolumnar_feeds_plan(run: &PlannerRun<'_>) -> bool {
    run.root.simple_rel_array.iter().any(|slot| {
        slot.is_some_and(|rid| {
            run.root.rel(rid).amflags & types_pathnodes::AMFLAG_PGRCOLUMNAR != 0
        })
    })
}

// cost_gather (costsize.c); no parameterized Gather paths exist (required
// outer is empty at every ported call site).
pub fn cost_gather(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId, rel: RelId, rows: Option<f64>) {
    let rel_rows = run.root.rel(rel).rows;
    let (sub_startup, sub_total, sub_disabled, sub_id) = {
        let PathNode::GatherPath(g) = run.root.path(path_id) else {
            panic!("cost_gather: not a GatherPath")
        };
        let sub_id = g.subpath.expect("Gather subpath");
        let sub = run.root.path(sub_id).base();
        (sub.startup_cost, sub.total_cost, sub.disabled_nodes, sub_id)
    };
    let setup_cost = gather_setup_cost(run);
    // Stage-4 §4.4 exchange transfer pricing: an admitted partial hashed Agg
    // under this Gather hands its tables to the finalize by pointer (the
    // radix-partitioned handoff), never through tuple-queue serialization —
    // price its "transfer" at the measured pgrcolumnar chunked-transport rate
    // (the install relocation memcpy class) instead of parallel_tuple_cost.
    // Ship-all-raw-rows Gathers (subpath ≠ partial Agg) keep the armed
    // pool's heap-rate pricing, so the degenerate leader-hash plan cannot
    // win on a free transfer.
    let exchange_partial = lane_exchange_partial_agg(run, sub_id);
    let tuple_cost = if exchange_partial {
        gucs::pgrcolumnar_parallel_tuple_cost()
    } else {
        gather_tuple_cost(run)
    };
    let p = run.root.path_mut(path_id).base_mut();
    debug_assert!(p.param_info.is_none());
    p.rows = rows.unwrap_or(rel_rows);
    let mut startup_cost = sub_startup;
    let mut run_cost = sub_total - sub_startup;
    startup_cost += setup_cost;
    run_cost += tuple_cost * p.rows;
    // GL-Q2829-FIX-1 plain-Gather leader-consumption floor (doc at
    // gucs::DEFAULT_GATHER_LEADER_MIN_TUPLE_COST; DEFAULT ON since the
    // regexp-keyed-grouping-class flip — 0 via the env restores):
    // raw-row Gather rows are consumed LEADER-SERIALLY by the parent —
    // that work does not ride the cheap-exchange transport discount, and
    // discounting it is what elects the ship-every-row-to-the-leader
    // aggregation family at fresh-stats estimate regimes. Partial-agg-fed
    // Gathers are exempt (pointer handoff, per-group leader work — the
    // exchange pricing above). Self-scoping per-row: low-row Gathers and
    // LIMIT-prorated consumers barely feel it.
    if !exchange_partial {
        run_cost += gather_leader_uplift(tuple_cost, p.rows);
    }
    p.disabled_nodes = sub_disabled;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// The plain-Gather leader-consumption uplift (GL-Q2829-FIX-1), the
/// [`gm_leader_uplift`] sibling without the Gather Merge 5% IPC factor:
/// the per-row delta that floors a raw-row Gather's transport rate at the
/// leader-consumption minimum. Zero when the floor is disarmed (env
/// override 0 — restores the pre-flip election), when the session's rate
/// already meets it (SET parallel_tuple_cost >= floor — C-parity
/// sessions), and when transport is EXPLICITLY zeroed (forced-plan bench
/// seams keep free parallelism).
fn gather_leader_uplift(tuple_cost: f64, rows: f64) -> f64 {
    let floor = gucs::gather_leader_min_tuple_cost();
    if floor > 0.0 && tuple_cost > 0.0 && tuple_cost < floor {
        (floor - tuple_cost) * rows
    } else {
        0.0
    }
}

// cost_gather_merge (costsize.c).
pub fn cost_gather_merge(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    rows: Option<f64>,
) {
    let rel_rows = run.root.rel(rel).rows;
    let num_workers = {
        let PathNode::GatherMergePath(g) = run.root.path(path_id) else {
            panic!("cost_gather_merge: not a GatherMergePath")
        };
        g.num_workers
    };
    debug_assert!(num_workers > 0);
    let setup_cost = gather_setup_cost(run);
    let tuple_cost = gather_tuple_cost(run);
    let p = run.root.path_mut(path_id).base_mut();
    debug_assert!(p.param_info.is_none());
    p.rows = rows.unwrap_or(rel_rows);

    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;
    // One extra for the leader, per C's admittedly overgenerous estimate.
    let n = (num_workers + 1) as f64;
    let log_n = n.log2();
    let comparison_cost = 2.0 * gucs::cpu_operator_cost();
    startup_cost += comparison_cost * n * log_n;
    run_cost += p.rows * comparison_cost * log_n;
    run_cost += gucs::cpu_operator_cost() * p.rows;
    startup_cost += setup_cost;
    run_cost += tuple_cost * p.rows * 1.05;
    // GL-GMLEADER-1 leader-consumption floor (doc at
    // gucs::DEFAULT_GM_LEADER_MIN_TUPLE_COST): GM rows are consumed
    // LEADER-SERIALLY (heap merge + the serial parent) — that work does
    // not ride the cheap-exchange transport discount. Self-scoping: the
    // uplift is per-row, so partial-agg-fed GMs (few rows) and
    // LIMIT-prorated consumers barely feel it; the raw-row catastrophe
    // family pays its real freight and loses its election on arithmetic.
    run_cost += gm_leader_uplift(tuple_cost, p.rows);

    p.disabled_nodes = input_disabled_nodes + if gucs::enable_gathermerge() { 0 } else { 1 };
    p.startup_cost = startup_cost + input_startup_cost;
    p.total_cost = startup_cost + run_cost + input_total_cost;
}

/// The GL-GMLEADER-1 uplift, factored pure for exhaustive pins: the
/// per-row delta that floors a Gather Merge's transport rate at the
/// leader-consumption minimum. Zero when the session's rate already
/// meets the floor (SET parallel_tuple_cost >= 0.1 — C-parity sessions,
/// the GL-STRAGG-2 bisection control) and when transport is EXPLICITLY
/// zeroed (the forced-plan bench seams: zeroed-cost sessions keep free
/// parallelism). The 1.05 factor mirrors the transport term's own
/// small-queue-wait fudge so the floored total equals what the same
/// session would pay at parallel_tuple_cost == the floor.
fn gm_leader_uplift(tuple_cost: f64, rows: f64) -> f64 {
    let floor = gucs::gm_leader_min_tuple_cost();
    if tuple_cost > 0.0 && tuple_cost < floor {
        (floor - tuple_cost) * rows * 1.05
    } else {
        0.0
    }
}

// pgrcolumnar column-fraction disk costing (pgrust-only, AMFLAG_PGRCOLUMNAR-gated;
// the sort-vs-hash grouping fix): a pgrcolumnar seqscan opens with a plan-derived
// column need-set and never touches the other columns' chunks, so the honest
// disk term is the referenced columns' share of the part's on-disk bytes.
// C's costing structure (pages you read x spc_seq_page_cost) is kept — only
// the page count is corrected for a columnar AM, exactly as heap's page
// count is honest for a row store. Referenced = the rel's reltarget +
// baserestrictinfo (+ any pushed-down ppi clauses) Vars; a whole-row Var
// reads everything. Heap rels (and footer-less pgrcolumnar) return 1.0.
//
// Why this matters beyond realism: the uncorrected all-columns disk term is
// a large shared constant in every candidate plan's total, which at scale
// compresses REAL differences (e.g. hash-vs-sort grouping) inside
// add_path's 1% STD_FUZZ_FACTOR, where the sorted path wins the pathkey
// tiebreak (two-key grouped agg @100M: planned Sort+GroupAggregate ~17x
// slower than the hash plan it fuzzily displaced).
fn pgrcolumnar_scan_col_fraction(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    path_id: types_pathnodes::PathId,
) -> f64 {
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    {
        let r = run.root.rel(rel);
        if r.amflags & types_pathnodes::AMFLAG_PGRCOLUMNAR == 0 || r.pgrcolumnar_col_bytes.is_empty() {
            return 1.0;
        }
    }
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid as i32;
    let mut attrs = types_nodes::Bitmapset::empty();
    let exprs =
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &run.root.rel_reltarget(rel).exprs);
    for &eid in exprs.iter() {
        vars::pull_varattnos(mcx, *run.root.expr_node(eid), varno, &mut attrs)
            .expect("pull_varattnos over reltarget");
    }
    let mut rids =
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &run.root.rel(rel).baserestrictinfo);
    if let Some(ppi) = run.root.path(path_id).base().param_info.as_deref() {
        let ppi_clauses = types_pathnodes::relids::pgvec_clone_shallow(mcx, &ppi.ppi_clauses);
        for &rid in ppi_clauses.iter() {
            rids.push(rid);
        }
    }
    for &rid in rids.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        vars::pull_varattnos(mcx, clause, varno, &mut attrs)
            .expect("pull_varattnos over baserestrictinfo");
    }
    // Whole-row reference reads every column.
    if attrs.is_member(0 - FirstLowInvalidHeapAttributeNumber) {
        return 1.0;
    }
    let col_bytes = &run.root.rel(rel).pgrcolumnar_col_bytes;
    let mut needed: u64 = 0;
    let mut total: u64 = 0;
    for (i, &b) in col_bytes.iter().enumerate() {
        total += b;
        let attno = i as i32 + 1;
        if attrs.is_member(attno - FirstLowInvalidHeapAttributeNumber) {
            needed += b;
        }
    }
    // A referenced attno past the footer's column count means the byte map
    // does not describe this descriptor; fall back to C costing.
    for m in 0..attrs.nwords() as i32 * 64 {
        if attrs.is_member(m) && m + FirstLowInvalidHeapAttributeNumber > col_bytes.len() as i32 {
            return 1.0;
        }
    }
    if total == 0 {
        return 1.0;
    }
    (needed as f64 / total as f64).clamp(0.0, 1.0)
}

pub fn cost_seqscan(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId, rel: RelId) {
    let (relid, rtekind, reltablespace, pages, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.tuples,
            baserel.rows,
        )
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    // Parameterized (lateral tlist refs) takes the PPI row estimate.
    let mut rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };

    let mut startup_cost = 0.0;
    let (_, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    let disk_run_cost =
        spc_seq_page_cost * pages as f64 * pgrcolumnar_scan_col_fraction(run, rel, path_id);

    let qpqual_cost =
        get_restriction_qual_cost(run, rel, path_id).expect("cost_qual_eval over ppi_clauses");
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut cpu_run_cost = cpu_per_tuple * tuples;

    // tlist eval costs are paid per output row, not per scanned tuple.
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    cpu_run_cost += target.cost.per_tuple * rows;

    let parallel_workers = run.root.path(path_id).base().parallel_workers;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(parallel_workers);
        // CPU splits across workers; disk cost doesn't amortize (the OS
        // already prefetches). rows becomes the per-worker tuple count.
        cpu_run_cost /= parallel_divisor;
        rows = clamp_row_est(rows / parallel_divisor);
    }

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if gucs::enable_seqscan() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

// cost_samplescan (costsize.c). TABLESAMPLE parameter expressions are
// evaluated once per scan, so their cost is ignored (as C).
pub fn cost_samplescan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> types_error::PgResult<()> {
    let (relid, rtekind, reltablespace, pages, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.tuples,
            baserel.rows,
        )
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    let tsc = run
        .rte(relid as usize)
        .tablesample
        .expect("sampled rel has a tablesample clause")
        .as_table_sample_clause()
        .expect("tablesample is a TableSampleClause");
    let tsm = ::tablesample::Tsm::get(run.mcx, tsc.tsmhandler)?;

    let rows = match &run.root.path(path_id).base().param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };
    let mut startup_cost = 0.0;
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    // NextSampleBlock implies random access, else sequential (as C).
    let spc_page_cost = if tsm.has_next_sample_block() {
        spc_random_page_cost
    } else {
        spc_seq_page_cost
    };
    let mut run_cost = spc_page_cost * pages as f64;

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    // tlist eval costs are paid per output row, not per scanned tuple.
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_functionscan (costsize.c): function eval is all startup cost (the
// executor materializes into a tuplestore before returning rows).
pub fn cost_functionscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_FUNCTION);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };

    let mut startup_cost = 0.0;
    let mut exprcost = QualCost::default();
    let fexprs: Vec<_> = run
        .rte(relid as usize)
        .functions
        .iter()
        .filter_map(|n| n.as_range_tbl_function().expect("functions cell").funcexpr)
        .collect();
    for fexpr in fexprs {
        cost_qual_eval_walker(Some(&mut *run), fexpr, &mut exprcost)?;
    }
    startup_cost += exprcost.startup + exprcost.per_tuple;

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_ctescan (costsize.c): 2× cpu_tuple_cost per scanned tuple (scan +
// tuplestore); the CTE query itself is charged as initplan cost, not here.
pub fn cost_ctescan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_CTE);
    assert!(
        run.root.path(path_id).base().param_info.is_none(),
        "cost_ctescan (costsize.c): parameterized path; M2 lateral lane"
    );
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let mut cpu_per_tuple = gucs::cpu_tuple_cost();

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    // Live C contracts `+= a * b` to fmadd on ARM64 (-ffp-contract;
    // docs/optimizations/adt_float-parity.md).
    let run_cost = target.cost.per_tuple.mul_add(rows, run_cost);

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_cte_size_estimates (costsize.c).
pub fn set_cte_size_estimates(run: &mut PlannerRun<'_>, rel: RelId, cte_rows: f64) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    debug_assert!(rti > 0);
    let self_reference = {
        let rte = run.rte(rti);
        debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_CTE);
        rte.self_reference
    };
    run.root.rel_mut(rel).tuples = if self_reference {
        clamp_row_est(gucs::recursive_worktable_factor() * cte_rows)
    } else {
        cte_rows
    };
    set_baserel_size_estimates(run, rel)
}

// set_namedtuplestore_size_estimates (costsize.c): enrtuples < 0 means the
// registrant offered no estimate; C's default is 1000.
pub fn set_namedtuplestore_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    debug_assert!(rti > 0);
    let enrtuples = {
        let rte = run.rte(rti);
        debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_NAMEDTUPLESTORE);
        rte.enrtuples
    };
    run.root.rel_mut(rel).tuples = if enrtuples < 0.0 { 1000.0 } else { enrtuples };
    set_baserel_size_estimates(run, rel)
}

pub fn cost_namedtuplestorescan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_NAMEDTUPLESTORE);
    debug_assert!(run.root.path(path_id).base().param_info.is_none());
    let rows = base_rows;

    let mut startup_cost = 0.0;
    let mut cpu_per_tuple = gucs::cpu_tuple_cost();

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let run_cost = cpu_per_tuple * tuples;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_recursive_union (costsize.c): ~10 recursive iterations assumed.
pub fn cost_recursive_union(
    run: &mut PlannerRun<'_>,
    runion: PathId,
    nrterm: PathId,
    rterm: PathId,
) {
    let (n_startup, n_total, n_rows, n_disabled, n_width) = {
        let p = run.root.path(nrterm).base();
        (
            p.startup_cost,
            p.total_cost,
            p.rows,
            p.disabled_nodes,
            run.root.path_pathtarget(nrterm).width,
        )
    };
    let (r_total, r_rows, r_disabled, r_width) = {
        let p = run.root.path(rterm).base();
        (p.total_cost, p.rows, p.disabled_nodes, run.root.path_pathtarget(rterm).width)
    };
    // Live C contracts each `+= a * b` to fmadd on ARM64 (-ffp-contract;
    // docs/optimizations/adt_float-parity.md).
    let total_cost = r_total.mul_add(10.0, n_total);
    let total_rows = r_rows.mul_add(10.0, n_rows);
    let total_cost = gucs::cpu_tuple_cost().mul_add(total_rows, total_cost);

    let p = run.root.path_mut(runion).base_mut();
    p.disabled_nodes = n_disabled + r_disabled;
    p.startup_cost = n_startup;
    p.total_cost = total_cost;
    p.rows = total_rows;
    run.root.path_pathtarget_mut(runion).width = n_width.max(r_width);
}

// set_values_size_estimates (costsize.c): tuples = row count of the list.
pub fn set_values_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    debug_assert!(rti > 0);
    debug_assert_eq!(
        run.rte(rti).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_VALUES
    );
    run.root.rel_mut(rel).tuples = run.rte(rti).values_lists.len() as f64;
    set_baserel_size_estimates(run, rel)
}

// set_tablefunc_size_estimates (costsize.c): whole-table estimate is a
// hardwired 100 rows.
pub fn set_tablefunc_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    run.root.rel_mut(rel).tuples = 100.0;
    set_baserel_size_estimates(run, rel)
}

// cost_tablefuncscan (costsize.c); tuplestore spill costs unmodeled, as C.
pub fn cost_tablefuncscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_TABLEFUNC);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };

    let mut startup_cost = 0.0;
    let mut exprcost = QualCost::default();
    let tf = run.rte(relid as usize).tablefunc;
    if let Some(tf) = tf {
        cost_qual_eval_walker(Some(&mut *run), tf, &mut exprcost)?;
    }
    startup_cost += exprcost.startup + exprcost.per_tuple;

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_valuesscan (costsize.c): one cpu_operator_cost per list evaluation.
pub fn cost_valuesscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_pathnodes::RTE_VALUES);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };

    let mut startup_cost = 0.0;
    let mut cpu_per_tuple = gucs::cpu_operator_cost();

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * tuples;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_result_size_estimates (costsize.c): RTE_RESULT natively yields one row.
pub fn set_result_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    run.root.rel_mut(rel).tuples = 1.0;
    set_baserel_size_estimates(run, rel)
}

// set_foreign_size_estimates (costsize.c): rows is a bogus default for the
// FDW's GetForeignRelSize to replace.
pub fn set_foreign_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    run.root.rel_mut(rel).rows = 1000.0;
    let quals =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).baserestrictinfo);
    let qcost = cost_qual_eval(run, &quals)?;
    run.root.rel_mut(rel).baserestrictcost = qcost;
    set_rel_width(run, rel)?;
    Ok(())
}

// cost_resultscan (costsize.c).
pub fn cost_resultscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
) -> PgResult<()> {
    let (relid, rtekind, tuples, base_rows) = {
        let baserel = run.root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows)
    };
    debug_assert!(relid > 0 && rtekind == types_nodes::parsenodes::RTEKind::RTE_RESULT as u32);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };
    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    let startup_cost = qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let run_cost = cpu_per_tuple * tuples;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_function_size_estimates (costsize.c).
pub fn set_function_size_estimates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let rti = run.root.rel(rel).relid as usize;
    let mut funcexprs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(run.mcx);
    for rtfunc_node in &run.rte(rti).functions {
        let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
        if let Some(fexpr) = rtfunc.funcexpr {
            funcexprs.push(fexpr);
        }
    }
    let mut tuples = 0.0f64;
    for &fexpr in funcexprs.iter() {
        let ntup = expression_returns_set_rows(fexpr)?;
        if ntup > tuples {
            tuples = ntup;
        }
    }
    run.root.rel_mut(rel).tuples = tuples;
    set_baserel_size_estimates(run, rel)
}

// expression_returns_set_rows (clauses.c); the OpExpr opretset arm is dead
// (no set-returning operators resolve on this lane).
pub fn expression_returns_set_rows(clause: Node<'_>) -> PgResult<f64> {
    if let Some(fe) = clause.as_func_expr() {
        if fe.funcretset {
            return Ok(clamp_row_est(planner_seams::get_function_rows::call(
                fe.funcid,
                Some(clause),
            )?));
        }
    }
    Ok(1.0)
}

// cost_index (costsize.c).
pub fn cost_index(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
    partial_path: bool,
) -> PgResult<()> {
    let (baserel_id, indexonly, index_total_pages, mut cond_sources) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("cost_index: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        // extract_nonindex_conditions source list; ppi_clauses appended below
        // for a parameterized path.
        let mut sources: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
        sources.extend(index.indrestrictinfo.borrow().iter().copied());
        (
            index.rel.expect("index rel set"),
            ip.path.pathtype == tag16(NodeTag::T_IndexOnlyScan),
            index.pages,
            sources,
        )
    };
    {
        let baserel = run.root.rel(baserel_id);
        debug_assert!(baserel.relid > 0 && baserel.rtekind == RTE_RELATION);
    }

    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;
    let mut cpu_run_cost = 0.0;

    // qpquals: restrictions not redundant with the index clauses.
    let indexclause_rinfos: mcx::PgVec<'_, (RinfoId, bool, Option<types_pathnodes::EcId>)> = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            unreachable!()
        };
        let mut v = mcx::PgVec::new_in(run.mcx);
        for ic in ip.indexclauses.iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            v.push((rid, ic.lossy, run.root.rinfo(rid).parent_ec));
        }
        v
    };
    let new_rows = if let Some(ppi) = run.root.path(path_id).base().param_info.as_deref() {
        cond_sources.extend(ppi.ppi_clauses.iter().copied());
        ppi.ppi_rows
    } else {
        run.root.rel(baserel_id).rows
    };
    let mut qpquals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    for &rid in cond_sources.iter() {
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        if indexclause_rinfos.iter().any(|&(c, lossy, parent_ec)| {
            !lossy
                && (c == rid || (parent_ec.is_some() && run.root.rinfo(rid).parent_ec == parent_ec))
        }) {
            continue;
        }
        qpquals.push(rid);
    }

    run.root.path_mut(path_id).base_mut().rows = new_rows;
    run.root.path_mut(path_id).base_mut().disabled_nodes =
        if gucs::enable_indexscan() { 0 } else { 1 };

    let am = planner_seams::amcostestimate::call(run, path_id, loop_count)?;
    if let PathNode::IndexPath(ip) = run.root.path_mut(path_id) {
        ip.indextotalcost = am.index_total_cost;
        ip.indexselectivity = am.index_selectivity;
    }
    startup_cost += am.index_startup_cost;
    run_cost += am.index_total_cost - am.index_startup_cost;

    let (baserel_tuples, baserel_pages, baserel_allvisfrac, reltablespace) = {
        let baserel = run.root.rel(baserel_id);
        (
            baserel.tuples,
            baserel.pages,
            baserel.allvisfrac,
            baserel.reltablespace,
        )
    };
    let tuples_fetched = clamp_row_est(am.index_selectivity * baserel_tuples);
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);

    let (max_io_cost, min_io_cost, rand_heap_pages) = if loop_count > 1.0 {
        // Repeated scans: scale tuples by the scan count in the Mackert and
        // Lohman formula, then pro-rate per scan; all fetches random.
        let mut pages_fetched = index_pages_fetched(
            run,
            tuples_fetched * loop_count,
            baserel_pages,
            index_total_pages as f64,
        );
        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
        }
        let rand_heap_pages = pages_fetched;
        let max_io_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        let mut pages_fetched = (am.index_selectivity * baserel_pages as f64).ceil();
        pages_fetched = index_pages_fetched(
            run,
            pages_fetched * loop_count,
            baserel_pages,
            index_total_pages as f64,
        );
        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
        }
        let min_io_cost = (pages_fetched * spc_random_page_cost) / loop_count;
        (max_io_cost, min_io_cost, rand_heap_pages)
    } else {
        let mut pages_fetched =
            index_pages_fetched(run, tuples_fetched, baserel_pages, index_total_pages as f64);
        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
        }
        let rand_heap_pages = pages_fetched;
        let max_io_cost = pages_fetched * spc_random_page_cost;

        pages_fetched = (am.index_selectivity * baserel_pages as f64).ceil();
        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - baserel_allvisfrac)).ceil();
        }
        let min_io_cost = if pages_fetched > 0.0 {
            let mut m = spc_random_page_cost;
            if pages_fetched > 1.0 {
                m += (pages_fetched - 1.0) * spc_seq_page_cost;
            }
            m
        } else {
            0.0
        };
        (max_io_cost, min_io_cost, rand_heap_pages)
    };

    // Partial-leg-only; outlined so the serial costing path stays lean.
    // Returns false when workers are unassignable: the caller rejects the
    // path, so the rest of the costing is skipped.
    #[cold]
    #[inline(never)]
    fn cost_index_partial_leg(
        run: &mut PlannerRun<'_>,
        path_id: types_pathnodes::PathId,
        baserel_id: types_pathnodes::RelId,
        rand_heap_pages: f64,
        index_pages: f64,
    ) -> bool {
        let parallel_workers = ::allpaths::compute_parallel_worker(
            run.root.rel(baserel_id),
            rand_heap_pages,
            index_pages,
            guc_tables::vars::max_parallel_workers_per_gather.read(),
        );
        let p = run.root.path_mut(path_id).base_mut();
        p.parallel_workers = parallel_workers;
        if parallel_workers <= 0 {
            return false;
        }
        p.parallel_aware = true;
        true
    }
    if partial_path {
        // Index-only scans size workers by index pages: heap fetches can be
        // few enough to spuriously rule out parallelism.
        let rand_heap_pages = if indexonly { -1.0 } else { rand_heap_pages };
        if !cost_index_partial_leg(run, path_id, baserel_id, rand_heap_pages, am.index_pages) {
            return Ok(());
        }
    }

    let csquared = am.index_correlation * am.index_correlation;
    run_cost += max_io_cost + csquared * (min_io_cost - max_io_cost);

    let qpqual_cost = cost_qual_eval(run, &qpquals)?;
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    cpu_run_cost += cpu_per_tuple * tuples_fetched;

    let path_rows = run.root.path(path_id).base().rows;
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    cpu_run_cost += target.cost.per_tuple * path_rows;

    let parallel_workers = run.root.path(path_id).base().parallel_workers;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(parallel_workers);
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = clamp_row_est(p.rows / parallel_divisor);
        cpu_run_cost /= parallel_divisor;
    }

    run_cost += cpu_run_cost;

    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_bitmap_tree_node (costsize.c): (cost, selectivity) of a bitmapqual.
pub fn cost_bitmap_tree_node(run: &PlannerRun<'_>, path_id: types_pathnodes::PathId) -> (f64, f64) {
    match run.root.path(path_id) {
        PathNode::IndexPath(ip) => (
            // Per-tuple bitmap-manipulation charge: a one-tuple bitmap scan
            // must not tie the plain indexscan.
            ip.indextotalcost + 0.1 * gucs::cpu_operator_cost() * ip.path.rows,
            ip.indexselectivity,
        ),
        PathNode::BitmapAndPath(ap) => (ap.path.total_cost, ap.bitmapselectivity),
        PathNode::BitmapOrPath(op) => (op.path.total_cost, op.bitmapselectivity),
        other => panic!(
            "cost_bitmap_tree_node (costsize.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

// cost_bitmap_and_node (costsize.c): AND selectivity assumes independent
// inputs; 100x cpu_operator_cost per tbm_intersect.
pub fn cost_bitmap_and_node(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let subs = {
        let PathNode::BitmapAndPath(p) = run.root.path(path_id) else {
            unreachable!()
        };
        p.bitmapquals.clone()
    };
    let mut total_cost = 0.0;
    let mut selec = 1.0;
    for (i, &sub) in subs.iter().enumerate() {
        let (sub_cost, sub_selec) = cost_bitmap_tree_node(run, sub);
        selec *= sub_selec;
        total_cost += sub_cost;
        if i > 0 {
            total_cost += 100.0 * gucs::cpu_operator_cost();
        }
    }
    let PathNode::BitmapAndPath(p) = run.root.path_mut(path_id) else {
        unreachable!()
    };
    p.bitmapselectivity = selec;
    p.path.rows = 0.0;
    p.path.disabled_nodes = 0;
    p.path.startup_cost = total_cost;
    p.path.total_cost = total_cost;
}

// cost_bitmap_or_node (costsize.c): OR selectivity assumes non-overlapping
// inputs, clamped to 1; tbm_unions are free when the input is an IndexPath.
pub fn cost_bitmap_or_node(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let subs = {
        let PathNode::BitmapOrPath(p) = run.root.path(path_id) else {
            unreachable!()
        };
        p.bitmapquals.clone()
    };
    let mut total_cost = 0.0;
    let mut selec = 0.0;
    for (i, &sub) in subs.iter().enumerate() {
        let (sub_cost, sub_selec) = cost_bitmap_tree_node(run, sub);
        selec += sub_selec;
        total_cost += sub_cost;
        if i > 0 && !matches!(run.root.path(sub), PathNode::IndexPath(_)) {
            total_cost += 100.0 * gucs::cpu_operator_cost();
        }
    }
    let PathNode::BitmapOrPath(p) = run.root.path_mut(path_id) else {
        unreachable!()
    };
    p.bitmapselectivity = selec.min(1.0);
    p.path.rows = 0.0;
    p.path.startup_cost = total_cost;
    p.path.total_cost = total_cost;
}

fn get_indexpath_pages(run: &PlannerRun<'_>, path_id: types_pathnodes::PathId) -> f64 {
    match run.root.path(path_id) {
        PathNode::IndexPath(ip) => ip.indexinfo.as_ref().expect("indexinfo set").pages as f64,
        PathNode::BitmapAndPath(p) => p
            .bitmapquals
            .clone()
            .iter()
            .map(|&q| get_indexpath_pages(run, q))
            .sum(),
        PathNode::BitmapOrPath(p) => p
            .bitmapquals
            .clone()
            .iter()
            .map(|&q| get_indexpath_pages(run, q))
            .sum(),
        other => panic!(
            "get_indexpath_pages (costsize.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

// compute_bitmap_pages (costsize.c) -> (pages_fetched, cost, tuples_fetched).
pub fn compute_bitmap_pages(
    run: &PlannerRun<'_>,
    rel: RelId,
    bitmapqual: types_pathnodes::PathId,
    loop_count: f64,
) -> (f64, f64, f64) {
    let (index_total_cost, index_selectivity) = cost_bitmap_tree_node(run, bitmapqual);
    let (pages, tuples) = {
        let baserel = run.root.rel(rel);
        (baserel.pages, baserel.tuples)
    };
    let mut tuples_fetched = clamp_row_est(index_selectivity * tuples);
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let mut pages_fetched = (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched);
    let heap_pages = pages_fetched.min(pages as f64);
    let maxentries =
        tidbitmap::tbm_calculate_entries(init_small::globals::work_mem() as usize * 1024) as f64;
    if loop_count > 1.0 {
        pages_fetched = index_pages_fetched(
            run,
            tuples_fetched * loop_count,
            pages,
            get_indexpath_pages(run, bitmapqual),
        );
        pages_fetched /= loop_count;
    }
    pages_fetched = if pages_fetched >= t {
        t
    } else {
        pages_fetched.ceil()
    };
    if maxentries < heap_pages {
        // tbm_lossify() sheds pages sharply once memory runs short; this
        // matches C's crude estimate of that shape.
        let lossy_pages = (heap_pages - maxentries / 2.0).max(0.0);
        let exact_pages = heap_pages - lossy_pages;
        if lossy_pages > 0.0 {
            tuples_fetched = clamp_row_est(
                index_selectivity * (exact_pages / heap_pages) * tuples
                    + (lossy_pages / heap_pages) * tuples,
            );
        }
    }
    (pages_fetched, index_total_cost, tuples_fetched)
}

// cost_bitmap_heap_scan (costsize.c); loop_count > 1 rides the join lane.
pub fn cost_bitmap_heap_scan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
    bitmapqual: types_pathnodes::PathId,
    loop_count: f64,
) {
    let (relid, rtekind, reltablespace, pages, base_rows) = {
        let baserel = run.root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.rows,
        )
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };

    let (pages_fetched, index_total_cost, tuples_fetched) =
        compute_bitmap_pages(run, rel, bitmapqual, loop_count);

    let mut startup_cost = index_total_cost;
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    // Interpolate between random (few pages) and sequential (most of the
    // table) per-page cost, nonlinearly, as C.
    let cost_per_page = if pages_fetched >= 2.0 {
        spc_random_page_cost
            - (spc_random_page_cost - spc_seq_page_cost) * (pages_fetched / t).sqrt()
    } else {
        spc_random_page_cost
    };
    let mut run_cost = pages_fetched * cost_per_page;

    // Indexquals are assumed rechecked at every tuple (lossy bitmaps), so the
    // full scan-clause freight is charged.
    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)
        .expect("unparameterized path has no param clauses");
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut cpu_run_cost = cpu_per_tuple * tuples_fetched;
    let parallel_workers = run.root.path(path_id).base().parallel_workers;
    let mut rows = rows;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(parallel_workers);
        rows = clamp_row_est(rows / parallel_divisor);
        cpu_run_cost /= parallel_divisor;
    }
    run_cost += cpu_run_cost;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if gucs::enable_bitmapscan() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

pub fn cost_material(
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    tuples: f64,
    width: i32,
) -> (f64, i32, f64, f64) {
    let startup_cost = input_startup_cost;
    let mut run_cost = input_total_cost - input_startup_cost;
    let nbytes = relation_byte_size(tuples, width);
    let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
    // 2x cpu_operator_cost per tuple: must exceed cost_rescan's charge or
    // A-outer/B-inner vs B-outer/A-inner materialized nestloops tie.
    run_cost += 2.0 * gucs::cpu_operator_cost() * tuples;
    if nbytes > work_mem_bytes {
        let npages = (nbytes / BLCKSZ as f64).ceil();
        run_cost += gucs::seq_page_cost() * npages;
    }
    (
        tuples,
        input_disabled_nodes + if gucs::enable_material() { 0 } else { 1 },
        startup_cost,
        startup_cost + run_cost,
    )
}

// cost_tidscan (costsize.c).
pub fn cost_tidscan(
    run: &mut PlannerRun<'_>,
    path_id: PathId,
    rel: RelId,
    tidquals: &[RinfoId],
) -> PgResult<()> {
    let (relid, rtekind, reltablespace, base_rows) = {
        let baserel = run.root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.rows,
        )
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    debug_assert!(!tidquals.is_empty());
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };
    run.root.path_mut(path_id).base_mut().rows = rows;

    let mut ntuples = 0.0f64;
    for &rid in tidquals {
        let qual = *run.root.expr_node(run.root.rinfo(rid).clause);
        debug_assert!(gucs::enable_tidscan() || qual.node_tag() == NodeTag::T_CurrentOfExpr);
        if let Some(saop) = qual.as_scalar_array_op_expr() {
            ntuples +=
                planner_seams::estimate_array_length::call(Some(&mut *run), saop.args.nth(1))?;
        } else if qual.node_tag() == NodeTag::T_CurrentOfExpr {
            ntuples += 1.0;
        } else {
            ntuples += 1.0;
        }
    }

    let mut quals: PgVec<'_, RinfoId> = PgVec::new_in(run.mcx);
    quals.extend(tidquals.iter().copied());
    let tid_qual_cost = cost_qual_eval(run, &quals)?;
    let (spc_random_page_cost, _) = get_tablespace_page_costs(reltablespace);
    let mut run_cost = spc_random_page_cost * ntuples;

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    // TID quals are assumed a subset of the qpquals (C's XXX note).
    let mut startup_cost = qpqual_cost.startup + tid_qual_cost.per_tuple;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let path_rows = run.root.path(path_id).base().rows;
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path_rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_tidrangescan (costsize.c).
pub fn cost_tidrangescan(
    run: &mut PlannerRun<'_>,
    path_id: PathId,
    rel: RelId,
    tidrangequals: &[RinfoId],
) -> PgResult<()> {
    let (relid, rtekind, reltablespace, base_rows, base_pages, base_tuples) = {
        let baserel = run.root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.rows,
            baserel.pages,
            baserel.tuples,
        )
    };
    debug_assert!(relid > 0 && rtekind == RTE_RELATION);
    let rows = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => base_rows,
    };
    run.root.path_mut(path_id).base_mut().rows = rows;

    let mut quals: PgVec<'_, RinfoId> = PgVec::new_in(run.mcx);
    quals.extend(tidrangequals.iter().copied());
    let selectivity = planner_seams::clauselist_selectivity::call(
        run,
        &quals,
        relid as i32,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut pages = (selectivity * base_pages as f64).ceil();
    if pages <= 0.0 {
        pages = 1.0;
    }
    // First page is a random seek, the rest sequential reads; kept costlier
    // than the equivalent seqscan on purpose (C's NOTE).
    let ntuples = selectivity * base_tuples;
    let nseqpages = pages - 1.0;

    let tid_qual_cost = cost_qual_eval(run, &quals)?;
    let (spc_random_page_cost, spc_seq_page_cost) = get_tablespace_page_costs(reltablespace);
    let mut run_cost = spc_random_page_cost + spc_seq_page_cost * nseqpages;

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    // TID quals are assumed a subset of the qpquals (C's XXX note).
    let mut startup_cost = qpqual_cost.startup + tid_qual_cost.per_tuple;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let path_rows = run.root.path(path_id).base().rows;
    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path_rows;

    debug_assert!(gucs::enable_tidscan());
    let p = run.root.path_mut(path_id).base_mut();
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
    Ok(())
}

// cost_agg (costsize.c), AGG_PLAIN/AGG_SORTED/AGG_HASHED arms.
#[allow(clippy::too_many_arguments)]
pub fn cost_agg(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    aggstrategy: u32,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[types_pathnodes::NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    input_width: i32,
) -> PgResult<()> {
    let (rows, disabled_nodes, startup_cost, total_cost) = cost_agg_shape(
        run,
        aggstrategy,
        aggcosts,
        num_group_cols,
        num_groups,
        quals,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        input_width,
    )?;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
    Ok(())
}

/// cost_agg without a Path to write into (C's dummy `Path agg_path` callers);
/// returns (rows, disabled_nodes, startup, total).
#[allow(clippy::too_many_arguments)]
pub fn cost_agg_shape(
    run: &mut PlannerRun<'_>,
    aggstrategy: u32,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[types_pathnodes::NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    input_width: i32,
) -> PgResult<(f64, i32, f64, f64)> {
    let mut disabled_nodes = input_disabled_nodes;

    let (mut startup_cost, mut total_cost, mut output_tuples);
    if aggstrategy == types_pathnodes::AGG_PLAIN {
        debug_assert!(num_group_cols == 0);
        startup_cost = input_total_cost;
        startup_cost += aggcosts.transCost.startup;
        // mul_add mirrors the C referee's fmadd (GCC fp-contract on aarch64
        // fuses `cost += expr * rows`); EXPLAIN costs are byte-compared and
        // a x.xx5 display boundary exposes the one-ulp difference.
        startup_cost = aggcosts.transCost.per_tuple.mul_add(input_tuples, startup_cost);
        startup_cost += aggcosts.finalCost.startup;
        startup_cost += aggcosts.finalCost.per_tuple;
        total_cost = startup_cost + gucs::cpu_tuple_cost();
        output_tuples = 1.0;
    } else if aggstrategy == types_pathnodes::AGG_SORTED
        || aggstrategy == types_pathnodes::AGG_MIXED
    {
        // Output is delivered on-the-fly, one group at a time.
        startup_cost = input_startup_cost;
        total_cost = input_total_cost;
        if aggstrategy == types_pathnodes::AGG_MIXED && !gucs::enable_hashagg() {
            disabled_nodes += 1;
        }
        total_cost += aggcosts.transCost.startup;
        total_cost = aggcosts.transCost.per_tuple.mul_add(input_tuples, total_cost);
        total_cost =
            (gucs::cpu_operator_cost() * num_group_cols as f64).mul_add(input_tuples, total_cost);
        total_cost += aggcosts.finalCost.startup;
        total_cost = aggcosts.finalCost.per_tuple.mul_add(num_groups, total_cost);
        total_cost = gucs::cpu_tuple_cost().mul_add(num_groups, total_cost);
        output_tuples = num_groups;
    } else if aggstrategy == types_pathnodes::AGG_HASHED {
        startup_cost = input_total_cost;
        if !gucs::enable_hashagg() {
            disabled_nodes += 1;
        }
        startup_cost += aggcosts.transCost.startup;
        startup_cost = aggcosts.transCost.per_tuple.mul_add(input_tuples, startup_cost);
        startup_cost = (gucs::cpu_operator_cost() * num_group_cols as f64)
            .mul_add(input_tuples, startup_cost);
        startup_cost += aggcosts.finalCost.startup;
        total_cost = startup_cost;
        total_cost = aggcosts.finalCost.per_tuple.mul_add(num_groups, total_cost);
        total_cost = gucs::cpu_tuple_cost().mul_add(num_groups, total_cost);
        output_tuples = num_groups;
    } else {
        unreachable!("cost_agg (costsize.c): aggstrategy {aggstrategy}");
    }

    if aggstrategy == types_pathnodes::AGG_HASHED || aggstrategy == types_pathnodes::AGG_MIXED {
        let hashentrysize = ::nodeagg::hash_agg_entry_size(
            run.root.aggtransinfos.len(),
            input_width.max(0) as usize,
            aggcosts.transitionSpace as usize,
        );
        let (mem_limit, ngroups_limit, num_partitions) =
            ::nodeagg::hash_agg_set_limits(hashentrysize, num_groups, 0);
        let nbatches = ((num_groups * hashentrysize) / mem_limit as f64)
            .max(num_groups / ngroups_limit as f64)
            .ceil()
            .max(1.0);
        let num_partitions = (num_partitions.max(2)) as f64;
        let depth = (nbatches.ln() / num_partitions.ln()).ceil();
        let pages = relation_byte_size(input_tuples, input_width) / BLCKSZ as f64;
        let pages_written = pages * depth * 2.0;
        let pages_read = pages_written;
        startup_cost = pages_written.mul_add(gucs::random_page_cost(), startup_cost);
        total_cost = pages_written.mul_add(gucs::random_page_cost(), total_cost);
        total_cost = pages_read.mul_add(gucs::seq_page_cost(), total_cost);
        let spill_cost = depth * input_tuples * 2.0 * gucs::cpu_tuple_cost();
        startup_cost += spill_cost;
        total_cost += spill_cost;
    }

    // HAVING quals: charged per output tuple, then filter selectivity.
    if !quals.is_empty() {
        let mut qual_cost = QualCost {
            startup: 0.0,
            per_tuple: 0.0,
        };
        for &q in quals {
            let node = *run.root.expr_node(q);
            let c = cost_qual_eval_node(Some(&mut *run), node)?;
            qual_cost.startup += c.startup;
            qual_cost.per_tuple += c.per_tuple;
        }
        startup_cost += qual_cost.startup;
        total_cost += output_tuples.mul_add(qual_cost.per_tuple, qual_cost.startup);

        // C passes the bare clauses; the transient RestrictInfo wrap feeds
        // the same restriction_selectivity legs.
        let mut rids: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
        for &q in quals {
            let clause = *run.root.expr_node(q);
            rids.push(planner_seams::make_restrictinfo::call(
                run, clause, true, false, false, false, 0, relids::relids_empty(), relids::relids_empty(), relids::relids_empty(),
            )?);
        }
        let sel = planner_seams::clauselist_selectivity::call(
            run,
            &rids,
            0,
            types_pathnodes::JOIN_INNER,
            None,
        )?;
        output_tuples = clamp_row_est(output_tuples * sel);
    }

    Ok((output_tuples, disabled_nodes, startup_cost, total_cost))
}

// ---------------------------------------------------------------------------
// Stage-4 §4.4 radix exchange — honest recosting of the engaged parallel-agg
// shape (create_agg_path rider; cost_gather carries the transfer half).
//
// With honest footer-NDV stats a high-cardinality GROUP BY prices partial
// aggregation as a poor reducer and the planner drops the parallel shape
// entirely (poolscale groupby_high: T-invariant serial ~4s). Under the
// exchange that pricing is wrong on three counts: (a) partial tables are
// bounded at the exchange cap, so the partial agg's disk-spill surcharge
// never materializes; (b) partial output crosses to the finalize by pointer
// handoff, not tuple-queue serialization; (c) the finalize's per-input work
// runs on the bucket-claim claimer pool (DOP+1 threads), not serially.
// The adjustments below apply ONLY when the executor's own admission
// (guc_tables::lane_pool::agg_exchange_admits over the same plan-time group
// estimate) says the exchange will engage — armed pool + NDV floor —
// and the plan is pgrcolumnar-fed; everything else keeps C costing untouched.
// ---------------------------------------------------------------------------

// The AGG_HASHED disk-spill surcharge exactly as cost_agg_shape adds it
// (keep in lockstep with its spill block; not refactored into a shared
// helper so the C-parity mul_add chains there stay byte-identical).
// Returns (startup_add, total_add).
fn hashed_agg_spill_surcharge(
    run: &PlannerRun<'_>,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
    input_tuples: f64,
    input_width: i32,
) -> (f64, f64) {
    hashed_agg_spill_surcharge_scaled(run, aggcosts, num_groups, input_tuples, input_width, 1.0)
}

// hashed_agg_spill_surcharge with the per-group entry size scaled by
// entry_scale (1.0 = the C estimate). The step-0b honest-Gather delta
// evaluates it at gucs::pgrcolumnar_leader_hashagg_entry_scale.
fn hashed_agg_spill_surcharge_scaled(
    run: &PlannerRun<'_>,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
    input_tuples: f64,
    input_width: i32,
    entry_scale: f64,
) -> (f64, f64) {
    let hashentrysize = ::nodeagg::hash_agg_entry_size(
        run.root.aggtransinfos.len(),
        input_width.max(0) as usize,
        aggcosts.transitionSpace as usize,
    ) * entry_scale;
    let (mem_limit, ngroups_limit, num_partitions) =
        ::nodeagg::hash_agg_set_limits(hashentrysize, num_groups, 0);
    let nbatches = ((num_groups * hashentrysize) / mem_limit as f64)
        .max(num_groups / ngroups_limit as f64)
        .ceil()
        .max(1.0);
    let num_partitions = (num_partitions.max(2)) as f64;
    let depth = (nbatches.ln() / num_partitions.ln()).ceil();
    let pages = relation_byte_size(input_tuples, input_width) / BLCKSZ as f64;
    let pages_written = pages * depth * 2.0;
    let pages_read = pages_written;
    let spill_cost = depth * input_tuples * 2.0 * gucs::cpu_tuple_cost();
    (
        pages_written * gucs::random_page_cost() + spill_cost,
        pages_written * gucs::random_page_cost() + pages_read * gucs::seq_page_cost() + spill_cost,
    )
}

// One ProjectionPath peel: grouping paths occasionally interpose a
// projection between the Gather and the partial Agg.
fn peel_projection(run: &PlannerRun<'_>, id: types_pathnodes::PathId) -> types_pathnodes::PathId {
    match run.root.path(id) {
        PathNode::ProjectionPath(p) => p.subpath.unwrap_or(id),
        _ => id,
    }
}

/// The exchange-eligible PARTIAL half: a parallel hashed
/// AGGSPLIT_INITIAL_SERIAL AggPath whose group estimate clears the
/// admission floor. Shared by cost_gather (transfer pricing) and the
/// finalize-side shape check.
pub fn lane_exchange_partial_agg(run: &PlannerRun<'_>, subpath_id: types_pathnodes::PathId) -> bool {
    if !pgrcolumnar_feeds_plan(run) {
        return false;
    }
    match run.root.path(peel_projection(run, subpath_id)) {
        PathNode::AggPath(a) => {
            a.aggstrategy == types_pathnodes::AGG_HASHED
                && a.aggsplit == types_pathnodes::AGGSPLIT_INITIAL_SERIAL
                && guc_tables::lane_pool::agg_exchange_admits(a.numGroups)
        }
        _ => false,
    }
}

/// create_agg_path's exchange rider: adjust the just-written costs of an
/// admitted shape. No-op (bit-exact untouched costs) everywhere else.
#[allow(clippy::too_many_arguments)]
pub fn cost_agg_lane_exchange_adjust(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    aggstrategy: u32,
    aggsplit: u32,
    subpath_id: types_pathnodes::PathId,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
    input_tuples: f64,
    input_width: i32,
    input_total_cost: f64,
) {
    if aggstrategy != types_pathnodes::AGG_HASHED
        || !guc_tables::lane_pool::agg_exchange_admits(num_groups)
        || !pgrcolumnar_feeds_plan(run)
    {
        return;
    }
    let is_partial = aggsplit == types_pathnodes::AGGSPLIT_INITIAL_SERIAL
        && run.root.path(path_id).base().parallel_workers > 0;
    let is_final = aggsplit == types_pathnodes::AGGSPLIT_FINAL_DESERIAL
        && match run.root.path(subpath_id) {
            PathNode::GatherPath(g) => g
                .subpath
                .is_some_and(|s| lane_exchange_partial_agg(run, s)),
            _ => false,
        };
    if !is_partial && !is_final {
        return;
    }
    // (a) Neither side spills: the partial table is cap-bounded and the
    // finalize merges handed tables bucket-by-bucket in place.
    let (s_add, t_add) =
        hashed_agg_spill_surcharge(run, aggcosts, num_groups, input_tuples, input_width);
    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost = (p.startup_cost - s_add).max(input_total_cost);
    p.total_cost = (p.total_cost - t_add).max(p.startup_cost);
    if is_final {
        // (c) The finalize's build-above-input work (combines + hashing over
        // the handed entries) runs on the claimer pool; the group emit tail
        // (total − startup) stays serial behind the RootAdapter.
        let claimers = (guc_tables::lane_pool::lane_parallel_pool_dop().max(1) + 1) as f64;
        let emit = p.total_cost - p.startup_cost;
        p.startup_cost = input_total_cost + (p.startup_cost - input_total_cost) / claimers;
        p.total_cost = p.startup_cost + emit;
    }
}

/// Step-0b honest-Gather spill pricing (runtime cost-model design §5,
/// scratchpad/night/runtime-cost-model-design.md): a leader-side hashed Agg
/// fed by a Gather/GatherMerge on a pgrcolumnar-fed plan re-prices its
/// disk-spill surcharge with the executor-honest per-group footprint
/// (gucs::DEFAULT_PGRCOLUMNAR_LEADER_HASHAGG_ENTRY_SCALE — provenance
/// there). C's entry estimate (96B for the probed shape) said a 10M-group
/// leader table fits any >=1GB budget, so cost_agg added NO spill term while
/// the real ~3GB working set crossed the 2GB budget and ran 10x slower
/// spilling (the high-card grouped-agg cliff, third sighting). Adds the
/// DELTA between the
/// honest-entry surcharge and the C-entry surcharge cost_agg already added —
/// exactly 0.0 whenever even the scaled working set fits the hash budget
/// (both evaluate to no-spill), so tiny/regress shapes are byte-identical.
/// Heap-only plans never reach this (pgrcolumnar_feeds_plan gate); serial
/// hashaggs (no Gather input) keep pure C costing — those fold to the
/// runtime engine, whose memory economics are step-1's spillrisk term.
pub fn cost_agg_leader_spill_adjust(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    aggstrategy: u32,
    subpath_id: types_pathnodes::PathId,
    aggcosts: &types_pathnodes::AggClauseCosts,
    num_groups: f64,
    input_tuples: f64,
    input_width: i32,
) {
    let Some(entry_scale) = gucs::pgrcolumnar_leader_hashagg_entry_scale() else {
        return;
    };
    if aggstrategy != types_pathnodes::AGG_HASHED || !pgrcolumnar_feeds_plan(run) {
        return;
    }
    // Leader-side only: the agg's direct input is a Gather/GatherMerge —
    // raw rows (the leader-hashagg AGGSPLIT_SIMPLE shape) or a gathered partial agg's
    // finalize; either way the leader builds the num_groups-entry table.
    let gather_sub = match run.root.path(peel_projection(run, subpath_id)) {
        PathNode::GatherPath(g) => g.subpath,
        PathNode::GatherMergePath(g) => g.subpath,
        _ => return,
    };
    // The admitted radix exchange hands partial tables to the finalize by
    // pointer and merges in place — cost_agg_lane_exchange_adjust owns that
    // shape's pricing (and deliberately strips the spill surcharge).
    if gather_sub.is_some_and(|s| lane_exchange_partial_agg(run, s)) {
        return;
    }
    let (s_base, t_base) =
        hashed_agg_spill_surcharge_scaled(run, aggcosts, num_groups, input_tuples, input_width, 1.0);
    let (s_honest, t_honest) = hashed_agg_spill_surcharge_scaled(
        run,
        aggcosts,
        num_groups,
        input_tuples,
        input_width,
        entry_scale,
    );
    let (ds, dt) = (s_honest - s_base, t_honest - t_base);
    if ds == 0.0 && dt == 0.0 {
        return;
    }
    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost += ds;
    p.total_cost += dt;
}

/// cost_group (costsize.c); caller ensures the input is sorted.
#[allow(clippy::too_many_arguments)]
pub fn cost_group(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[types_pathnodes::NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
) -> PgResult<()> {
    let mut output_tuples = num_groups;
    let mut startup_cost = input_startup_cost;
    let mut total_cost = input_total_cost;

    // C associates cpu_operator_cost * input_tuples * numGroupCols left-to-
    // right and fp-contracts the tail multiply; mirrored for EXPLAIN parity.
    total_cost =
        (gucs::cpu_operator_cost() * input_tuples).mul_add(num_group_cols as f64, total_cost);

    if !quals.is_empty() {
        let mut qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
        for &q in quals {
            let node = *run.root.expr_node(q);
            let c = cost_qual_eval_node(Some(&mut *run), node)?;
            qual_cost.startup += c.startup;
            qual_cost.per_tuple += c.per_tuple;
        }
        startup_cost += qual_cost.startup;
        total_cost += output_tuples.mul_add(qual_cost.per_tuple, qual_cost.startup);

        // C passes the bare clauses; the transient RestrictInfo wrap feeds
        // the same restriction_selectivity legs.
        let mut rids: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
        for &q in quals {
            let clause = *run.root.expr_node(q);
            rids.push(planner_seams::make_restrictinfo::call(
                run, clause, true, false, false, false, 0, relids::relids_empty(), relids::relids_empty(), relids::relids_empty(),
            )?);
        }
        let sel = planner_seams::clauselist_selectivity::call(
            run,
            &rids,
            0,
            types_pathnodes::JOIN_INNER,
            None,
        )?;
        output_tuples = clamp_row_est(output_tuples * sel);
    }

    let p = run.root.path_mut(path_id).base_mut();
    p.rows = output_tuples;
    p.disabled_nodes = input_disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
    Ok(())
}

const BLCKSZ: usize = 8192;
const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

// relation_byte_size (costsize.c).
pub fn relation_byte_size(tuples: f64, width: i32) -> f64 {
    tuples * ((maxalign(width.max(0) as usize) + maxalign(SIZEOF_HEAP_TUPLE_HEADER)) as f64)
}

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// index_pages_fetched (costsize.c): the Mackert-Lohman formula.
pub fn index_pages_fetched(
    run: &PlannerRun<'_>,
    tuples_fetched: f64,
    pages: u32,
    index_pages: f64,
) -> f64 {
    let t = if pages > 1 { pages as f64 } else { 1.0 };
    let total_pages = (run.root.total_table_pages + index_pages).max(1.0);
    debug_assert!(t <= total_pages);

    let mut b = gucs::effective_cache_size() as f64 * t / total_pages;
    b = if b <= 1.0 { 1.0 } else { b.ceil() };

    if t <= b {
        let pf = (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched);
        if pf >= t {
            t
        } else {
            pf.ceil()
        }
    } else {
        let lim = (2.0 * t * b) / (2.0 * t - b);
        let pf = if tuples_fetched <= lim {
            (2.0 * t * tuples_fetched) / (2.0 * t + tuples_fetched)
        } else {
            b + (tuples_fetched - lim) * (t - b) / t
        };
        pf.ceil()
    }
}

// set_baserel_size_estimates (costsize.c).
pub fn set_baserel_size_estimates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    let quals =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).baserestrictinfo);
    let selec = planner_seams::clauselist_selectivity::call(
        run,
        &quals,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let nrows = run.root.rel(rel).tuples * selec;
    run.root.rel_mut(rel).rows = clamp_row_est(nrows);
    let qcost = cost_qual_eval(run, &quals)?;
    run.root.rel_mut(rel).baserestrictcost = qcost;
    set_rel_width(run, rel)?;
    Ok(())
}

// get_expr_width (costsize.c).
pub fn get_expr_width(run: &PlannerRun<'_>, expr: NodeId) -> PgResult<i32> {
    let node = *run.root.expr_node(expr);
    if let Some(var) = node.as_var() {
        debug_assert!(var.varlevelsup == 0);
        if var.varno >= 0 && var.varno < run.root.simple_rel_array_size {
            if let Some(rel_id) = run
                .root
                .simple_rel_array
                .get(var.varno as usize)
                .copied()
                .flatten()
            {
                let rel = run.root.rel(rel_id);
                if var.varattno >= rel.min_attr && var.varattno <= rel.max_attr {
                    let ndx = (var.varattno - rel.min_attr) as usize;
                    if rel.attr_widths[ndx] > 0 {
                        return Ok(rel.attr_widths[ndx]);
                    }
                }
            }
        }
        let width = lsyscache::get_typavgwidth(var.vartype, var.vartypmod)?;
        debug_assert!(width > 0);
        return Ok(width);
    }
    let (typid, typmod) = expr_type_typmod(node);
    let width = lsyscache::get_typavgwidth(typid, typmod)?;
    debug_assert!(width > 0);
    Ok(width)
}

// exprType/exprTypmod (nodeFuncs.c), the arms this lane can carry.
pub fn expr_type_typmod(node: Node<'_>) -> (u32, i32) {
    match node.node_tag() {
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            (c.consttype, c.consttypmod)
        }
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            (v.vartype, v.vartypmod)
        }
        NodeTag::T_PlaceHolderVar => {
            expr_type_typmod(node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            (r.resulttype, r.resulttypmod)
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            (f.resulttype, f.resulttypmod)
        }
        NodeTag::T_CoerceToDomain => {
            let cd = node.as_coerce_to_domain().unwrap();
            (cd.resulttype, cd.resulttypmod)
        }
        NodeTag::T_OpExpr => (node.as_op_expr().unwrap().opresulttype, -1),
        NodeTag::T_DistinctExpr => (node.as_distinct_expr().unwrap().opresulttype, -1),
        NodeTag::T_BooleanTest | NodeTag::T_BoolExpr | NodeTag::T_NullTest => {
            (types_core::catalog::BOOLOID, -1)
        }
        NodeTag::T_RowExpr => (node.as_row_expr().unwrap().row_typeid, -1),
        NodeTag::T_FuncExpr => (node.as_func_expr().unwrap().funcresulttype, -1),
        NodeTag::T_Aggref => (node.as_aggref().unwrap().aggtype, -1),
        NodeTag::T_GroupingFunc => (23, -1),
        NodeTag::T_WindowFunc => (node.as_window_func().unwrap().wintype, -1),
        NodeTag::T_Param => {
            let p = node.as_param().unwrap();
            (p.paramtype, p.paramtypmod)
        }
        NodeTag::T_SQLValueFunction => {
            let svf = node.as_sql_value_function().unwrap();
            (svf.r#type, svf.typmod)
        }
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK => (sp.firstColType, sp.firstColTypmod),
                SubLinkType::ARRAY_SUBLINK => (
                    nodes_core::node_funcs::promoted_array_type(sp.firstColType),
                    sp.firstColTypmod,
                ),
                // C: a MULTIEXPR SubPlan returns a dummy NULL::record.
                SubLinkType::MULTIEXPR_SUBLINK => (::types_core::RECORDOID, -1),
                _ => (types_core::catalog::BOOLOID, -1),
            }
        }
        NodeTag::T_AlternativeSubPlan => expr_type_typmod(
            node.as_alternative_sub_plan()
                .unwrap()
                .subplans
                .first()
                .expect("alternatives"),
        ),
        NodeTag::T_SubLink => {
            use types_nodes::primnodes::SubLinkType;
            let sl = node.as_sub_link().unwrap();
            match sl.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => {
                    let tent = sl
                        .subselect
                        .as_query()
                        .unwrap_or_else(|| panic!("cannot get type for untransformed sublink"))
                        .targetList
                        .first()
                        .expect("sublink tlist")
                        .as_target_entry()
                        .expect("tlist entry");
                    let (ty, tm) = expr_type_typmod(tent.expr);
                    if sl.subLinkType == SubLinkType::ARRAY_SUBLINK {
                        (nodes_core::node_funcs::promoted_array_type(ty), tm)
                    } else {
                        (ty, tm)
                    }
                }
                _ => (types_core::catalog::BOOLOID, -1),
            }
        }
        NodeTag::T_CaseTestExpr => {
            let ct = node.as_case_test_expr().unwrap();
            (ct.typeId, ct.typeMod)
        }
        // C exprTypmod CaseExpr: typmod only when every result agrees.
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            (c.casetype, case_expr_typmod(c))
        }
        NodeTag::T_CoerceViaIO => (node.as_coerce_via_io().unwrap().resulttype, -1),
        NodeTag::T_NextValueExpr => (
            node.as_variant::<types_nodes::primnodes::NextValueExpr>()
                .unwrap()
                .typeId,
            -1,
        ),
        _ => (nodes_core::expr_type(node), nodes_core::expr_typmod(node)),
    }
}

fn case_expr_typmod(c: &types_nodes::primnodes::CaseExpr<'_>) -> i32 {
    let Some(defresult) = c.defresult else {
        return -1;
    };
    let (dtype, typmod) = expr_type_typmod(defresult);
    if dtype != c.casetype || typmod < 0 {
        return -1;
    }
    for w in &c.args {
        let result = w
            .as_case_when()
            .expect("CaseWhen")
            .result
            .expect("CaseWhen.result");
        let (rtype, rtypmod) = expr_type_typmod(result);
        if rtype != c.casetype || rtypmod != typmod {
            return -1;
        }
    }
    typmod
}

// set_rel_width (costsize.c).
pub fn set_rel_width<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let relid_idx = run.root.rel(rel).relid;
    let reloid = run.rte(relid_idx as usize).relid;
    let min_attr = run.root.rel(rel).min_attr;
    let max_attr = run.root.rel(rel).max_attr;
    let mut tuple_width: i64 = 0;
    let mut have_wholerow_var = false;

    {
        let rt = run.root.rel_reltarget_mut(rel);
        rt.cost.startup = 0.0;
        rt.cost.per_tuple = 0.0;
    }

    let exprs = match run.root.rel(rel).pathtarget_id {
        Some(id) => {
            types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(id).exprs)
        }
        None => mcx::PgVec::new_in(run.mcx),
    };

    for &node_id in exprs.iter() {
        let node = *run.root.expr_node(node_id);
        let var = match node.as_var() {
            Some(v) if v.varno as u32 == relid_idx => Some((v.varattno, v.vartype, v.vartypmod)),
            _ => None,
        };
        if let Some((varattno, vartype, vartypmod)) = var {
            debug_assert!(varattno >= min_attr && varattno <= max_attr);
            let ndx = (varattno - min_attr) as usize;
            if varattno == 0 {
                have_wholerow_var = true;
                continue;
            }
            let cached = run.root.rel(rel).attr_widths[ndx];
            if cached > 0 {
                tuple_width += cached as i64;
                continue;
            }
            if reloid != 0 && varattno > 0 {
                let item_width = lsyscache::get_attavgwidth(reloid, varattno)?;
                if item_width > 0 {
                    run.root.rel_mut(rel).attr_widths[ndx] = item_width;
                    tuple_width += item_width as i64;
                    continue;
                }
            }
            let item_width = lsyscache::get_typavgwidth(vartype, vartypmod)?;
            debug_assert!(item_width > 0);
            run.root.rel_mut(rel).attr_widths[ndx] = item_width;
            tuple_width += item_width as i64;
        } else if let Some(phv) = node.as_place_holder_var() {
            // The PHV's contained expression is evaluated while scanning this
            // rel: charge it to reltarget->cost; width from phinfo (created
            // before placeholdersFrozen, so it must exist here).
            let phinfo_id = run
                .root
                .placeholder_array
                .get(phv.phid as usize)
                .copied()
                .flatten()
                .expect("set_rel_width: PlaceHolderInfo missing");
            tuple_width += run.root.phinfo(phinfo_id).ph_width as i64;
            let cost = cost_qual_eval_node(Some(&mut *run), phv.phexpr)?;
            let rt = run.root.rel_reltarget_mut(rel);
            rt.cost.startup += cost.startup;
            rt.cost.per_tuple += cost.per_tuple;
        } else {
            // C's catch-all: an expression, or a Var of another relation (a
            // lateral reference) — width from the type, eval cost charged.
            let (typid, typmod) = expr_type_typmod(node);
            let item_width = lsyscache::get_typavgwidth(typid, typmod)?;
            debug_assert!(item_width > 0);
            tuple_width += item_width as i64;
            let cost = cost_qual_eval_node(Some(&mut *run), node)?;
            let rt = run.root.rel_reltarget_mut(rel);
            rt.cost.startup += cost.startup;
            rt.cost.per_tuple += cost.per_tuple;
        }
    }

    if have_wholerow_var {
        let mut wholerow_width: i64 =
            types_tuple::MAXALIGN(types_tuple::SizeofHeapTupleHeader) as i64;
        if reloid != 0 {
            let relation = table::table_open(run.mcx, reloid, types_rel::NoLock)?;
            let empty = mcx::PgVec::new_in(run.mcx);
            let mut widths = core::mem::replace(&mut run.root.rel_mut(rel).attr_widths, empty);
            wholerow_width +=
                planner_seams::get_rel_data_width::call(&relation, Some(&mut widths), min_attr)?
                    as i64;
            run.root.rel_mut(rel).attr_widths = widths;
            relation.close(types_rel::NoLock)?;
        } else {
            for i in 1..=max_attr {
                wholerow_width += run.root.rel(rel).attr_widths[(i - min_attr) as usize] as i64;
            }
        }
        let clamped = clamp_width_est(wholerow_width);
        run.root.rel_mut(rel).attr_widths[(0 - min_attr) as usize] = clamped;
        tuple_width += wholerow_width;
    }

    let width = clamp_width_est(tuple_width);
    run.root.rel_reltarget_mut(rel).width = width;
    Ok(())
}

const LOG2_DIVISOR: f64 = 0.693147180559945;
fn log2(x: f64) -> f64 {
    x.ln() / LOG2_DIVISOR
}

// tuplesort_merge_order (tuplesort.c); consts pinned to tuplesort.c:176-179.
fn tuplesort_merge_order(allowed_mem: i64) -> f64 {
    const MINORDER: i64 = 6;
    const MAXORDER: i64 = 500;
    const TAPE_BUFFER_OVERHEAD: i64 = BLCKSZ as i64;
    const MERGE_BUFFER_SIZE: i64 = BLCKSZ as i64 * 32;
    (allowed_mem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)).clamp(MINORDER, MAXORDER) as f64
}

fn cost_tuplesort(
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> (f64, f64) {
    let input_bytes = relation_byte_size(tuples, width);
    let sort_mem_bytes = sort_mem as i64 * 1024;
    let tuples = tuples.max(2.0);
    let comparison_cost = comparison_cost + 2.0 * gucs::cpu_operator_cost();

    let (output_tuples, output_bytes) = if limit_tuples > 0.0 && limit_tuples < tuples {
        (limit_tuples, relation_byte_size(limit_tuples, width))
    } else {
        (tuples, input_bytes)
    };

    let startup_cost = if output_bytes > sort_mem_bytes as f64 {
        let npages = (input_bytes / BLCKSZ as f64).ceil();
        let nruns = input_bytes / sort_mem_bytes as f64;
        let mergeorder = tuplesort_merge_order(sort_mem_bytes);
        let log_runs = if nruns > mergeorder {
            (nruns.ln() / mergeorder.ln()).ceil()
        } else {
            1.0
        };
        let npageaccesses = 2.0 * npages * log_runs;
        comparison_cost * tuples * log2(tuples)
            + npageaccesses * (gucs::seq_page_cost() * 0.75 + gucs::random_page_cost() * 0.25)
    } else if tuples > 2.0 * output_tuples || input_bytes > sort_mem_bytes as f64 {
        comparison_cost * tuples * log2(2.0 * output_tuples)
    } else {
        comparison_cost * tuples * log2(tuples)
    };
    (startup_cost, gucs::cpu_operator_cost() * tuples)
}

/// The cost_sort computation without a Path to write into (C's dummy
/// `Path sort_path` callers); returns (disabled_nodes, startup, total).
#[allow(clippy::too_many_arguments)]
pub fn cost_sort_shape(
    input_disabled_nodes: i32,
    input_cost: f64,
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> (i32, f64, f64) {
    let (startup, run_cost) =
        cost_tuplesort(tuples, width, comparison_cost, sort_mem, limit_tuples);
    let startup_cost = startup + input_cost;
    (
        input_disabled_nodes + if gucs::enable_sort() { 0 } else { 1 },
        startup_cost,
        startup_cost + run_cost,
    )
}

/// cost_incremental_sort (costsize.c) without a Path to write into; returns
/// (disabled_nodes, startup, total, rows).
#[allow(clippy::too_many_arguments)]
pub fn cost_incremental_sort_shape<'mcx>(
    run: &mut PlannerRun<'mcx>,
    pathkeys: &[types_pathnodes::PathKey],
    presorted_keys: usize,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> PgResult<(i32, f64, f64, f64)> {
    debug_assert!(presorted_keys > 0 && presorted_keys < pathkeys.len());
    let input_run_cost = input_total_cost - input_startup_cost;
    let input_tuples = input_tuples.max(2.0);
    let mut input_groups = input_tuples.min(types_pathnodes::DEFAULT_NUM_DISTINCT);

    let mcx = run.mcx;
    let mut presorted_exprs: mcx::PgVec<'_, (types_pathnodes::NodeId, Node<'mcx>)> =
        mcx::PgVec::new_in(mcx);
    let mut unknown_varno = false;
    for (i, key) in pathkeys.iter().enumerate() {
        let ec = key.pk_eclass.expect("canonical pathkey has an eclass");
        let em_id = run.root.ec(ec).ec_members[0];
        let em_expr = run.root.em(em_id).em_expr;
        let expr = *run.root.expr_node(em_expr);
        // Vars with varno 0 (generate_append_tlist) confuse estimate_num_groups.
        if vars::pull_varnos(mcx, expr)?.is_member(0) {
            unknown_varno = true;
            break;
        }
        presorted_exprs.push((em_expr, expr));
        if i + 1 >= presorted_keys {
            break;
        }
    }
    if !unknown_varno {
        input_groups =
            planner_seams::estimate_num_groups::call(run, &presorted_exprs, input_tuples)?;
    }

    let group_tuples = input_tuples / input_groups;
    let group_input_run_cost = input_run_cost / input_groups;
    let (group_startup_cost, group_run_cost) =
        cost_tuplesort(group_tuples, width, comparison_cost, sort_mem, limit_tuples);

    let startup_cost = group_startup_cost + input_startup_cost + group_input_run_cost;
    let mut run_cost = group_run_cost
        + (group_run_cost + group_startup_cost) * (input_groups - 1.0)
        + group_input_run_cost * (input_groups - 1.0);
    run_cost += (gucs::cpu_tuple_cost() + comparison_cost) * input_tuples;
    run_cost += 2.0 * gucs::cpu_tuple_cost() * input_groups;

    debug_assert!(gucs::enable_incremental_sort());
    Ok((
        input_disabled_nodes,
        startup_cost,
        startup_cost + run_cost,
        input_tuples,
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn cost_incremental_sort<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
    pathkeys: &[types_pathnodes::PathKey],
    presorted_keys: usize,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) -> PgResult<()> {
    let (disabled_nodes, startup_cost, total_cost, rows) = cost_incremental_sort_shape(
        run,
        pathkeys,
        presorted_keys,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    )?;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn cost_sort(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    input_disabled_nodes: i32,
    input_cost: f64,
    tuples: f64,
    width: i32,
    comparison_cost: f64,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (disabled_nodes, startup_cost, total_cost) = cost_sort_shape(
        input_disabled_nodes,
        input_cost,
        tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = tuples;
    p.disabled_nodes = disabled_nodes;
    p.startup_cost = startup_cost;
    p.total_cost = total_cost;
}

// cost_windowagg (costsize.c).
#[allow(clippy::too_many_arguments)]
pub fn cost_windowagg<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
    window_funcs: &[Node<'mcx>],
    wc_node: Node<'mcx>,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    input_tuples: f64,
) -> PgResult<()> {
    let wc = wc_node.as_window_clause().expect("WindowClause");
    let num_part_cols = wc.partitionClause.len();
    let num_order_cols = wc.orderClause.len();

    let mut startup_cost = input_startup_cost;
    let mut total_cost = input_total_cost;
    for wf_node in window_funcs {
        let wf = wf_node.as_window_func().expect("WindowFunc");
        let mut argcosts = QualCost::default();
        planner_seams::add_function_cost::call(wf.winfnoid, &mut argcosts)?;
        startup_cost += argcosts.startup;
        let mut wfunccost = argcosts.per_tuple;
        let mut argcosts = QualCost::default();
        for arg in &wf.args {
            let c = cost_qual_eval_node(Some(&mut *run), arg)?;
            argcosts.startup += c.startup;
            argcosts.per_tuple += c.per_tuple;
        }
        startup_cost += argcosts.startup;
        wfunccost += argcosts.per_tuple;
        if let Some(f) = wf.aggfilter {
            let c = cost_qual_eval_node(Some(&mut *run), f)?;
            startup_cost += c.startup;
            wfunccost += c.per_tuple;
        }
        total_cost += wfunccost * input_tuples;
    }

    total_cost +=
        gucs::cpu_operator_cost() * (num_part_cols + num_order_cols) as f64 * input_tuples;
    total_cost += gucs::cpu_tuple_cost() * input_tuples;

    {
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = input_tuples;
        p.disabled_nodes = input_disabled_nodes;
        p.startup_cost = startup_cost;
        p.total_cost = total_cost;
    }

    let startup_tuples = get_windowclause_startup_tuples(run, wc_node, input_tuples)?;
    if startup_tuples > 1.0 {
        let p = run.root.path_mut(path_id).base_mut();
        p.startup_cost += (total_cost - startup_cost) / input_tuples * (startup_tuples - 1.0);
    }
    Ok(())
}

// get_windowclause_startup_tuples (costsize.c).
fn get_windowclause_startup_tuples<'mcx>(
    run: &mut PlannerRun<'mcx>,
    wc_node: Node<'mcx>,
    input_tuples: f64,
) -> PgResult<f64> {
    use types_nodes::rawnodes::{
        FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET_FOLLOWING,
        FRAMEOPTION_END_OFFSET_PRECEDING, FRAMEOPTION_END_UNBOUNDED_FOLLOWING, FRAMEOPTION_GROUPS,
        FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
    };
    let wc = wc_node.as_window_clause().expect("WindowClause");
    let frame_options = wc.frameOptions;

    let partition_tuples = if !wc.partitionClause.is_nil() {
        let mut clause_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
        for n in &wc.partitionClause {
            clause_ids.push(run.intern_expr(n));
        }
        let exprs =
            types_pathnodes::run::sortgrouplist_exprs(run, &clause_ids, &run.parse().targetList);
        let num_partitions = planner_seams::estimate_num_groups::call(run, &exprs, input_tuples)?;
        input_tuples / num_partitions
    } else {
        input_tuples
    };

    let wc = wc_node.as_window_clause().expect("WindowClause");
    let peer_tuples = if !wc.orderClause.is_nil() {
        let mut clause_ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
        for n in &wc.orderClause {
            clause_ids.push(run.intern_expr(n));
        }
        let exprs =
            types_pathnodes::run::sortgrouplist_exprs(run, &clause_ids, &run.parse().targetList);
        let num_groups = planner_seams::estimate_num_groups::call(run, &exprs, partition_tuples)?;
        partition_tuples / num_groups
    } else {
        1.0
    };

    let wc = wc_node.as_window_clause().expect("WindowClause");
    let return_tuples = if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        partition_tuples
    } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
        if frame_options & FRAMEOPTION_ROWS != 0 {
            1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            if wc.orderClause.is_nil() {
                partition_tuples
            } else {
                peer_tuples
            }
        } else {
            unreachable!()
        }
    } else if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        1.0
    } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
        let end_offset_value = match wc.endOffset.and_then(|n| n.as_const()) {
            Some(c) => {
                if c.constisnull {
                    // NULL errors at execution; assume one row/range/group.
                    1.0
                } else {
                    match c.consttype {
                        types_core::catalog::INT2OID => c.constvalue.as_i16() as f64,
                        types_core::catalog::INT4OID => c.constvalue.as_i32() as f64,
                        types_core::catalog::INT8OID => c.constvalue.as_i64() as f64,
                        _ => partition_tuples / peer_tuples * types_pathnodes::DEFAULT_INEQ_SEL,
                    }
                }
            }
            None => partition_tuples / peer_tuples * types_pathnodes::DEFAULT_INEQ_SEL,
        };
        if frame_options & FRAMEOPTION_ROWS != 0 {
            end_offset_value + 1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            peer_tuples * (end_offset_value + 1.0)
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    };

    let return_tuples = if !wc.partitionClause.is_nil() || !wc.orderClause.is_nil() {
        f64::min(return_tuples + 1.0, partition_tuples)
    } else {
        f64::min(return_tuples, partition_tuples)
    };
    Ok(clamp_row_est(return_tuples))
}

const APPEND_CPU_COST_MULTIPLIER: f64 = 0.5;

// cost_append (costsize.c), serial arm; parallel append has no lane.
// append_nonpartial_cost (costsize.c): greedy assignment of the (cost-
// descending) non-partial subpaths to workers; returns the highest per-worker
// total.
fn append_nonpartial_cost(
    run: &PlannerRun<'_>,
    subpaths: &[types_pathnodes::PathId],
    numpaths: usize,
    parallel_workers: i32,
) -> f64 {
    if numpaths == 0 {
        return 0.0;
    }
    let arrlen = (parallel_workers.max(0) as usize).min(numpaths);
    let mut costarr: Vec<f64> = subpaths
        .iter()
        .take(arrlen)
        .map(|&sp| run.root.path(sp).base().total_cost)
        .collect();
    let mut min_index = arrlen - 1;
    for (path_index, &sp) in subpaths.iter().enumerate().skip(arrlen) {
        if path_index == numpaths {
            break;
        }
        costarr[min_index] += run.root.path(sp).base().total_cost;
        min_index = 0;
        for i in 0..arrlen {
            if costarr[i] < costarr[min_index] {
                min_index = i;
            }
        }
    }
    let mut max_index = 0;
    for i in 0..arrlen {
        if costarr[i] > costarr[max_index] {
            max_index = i;
        }
    }
    costarr[max_index]
}

pub fn cost_append(run: &mut PlannerRun<'_>, path_id: types_pathnodes::PathId) {
    let (subpaths, parallel_aware, pathkeys_empty) = match run.root.path(path_id) {
        types_pathnodes::PathNode::AppendPath(a) => (
            types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &a.subpaths),
            a.path.parallel_aware,
            a.path.pathkeys.is_empty(),
        ),
        _ => panic!("cost_append: not an AppendPath"),
    };
    {
        let p = run.root.path_mut(path_id).base_mut();
        p.disabled_nodes = 0;
        p.startup_cost = 0.0;
        p.total_cost = 0.0;
        p.rows = 0.0;
    }
    if subpaths.is_empty() {
        return;
    }
    if parallel_aware {
        let (first_partial, parallel_workers) = match run.root.path(path_id) {
            types_pathnodes::PathNode::AppendPath(a) => {
                (a.first_partial_path, a.path.parallel_workers)
            }
            _ => unreachable!(),
        };
        debug_assert!(pathkeys_empty);
        let parallel_divisor = get_parallel_divisor(parallel_workers);
        let mut rows = 0.0;
        let mut disabled = 0;
        let mut startup = 0.0;
        let mut total = 0.0;
        for (i, &sp) in subpaths.iter().enumerate() {
            let s = run.root.path(sp).base();
            // Startup: the cheapest-startup child among those that get a
            // worker assigned immediately.
            if i == 0 {
                startup = s.startup_cost;
            } else if (i as i32) < parallel_workers {
                startup = startup.min(s.startup_cost);
            }
            if (i as i32) < first_partial {
                rows += s.rows / parallel_divisor;
            } else {
                let subpath_divisor = get_parallel_divisor(s.parallel_workers);
                rows += s.rows * (subpath_divisor / parallel_divisor);
                total += s.total_cost;
            }
            disabled += s.disabled_nodes;
            rows = clamp_row_est(rows);
        }
        total +=
            append_nonpartial_cost(run, &subpaths, first_partial as usize, parallel_workers);
        total += gucs::cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * rows;
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = rows;
        p.disabled_nodes = disabled;
        p.startup_cost = startup;
        p.total_cost = total;
        return;
    }
    let mut rows = 0.0;
    let mut disabled = 0;
    let mut startup = 0.0;
    let mut total = 0.0;
    if pathkeys_empty {
        startup = run.root.path(subpaths[0]).base().startup_cost;
        for &sp in subpaths.iter() {
            let s = run.root.path(sp).base();
            rows += s.rows;
            disabled += s.disabled_nodes;
            total += s.total_cost;
        }
    } else {
        // Ordered append: startup is the SUM of subpath startups, and any
        // subpath not already ordered is charged a sort.
        let (pathkeys, limit_tuples) = match run.root.path(path_id) {
            types_pathnodes::PathNode::AppendPath(a) => (
                types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &a.path.pathkeys),
                a.limit_tuples,
            ),
            _ => unreachable!(),
        };
        for &sp in subpaths.iter() {
            let s = run.root.path(sp).base();
            let (s_rows, s_disabled, s_startup, s_total) =
                if pathkeys_contained_in(&pathkeys, &s.pathkeys) {
                    (s.rows, s.disabled_nodes, s.startup_cost, s.total_cost)
                } else {
                    let width = s.pathtarget_id.map_or(0, |pt| run.root.pathtarget(pt).width);
                    let (d, st, t) = cost_sort_shape(
                        s.disabled_nodes,
                        s.total_cost,
                        s.rows,
                        width,
                        0.0,
                        init_small::globals::work_mem(),
                        limit_tuples,
                    );
                    (s.rows, d, st, t)
                };
            rows += s_rows;
            disabled += s_disabled;
            startup += s_startup;
            total += s_total;
        }
    }
    total += gucs::cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * rows;
    let p = run.root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = disabled;
    p.startup_cost = startup;
    p.total_cost = total;
}

// cost_merge_append (costsize.c): N*log2(N) heap build at startup, log2(N)
// heap maintenance per tuple, two operator evals per comparison.
pub fn cost_merge_append(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    n_streams: usize,
    input_disabled_nodes: i32,
    input_startup_cost: f64,
    input_total_cost: f64,
    tuples: f64,
) {
    let n = if n_streams < 2 { 2.0 } else { n_streams as f64 };
    let log_n = log2(n);
    let comparison_cost = 2.0 * gucs::cpu_operator_cost();
    let startup_cost = comparison_cost * n * log_n;
    let mut run_cost = tuples * comparison_cost * log_n;
    run_cost += gucs::cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * tuples;
    let p = run.root.path_mut(path_id).base_mut();
    p.disabled_nodes = input_disabled_nodes;
    p.startup_cost = startup_cost + input_startup_cost;
    p.total_cost = startup_cost + run_cost + input_total_cost;
}

// cost_subqueryscan (costsize.c); param_info is always None on this lane.
pub fn cost_subqueryscan(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    rel: RelId,
    sub: &SubqueryScanInfo,
    trivial_pathtarget: bool,
) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    debug_assert!(
        run.root.rel(rel).rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY as u32
    );
    let qpquals = match run.root.path(path_id).base().param_info.as_deref() {
        Some(ppi) => {
            let mut q = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &ppi.ppi_clauses);
            for i in 0..run.root.rel(rel).baserestrictinfo.len() {
                q.push(run.root.rel(rel).baserestrictinfo[i]);
            }
            q
        }
        None => types_pathnodes::relids::pgvec_clone_shallow(
            run.mcx,
            &run.root.rel(rel).baserestrictinfo,
        ),
    };
    let selec = planner_seams::clauselist_selectivity::call(
        run,
        &qpquals,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let rows = clamp_row_est(sub.rows * selec);
    {
        let p = run.root.path_mut(path_id).base_mut();
        p.rows = rows;
        p.disabled_nodes = sub.disabled_nodes;
        p.startup_cost = sub.startup_cost;
        p.total_cost = sub.total_cost;
    }
    // With no quals and a trivial target, setrefs elides the SubqueryScan.
    if qpquals.is_empty() && trivial_pathtarget {
        return Ok(());
    }

    let qpqual_cost = get_restriction_qual_cost(run, rel, path_id)?;
    let mut startup_cost = qpqual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut run_cost = cpu_per_tuple * sub.rows;

    let target = run.root.path_pathtarget(path_id);
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * rows;

    let p = run.root.path_mut(path_id).base_mut();
    p.startup_cost += startup_cost;
    p.total_cost += startup_cost + run_cost;
    Ok(())
}

// set_subquery_size_estimates (costsize.c).
pub fn set_subquery_size_estimates(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).relid > 0);
    let idx = run
        .root
        .rel(rel)
        .subroot_idx
        .expect("subquery rel has a subroot");

    run.swap_with_rel_subroot(idx);
    let (tuples, widths) = {
        let final_rel = types_pathnodes::relids::fetch_upper_rel(
            &mut run.root,
            types_pathnodes::UPPERREL_FINAL,
        );
        let cheapest = run
            .root
            .rel(final_rel)
            .cheapest_total_path
            .expect("subquery final rel has a cheapest path");
        let tuples = run.root.path(cheapest).base().rows;
        let sub_parse = run.parse();
        let mut widths: mcx::PgVec<'_, (i16, i32)> = mcx::PgVec::new_in(run.mcx);
        for tle_node in &sub_parse.targetList {
            let te = tle_node.as_target_entry().expect("tlist cell");
            if te.resjunk {
                continue;
            }
            let mut item_width = 0;
            if let Some(v) = te.expr.as_var() {
                if sub_parse.setOperations.is_none() {
                    let subrel_id = types_pathnodes::relids::find_base_rel(&run.root, v.varno);
                    let subrel = run.root.rel(subrel_id);
                    item_width = subrel.attr_widths[(v.varattno - subrel.min_attr) as usize];
                }
            }
            widths.push((te.resno, item_width));
        }
        (tuples, widths)
    };
    run.swap_with_rel_subroot(idx);

    run.root.rel_mut(rel).tuples = tuples;
    let (min_attr, max_attr) = {
        let r = run.root.rel(rel);
        (r.min_attr, r.max_attr)
    };
    for &(resno, w) in widths.iter() {
        if resno < min_attr || resno > max_attr {
            continue;
        }
        run.root.rel_mut(rel).attr_widths[(resno - min_attr) as usize] = w;
    }
    set_baserel_size_estimates(run, rel)
}

/// Subpath fields copied out of a subquery's subroot arena (cross-root PathId
/// can't be dereferenced through the outer root).
#[derive(Clone, Copy)]
pub struct SubqueryScanInfo {
    pub rows: f64,
    pub disabled_nodes: i32,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub parallel_safe: bool,
    pub parallel_workers: i32,
}

#[derive(Default)]
pub struct JoinCostWorkspace {
    pub startup_cost: f64,
    pub total_cost: f64,
    pub run_cost: f64,
    pub disabled_nodes: i32,
    // hashjoin-only (ExecChooseHashTableSize outputs)
    pub numbuckets: i32,
    pub numbatches: i32,
    pub inner_rows_total: f64,
    // mergejoin-only (initial_cost_mergejoin outputs)
    pub inner_run_cost: f64,
    pub outer_rows: f64,
    pub inner_rows: f64,
    pub outer_skip_rows: f64,
    pub inner_skip_rows: f64,
    // nestloop SEMI/ANTI/inner_unique early-stop private data
    pub inner_rescan_run_cost: f64,
}

// cost_memoize_rescan (costsize.c); writes back mpath->est_entries.
fn cost_memoize_rescan(run: &mut PlannerRun<'_>, path: PathId) -> PgResult<(f64, f64)> {
    let (subpath, calls, param_exprs) = match run.root.path(path) {
        types_pathnodes::PathNode::MemoizePath(mp) => (
            mp.subpath.expect("Memoize subpath"),
            mp.calls,
            types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &mp.param_exprs),
        ),
        other => panic!("cost_memoize_rescan: pathtype {}", other.base().pathtype),
    };
    let (input_startup_cost, input_total_cost, tuples) = {
        let sub = run.root.path(subpath).base();
        (sub.startup_cost, sub.total_cost, sub.rows)
    };
    let width = run.root.path_pathtarget(subpath).width;

    let hash_mem_bytes = nodehash::get_hash_memory_limit() as f64;
    let mut est_entry_bytes = crate::relation_byte_size(tuples, width)
        + nodememoize::exec_estimate_cache_entry_overhead_bytes(tuples);
    for &e in param_exprs.iter() {
        est_entry_bytes += get_expr_width(run, e)? as f64;
    }
    let est_cache_entries = (hash_mem_bytes / est_entry_bytes).floor();

    let mut group_exprs: PgVec<'_, (NodeId, Node<'_>)> = PgVec::new_in(run.mcx);
    for &e in param_exprs.iter() {
        group_exprs.push((e, *run.root.expr_node(e)));
    }
    let (mut ndistinct, used_default) =
        planner_seams::estimate_num_groups_estinfo::call(run, &group_exprs, calls)?;
    // A default ndistinct makes memoization too risky: assume every call has
    // unique parameters so the path never survives add_path.
    if used_default {
        ndistinct = calls;
    }

    let est_entries = ndistinct.min(est_cache_entries).min(u32::MAX as f64) as u32;
    match run.root.path_mut(path) {
        types_pathnodes::PathNode::MemoizePath(mp) => mp.est_entries = est_entries,
        _ => unreachable!(),
    }

    let evict_ratio = 1.0 - est_cache_entries.min(ndistinct) / ndistinct;
    let hit_ratio =
        ((calls - ndistinct) / calls) * (est_cache_entries / ndistinct.max(est_cache_entries));
    debug_assert!((0.0..=1.0).contains(&hit_ratio));

    let mut total_cost = input_total_cost * (1.0 - hit_ratio) + gucs::cpu_operator_cost();
    total_cost += gucs::cpu_tuple_cost() * evict_ratio;
    // Per-tuple eviction is just a pfree: a tenth of cpu_operator_cost.
    total_cost += gucs::cpu_operator_cost() / 10.0 * evict_ratio * tuples;
    total_cost += gucs::cpu_tuple_cost() + gucs::cpu_operator_cost() * tuples;

    let mut startup_cost = input_startup_cost * (1.0 - hit_ratio);
    startup_cost += gucs::cpu_tuple_cost();

    Ok((startup_cost, total_cost))
}

pub fn cost_rescan(run: &mut PlannerRun<'_>, path: PathId) -> PgResult<(f64, f64)> {
    let p = run.root.path(path).base();
    let pathtype = p.pathtype;
    if pathtype == tag16(NodeTag::T_Material)
        || pathtype == tag16(NodeTag::T_Sort)
        || pathtype == tag16(NodeTag::T_CteScan)
        || pathtype == tag16(NodeTag::T_WorkTableScan)
    {
        // C charges Material/Sort rescans cpu_operator_cost per row and
        // CteScan/WorkTableScan rescans cpu_tuple_cost per row.
        let per_row = if pathtype == tag16(NodeTag::T_CteScan)
            || pathtype == tag16(NodeTag::T_WorkTableScan)
        {
            gucs::cpu_tuple_cost()
        } else {
            gucs::cpu_operator_cost()
        };
        let mut run_cost = per_row * p.rows;
        let width = run.root.path_pathtarget(path).width;
        let nbytes = crate::relation_byte_size(p.rows, width);
        let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
        if nbytes > work_mem_bytes {
            let npages = (nbytes / 8192.0).ceil();
            run_cost += gucs::seq_page_cost() * npages;
        }
        Ok((0.0, run_cost))
    } else if pathtype == tag16(NodeTag::T_FunctionScan) {
        // nodeFunctionscan materializes into a tuplestore: function eval is
        // all startup cost, rescans pay only the per-row freight.
        Ok((0.0, p.total_cost - p.startup_cost))
    } else if pathtype == tag16(NodeTag::T_HashJoin) {
        // Rescan of a single-batch hashjoin repays only the run cost.
        let num_batches = match run.root.path(path) {
            types_pathnodes::PathNode::HashPath(hp) => hp.num_batches,
            _ => panic!("cost_rescan: T_HashJoin pathtype on non-HashPath"),
        };
        if num_batches == 1 {
            Ok((0.0, p.total_cost - p.startup_cost))
        } else {
            Ok((p.startup_cost, p.total_cost))
        }
    } else if pathtype == tag16(NodeTag::T_Memoize) {
        cost_memoize_rescan(run, path)
    } else {
        Ok((p.startup_cost, p.total_cost))
    }
}

pub fn initial_cost_nestloop(
    run: &mut PlannerRun<'_>,
    jointype: u32,
    inner_unique: bool,
    outer_path: PathId,
    inner_path: PathId,
) -> PgResult<JoinCostWorkspace> {
    let (inner_rescan_start, inner_rescan_total) = cost_rescan(run, inner_path)?;
    let outer = run.root.path(outer_path).base();
    let inner = run.root.path(inner_path).base();
    let mut disabled_nodes = if gucs::enable_nestloop() { 0 } else { 1 };
    disabled_nodes += inner.disabled_nodes + outer.disabled_nodes;

    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;
    let outer_path_rows = outer.rows;
    startup_cost += outer.startup_cost + inner.startup_cost;
    run_cost += outer.total_cost - outer.startup_cost;
    if outer_path_rows > 1.0 {
        run_cost += (outer_path_rows - 1.0) * inner_rescan_start;
    }
    let inner_run_cost = inner.total_cost - inner.startup_cost;
    let inner_rescan_run_cost = inner_rescan_total - inner_rescan_start;

    let early_stop = matches!(
        jointype,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI
    ) || inner_unique;
    if !early_stop {
        run_cost += inner_run_cost;
        if outer_path_rows > 1.0 {
            run_cost += (outer_path_rows - 1.0) * inner_rescan_run_cost;
        }
    }

    Ok(JoinCostWorkspace {
        startup_cost,
        total_cost: startup_cost + run_cost,
        run_cost,
        disabled_nodes,
        numbatches: 1,
        inner_run_cost: if early_stop { inner_run_cost } else { 0.0 },
        inner_rescan_run_cost: if early_stop {
            inner_rescan_run_cost
        } else {
            0.0
        },
        ..Default::default()
    })
}

pub fn final_cost_nestloop(
    run: &mut PlannerRun<'_>,
    path: &mut NestPath<'_>,
    workspace: &JoinCostWorkspace,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    let outer = run.root.path(path.jpath.outerjoinpath.unwrap()).base();
    let inner = run.root.path(path.jpath.innerjoinpath.unwrap()).base();
    let outer_path_rows = if outer.rows <= 0.0 { 1.0 } else { outer.rows };
    let inner_path_rows = if inner.rows <= 0.0 { 1.0 } else { inner.rows };
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = match path.jpath.path.param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => run.root.rel(path.jpath.path.parent).rows,
    };
    if path.jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(path.jpath.path.parallel_workers);
        path.jpath.path.rows = clamp_row_est(path.jpath.path.rows / parallel_divisor);
    }

    let early_stop = matches!(
        path.jpath.jointype,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI
    ) || path.jpath.inner_unique;
    let ntuples;
    if early_stop {
        let sf = semifactors.expect("SEMI/ANTI/inner_unique costing has semifactors");
        let inner_run_cost = workspace.inner_run_cost;
        let inner_rescan_run_cost = workspace.inner_rescan_run_cost;
        let mut outer_matched_rows = (outer_path_rows * sf.outer_match_frac).round_ties_even();
        let mut outer_unmatched_rows = outer_path_rows - outer_matched_rows;
        let inner_scan_frac = 2.0 / (sf.match_count + 1.0);

        let mut nt = outer_matched_rows * inner_path_rows * inner_scan_frac;
        // has_indexed_join_quals is constantly false here: it requires a
        // parameterized inner path, and the param-path lane is loud upstream.
        nt += outer_unmatched_rows * inner_path_rows;
        run_cost += inner_run_cost;
        if outer_unmatched_rows >= 1.0 {
            outer_unmatched_rows -= 1.0;
        } else {
            outer_matched_rows -= 1.0;
        }
        if outer_matched_rows > 0.0 {
            run_cost += outer_matched_rows * inner_rescan_run_cost * inner_scan_frac;
        }
        if outer_unmatched_rows > 0.0 {
            run_cost += outer_unmatched_rows * inner_rescan_run_cost;
        }
        ntuples = nt;
    } else {
        ntuples = outer_path_rows * inner_path_rows;
    }

    let quals = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let restrict_qual_cost = crate::cost_qual_eval(run, &quals)?;
    startup_cost += restrict_qual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + restrict_qual_cost.per_tuple;
    // mul_add mirrors the C referee's fmadd (GCC fp-contract fuses
    // `cost += expr * rows`); a x.xx5 display boundary in a consumer node
    // exposes the one-ulp difference.
    run_cost = cpu_per_tuple.mul_add(ntuples, run_cost);

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost = target.cost.per_tuple.mul_add(path.jpath.path.rows, run_cost);

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

fn page_size(tuples: f64, width: i32) -> f64 {
    (crate::relation_byte_size(tuples, width) / 8192.0).ceil()
}

pub fn initial_cost_hashjoin(
    run: &PlannerRun<'_>,
    hashclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
    parallel_hash: bool,
) -> JoinCostWorkspace {
    let (o_rows, o_startup, o_total, o_disabled, o_workers) = {
        let o = run.root.path(outer_path).base();
        (o.rows, o.startup_cost, o.total_cost, o.disabled_nodes, o.parallel_workers)
    };
    let (i_rows, i_startup, i_total, i_disabled) = {
        let i = run.root.path(inner_path).base();
        (i.rows, i.startup_cost, i.total_cost, i.disabled_nodes)
    };
    let _ = i_startup;
    let mut disabled_nodes = if gucs::enable_hashjoin() { 0 } else { 1 };
    disabled_nodes += i_disabled + o_disabled;

    let num_hashclauses = hashclauses.len() as f64;
    let mut startup_cost = o_startup;
    let mut run_cost = o_total - o_startup;
    startup_cost += i_total;
    // mul_add mirrors the C referee's fmadd (GCC fp-contract on aarch64
    // fuses `cost += expr * rows`); EXPLAIN costs are byte-compared and a
    // 42.425-style display boundary exposes the one-ulp difference.
    startup_cost = (gucs::cpu_operator_cost() * num_hashclauses + gucs::cpu_tuple_cost())
        .mul_add(i_rows, startup_cost);
    run_cost = (gucs::cpu_operator_cost() * num_hashclauses).mul_add(o_rows, run_cost);

    // A parallel hash build divides inner rows across participants; undo the
    // split so hash-table sizing sees the whole relation (C initial_cost_hashjoin).
    let inner_rows_total = if parallel_hash {
        i_rows * get_parallel_divisor(run.root.path(inner_path).base().parallel_workers)
    } else {
        i_rows
    };

    let inner_width = run.root.path_pathtarget(inner_path).width;
    let (numbuckets, numbatches, _skew) = ::nodehash::exec_choose_hash_table_size(
        inner_rows_total,
        inner_width,
        true,
        parallel_hash,
        o_workers,
    );

    if numbatches > 1 {
        let outer_width = run.root.path_pathtarget(outer_path).width;
        let outerpages = page_size(o_rows, outer_width);
        let innerpages = page_size(i_rows, inner_width);
        startup_cost += gucs::seq_page_cost() * innerpages;
        run_cost += gucs::seq_page_cost() * (innerpages + 2.0 * outerpages);
    }

    JoinCostWorkspace {
        startup_cost,
        total_cost: startup_cost + run_cost,
        run_cost,
        disabled_nodes,
        numbuckets: numbuckets as i32,
        numbatches,
        inner_rows_total,
        ..Default::default()
    }
}

const DISABLE_COST: f64 = 1.0e10;

pub fn final_cost_hashjoin(
    run: &mut PlannerRun<'_>,
    path: &mut HashPath<'_>,
    workspace: &JoinCostWorkspace,
    semifactors: Option<SemiAntiJoinFactors>,
) -> PgResult<()> {
    let outer_path = path.jpath.outerjoinpath.unwrap();
    let inner_path = path.jpath.innerjoinpath.unwrap();
    let outer_path_rows = run.root.path(outer_path).base().rows;
    let inner_path_rows = run.root.path(inner_path).base().rows;
    let inner_width = run.root.path_pathtarget(inner_path).width;
    let inner_parent = run.root.path(inner_path).base().parent;
    let inner_is_unique_path = matches!(
        run.root.path(inner_path),
        types_pathnodes::PathNode::UniquePath(_)
    );
    let outer_is_unique_path = matches!(
        run.root.path(outer_path),
        types_pathnodes::PathNode::UniquePath(_)
    );

    let numbuckets = workspace.numbuckets;
    let numbatches = workspace.numbatches;
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = match path.jpath.path.param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => run.root.rel(path.jpath.path.parent).rows,
    };
    if path.jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(path.jpath.path.parallel_workers);
        path.jpath.path.rows = clamp_row_est(path.jpath.path.rows / parallel_divisor);
    }

    path.num_batches = numbatches;
    path.inner_rows_total = workspace.inner_rows_total;

    let virtualbuckets = numbuckets as f64 * numbatches as f64;

    // A unique-ified inner is assumed perfectly hashable (C's UniquePath arm).
    let mut innerbucketsize = 1.0f64;
    let mut innermcvfreq = 1.0f64;
    let inner_relids = run.root.rel(inner_parent).relids.clone();
    let hcls = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.path_hashclauses);
    let otherclauses = if inner_is_unique_path {
        innerbucketsize = 1.0 / virtualbuckets;
        innermcvfreq = 0.0;
        PgVec::new_in(run.mcx)
    } else {
        let (other, bs) = planner_seams::estimate_multivariate_bucketsize::call(
            run,
            inner_parent,
            &hcls,
        )?;
        innerbucketsize = bs;
        other
    };
    for &hcl in otherclauses.iter() {
        let right_is_inner = {
            let r = run.root.rinfo(hcl);
            types_pathnodes::relids::relids_is_subset(&r.right_relids, &inner_relids)
        };
        let (thisbucketsize, thismcvfreq) = if right_is_inner {
            let cached = run.root.rinfo(hcl).right_bucketsize;
            if cached < 0.0 {
                let clause = *run.root.expr_node(run.root.rinfo(hcl).clause);
                let rightop = clause.as_op_expr().unwrap().args.nth(1);
                let (mcv, bs) =
                    planner_seams::estimate_hash_bucket_stats::call(run, rightop, virtualbuckets)?;
                let r = run.root.rinfo_mut(hcl);
                r.right_mcvfreq = mcv;
                r.right_bucketsize = bs;
                (bs, mcv)
            } else {
                (cached, run.root.rinfo(hcl).right_mcvfreq)
            }
        } else {
            let cached = run.root.rinfo(hcl).left_bucketsize;
            if cached < 0.0 {
                let clause = *run.root.expr_node(run.root.rinfo(hcl).clause);
                let leftop = clause.as_op_expr().unwrap().args.nth(0);
                let (mcv, bs) =
                    planner_seams::estimate_hash_bucket_stats::call(run, leftop, virtualbuckets)?;
                let r = run.root.rinfo_mut(hcl);
                r.left_mcvfreq = mcv;
                r.left_bucketsize = bs;
                (bs, mcv)
            } else {
                (cached, run.root.rinfo(hcl).left_mcvfreq)
            }
        };
        if innerbucketsize > thisbucketsize {
            innerbucketsize = thisbucketsize;
        }
        if innermcvfreq > thismcvfreq {
            innermcvfreq = thismcvfreq;
        }
    }

    if crate::relation_byte_size(
        crate::clamp_row_est(inner_path_rows * innermcvfreq),
        inner_width,
    ) > ::nodehash::get_hash_memory_limit() as f64
    {
        startup_cost += DISABLE_COST;
    }

    let hash_qual_cost = crate::cost_qual_eval(run, &hcls)?;
    let joinrestrict =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let qp_qual_cost = crate::cost_qual_eval(run, &joinrestrict)?;
    let qp_startup = qp_qual_cost.startup - hash_qual_cost.startup;
    let qp_per_tuple = qp_qual_cost.per_tuple - hash_qual_cost.per_tuple;

    let early_stop = matches!(
        path.jpath.jointype,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI
    ) || path.jpath.inner_unique;
    let hashjointuples;
    if early_stop {
        let sf = semifactors.expect("SEMI/ANTI/inner_unique costing has semifactors");
        let outer_matched_rows = (outer_path_rows * sf.outer_match_frac).round_ties_even();
        let inner_scan_frac = 2.0 / (sf.match_count + 1.0);

        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_matched_rows
            * crate::clamp_row_est(inner_path_rows * innerbucketsize * inner_scan_frac)
            * 0.5;
        run_cost = (hash_qual_cost.per_tuple
            * (outer_path_rows - outer_matched_rows)
            * crate::clamp_row_est(inner_path_rows / virtualbuckets))
        .mul_add(0.05, run_cost);
        hashjointuples = if path.jpath.jointype == types_pathnodes::JOIN_ANTI {
            outer_path_rows - outer_matched_rows
        } else {
            outer_matched_rows
        };
    } else {
        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_path_rows
            * crate::clamp_row_est(inner_path_rows * innerbucketsize)
            * 0.5;

        // approx_tuple_count divergence (plain inner arm): the joinrel size
        // estimate already applies the (equijoin) hashclause selectivity, so
        // reuse it for the CPU term. Outer joins and unique-ified inputs
        // can't reuse it (null-extension clamp / semijoin row estimate), so
        // take C's approx_tuple_count directly.
        hashjointuples = if path.jpath.jointype == JOIN_INNER
            && !inner_is_unique_path
            && !outer_is_unique_path
            && path.jpath.path.parallel_workers == 0
        {
            path.jpath.path.rows
        } else {
            approx_tuple_count(run, outer_path, inner_path, &hcls)?
        };
    }

    startup_cost += qp_startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qp_per_tuple;
    run_cost = cpu_per_tuple.mul_add(hashjointuples, run_cost);

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost = target.cost.per_tuple.mul_add(path.jpath.path.rows, run_cost);

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

// ExecSupportsMarkRestore (execAmi.c), keyed on pathtype like C.
fn exec_supports_mark_restore(run: &PlannerRun<'_>, path_id: PathId) -> bool {
    let node = run.root.path(path_id);
    let pathtype = node.base().pathtype;
    if pathtype == tag16(NodeTag::T_IndexScan) || pathtype == tag16(NodeTag::T_IndexOnlyScan) {
        let types_pathnodes::PathNode::IndexPath(ip) = node else {
            panic!("index pathtype on a non-IndexPath")
        };
        return ip.indexinfo.as_ref().expect("indexinfo set").amcanmarkpos;
    }
    if pathtype == tag16(NodeTag::T_Material) || pathtype == tag16(NodeTag::T_Sort) {
        return true;
    }
    if pathtype == tag16(NodeTag::T_Result) {
        return match node {
            types_pathnodes::PathNode::ProjectionPath(pp) => {
                exec_supports_mark_restore(run, pp.subpath.expect("projection has a subpath"))
            }
            _ => false,
        };
    }
    false
}

// cached_scansel (costsize.c): mergejoinscansel memoized on the RestrictInfo
// (leaving scansel_cache unwritten cost fabled 53x on joinplan).
pub fn cached_scansel(
    run: &mut PlannerRun<'_>,
    rinfo: RinfoId,
    pathkey: &PathKey,
) -> PgResult<MergeScanSelCache> {
    let collation = run
        .root
        .ec(pathkey.pk_eclass.expect("canonical pathkey has an eclass"))
        .ec_collation;
    for cache in run.root.rinfo(rinfo).scansel_cache.iter() {
        if cache.opfamily == pathkey.pk_opfamily
            && cache.collation == collation
            && cache.cmptype == pathkey.pk_cmptype
            && cache.nulls_first == pathkey.pk_nulls_first
        {
            return Ok(*cache);
        }
    }
    let (leftstartsel, leftendsel, rightstartsel, rightendsel) =
        planner_seams::mergejoinscansel::call(
            run,
            rinfo,
            pathkey.pk_opfamily,
            pathkey.pk_cmptype,
            pathkey.pk_nulls_first,
        )?;
    let cache = MergeScanSelCache {
        opfamily: pathkey.pk_opfamily,
        collation,
        cmptype: pathkey.pk_cmptype,
        nulls_first: pathkey.pk_nulls_first,
        leftstartsel,
        leftendsel,
        rightstartsel,
        rightendsel,
    };
    run.root.rinfo_mut(rinfo).scansel_cache.push(cache);
    Ok(cache)
}

#[allow(clippy::too_many_arguments)]
pub fn initial_cost_mergejoin(
    run: &mut PlannerRun<'_>,
    jointype: u32,
    mergeclauses: &[RinfoId],
    outer_path: PathId,
    inner_path: PathId,
    outersortkeys: &[PathKey],
    innersortkeys: &[PathKey],
    outer_presorted_keys: usize,
) -> PgResult<JoinCostWorkspace> {
    let mut startup_cost = 0.0f64;
    let mut run_cost = 0.0f64;
    let outer_path_rows = run.root.path(outer_path).base().rows.max(1.0);
    let inner_path_rows = run.root.path(inner_path).base().rows.max(1.0);

    let (mut outerstartsel, mut outerendsel, mut innerstartsel, mut innerendsel);
    if !mergeclauses.is_empty() && jointype != types_pathnodes::JOIN_FULL {
        let firstclause = mergeclauses[0];
        let opathkey = if !outersortkeys.is_empty() {
            outersortkeys[0]
        } else {
            run.root.path(outer_path).base().pathkeys[0]
        };
        let ipathkey = if !innersortkeys.is_empty() {
            innersortkeys[0]
        } else {
            run.root.path(inner_path).base().pathkeys[0]
        };
        assert!(
            opathkey.pk_opfamily == ipathkey.pk_opfamily
                && run.root.ec(opathkey.pk_eclass.unwrap()).ec_collation
                    == run.root.ec(ipathkey.pk_eclass.unwrap()).ec_collation
                && opathkey.pk_cmptype == ipathkey.pk_cmptype
                && opathkey.pk_nulls_first == ipathkey.pk_nulls_first,
            "left and right pathkeys do not match in mergejoin"
        );

        let cache = cached_scansel(run, firstclause, &opathkey)?;
        let left_is_outer = types_pathnodes::relids::relids_is_subset(
            &run.root.rinfo(firstclause).left_relids,
            &run.root.rel(run.root.path(outer_path).base().parent).relids,
        );
        if left_is_outer {
            outerstartsel = cache.leftstartsel;
            outerendsel = cache.leftendsel;
            innerstartsel = cache.rightstartsel;
            innerendsel = cache.rightendsel;
        } else {
            outerstartsel = cache.rightstartsel;
            outerendsel = cache.rightendsel;
            innerstartsel = cache.leftstartsel;
            innerendsel = cache.leftendsel;
        }
        if jointype == JOIN_LEFT || jointype == types_pathnodes::JOIN_ANTI {
            outerstartsel = 0.0;
            outerendsel = 1.0;
        } else if jointype == JOIN_RIGHT || jointype == types_pathnodes::JOIN_RIGHT_ANTI {
            innerstartsel = 0.0;
            innerendsel = 1.0;
        }
    } else {
        outerstartsel = 0.0;
        innerstartsel = 0.0;
        outerendsel = 1.0;
        innerendsel = 1.0;
    }

    let outer_skip_rows = (outer_path_rows * outerstartsel).round_ties_even();
    let inner_skip_rows = (inner_path_rows * innerstartsel).round_ties_even();
    let outer_rows = crate::clamp_row_est(outer_path_rows * outerendsel);
    let inner_rows = crate::clamp_row_est(inner_path_rows * innerendsel);
    debug_assert!(outer_skip_rows <= outer_rows);
    debug_assert!(inner_skip_rows <= inner_rows);

    let outerstartsel = outer_skip_rows / outer_path_rows;
    let innerstartsel = inner_skip_rows / inner_path_rows;
    let outerendsel = outer_rows / outer_path_rows;
    let innerendsel = inner_rows / inner_path_rows;

    let mut disabled_nodes = if gucs::enable_mergejoin() { 0 } else { 1 };

    let work_mem = init_small::globals::work_mem();
    if !outersortkeys.is_empty() {
        debug_assert!(!pathkeys_contained_in(
            outersortkeys,
            &run.root.path(outer_path).base().pathkeys
        ));
        let outer = run.root.path(outer_path).base();
        let (o_disabled, o_startup, o_total) =
            (outer.disabled_nodes, outer.startup_cost, outer.total_cost);
        let width = run.root.path_pathtarget(outer_path).width;
        let (sort_disabled, sort_startup, sort_total) =
            if gucs::enable_incremental_sort() && outer_presorted_keys > 0 {
                let (d, s, t, _) = crate::cost_incremental_sort_shape(
                    run,
                    outersortkeys,
                    outer_presorted_keys,
                    o_disabled,
                    o_startup,
                    o_total,
                    outer_path_rows,
                    width,
                    0.0,
                    work_mem,
                    -1.0,
                )?;
                (d, s, t)
            } else {
                crate::cost_sort_shape(
                    o_disabled,
                    o_total,
                    outer_path_rows,
                    width,
                    0.0,
                    work_mem,
                    -1.0,
                )
            };
        disabled_nodes += sort_disabled;
        startup_cost += sort_startup;
        startup_cost += (sort_total - sort_startup) * outerstartsel;
        run_cost += (sort_total - sort_startup) * (outerendsel - outerstartsel);
    } else {
        let outer = run.root.path(outer_path).base();
        disabled_nodes += outer.disabled_nodes;
        startup_cost += outer.startup_cost;
        startup_cost += (outer.total_cost - outer.startup_cost) * outerstartsel;
        run_cost += (outer.total_cost - outer.startup_cost) * (outerendsel - outerstartsel);
    }

    let inner_run_cost;
    if !innersortkeys.is_empty() {
        debug_assert!(!pathkeys_contained_in(
            innersortkeys,
            &run.root.path(inner_path).base().pathkeys
        ));
        let inner = run.root.path(inner_path).base();
        let (i_disabled, i_total) = (inner.disabled_nodes, inner.total_cost);
        let width = run.root.path_pathtarget(inner_path).width;
        let (sort_disabled, sort_startup, sort_total) = crate::cost_sort_shape(
            i_disabled,
            i_total,
            inner_path_rows,
            width,
            0.0,
            work_mem,
            -1.0,
        );
        disabled_nodes += sort_disabled;
        startup_cost += sort_startup;
        startup_cost += (sort_total - sort_startup) * innerstartsel;
        inner_run_cost = (sort_total - sort_startup) * (innerendsel - innerstartsel);
    } else {
        let inner = run.root.path(inner_path).base();
        disabled_nodes += inner.disabled_nodes;
        startup_cost += inner.startup_cost;
        startup_cost += (inner.total_cost - inner.startup_cost) * innerstartsel;
        inner_run_cost = (inner.total_cost - inner.startup_cost) * (innerendsel - innerstartsel);
    }

    Ok(JoinCostWorkspace {
        disabled_nodes,
        startup_cost,
        total_cost: startup_cost + run_cost + inner_run_cost,
        run_cost,
        inner_run_cost,
        outer_rows,
        inner_rows,
        outer_skip_rows,
        inner_skip_rows,
        ..Default::default()
    })
}

// approx_tuple_count (costsize.c).
pub fn approx_tuple_count(
    run: &mut PlannerRun<'_>,
    outer_path: PathId,
    inner_path: PathId,
    quals: &[RinfoId],
) -> PgResult<f64> {
    let outer_tuples = run.root.path(outer_path).base().rows;
    let inner_tuples = run.root.path(inner_path).base().rows;
    let outer_relids = types_pathnodes::relids::relids_copy(
        run.mcx,
        &run.root.rel(run.root.path(outer_path).base().parent).relids,
    );
    let inner_relids = types_pathnodes::relids::relids_copy(
        run.mcx,
        &run.root.rel(run.root.path(inner_path).base().parent).relids,
    );
    let sjinfo = types_pathnodes::run::init_dummy_sjinfo(run, outer_relids, inner_relids);
    let mut selec = 1.0f64;
    for &q in quals {
        selec *= planner_seams::clause_selectivity::call(run, q, 0, JOIN_INNER, Some(&sjinfo))?;
    }
    Ok(crate::clamp_row_est(selec * outer_tuples * inner_tuples))
}

pub fn final_cost_mergejoin(
    run: &mut PlannerRun<'_>,
    path: &mut MergePath<'_>,
    workspace: &JoinCostWorkspace,
    inner_unique: bool,
) -> PgResult<()> {
    let outer_path = path.jpath.outerjoinpath.unwrap();
    let inner_path = path.jpath.innerjoinpath.unwrap();
    let inner_path_rows = run.root.path(inner_path).base().rows.max(1.0);
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;
    let inner_run_cost = workspace.inner_run_cost;
    let outer_rows = workspace.outer_rows;
    let inner_rows = workspace.inner_rows;
    let outer_skip_rows = workspace.outer_skip_rows;
    let inner_skip_rows = workspace.inner_skip_rows;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = match path.jpath.path.param_info.as_deref() {
        Some(ppi) => ppi.ppi_rows,
        None => run.root.rel(path.jpath.path.parent).rows,
    };
    if path.jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(path.jpath.path.parallel_workers);
        path.jpath.path.rows = clamp_row_est(path.jpath.path.rows / parallel_divisor);
    }

    let mergeclauses =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.path_mergeclauses);
    let restrictinfos =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &path.jpath.joinrestrictinfo);
    let merge_qual_cost = crate::cost_qual_eval(run, &mergeclauses)?;
    let mut qp_qual_cost = crate::cost_qual_eval(run, &restrictinfos)?;
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    let early_stop = matches!(
        path.jpath.jointype,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI
    ) || inner_unique;
    path.skip_mark_restore =
        early_stop && path.jpath.joinrestrictinfo.len() == path.path_mergeclauses.len();

    let mergejointuples = approx_tuple_count(run, outer_path, inner_path, &mergeclauses)?;

    let rescannedtuples = if path.skip_mark_restore {
        0.0
    } else {
        (mergejointuples - inner_path_rows).max(0.0)
    };
    let rescanratio = 1.0 + rescannedtuples / inner_rows;

    let bare_inner_cost = inner_run_cost * rescanratio;
    let mat_inner_cost = inner_run_cost + gucs::cpu_operator_cost() * inner_rows * rescanratio;

    let inner_width = run.root.path_pathtarget(inner_path).width;
    path.materialize_inner = if path.skip_mark_restore {
        false
    } else if gucs::enable_material() && mat_inner_cost < bare_inner_cost {
        true
    } else if path.innersortkeys.is_empty() && !exec_supports_mark_restore(run, inner_path) {
        true
    } else if gucs::enable_material()
        && !path.innersortkeys.is_empty()
        && crate::relation_byte_size(inner_path_rows, inner_width)
            > init_small::globals::work_mem() as f64 * 1024.0
    {
        true
    } else {
        false
    };

    run_cost += if path.materialize_inner {
        mat_inner_cost
    } else {
        bare_inner_cost
    };

    startup_cost += merge_qual_cost.startup;
    startup_cost += merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost += merge_qual_cost.per_tuple
        * ((outer_rows - outer_skip_rows) + (inner_rows - inner_skip_rows) * rescanratio);

    startup_cost += qp_qual_cost.startup;
    let cpu_per_tuple = gucs::cpu_tuple_cost() + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    let target = run.root.pathtarget(path.jpath.path.pathtarget_id.unwrap());
    startup_cost += target.cost.startup;
    run_cost += target.cost.per_tuple * path.jpath.path.rows;

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
    Ok(())
}

// set_joinrel_size_estimates + calc_joinrel_size_estimate (costsize.c),
// INNER/LEFT/SEMI/ANTI arms. FK-matched joinclauses are dropped from the
// restrictlist and estimated with FK semantics; outer joins (incl. ANTI)
// split joinquals from pushed-down quals; INNER and SEMI use the whole
// restrictlist.
pub fn set_joinrel_size_estimates<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[types_pathnodes::RinfoId],
) -> PgResult<()> {
    let outer_rows = run.root.rel(outer_rel).rows;
    let inner_rows = run.root.rel(inner_rel).rows;
    let nrows = calc_joinrel_size_estimate(
        run, joinrel, outer_rel, inner_rel, outer_rows, inner_rows, sjinfo, restrictlist,
    )?;
    run.root.rel_mut(joinrel).rows = nrows;
    Ok(())
}

pub fn get_parameterized_joinrel_size<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    outer_path: types_pathnodes::PathId,
    inner_path: types_pathnodes::PathId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrict_clauses: &[types_pathnodes::RinfoId],
) -> PgResult<f64> {
    let outer_rel = run.root.path(outer_path).base().parent;
    let inner_rel = run.root.path(inner_path).base().parent;
    let outer_rows = run.root.path(outer_path).base().rows;
    let inner_rows = run.root.path(inner_path).base().rows;
    let mut nrows = calc_joinrel_size_estimate(
        run, rel, outer_rel, inner_rel, outer_rows, inner_rows, sjinfo, restrict_clauses,
    )?;
    if nrows > run.root.rel(rel).rows {
        nrows = run.root.rel(rel).rows;
    }
    Ok(nrows)
}

// get_foreign_key_join_selectivity (costsize.c): substitute estimate for join
// clauses matched to FK constraints, removing them from the worklist; 1.0
// when there are none.
fn get_foreign_key_join_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    worklist: &mut PgVec<'mcx, types_pathnodes::RinfoId>,
) -> PgResult<f64> {
    use types_pathnodes::relids;
    let mcx = run.mcx;
    let mut fkselec = 1.0f64;
    let jointype = sjinfo.jointype;
    let fkeys = {
        let mut v: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
        v.extend(run.root.fkey_list.iter().copied());
        v
    };
    for &fkid in fkeys.iter() {
        let (con_relid, ref_relid, nkeys) = {
            let fk = run.root.foreign_key(fkid);
            (fk.con_relid as i32, fk.ref_relid as i32, fk.nkeys as usize)
        };
        let outer_relids = &run.root.rel(outer_rel).relids;
        let inner_relids = &run.root.rel(inner_rel).relids;
        // Relevant only if the FK connects a baserel on one side of this
        // join to a baserel on the other side.
        let ref_is_outer = if relids::relids_is_member(con_relid, outer_relids)
            && relids::relids_is_member(ref_relid, inner_relids)
        {
            false
        } else if relids::relids_is_member(ref_relid, outer_relids)
            && relids::relids_is_member(con_relid, inner_relids)
        {
            true
        } else {
            continue;
        };
        // Semi/anti: the FK only tells us the fraction of outer rows with
        // matches when the referenced rel is exactly the inside of the join.
        if (jointype == types_pathnodes::JOIN_SEMI || jointype == types_pathnodes::JOIN_ANTI)
            && (ref_is_outer || relids::relids_singleton_member(inner_relids).is_none())
        {
            continue;
        }
        let mut removedlist: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(mcx);
        let mut kept: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(mcx);
        for &rid in worklist.iter() {
            let mut remove_it = false;
            for i in 0..nkeys {
                if let Some(parent_ec) = run.root.rinfo(rid).parent_ec {
                    // Any clause derived from the matched EC counts as
                    // matching the FK: equivclass.c could equally have
                    // generated one equating the FK's Vars. EC-derived
                    // clauses without parent_ec compare non-Var expressions
                    // and can't match the FK anyway.
                    if run.root.foreign_key(fkid).eclass[i] == Some(parent_ec) {
                        remove_it = true;
                        break;
                    }
                } else if run.root.foreign_key(fkid).rinfos[i].contains(&rid) {
                    remove_it = true;
                    break;
                }
            }
            if remove_it {
                removedlist.push(rid);
            } else {
                kept.push(rid);
            }
        }
        // If we failed to remove all the clauses we expected (a const EC
        // generates no join clause at all; a previous FK may have consumed a
        // shared EC-derived clause), applying this FK's selectivity would
        // double-count: put the removed clauses back and punt.
        let expected = {
            let fk = run.root.foreign_key(fkid);
            fk.nmatched_ec - fk.nconst_ec + fk.nmatched_ri
        };
        if removedlist.is_empty() || removedlist.len() != expected as usize {
            kept.extend(removedlist.iter().copied());
            *worklist = kept;
            continue;
        }
        *worklist = kept;
        // Each referencing row matches exactly one referenced-table row; the
        // null-fraction and inheritance derates are skipped as in C.
        let ref_rel = relids::find_base_rel(&run.root, ref_relid);
        let ref_tuples = run.root.rel(ref_rel).tuples.max(1.0);
        if jointype == types_pathnodes::JOIN_SEMI || jointype == types_pathnodes::JOIN_ANTI {
            // Referenced table exactly the inside of the join: selectivity is
            // that of its restriction clauses, rows / tuples.
            fkselec *= run.root.rel(ref_rel).rows / ref_tuples;
        } else {
            fkselec *= 1.0 / ref_tuples;
        }
        // Columns in ec_has_const ECs got "var = const" restrictions on both
        // input rels; divide out the referencing Var's restriction
        // selectivity so it isn't double-counted.
        if run.root.foreign_key(fkid).nconst_ec > 0 {
            for i in 0..nkeys {
                let Some(ec) = run.root.foreign_key(fkid).eclass[i] else { continue };
                if !run.root.ec(ec).ec_has_const {
                    continue;
                }
                let em = run.root.foreign_key(fkid).fk_eclass_member[i]
                    .expect("matched EC column carries fk_eclass_member");
                if let Some(rinfo) =
                    planner_seams::find_derived_clause_for_ec_member::call(run, ec, em)
                {
                    let s0 = planner_seams::clause_selectivity::call(
                        run,
                        rinfo,
                        0,
                        jointype,
                        Some(sjinfo),
                    )?;
                    if s0 > 0.0 {
                        fkselec /= s0;
                    }
                }
            }
        }
    }
    Ok(fkselec.clamp(0.0, 1.0))
}

#[allow(clippy::too_many_arguments)]
fn calc_joinrel_size_estimate<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    outer_rows: f64,
    inner_rows: f64,
    sjinfo: &SpecialJoinInfo<'mcx>,
    restrictlist: &[types_pathnodes::RinfoId],
) -> PgResult<f64> {
    let jointype = sjinfo.jointype;
    let mut fk_worklist = {
        let mut v: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
        v.extend(restrictlist.iter().copied());
        v
    };
    let fkselec =
        get_foreign_key_join_selectivity(run, outer_rel, inner_rel, sjinfo, &mut fk_worklist)?;
    let restrictlist = &fk_worklist[..];
    let is_outer = is_outer_join(jointype);
    let (jselec, pselec) = if is_outer {
        let joinrelids =
            types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(joinrel).relids);
        let mut joinquals: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
        let mut pushedquals: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
        for &rid in restrictlist {
            if types_pathnodes::run::rinfo_is_pushed_down(run, rid, &joinrelids) {
                pushedquals.push(rid);
            } else {
                joinquals.push(rid);
            }
        }
        let jselec = planner_seams::clauselist_selectivity::call(
            run,
            &joinquals,
            0,
            jointype,
            Some(sjinfo),
        )?;
        let pselec = planner_seams::clauselist_selectivity::call(
            run,
            &pushedquals,
            0,
            jointype,
            Some(sjinfo),
        )?;
        (jselec, pselec)
    } else {
        let jselec = planner_seams::clauselist_selectivity::call(
            run,
            restrictlist,
            0,
            jointype,
            Some(sjinfo),
        )?;
        (jselec, 0.0)
    };
    let nrows = match jointype {
        JOIN_INNER => outer_rows * inner_rows * fkselec * jselec,
        JOIN_LEFT => {
            let mut nrows = outer_rows * inner_rows * fkselec * jselec;
            if nrows < outer_rows {
                nrows = outer_rows;
            }
            nrows * pselec
        }
        types_pathnodes::JOIN_SEMI => outer_rows * fkselec * jselec,
        types_pathnodes::JOIN_ANTI => outer_rows * (1.0 - fkselec * jselec) * pselec,
        types_pathnodes::JOIN_FULL => {
            let mut nrows = outer_rows * inner_rows * fkselec * jselec;
            if nrows < outer_rows {
                nrows = outer_rows;
            }
            if nrows < inner_rows {
                nrows = inner_rows;
            }
            nrows * pselec
        }
        other => panic!("calc_joinrel_size_estimate (costsize.c): jointype {other}"),
    };
    Ok(crate::clamp_row_est(nrows))
}

#[cfg(test)]
mod tests {
    /// GL-GMLEADER-1 pins (the leader-consumption floor, exhaustive over
    /// the uplift's regions): the product defaults uplift GM rows to C's
    /// per-row rate; C-parity sessions (rate >= the floor) and
    /// explicitly-zeroed transport (the forced-plan bench seams) pay
    /// ZERO delta; the floored total equals the same session at
    /// parallel_tuple_cost == the floor (the GL-STRAGG-2 bisection
    /// equivalence — ptc=0.1 ALONE restored C's election, and this term
    /// reproduces exactly that arithmetic at the default rates).
    #[test]
    fn gm_leader_floor_prices_leader_consumption() {
        if std::env::var("PGRUST_GM_LEADER_MIN_TUPLE_COST").is_ok() {
            return; // env-swept run; the pin targets the default posture
        }
        let floor = gucs::gm_leader_min_tuple_cost();
        assert_eq!(floor, 0.1, "the floor IS C's parallel_tuple_cost default");
        assert_eq!(floor, gucs::DEFAULT_GM_LEADER_MIN_TUPLE_COST);
        let rows = 6_680_000.0; // the witnessed catastrophe stream
        // Product default (heap 0.01): uplift = (0.1-0.01)*rows*1.05 —
        // the exact delta the witnessed ptc=0.1 bisection added.
        let up = super::gm_leader_uplift(0.01, rows);
        assert!((up - 0.09 * rows * 1.05).abs() < 1e-6);
        let bisection_total = 0.1 * rows * 1.05;
        assert!((0.01 * rows * 1.05 + up - bisection_total).abs() < 1e-6);
        // pgrcolumnar default (0.005): floored the same way.
        let up_cb = super::gm_leader_uplift(0.005, rows);
        assert!((up_cb - 0.095 * rows * 1.05).abs() < 1e-6);
        // C-parity sessions: at or above the floor, zero delta.
        assert_eq!(super::gm_leader_uplift(0.1, rows), 0.0);
        assert_eq!(super::gm_leader_uplift(0.5, rows), 0.0);
        // Zeroed-transport bench seams: exempt.
        assert_eq!(super::gm_leader_uplift(0.0, rows), 0.0);
        // Small streams barely feel it (self-scoping): a partial-agg-fed
        // GM of 63k groups x 5 participants pays ~30k units — noise
        // against the plans it rides in; the 6.68M raw-row stream pays
        // ~631k — the witnessed election-flipping magnitude.
        assert!(super::gm_leader_uplift(0.01, 315_000.0) < 31_000.0);
        assert!(super::gm_leader_uplift(0.01, rows) > 600_000.0);
    }

    /// GL-Q2829-FIX-1 pins (the plain-Gather leader-consumption floor,
    /// DEFAULT ON since the regexp-keyed-grouping-class flip): the shipped
    /// default IS the GM floor's C-parity rate, and the uplift reproduces
    /// the same flooring arithmetic (minus the Gather Merge 5% IPC
    /// factor) with the identical C-parity-session and zeroed-transport
    /// exemptions — the floored total equals the same session at
    /// parallel_tuple_cost == the floor.
    #[test]
    fn gather_leader_floor_default_on_prices_leader_consumption() {
        if std::env::var("PGRUST_GATHER_LEADER_MIN_TUPLE_COST").is_ok() {
            return; // env-swept run; the pin targets the default posture
        }
        let floor = gucs::gather_leader_min_tuple_cost();
        assert_eq!(floor, 0.1, "the floor IS C's parallel_tuple_cost default");
        assert_eq!(floor, gucs::DEFAULT_GATHER_LEADER_MIN_TUPLE_COST);
        assert_eq!(floor, gucs::DEFAULT_GM_LEADER_MIN_TUPLE_COST, "GM-floor derivation parity");
        let rows = 8_713_000.0; // the witnessed ship-to-leader stream
        // pgrcolumnar default (0.005): floored to the C rate.
        let up_cb = super::gather_leader_uplift(0.005, rows);
        assert!((up_cb - 0.095 * rows).abs() < 1e-6);
        assert!((0.005 * rows + up_cb - 0.1 * rows).abs() < 1e-6, "floored == C-rate total");
        // Heap default (0.01): floored the same way.
        let up = super::gather_leader_uplift(0.01, rows);
        assert!((up - 0.09 * rows).abs() < 1e-6);
        // C-parity sessions at/above the floor: zero delta.
        assert_eq!(super::gather_leader_uplift(0.1, rows), 0.0);
        assert_eq!(super::gather_leader_uplift(0.5, rows), 0.0);
        // Zeroed-transport bench seams (incl. the cost-route gate's
        // zeroed conf): exempt.
        assert_eq!(super::gather_leader_uplift(0.0, rows), 0.0);
        // Self-scoping: partial-agg-output-scale streams barely feel it;
        // the raw-row stream pays the election-flipping magnitude.
        assert!(super::gather_leader_uplift(0.005, 315_000.0) < 31_000.0);
        assert!(super::gather_leader_uplift(0.005, rows) > 800_000.0);
    }

    use super::*;

    // -- Step-0a: pgrcolumnar Gather pricing is GUC-anchored -----------------

    /// The anchoring contract: at the shipped t34 defaults the GUC-anchored
    /// prices reproduce the retired flat constants EXACTLY (bit-for-bit), so
    /// default-config plans and EXPLAIN costs are unchanged by step 0a.
    #[test]
    fn pgrcolumnar_gather_pricing_anchored_at_defaults() {
        assert_eq!(gucs::DEFAULT_PGRCOLUMNAR_PARALLEL_SETUP_COST, 32000.0);
        assert_eq!(gucs::DEFAULT_PGRCOLUMNAR_PARALLEL_TUPLE_COST, 0.005);
        // Session cells boot at the defaults (thread-local, untouched here).
        assert_eq!(gucs::pgrcolumnar_parallel_setup_cost(), 32000.0);
        assert_eq!(gucs::pgrcolumnar_parallel_tuple_cost(), 0.005);
    }

    /// The regression probe's "A/B inert" bug stays fixed: sweeping the parallel
    /// cost GUCs moves the pgrcolumnar Gather prices proportionally.
    #[test]
    fn pgrcolumnar_gather_pricing_tracks_parallel_gucs() {
        gucs::set_parallel_setup_cost(1000.0);
        gucs::set_parallel_tuple_cost(0.1);
        assert_eq!(gucs::pgrcolumnar_parallel_setup_cost(), 320_000.0);
        assert_eq!(gucs::pgrcolumnar_parallel_tuple_cost(), 0.05);
        gucs::set_parallel_setup_cost(guc_tables::consts::DEFAULT_PARALLEL_SETUP_COST);
        gucs::set_parallel_tuple_cost(guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST);
    }

    // -- Step-0b: leader-hashagg-over-Gather spill delta ---------------------

    fn install_seams_once() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            if !guc_tables::vars::work_mem.installed() {
                init_small::init_seams();
            }
        });
    }

    fn scratch_path<'m>(mcx: mcx::Mcx<'m>, rel: RelId) -> types_pathnodes::Path<'m> {
        types_pathnodes::Path {
            type_: 0,
            pathtype: 0,
            parent: rel,
            pathtarget_id: None,
            param_info: None,
            parallel_aware: false,
            parallel_safe: true,
            parallel_workers: 0,
            rows: 0.0,
            disabled_nodes: 0,
            startup_cost: 1000.0,
            total_cost: 2000.0,
            pathkeys: mcx::PgVec::new_in(mcx),
        }
    }

    // (run, agg_path_id, gather_path_id): an AGG-shaped scratch path above a
    // Gather above a scan-shaped scratch path, over one baserel whose
    // amflags carry `columnar`.
    fn leader_hashagg_fixture<'m>(
        mcx: mcx::Mcx<'m>,
        columnar: bool,
    ) -> (PlannerRun<'m>, types_pathnodes::PathId, types_pathnodes::PathId) {
        let mut run = PlannerRun::new(mcx);
        let mut rel = types_pathnodes::RelOptInfo::new(mcx);
        if columnar {
            rel.amflags |= types_pathnodes::AMFLAG_PGRCOLUMNAR;
        }
        let rid = run.root.alloc_rel(rel);
        run.root.simple_rel_array.push(Some(rid));
        let inner = run.root.alloc_path(PathNode::Path(scratch_path(mcx, rid)));
        let gather = run.root.alloc_path(PathNode::GatherPath(types_pathnodes::GatherPath {
            path: scratch_path(mcx, rid),
            subpath: Some(inner),
            single_copy: false,
            num_workers: 16,
        }));
        let agg = run.root.alloc_path(PathNode::Path(scratch_path(mcx, rid)));
        (run, agg, gather)
    }

    // high-card grouped shape scaled to the unit-test budget: work_mem 4096kB x
    // hash_mem_multiplier 2.0 (init_small boot values) = 8.39MB. Entry size
    // for width 12 / no transinfos / transitionSpace 0 is 16 + MAXALIGN(
    // MAXALIGN(15) + 12) = 48B: at 100k groups the C estimate says 4.8MB
    // (fits, no spill term) while the honest 1.8x scale says 8.64MB
    // (spills) — the exact leader-spill disease in miniature.
    const SPILLY_GROUPS: f64 = 100_000.0;
    const TINY_GROUPS: f64 = 10_000.0;
    const INPUT_TUPLES: f64 = 1_000_000.0;
    const INPUT_WIDTH: i32 = 12;

    #[test]
    fn leader_hashagg_over_gather_prices_the_spill() {
        install_seams_once();
        let cx = mcx::MemoryContext::new_bump("costsize-test");
        let mcx = cx.mcx();
        let (mut run, agg, gather) = leader_hashagg_fixture(mcx, true);
        let costs = types_pathnodes::AggClauseCosts::default();

        // Precondition (the leader-spill disease): C's own pricing sees no spill.
        let (s_base, t_base) = hashed_agg_spill_surcharge_scaled(
            &run, &costs, SPILLY_GROUPS, INPUT_TUPLES, INPUT_WIDTH, 1.0,
        );
        assert_eq!((s_base, t_base), (0.0, 0.0), "C estimate must fit the 8MB budget");

        let (before_s, before_t) = {
            let p = run.root.path(agg).base();
            (p.startup_cost, p.total_cost)
        };
        cost_agg_leader_spill_adjust(
            &mut run,
            agg,
            types_pathnodes::AGG_HASHED,
            gather,
            &costs,
            SPILLY_GROUPS,
            INPUT_TUPLES,
            INPUT_WIDTH,
        );
        let p = run.root.path(agg).base();
        assert!(
            p.startup_cost > before_s && p.total_cost > before_t,
            "honest scale must price the spill: {} / {}",
            p.startup_cost,
            p.total_cost
        );
        // total picks up the read-back leg on top of startup's write leg.
        assert!(p.total_cost - before_t > p.startup_cost - before_s);
    }

    #[test]
    fn leader_hashagg_spill_delta_is_exact_noop_when_it_fits() {
        install_seams_once();
        let cx = mcx::MemoryContext::new_bump("costsize-test");
        let mcx = cx.mcx();
        let (mut run, agg, gather) = leader_hashagg_fixture(mcx, true);
        let costs = types_pathnodes::AggClauseCosts::default();
        cost_agg_leader_spill_adjust(
            &mut run,
            agg,
            types_pathnodes::AGG_HASHED,
            gather,
            &costs,
            TINY_GROUPS,
            INPUT_TUPLES,
            INPUT_WIDTH,
        );
        let p = run.root.path(agg).base();
        assert_eq!((p.startup_cost, p.total_cost), (1000.0, 2000.0));
    }

    /// The two fleet anchors of the entry-scale constant, pinned at the REAL
    /// probed 10M-group shape as hash_agg_entry_size itself prices it (Gather width 16,
    /// three transinfos for count(*)+SUM+AVG, transitionSpace 48 from AVG's
    /// by-ref int8-array transvalue — jobs -5fd0/-07cf explain captures +
    /// prepagg derivation): at the default scale the 10M-group working set
    /// must cross the 512MB-arm budget (2.15GB: spill priced — the cliff)
    /// and FIT the 1GB-arm budget (4.29GB: delta 0 — the byte-identical
    /// bar). Two prior constants (4.7, 3.2) failed the second anchor by
    /// deriving against hand models of the entry (64B/96B) instead of the
    /// function's real 168B output — both caught by the fleet bar; this
    /// test makes the third such miss a red test instead.
    #[test]
    fn leader_hashagg_entry_scale_reproduces_q33_fleet_anchors() {
        let entry = ::nodeagg::hash_agg_entry_size(3, 16, 48);
        assert_eq!(entry, 168.0);
        let scale = gucs::DEFAULT_PGRCOLUMNAR_LEADER_HASHAGG_ENTRY_SCALE;
        let working_set = 10_000_000.0 * entry * scale;
        let budget_512mb = 524288.0 * 1024.0 * 4.0; // work_mem 512MB x hmm 4
        let budget_1gb = 1048576.0 * 1024.0 * 4.0; // work_mem 1GB x hmm 4
        assert!(working_set > budget_512mb, "the 10M-group shape must spill-price at 512MB");
        assert!(working_set < budget_1gb, "the 10M-group shape must stay delta-0 at 1GB");
        // Calibration target: the measured ~3.03GB leader working set
        // (probe E2 TreeRssAnon peak) within 5%.
        assert!((working_set / 3.03e9 - 1.0).abs() < 0.05);
    }

    #[test]
    fn leader_hashagg_spill_delta_skips_non_gather_and_heap() {
        install_seams_once();
        let cx = mcx::MemoryContext::new_bump("costsize-test");
        let mcx = cx.mcx();
        let costs = types_pathnodes::AggClauseCosts::default();

        // Serial hashagg (input is not a Gather): pure C costing kept.
        let (mut run, agg, gather) = leader_hashagg_fixture(mcx, true);
        let PathNode::GatherPath(g) = run.root.path(gather) else { unreachable!() };
        let inner = g.subpath.unwrap();
        cost_agg_leader_spill_adjust(
            &mut run,
            agg,
            types_pathnodes::AGG_HASHED,
            inner,
            &costs,
            SPILLY_GROUPS,
            INPUT_TUPLES,
            INPUT_WIDTH,
        );
        let p = run.root.path(agg).base();
        assert_eq!((p.startup_cost, p.total_cost), (1000.0, 2000.0));

        // Heap-only plan (no pgrcolumnar baserel): untouched bit-for-bit.
        let (mut run, agg, gather) = leader_hashagg_fixture(mcx, false);
        cost_agg_leader_spill_adjust(
            &mut run,
            agg,
            types_pathnodes::AGG_HASHED,
            gather,
            &costs,
            SPILLY_GROUPS,
            INPUT_TUPLES,
            INPUT_WIDTH,
        );
        let p = run.root.path(agg).base();
        assert_eq!((p.startup_cost, p.total_cost), (1000.0, 2000.0));
    }
}
