//! optimizer/util/clauses.c — clause inspection/classification over the
//! opaque `Node` vocabulary, plus the eval_const_expressions fold core.
//! The nodeFuncs.c walker/mutator engine lives in `nodes_core`. Unported-
//! vocab arms panic loud; the executor-evaluation leg rides
//! clauses_seams::evaluate_expr.

pub mod classify;
pub mod fold;
pub mod srf_inline;
pub mod walker;

#[cfg(test)]
mod tests;

pub use classify::{
    commute_op_expr, contain_agg_clause, contain_context_dependent_node, contain_exec_param, contain_exec_params,
    contain_leaked_vars, contain_mutable_functions, contain_mutable_functions_after_planning,
    contain_nonstrict_functions, contain_subplans, contain_volatile_functions,
    contain_volatile_functions_after_planning, contain_volatile_functions_not_nextval,
    contain_window_function, convert_saop_to_hashed_saop, expression_returns_set_rows,
    find_forced_null_var, find_forced_null_vars, find_nonnullable_rels, find_nonnullable_vars,
    expression_has_grouping_conflict,
    find_window_functions, is_andclause, is_notclause, is_orclause, is_parallel_safe,
    is_pseudo_constant_clause, is_pseudo_constant_clause_relids, make_andclause,
    make_ands_explicit, make_ands_implicit, make_notclause, make_orclause, make_saop_expr,
    max_parallel_hazard,
    mbms_add_member, mbms_add_members, mbms_overlap_sets, num_relids, pull_paramids,
    MultiBitmapset,
};
pub use fold::negate_clause;
pub use srf_inline::inline_set_returning_function;
pub use fold::{
    all_arguments_const, estimate_expression_value, eval_const_expressions,
    eval_const_expressions_with_params, expand_function_arguments, make_bool_const,
};
pub fn init_seams() {
    clauses_seams::eval_const_expressions::set(fold::eval_const_expressions);
}

pub use walker::{
    check_functions_in_node, expression_tree_mutator, expression_tree_walker, mutate_list,
    query_or_expression_tree_walker, query_tree_walker, range_table_entry_walker,
    range_table_walker, walk_list, walk_opt, NodeWalker,
};

// is_parallel_safe (clauses.c): the init-plan output Params of this level
// and every ancestor level are safe to reference.
fn collect_safe_param_ids(run: &types_pathnodes::run::PlannerRun<'_>) -> Vec<i32> {
    let mut safe: Vec<i32> = Vec::new();
    for root in run
        .suspended_roots
        .iter()
        .map(|s| &s.root)
        .chain(core::iter::once(&run.root))
    {
        for &ipid in root.init_plans.iter() {
            let sp = root
                .expr_node(ipid)
                .as_sub_plan()
                .expect("init_plans holds SubPlan nodes");
            safe.extend(sp.setParam.iter());
        }
    }
    safe
}

// is_parallel_safe (clauses.c) over a PathTarget's exprs; C passes the List*.
pub fn is_parallel_safe_exprs(run: &types_pathnodes::run::PlannerRun<'_>, target: types_pathnodes::PtId) -> types_error::PgResult<bool> {
    if run.glob.max_parallel_hazard == b's' as i8 && run.glob.param_exec_types.is_nil() {
        return Ok(true);
    }
    let mcx = run.mcx;
    let mut list = types_nodes::list::NodeList::nil();
    let n = run.root.pathtarget(target).exprs.len();
    for i in 0..n {
        let id = run.root.pathtarget(target).exprs[i];
        list.lappend(mcx, *run.root.expr_node(id))?;
    }
    let node = types_nodes::Node::mk_list(mcx, list)?;
    crate::is_parallel_safe(
        run.glob.max_parallel_hazard,
        run.glob.param_exec_types.is_nil(),
        collect_safe_param_ids(run),
        node,
    )
}

pub fn is_parallel_safe_opt(
    run: &types_pathnodes::run::PlannerRun<'_>,
    node: Option<types_nodes::Node<'_>>,
) -> types_error::PgResult<bool> {
    match node {
        // C is_parallel_safe(root, NULL): the walker sees nothing unsafe.
        None => Ok(true),
        Some(n) => crate::is_parallel_safe(
            run.glob.max_parallel_hazard,
            run.glob.param_exec_types.is_nil(),
            collect_safe_param_ids(run),
            n,
        ),
    }
}
