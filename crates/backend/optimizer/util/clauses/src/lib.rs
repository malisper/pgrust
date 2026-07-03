//! optimizer/util/clauses.c — clause inspection/classification over the
//! opaque `Node` vocabulary, plus the eval_const_expressions fold core.
//! The nodeFuncs.c walker/mutator engine lives in `nodes_core`. Unported-
//! vocab arms panic loud; the executor-evaluation leg rides
//! clauses_seams::evaluate_expr.

pub mod classify;
pub mod fold;
pub mod walker;

#[cfg(test)]
mod tests;

pub use classify::{
    commute_op_expr, contain_agg_clause, contain_context_dependent_node, contain_exec_param,
    contain_leaked_vars, contain_mutable_functions, contain_mutable_functions_after_planning,
    contain_nonstrict_functions, contain_subplans, contain_volatile_functions,
    contain_volatile_functions_after_planning, contain_volatile_functions_not_nextval,
    contain_window_function, convert_saop_to_hashed_saop, expression_returns_set_rows,
    find_forced_null_var, find_forced_null_vars, find_nonnullable_rels, find_nonnullable_vars,
    find_window_functions, is_parallel_safe, is_pseudo_constant_clause,
    is_pseudo_constant_clause_relids, max_parallel_hazard, num_relids, pull_paramids,
};
pub use fold::{
    all_arguments_const, estimate_expression_value, eval_const_expressions,
    eval_const_expressions_with_params,
};
pub use walker::{
    check_functions_in_node, expression_tree_mutator, expression_tree_walker, mutate_list,
    query_or_expression_tree_walker, query_tree_walker, range_table_entry_walker,
    range_table_walker, walk_list, walk_opt, NodeWalker,
};
