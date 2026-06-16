//! Port of `src/backend/rewrite/rewriteManip.c` — the `Var`-manipulation engine
//! the rewriter and the planner's `prepjointree`/`subselect` depend on.
//!
//! # Scope
//!
//! The full rewriteManip.c surface, over the canonical `Expr` / `Query<'mcx>`
//! model and the central `Node`-level tree-walker engine
//! ([`backend_nodes_core::node_walker`]):
//!
//! * [`walkers`] — the read-only predicates: `contain_aggs_of_level`,
//!   `locate_agg_of_level`, `contain_windowfuncs`, `locate_windowfunc`,
//!   `checkExprHasSubLink`, `contains_multiexpr_param`, `rangeTableEntry_used`.
//! * [`offset`] — `OffsetVarNodes` (+ `offset_relid_set`) and
//!   `CombineRangeTables` analogue.
//! * [`change`] — `ChangeVarNodes` / `ChangeVarNodesExtended` /
//!   `ChangeVarNodesWalkExpression` and `adjust_relid_set`.
//! * [`increment`] — `IncrementVarSublevelsUp` (+ `_rtable`) and
//!   `SetVarReturningType`.
//! * [`nulling`] — `add_nulling_relids`, `remove_nulling_relids`.
//! * [`replace`] — `replace_rte_variables` (+ mutator), `map_variable_attnos`,
//!   `ReplaceVarsFromTargetList` (+ callback), `ReplaceVarFromTargetList`.
//! * [`relids`] — the inline `ExprRelids` word-vector set algebra.
//!
//! # Rule-action manipulation primitives
//!
//! [`manip_rule`] holds the rule-action node-manipulation helpers the RIR / DML
//! rule engine (`rewriteHandler.c`, sibling lane) consumes: `AddQual`,
//! `AddInvertedQual`, `CombineRangeTables` (rewriteManip.c) and the one
//! jointree-list helper `adjustJoinTreeList` (rewriteHandler.c). They are
//! defined over the owned `Query<'mcx>` / `Expr` model.
//!
//! `getInsertSelectQuery` lives in [`insert_select`]; its C `Query
//! ***subquery_ptr` out-parameter is always `NULL` at the rewriteDefine.c call
//! sites, so the owned form returns a plain borrow.
//!
//! `CombineRangeTables` (rewriteManip.c:347) is given its faithful home here.
//! `backend-optimizer-plan-subselect-pullup` still carries a private
//! `combine_range_tables` copy (a `&mut Query`-shaped specialization); folding it
//! onto this one is a follow-up that touches that audited sibling crate.
//!
//! `contain_vars_of_level` is an `optimizer/util/var.c` function, already
//! faithfully ported and exported as
//! `backend_optimizer_util_vars::var::contain_vars_of_level`; it is intentionally
//! NOT duplicated here (the rule engine calls the var.c owner directly).
//!
//! # The C "cheat and modify in-place" mutators
//!
//! Several C functions (`OffsetVarNodes`/`ChangeVarNodes`/
//! `IncrementVarSublevelsUp`/`SetVarReturningType`) document that they "cheat
//! and modify the nodes in-place" — the caller copies the tree first. This is
//! exactly the repo's mutator model (`&mut Node -> bool`), so they map directly
//! onto the `*_mut` walker / `query_tree_mutator` family. The copy-mutators
//! (`add_nulling_relids`/`remove_nulling_relids`/`replace_rte_variables`/
//! `map_variable_attnos`) return a fresh node in C; over the owned in-place tree
//! that is the same as editing/overwriting the node through `*node`.
//!
//! # Installed seams
//!
//! `init_seams()` installs the three rewriteManip.c-owned seams declared in
//! `backend-rewrite-rewritemanip-seams` and consumed by the parser
//! (`parse_agg`/`parse_clause`): `contain_windowfuncs`, `locate_windowfunc`,
//! `locate_agg_of_level`. The fourth declared seam, `flatten_join_alias_vars`,
//! lives in `optimizer/util/var.c` (NOT rewriteManip.c) and is owned/installed
//! by `backend-optimizer-util-vars`; it is intentionally not installed here.

#![allow(non_snake_case)]
#![no_std]

extern crate alloc;

pub mod change;
pub mod increment;
pub mod insert_select;
pub mod manip_rule;
pub mod nulling;
pub mod offset;
pub mod relids;
pub mod replace;
pub mod support;
pub mod walkers;

#[cfg(test)]
mod tests;

pub use change::{
    adjust_relid_set, ChangeVarNodes, ChangeVarNodesContext, ChangeVarNodesExtended,
    ChangeVarNodesWalkExpression,
};
pub use increment::{IncrementVarSublevelsUp, IncrementVarSublevelsUp_rtable, SetVarReturningType};
pub use insert_select::getInsertSelectQuery;
pub use manip_rule::{adjustJoinTreeList, AddInvertedQual, AddQual, CombineRangeTables};
pub use nulling::{add_nulling_relids, remove_nulling_relids, remove_nulling_relids_in_query};
pub use offset::OffsetVarNodes;
pub use replace::{
    map_variable_attnos, replace_rte_variables, ReplaceVarFromTargetList, ReplaceVarsFromTargetList,
    ReplaceVarsNoMatchOption,
};
pub use support::{get_rewrite_oid, IsDefinedRewriteRule, SetRelationRuleStatus};
pub use walkers::{
    checkExprHasSubLink, contain_aggs_of_level, contain_windowfuncs, contains_multiexpr_param,
    locate_agg_of_level, locate_windowfunc, rangeTableEntry_used,
};

/// Install the rewriteManip.c- and rewriteSupport.c-owned seams.
pub fn init_seams() {
    use backend_rewrite_rewritemanip_seams as s;
    s::contain_windowfuncs::set(|node| walkers::contain_windowfuncs(node));
    s::locate_windowfunc::set(|node| walkers::locate_windowfunc(node));
    s::locate_agg_of_level::set(|node, levelsup| walkers::locate_agg_of_level(node, levelsup));

    // rewriteSupport.c
    backend_rewrite_rewritesupport_seams::get_rewrite_oid::set(support::get_rewrite_oid);
}
