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
//! # Genuine remaining gaps (NOT stubbed)
//!
//! The rule-rewriter helpers `getInsertSelectQuery`, `AddQual`,
//! `AddInvertedQual` and `CombineRangeTables` also live in rewriteManip.c but
//! belong to the rule-action rewrite path (`rewriteHandler.c`, the sibling
//! `backend-rewrite-core` files which are still `todo`). They have no consumer
//! in the parser / `prepjointree` / `subselect` Var-manipulation path this unit
//! serves, and `getInsertSelectQuery`'s C signature returns both a borrow of the
//! sub-Query and a mutable link to it (`Query ***subquery_ptr`), which has no
//! caller here. They are intentionally not defined (no own-logic stubs); they
//! land with the rewriteHandler rule engine.
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
pub mod nulling;
pub mod offset;
pub mod relids;
pub mod replace;
pub mod walkers;

#[cfg(test)]
mod tests;

pub use change::{
    adjust_relid_set, ChangeVarNodes, ChangeVarNodesContext, ChangeVarNodesExtended,
    ChangeVarNodesWalkExpression,
};
pub use increment::{IncrementVarSublevelsUp, IncrementVarSublevelsUp_rtable, SetVarReturningType};
pub use nulling::{add_nulling_relids, remove_nulling_relids, remove_nulling_relids_in_query};
pub use offset::OffsetVarNodes;
pub use replace::{
    map_variable_attnos, replace_rte_variables, ReplaceVarFromTargetList, ReplaceVarsFromTargetList,
    ReplaceVarsNoMatchOption,
};
pub use walkers::{
    checkExprHasSubLink, contain_aggs_of_level, contain_windowfuncs, contains_multiexpr_param,
    locate_agg_of_level, locate_windowfunc, rangeTableEntry_used,
};

/// Install the rewriteManip.c-owned seams.
pub fn init_seams() {
    use backend_rewrite_rewritemanip_seams as s;
    s::contain_windowfuncs::set(|node| walkers::contain_windowfuncs(node));
    s::locate_windowfunc::set(|node| walkers::locate_windowfunc(node));
    s::locate_agg_of_level::set(|node, levelsup| walkers::locate_agg_of_level(node, levelsup));
}
