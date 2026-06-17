//! Port of `src/backend/optimizer/util/var.c` + `tlist.c` — the planner's `Var`
//! node manipulation routines and target-list / `PathTarget` helpers.
//!
//! # Scope
//!
//! * [`var`] — the read-only `Var`/`PlaceHolderVar`/`CurrentOfExpr` walkers
//!   (`pull_varnos*`, `pull_varattnos`, `pull_vars_of_level`,
//!   `contain_var_clause`, `contain_vars_of_level`,
//!   `contain_vars_returning_old_or_new`, `locate_var_of_level`,
//!   `pull_var_clause`), each a 1:1 transcription over the central node-walker
//!   engine, plus the inline `Relids` word-vector set algebra. This module
//!   installs the four var.c-owned join-path seams (`pull_varnos`,
//!   `pull_vars_of_level`, `node_is_var`, `var_varno`) consumed by
//!   `get_memoize_path`, and the `pull_varattnos` seam consumed by
//!   `nodeModifyTable`.
//! * [`tlist`] — the target-list / `PathTarget` helpers that operate purely over
//!   `TargetEntry`/`SortGroupClause`/`PathTarget` (search, extraction, grouping
//!   ops, labeling, `PathTarget` builders). Structural expression equality
//!   (`equal()`) crosses to the not-yet-ported equalfuncs.c via the
//!   `backend-nodes-equalfuncs-seams::equal_expr` seam (panics until that lands).
//!
//! # Genuine remaining gaps (NOT stubbed)
//!
//! The following var.c / tlist.c routines are blocked on still-`todo` sibling
//! subsystems and are intentionally **not** defined here (no own-logic stubs):
//!
//! * `flatten_join_alias_vars` (+ `flatten_join_alias_vars_mutator` and its
//!   private helpers `add_nullingrels_if_needed`,
//!   `is_standard_join_alias_expression`,
//!   `adjust_standard_join_alias_expression`, `alias_relid_set`) is **ported now**
//!   in [`flatten`] and installs the `flatten_join_alias_vars` seam (declared in
//!   `backend-rewrite-rewritemanip-seams`, consumed by `parse_agg` and
//!   `pull_up_simple_subquery`). It uses rewriteManip.c's `IncrementVarSublevelsUp`
//!   / `checkExprHasSubLink` / `add_nulling_relids` (backend-rewrite-core) and the
//!   prepjointree.c `get_relids_for_join` (a new prepjointree-seams seam). The
//!   `PlannerInfo *root` argument is always NULL at the seam call site, so the
//!   non-NULL-`root` PlaceHolderVar fallback in `add_nullingrels_if_needed`
//!   (`make_placeholder_expr` / `pull_varnos_of_level`) is unreachable and the C
//!   `elog(ERROR, "unsupported join alias expression")` else-arm is preserved.
//! * `flatten_group_exprs` and `mark_nullable_by_grouping` — the group-expr
//!   *mutator* sibling — stay unported (a *different* entry, called only with a
//!   real `root`): they require `make_placeholder_expr` (placeholder.c),
//!   `get_relids_in_jointree` (prepjointree.c), and the `contain_*` /
//!   `expression_returns_set` predicates (clauses.c), none of which has a
//!   consumer in this repo yet. (The src-idiomatic reference likewise deferred
//!   this sibling.)
//! * The `split_pathtarget_at_srfs*` SRF-leveling family + `split_pathtarget_*`
//!   walkers + `make_pathtarget_from_tlist` — these read `root->parse`'s
//!   `hasGroupRTE`/`groupingSets` and need `set_pathtarget_cost_width`
//!   (costsize.c). The consumer-facing `PlannerInfo.parse` is the opaque
//!   `QueryId` handle with no `Query` resolver, so the splitter cannot reach the
//!   `Query` fields it switches on. No consumer exists yet.

#![allow(non_snake_case)]

extern crate alloc;

pub mod fix_indexqual;
pub mod flatten;
pub mod tlist;
pub mod var;

#[cfg(test)]
mod tests;

pub use fix_indexqual::fix_indexqual_operand;
pub use flatten::flatten_join_alias_vars;
pub use var::{
    contain_var_clause, contain_vars_of_level, contain_vars_returning_old_or_new,
    locate_var_of_level, pull_var_clause, pull_varattnos, pull_varnos, pull_varnos_of_level,
    pull_vars_of_level, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_INCLUDE_WINDOWFUNCS,
    PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_WINDOWFUNCS,
};

/// Install every seam this unit owns. Wired into `seams-init::init_all()`.
pub fn init_seams() {
    var::init_seams();
    // `flatten_join_alias_vars` is declared in the rewritemanip-seams crate
    // (its sole consumers are the parser / prepjointree across the cycle), but
    // it lives in optimizer/util/var.c — this is its real owner.
    backend_rewrite_rewritemanip_seams::flatten_join_alias_vars::set(
        flatten::flatten_join_alias_vars,
    );
    // `create_empty_pathtarget` lives in tlist.c (this owner); relnode.c reaches
    // it via the relnode-ext consumer-side seam crate.
    backend_optimizer_util_relnode_ext_seams::create_empty_pathtarget::set(
        tlist::create_empty_pathtarget,
    );
}
