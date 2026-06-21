//! `optimizer/util/predtest.c` + `optimizer/util/inherit.c`.
//!
//! `predtest` — the predicate implication/refutation proof engine — is ported
//! in full.  `inherit` is ported to the extent the value model allows: the pure
//! attribute-set translation helpers are complete; the inheritance/partition
//! expansion entry points are keystone-blocked on the parser's owned
//! `RangeTblEntry`/`PlanRowMark`/`Query` value model (see [`inherit`]).

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// predtest.c declares locals up front, nests `if (IsA(x)) { if (...) }` type
// checks, and writes proof predicates like `!(weak && !refute_it)` verbatim;
// preserve that 1:1 and accept the corresponding style lints.
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::nonminimal_bool)]
#![allow(clippy::needless_late_init)]

extern crate alloc;

pub mod inherit;
pub mod predtest;

pub use inherit::{translate_col_privs, translate_col_privs_multilevel};
pub use predtest::{predicate_implied_by, predicate_refuted_by};

/// Install this unit's INWARD seam.  `predicate_implied_by` is consumed by
/// `backend-optimizer-path-indxpath` across the optimizer cycle and declared in
/// `backend-optimizer-util-predtest-seams`.
///
/// The remaining OUTWARD seam the unit declares (`eval_const_test`, in
/// `backend-optimizer-util-inherit-predtest-seams`) is installed by its real
/// owner (executor / execExpr). The `pg_amop` proof-cache invalidation callback
/// is registered directly through the real inval.c owner seam
/// (`backend_utils_cache_inval_seams::cache_register_syscache_callback`), so it
/// needs no consumer-owned seam.
pub fn init_seams() {
    backend_optimizer_util_predtest_seams::predicate_implied_by::set(predicate_implied_by);
    backend_optimizer_util_predtest_seams::predicate_implied_by_exprs::set(
        predtest::predicate_implied_by_exprs,
    );
    backend_optimizer_util_predtest_seams::predicate_refuted_by_exprs::set(
        predtest::predicate_refuted_by_exprs,
    );
    // relation_excluded_by_constraints (plancat.c) refutation half; predtest.c
    // owns predicate_refuted_by, declared in plancat-ext-seams.
    backend_optimizer_util_plancat_ext_seams::predicate_refuted_by::set(
        predtest::predicate_refuted_by,
    );
}

#[cfg(test)]
mod tests;
