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
/// The two OUTWARD seams the unit declares (`eval_const_test`,
/// `register_oprproof_syscache_callback`, in
/// `backend-optimizer-util-inherit-predtest-seams`) are installed by their real
/// owners (executor / inval) when those land; tracked in `seams-init`'s
/// `CONTRACT_RECONCILE_PENDING` until then.
pub fn init_seams() {
    backend_optimizer_util_predtest_seams::predicate_implied_by::set(predicate_implied_by);
}

#[cfg(test)]
mod tests;
