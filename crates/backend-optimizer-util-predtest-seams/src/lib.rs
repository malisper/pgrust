//! Seam declarations for `optimizer/util/predtest.c` (the predicate theorem
//! prover), arena-shaped over [`types_pathnodes::PlannerInfo`].
//!
//! indxpath.c uses `predicate_implied_by` to prove partial-index predicates
//! from the query's WHERE/predicate clauses (in `build_paths_for_OR`,
//! `choose_bitmap_and`, `check_index_predicates`). The prover lives in
//! predtest.c; we cross that boundary here. Defaults to a loud panic until
//! predtest.c is ported.

extern crate alloc;

use alloc::vec::Vec;

use types_pathnodes::{NodeId, PlannerInfo};

seam_core::seam!(
    /// `predicate_implied_by(predicate_clauses, restriction_clauses, weak)`
    /// (predtest.c) — does the conjunction of `restriction_clauses` prove the
    /// conjunction of `predicate_clauses`? Both are lists of bare clause
    /// expressions (already in the arena, identified by `NodeId`); `weak`
    /// selects weak vs. strong implication. The C signature passes `List *` of
    /// `Expr *`; here the lists are resolved to `NodeId` slices by the caller.
    pub fn predicate_implied_by(
        root: &PlannerInfo,
        predicate_clauses: &[NodeId],
        restriction_clauses: &[NodeId],
        weak: bool
    ) -> bool
);
