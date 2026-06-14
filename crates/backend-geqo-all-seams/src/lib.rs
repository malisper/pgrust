//! Planner-external seams consumed by the GEQO subsystem
//! (`backend/optimizer/geqo/`), arena-shaped over
//! [`types_pathnodes::PlannerInfo`].
//!
//! `geqo_eval`/`gimme_tree` reach back into the rest of the planner to build
//! and cost candidate join relations and to decide whether a join is
//! "desirable". Pulling those sibling-optimizer crates in directly would form a
//! dependency cycle (`geqo_eval` → `make_join_rel` → … → the join-search hook →
//! `geqo`), so they cross the boundary here. Each seam defaults to a loud panic
//! until its owning crate installs the real implementation at single-threaded
//! startup.
//!
//! Cross-crate owners: joinrels.c (`make_join_rel` →
//! `build_and_cost_join_rel`, `have_join_order_restriction`), joininfo.c
//! (`have_relevant_joinclause`), and the planner private-memory machinery
//! reached by `geqo_eval` (`geqo_eval_context_create` /
//! `geqo_eval_context_delete`). None of these owners is ported yet, so the
//! seams are declared but installed by nobody for now.

#![forbid(unsafe_code)]

use types_pathnodes::{PathnodesMemoryContext, PlannerInfo, RelId};

seam_core::seam!(
    /// `make_join_rel(root, rel1, rel2)` followed (on success) by
    /// `generate_partitionwise_join_paths`, the topmost-rel-guarded
    /// `generate_useful_gather_paths`, and `set_cheapest` — the body of
    /// `geqo_eval.c:merge_clump`. Returns the new joinrel's arena handle, or
    /// `None` if the join order is invalid.
    pub fn build_and_cost_join_rel(
        root: &mut PlannerInfo,
        rel1: RelId,
        rel2: RelId,
    ) -> Option<RelId>
);

seam_core::seam!(
    /// `have_relevant_joinclause(root, rel1, rel2)` (`optimizer/util/joininfo.c`).
    pub fn have_relevant_joinclause(
        root: &PlannerInfo,
        rel1: RelId,
        rel2: RelId,
    ) -> bool
);

seam_core::seam!(
    /// `have_join_order_restriction(root, rel1, rel2)`
    /// (`optimizer/path/joinrels.c`).
    pub fn have_join_order_restriction(
        root: &PlannerInfo,
        rel1: RelId,
        rel2: RelId,
    ) -> bool
);

seam_core::seam!(
    /// Create the private GEQO temp context, switch into it, and return the
    /// *old* context (`geqo_eval.c`).
    pub fn geqo_eval_context_create() -> PathnodesMemoryContext
);

seam_core::seam!(
    /// Restore the saved (old) context and delete the private GEQO temp context
    /// (`geqo_eval.c`).
    pub fn geqo_eval_context_delete(oldcxt: PathnodesMemoryContext)
);
