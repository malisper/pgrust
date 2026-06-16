//! Seam declarations for the unported `analyzejoins.c` join-removal callees of
//! `query_planner` (planmain.c), owned by `backend-optimizer-plan-analyzejoins`
//! when it lands (`optimizer/plan/analyzejoins.c`).
//!
//! `query_planner` calls these three after building base-rel data structures
//! and classifying qual clauses: they are reached only on the general join
//! path, not on the trivial single-`RTE_RESULT` fast path. Until the owner
//! installs them each call panics loudly with its seam path — never a silent
//! stub.
//!
//! Signatures mirror `optimizer/planmain.h` 1:1. `remove_useless_joins` /
//! `remove_useless_self_joins` consume and return the joinlist (`List *`);
//! `reduce_unique_semijoins` is `void`. None can `ereport(ERROR)` on the
//! query_planner path, so they return bare values.

#![allow(non_snake_case)]

extern crate alloc;

use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{JoinlistNode, PlannerInfo};

seam_core::seam!(
    /// `remove_useless_joins(root, joinlist)` (analyzejoins.c:90): remove any
    /// useless outer joins, returning the (possibly trimmed) joinlist.
    ///
    /// Threads the planner-run resolver (`run`): `join_is_removable` proves the
    /// inner rel distinct, and for a subquery inner rel that resolves the
    /// sub-`Query` off the RTE store via `&PlannerRun<'mcx>`.
    pub fn remove_useless_joins<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        joinlist: alloc::vec::Vec<JoinlistNode>,
    ) -> alloc::vec::Vec<JoinlistNode>
);

seam_core::seam!(
    /// `reduce_unique_semijoins(root)` (analyzejoins.c:844): reduce semijoins
    /// whose inner rel is unique to plain inner joins.
    ///
    /// Threads the planner-run resolver (`run`): the body reaches
    /// `generate_join_implied_equalities`, which reads RTE fields through the
    /// re-signed `rte_*` seams that take `&PlannerRun<'mcx>`.
    pub fn reduce_unique_semijoins<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `remove_useless_self_joins(root, joinlist)` (analyzejoins.c:2488): remove
    /// self joins on a unique column, returning the (possibly trimmed) joinlist.
    pub fn remove_useless_self_joins(
        root: &mut PlannerInfo,
        joinlist: alloc::vec::Vec<JoinlistNode>,
    ) -> alloc::vec::Vec<JoinlistNode>
);
