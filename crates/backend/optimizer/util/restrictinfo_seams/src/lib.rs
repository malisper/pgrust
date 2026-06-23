//! Seam declarations for `optimizer/util/restrictinfo.c`, arena-shaped over
//! [`::pathnodes::PlannerInfo`] (the `RinfoId`/`NodeId` handles).
//!
//! indxpath.c builds derived index quals by wrapping freshly-constructed
//! expression nodes in `RestrictInfo`s. `make_restrictinfo` runs the
//! initsplan-machinery (relids extraction, mergejoinability analysis, …) that
//! lives in restrictinfo.c; we cross that boundary here. Each seam defaults to a
//! loud panic until restrictinfo.c is ported.

extern crate alloc;

use pathnodes::{NodeId, PlannerInfo, Relids, RinfoId};

seam_core::seam!(
    /// `make_simple_restrictinfo(root, clause)` (restrictinfo.h macro over
    /// `make_restrictinfo`) — wrap a bare clause node (already allocated into the
    /// arena, identified by `NodeId`) in a fresh `RestrictInfo`, pushed into the
    /// arena, and return its handle. Used to wrap each derived index qual.
    pub fn make_simple_restrictinfo(
        mcx: mcx::Mcx<'_>,
        root: &mut PlannerInfo,
        clause: NodeId,
    ) -> RinfoId
);

seam_core::seam!(
    /// `make_plain_restrictinfo(root, clause, orclause, is_pushed_down,
    /// has_clone, is_clone, pseudoconstant, security_level, required_relids,
    /// incompatible_relids, outer_relids)` (restrictinfo.c) — build a
    /// `RestrictInfo` over a precomputed clause + orclause (both already in the
    /// arena), copying the flag/relids bookkeeping from the source rinfo, and
    /// push it into the arena. Used by `group_similar_or_args` to build nested OR
    /// sub-restrictinfos.
    pub fn make_plain_restrictinfo(
        mcx: mcx::Mcx<'_>,
        root: &mut PlannerInfo,
        clause: NodeId,
        orclause: NodeId,
        is_pushed_down: bool,
        has_clone: bool,
        is_clone: bool,
        pseudoconstant: bool,
        security_level: u32,
        required_relids: &Relids,
        incompatible_relids: &Relids,
        outer_relids: &Relids
    ) -> RinfoId
);

seam_core::seam!(
    /// `restriction_is_securely_promotable(rinfo, rel)` (restrictinfo.c) — true
    /// if the clause may be applied at the rel without violating security-quals
    /// ordering: `rinfo->security_level <= rel->baserestrict_min_security ||
    /// rinfo->leakproof`. Kept as a seam because it reads the rel's
    /// `baserestrict_min_security` together with the rinfo flags through the C
    /// helper. (indxpath inlines the equivalent test directly off the arena, so
    /// this decl is provided for parity / external callers.)
    pub fn restriction_is_securely_promotable(
        root: &PlannerInfo,
        rinfo: RinfoId,
        rel: ::pathnodes::RelId
    ) -> bool
);

seam_core::seam!(
    /// `join_clause_is_movable_to(rinfo, baserel)` (joininfo.c) — can the join
    /// clause be evaluated at `baserel` (using only that rel's own vars plus
    /// allowed lateral/parameterization)? Used by `check_index_predicates` to
    /// collect movable join clauses.
    pub fn join_clause_is_movable_to(
        root: &PlannerInfo,
        rinfo: RinfoId,
        baserel: ::pathnodes::RelId
    ) -> bool
);

seam_core::seam!(
    /// `find_childrel_parents(root, rel)` (relnode.c) — the relids of the
    /// topmost parents of an "other member" (partition/inheritance child) rel.
    /// Used by `check_index_predicates` to subtract parent relids from
    /// `all_query_rels`.
    pub fn find_childrel_parents(
        root: &PlannerInfo,
        rel: ::pathnodes::RelId
    ) -> Relids
);

seam_core::seam!(
    /// `get_plan_rowmark(rowmarks, rtindex) != NULL` (preptlist.c /
    /// plancat.c) — does the query carry a PlanRowMark for `rtindex`? Used by
    /// `check_index_predicates` to detect a FOR-UPDATE/target relation.
    ///
    /// Threads `&PlannerRun` so the lookup can resolve each `root.rowMarks`
    /// `PlanRowMarkId` handle to its `PlanRowMark.rti` in the run's store
    /// (`root.rowMarks` carries handles, not values).
    pub fn has_plan_rowmark<'mcx>(
        run: &::pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rtindex: u32
    ) -> bool
);
