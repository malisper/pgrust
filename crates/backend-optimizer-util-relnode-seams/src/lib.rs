//! Seam declarations for `optimizer/util/relnode.c` — the `bms_*` set algebra
//! over the planner's `Relids` that the join-path enumerator is built out of.
//!
//! `Relids` is the planner relation-id set (`Bitmapset *`); the planner
//! convention represents the empty set as `None`. The `bms_*` operations are
//! owned by the not-yet-ported `nodes/bitmapset.c` + `relnode.c`; each crosses
//! here as a `Relids`-typed seam that panics loudly until the owner installs the
//! real implementation. Set-returning ops hand back a freshly-owned `Relids`;
//! query ops return a scalar.

use types_error::PgResult;
use types_pathnodes::{PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};

seam_core::seam!(
    /// `bms_copy(a)` — a fresh copy of set `a` (empty copies to empty).
    pub fn relids_copy(a: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_intersect(a, b)` — a fresh set `a ∩ b` (inputs unchanged).
    pub fn relids_intersect(a: &Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_add_members(a, b)` — `a ∪ b`; `a` is recycled, `b` unchanged.
    pub fn relids_add_members(a: Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_join(a, b)` — destructive union; both inputs recycled into result.
    pub fn relids_join(a: Relids, b: Relids) -> Relids
);
seam_core::seam!(
    /// `bms_is_subset(a, b)` — every member of `a` is in `b` (empty ⊆ anything).
    pub fn relids_is_subset(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_is_member(x, a)` — `x ∈ a`.
    pub fn relids_is_member(x: i32, a: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_is_empty(a)` — `a` has no members.
    pub fn relids_is_empty(a: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_overlap(a, b)` — `a ∩ b` is non-empty.
    pub fn relids_overlap(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_nonempty_difference(a, b)` — `a \ b` is non-empty.
    pub fn relids_nonempty_difference(a: &Relids, b: &Relids) -> bool
);

/* ==========================================================================
 * Additional `bms_*` ops consumed by joinrels.c that the original set above
 * did not expose (additive — appended for the join-relation enumerator).
 * ======================================================================== */

seam_core::seam!(
    /// `bms_add_member(a, x)` — `a ∪ {x}`; `a` is recycled into the result.
    pub fn relids_add_member(a: Relids, x: i32) -> Relids
);
seam_core::seam!(
    /// `bms_num_members(a)` — the number of members in `a`.
    pub fn relids_num_members(a: &Relids) -> i32
);
seam_core::seam!(
    /// `bms_singleton_member(a)` — the single member of a one-element set
    /// (the caller has established `a` has exactly one member).
    pub fn relids_singleton_member(a: &Relids) -> i32
);

/* ==========================================================================
 * relnode.c find/build-join-rel routines the join enumerator drives. These
 * `palloc` the joinrel / restrictlist (so can `ereport(ERROR)`), returning
 * `PgResult`; the pure find-by-relids lookups return bare handles.
 * ======================================================================== */

seam_core::seam!(
    /// `find_base_rel(root, relid)` (relnode.c) — the base `RelOptInfo` for an
    /// RT index.
    pub fn find_base_rel(root: &PlannerInfo, relid: i32) -> RelId
);
seam_core::seam!(
    /// `find_join_rel(root, relids)` (relnode.c) — an existing join
    /// `RelOptInfo` for the given relid set, or `None` if not built yet.
    pub fn find_join_rel(root: &PlannerInfo, relids: &Relids) -> Option<RelId>
);
seam_core::seam!(
    /// `build_join_rel(root, joinrelids, outer_rel, inner_rel, sjinfo,
    /// pushed_down_joins, &restrictlist)` (relnode.c) — find or build the join
    /// `RelOptInfo` and compute its restrictlist; returns
    /// `(joinrel, restrictlist)`.
    pub fn build_join_rel(
        root: &mut PlannerInfo,
        joinrelids: &Relids,
        outer_rel: RelId,
        inner_rel: RelId,
        sjinfo: &SpecialJoinInfo,
        pushed_down_joins: &[SpecialJoinInfo],
    ) -> PgResult<(RelId, ::std::vec::Vec<RinfoId>)>
);
seam_core::seam!(
    /// `build_child_join_rel(root, outer_rel, inner_rel, parent_joinrel,
    /// restrictlist, sjinfo, nappinfos, appinfos)` (relnode.c) — build the
    /// child-join `RelOptInfo` for a partitionwise join segment.
    pub fn build_child_join_rel(
        root: &mut PlannerInfo,
        outer_rel: RelId,
        inner_rel: RelId,
        parent_joinrel: RelId,
        restrictlist: &[RinfoId],
        sjinfo: &SpecialJoinInfo,
        appinfos: &[types_pathnodes::AppendRelInfo],
    ) -> PgResult<RelId>
);
seam_core::seam!(
    /// `min_join_parameterization(root, joinrelids, outer_rel, inner_rel)`
    /// (relnode.c) — the minimum set of LATERAL rels the joinrel will be
    /// parameterized by.
    pub fn min_join_parameterization(
        root: &PlannerInfo,
        joinrelids: &Relids,
        outer_rel: RelId,
        inner_rel: RelId,
    ) -> Relids
);
