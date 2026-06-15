//! Seam declarations for `optimizer/util/relnode.c` — the `bms_*` set algebra
//! over the planner's `Relids` that the join-path enumerator is built out of.
//!
//! `Relids` is the planner relation-id set (`Bitmapset *`); the planner
//! convention represents the empty set as `None`. The `bms_*` operations are
//! owned by the not-yet-ported `nodes/bitmapset.c` + `relnode.c`; each crosses
//! here as a `Relids`-typed seam that panics loudly until the owner installs the
//! real implementation. Set-returning ops hand back a freshly-owned `Relids`;
//! query ops return a scalar.

use types_pathnodes::Relids;

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
seam_core::seam!(
    /// `bms_next_member(a, prevbit)` — the next member of `a` greater than
    /// `prevbit` (start with -1), or -2 when there are none left. Note: callers
    /// that loop `while ((i = …) >= 0)` treat any negative result as "done".
    pub fn relids_next_member(a: &Relids, prevbit: i32) -> i32
);
seam_core::seam!(
    /// `bms_get_singleton_member(a, &member)` — if `a` has exactly one member,
    /// return `Some(member)`; otherwise `None`.
    pub fn relids_get_singleton_member(a: &Relids) -> Option<i32>
);
seam_core::seam!(
    /// `bms_int_members(a, b)` — `a ∩ b`; `a` is recycled into the result, `b`
    /// unchanged.
    pub fn relids_int_members(a: Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_union(a, b)` — a fresh set `a ∪ b` (inputs unchanged).
    pub fn relids_union(a: &Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_difference(a, b)` — a fresh set `a \ b` (inputs unchanged).
    pub fn relids_difference(a: &Relids, b: &Relids) -> Relids
);
seam_core::seam!(
    /// `bms_add_member(a, x)` — add member `x` to `a` (recycled), returning the
    /// result.
    pub fn relids_add_member(a: Relids, x: i32) -> Relids
);
seam_core::seam!(
    /// `bms_add_range(a, lower, upper)` — add members `lower..=upper` to `a`
    /// (recycled), returning the result.
    pub fn relids_add_range(a: Relids, lower: i32, upper: i32) -> Relids
);
seam_core::seam!(
    /// `bms_make_singleton(x)` — a fresh set `{x}`.
    pub fn relids_make_singleton(x: i32) -> Relids
);
seam_core::seam!(
    /// `bms_equal(a, b)` — `a` and `b` contain the same members (empty == empty).
    pub fn relids_equal(a: &Relids, b: &Relids) -> bool
);
seam_core::seam!(
    /// `bms_membership(a)` — `BMS_EMPTY_SET` (0) / `BMS_SINGLETON` (1) /
    /// `BMS_MULTIPLE` (2).
    pub fn relids_membership(a: &Relids) -> i32
);
