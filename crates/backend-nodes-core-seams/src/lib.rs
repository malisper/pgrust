//! Seam declarations for the `backend-nodes-core` unit (here:
//! `nodes/bitmapset.c`, the `Bitmapset` set operations).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Allocating operations take the target context
//! handle (C: they palloc in `CurrentMemoryContext`).

seam_core::seam!(
    /// `bms_is_member(x, a)` (bitmapset.c): is `x` a member of `a`? A `None`
    /// set is the C NULL (empty) set. Infallible (the C can `elog(ERROR)` on
    /// a negative `x`, which the owner ports as a panic — caller bug).
    pub fn bms_is_member(x: i32, a: Option<&types_nodes::Bitmapset<'_>>) -> bool
);

seam_core::seam!(
    /// `bms_add_member(a, x)` (bitmapset.c): add `x` to the set, recycling
    /// the input (the C reallocs/extends `a` in place and returns it; a
    /// `None` input is the C NULL set). Growth allocates in `mcx`, so the
    /// call is fallible on OOM; the C `elog(ERROR)` on a negative `x` is the
    /// owner's to raise, also carried on `Err`.
    pub fn bms_add_member<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        x: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

seam_core::seam!(
    /// `bms_intersect(a, b)` (bitmapset.c): form a new set with the
    /// intersection of the inputs (allocates the copy in `mcx`; `None` in or
    /// empty result is the C NULL).
    pub fn bms_intersect<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_join(a, b)` (bitmapset.c): form the union, recycling the inputs
    /// (both are consumed; the C reuses the larger input's storage and frees
    /// the other — no allocation, so the call is infallible).
    pub fn bms_join<'mcx>(
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        b: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
    ) -> Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

seam_core::seam!(
    /// `bms_union(a, b)` (bitmapset.c): form a new set with the union of the
    /// inputs (copies the larger input into `mcx`).
    pub fn bms_union<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_nonempty_difference(a, b)` (bitmapset.c): is there a member of `a`
    /// that is not in `b`? Computes `a - b` and reports whether it is nonempty
    /// without materializing the difference; a `None` set is the C NULL (empty).
    /// Infallible (no allocation).
    pub fn bms_nonempty_difference(
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);
