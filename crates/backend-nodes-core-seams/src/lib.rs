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
    /// `bms_next_member(a, prevbit)` (bitmapset.c): return the next set bit
    /// strictly greater than `prevbit`, or `-2` past the last member (the C
    /// returns `-2` once exhausted; callers stop on `< 0`). A `None` set is the
    /// C NULL (empty) set. Infallible.
    pub fn bms_next_member(a: Option<&types_nodes::Bitmapset<'_>>, prevbit: i32) -> i32
);

seam_core::seam!(
    /// `bms_is_empty(a)` (bitmapset.c): is the set empty? A `None` set is the
    /// C NULL set, which is empty. Infallible.
    pub fn bms_is_empty(a: Option<&types_nodes::Bitmapset<'_>>) -> bool
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

// === tidbitmap (tidbitmap.c) ===============================================

/// Opaque token standing in for C's `TIDBitmap *` while the executor owns the
/// live bitmap (it outlives a single `btgetbitmap` call).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TbmHandle(pub u64);

seam_core::seam!(
    /// `tbm_add_tuples(tbm, &tid, 1, false)` (tidbitmap.c): add one heap TID
    /// to the bitmap. `Err` carries OOM from growing the bitmap.
    pub fn tbm_add_tuple(
        tbm: TbmHandle,
        tid: types_tuple::heaptuple::ItemPointerData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `bms_num_members(a)` (bitmapset.c): count the members of `a`. A `None`
    /// set is the C NULL (empty) set, yielding 0. Infallible.
    pub fn bms_num_members(a: Option<&types_nodes::Bitmapset<'_>>) -> i32
);

seam_core::seam!(
    /// `bms_add_range(a, lower, upper)` (bitmapset.c): add all integers in the
    /// inclusive range `[lower, upper]` to the set, recycling the input (the C
    /// extends `a` in place and returns it; a `None` input is the C NULL set,
    /// and an empty range with `upper < lower` returns it unchanged). Growth
    /// allocates in `mcx`, so the call is fallible on OOM.
    pub fn bms_add_range<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        lower: i32,
        upper: i32,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_overlap(a, b)` (bitmapset.c): do the two sets overlap (have any
    /// member in common)? A `None` set is the C NULL (empty) set. Infallible.
    pub fn bms_overlap(
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);
