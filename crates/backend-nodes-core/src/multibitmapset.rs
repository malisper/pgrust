//! Family: **multibitmapset** — `nodes/multibitmapset.c`, a `List` of
//! `Bitmapset`.
//!
//! `mbms_add_member`, `mbms_add_members`, `mbms_int_members`, `mbms_is_member`,
//! `mbms_overlap_sets`. Built directly on the keystone (`bms_*` operations) and
//! the list family (the outer `List`).
//!
//! ## Owned model
//!
//! C models a multibitmapset as a `List *` whose every list-cell holds a
//! `Bitmapset *` (`lfirst_node(Bitmapset, lc)`), where the empty set is `NIL`
//! and an empty list-element is a NULL `Bitmapset *`. The zero-based list index
//! is the first identifying value and the bit number within that bitmapset is
//! the second.
//!
//! The owned mirror is a [`MultiBitmapset`] = `PgVec` of `Option<Bitmapset>`:
//! each cell is an owned `Bitmapset` (the C palloc'd struct) or `None` (the C
//! NULL `Bitmapset *`); the whole-list NIL is the empty vec. As in
//! [`crate::bitmapset`], the `bms_*` operations that recycle their left input
//! consume the owning `PgBox` and hand it back, so the per-cell ops here
//! take/replace the cell's owned set in place — exactly the C
//! `bms = lfirst_node(...); bms = bms_xxx(bms, ...); lfirst(lc) = bms` dance.
//!
//! Growth (`lappend`) allocates in `mcx`, so the appending ops are fallible
//! (`PgResult`); the C `elog(ERROR, "negative multibitmapset member index not
//! allowed")` on a negative index is a caller bug and panics, matching the C
//! failure surface (the keystone's `bms_*` panic identically).

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;

use crate::bitmapset;

/// A multibitmapset: the owned mirror of C's `List *` of `Bitmapset *`.
///
/// Cell `i` holds the bitmapset identified by list index `i`; `None` mirrors a
/// NULL `Bitmapset *` element, and an empty vec mirrors `NIL`.
pub type MultiBitmapset<'mcx> = PgVec<'mcx, Option<PgBox<'mcx, Bitmapset<'mcx>>>>;

/// `lappend(a, NULL)` for the multibitmapset list: append an empty (NULL)
/// element, growing the backing storage fallibly (C: palloc in
/// `CurrentMemoryContext`).
#[inline]
fn append_empty<'mcx>(mcx: Mcx<'mcx>, a: &mut MultiBitmapset<'mcx>) -> PgResult<()> {
    a.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<Option<PgBox<Bitmapset>>>()))?;
    a.push(None);
    Ok(())
}

/// Borrow a cell's owned set as `Option<&Bitmapset>` (the read-side argument the
/// keystone `bms_*` ops take), threading the C `const Bitmapset *bmsb`.
#[inline]
fn cell_ref<'a, 'mcx>(
    cell: &'a Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> Option<&'a Bitmapset<'mcx>> {
    cell.as_deref()
}

/// `mbms_add_member` — add a new member to a multibitmapset.
///
/// The new member is identified by `listidx`, the zero-based index of the list
/// element it should go into, and `bitidx`, the bit number to be set therein.
/// This is like [`bitmapset::bms_add_member`], but for multibitmapsets.
///
/// `a` is modified in-place and returned (C: the `List *a` is extended/updated
/// and returned).
pub fn mbms_add_member<'mcx>(
    mcx: Mcx<'mcx>,
    mut a: MultiBitmapset<'mcx>,
    listidx: i32,
    bitidx: i32,
) -> PgResult<MultiBitmapset<'mcx>> {
    if listidx < 0 || bitidx < 0 {
        panic!("negative multibitmapset member index not allowed");
    }
    // Add empty elements as needed.
    while (a.len() as i32) <= listidx {
        append_empty(mcx, &mut a)?;
    }
    // Update the target element.
    let cell = &mut a[listidx as usize];
    let bms = cell.take();
    let bms = bitmapset::bms_add_member(mcx, bms, bitidx)?;
    *cell = Some(bms);
    Ok(a)
}

/// `mbms_add_members` — add all members of set `b` to set `a`.
///
/// This is a UNION operation, but the left input is modified in-place. This is
/// like [`bitmapset::bms_add_members`], but for multibitmapsets.
pub fn mbms_add_members<'mcx>(
    mcx: Mcx<'mcx>,
    mut a: MultiBitmapset<'mcx>,
    b: &MultiBitmapset<'mcx>,
) -> PgResult<MultiBitmapset<'mcx>> {
    // Add empty elements to a, as needed.
    while a.len() < b.len() {
        append_empty(mcx, &mut a)?;
    }
    // forboth stops at the end of the shorter list, which is fine.
    let n = a.len().min(b.len());
    for i in 0..n {
        let bmsa = a[i].take();
        let bmsb = cell_ref(&b[i]);
        let bmsa = bitmapset::bms_add_members(mcx, bmsa, bmsb)?;
        a[i] = bmsa;
    }
    Ok(a)
}

/// `mbms_int_members` — reduce set `a` to its intersection with set `b`.
///
/// This is an INTERSECT operation, but the left input is modified in-place.
/// This is like [`bitmapset::bms_int_members`], but for multibitmapsets.
pub fn mbms_int_members<'mcx>(
    mut a: MultiBitmapset<'mcx>,
    b: &MultiBitmapset<'mcx>,
) -> MultiBitmapset<'mcx> {
    // Remove any elements of a that are no longer of use (list_truncate only
    // shrinks; if a is already shorter this is a no-op).
    if a.len() > b.len() {
        a.truncate(b.len());
    }
    // forboth stops at the end of the shorter list, which is fine.
    let n = a.len().min(b.len());
    for i in 0..n {
        let bmsa = a[i].take();
        let bmsb = cell_ref(&b[i]);
        let bmsa = bitmapset::bms_int_members(bmsa, bmsb);
        a[i] = bmsa;
    }
    a
}

/// `mbms_is_member` — is `listidx`/`bitidx` a member of `a`?
///
/// This is like [`bitmapset::bms_is_member`], but for multibitmapsets.
pub fn mbms_is_member(listidx: i32, bitidx: i32, a: &MultiBitmapset<'_>) -> bool {
    // XXX (mirrors the C comment) better to just return false for negatives?
    if listidx < 0 || bitidx < 0 {
        panic!("negative multibitmapset member index not allowed");
    }
    if listidx >= a.len() as i32 {
        return false;
    }
    let bms = cell_ref(&a[listidx as usize]);
    bitmapset::bms_is_member(bitidx, bms)
}

/// `mbms_overlap_sets` — identify the bitmapsets having common members in `a`
/// and `b`.
///
/// The result is a bitmapset of the list indexes of bitmapsets that overlap.
pub fn mbms_overlap_sets<'mcx>(
    mcx: Mcx<'mcx>,
    a: &MultiBitmapset<'mcx>,
    b: &MultiBitmapset<'mcx>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    // forboth stops at the end of the shorter list, which is fine.
    let n = a.len().min(b.len());
    for i in 0..n {
        let bmsa = cell_ref(&a[i]);
        let bmsb = cell_ref(&b[i]);
        if bitmapset::bms_overlap(bmsa, bmsb) {
            result = Some(bitmapset::bms_add_member(mcx, result, i as i32)?);
        }
    }
    Ok(result)
}
