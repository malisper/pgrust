//! Family: **list** — `nodes/list.c`, the generic `List` (T_List / T_IntList /
//! T_OidList / T_XidList).
//!
//! ## Owned model
//!
//! C's `List *` is a NULL pointer for the empty list (`NIL`) and a palloc'd
//! header + expansible `ListCell[]` otherwise.  The owned model mirrors that
//! exactly:
//!
//! * `NIL` is Rust `None` (`Option<&List>` / `Option<PgBox<List>>`);
//! * a non-empty list is `types_nodes::list::List { type, elements }` whose
//!   [`PgVec`] of [`ListCell`] is guaranteed non-empty (`length >= 1`).
//!
//! The C header's `length` / `max_length` / `elements` / `initial_elements[]`
//! quartet (a hand-rolled growable array over a flexible array member) folds
//! into the single context-allocated [`PgVec`], which already tracks length and
//! capacity.  `new_list()` / `enlarge_list()` / the `*_cell` helpers therefore
//! become ordinary `PgVec` growth — the `pg_nextpower2` over-allocation is left
//! to the allocator's own growth policy (it is a pure performance detail with
//! no observable behaviour: C explicitly documents that cell addresses are not
//! stable across mutation, and that callers must use the returned pointer).
//!
//! Functions that allocate take [`Mcx`] and return [`PgResult`] (C palloc's in
//! `CurrentMemoryContext` and `ereport(ERROR)`s on OOM).  Mutating functions
//! consume the owning `PgBox<List>` and hand back the (possibly-`NIL`) result,
//! matching the C "always use the return value" contract.  Pure reads borrow.
//!
//! The two genuinely cross-unit operations on *pointer* lists — `equal()`
//! (`nodes/equalfuncs.c`) and `copyObjectImpl()` (`nodes/copyfuncs.c`) — are
//! seam calls into their (still-unported) owners; `mirror-pg-and-panic`.

#![allow(unused)]

use core::ffi::c_void;

use mcx::{Mcx, PgBox, PgVec};
use types_core::{Oid, TransactionId};
use types_error::PgResult;
use types_nodes::list::{List, ListCell};
use types_nodes::nodes::{NodeTag, T_IntList, T_List, T_OidList, T_XidList};

// ---------------------------------------------------------------------------
// Cross-unit seams (genuinely unported owners — mirror-pg-and-panic).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `equal(a, b)` (`nodes/equalfuncs.c`): structural node equality, used by
    /// the `equal()`-based pointer-list membership/delete/set operations.
    /// Operates on the raw `ListCell.ptr_value` (a `Node *`), exactly as in C.
    /// Owner: `nodes/equalfuncs.c` (not yet ported).
    pub fn equal(a: *const c_void, b: *const c_void) -> bool
);

seam_core::seam!(
    /// `copyObjectImpl(from)` (`nodes/copyfuncs.c`): deep node copy, used by
    /// `list_copy_deep`.  Owner: `nodes/copyfuncs.c` (not yet ported).
    pub fn copy_object_impl(from: *const c_void) -> *mut c_void
);

// ---------------------------------------------------------------------------
// ListCell accessors (pg_list.h inline macros, on the owned representation).
// ---------------------------------------------------------------------------

/// `lfirst(lc)` — the cell's pointer value.
#[inline]
fn cell_ptr(lc: &ListCell) -> *mut c_void {
    // SAFETY: caller guarantees the owning list is a pointer list (T_List).
    unsafe { lc.ptr_value }
}

/// `lfirst_int(lc)` — the cell's int value.
#[inline]
fn cell_int(lc: &ListCell) -> i32 {
    unsafe { lc.int_value }
}

/// `lfirst_oid(lc)` — the cell's Oid value.
#[inline]
fn cell_oid(lc: &ListCell) -> Oid {
    unsafe { lc.oid_value }
}

/// `lfirst_xid(lc)` — the cell's TransactionId value.
#[inline]
fn cell_xid(lc: &ListCell) -> TransactionId {
    unsafe { lc.xid_value }
}

/// `list_length(l)` (pg_list.h): `l ? l->length : 0`.
#[inline]
pub fn list_length(l: Option<&List<'_>>) -> i32 {
    types_nodes::list::list_length(l)
}

// ---------------------------------------------------------------------------
// new_list / enlarge_list / *_cell internals.
// ---------------------------------------------------------------------------

/// `new_list(type, min_size)` (list.c): a freshly allocated List with `min_size`
/// cells marked valid (their data is left for the caller to fill).
///
/// C over-allocates to a power-of-two cell count to amortize growth; that is a
/// pure performance detail (cell addresses are explicitly non-stable), so the
/// owned port lets `PgVec`'s own growth policy handle reserve, and only the
/// `min_size` valid cells are materialized.  The cells are zero-initialized
/// (`ListCell { ptr_value: NULL }`), matching the C invariant that the caller
/// overwrites them before use.
fn new_list<'mcx>(mcx: Mcx<'mcx>, r#type: NodeTag, min_size: i32) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    debug_assert!(min_size > 0);
    let n = min_size as usize;
    let mut elements = mcx::vec_with_capacity_in::<ListCell>(mcx, n)?;
    for _ in 0..n {
        elements.push(ListCell { ptr_value: core::ptr::null_mut() });
    }
    let list = List { r#type, elements };
    mcx::alloc_in(mcx, list)
}

/// Reserve room for one more cell (C: `enlarge_list` is folded into PgVec's
/// fallible reserve; `new_tail_cell`/`new_head_cell`/`insert_new_cell` all just
/// grow by one).
#[inline]
fn reserve_one<'mcx>(mcx: Mcx<'mcx>, list: &mut List<'mcx>) -> PgResult<()> {
    list
        .elements
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<ListCell>()))
}

// ---------------------------------------------------------------------------
// list_makeN_impl.
// ---------------------------------------------------------------------------

/// `list_make1_impl(t, datum1)`.
pub fn list_make1_impl<'mcx>(mcx: Mcx<'mcx>, t: NodeTag, datum1: ListCell) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    let mut list = new_list(mcx, t, 1)?;
    list.elements[0] = datum1;
    list.check_invariants();
    Ok(list)
}

/// `list_make2_impl(t, datum1, datum2)`.
pub fn list_make2_impl<'mcx>(
    mcx: Mcx<'mcx>,
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    let mut list = new_list(mcx, t, 2)?;
    list.elements[0] = datum1;
    list.elements[1] = datum2;
    list.check_invariants();
    Ok(list)
}

/// `list_make3_impl(t, datum1, datum2, datum3)`.
pub fn list_make3_impl<'mcx>(
    mcx: Mcx<'mcx>,
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    let mut list = new_list(mcx, t, 3)?;
    list.elements[0] = datum1;
    list.elements[1] = datum2;
    list.elements[2] = datum3;
    list.check_invariants();
    Ok(list)
}

/// `list_make4_impl(t, datum1..datum4)`.
pub fn list_make4_impl<'mcx>(
    mcx: Mcx<'mcx>,
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
    datum4: ListCell,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    let mut list = new_list(mcx, t, 4)?;
    list.elements[0] = datum1;
    list.elements[1] = datum2;
    list.elements[2] = datum3;
    list.elements[3] = datum4;
    list.check_invariants();
    Ok(list)
}

/// `list_make5_impl(t, datum1..datum5)`.
pub fn list_make5_impl<'mcx>(
    mcx: Mcx<'mcx>,
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
    datum4: ListCell,
    datum5: ListCell,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    let mut list = new_list(mcx, t, 5)?;
    list.elements[0] = datum1;
    list.elements[1] = datum2;
    list.elements[2] = datum3;
    list.elements[3] = datum4;
    list.elements[4] = datum5;
    list.check_invariants();
    Ok(list)
}

// ---------------------------------------------------------------------------
// lappend family.
// ---------------------------------------------------------------------------

/// `lappend(list, datum)` (list.c): append a pointer; returns the (possibly
/// reallocated) list.
pub fn lappend<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: *mut c_void,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_List, ListCell::from_ptr(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_List);
            reserve_one(mcx, &mut list)?;
            list.elements.push(ListCell::from_ptr(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `lappend_int(list, datum)`.
pub fn lappend_int<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: i32,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_IntList, ListCell::from_int(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_IntList);
            reserve_one(mcx, &mut list)?;
            list.elements.push(ListCell::from_int(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `lappend_oid(list, datum)`.
pub fn lappend_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: Oid,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_OidList, ListCell::from_oid(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_OidList);
            reserve_one(mcx, &mut list)?;
            list.elements.push(ListCell::from_oid(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `lappend_xid(list, datum)`.
pub fn lappend_xid<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: TransactionId,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_XidList, ListCell::from_xid(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_XidList);
            reserve_one(mcx, &mut list)?;
            list.elements.push(ListCell::from_xid(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

// ---------------------------------------------------------------------------
// list_insert_nth family.
// ---------------------------------------------------------------------------

/// `list_insert_nth(list, pos, datum)` (list.c): insert a pointer at `pos`.
pub fn list_insert_nth<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    pos: i32,
    datum: *mut c_void,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => {
            debug_assert!(pos == 0);
            list_make1_impl(mcx, T_List, ListCell::from_ptr(datum))
        }
        Some(mut list) => {
            debug_assert!(list.r#type == T_List);
            debug_assert!(pos >= 0 && pos <= list.length());
            reserve_one(mcx, &mut list)?;
            list.elements.insert(pos as usize, ListCell::from_ptr(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `list_insert_nth_int(list, pos, datum)`.
pub fn list_insert_nth_int<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    pos: i32,
    datum: i32,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => {
            debug_assert!(pos == 0);
            list_make1_impl(mcx, T_IntList, ListCell::from_int(datum))
        }
        Some(mut list) => {
            debug_assert!(list.r#type == T_IntList);
            debug_assert!(pos >= 0 && pos <= list.length());
            reserve_one(mcx, &mut list)?;
            list.elements.insert(pos as usize, ListCell::from_int(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `list_insert_nth_oid(list, pos, datum)`.
pub fn list_insert_nth_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    pos: i32,
    datum: Oid,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => {
            debug_assert!(pos == 0);
            list_make1_impl(mcx, T_OidList, ListCell::from_oid(datum))
        }
        Some(mut list) => {
            debug_assert!(list.r#type == T_OidList);
            debug_assert!(pos >= 0 && pos <= list.length());
            reserve_one(mcx, &mut list)?;
            list.elements.insert(pos as usize, ListCell::from_oid(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

// ---------------------------------------------------------------------------
// lcons family.
// ---------------------------------------------------------------------------

/// `lcons(datum, list)` (list.c): prepend a pointer.
pub fn lcons<'mcx>(
    mcx: Mcx<'mcx>,
    datum: *mut c_void,
    list: Option<PgBox<'mcx, List<'mcx>>>,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_List, ListCell::from_ptr(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_List);
            reserve_one(mcx, &mut list)?;
            list.elements.insert(0, ListCell::from_ptr(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `lcons_int(datum, list)`.
pub fn lcons_int<'mcx>(
    mcx: Mcx<'mcx>,
    datum: i32,
    list: Option<PgBox<'mcx, List<'mcx>>>,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_IntList, ListCell::from_int(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_IntList);
            reserve_one(mcx, &mut list)?;
            list.elements.insert(0, ListCell::from_int(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

/// `lcons_oid(datum, list)`.
pub fn lcons_oid<'mcx>(
    mcx: Mcx<'mcx>,
    datum: Oid,
    list: Option<PgBox<'mcx, List<'mcx>>>,
) -> PgResult<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => list_make1_impl(mcx, T_OidList, ListCell::from_oid(datum)),
        Some(mut list) => {
            debug_assert!(list.r#type == T_OidList);
            reserve_one(mcx, &mut list)?;
            list.elements.insert(0, ListCell::from_oid(datum));
            list.check_invariants();
            Ok(list)
        }
    }
}

// ---------------------------------------------------------------------------
// list_concat / list_concat_copy.
// ---------------------------------------------------------------------------

/// `list_concat(list1, list2)` (list.c): append every cell of `list2` to
/// `list1`, returning `list1`.  `list2` is unchanged.
pub fn list_concat<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<PgBox<'mcx, List<'mcx>>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let list1 = match list1 {
        None => return list_copy(mcx, list2),
        Some(l) => l,
    };
    let list2 = match list2 {
        None => return Ok(Some(list1)),
        Some(l) => l,
    };
    debug_assert!(list1.r#type == list2.r#type);

    let mut list1 = list1;
    let new_len = list1.length() + list2.length();
    list1
        .elements
        .try_reserve(list2.elements.len())
        .map_err(|_| mcx.oom(list2.elements.len() * core::mem::size_of::<ListCell>()))?;
    // Even if list1 == list2 in C this would be a memcpy; here list2 is a
    // distinct borrow, so a straight extend is safe.
    list1.elements.extend_from_slice(&list2.elements);
    list1.check_invariants();
    Ok(Some(list1))
}

/// `list_concat_copy(list1, list2)` (list.c): a freshly allocated list holding
/// the cells of `list1` then `list2`.  Neither input is modified.
pub fn list_concat_copy<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let list1 = match list1 {
        None => return list_copy(mcx, list2),
        Some(l) => l,
    };
    let list2 = match list2 {
        None => return list_copy(mcx, Some(list1)),
        Some(l) => l,
    };
    debug_assert!(list1.r#type == list2.r#type);

    let new_len = list1.length() + list2.length();
    let mut result = new_list(mcx, list1.r#type, new_len)?;
    result.elements[..list1.elements.len()].copy_from_slice(&list1.elements);
    result.elements[list1.elements.len()..].copy_from_slice(&list2.elements);
    result.check_invariants();
    Ok(Some(result))
}

// ---------------------------------------------------------------------------
// list_truncate.
// ---------------------------------------------------------------------------

/// `list_truncate(list, new_size)` (list.c): drop all but the first `new_size`
/// cells.  `new_size <= 0` truncates to `NIL`; an over-long `new_size` is a
/// no-op.
pub fn list_truncate<'mcx>(
    list: Option<PgBox<'mcx, List<'mcx>>>,
    new_size: i32,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    if new_size <= 0 {
        return None; // truncate to zero length
    }
    let mut list = match list {
        None => return None,
        Some(l) => l,
    };
    if new_size < list.length() {
        list.elements.truncate(new_size as usize);
    }
    Some(list)
}

// ---------------------------------------------------------------------------
// list_member family.
// ---------------------------------------------------------------------------

/// `list_member(list, datum)` (list.c): membership by `equal()`.  Pointer list.
pub fn list_member(list: Option<&List<'_>>, datum: *const c_void) -> bool {
    let list = match list {
        None => return false,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_List);
    list.check_invariants();
    for cell in list.elements.iter() {
        if equal::call(cell_ptr(cell), datum) {
            return true;
        }
    }
    false
}

/// `list_member_ptr(list, datum)` (list.c): membership by pointer identity.
pub fn list_member_ptr(list: Option<&List<'_>>, datum: *const c_void) -> bool {
    let list = match list {
        None => return false,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_List);
    list.check_invariants();
    for cell in list.elements.iter() {
        if cell_ptr(cell) as *const c_void == datum {
            return true;
        }
    }
    false
}

/// `list_member_int(list, datum)` (list.c).
pub fn list_member_int(list: Option<&List<'_>>, datum: i32) -> bool {
    let list = match list {
        None => return false,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_IntList);
    list.check_invariants();
    list.elements.iter().any(|c| cell_int(c) == datum)
}

/// `list_member_oid(list, datum)` (list.c).
pub fn list_member_oid(list: Option<&List<'_>>, datum: Oid) -> bool {
    let list = match list {
        None => return false,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_OidList);
    list.check_invariants();
    list.elements.iter().any(|c| cell_oid(c) == datum)
}

/// `list_member_xid(list, datum)` (list.c).
pub fn list_member_xid(list: Option<&List<'_>>, datum: TransactionId) -> bool {
    let list = match list {
        None => return false,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_XidList);
    list.check_invariants();
    list.elements.iter().any(|c| cell_xid(c) == datum)
}

// ---------------------------------------------------------------------------
// list_delete family.
// ---------------------------------------------------------------------------

/// `list_delete_nth_cell(list, n)` (list.c): delete the `n`'th cell; returns
/// `NIL` if that was the last cell (C frees the whole list).
pub fn list_delete_nth_cell<'mcx>(
    mut list: PgBox<'mcx, List<'mcx>>,
    n: i32,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    list.check_invariants();
    debug_assert!(n >= 0 && n < list.length());

    if list.length() == 1 {
        // C frees the whole list and returns NIL.
        list_free(Some(list));
        return None;
    }
    list.elements.remove(n as usize);
    Some(list)
}

/// `list_delete_cell(list, cell)` (list.c): delete the cell at index `cell_idx`
/// (C computes the index from the cell address).
pub fn list_delete_cell<'mcx>(
    list: PgBox<'mcx, List<'mcx>>,
    cell_idx: i32,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    list_delete_nth_cell(list, cell_idx)
}

/// `list_delete(list, datum)` (list.c): delete the first `equal()` match.
pub fn list_delete<'mcx>(
    list: PgBox<'mcx, List<'mcx>>,
    datum: *mut c_void,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    debug_assert!(list.r#type == T_List);
    list.check_invariants();
    let mut found = None;
    for (i, cell) in list.elements.iter().enumerate() {
        if equal::call(cell_ptr(cell), datum) {
            found = Some(i as i32);
            break;
        }
    }
    match found {
        Some(i) => list_delete_cell(list, i),
        None => Some(list),
    }
}

/// `list_delete_ptr(list, datum)` (list.c): delete by pointer identity.
pub fn list_delete_ptr<'mcx>(
    list: PgBox<'mcx, List<'mcx>>,
    datum: *mut c_void,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    debug_assert!(list.r#type == T_List);
    list.check_invariants();
    let mut found = None;
    for (i, cell) in list.elements.iter().enumerate() {
        if cell_ptr(cell) == datum {
            found = Some(i as i32);
            break;
        }
    }
    match found {
        Some(i) => list_delete_cell(list, i),
        None => Some(list),
    }
}

/// `list_delete_int(list, datum)` (list.c).
pub fn list_delete_int<'mcx>(
    list: PgBox<'mcx, List<'mcx>>,
    datum: i32,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    debug_assert!(list.r#type == T_IntList);
    list.check_invariants();
    let found = list.elements.iter().position(|c| cell_int(c) == datum);
    match found {
        Some(i) => list_delete_cell(list, i as i32),
        None => Some(list),
    }
}

/// `list_delete_oid(list, datum)` (list.c).
pub fn list_delete_oid<'mcx>(
    list: PgBox<'mcx, List<'mcx>>,
    datum: Oid,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    debug_assert!(list.r#type == T_OidList);
    list.check_invariants();
    let found = list.elements.iter().position(|c| cell_oid(c) == datum);
    match found {
        Some(i) => list_delete_cell(list, i as i32),
        None => Some(list),
    }
}

/// `list_delete_first(list)` (list.c): delete the first element.
pub fn list_delete_first<'mcx>(list: Option<PgBox<'mcx, List<'mcx>>>) -> Option<PgBox<'mcx, List<'mcx>>> {
    match list {
        None => None,
        Some(list) => list_delete_nth_cell(list, 0),
    }
}

/// `list_delete_last(list)` (list.c): delete the last element.
pub fn list_delete_last<'mcx>(list: Option<PgBox<'mcx, List<'mcx>>>) -> Option<PgBox<'mcx, List<'mcx>>> {
    let list = match list {
        None => return None,
        Some(l) => l,
    };
    if list_length(Some(&list)) <= 1 {
        // list_truncate won't free, but this should.
        list_free(Some(list));
        return None;
    }
    let new_len = list.length() - 1;
    list_truncate(Some(list), new_len)
}

/// `list_delete_first_n(list, n)` (list.c): delete the first `n` cells.
pub fn list_delete_first_n<'mcx>(
    list: Option<PgBox<'mcx, List<'mcx>>>,
    n: i32,
) -> Option<PgBox<'mcx, List<'mcx>>> {
    let mut list = match list {
        None => return None,
        Some(l) => l,
    };
    list.check_invariants();
    // No-op request?
    if n <= 0 {
        return Some(list);
    }
    // Delete whole list?
    if n >= list_length(Some(&list)) {
        list_free(Some(list));
        return None;
    }
    list.elements.drain(0..n as usize);
    Some(list)
}

// ---------------------------------------------------------------------------
// list_union family.
// ---------------------------------------------------------------------------

/// `list_union(list1, list2)` (list.c): `list_copy(list1)` plus every member of
/// `list2` not already present (by `equal()`).
pub fn list_union<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    let mut result = list_copy(mcx, list1)?;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member(result.as_deref(), v) {
                result = Some(lappend(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_union_ptr(list1, list2)` (list.c): as above, by pointer identity.
pub fn list_union_ptr<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    let mut result = list_copy(mcx, list1)?;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member_ptr(result.as_deref(), v) {
                result = Some(lappend(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_union_int(list1, list2)` (list.c).
pub fn list_union_int<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_IntList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_IntList));

    let mut result = list_copy(mcx, list1)?;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_int(cell);
            if !list_member_int(result.as_deref(), v) {
                result = Some(lappend_int(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_union_oid(list1, list2)` (list.c).
pub fn list_union_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_OidList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_OidList));

    let mut result = list_copy(mcx, list1)?;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_oid(cell);
            if !list_member_oid(result.as_deref(), v) {
                result = Some(lappend_oid(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// list_intersection family.
// ---------------------------------------------------------------------------

/// `list_intersection(list1, list2)` (list.c): cells of `list1` whose value is
/// in `list2` (by `equal()`).  Freshly allocated; `NIL` if either input is NIL.
pub fn list_intersection<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let (l1, l2) = match (list1, list2) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(None),
    };
    debug_assert!(l1.r#type == T_List);
    debug_assert!(l2.r#type == T_List);

    let mut result = None;
    for cell in l1.elements.iter() {
        let v = cell_ptr(cell);
        if list_member(Some(l2), v) {
            result = Some(lappend(mcx, result, v)?);
        }
    }
    Ok(result)
}

/// `list_intersection_int(list1, list2)` (list.c).
pub fn list_intersection_int<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let (l1, l2) = match (list1, list2) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(None),
    };
    debug_assert!(l1.r#type == T_IntList);
    debug_assert!(l2.r#type == T_IntList);

    let mut result = None;
    for cell in l1.elements.iter() {
        let v = cell_int(cell);
        if list_member_int(Some(l2), v) {
            result = Some(lappend_int(mcx, result, v)?);
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// list_difference family.
// ---------------------------------------------------------------------------

/// `list_difference(list1, list2)` (list.c): cells of `list1` not in `list2`
/// (by `equal()`).
pub fn list_difference<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    if list2.is_none() {
        return list_copy(mcx, list1);
    }
    let mut result = None;
    if let Some(l1) = list1 {
        for cell in l1.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member(list2, v) {
                result = Some(lappend(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_difference_ptr(list1, list2)` (list.c): by pointer identity.
pub fn list_difference_ptr<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    if list2.is_none() {
        return list_copy(mcx, list1);
    }
    let mut result = None;
    if let Some(l1) = list1 {
        for cell in l1.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member_ptr(list2, v) {
                result = Some(lappend(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_difference_int(list1, list2)` (list.c).
pub fn list_difference_int<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_IntList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_IntList));

    if list2.is_none() {
        return list_copy(mcx, list1);
    }
    let mut result = None;
    if let Some(l1) = list1 {
        for cell in l1.elements.iter() {
            let v = cell_int(cell);
            if !list_member_int(list2, v) {
                result = Some(lappend_int(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

/// `list_difference_oid(list1, list2)` (list.c).
pub fn list_difference_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<&List<'mcx>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.map_or(true, |l| l.r#type == T_OidList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_OidList));

    if list2.is_none() {
        return list_copy(mcx, list1);
    }
    let mut result = None;
    if let Some(l1) = list1 {
        for cell in l1.elements.iter() {
            let v = cell_oid(cell);
            if !list_member_oid(list2, v) {
                result = Some(lappend_oid(mcx, result, v)?);
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// list_append_unique family.
// ---------------------------------------------------------------------------

/// `list_append_unique(list, datum)` (list.c): append unless already present
/// (by `equal()`).
pub fn list_append_unique<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: *mut c_void,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    if list_member(list.as_deref(), datum) {
        Ok(list)
    } else {
        Ok(Some(lappend(mcx, list, datum)?))
    }
}

/// `list_append_unique_ptr(list, datum)` (list.c): by pointer identity.
pub fn list_append_unique_ptr<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: *mut c_void,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    if list_member_ptr(list.as_deref(), datum) {
        Ok(list)
    } else {
        Ok(Some(lappend(mcx, list, datum)?))
    }
}

/// `list_append_unique_int(list, datum)` (list.c).
pub fn list_append_unique_int<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: i32,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    if list_member_int(list.as_deref(), datum) {
        Ok(list)
    } else {
        Ok(Some(lappend_int(mcx, list, datum)?))
    }
}

/// `list_append_unique_oid(list, datum)` (list.c).
pub fn list_append_unique_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list: Option<PgBox<'mcx, List<'mcx>>>,
    datum: Oid,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    if list_member_oid(list.as_deref(), datum) {
        Ok(list)
    } else {
        Ok(Some(lappend_oid(mcx, list, datum)?))
    }
}

// ---------------------------------------------------------------------------
// list_concat_unique family.
// ---------------------------------------------------------------------------

/// `list_concat_unique(list1, list2)` (list.c): append each member of `list2`
/// not already in `list1` (by `equal()`), in order; modifies `list1`.
pub fn list_concat_unique<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<PgBox<'mcx, List<'mcx>>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.as_deref().map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    let mut list1 = list1;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member(list1.as_deref(), v) {
                list1 = Some(lappend(mcx, list1, v)?);
            }
        }
    }
    Ok(list1)
}

/// `list_concat_unique_ptr(list1, list2)` (list.c): by pointer identity.
pub fn list_concat_unique_ptr<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<PgBox<'mcx, List<'mcx>>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.as_deref().map_or(true, |l| l.r#type == T_List));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_List));

    let mut list1 = list1;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_ptr(cell);
            if !list_member_ptr(list1.as_deref(), v) {
                list1 = Some(lappend(mcx, list1, v)?);
            }
        }
    }
    Ok(list1)
}

/// `list_concat_unique_int(list1, list2)` (list.c).
pub fn list_concat_unique_int<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<PgBox<'mcx, List<'mcx>>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.as_deref().map_or(true, |l| l.r#type == T_IntList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_IntList));

    let mut list1 = list1;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_int(cell);
            if !list_member_int(list1.as_deref(), v) {
                list1 = Some(lappend_int(mcx, list1, v)?);
            }
        }
    }
    Ok(list1)
}

/// `list_concat_unique_oid(list1, list2)` (list.c).
pub fn list_concat_unique_oid<'mcx>(
    mcx: Mcx<'mcx>,
    list1: Option<PgBox<'mcx, List<'mcx>>>,
    list2: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    debug_assert!(list1.as_deref().map_or(true, |l| l.r#type == T_OidList));
    debug_assert!(list2.map_or(true, |l| l.r#type == T_OidList));

    let mut list1 = list1;
    if let Some(l2) = list2 {
        for cell in l2.elements.iter() {
            let v = cell_oid(cell);
            if !list_member_oid(list1.as_deref(), v) {
                list1 = Some(lappend_oid(mcx, list1, v)?);
            }
        }
    }
    Ok(list1)
}

// ---------------------------------------------------------------------------
// list_deduplicate_oid.
// ---------------------------------------------------------------------------

/// `list_deduplicate_oid(list)` (list.c): collapse *adjacent* duplicate OIDs
/// in-place.  Caller must have sorted the list first.
pub fn list_deduplicate_oid(list: Option<&mut List<'_>>) {
    let list = match list {
        None => return,
        Some(l) => l,
    };
    debug_assert!(list.r#type == T_OidList);
    let len = list.length();
    if len > 1 {
        let elements = &mut list.elements;
        let mut i = 0usize;
        for j in 1..len as usize {
            // elements[i].oid_value != elements[j].oid_value
            let ej = unsafe { elements[j].oid_value };
            if unsafe { elements[i].oid_value } != ej {
                i += 1;
                elements[i].oid_value = ej;
            }
        }
        list.elements.truncate(i + 1);
    }
    list.check_invariants();
}

// ---------------------------------------------------------------------------
// list_free / list_free_deep.
// ---------------------------------------------------------------------------

/// `list_free_private(list, deep)` (list.c): drop all storage, and (if `deep`)
/// the pointed-to objects too.
///
/// In the owned model dropping the `PgBox<List>` reclaims the cell array and
/// header.  C's `deep` variant additionally `pfree`s each `ptr_value`; since
/// those nodes are owned/freed by their owning context here, the deep free is a
/// no-op beyond dropping the list itself — the pointers are non-owning views.
fn list_free_private(list: Option<PgBox<'_, List<'_>>>, _deep: bool) {
    // Dropping the PgBox frees the cell array + header in `mcx`.
    drop(list);
}

/// `list_free(list)` (list.c): free the cells and the list itself; pointed-to
/// objects are NOT freed.
pub fn list_free(list: Option<PgBox<'_, List<'_>>>) {
    list_free_private(list, false);
}

/// `list_free_deep(list)` (list.c): like `list_free`, but also frees every
/// pointed-to object.  Only valid for pointer lists.
pub fn list_free_deep(list: Option<PgBox<'_, List<'_>>>) {
    debug_assert!(list.as_deref().map_or(true, |l| l.r#type == T_List));
    list_free_private(list, true);
}

// ---------------------------------------------------------------------------
// list_copy family.
// ---------------------------------------------------------------------------

/// `list_copy(oldlist)` (list.c): a shallow copy (cells copied, pointed-to
/// objects shared).
pub fn list_copy<'mcx>(
    mcx: Mcx<'mcx>,
    oldlist: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let oldlist = match oldlist {
        None => return Ok(None),
        Some(l) => l,
    };
    let mut newlist = new_list(mcx, oldlist.r#type, oldlist.length())?;
    newlist.elements.copy_from_slice(&oldlist.elements);
    newlist.check_invariants();
    Ok(Some(newlist))
}

/// `list_copy_head(oldlist, len)` (list.c): a shallow copy of the first `len`
/// cells (clamped to the list length); `NIL` if `len <= 0`.
pub fn list_copy_head<'mcx>(
    mcx: Mcx<'mcx>,
    oldlist: Option<&List<'mcx>>,
    len: i32,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let oldlist = match oldlist {
        None => return Ok(None),
        Some(l) => l,
    };
    if len <= 0 {
        return Ok(None);
    }
    let len = core::cmp::min(oldlist.length(), len);
    let mut newlist = new_list(mcx, oldlist.r#type, len)?;
    newlist
        .elements
        .copy_from_slice(&oldlist.elements[..len as usize]);
    newlist.check_invariants();
    Ok(Some(newlist))
}

/// `list_copy_tail(oldlist, nskip)` (list.c): a shallow copy without the first
/// `nskip` cells; `NIL` if that skips the whole list.
pub fn list_copy_tail<'mcx>(
    mcx: Mcx<'mcx>,
    oldlist: Option<&List<'mcx>>,
    nskip: i32,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let nskip = if nskip < 0 { 0 } else { nskip };
    let oldlist = match oldlist {
        None => return Ok(None),
        Some(l) => l,
    };
    if nskip >= oldlist.length() {
        return Ok(None);
    }
    let new_len = oldlist.length() - nskip;
    let mut newlist = new_list(mcx, oldlist.r#type, new_len)?;
    newlist
        .elements
        .copy_from_slice(&oldlist.elements[nskip as usize..]);
    newlist.check_invariants();
    Ok(Some(newlist))
}

/// `list_copy_deep(oldlist)` (list.c): a deep copy — each element is copied via
/// `copyObject()`.  Only sensible for pointer lists.
pub fn list_copy_deep<'mcx>(
    mcx: Mcx<'mcx>,
    oldlist: Option<&List<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, List<'mcx>>>> {
    let oldlist = match oldlist {
        None => return Ok(None),
        Some(l) => l,
    };
    debug_assert!(oldlist.r#type == T_List);
    let mut newlist = new_list(mcx, oldlist.r#type, oldlist.length())?;
    for i in 0..newlist.elements.len() {
        let copied = copy_object_impl::call(cell_ptr(&oldlist.elements[i]));
        newlist.elements[i] = ListCell::from_ptr(copied);
    }
    newlist.check_invariants();
    Ok(Some(newlist))
}

// ---------------------------------------------------------------------------
// list_sort and comparators.
// ---------------------------------------------------------------------------

/// `list_sort_comparator` (pg_list.h): `int (*)(const ListCell *, const ListCell *)`.
pub type ListSortComparator = fn(&ListCell, &ListCell) -> i32;

/// `list_sort(list, cmp)` (list.c): sort the cells in place via `qsort`.
///
/// C's `qsort` is not stability-guaranteed; Rust's `sort_unstable_by` matches
/// that contract and runtime (O(N log N)).
pub fn list_sort(list: Option<&mut List<'_>>, cmp: ListSortComparator) {
    let list = match list {
        None => return,
        Some(l) => l,
    };
    list.check_invariants();
    let len = list.length();
    if len > 1 {
        list.elements
            .sort_unstable_by(|a, b| cmp(a, b).cmp(&0));
    }
}

/// `list_int_cmp(p1, p2)` (list.c): ascending int order (`pg_cmp_s32`).
pub fn list_int_cmp(p1: &ListCell, p2: &ListCell) -> i32 {
    let v1 = cell_int(p1);
    let v2 = cell_int(p2);
    // pg_cmp_s32: (a > b) - (a < b)
    (v1 > v2) as i32 - (v1 < v2) as i32
}

/// `list_oid_cmp(p1, p2)` (list.c): ascending OID order (`pg_cmp_u32`).
pub fn list_oid_cmp(p1: &ListCell, p2: &ListCell) -> i32 {
    let v1 = cell_oid(p1);
    let v2 = cell_oid(p2);
    // pg_cmp_u32: (a > b) - (a < b)
    (v1 > v2) as i32 - (v1 < v2) as i32
}
