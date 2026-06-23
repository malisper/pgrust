//! `List` / `ListCell` (nodes/pg_list.h) — the generic `List` carrier.
//!
//! Postgres' `List` is an expansible array of `ListCell` unions, tagged with a
//! `NodeTag` selecting which of the four list flavours it is (`T_List` /
//! `T_IntList` / `T_OidList` / `T_XidList`).  The *only* valid representation of
//! an empty list is the NULL pointer (`NIL`); a non-NIL list always has
//! `length >= 1`.
//!
//! ```c
//! typedef union ListCell {
//!     void          *ptr_value;
//!     int            int_value;
//!     Oid            oid_value;
//!     TransactionId  xid_value;
//! } ListCell;
//!
//! typedef struct List {
//!     NodeTag    type;        /* T_List, T_IntList, T_OidList, or T_XidList */
//!     int        length;      /* number of elements currently present */
//!     int        max_length;  /* allocated length of elements[] */
//!     ListCell  *elements;    /* re-allocatable array of cells */
//!     ListCell   initial_elements[FLEXIBLE_ARRAY_MEMBER];
//! } List;
//! ```
//!
//! ## Owned model
//!
//! C represents `NIL` as a NULL `List *`, so the owned model is
//! `Option<PgBox<List>>` (or any borrow that is `Option<&List>`): `None` is
//! `NIL`, and a `Some` list is guaranteed non-empty.
//!
//! The C `length` / `max_length` / `elements` / `initial_elements` quartet only
//! exists to hand-roll an expansible array out of a flexible array member; the
//! owned model carries that as a single context-allocated [`PgVec`] of cells,
//! which already knows its length and capacity.  `ListCell` keeps the exact C
//! ABI: it is a `union` over `*mut c_void` / `i32` / `Oid` / `TransactionId`.
//!
//! The `bms_*`-style set operations and constructors live with their owning
//! `nodes/list.c` unit (`backend-nodes-core::list`); this module only carries
//! the storage type so the plan/parse trees can thread `List` without a
//! dependency cycle.

use core::ffi::c_void;

use ::mcx::PgVec;
use ::types_core::{Oid, TransactionId};

use crate::nodes::NodeTag;

/// `union ListCell` (nodes/pg_list.h).  Exact C ABI: a word-sized union over a
/// raw pointer (for `T_List`), an `int` (`T_IntList`), an `Oid` (`T_OidList`),
/// or a `TransactionId` (`T_XidList`).  Which member is live is determined by
/// the owning [`List`]'s `type` tag, exactly as in C.
#[derive(Clone, Copy)]
#[repr(C)]
pub union ListCell {
    /// `void *ptr_value` — the cell holds a `Node *` (usually) for `T_List`.
    pub ptr_value: *mut c_void,
    /// `int int_value` — for `T_IntList`.
    pub int_value: i32,
    /// `Oid oid_value` — for `T_OidList`.
    pub oid_value: Oid,
    /// `TransactionId xid_value` — for `T_XidList`.
    pub xid_value: TransactionId,
}

impl ListCell {
    /// `(ListCell) {.ptr_value = v}` (`list_make_ptr_cell`).
    #[inline]
    pub fn from_ptr(v: *mut c_void) -> ListCell {
        ListCell { ptr_value: v }
    }

    /// `(ListCell) {.int_value = v}` (`list_make_int_cell`).
    #[inline]
    pub fn from_int(v: i32) -> ListCell {
        ListCell { int_value: v }
    }

    /// `(ListCell) {.oid_value = v}` (`list_make_oid_cell`).
    #[inline]
    pub fn from_oid(v: Oid) -> ListCell {
        ListCell { oid_value: v }
    }

    /// `(ListCell) {.xid_value = v}` (`list_make_xid_cell`).
    #[inline]
    pub fn from_xid(v: TransactionId) -> ListCell {
        ListCell { xid_value: v }
    }
}

impl core::fmt::Debug for ListCell {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The live member is only known from the owning List's tag; show the
        // raw word.
        write!(f, "ListCell({:?})", unsafe { self.ptr_value })
    }
}

/// `struct List` (nodes/pg_list.h).  A non-empty, tagged, expansible array of
/// [`ListCell`].  The C NULL list (`NIL`) is represented as `None` in the owned
/// model, so a live `List` always has `length >= 1` (invariant checked by
/// [`List::check_invariants`]).
#[derive(Debug)]
pub struct List<'mcx> {
    /// `NodeTag type` — `T_List`, `T_IntList`, `T_OidList`, or `T_XidList`.
    pub r#type: NodeTag,
    /// `ListCell *elements` — the re-allocatable cell array.  The C
    /// `length` / `max_length` / `initial_elements[]` machinery folds into this
    /// single context-allocated vector.
    pub elements: PgVec<'mcx, ListCell>,
}

impl<'mcx> List<'mcx> {
    /// `list_length(l)` — number of valid cells.  (For a live `List` this is
    /// `>= 1`; `NIL` lists are `None` and report 0 via [`list_length`].)
    #[inline]
    pub fn length(&self) -> i32 {
        self.elements.len() as i32
    }

    /// `check_list_invariants(list)` (list.c, USE_ASSERT_CHECKING): a live List
    /// has positive length and a valid type tag.
    #[inline]
    pub fn check_invariants(&self) {
        debug_assert!(self.length() > 0);
        debug_assert!(
            self.r#type == crate::nodes::T_List
                || self.r#type == crate::nodes::T_IntList
                || self.r#type == crate::nodes::T_OidList
                || self.r#type == crate::nodes::T_XidList
        );
    }
}

/// `list_length(l)` (pg_list.h) — `l ? l->length : 0`, treating `NIL`
/// (`None`) as length 0.
#[inline]
pub fn list_length(l: Option<&List<'_>>) -> i32 {
    l.map_or(0, |l| l.length())
}

// `List` is handled specially by `copyfuncs.c`/`equalfuncs.c` (it is not a
// generated `_copyFoo`/`_equalFoo`; `copyObjectImpl` switches on the list tag
// and `equal()` calls `_equalList`). The owned-tree analogue is these
// hand-written trait impls, mirroring those special cases.

impl<'mcx> node_support::PgNodeCopy for List<'mcx> {
    type Bound<'dst> = List<'dst>;
    /// `copyObjectImpl` list arm. For `T_IntList`/`T_OidList`/`T_XidList` C does
    /// a shallow `list_copy` (scalar cells need no deep copy). For `T_List` C
    /// does `list_copy_deep` (each cell is a `Node *` deep-copied via
    /// `copyObject`). This `List` is the raw `pg_list.h` cell carrier: a
    /// `T_List` cell holds an opaque `void *ptr_value` ([`ListCell`]), so the
    /// carrier deep-copies the cell array verbatim (the pointed-to `Node`s are
    /// owned by whatever typed list owner threads them — opacity inherited from
    /// the C union, not introduced here). The word storage is re-homed onto the
    /// target context.
    fn copy_node_in<'dst>(
        &self,
        dst: ::mcx::Mcx<'dst>,
    ) -> types_error::PgResult<Self::Bound<'dst>> {
        Ok(List {
            r#type: self.r#type,
            elements: ::mcx::slice_in(dst, &self.elements)?,
        })
    }
}

impl node_support::PgNodeEqual for List<'_> {
    /// `_equalList(a, b)` (equalfuncs.c). Reject quickly on `type`/`length`, then
    /// compare cells per the list flavour: `T_IntList`/`T_OidList`/`T_XidList`
    /// compare the live scalar union member; `T_List` compares the cell pointer
    /// word (the carrier holds opaque `void *` payloads — see `copy_node_in`).
    fn equal_node(&self, other: &Self) -> bool {
        // COMPARE_SCALAR_FIELD(type); COMPARE_SCALAR_FIELD(length);
        if self.r#type != other.r#type || self.length() != other.length() {
            return false;
        }
        // The switch is outside the loop, like C, for efficiency.
        match self.r#type {
            crate::nodes::T_IntList => self
                .elements
                .iter()
                .zip(other.elements.iter())
                // SAFETY: a `T_IntList` keeps the `int_value` member live in
                // every cell (the list tag selects the union member, exactly as
                // in C's `lfirst_int`).
                .all(|(a, b)| unsafe { a.int_value == b.int_value }),
            crate::nodes::T_OidList => self
                .elements
                .iter()
                .zip(other.elements.iter())
                // SAFETY: a `T_OidList` keeps the `oid_value` member live.
                .all(|(a, b)| unsafe { a.oid_value == b.oid_value }),
            crate::nodes::T_XidList => self
                .elements
                .iter()
                .zip(other.elements.iter())
                // SAFETY: a `T_XidList` keeps the `xid_value` member live.
                .all(|(a, b)| unsafe { a.xid_value == b.xid_value }),
            _ => self
                .elements
                .iter()
                .zip(other.elements.iter())
                // T_List: opaque `ptr_value` cells. The carrier compares the
                // pointer word (it does not own the typed `Node` payload to
                // recurse into via `equal()` — opacity inherited from the C
                // union). SAFETY: a `T_List` keeps the `ptr_value` member live.
                .all(|(a, b)| unsafe { a.ptr_value == b.ptr_value }),
        }
    }
}
