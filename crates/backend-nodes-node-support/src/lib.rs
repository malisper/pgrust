#![no_std]
//! `backend-nodes-node-support` — runtime support traits for the
//! `#[derive(PgNode)]` macro (see the `backend-nodes-macros` crate) and the
//! central node-tree generator.
//!
//! A `proc-macro = true` crate may only export the macros themselves, so the
//! traits and the container/leaf impls that the *generated* code calls into must
//! live in an ordinary library crate. This is that crate. The generated code
//! refers to these items as `::backend_nodes_node_support::PgNodeCopy` etc.
//!
//! This crate is `#![no_std]` (it only needs `alloc` for the few owned-`Vec`
//! helpers, but otherwise allocates exclusively through the charged `mcx`
//! containers) so that the `#![no_std]` `types-nodes` crate can depend on it
//! without dragging in `std`. It is a LEAF crate: it depends only on `mcx`
//! (memory contexts + charged containers) and `types-error` (the `PgResult`
//! error lane), NOT on `types-nodes`, which is what lets `types-nodes` depend on
//! it without forming a cycle (the central `Node` enum is generated INTO
//! `types-nodes` itself).
//!
//! # Uniform dispatch (mirrors the C COPY_*/COMPARE_* macro families)
//!
//! Every node field — scalar or child — is copied with
//! `field.copy_node_in(dst)?` and compared with `a.equal_node(&b)`. The two
//! trait families are populated by:
//!
//! * **Container impls** (`PgBox<T>`, `Option<T>`, `PgVec<T>`) that forward
//!   through their element so child links recurse automatically, re-homing the
//!   allocation onto the TARGET context. This is `COPY_NODE_FIELD` /
//!   `COMPARE_NODE_FIELD` over a `Node *` / `List *`.
//! * **Node-struct impls**, generated per struct by `#[derive(PgNode)]`, which
//!   recurse field-by-field.
//! * **Scalar-leaf impls**, supplied by the owning crate (here for the Rust
//!   primitives + `Oid` and friends, and in `types-nodes` via the
//!   [`pg_scalar_eq!`] / [`pg_scalar_ignore!`] helpers for the many node-local
//!   enums / aliases / opaque handles). These collapse to `clone()` / a per-type
//!   scalar equality, exactly like `COPY_SCALAR_FIELD` / `COMPARE_SCALAR_FIELD`.
//!
//! # Why copy is FALLIBLE and threads a target context
//!
//! C's `copyObject` deep-copies an arbitrary node tree into
//! `CurrentMemoryContext`; the copy can fail (OOM → `ereport(ERROR)`). The
//! owned-tree port re-homes ALL allocation onto `mcx`: a copy allocates a fresh
//! `PgVec`/`PgBox`/`PgString` charged to the explicit destination context
//! `dst`, and the call returns `PgResult` so an allocation failure surfaces as
//! the destination context's OOM error — exactly the contract of the existing
//! ~42 hand-written `clone_in` methods. The associated type `Bound<'dst>` is the
//! node value re-parameterized to live in `dst` (`Foo<'mcx>` copies to
//! `Foo<'dst>`; a lifetime-free leaf copies to itself).

extern crate alloc;

use mcx::{PgBox, PgString, PgVec};

// `Mcx` (the target context handle) and `PgResult` (the fallible-copy error
// lane) are re-exported under this crate's path so the `#[derive(PgNode)]`-
// generated code (expanded in the downstream `types-nodes` crate) and the
// exported `pg_scalar_*!` macros can name them as
// `::backend_nodes_node_support::Mcx` / `::PgResult` without bringing `mcx` /
// `types-error` into scope themselves.
pub use mcx::Mcx;
pub use types_error::PgResult;

/// Deep-copy a node value INTO a target memory context. Fallible owned-tree
/// analogue of `copyObject` (`copyfuncs.c`), which deep-copies into
/// `CurrentMemoryContext`; here the destination context is threaded explicitly
/// as `dst` and the copy allocates against it.
///
/// `Bound<'dst>` is `Self` re-parameterized to live in `dst`: a node value
/// `Foo<'mcx>` has `Bound<'dst> = Foo<'dst>`, and a lifetime-free leaf has
/// `Bound<'dst> = Self`. The matching `#[derive(PgNode)]` impl sets `Bound`
/// accordingly.
pub trait PgNodeCopy {
    /// The copied value, re-homed to the destination context's lifetime.
    type Bound<'dst>;
    /// Deep-copy `self` into `dst`, allocating the copy there. Fallible: a
    /// charged allocation can hit the context's limit (the C `ereport(ERROR)`
    /// on OOM).
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>>;
}

/// Structural equality of two node values. Owned-tree analogue of `equal()`
/// (`equalfuncs.c`). Infallible and lifetime-agnostic (equality never
/// allocates and is invisible to the memory context the values live in).
pub trait PgNodeEqual {
    fn equal_node(&self, other: &Self) -> bool;
}

/// Opt the Rust primitive leaf types into `PgNodeCopy`/`PgNodeEqual` with a flat
/// `clone()` / `==`. `COPY_SCALAR_FIELD`/`COPY_STRING_FIELD` and
/// `COMPARE_SCALAR_FIELD`/`COMPARE_STRING_FIELD` collapse here. A scalar leaf is
/// lifetime-free, so `Bound<'dst> = Self` and the copy never touches `dst` (a
/// flat value lives wherever its owning node does). These are per-type
/// *concrete* impls (NOT a blanket over a marker) — a blanket over a
/// downstream-implementable marker trait would coherence-conflict with the
/// generic container impls below (`PgBox<T>`/`PgVec<T>`/…), since the compiler
/// cannot prove a downstream type won't be both. Per-type impls sidestep that.
macro_rules! impl_scalar_leaf {
    ($($t:ty),* $(,)?) => {
        $(
            impl PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(&self, _dst: Mcx<'dst>) -> PgResult<Self> {
                    Ok(::core::clone::Clone::clone(self))
                }
            }
            impl PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, other: &Self) -> bool { self == other }
            }
        )*
    };
}

// The Rust primitive scalars. `Oid` and its friends (`Index`, `AttrNumber`,
// `SubTransactionId`, `RepOriginId`, …) are integer *aliases* in `types-core`
// (`Oid = u32`, `Index = u32`, `AttrNumber = i16`, …), so these primitive impls
// ARE their impls — a separate `impl PgNodeCopy for Oid` would coherence-collide
// with the `u32` impl. (Node-local *enums* / newtypes opt in from `types-nodes`
// via `pg_scalar_eq!` / `pg_scalar_ignore!`.)
impl_scalar_leaf!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, bool, char, f32, f64
);

/// Opt a list of *leaf* field types into `PgNodeCopy`/`PgNodeEqual` directly,
/// with `clone()` copy and **`==`** equality — ordinary value leaves
/// (`COMPARE_SCALAR_FIELD`). Use this from the owning crate (`types-nodes`) for
/// every node-struct field type that is a plain value (node-local enums, small
/// `Copy`/`PartialEq` structs/newtypes).
///
/// These emit *concrete* `impl PgNodeCopy for T` / `impl PgNodeEqual for T` (not
/// a blanket), so they never collide with the generic container impls. The
/// listed type must be `Clone + PartialEq` and lifetime-free (`Bound<'dst> =
/// Self`).
///
/// ```ignore
/// pg_scalar_eq!(crate::AggStrategy, crate::ItemPointerData);
/// ```
#[macro_export]
macro_rules! pg_scalar_eq {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(
                    &self,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    ::core::result::Result::Ok(::core::clone::Clone::clone(self))
                }
            }
            impl $crate::PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, other: &Self) -> bool { self == other }
            }
        )*
    };
}

/// Opt a list of *leaf* field types into `PgNodeCopy`/`PgNodeEqual` with a real
/// `clone()` copy but **always-equal** (`true`) equality — the owned-tree
/// analogue of an `equal_ignore` field. Use this for opaque handles /
/// function-pointer aliases that `equalfuncs.c` skips and that cannot derive a
/// meaningful `PartialEq`. The listed type must be `Clone` and lifetime-free
/// (`Bound<'dst> = Self`).
///
/// ```ignore
/// pg_scalar_ignore!(crate::ExprStateEvalFunc);
/// ```
#[macro_export]
macro_rules! pg_scalar_ignore {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(
                    &self,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    ::core::result::Result::Ok(::core::clone::Clone::clone(self))
                }
            }
            impl $crate::PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, _other: &Self) -> bool { true }
            }
        )*
    };
}

// Re-export the leaf macros' helper types under a stable path so the exported
// macros above (expanded in the downstream `types-nodes` crate) can name `Mcx`
// and `PgResult` without the downstream crate having to bring them into scope.
/// `palloc`-shaped fallible `PgVec` constructor, re-exported under a stable path
/// so the `#[derive(PgNode)]`-generated `array_size` copy code (expanded in the
/// downstream `types-nodes` crate) can build a destination-charged `PgVec`
/// without naming `mcx` directly. Forwards to [`mcx::vec_with_capacity_in`].
#[doc(hidden)]
#[inline]
pub fn mcx_vec_with_capacity_in<'dst, T>(
    dst: Mcx<'dst>,
    cap: usize,
) -> PgResult<PgVec<'dst, T>> {
    mcx::vec_with_capacity_in(dst, cap)
}

// ---------------------------------------------------------------------------
// Container impls — these RECURSE through their element via `copy_node_in` /
// `equal_node`, re-homing the allocation onto the TARGET context, so a
// `PgBox<Node>` / `PgVec<Node>` / `Option<PgBox<Node>>` child link deep-copies
// and deep-compares into `dst`.
// ---------------------------------------------------------------------------

/// `Option<T>` field: copy/compare element-wise. Used for nullable scalars and
/// for `Option<PgBox<Node>>` node children (a possibly-NULL `Node *` in C). The
/// `None` arm is the C NULL pointer (copies to NULL without allocating).
impl<T: PgNodeCopy> PgNodeCopy for Option<T> {
    type Bound<'dst> = Option<T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        match self {
            Some(v) => Ok(Some(v.copy_node_in(dst)?)),
            None => Ok(None),
        }
    }
}
impl<T: PgNodeEqual> PgNodeEqual for Option<T> {
    fn equal_node(&self, other: &Self) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => a.equal_node(b),
            (None, None) => true,
            _ => false,
        }
    }
}

/// `PgBox<T>` field — a charged `Node *` child link. Deep-copy the payload into
/// `dst` and box it in a fresh `PgBox` charged to `dst` (the owned-tree analogue
/// of `copyObject`'s `palloc` + recurse on a `Node *`). Compare through the box.
impl<'a, T: PgNodeCopy> PgNodeCopy for PgBox<'a, T> {
    type Bound<'dst> = PgBox<'dst, T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        let payload = (**self).copy_node_in(dst)?;
        mcx::alloc_in(dst, payload)
    }
}
impl<'a, T: PgNodeEqual> PgNodeEqual for PgBox<'a, T> {
    fn equal_node(&self, other: &Self) -> bool {
        (**self).equal_node(&**other)
    }
}

/// `PgVec<T>` field — a charged node `List *`. Deep-copy each element into `dst`
/// (recursing through the element's `copy_node_in`), pushing into a fresh
/// `PgVec` charged to `dst` (the owned-tree analogue of `COPY_NODE_FIELD` over a
/// `List *`, which recurses into every element). Compares length then
/// element-wise (the charge is invisible to equality).
impl<'a, T: PgNodeCopy> PgNodeCopy for PgVec<'a, T> {
    type Bound<'dst> = PgVec<'dst, T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        let mut out = mcx::vec_with_capacity_in(dst, self.len())?;
        for elem in self.iter() {
            out.push(elem.copy_node_in(dst)?);
        }
        Ok(out)
    }
}
impl<'a, T: PgNodeEqual> PgNodeEqual for PgVec<'a, T> {
    fn equal_node(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a.equal_node(b))
    }
}

/// `PgString` field — a charged `char *` (`COPY_STRING_FIELD`). Deep-copy the
/// string bytes into `dst` via `PgString::clone_in`; compare by contents.
impl<'a> PgNodeCopy for PgString<'a> {
    type Bound<'dst> = PgString<'dst>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        self.clone_in(dst)
    }
}
impl<'a> PgNodeEqual for PgString<'a> {
    fn equal_node(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
