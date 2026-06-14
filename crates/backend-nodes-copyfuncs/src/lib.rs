//! `backend-nodes-copyfuncs` — idiomatic owned-tree port of
//! `src/backend/nodes/copyfuncs.c`.
//!
//! `copyfuncs.c` provides one public entry point, `copyObjectImpl(const void
//! *from)` (spelled `copyObject(obj)` via the macro in `nodes/nodes.h`): a deep
//! copy of an arbitrary `Node` tree. In C the body is a generated `switch` on
//! the node tag that dispatches to a per-node `_copy<Type>` routine, each of
//! which copies scalar fields and recurses into child nodes through
//! `copyObjectImpl` again.
//!
//! In the owned-tree rewrite that generated switch + per-node copy logic *is*
//! the central [`types_nodes::nodes::Node::clone_in`] dispatch: a `match` over
//! every variant of the unified [`types_nodes::nodes::Node`] enum delegating each
//! arm to that struct's `#[derive(PgNode)]`/`clone_in` copy, which recurses into
//! its child links through the same central dispatch. C's `copyObject` allocates
//! into `CurrentMemoryContext`; the owned-tree analogue re-homes ALL allocation
//! onto `mcx`, threading the destination context explicitly and returning
//! `PgResult` (a charged allocation can OOM — the C `ereport(ERROR)`).
//!
//! This crate is therefore a **thin wrapper**: it exposes the `copyObject`
//! public API and wires the centralized
//! [`backend_nodes_copyfuncs_seams::copy_object`] seam — every call delegates
//! directly to [`types_nodes::nodes::Node::clone_in`]. No per-node copy logic is
//! reimplemented here, and the crate declares zero local seams.

#![no_std]
#![forbid(unsafe_code)]
// The public API spells the C entry points `copyObjectImpl` / `copyObject`
// (`copyfuncs.c` / `nodes/nodes.h`) verbatim, matching every other ported node
// crate's faithful-name convention.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;

/// `copyObjectImpl(from)` — implementation of `copyObject()`; see
/// `nodes/nodes.h`. Deep-copies an arbitrary `Node` tree into the destination
/// context `dst`, returning a freshly allocated, structurally equal tree.
/// Fallible: copying allocates against `dst` (the C `ereport(ERROR)` on OOM).
///
/// This is the thin owned-tree analogue of `copyfuncs.c`'s `copyObjectImpl`: it
/// delegates straight to the central [`Node::clone_in`] dispatch, which performs
/// the tag-discriminated recursion that the C generated `switch` + per-node
/// `_copy<Type>` routines perform.
#[inline]
pub fn copyObjectImpl<'dst>(from: &Node<'_>, dst: Mcx<'dst>) -> PgResult<Node<'dst>> {
    from.clone_in(dst)
}

/// `copyObject(obj)` — public deep-copy entry point (`nodes/nodes.h` macro).
///
/// In C this is a type-preserving macro over `copyObjectImpl`; here the owned
/// `Node` enum already preserves the concrete variant, so `copyObject` is a
/// thin alias of [`copyObjectImpl`]. The destination context `dst` is the
/// owned-tree analogue of C's implicit `CurrentMemoryContext`.
#[inline]
pub fn copyObject<'dst>(obj: &Node<'_>, dst: Mcx<'dst>) -> PgResult<Node<'dst>> {
    copyObjectImpl(obj, dst)
}

/// Wire the centralized [`backend_nodes_copyfuncs_seams::copy_object`] seam to
/// this crate's implementation. Call once, single-threaded, during startup
/// (`init_seams()`). The installed implementation delegates directly to the
/// central [`Node::clone_in`] dispatch.
pub fn init_seams() {
    backend_nodes_copyfuncs_seams::copy_object::set(|dst, n| n.clone_in(dst));
}
