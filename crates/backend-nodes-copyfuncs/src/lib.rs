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
//! the central [`types_nodes::node_tree::Node::copy_node_in`] dispatch: a `match` over
//! every variant of the unified [`types_nodes::node_tree::Node`] enum delegating each
//! arm to that struct's `#[derive(PgNode)]`/`copy_node_in` copy, which recurses into
//! its child links through the same central dispatch. C's `copyObject` allocates
//! into `CurrentMemoryContext`; the owned-tree analogue re-homes ALL allocation
//! onto `mcx`, threading the destination context explicitly and returning
//! `PgResult` (a charged allocation can OOM — the C `ereport(ERROR)`).
//!
//! This crate is therefore a **thin wrapper**: it exposes the `copyObject`
//! public API and wires the centralized
//! [`backend_nodes_copyfuncs_seams::copy_object`] seam — every call delegates
//! directly to [`types_nodes::node_tree::Node::copy_node_in`]. No per-node copy logic is
//! reimplemented here, and the crate declares zero local seams.

#![no_std]
#![forbid(unsafe_code)]
// The public API spells the C entry points `copyObjectImpl` / `copyObject`
// (`copyfuncs.c` / `nodes/nodes.h`) verbatim, matching every other ported node
// crate's faithful-name convention.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::node_tree::Node;

/// `copyObjectImpl(from)` — implementation of `copyObject()`; see
/// `nodes/nodes.h`. Deep-copies an arbitrary `Node` tree into the destination
/// context `dst`, returning a freshly allocated, structurally equal tree.
/// Fallible: copying allocates against `dst` (the C `ereport(ERROR)` on OOM).
///
/// This is the thin owned-tree analogue of `copyfuncs.c`'s `copyObjectImpl`: it
/// delegates straight to the central [`Node::copy_node_in`] dispatch, which performs
/// the tag-discriminated recursion that the C generated `switch` + per-node
/// `_copy<Type>` routines perform.
#[inline]
pub fn copyObjectImpl<'dst>(from: &Node<'_>, dst: Mcx<'dst>) -> PgResult<Node<'dst>> {
    from.copy_node_in(dst)
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
/// central [`Node::copy_node_in`] dispatch.
pub fn init_seams() {
    backend_nodes_copyfuncs_seams::copy_object::set(|dst, n| n.copy_node_in(dst));
    // `list_member_oid(list, datum)` (nodes/list.c:722): linear OID membership
    // test over an OID `List`. The caller hands the OID list as a `&[Oid]`
    // slice, so the `foreach` reduces to a slice scan.
    backend_nodes_copyfuncs_pc_seams::list_member_oid::set(|list, oid| {
        Ok(list.iter().any(|&x| x == oid))
    });
}

// The tests need `std` (memory-context construction, `MemoryContext`, the seam
// `static mut` serialization mutex); the crate proper is `#![no_std]`.
#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use mcx::{MemoryContext, PgString};
    use types_nodes::node_tree::Node;
    use types_nodes::value::Float;

    // Installing the process-global seam mutates a `static mut`; serialize the
    // tests that touch it so they cannot race.
    static SEAM_LOCK: Mutex<()> = Mutex::new(());

    /// Build a small CONVERTED-leaf node tree in context `mcx`: a `T_Float`
    /// value node whose `fval` is a context-allocated string. `Float` carries a
    /// `PgString` payload (`char *fval`, `COPY_STRING_FIELD`), so a deep copy of
    /// this node must actually re-home a heap allocation onto the destination
    /// context — exactly the property that makes copy fallible and context-
    /// threaded. (The `pgrust` reference builds `OpExpr(Var, CaseExpr)` from the
    /// primnode families; those have not converted yet in this repo, so the tree
    /// is adapted to the converted Value/Bitmapset/List subset — the deep-copy /
    /// equal / fallible-copy contract under test is identical.)
    fn sample_tree(mcx: mcx::Mcx<'_>) -> Node<'_> {
        Node::Float(Float {
            fval: PgString::from_str_in("12345", mcx).unwrap(),
        })
    }

    #[test]
    fn copy_object_round_trips_across_contexts_and_is_equal() {
        // Source context A and destination context B are independent memory
        // contexts — the owned-tree analogue of copying a node tree out of one
        // `CurrentMemoryContext` into another.
        let ctx_a = MemoryContext::new("copyfuncs-src-A");
        let ctx_b = MemoryContext::new("copyfuncs-dst-B");

        let original = sample_tree(ctx_a.mcx());

        // `copyObject` deep-copies into B.
        let copied = copyObject(&original, ctx_b.mcx()).expect("copy into B must succeed");

        // The deep copy is structurally equal to the source.
        assert!(
            copied.equal_node(&original),
            "copyObject result must be deep-equal to the source"
        );

        // `copyObjectImpl` is the same operation.
        let copied2 = copyObjectImpl(&original, ctx_b.mcx()).expect("copy into B must succeed");
        assert!(copied2.equal_node(&original));

        // The copy genuinely lives in B (it charged bytes there): the `char
        // *fval` string was re-homed onto B's allocator.
        assert!(
            ctx_b.subtree_used() > 0,
            "the deep copy must have charged its string allocation to B"
        );
    }

    #[test]
    fn copy_survives_dropping_the_source_context() {
        // The mcx deep-copy property: a copy made into B outlives the source
        // context A. Build in A, copy into B, then DROP A — the copy must remain
        // valid and equal to a reference value rebuilt independently in B.
        let ctx_b = MemoryContext::new("copyfuncs-dst-B");

        let copied = {
            let ctx_a = MemoryContext::new("copyfuncs-src-A");
            let original = sample_tree(ctx_a.mcx());
            let copied = copyObject(&original, ctx_b.mcx()).expect("copy into B must succeed");
            // `ctx_a` (and `original`, borrowing it) are dropped at the end of
            // this block; `copied` borrows only B and so escapes.
            copied
        };

        // A is gone. The copy still reads correctly out of B.
        let reference = sample_tree(ctx_b.mcx());
        assert!(
            copied.equal_node(&reference),
            "the copy must survive dropping the source context (it lives in B)"
        );
        if let Some(f) = copied.as_float() {
            assert_eq!(f.fval.as_str(), "12345");
        } else {
            panic!("expected a T_Float node");
        }
    }

    /// A small derived node struct carrying a plain scalar field and a
    /// parse-location field, to exercise the `#[derive(PgNode)]` macro's
    /// `COMPARE_SCALAR_FIELD` (compared) vs `COMPARE_LOCATION_FIELD` (no-op)
    /// generation — the analogue of the primnode `location` field the `pgrust`
    /// reference relies on (no converted leaf carries one yet).
    #[derive(Debug, backend_nodes_macros::PgNode)]
    struct LocNode {
        /// `COMPARE_SCALAR_FIELD` — participates in equality.
        value: i32,
        /// `ParseLoc location` — `COMPARE_LOCATION_FIELD` is a no-op, so a diff
        /// here must NOT make two nodes unequal.
        #[pg_node(location)]
        location: i32,
    }

    #[test]
    fn equal_distinguishes_a_scalar_but_ignores_location() {
        use backend_nodes_node_support::PgNodeEqual;

        let base = LocNode { value: 7, location: 10 };

        // Same scalar, different location -> still equal (location ignored).
        let loc_only = LocNode { value: 7, location: 999 };
        assert!(
            base.equal_node(&loc_only),
            "equal_node must ignore a location-only difference (COMPARE_LOCATION_FIELD)"
        );

        // Different scalar -> not equal (location identical, to isolate the
        // scalar as the sole difference).
        let scalar_diff = LocNode { value: 8, location: 10 };
        assert!(
            !base.equal_node(&scalar_diff),
            "equal_node must detect a changed scalar field (COMPARE_SCALAR_FIELD)"
        );
    }

    #[test]
    fn copy_into_a_tiny_limit_context_returns_err_not_panic() {
        // The fallible-copy contract: copying allocates against the destination
        // context, so a context whose limit is too small to hold the copy must
        // surface an `Err` (the C `ereport(ERROR)` on OOM) rather than panic.
        let ctx_src = MemoryContext::new("copyfuncs-src");
        let original = sample_tree(ctx_src.mcx());

        // A destination capped at 1 byte cannot hold the re-homed `fval` string.
        let tiny = MemoryContext::new("copyfuncs-tiny").with_limit(1);
        let result = copyObject(&original, tiny.mcx());
        assert!(
            result.is_err(),
            "copying into an over-limit context must return Err, not panic"
        );
    }

    #[test]
    fn install_routes_the_seam_to_this_impl() {
        let _guard = SEAM_LOCK.lock().unwrap();

        // Wire the centralized seam to this crate's implementation.
        init_seams();

        let ctx_src = MemoryContext::new("copyfuncs-src");
        let ctx_dst = MemoryContext::new("copyfuncs-dst");
        let original = sample_tree(ctx_src.mcx());

        // Reach the implementation purely through the centralized seam.
        let via_seam = backend_nodes_copyfuncs_seams::copy_object::call(ctx_dst.mcx(), &original)
            .expect("seam copy must succeed");

        // The seam must produce a deep copy identical to a direct copy.
        let direct = copyObject(&original, ctx_dst.mcx()).expect("direct copy must succeed");
        assert!(via_seam.equal_node(&direct));
        assert!(via_seam.equal_node(&original));
    }
}
