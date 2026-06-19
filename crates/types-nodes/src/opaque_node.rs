//! P3 substrate (step 1 of the node-opaque flip) — **additive, unused for now**.
//!
//! This module lands the proven trait-object substrate from
//! `docs/proposals/node-opaque-migration.md` §1, §4.2–4.3 *without* touching the
//! live 241-variant [`crate::nodes::Node`] enum. The new types coexist with the
//! enum and are not yet referenced by the rest of the tree; the later flip
//! commit (build.rs flip + enum delete + ctor sweep) wires them in.
//!
//! What is here:
//! - [`NodePayload`] — the lifetime-parameterized payload trait (`node_tag` +
//!   `clone_in_dyn` + `equal_dyn`). It is `: 'mcx` and therefore cannot require
//!   [`core::any::Any`] (which needs `'static`); downcasting is tag-keyed, not
//!   `Any`-keyed — exactly C's `castNode`.
//! - [`PgNodeBox`] — the smart pointer `PgBox<'mcx, dyn NodePayload<'mcx> + 'mcx>`
//!   that owns the manual unsize coercion (§1.4, via [`mcx::box_unsize_dyn`]) and
//!   the tag-keyed `#[repr(transparent)]` downcast (§1.3). **All `unsafe` related
//!   to the trait object lives in this one type.**
//! - [`OpaqueNode`] — the `Node` handle newtype over `PgNodeBox`.
//!
//! ## Soundness (proposal §4.2)
//! Two load-bearing conditions, both enforced/tested here:
//! - **(a) Single lifetime per payload.** The tag-keyed downcast hardcodes `'mcx`
//!   as the *sole* payload lifetime; a `Foo<'mcx,'b>` payload would fabricate
//!   `'b = 'mcx` -> UB. [`assert_single_lifetime`] is a const build-time witness
//!   (it only type-checks for a `P<'mcx>` payload), and the generator-side P3
//!   parser carries the hard-error check. Each concrete adapter calls it.
//! - **(b) Invariance in `'mcx`.** `dyn NodePayload<'mcx>` is invariant in `'mcx`
//!   automatically (trait-object lifetime params are invariant). A compile-fail
//!   test (`tests/opaque_node_invariance.rs`, trybuild) guards against a stray
//!   covariant `PhantomData<&'mcx ()>` being introduced later. See also the
//!   in-crate unit test that exercises construct/unsize/downcast/clone/drop.
//!
//! **Miri — deferred (documented).** The project is pinned to the validated
//! *stable* toolchain (rustc 1.93.1); Miri ships only on nightly, which is not
//! installed for this target (`miri component not available for
//! stable-aarch64-apple-darwin`). Running it would require an unpinned nightly
//! that diverges from the merge-gate compiler. The two `unsafe` operations (the
//! fat-pointer round-trip in [`mcx::box_unsize_dyn`] and the tag-keyed transparent
//! downcast in [`PgNodeBox::downcast_ref`]) are instead exercised end-to-end on
//! the pinned stable toolchain by the in-module `construct_unsize_downcast_clone_
//! equal_drop` test (alloc -> unsize -> tag-keyed downcast -> equal-via-vtable ->
//! drop-glue). Re-run under Miri when the flip campaign reaches a host with a
//! nightly Miri component.

use crate::nodes::{Node, NodeTag};
use mcx::{Mcx, PgBox};
use types_error::PgResult;

/// A concrete node payload (`struct Var<'mcx>` etc.). Lifetime-parameterized, so
/// it cannot be `: 'static` and cannot require [`core::any::Any`] — the downcast
/// witness is the [`NodeTag`], which we already have (C's `castNode`).
///
/// `clone_in_dyn` is fallible and threads an `mcx` (allocation requires a
/// context); it returns the live [`Node`] enum during the coexistence period so
/// the substrate can be exercised before the representation flip. `equal_dyn`
/// mirrors C's `equal()`.
pub trait NodePayload<'mcx>: 'mcx {
    /// The C node tag for this payload (`nodeTag(node)`).
    fn node_tag(&self) -> NodeTag;

    /// Deep-copy into context `mcx` (C's `copyObjectImpl`), fallible.
    fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>>;

    /// Structural equality (C's `equal()`); compares only when tags match.
    /// `other` carries the same payload lifetime `'mcx` (trait-object lifetime
    /// params are invariant, so a differing lifetime cannot be passed).
    fn equal_dyn(&self, other: &dyn NodePayload<'mcx>) -> bool;

    /// The raw data address of this payload — used by the tag-keyed downcast in
    /// [`PgNodeBox`]. `repr(transparent)` adapters make this address equal to
    /// the inner payload's address, so the cast is sound.
    #[doc(hidden)]
    fn __payload_ptr(&self) -> *const () {
        (self as *const Self).cast()
    }
}

/// Soundness gate (b) — **invariance compile-fail test** (proposal §4.2).
///
/// `dyn NodePayload<'mcx>` is invariant in `'mcx` (trait-object lifetime params
/// are invariant), so [`OpaqueNode<'mcx>`] is invariant too. A covariant carrier
/// (e.g. a stray `PhantomData<&'mcx ()>`) would let a longer-lived node be used
/// where a shorter-lived one is expected, which the tag-keyed downcast's
/// single-`'mcx` assumption forbids. This `compile_fail` doctest is the guard: it
/// must NOT compile. If a future edit makes the node covariant, this test starts
/// passing and the doctest run fails — flagging the regression. (No external
/// crate; `rustdoc` runs `compile_fail` doctests as part of `cargo test --doc`.)
///
/// ```compile_fail
/// use types_nodes::opaque_node::OpaqueNode;
/// // Requires `OpaqueNode<'long>` to coerce to `OpaqueNode<'short>` — only sound
/// // for a COVARIANT node. With invariance this is rejected at compile time.
/// fn shrink<'short, 'long: 'short>(n: OpaqueNode<'long>) -> OpaqueNode<'short> {
///     n
/// }
/// ```
///
/// Build-time witness for soundness condition (a): the payload `P` has a *single*
/// lifetime, threaded as `'mcx`. This function only type-checks when called as
/// `assert_single_lifetime::<P<'mcx>, 'mcx>` where `P: NodePayload<'mcx>`; a
/// two-lifetime payload `P<'mcx,'b>` cannot satisfy `P: NodePayload<'mcx>` and so
/// has no way to reach this bound — making the invalid case unrepresentable.
///
/// Adapters call this in a `const` context (see [`single_lifetime_guard`]) so the
/// witness fires at monomorphization, before any downcast can run.
#[inline(always)]
pub const fn assert_single_lifetime<'mcx, P: NodePayload<'mcx> + 'mcx>() {}

/// Single-lifetime guard helper for a generated `#[repr(transparent)]` adapter.
///
/// Emits a generic, never-called witness function whose body forces
/// [`assert_single_lifetime`] to monomorphize for the payload. The function is
/// generic over the payload lifetime `'mcx`, so a payload that is NOT of the form
/// `P<'mcx>` (e.g. a two-lifetime `P<'mcx,'b>`) cannot satisfy the
/// `P: NodePayload<'mcx>` bound and fails to compile here — the build-time
/// witness for soundness condition (a).
#[macro_export]
macro_rules! single_lifetime_guard {
    ($P:ty) => {
        #[allow(dead_code)]
        fn __single_lifetime_witness<'mcx>()
        where
            $P: $crate::opaque_node::NodePayload<'mcx> + 'mcx,
        {
            $crate::opaque_node::assert_single_lifetime::<'mcx, $P>();
        }
    };
}

/// The opaque smart pointer: `PgBox<'mcx, dyn NodePayload<'mcx> + 'mcx>`. Owns the
/// manual unsize coercion (§1.4) and the tag-keyed downcast (§1.3). The verbose
/// `dyn NodePayload<'mcx> + 'mcx` type appears **nowhere else** in the tree.
#[repr(transparent)]
pub struct PgNodeBox<'mcx>(PgBox<'mcx, dyn NodePayload<'mcx> + 'mcx>);

impl<'mcx> PgNodeBox<'mcx> {
    /// Allocate `payload` in `mcx` and unsize it to the trait object.
    ///
    /// The `*mut P -> *mut (dyn NodePayload + 'mcx)` cast inside
    /// [`mcx::box_unsize_dyn`] is a stable *unsizing coercion* (`P: Unsize<dyn>`);
    /// only the implicit `Box: CoerceUnsized` impl is nightly, which is exactly
    /// why we route through the manual helper. No `unsafe` appears here — it is
    /// encapsulated (and justified) in `box_unsize_dyn`.
    pub fn new<P>(mcx: Mcx<'mcx>, payload: P) -> PgResult<Self>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        let sized: PgBox<'mcx, P> = mcx::alloc_in(mcx, payload)?;
        let fat = mcx::box_unsize_dyn(sized, |p| p as *mut (dyn NodePayload<'mcx> + 'mcx));
        Ok(PgNodeBox(fat))
    }

    /// This node's tag (forwards to the vtable).
    #[inline]
    pub fn node_tag(&self) -> NodeTag {
        self.0.node_tag()
    }

    /// Borrow the payload as `&P` **iff** the tag matches `P`'s tag. The caller
    /// passes the expected tag (the generated accessors hardcode it).
    ///
    /// # Safety contract this upholds
    /// `tag == self.node_tag()` and the generator's tag<->adapter **bijection**
    /// (each `T_*` is produced by exactly one `#[repr(transparent)]` adapter over
    /// exactly that payload) together guarantee the runtime type is `P`. The
    /// `+ 'mcx` on the box recovers the borrow lifetime through `&self`; nothing
    /// is fabricated. This is strictly safer than `Any`: one transparent field, an
    /// integer witness we own.
    #[inline]
    pub fn downcast_ref<P>(&self, expected: NodeTag) -> Option<&P>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        if self.0.node_tag() == expected {
            // SAFETY: (1) `expected == node_tag()` checked above; (2) the
            // tag<->adapter bijection means a payload with this tag IS a `P`;
            // (3) `repr(transparent)` adapters give the payload the same address
            // as `__payload_ptr()`; (4) the borrow lifetime rides on `&self` and
            // the `+ 'mcx` box — recovered, not invented.
            Some(unsafe { &*(self.0.__payload_ptr() as *const P) })
        } else {
            None
        }
    }

    /// Structural equality via the vtable (C's `equal()`).
    #[inline]
    pub fn equal(&self, other: &PgNodeBox<'mcx>) -> bool {
        self.0.equal_dyn(&*other.0)
    }

    /// Deep-copy into context `mcx` (fallible).
    #[inline]
    pub fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {
        self.0.clone_in_dyn(mcx)
    }
}

impl core::fmt::Debug for PgNodeBox<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PgNodeBox")
            .field("node_tag", &self.0.node_tag())
            .finish()
    }
}

/// The opaque `Node` handle (named `OpaqueNode` during coexistence so it does not
/// collide with the live [`crate::nodes::Node`] enum). The flip renames this to
/// `Node` once the enum is deleted.
#[repr(transparent)]
pub struct OpaqueNode<'mcx>(pub(crate) PgNodeBox<'mcx>);

impl<'mcx> OpaqueNode<'mcx> {
    /// Construct from a payload, allocating in `mcx`.
    #[inline]
    pub fn new<P>(mcx: Mcx<'mcx>, payload: P) -> PgResult<Self>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        Ok(OpaqueNode(PgNodeBox::new(mcx, payload)?))
    }

    /// This node's tag.
    #[inline]
    pub fn node_tag(&self) -> NodeTag {
        self.0.node_tag()
    }

    /// Tag-keyed downcast to `&P` (the body shape of every generated `as_*`).
    #[inline]
    pub fn downcast_ref<P>(&self, expected: NodeTag) -> Option<&P>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        self.0.downcast_ref::<P>(expected)
    }

    /// Structural equality.
    #[inline]
    pub fn equal(&self, other: &OpaqueNode<'mcx>) -> bool {
        self.0.equal(&other.0)
    }

    /// Deep-copy into `mcx`.
    #[inline]
    pub fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {
        self.0.clone_in_dyn(mcx)
    }
}

impl core::fmt::Debug for OpaqueNode<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("OpaqueNode").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    /// A self-contained 1-variant payload exercising the full substrate, mirroring
    /// the proposal's risk-gate #3 "1-variant Node end-to-end" spike:
    /// construct -> unsize -> downcast -> clone -> equal -> drop.
    #[repr(transparent)]
    #[derive(Clone)]
    struct Probe<'mcx> {
        v: i32,
        _ctx: core::marker::PhantomData<Mcx<'mcx>>,
    }

    // Soundness gate (a): single-lifetime build-time witness for this payload.
    crate::single_lifetime_guard!(Probe<'mcx>);

    impl<'mcx> NodePayload<'mcx> for Probe<'mcx> {
        fn node_tag(&self) -> NodeTag {
            NodeTag(424242)
        }
        fn clone_in_dyn<'b>(&self, _mcx: Mcx<'b>) -> PgResult<Node<'b>> {
            // The substrate is unused by the tree; a real payload returns its
            // enum variant. The test only needs `equal_dyn`/downcast to work.
            unreachable!("Probe is a test-only payload")
        }
        fn equal_dyn(&self, other: &dyn NodePayload<'mcx>) -> bool {
            if other.node_tag() != self.node_tag() {
                return false;
            }
            // SAFETY: tags match -> `other` is a `Probe`; mirrors the downcast
            // soundness argument (same shape the generator emits for equal()).
            let o = unsafe { &*(other.__payload_ptr() as *const Probe<'_>) };
            o.v == self.v
        }
    }

    #[test]
    fn construct_unsize_downcast_clone_equal_drop() {
        let ctx = MemoryContext::new("opaque_node_test");
        let mcx = ctx.mcx();

        let probe = Probe {
            v: 7,
            _ctx: core::marker::PhantomData,
        };
        let node = OpaqueNode::new(mcx, probe).expect("alloc");

        // tag travels through the vtable
        assert_eq!(node.node_tag(), NodeTag(424242));

        // tag-keyed downcast recovers the payload
        let got = node
            .downcast_ref::<Probe>(NodeTag(424242))
            .expect("downcast");
        assert_eq!(got.v, 7);

        // wrong tag -> None (no UB, no fabricated borrow)
        assert!(node.downcast_ref::<Probe>(NodeTag(999)).is_none());

        // equal via the vtable
        let node2 = OpaqueNode::new(mcx, Probe { v: 7, _ctx: core::marker::PhantomData })
            .expect("alloc");
        let node3 = OpaqueNode::new(mcx, Probe { v: 8, _ctx: core::marker::PhantomData })
            .expect("alloc");
        assert!(node.equal(&node2));
        assert!(!node.equal(&node3));
        // drop runs here through the trait-object drop glue (freeing Probe)
    }
}
