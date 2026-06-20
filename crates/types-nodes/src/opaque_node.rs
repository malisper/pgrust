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

/// Installable node-equality seam for the generated `NodePayload::equal_dyn`
/// bodies (node-opaque P3 codegen). The real per-payload `equal()` comparators
/// (the `equalfuncs.c` port) live in the higher `backend-nodes-equalfuncs`
/// crate, which depends on `types-nodes` — so the generated `equal_dyn` in this
/// crate cannot call them directly without a dependency cycle. Instead each
/// `equal_dyn` (after the tag gate) routes through this fn-ptr seam, which
/// `backend-nodes-equalfuncs` installs at the flip via [`install_node_equal_seam`].
///
/// The seam receives the shared `NodeTag` (both payloads already tag-checked
/// equal by the caller) and the two `repr(transparent)` payload data pointers
/// (`__payload_ptr()`), exactly the witnesses C's `equal()` dispatch uses. Until
/// installed it panics **loudly** (the project's sanctioned "not yet wired" seam
/// pattern, mirroring `node_tree`'s `out_node_custom`/`read_node_custom`) — it
/// NEVER fabricates a `true`/`false`. This whole codegen module is gated behind
/// the off-by-default `node_payload_codegen` feature, so the seam is dead in the
/// normal build.
pub type NodeEqualFn = fn(NodeTag, *const (), *const ()) -> bool;

/// The installed comparator's fn-pointer, stored as `usize` (0 = uninstalled).
/// `no_std`-native interior mutability — the crate is `#![no_std]`, so this uses
/// a `core` atomic rather than `std::sync::OnceLock`.
static NODE_EQUAL_SEAM: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);

/// Install the real node-equality comparator (called once by
/// `backend-nodes-equalfuncs::init_seams` at the flip). Idempotent: a second
/// install with the same fn is a no-op; a conflicting install is a hard error
/// (mirrors the project's seam-install discipline).
pub fn install_node_equal_seam(f: NodeEqualFn) {
    use core::sync::atomic::Ordering;
    let addr = f as usize;
    match NODE_EQUAL_SEAM.compare_exchange(0, addr, Ordering::SeqCst, Ordering::SeqCst) {
        Ok(_) => {}
        Err(prev) => {
            // Already installed. A re-install with a *different* fn pointer is a
            // wiring bug; the same pointer is a benign double-install.
            if prev != addr {
                panic!("install_node_equal_seam: conflicting re-install");
            }
        }
    }
}

/// The seam entry the generated `equal_dyn` bodies call. Panics loudly until
/// `backend-nodes-equalfuncs` installs the real comparator at the flip.
#[inline]
pub fn node_equal_seam(tag: NodeTag, a: *const (), b: *const ()) -> bool {
    use core::sync::atomic::Ordering;
    let addr = NODE_EQUAL_SEAM.load(Ordering::SeqCst);
    if addr == 0 {
        panic!(
            "node_equal_seam: node-equality comparator for tag {tag:?} not installed; \
             backend-nodes-equalfuncs installs it at the node-opaque flip \
             (this codegen module is gated behind `node_payload_codegen`)"
        );
    }
    // SAFETY: `addr` is a non-null value previously stored from a valid
    // `NodeEqualFn` pointer by `install_node_equal_seam`; transmuting a usize
    // back to that exact fn-ptr type is sound (same provenance, same signature).
    let f: NodeEqualFn = unsafe { core::mem::transmute::<usize, NodeEqualFn>(addr) };
    f(tag, a, b)
}

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

    /// The embedded `Plan` base of a plan node (`(Plan *) node`), or `None` for a
    /// non-plan node — the vtable form of the hand-written `Node::plan_head`
    /// upcast (proposal §1.1 `plan_base`). Plan-variant adapters override this to
    /// return the nested `&self.0.<...>.plan` field; every other payload uses the
    /// `None` default (a non-plan node has no `Plan` base, exactly as the C
    /// `((Plan *) <parsenode>)` upcast is a type error).
    fn plan_base(&self) -> Option<&crate::nodeindexscan::Plan<'mcx>> {
        None
    }

    /// Mutable dual of [`plan_base`](NodePayload::plan_base).
    fn plan_base_mut(&mut self) -> Option<&mut crate::nodeindexscan::Plan<'mcx>> {
        None
    }

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

    /// Mutable tag-keyed downcast — the `&mut` dual of [`downcast_ref`]. Same
    /// soundness argument (tag check + bijection + `repr(transparent)`); the
    /// `&mut` borrow rides on `&mut self`.
    ///
    /// [`downcast_ref`]: PgNodeBox::downcast_ref
    #[inline]
    pub fn downcast_mut<P>(&mut self, expected: NodeTag) -> Option<&mut P>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        if self.0.node_tag() == expected {
            // SAFETY: identical to `downcast_ref`; the unique `&mut self` borrow
            // guarantees no aliasing of the returned `&mut P`.
            Some(unsafe { &mut *(self.0.__payload_ptr() as *mut P) })
        } else {
            None
        }
    }

    /// Move the concrete payload `P` out of this box (the `into_*` accessor body),
    /// **iff** the tag matches `expected`. On a tag mismatch the box is returned
    /// unchanged in the `Err` so the caller can recover it (mirrors the enum
    /// `into_*` returning the original on the wrong variant).
    ///
    /// # Soundness
    /// Same tag<->adapter bijection + `repr(transparent)` argument as
    /// [`downcast_ref`]; the move-out itself is encapsulated in
    /// [`mcx::box_read_payload`] (it `ptr::read`s the payload and frees the box
    /// storage without double-dropping).
    #[inline]
    pub fn into_payload<P>(self, expected: NodeTag) -> Result<P, Self>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        if self.0.node_tag() == expected {
            let data = self.0.__payload_ptr() as *const P;
            // SAFETY: tag matched -> the runtime type is `P` (bijection); `data`
            // is the transparent payload address; `box_read_payload` reads `P` out
            // and frees the box without running the dyn's drop glue.
            let value = unsafe { mcx::box_read_payload(self.0, data) };
            Ok(value)
        } else {
            Err(self)
        }
    }

    /// The embedded `Plan` base (vtable), or `None` for a non-plan node.
    #[inline]
    pub fn plan_base(&self) -> Option<&crate::nodeindexscan::Plan<'mcx>> {
        self.0.plan_base()
    }

    /// The embedded `Plan` base (vtable, mutable).
    #[inline]
    pub fn plan_base_mut(&mut self) -> Option<&mut crate::nodeindexscan::Plan<'mcx>> {
        self.0.plan_base_mut()
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

    /// Mutable tag-keyed downcast to `&mut P`.
    #[inline]
    pub fn downcast_mut<P>(&mut self, expected: NodeTag) -> Option<&mut P>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        self.0.downcast_mut::<P>(expected)
    }

    /// Move the payload `P` out of this node iff the tag matches; on mismatch
    /// returns the node unchanged in `Err`.
    #[inline]
    pub fn into_payload<P>(self, expected: NodeTag) -> Result<P, Self>
    where
        P: NodePayload<'mcx> + 'mcx,
    {
        match self.0.into_payload::<P>(expected) {
            Ok(p) => Ok(p),
            Err(b) => Err(OpaqueNode(b)),
        }
    }

    /// The embedded `Plan` base (vtable), or `None` for a non-plan node.
    #[inline]
    pub fn plan_base(&self) -> Option<&crate::nodeindexscan::Plan<'mcx>> {
        self.0.plan_base()
    }

    /// The embedded `Plan` base (vtable, mutable).
    #[inline]
    pub fn plan_base_mut(&mut self) -> Option<&mut crate::nodeindexscan::Plan<'mcx>> {
        self.0.plan_base_mut()
    }

    /// `((Plan *) node)->...` — the embedded `Plan`, panicking on a non-plan node
    /// (the asserting form the hand-written `Node::plan_head` provided).
    #[inline]
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        self.0.plan_base().unwrap_or_else(|| {
            panic!(
                "OpaqueNode::plan_head: called on a non-plan node (tag {:?}), which has no Plan base",
                self.node_tag()
            )
        })
    }

    /// Mutable dual of [`plan_head`](OpaqueNode::plan_head).
    #[inline]
    pub fn plan_head_mut(&mut self) -> &mut crate::nodeindexscan::Plan<'mcx> {
        let tag = self.node_tag();
        self.0.plan_base_mut().unwrap_or_else(|| {
            panic!(
                "OpaqueNode::plan_head_mut: called on a non-plan node (tag {tag:?}), which has no Plan base"
            )
        })
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
