use core::alloc::Layout;
use core::marker::PhantomData;
use core::ptr::NonNull;

use mcx::Mcx;
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, List, ListFlavor, NodeList, OidList, XidList};
use crate::tags::NodeTag;

// C node layout: the tag is the first field of every node struct.
#[repr(C)]
pub(crate) struct NodeRep<T> {
    tag: NodeTag,
    pub(crate) payload: T,
}

// Thin generic shell over the non-generic `alloc_uninit_bytes`: only the const
// layout and the single init write are emitted per T; the backend alloc
// dispatch is shared. Callers assert `!needs_drop::<T>()`.
#[inline]
fn mk_rep<'mcx, T>(mcx: Mcx<'mcx>, tag: NodeTag, payload: T) -> PgResult<NonNull<NodeRep<T>>> {
    let rep = mcx
        .alloc_uninit_bytes(Layout::new::<NodeRep<T>>())
        .map_err(|_| mcx.oom(core::mem::size_of::<NodeRep<T>>()))?
        .cast::<NodeRep<T>>();
    // SAFETY: fresh uninit arena bytes sized/aligned for `NodeRep<T>`; one init write.
    unsafe { rep.as_ptr().write(NodeRep { tag, payload }) };
    Ok(rep)
}

/// Opaque tagged node handle: one arena pointer, inspected via `node_tag()` /
/// `as_<variant>()`, built via fallible `mk_<variant>(mcx, ...)`.
#[derive(Clone, Copy)]
pub struct Node<'mcx> {
    p: NonNull<()>,
    _arena: PhantomData<&'mcx ()>,
}

// 64-bit layout pin (NonNull); wasm32 (ILP32) shrinks it to 4.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<Node<'static>>() == 8);
const _: () = assert!(!core::mem::needs_drop::<Node<'static>>());
// SAFETY: Copy arena pointer — nothing to run, nothing to leak.
unsafe impl mcx::ForgetSafe for Node<'_> {}

impl Node<'_> {
    /// Arena identity (C pointer equality).
    #[inline]
    pub fn ptr_eq(self, other: Node<'_>) -> bool {
        self.p == other.p
    }

    #[inline]
    pub fn as_raw(self) -> NonNull<()> {
        self.p
    }
}

impl<'mcx> Node<'mcx> {
    /// # Safety: `p` must come from `as_raw` of a `Node<'mcx>` still live in
    /// its arena.
    #[inline]
    pub unsafe fn from_raw(p: NonNull<()>) -> Node<'mcx> {
        Node { p, _arena: PhantomData }
    }
}

/// # Safety: `TAG` must uniquely identify `Self` among all `NodeVariant` impls
/// (the `as_*` cast trusts the tag alone).
pub unsafe trait NodeVariant<'mcx>: Sized + 'mcx {
    const TAG: NodeTag;
}

#[derive(Clone, Copy)]
pub struct Integer {
    pub ival: i32,
}

#[derive(Clone, Copy)]
pub struct Float<'mcx> {
    pub fval: &'mcx str,
}

#[derive(Clone, Copy)]
pub struct Boolean {
    pub boolval: bool,
}

#[derive(Clone, Copy)]
pub struct String<'mcx> {
    pub sval: &'mcx str,
}

#[derive(Clone, Copy)]
pub struct BitString<'mcx> {
    pub bsval: &'mcx str,
}

// SAFETY (each): tag/type pairing mirrors value.h and pg_list.h/bitmapset.h.
unsafe impl NodeVariant<'_> for Integer {
    const TAG: NodeTag = NodeTag::T_Integer;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Float<'mcx> {
    const TAG: NodeTag = NodeTag::T_Float;
}
unsafe impl NodeVariant<'_> for Boolean {
    const TAG: NodeTag = NodeTag::T_Boolean;
}
unsafe impl<'mcx> NodeVariant<'mcx> for String<'mcx> {
    const TAG: NodeTag = NodeTag::T_String;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BitString<'mcx> {
    const TAG: NodeTag = NodeTag::T_BitString;
}
unsafe impl<'mcx, F: ListFlavor + 'mcx> NodeVariant<'mcx> for List<'mcx, F> {
    const TAG: NodeTag = F::TAG;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Bitmapset<'mcx> {
    const TAG: NodeTag = NodeTag::T_Bitmapset;
}

impl<'mcx> Node<'mcx> {
    pub fn mk<T: NodeVariant<'mcx>>(mcx: Mcx<'mcx>, payload: T) -> PgResult<Node<'mcx>> {
        const { assert!(!core::mem::needs_drop::<T>()) };
        // `p` retains write provenance for `with_mut` (came from a `&mut` slot).
        let rep = mk_rep(mcx, T::TAG, payload)?;
        Ok(Node { p: rep.cast(), _arena: PhantomData })
    }

    /// C `makeNode(T)` (palloc0 + tag): zeroed payload, mutable until sealed.
    pub fn build<T: NodeVariant<'mcx> + Default>(mcx: Mcx<'mcx>) -> PgResult<NodeMut<'mcx, T>> {
        Self::mk_mut(mcx, T::default())
    }

    pub fn mk_mut<T: NodeVariant<'mcx>>(mcx: Mcx<'mcx>, payload: T) -> PgResult<NodeMut<'mcx, T>> {
        const { assert!(!core::mem::needs_drop::<T>()) };
        let rep = mk_rep(mcx, T::TAG, payload)?;
        // SAFETY: fresh, uniquely-owned arena `NodeRep<T>` initialized below.
        Ok(NodeMut { rep: unsafe { &mut *rep.as_ptr() } })
    }

    #[inline]
    pub fn node_tag(self) -> NodeTag {
        // SAFETY: every node is a NodeRep<T> (repr(C)), tag at offset 0.
        unsafe { *self.p.cast::<NodeTag>().as_ref() }
    }

    #[inline]
    pub fn is<T: NodeVariant<'mcx>>(self) -> bool {
        self.node_tag() == T::TAG
    }

    #[inline]
    pub fn as_variant<T: NodeVariant<'mcx>>(self) -> Option<&'mcx T> {
        if self.node_tag() == T::TAG {
            // SAFETY: tag match proves the pointee is NodeRep<T> (NodeVariant
            // contract); the arena keeps it alive and immutable for 'mcx.
            Some(unsafe { &(*self.rep_ptr::<T>()).payload })
        } else {
            None
        }
    }

    // Callers must prove the pointee is (a layout prefix of) NodeRep<T>
    // before dereferencing. Retains the allocation's original write
    // provenance (`p` came from the `&mut` that mk/seal consumed).
    #[inline]
    pub(crate) fn rep_ptr<T>(self) -> *mut NodeRep<T> {
        self.p.cast::<NodeRep<T>>().as_ptr()
    }

    /// In-place fixup of an already-sealed node (C's setrefs.c mutates the
    /// just-built plan tree through shared pointers). The `&mut` is confined
    /// to the closure; returns `None` on a tag mismatch.
    ///
    /// # Safety
    /// The caller must hold exclusive access to this node for the duration of
    /// the call: no reference previously derived from it (`as_*`, `seal_ref`)
    /// may be used during or after the mutation.
    pub unsafe fn with_mut<T: NodeVariant<'mcx>, R>(
        self,
        f: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        if self.node_tag() != T::TAG {
            return None;
        }
        // SAFETY: tag match proves NodeRep<T>; exclusivity is the caller's
        // contract; `p` carries write provenance (see rep_ptr).
        Some(f(unsafe { &mut (*self.rep_ptr::<T>()).payload }))
    }

    pub fn mk_integer(mcx: Mcx<'mcx>, ival: i32) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, Integer { ival })
    }

    pub fn mk_float(mcx: Mcx<'mcx>, fval: &'mcx str) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, Float { fval })
    }

    pub fn mk_boolean(mcx: Mcx<'mcx>, boolval: bool) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, Boolean { boolval })
    }

    pub fn mk_string(mcx: Mcx<'mcx>, sval: &'mcx str) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, String { sval })
    }

    pub fn mk_bitstring(mcx: Mcx<'mcx>, bsval: &'mcx str) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, BitString { bsval })
    }

    pub fn mk_list(mcx: Mcx<'mcx>, list: NodeList<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, list)
    }

    pub fn mk_int_list(mcx: Mcx<'mcx>, list: IntList<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, list)
    }

    pub fn mk_oid_list(mcx: Mcx<'mcx>, list: OidList<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, list)
    }

    pub fn mk_xid_list(mcx: Mcx<'mcx>, list: XidList<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, list)
    }

    pub fn mk_bitmapset(mcx: Mcx<'mcx>, bms: Bitmapset<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, bms)
    }

    #[inline]
    pub fn as_integer(self) -> Option<&'mcx Integer> {
        self.as_variant()
    }

    #[inline]
    pub fn as_float(self) -> Option<&'mcx Float<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_boolean(self) -> Option<&'mcx Boolean> {
        self.as_variant()
    }

    #[inline]
    pub fn as_string(self) -> Option<&'mcx String<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitstring(self) -> Option<&'mcx BitString<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_list(self) -> Option<&'mcx NodeList<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_int_list(self) -> Option<&'mcx IntList<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_oid_list(self) -> Option<&'mcx OidList<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_xid_list(self) -> Option<&'mcx XidList<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitmapset(self) -> Option<&'mcx Bitmapset<'mcx>> {
        self.as_variant()
    }
}

/// Exclusive access to a node under construction (C fills fields after
/// `makeNode`). Aliased `Node` handles exist only after `seal`/`seal_ref`
/// consumes the `&mut`, so shared and mutable access never coexist.
pub struct NodeMut<'mcx, T> {
    rep: &'mcx mut NodeRep<T>,
}

impl<'mcx, T: NodeVariant<'mcx>> NodeMut<'mcx, T> {
    #[inline]
    pub fn seal(self) -> Node<'mcx> {
        let rep: &'mcx mut NodeRep<T> = self.rep;
        Node { p: NonNull::from(rep).cast(), _arena: PhantomData }
    }

    #[inline]
    pub fn seal_ref(self) -> &'mcx T {
        let rep: &'mcx mut NodeRep<T> = self.rep;
        &rep.payload
    }
}

impl<'mcx, T> core::ops::Deref for NodeMut<'mcx, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.rep.payload
    }
}

impl<'mcx, T> core::ops::DerefMut for NodeMut<'mcx, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.rep.payload
    }
}

impl core::fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Node({:?})", self.node_tag())
    }
}
