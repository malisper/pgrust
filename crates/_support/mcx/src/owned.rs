//! [`McxOwned`]: a context and the state allocated in it, as one movable
//! value. The replacement for C's `MemoryContextSetParent` lifetime
//! extension (plancache saving a finished plan): instead of reparenting
//! pointers, you *move the bundle*.
//!
//! Why this needs a special type: state borrowing its sibling context field
//! is a self-referential struct, which safe Rust rejects because moving the
//! struct would relocate the context out from under the borrow. The fix is
//! to heap-allocate the context (its address survives moves of the wrapper)
//! and erase the borrow's lifetime internally; the API keeps the erasure
//! sound by construction:
//!
//! - the state can only be **built** through a closure generic over the
//!   context lifetime (`for<'mcx>`), so it cannot borrow anything except the
//!   supplied context (and `'static` data);
//! - the state can only be **accessed** through borrows of the wrapper
//!   `for<'mcx>` closures ([`with`](McxOwned::with) /
//!   [`with_mut`](McxOwned::with_mut)) whose bodies must typecheck for an
//!   arbitrary lifetime and therefore cannot smuggle borrows out;
//! - drop order is state **then** context — load-bearing, because the
//!   state's destructors deallocate into the context.
//!
//! Aliasing discipline (Miri / Stacked + Tree Borrows): the self-reference
//! must NOT go through a tagged `&MemoryContext` derived from a local `Box`,
//! because moving that `Box` into the wrapper performs a `Unique` retag that
//! invalidates the borrow the state still holds (a sibling-retag violation,
//! the same class the `BumpDrop` arena hit). Instead the boxed context's
//! provenance is **exposed** at construction ([`core::ptr::expose_provenance`])
//! and the long-lived `&'static MemoryContext` the state borrows is rebuilt
//! from that exposed address ([`core::ptr::with_exposed_provenance`]) — an
//! exposed pointer is not a tracked sibling in the borrow stack, so no later
//! retag of the wrapper invalidates it. The wrapper stores the *raw* heap
//! address (not a `Box`), and `Drop` reconstitutes the `Box` from it to free
//! the context after the state.

use core::mem::ManuallyDrop;
use core::ptr::NonNull;

use ::types_error::PgResult;

use crate::{Mcx, MemoryContext};

/// Type constructor for the state: lets `McxOwned` name "your type at any
/// lifetime". One marker impl per state type:
///
/// ```ignore
/// struct SavedPlanTy;
/// impl Bind for SavedPlanTy {
///     type Out<'mcx> = SavedPlan<'mcx>;
/// }
/// type CachedPlan = McxOwned<SavedPlanTy>;
/// ```
pub trait Bind {
    type Out<'mcx>;
}

/// Declare a [`Bind`] marker: `bind!(pub SavedPlanTy => SavedPlan<'mcx>)`.
#[macro_export]
macro_rules! bind {
    ($vis:vis $marker:ident => $state:ident<$lt:lifetime>) => {
        $vis struct $marker;
        impl $crate::Bind for $marker {
            type Out<$lt> = $state<$lt>;
        }
    };
}

/// Rebuild a shared `&MemoryContext` from a stable heap pointer **through
/// exposed provenance**, so the resulting reference is not a borrow-stack
/// sibling of `p` (or of whatever field holds `p`). Used for every access path
/// into the owned context, keeping the state's long-lived self-borrow valid
/// across retags of the wrapper.
///
/// # Safety
/// `p` must point to a live, initialized `MemoryContext` whose provenance was
/// exposed (here, via [`core::ptr::expose_provenance`] in [`McxOwned::try_new`]),
/// and the returned reference must not outlive that liveness (callers bound it
/// to a borrow of `self`, or — at construction — to a frame that ends before the
/// context is freed).
unsafe fn ctx_from_exposed<'a>(p: *const MemoryContext) -> &'a MemoryContext {
    let exposed = p.expose_provenance();
    &*core::ptr::with_exposed_provenance::<MemoryContext>(exposed)
}

/// A heap-allocated [`MemoryContext`] together with state `B::Out<'_>` allocated
/// in it — movable and storable as one value (`'static`-friendly if the state
/// borrows nothing else).
///
/// Borrows of the state cannot outlive the wrapper:
///
/// ```compile_fail
/// mcx::bind!(VTy => V<'mcx>);
/// struct V<'mcx> { v: mcx::PgVec<'mcx, u8> }
///
/// let owned = mcx::McxOwned::<VTy>::try_new(
///     mcx::MemoryContext::new("c"),
///     |m| Ok(V { v: mcx::PgVec::new_in(m) }),
/// ).unwrap();
/// let stolen = owned.with(|s| &s.v);
/// drop(owned);
/// assert_eq!(stolen.len(), 0);
/// ```
pub struct McxOwned<B: Bind> {
    /// Field order is NOT the drop mechanism (both are dropped explicitly); the
    /// `Drop` impl drops state before freeing the context.
    state: ManuallyDrop<B::Out<'static>>,
    /// The owning context's stable heap address, held as a raw pointer with
    /// **exposed provenance** (recorded via [`core::ptr::expose_provenance`] in
    /// [`try_new`](McxOwned::try_new)). It is NOT a `Box`/`&` field on purpose:
    /// a tagged owner would make the `&'static MemoryContext` the state borrows
    /// a sibling in the borrow stack, which a retag of this field would
    /// invalidate. The address is the sole owner of the heap allocation; `Drop`
    /// reconstitutes the `Box` from it (after the state) to free it.
    ctx: NonNull<MemoryContext>,
}

impl<B: Bind> McxOwned<B> {
    /// Build state inside `ctx` and bundle the two. On build failure the
    /// context is dropped normally and the error passes through.
    ///
    /// The closure is universal over `'mcx`, so the state it returns can
    /// only borrow from the supplied handle.
    pub fn try_new(
        ctx: MemoryContext,
        build: impl for<'mcx> FnOnce(Mcx<'mcx>) -> PgResult<B::Out<'mcx>>,
    ) -> PgResult<Self> {
        // Move the context onto the heap so its address is stable across moves
        // of `self`, then take ownership as a raw pointer and **expose** its
        // provenance. The state's long-lived borrow is rebuilt from that
        // exposed address rather than from a box owner, so no later retag of
        // `self.ctx` invalidates it.
        let raw: *mut MemoryContext = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(ctx));
        // SAFETY: `raw` is the live, initialized context just heap-allocated.
        // `ctx_from_exposed` exposes its provenance and rebuilds an unsibling'd
        // `&'static MemoryContext`. The `'static` is bounded in practice by the
        // wrapper: every public access path re-shortens it, and the allocation
        // is freed only in `Drop`, after the state.
        let ctx_ref: &'static MemoryContext = unsafe { ctx_from_exposed(raw) };
        match build(ctx_ref.mcx()) {
            Ok(state) => Ok(McxOwned {
                state: ManuallyDrop::new(state),
                // SAFETY: `raw` came from `Box::into_raw`, hence non-null.
                ctx: unsafe { NonNull::new_unchecked(raw) },
            }),
            Err(e) => {
                // Build failed; the state was never created, so reclaim the
                // context immediately by reconstituting the `Box`.
                // SAFETY: `raw` is the live, sole-owner pointer just produced by
                // `Box::into_raw`; the borrow above is dead (`build` consumed the
                // `Mcx` and returned), so this is the unique free.
                drop(unsafe { alloc::boxed::Box::from_raw(raw) });
                Err(e)
            }
        }
    }

    /// Shared access through a lifetime-universal closure.
    ///
    /// Universal (not a concrete `&'a Out<'a>` return) for soundness, not
    /// style: a concrete `'a` unifies with the caller's scope, so if the
    /// state carries interior mutability over its lifetime parameter (e.g. a
    /// `Cell<Option<Mcx<'mcx>>>` field), safe code could store a *different*,
    /// shorter-lived context's handle into this state and read it back after
    /// that context died. With `for<'mcx>`, no external lifetime can unify
    /// with the state's, so nothing can be written into it from outside.
    pub fn with<R>(&self, f: impl for<'mcx> FnOnce(&B::Out<'mcx>) -> R) -> R {
        f(&self.state)
    }

    /// Mutable access through a lifetime-universal closure: the body must
    /// typecheck for an arbitrary `'mcx`, which is what prevents both
    /// smuggling borrows out and cross-wrapper `mem::swap` of states (two
    /// wrappers' states never unify to one type).
    pub fn with_mut<R>(&mut self, f: impl for<'mcx> FnOnce(&mut B::Out<'mcx>) -> R) -> R {
        f(&mut self.state)
    }

    /// The owning context — for accounting (`used`/`stats`), naming, and
    /// linking it as an accounting child at creation time.
    pub fn context(&self) -> &MemoryContext {
        // SAFETY: `self.ctx` points to the live heap context (freed only in
        // `Drop`); the returned borrow is reshortened to `&self`. Rebuilding
        // through the exposed address keeps this access from invalidating the
        // state's own long-lived borrow.
        unsafe { ctx_from_exposed(self.ctx.as_ptr()) }
    }
}

impl<B: Bind> Drop for McxOwned<B> {
    fn drop(&mut self) {
        // SAFETY: `state` is dropped exactly once, first — its destructors
        // deallocate into the still-live context. We then reconstitute the
        // `Box` from the heap address and drop it (the unique free of the
        // context), exactly once, after the state.
        unsafe {
            ManuallyDrop::drop(&mut self.state);
            drop(alloc::boxed::Box::from_raw(self.ctx.as_ptr()));
        }
    }
}

impl<B: Bind> core::fmt::Debug for McxOwned<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("McxOwned").field("ctx", self.context()).finish_non_exhaustive()
    }
}
