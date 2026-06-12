//! [`McxOwned`]: a context and the state allocated in it, as one movable
//! value. The replacement for C's `MemoryContextSetParent` lifetime
//! extension (plancache saving a finished plan): instead of reparenting
//! pointers, you *move the bundle*.
//!
//! Why this needs a special type: state borrowing its sibling context field
//! is a self-referential struct, which safe Rust rejects because moving the
//! struct would relocate the context out from under the borrow. The fix is
//! to heap-pin the context (`Box` — its address survives moves of the
//! wrapper) and erase the borrow's lifetime internally; the API keeps the
//! erasure sound by construction:
//!
//! - the state can only be **built** through a closure generic over the
//!   context lifetime (`for<'mcx>`), so it cannot borrow anything except the
//!   supplied context (and `'static` data);
//! - the state can only be **accessed** through borrows of the wrapper
//!   ([`get`](McxOwned::get), shared) or through a `for<'mcx>` closure
//!   ([`with_mut`](McxOwned::with_mut)) whose body must typecheck for an
//!   arbitrary lifetime and therefore cannot smuggle borrows out;
//! - drop order is state **then** context — load-bearing, because the
//!   state's destructors deallocate into the context.

use core::mem::ManuallyDrop;

use types_error::PgResult;

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

/// A heap-pinned [`MemoryContext`] together with state `B::Out<'_>` allocated
/// in it — movable and storable as one value (`'static`-friendly if the state
/// borrows nothing else).
///
/// Borrows of the state cannot outlive the wrapper:
///
/// ```compile_fail,E0505
/// mcx::bind!(VTy => V<'mcx>);
/// struct V<'mcx> { v: mcx::PgVec<'mcx, u8> }
///
/// let owned = mcx::McxOwned::<VTy>::try_new(
///     mcx::MemoryContext::new("c"),
///     |m| Ok(V { v: mcx::PgVec::new_in(m) }),
/// ).unwrap();
/// let stolen = &owned.get().v;
/// drop(owned); // ERROR: cannot move out of `owned` while borrowed
/// assert_eq!(stolen.len(), 0);
/// ```
pub struct McxOwned<B: Bind> {
    /// Field order is NOT the drop mechanism (both are `ManuallyDrop`); the
    /// explicit `Drop` impl drops state before context.
    state: ManuallyDrop<B::Out<'static>>,
    ctx: ManuallyDrop<alloc::boxed::Box<MemoryContext>>,
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
        let ctx = alloc::boxed::Box::new(ctx);
        // SAFETY: extending the borrow of the boxed context to 'static.
        // Sound because the box's heap address is stable across moves of
        // `self`, the context is dropped only after the state (explicit Drop
        // impl), and `Out<'static>` never escapes this module at 'static —
        // every access path re-shortens the lifetime.
        let mcx: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(ctx.mcx()) };
        let state = build(mcx)?;
        Ok(McxOwned { state: ManuallyDrop::new(state), ctx: ManuallyDrop::new(ctx) })
    }

    /// Shared access. The returned borrow's lifetime is the borrow of
    /// `self`, so neither the state nor anything inside it (including its
    /// `Mcx` handle) can outlive the wrapper.
    pub fn get<'a>(&'a self) -> &'a B::Out<'a> {
        let ptr: *const B::Out<'static> = &*self.state;
        // SAFETY: shortening 'static back to 'a ≤ the wrapper's life.
        unsafe { &*ptr.cast::<B::Out<'a>>() }
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
        &self.ctx
    }
}

impl<B: Bind> Drop for McxOwned<B> {
    fn drop(&mut self) {
        // SAFETY: each ManuallyDrop is dropped exactly once, state first —
        // its destructors deallocate into the still-live context.
        unsafe {
            ManuallyDrop::drop(&mut self.state);
            ManuallyDrop::drop(&mut self.ctx);
        }
    }
}

impl<B: Bind> core::fmt::Debug for McxOwned<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("McxOwned").field("ctx", &**self.ctx).finish_non_exhaustive()
    }
}
