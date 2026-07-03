use core::ptr::NonNull;

use ::types_error::PgResult;

use crate::{Mcx, MemoryContext};

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

/// # Safety
/// `p`: live exposed-provenance `MemoryContext` outliving the return (exposed rebuild, not a borrow-stack sibling).
unsafe fn ctx_from_exposed<'a>(p: *const MemoryContext) -> &'a MemoryContext {
    let exposed = p.expose_provenance();
    &*core::ptr::with_exposed_provenance::<MemoryContext>(exposed)
}

/// Heap context + its state, movable as one value; `for<'mcx>` closure access only; state drops first.
#[doc = "```compile_fail"]
#[doc = "mcx::bind!(VTy => V<'mcx>);"]
#[doc = "struct V<'mcx> { v: mcx::PgVec<'mcx, u8> }"]
#[doc = "let owned = mcx::McxOwned::<VTy>::try_new("]
#[doc = "    mcx::MemoryContext::new(\"c\"),"]
#[doc = "    |m| Ok(V { v: mcx::PgVec::new_in(m) }),"]
#[doc = ").unwrap();"]
#[doc = "let stolen = owned.with(|s| &s.v);"]
#[doc = "drop(owned);"]
#[doc = "assert_eq!(stolen.len(), 0);"]
#[doc = "```"]
pub struct McxOwned<B: Bind> {
    // State lives IN the context arena (C's palloc'd-EState shape): the handle
    // is two pointers, so moving it never memcpys the state (select1-gate
    // attribution: inline state cost ~1.8KB copies per move). Raw exposed
    // owner (a Box/& field would sibling-retag the state's self-borrow).
    state: NonNull<B::Out<'static>>,
    ctx: NonNull<MemoryContext>,
}

impl<B: Bind> McxOwned<B> {
    pub fn try_new(
        ctx: MemoryContext,
        build: impl for<'mcx> FnOnce(Mcx<'mcx>) -> PgResult<B::Out<'mcx>>,
    ) -> PgResult<Self> {
        let raw: *mut MemoryContext = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(ctx));
        // SAFETY: live heap context; the 'static is re-shortened by every access path.
        let ctx_ref: &'static MemoryContext = unsafe { ctx_from_exposed(raw) };
        let mcx = ctx_ref.mcx();
        // Slot allocated before build runs so codegen can construct in place.
        let slot: Result<crate::PgBox<'static, core::mem::MaybeUninit<B::Out<'static>>>, _> =
            crate::PgBox::try_new_uninit_in(mcx);
        let built = match slot {
            Ok(mut slot) => match build(mcx) {
                Ok(state) => {
                    slot.write(state);
                    let (p, _mcx) = allocator_api2::boxed::Box::into_raw_with_allocator(slot);
                    Ok(p.cast::<B::Out<'static>>())
                }
                Err(e) => Err(e),
            },
            Err(_) => Err(mcx.oom(core::mem::size_of::<B::Out<'static>>()).into()),
        };
        match built {
            // SAFETY: from Box::into_raw, hence non-null.
            Ok(p) => Ok(McxOwned {
                state: unsafe { NonNull::new_unchecked(p) },
                // SAFETY: from Box::into_raw, hence non-null.
                ctx: unsafe { NonNull::new_unchecked(raw) },
            }),
            Err(e) => {
                // SAFETY: sole owner from Box::into_raw; build borrow dead — unique free.
                drop(unsafe { alloc::boxed::Box::from_raw(raw) });
                Err(e)
            }
        }
    }

    /// In-place variant: `build` writes the state through the slot, so large
    /// states (EStateData ~1.2KB) are constructed in the arena, never returned
    /// by value through a `PgResult` (that shape cost a stack zero+copy per
    /// query — select1-gate attribution).
    pub fn try_new_in_place(
        ctx: MemoryContext,
        build: impl for<'mcx> FnOnce(
            Mcx<'mcx>,
            &mut core::mem::MaybeUninit<B::Out<'mcx>>,
        ) -> PgResult<()>,
    ) -> PgResult<Self> {
        Self::try_new_in_place_boxed(alloc::boxed::Box::new(ctx), build)
    }

    /// Recycled-context variant (C's context_freelists shape): the caller owns
    /// a reset heap context (from [`Self::free_recycle`]) and hands it back.
    pub fn try_new_in_place_boxed(
        ctx: alloc::boxed::Box<MemoryContext>,
        build: impl for<'mcx> FnOnce(
            Mcx<'mcx>,
            &mut core::mem::MaybeUninit<B::Out<'mcx>>,
        ) -> PgResult<()>,
    ) -> PgResult<Self> {
        let raw: *mut MemoryContext = alloc::boxed::Box::into_raw(ctx);
        // SAFETY: live heap context; the 'static is re-shortened by every access path.
        let ctx_ref: &'static MemoryContext = unsafe { ctx_from_exposed(raw) };
        let mcx = ctx_ref.mcx();
        let slot: Result<crate::PgBox<'static, core::mem::MaybeUninit<B::Out<'static>>>, _> =
            crate::PgBox::try_new_uninit_in(mcx);
        let built = match slot {
            Ok(mut slot) => match build(mcx, &mut slot) {
                Ok(()) => {
                    let (p, _mcx) = allocator_api2::boxed::Box::into_raw_with_allocator(slot);
                    Ok(p.cast::<B::Out<'static>>())
                }
                Err(e) => Err(e),
            },
            Err(_) => Err(mcx.oom(core::mem::size_of::<B::Out<'static>>()).into()),
        };
        match built {
            // SAFETY: from Box::into_raw, hence non-null.
            Ok(p) => Ok(McxOwned {
                state: unsafe { NonNull::new_unchecked(p) },
                // SAFETY: from Box::into_raw, hence non-null.
                ctx: unsafe { NonNull::new_unchecked(raw) },
            }),
            Err(e) => {
                // SAFETY: sole owner from Box::into_raw; build borrow dead — unique free.
                drop(unsafe { alloc::boxed::Box::from_raw(raw) });
                Err(e)
            }
        }
    }

/// Universal over `'mcx`: no external lifetime unifies, nothing smuggles out or in.
    pub fn with<R>(&self, f: impl for<'mcx> FnOnce(&B::Out<'mcx>) -> R) -> R {
        // SAFETY: state live until Drop; shared reborrow shortened to &self.
        f(unsafe { self.state.as_ref() })
    }

    pub fn with_mut<R>(&mut self, f: impl for<'mcx> FnOnce(&mut B::Out<'mcx>) -> R) -> R {
        // SAFETY: state live until Drop; unique reborrow shortened to &mut self.
        f(unsafe { self.state.as_mut() })
    }

    pub fn with_mut_mcx<R>(
        &mut self,
        f: impl for<'mcx> FnOnce(Mcx<'mcx>, &mut B::Out<'mcx>) -> PgResult<R>,
    ) -> PgResult<R> {
        // SAFETY: live heap context (freed only in Drop); exposed rebuild, no sibling.
        let ctx: &MemoryContext = unsafe { ctx_from_exposed(self.ctx.as_ptr()) };
        // SAFETY: state live until Drop; unique reborrow shortened to &mut self.
        f(ctx.mcx(), unsafe { self.state.as_mut() })
    }

    pub fn context(&self) -> &MemoryContext {
        // SAFETY: live heap context; borrow reshortened to `&self`.
        unsafe { ctx_from_exposed(self.ctx.as_ptr()) }
    }
}

impl<B: Bind> McxOwned<B> {
    /// C's FreeExecutorState shape: one context delete, the state's drop glue
    /// never runs. `ForgetSafe` bounds the loss to arena bytes; census-exempt
    /// fields must have been released by the caller (Drop remains the abort
    /// path).
    pub fn free_forget(self)
    where
        B::Out<'static>: crate::ForgetSafe,
    {
        let ctx = self.ctx;
        core::mem::forget(self);
        // SAFETY: unique heap context from try_new; the arena state dies with it.
        drop(unsafe { alloc::boxed::Box::from_raw(ctx.as_ptr()) });
    }

    /// free_forget, but the reset heap context survives for reuse via
    /// [`Self::try_new_in_place_boxed`] (C's context_freelists: the create +
    /// destroy pair was ~4x C's freelist hit per query).
    pub fn free_recycle(self) -> alloc::boxed::Box<MemoryContext>
    where
        B::Out<'static>: crate::ForgetSafe,
    {
        let ctx = self.ctx;
        core::mem::forget(self);
        // SAFETY: unique heap context from try_new; the arena state dies with the reset.
        let mut ctx = unsafe { alloc::boxed::Box::from_raw(ctx.as_ptr()) };
        ctx.reset();
        ctx
    }
}

impl<B: Bind> Drop for McxOwned<B> {
    fn drop(&mut self) {
        // SAFETY: state dropped in place exactly once, first (its arena memory
        // is reclaimed by the context free); then the unique context free.
        unsafe {
            core::ptr::drop_in_place(self.state.as_ptr());
            drop(alloc::boxed::Box::from_raw(self.ctx.as_ptr()));
        }
    }
}

impl<B: Bind> core::fmt::Debug for McxOwned<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("McxOwned").field("ctx", self.context()).finish_non_exhaustive()
    }
}
