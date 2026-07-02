use core::mem::ManuallyDrop;
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
    state: ManuallyDrop<B::Out<'static>>,
    // Raw exposed owner (a Box/& field would sibling-retag the state's self-borrow).
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
        match build(ctx_ref.mcx()) {
            Ok(state) => Ok(McxOwned {
                state: ManuallyDrop::new(state),
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
        f(&self.state)
    }

    pub fn with_mut<R>(&mut self, f: impl for<'mcx> FnOnce(&mut B::Out<'mcx>) -> R) -> R {
        f(&mut self.state)
    }

    pub fn with_mut_mcx<R>(
        &mut self,
        f: impl for<'mcx> FnOnce(Mcx<'mcx>, &mut B::Out<'mcx>) -> PgResult<R>,
    ) -> PgResult<R> {
        // SAFETY: live heap context (freed only in Drop); exposed rebuild, no sibling.
        let ctx: &MemoryContext = unsafe { ctx_from_exposed(self.ctx.as_ptr()) };
        f(ctx.mcx(), &mut self.state)
    }

    pub fn context(&self) -> &MemoryContext {
        // SAFETY: live heap context; borrow reshortened to `&self`.
        unsafe { ctx_from_exposed(self.ctx.as_ptr()) }
    }
}

impl<B: Bind> Drop for McxOwned<B> {
    fn drop(&mut self) {
        // SAFETY: state dropped exactly once, first; then the unique context free.
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
