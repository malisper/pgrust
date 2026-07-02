use mcx::{Mcx, MemoryContext};
use std::cell::RefCell;
use types_error::PgResult;

thread_local! {
    static LSYS_SCRATCH: RefCell<MemoryContext> =
        RefCell::new(MemoryContext::new("lsyscache scratch"));
}

// Reset-per-acquisition scratch for transient catlist projections (C keeps
// them in the catcache); re-entrant use falls back to a fresh context.
pub(crate) fn with_scratch<R>(f: impl for<'s> FnOnce(Mcx<'s>) -> PgResult<R>) -> PgResult<R> {
    LSYS_SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut ctx) => {
            ctx.reset();
            f(ctx.mcx())
        }
        Err(_) => {
            let ctx = MemoryContext::new("lsyscache scratch (reentrant)");
            f(ctx.mcx())
        }
    })
}
