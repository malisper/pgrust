//! The C `error_context_stack` (`elog.h` `ErrorContextCallback` chain) as a
//! thread-local chain plus RAII guard.
//!
//! Subsystems push a callback; `errfinish` walks the chain innermost-first and
//! each callback annotates the in-flight error (the owned equivalent of
//! calling `errcontext()` while the errordata entry is current). The guard's
//! `Drop` pops the frame, so an early return can never leak a stale frame.

use std::cell::{Cell, RefCell};

use types_error::PgError;

struct Frame {
    id: u64,
    callback: Box<dyn Fn(&mut PgError)>,
}

thread_local! {
    /// `ErrorContextCallback *error_context_stack` — innermost frame last.
    static CHAIN: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
    static NEXT_ID: Cell<u64> = const { Cell::new(1) };
    /// Reentrancy guard: a callback that itself builds an error must not
    /// recursively re-fire the chain.
    static FIRING: Cell<bool> = const { Cell::new(false) };
}

/// RAII registration of one error-context callback. Dropping the guard pops
/// the frame (the C `error_context_stack = errcallback.previous`).
#[must_use = "dropping the guard immediately unregisters the error-context callback"]
pub struct ErrorContextGuard {
    id: u64,
}

/// Push a callback onto the error-context chain.
pub fn error_context_push(callback: Box<dyn Fn(&mut PgError)>) -> ErrorContextGuard {
    let id = NEXT_ID.with(|n| {
        let v = n.get();
        n.set(v.wrapping_add(1));
        v
    });
    CHAIN.with(|c| c.borrow_mut().push(Frame { id, callback }));
    ErrorContextGuard { id }
}

impl Drop for ErrorContextGuard {
    fn drop(&mut self) {
        CHAIN.with(|c| {
            let mut chain = c.borrow_mut();
            if let Some(pos) = chain.iter().rposition(|f| f.id == self.id) {
                chain.remove(pos);
            }
        });
    }
}

/// The number of live frames (diagnostics / tests).
pub fn error_context_depth() -> usize {
    CHAIN.with(|c| c.borrow().len())
}

/// `error_context_stack = NULL` — drop every registered frame at once.
/// Outstanding [`ErrorContextGuard`]s remain safe: their `Drop` removes by id.
pub fn error_context_stack_clear() {
    CHAIN.with(|c| c.borrow_mut().clear());
}

/// Pop the innermost frame without a guard. No-op on an empty chain.
pub fn error_context_stack_pop_innermost() {
    CHAIN.with(|c| {
        c.borrow_mut().pop();
    });
}

/// `errcontext()` body for use inside a callback: append one context line to
/// the in-flight error, newline-joined.
pub fn append_error_context(error: &mut PgError, line: &str) {
    error.context = match error.context.take() {
        Some(mut existing) => {
            existing.push('\n');
            existing.push_str(line);
            Some(existing)
        }
        None => Some(String::from(line)),
    };
}

/// Fire the registered callbacks against `error`, innermost-first (the C
/// `for (econtext = error_context_stack; ...)` walk in `errfinish`). No-op
/// when the chain is empty or when already firing.
pub(crate) fn run_error_context_callbacks(error: &mut PgError) {
    if FIRING.with(Cell::get) {
        return;
    }
    struct ResetFiring;
    impl Drop for ResetFiring {
        fn drop(&mut self) {
            FIRING.with(|f| f.set(false));
        }
    }
    FIRING.with(|f| f.set(true));
    let _reset = ResetFiring;

    CHAIN.with(|c| {
        let chain = c.borrow();
        for frame in chain.iter().rev() {
            (frame.callback)(error);
        }
    });
}
