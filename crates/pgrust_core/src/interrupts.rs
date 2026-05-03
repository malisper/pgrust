use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InterruptReason {
    StatementTimeout = 1,
    QueryCancel = 2,
}

impl InterruptReason {
    pub fn message(self) -> &'static str {
        match self {
            Self::StatementTimeout => "canceling statement due to statement timeout",
            Self::QueryCancel => "canceling statement due to user request",
        }
    }

    pub fn sqlstate(self) -> &'static str {
        match self {
            Self::StatementTimeout => "57014",
            Self::QueryCancel => "57014",
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::StatementTimeout),
            2 => Some(Self::QueryCancel),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct InterruptState {
    pending: AtomicU8,
    deadline: Mutex<Option<Instant>>,
}

impl InterruptState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pending(&self, reason: InterruptReason) {
        self.pending.store(reason as u8, Ordering::SeqCst);
    }

    pub fn clear_pending(&self) {
        self.pending.store(0, Ordering::SeqCst);
    }

    pub fn reset_statement_state(&self) {
        self.clear_pending();
        *self.deadline.lock() = None;
    }

    pub fn pending_reason(&self) -> Option<InterruptReason> {
        InterruptReason::from_code(self.pending.load(Ordering::SeqCst))
    }

    pub fn statement_interrupt_guard(
        self: &Arc<Self>,
        timeout: Option<Duration>,
    ) -> StatementInterruptGuard {
        StatementInterruptGuard::new(Arc::clone(self), timeout)
    }
}

pub struct StatementInterruptGuard {
    state: Arc<InterruptState>,
    previous_deadline: Option<Instant>,
    previous_pending: u8,
}

impl StatementInterruptGuard {
    fn new(state: Arc<InterruptState>, timeout: Option<Duration>) -> Self {
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let previous_deadline = {
            let mut guard = state.deadline.lock();
            let previous = *guard;
            *guard = deadline;
            previous
        };
        let previous_pending = state.pending.swap(0, Ordering::SeqCst);
        Self {
            state,
            previous_deadline,
            previous_pending,
        }
    }
}

impl Drop for StatementInterruptGuard {
    fn drop(&mut self) {
        self.state
            .pending
            .store(self.previous_pending, Ordering::SeqCst);
        *self.state.deadline.lock() = self.previous_deadline;
    }
}

pub fn check_for_interrupts(state: &InterruptState) -> Result<(), InterruptReason> {
    if let Some(reason) = state.pending_reason() {
        return Err(reason);
    }

    let deadline = *state.deadline.lock();
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        state.set_pending(InterruptReason::StatementTimeout);
        return Err(InterruptReason::StatementTimeout);
    }

    Ok(())
}
