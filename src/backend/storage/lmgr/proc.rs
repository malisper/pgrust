use parking_lot::Mutex;
use parking_lot::{Condvar, RwLock};

use crate::backend::access::transam::xact::{TransactionId, TransactionManager, TransactionStatus};
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};

pub enum WaitOutcome {
    Completed,
    DeadlockTimeout,
    Interrupted(InterruptReason),
}

/// Allows threads to wait until a specific transaction commits or aborts.
///
/// Lives outside `RwLock<TransactionManager>` so waiters don't hold the
/// read lock while sleeping.
pub struct TransactionWaiter {
    mu: Mutex<()>,
    cv: Condvar,
}

impl TransactionWaiter {
    pub fn new() -> Self {
        Self {
            mu: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub fn wait_for(
        &self,
        txns: &RwLock<TransactionManager>,
        xid: TransactionId,
        interrupts: &InterruptState,
    ) -> WaitOutcome {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Err(reason) = check_for_interrupts(interrupts) {
                return WaitOutcome::Interrupted(reason);
            }
            {
                let txns_guard = txns.read();
                match txns_guard.status(xid) {
                    Some(TransactionStatus::InProgress) => {}
                    _ => return WaitOutcome::Completed,
                }
            }
            if std::time::Instant::now() >= deadline {
                return WaitOutcome::DeadlockTimeout;
            }
            let mut guard = self.mu.lock();
            self.cv
                .wait_for(&mut guard, std::time::Duration::from_millis(10));
        }
    }

    pub fn notify(&self) {
        self.cv.notify_all();
    }
}
