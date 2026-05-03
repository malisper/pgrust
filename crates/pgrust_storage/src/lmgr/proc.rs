use parking_lot::Mutex;
use parking_lot::{Condvar, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::ClientId;
use crate::backend::utils::activity::now_timestamptz;
use crate::backend::utils::time::instant::Instant;
use pgrust_core::{InterruptReason, InterruptState, TransactionId, check_for_interrupts};
use pgrust_nodes::datetime::TimestampTzADT;

pub enum WaitOutcome {
    Completed,
    DeadlockTimeout,
    Interrupted(InterruptReason),
}

pub trait TransactionStatusLookup {
    fn transaction_in_progress(&self, xid: TransactionId) -> bool;
}

/// Allows threads to wait until a specific transaction commits or aborts.
///
/// Lives outside transaction manager locks so waiters don't hold the
/// read lock while sleeping.
pub struct TransactionWaiter {
    state: Mutex<TransactionWaitState>,
    cv: Condvar,
    next_waiter_id: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionLockSnapshotRow {
    pub xid: TransactionId,
    pub client_id: ClientId,
    pub granted: bool,
    pub waitstart: Option<TimestampTzADT>,
}

#[derive(Debug, Clone, Copy)]
struct TransactionWaitEntry {
    id: u64,
    client_id: ClientId,
    waitstart: TimestampTzADT,
}

#[derive(Debug, Default)]
struct TransactionWaitState {
    holders: HashMap<TransactionId, ClientId>,
    waiters: HashMap<TransactionId, Vec<TransactionWaitEntry>>,
}

impl TransactionWaiter {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TransactionWaitState::default()),
            cv: Condvar::new(),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    pub fn register_holder(&self, xid: TransactionId, client_id: ClientId) {
        self.state.lock().holders.insert(xid, client_id);
        self.cv.notify_all();
    }

    pub fn unregister_holder(&self, xid: TransactionId) {
        let mut state = self.state.lock();
        state.holders.remove(&xid);
        state.waiters.remove(&xid);
        self.cv.notify_all();
    }

    pub fn snapshot(&self) -> Vec<TransactionLockSnapshotRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (xid, client_id) in &state.holders {
            rows.push(TransactionLockSnapshotRow {
                xid: *xid,
                client_id: *client_id,
                granted: true,
                waitstart: None,
            });
        }
        for (xid, waiters) in &state.waiters {
            for waiter in waiters {
                rows.push(TransactionLockSnapshotRow {
                    xid: *xid,
                    client_id: waiter.client_id,
                    granted: false,
                    waitstart: Some(waiter.waitstart),
                });
            }
        }
        rows.sort_by_key(|row| (row.xid, row.client_id, !row.granted, row.waitstart));
        rows
    }

    pub fn blocking_pids(&self, blocked_pid: ClientId) -> Vec<ClientId> {
        let state = self.state.lock();
        state
            .waiters
            .iter()
            .filter(|(_, waiters)| waiters.iter().any(|waiter| waiter.client_id == blocked_pid))
            .filter_map(|(xid, _)| state.holders.get(xid).copied())
            .filter(|holder| *holder != blocked_pid)
            .collect()
    }

    pub fn wait_for(
        &self,
        txns: &RwLock<impl TransactionStatusLookup>,
        xid: TransactionId,
        client_id: ClientId,
        interrupts: &InterruptState,
    ) -> WaitOutcome {
        let deadline = Instant::now() + Duration::from_secs(2);
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let mut waiting = false;
        loop {
            if let Err(reason) = check_for_interrupts(interrupts) {
                if waiting {
                    self.remove_waiter(xid, waiter_id);
                }
                return WaitOutcome::Interrupted(reason);
            }
            {
                let txns_guard = txns.read();
                if !txns_guard.transaction_in_progress(xid) {
                    if waiting {
                        self.remove_waiter(xid, waiter_id);
                    }
                    return WaitOutcome::Completed;
                }
            }
            if Instant::now() >= deadline {
                if waiting {
                    self.remove_waiter(xid, waiter_id);
                }
                return WaitOutcome::DeadlockTimeout;
            }
            let mut state = self.state.lock();
            if !waiting {
                state
                    .waiters
                    .entry(xid)
                    .or_default()
                    .push(TransactionWaitEntry {
                        id: waiter_id,
                        client_id,
                        waitstart: now_timestamptz(),
                    });
                waiting = true;
            }
            self.cv.wait_for(&mut state, Duration::from_millis(10));
        }
    }

    pub fn notify(&self) {
        self.cv.notify_all();
    }

    fn remove_waiter(&self, xid: TransactionId, waiter_id: u64) {
        let mut state = self.state.lock();
        if let Some(waiters) = state.waiters.get_mut(&xid) {
            waiters.retain(|waiter| waiter.id != waiter_id);
            if waiters.is_empty() {
                state.waiters.remove(&xid);
            }
        }
        self.cv.notify_all();
    }
}
