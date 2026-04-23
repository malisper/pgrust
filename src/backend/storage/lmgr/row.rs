use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::backend::utils::activity::now_timestamptz;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::backend::utils::time::instant::Instant;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datetime::TimestampTzADT;
use crate::include::nodes::parsenodes::SelectLockingClause;
use crate::ClientId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowLockScope {
    Session,
    Transaction(u64),
    Statement(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowLockOwner {
    pub client_id: ClientId,
    pub scope: RowLockScope,
}

impl RowLockOwner {
    pub fn session(client_id: ClientId) -> Self {
        Self {
            client_id,
            scope: RowLockScope::Session,
        }
    }

    pub fn transaction(client_id: ClientId, scope_id: u64) -> Self {
        Self {
            client_id,
            scope: RowLockScope::Transaction(scope_id),
        }
    }

    pub fn statement(client_id: ClientId, scope_id: u64) -> Self {
        Self {
            client_id,
            scope: RowLockScope::Statement(scope_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowLockMode {
    KeyShare,
    Share,
    NoKeyExclusive,
    Exclusive,
}

impl RowLockMode {
    pub fn from_select_locking_clause(strength: SelectLockingClause) -> Self {
        match strength {
            SelectLockingClause::ForKeyShare => Self::KeyShare,
            SelectLockingClause::ForShare => Self::Share,
            SelectLockingClause::ForNoKeyUpdate => Self::NoKeyExclusive,
            SelectLockingClause::ForUpdate => Self::Exclusive,
        }
    }

    pub fn pg_name(self) -> &'static str {
        match self {
            Self::KeyShare => "For Key Share",
            Self::Share => "For Share",
            Self::NoKeyExclusive => "For No Key Update",
            Self::Exclusive => "For Update",
        }
    }

    fn conflicts_with(self, other: RowLockMode) -> bool {
        matches!(
            (self, other),
            (RowLockMode::Exclusive, _)
                | (_, RowLockMode::Exclusive)
                | (RowLockMode::Share, RowLockMode::NoKeyExclusive)
                | (RowLockMode::NoKeyExclusive, RowLockMode::Share)
                | (RowLockMode::NoKeyExclusive, RowLockMode::NoKeyExclusive)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowLockTag {
    pub relation_oid: u32,
    pub tid: ItemPointerData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockError {
    DeadlockTimeout,
    Interrupted(InterruptReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantedRowLock {
    owner: RowLockOwner,
    mode: RowLockMode,
    count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitingRowLock {
    id: u64,
    owner: RowLockOwner,
    mode: RowLockMode,
    waitstart: TimestampTzADT,
}

#[derive(Debug, Default)]
struct RowLockStateForTag {
    granted: Vec<GrantedRowLock>,
    waiting: Vec<WaitingRowLock>,
}

#[derive(Debug, Default)]
struct RowLockState {
    tags: HashMap<RowLockTag, RowLockStateForTag>,
}

pub struct RowLockManager {
    state: Mutex<RowLockState>,
    cv: Condvar,
    next_waiter_id: AtomicU64,
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RowLockState::default()),
            cv: Condvar::new(),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    pub fn lock_interruptible(
        &self,
        tag: RowLockTag,
        mode: RowLockMode,
        owner: RowLockOwner,
        interrupts: &InterruptState,
    ) -> Result<(), RowLockError> {
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut state = self.state.lock();
        let mut waiting = false;
        loop {
            let tag_state = state.tags.entry(tag).or_default();
            let has_conflict = tag_state.granted.iter().any(|entry| {
                entry.owner.client_id != owner.client_id && entry.mode.conflicts_with(mode)
            });
            if !has_conflict {
                if waiting {
                    remove_waiter(tag_state, waiter_id);
                }
                grant_row_lock(tag_state, owner, mode);
                return Ok(());
            }
            if !waiting {
                tag_state.waiting.push(WaitingRowLock {
                    id: waiter_id,
                    owner,
                    mode,
                    waitstart: now_timestamptz(),
                });
                waiting = true;
            }
            if let Err(reason) = check_for_interrupts(interrupts) {
                let tag_state = state.tags.entry(tag).or_default();
                remove_waiter(tag_state, waiter_id);
                if tag_state.granted.is_empty() && tag_state.waiting.is_empty() {
                    state.tags.remove(&tag);
                }
                self.cv.notify_all();
                return Err(RowLockError::Interrupted(reason));
            }
            if Instant::now() >= deadline {
                let tag_state = state.tags.entry(tag).or_default();
                remove_waiter(tag_state, waiter_id);
                if tag_state.granted.is_empty() && tag_state.waiting.is_empty() {
                    state.tags.remove(&tag);
                }
                self.cv.notify_all();
                return Err(RowLockError::DeadlockTimeout);
            }
            self.cv.wait_for(&mut state, Duration::from_millis(10));
        }
    }

    pub fn unlock_all_session(&self, client_id: ClientId) {
        self.unlock_matching(|owner| owner == RowLockOwner::session(client_id));
    }

    pub fn unlock_all_transaction(&self, client_id: ClientId, scope_id: u64) {
        self.unlock_matching(|owner| owner == RowLockOwner::transaction(client_id, scope_id));
    }

    pub fn unlock_all_statement(&self, client_id: ClientId, scope_id: u64) {
        self.unlock_matching(|owner| owner == RowLockOwner::statement(client_id, scope_id));
    }

    fn unlock_matching(&self, predicate: impl Fn(RowLockOwner) -> bool) {
        let mut state = self.state.lock();
        let mut changed = false;
        state.tags.retain(|_, tag_state| {
            let granted_before = tag_state.granted.len();
            let waiting_before = tag_state.waiting.len();
            tag_state.granted.retain(|entry| !predicate(entry.owner));
            tag_state.waiting.retain(|entry| !predicate(entry.owner));
            changed |= granted_before != tag_state.granted.len()
                || waiting_before != tag_state.waiting.len();
            !tag_state.granted.is_empty() || !tag_state.waiting.is_empty()
        });
        if changed {
            self.cv.notify_all();
        }
    }
}

fn grant_row_lock(tag_state: &mut RowLockStateForTag, owner: RowLockOwner, mode: RowLockMode) {
    if let Some(entry) = tag_state
        .granted
        .iter_mut()
        .find(|entry| entry.owner == owner && entry.mode == mode)
    {
        entry.count += 1;
        return;
    }
    tag_state.granted.push(GrantedRowLock {
        owner,
        mode,
        count: 1,
    });
}

fn remove_waiter(tag_state: &mut RowLockStateForTag, waiter_id: u64) {
    if let Some(index) = tag_state.waiting.iter().position(|entry| entry.id == waiter_id) {
        tag_state.waiting.remove(index);
    }
}
