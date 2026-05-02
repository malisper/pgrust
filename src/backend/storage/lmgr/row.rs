use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::ClientId;
use crate::backend::utils::activity::now_timestamptz;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::backend::utils::time::instant::Instant;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datetime::TimestampTzADT;
use crate::include::nodes::parsenodes::SelectLockingClause;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum RowLockScope {
    Session,
    Transaction(u64),
    Statement(u64),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum RowLockMode {
    SIRead,
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
            Self::SIRead => "SIReadLock",
            Self::KeyShare => "For Key Share",
            Self::Share => "For Share",
            Self::NoKeyExclusive => "For No Key Update",
            Self::Exclusive => "For Update",
        }
    }

    pub fn pg_lock_mode_name(self) -> &'static str {
        match self {
            Self::SIRead => "SIReadLock",
            Self::KeyShare => "AccessShareLock",
            Self::Share => "RowShareLock",
            Self::NoKeyExclusive => "ExclusiveLock",
            Self::Exclusive => "AccessExclusiveLock",
        }
    }

    fn conflicts_with(self, other: RowLockMode) -> bool {
        if matches!(self, RowLockMode::SIRead) || matches!(other, RowLockMode::SIRead) {
            return false;
        }
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct RowLockTag {
    pub relation_oid: u32,
    pub tid: ItemPointerData,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RowLockPreparedEntry {
    pub tag: RowLockTag,
    pub mode: RowLockMode,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockError {
    DeadlockTimeout,
    Interrupted(InterruptReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLockSnapshotRow {
    pub tag: RowLockTag,
    pub owner: RowLockOwner,
    pub mode: RowLockMode,
    pub granted: bool,
    pub waitstart: Option<TimestampTzADT>,
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

    pub fn try_lock(&self, tag: RowLockTag, mode: RowLockMode, owner: RowLockOwner) -> bool {
        let mut state = self.state.lock();
        let tag_state = state.tags.entry(tag).or_default();
        let has_conflict = tag_state.granted.iter().any(|entry| {
            entry.owner.client_id != owner.client_id && entry.mode.conflicts_with(mode)
        });
        if has_conflict {
            return false;
        }
        grant_row_lock(tag_state, owner, mode);
        true
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

    pub fn transfer_transaction(
        &self,
        old_client_id: ClientId,
        old_scope_id: u64,
        new_client_id: ClientId,
        new_scope_id: u64,
    ) {
        let old_owner = RowLockOwner::transaction(old_client_id, old_scope_id);
        let new_owner = RowLockOwner::transaction(new_client_id, new_scope_id);
        let mut state = self.state.lock();
        let mut changed = false;
        for tag_state in state.tags.values_mut() {
            for entry in &mut tag_state.granted {
                if entry.owner == old_owner {
                    entry.owner = new_owner;
                    changed = true;
                }
            }
            for entry in &mut tag_state.waiting {
                if entry.owner == old_owner {
                    entry.owner = new_owner;
                    changed = true;
                }
            }
        }
        if changed {
            self.cv.notify_all();
        }
    }

    pub fn granted_transaction_locks(
        &self,
        client_id: ClientId,
        scope_id: u64,
    ) -> Vec<RowLockPreparedEntry> {
        let owner = RowLockOwner::transaction(client_id, scope_id);
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (tag, tag_state) in &state.tags {
            for entry in &tag_state.granted {
                if entry.owner == owner {
                    rows.push(RowLockPreparedEntry {
                        tag: *tag,
                        mode: entry.mode,
                        count: entry.count,
                    });
                }
            }
        }
        rows
    }

    pub fn restore_transaction_locks(
        &self,
        client_id: ClientId,
        scope_id: u64,
        locks: &[RowLockPreparedEntry],
    ) {
        let owner = RowLockOwner::transaction(client_id, scope_id);
        let mut state = self.state.lock();
        for lock in locks {
            let tag_state = state.tags.entry(lock.tag).or_default();
            for _ in 0..lock.count.max(1) {
                grant_row_lock(tag_state, owner, lock.mode);
            }
        }
        self.cv.notify_all();
    }

    pub fn snapshot(&self) -> Vec<RowLockSnapshotRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (tag, tag_state) in &state.tags {
            for entry in &tag_state.granted {
                rows.push(RowLockSnapshotRow {
                    tag: *tag,
                    owner: entry.owner,
                    mode: entry.mode,
                    granted: true,
                    waitstart: None,
                });
            }
            for entry in &tag_state.waiting {
                rows.push(RowLockSnapshotRow {
                    tag: *tag,
                    owner: entry.owner,
                    mode: entry.mode,
                    granted: false,
                    waitstart: Some(entry.waitstart),
                });
            }
        }
        rows.sort_by_key(|row| {
            (
                row.tag,
                row.owner.client_id,
                row.owner.scope,
                row.mode,
                !row.granted,
                row.waitstart,
            )
        });
        rows
    }

    pub fn blocking_pids(&self, blocked_pid: ClientId) -> Vec<ClientId> {
        let state = self.state.lock();
        let mut blockers = Vec::new();
        for tag_state in state.tags.values() {
            for (wait_index, waiter) in tag_state
                .waiting
                .iter()
                .enumerate()
                .filter(|(_, waiter)| waiter.owner.client_id == blocked_pid)
            {
                blockers.extend(tag_state.granted.iter().filter_map(|entry| {
                    (entry.owner.client_id != blocked_pid && entry.mode.conflicts_with(waiter.mode))
                        .then_some(entry.owner.client_id)
                }));
                blockers.extend(
                    tag_state
                        .waiting
                        .iter()
                        .take(wait_index)
                        .filter_map(|entry| {
                            (entry.owner.client_id != blocked_pid
                                && entry.mode.conflicts_with(waiter.mode))
                            .then_some(entry.owner.client_id)
                        }),
                );
            }
        }
        blockers
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
    if let Some(index) = tag_state
        .waiting
        .iter()
        .position(|entry| entry.id == waiter_id)
    {
        tag_state.waiting.remove(index);
    }
}
