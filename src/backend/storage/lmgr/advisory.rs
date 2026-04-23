use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::ClientId;
use crate::backend::utils::activity::now_timestamptz;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::include::nodes::datetime::TimestampTzADT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdvisoryLockKey {
    BigInt(i64),
    TwoInt(i32, i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdvisoryLockMode {
    Shared,
    Exclusive,
}

impl AdvisoryLockMode {
    fn conflicts_with(self, other: AdvisoryLockMode) -> bool {
        matches!(
            (self, other),
            (AdvisoryLockMode::Exclusive, _) | (_, AdvisoryLockMode::Exclusive)
        )
    }

    pub(crate) fn pg_mode_name(self) -> &'static str {
        match self {
            AdvisoryLockMode::Shared => "ShareLock",
            AdvisoryLockMode::Exclusive => "ExclusiveLock",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdvisoryLockScope {
    Session,
    Transaction(u64),
    Statement(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdvisoryLockOwner {
    pub client_id: ClientId,
    pub scope: AdvisoryLockScope,
}

impl AdvisoryLockOwner {
    pub fn session(client_id: ClientId) -> Self {
        Self {
            client_id,
            scope: AdvisoryLockScope::Session,
        }
    }

    pub fn transaction(client_id: ClientId, scope_id: u64) -> Self {
        Self {
            client_id,
            scope: AdvisoryLockScope::Transaction(scope_id),
        }
    }

    pub fn statement(client_id: ClientId, scope_id: u64) -> Self {
        Self {
            client_id,
            scope: AdvisoryLockScope::Statement(scope_id),
        }
    }

    pub(crate) fn virtualtransaction(self) -> String {
        match self.scope {
            AdvisoryLockScope::Session => format!("{}/session", self.client_id),
            AdvisoryLockScope::Transaction(scope_id) => {
                format!("{}/xact:{scope_id}", self.client_id)
            }
            AdvisoryLockScope::Statement(scope_id) => {
                format!("{}/stmt:{scope_id}", self.client_id)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvisoryLockSnapshotRow {
    pub key: AdvisoryLockKey,
    pub owner: AdvisoryLockOwner,
    pub mode: AdvisoryLockMode,
    pub granted: bool,
    pub waitstart: Option<TimestampTzADT>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisoryLockError {
    Interrupted(InterruptReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantedAdvisoryLock {
    owner: AdvisoryLockOwner,
    mode: AdvisoryLockMode,
    count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitingAdvisoryLock {
    id: u64,
    owner: AdvisoryLockOwner,
    mode: AdvisoryLockMode,
    waitstart: TimestampTzADT,
}

#[derive(Debug, Default)]
struct AdvisoryKeyState {
    granted: Vec<GrantedAdvisoryLock>,
    waiting: Vec<WaitingAdvisoryLock>,
}

#[derive(Debug, Default)]
struct AdvisoryLockState {
    keys: HashMap<AdvisoryLockKey, AdvisoryKeyState>,
}

pub struct AdvisoryLockManager {
    state: Mutex<AdvisoryLockState>,
    cv: Condvar,
    next_waiter_id: AtomicU64,
}

impl AdvisoryLockManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(AdvisoryLockState::default()),
            cv: Condvar::new(),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    pub fn try_lock(
        &self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> bool {
        let mut state = self.state.lock();
        let key_state = state.keys.entry(key).or_default();
        if key_state.granted.iter().any(|entry| {
            conflicts_between_owners(entry.owner, owner) && entry.mode.conflicts_with(mode)
        }) {
            return false;
        }
        grant_lock(key_state, owner, mode);
        true
    }

    pub fn lock_interruptible(
        &self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
        interrupts: &InterruptState,
    ) -> Result<(), AdvisoryLockError> {
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock();
        let mut waiting = false;
        loop {
            let key_state = state.keys.entry(key).or_default();
            let has_conflict = key_state.granted.iter().any(|entry| {
                conflicts_between_owners(entry.owner, owner) && entry.mode.conflicts_with(mode)
            });
            if !has_conflict {
                if waiting {
                    remove_waiter(key_state, waiter_id);
                }
                grant_lock(key_state, owner, mode);
                return Ok(());
            }
            if !waiting {
                key_state.waiting.push(WaitingAdvisoryLock {
                    id: waiter_id,
                    owner,
                    mode,
                    waitstart: now_timestamptz(),
                });
                waiting = true;
            }
            if let Err(reason) = check_for_interrupts(interrupts) {
                let key_state = state.keys.entry(key).or_default();
                remove_waiter(key_state, waiter_id);
                if key_state.granted.is_empty() && key_state.waiting.is_empty() {
                    state.keys.remove(&key);
                }
                self.cv.notify_all();
                return Err(AdvisoryLockError::Interrupted(reason));
            }
            self.cv.wait_for(&mut state, Duration::from_millis(10));
        }
    }

    pub fn unlock(
        &self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> bool {
        let mut state = self.state.lock();
        let mut released = false;
        if let Some(key_state) = state.keys.get_mut(&key) {
            if let Some(entry) = key_state
                .granted
                .iter_mut()
                .find(|entry| entry.owner == owner && entry.mode == mode)
            {
                entry.count = entry.count.saturating_sub(1);
                released = true;
            }
            key_state.granted.retain(|entry| entry.count > 0);
            if key_state.granted.is_empty() && key_state.waiting.is_empty() {
                state.keys.remove(&key);
            }
        }
        if released {
            self.cv.notify_all();
        }
        released
    }

    pub fn unlock_all_session(&self, client_id: ClientId) {
        self.unlock_matching(|owner| owner == AdvisoryLockOwner::session(client_id));
    }

    pub fn unlock_all_transaction(&self, client_id: ClientId, scope_id: u64) {
        self.unlock_matching(|owner| owner == AdvisoryLockOwner::transaction(client_id, scope_id));
    }

    pub fn unlock_all_statement(&self, client_id: ClientId, scope_id: u64) {
        self.unlock_matching(|owner| owner == AdvisoryLockOwner::statement(client_id, scope_id));
    }

    pub fn snapshot(&self) -> Vec<AdvisoryLockSnapshotRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (key, key_state) in &state.keys {
            for entry in &key_state.granted {
                rows.push(AdvisoryLockSnapshotRow {
                    key: *key,
                    owner: entry.owner,
                    mode: entry.mode,
                    granted: true,
                    waitstart: None,
                });
            }
            for entry in &key_state.waiting {
                rows.push(AdvisoryLockSnapshotRow {
                    key: *key,
                    owner: entry.owner,
                    mode: entry.mode,
                    granted: false,
                    waitstart: Some(entry.waitstart),
                });
            }
        }
        rows.sort_by_key(|row| {
            (
                row.key,
                row.owner.client_id,
                row.owner.scope,
                row.mode,
                !row.granted,
                row.waitstart,
            )
        });
        rows
    }

    fn unlock_matching(&self, predicate: impl Fn(AdvisoryLockOwner) -> bool) {
        let mut state = self.state.lock();
        let mut changed = false;
        state.keys.retain(|_, key_state| {
            let granted_before = key_state.granted.len();
            let waiting_before = key_state.waiting.len();
            key_state.granted.retain(|entry| !predicate(entry.owner));
            key_state.waiting.retain(|entry| !predicate(entry.owner));
            changed |= granted_before != key_state.granted.len()
                || waiting_before != key_state.waiting.len();
            !key_state.granted.is_empty() || !key_state.waiting.is_empty()
        });
        if changed {
            self.cv.notify_all();
        }
    }
}

fn grant_lock(key_state: &mut AdvisoryKeyState, owner: AdvisoryLockOwner, mode: AdvisoryLockMode) {
    if let Some(entry) = key_state
        .granted
        .iter_mut()
        .find(|entry| entry.owner == owner && entry.mode == mode)
    {
        entry.count += 1;
        return;
    }
    key_state.granted.push(GrantedAdvisoryLock {
        owner,
        mode,
        count: 1,
    });
}

fn remove_waiter(key_state: &mut AdvisoryKeyState, waiter_id: u64) {
    if let Some(index) = key_state
        .waiting
        .iter()
        .position(|entry| entry.id == waiter_id)
    {
        key_state.waiting.remove(index);
    }
}

fn conflicts_between_owners(left: AdvisoryLockOwner, right: AdvisoryLockOwner) -> bool {
    left.client_id != right.client_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_lock_allows_reentrant_owner() {
        let locks = AdvisoryLockManager::new();
        let owner = AdvisoryLockOwner::session(1);
        assert!(locks.try_lock(AdvisoryLockKey::BigInt(7), AdvisoryLockMode::Shared, owner,));
        assert!(locks.try_lock(
            AdvisoryLockKey::BigInt(7),
            AdvisoryLockMode::Exclusive,
            owner,
        ));
        let snapshot = locks.snapshot();
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn unlock_all_session_clears_held_rows() {
        let locks = AdvisoryLockManager::new();
        assert!(locks.try_lock(
            AdvisoryLockKey::BigInt(7),
            AdvisoryLockMode::Exclusive,
            AdvisoryLockOwner::session(1),
        ));
        locks.unlock_all_session(1);
        assert!(locks.snapshot().is_empty());
    }

    #[test]
    fn same_backend_different_scopes_do_not_conflict() {
        let locks = AdvisoryLockManager::new();
        assert!(locks.try_lock(
            AdvisoryLockKey::BigInt(7),
            AdvisoryLockMode::Exclusive,
            AdvisoryLockOwner::transaction(1, 42),
        ));
        assert!(locks.try_lock(
            AdvisoryLockKey::BigInt(7),
            AdvisoryLockMode::Exclusive,
            AdvisoryLockOwner::session(1),
        ));
    }
}
