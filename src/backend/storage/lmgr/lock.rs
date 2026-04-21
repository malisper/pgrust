use std::collections::HashMap;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::backend::utils::activity::now_timestamptz;
use crate::backend::utils::misc::interrupts::{
    InterruptReason, InterruptState, check_for_interrupts,
};
use crate::include::nodes::datetime::TimestampTzADT;
use crate::{ClientId, RelFileLocator};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableLockMode {
    AccessShare,
    RowExclusive,
    ShareUpdateExclusive,
    AccessExclusive,
}

impl TableLockMode {
    pub(crate) fn strongest(self, other: TableLockMode) -> TableLockMode {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }

    pub(crate) fn pg_mode_name(self) -> &'static str {
        match self {
            TableLockMode::AccessShare => "AccessShareLock",
            TableLockMode::RowExclusive => "RowExclusiveLock",
            TableLockMode::ShareUpdateExclusive => "ShareUpdateExclusiveLock",
            TableLockMode::AccessExclusive => "AccessExclusiveLock",
        }
    }

    fn rank(self) -> u8 {
        match self {
            TableLockMode::AccessShare => 0,
            TableLockMode::ShareUpdateExclusive => 1,
            TableLockMode::RowExclusive => 2,
            TableLockMode::AccessExclusive => 3,
        }
    }

    fn conflicts_with(self, other: TableLockMode) -> bool {
        matches!(
            (self, other),
            (TableLockMode::AccessExclusive, _)
                | (_, TableLockMode::AccessExclusive)
                | (
                    TableLockMode::ShareUpdateExclusive,
                    TableLockMode::ShareUpdateExclusive
                )
                | (
                    TableLockMode::ShareUpdateExclusive,
                    TableLockMode::RowExclusive
                )
                | (
                    TableLockMode::RowExclusive,
                    TableLockMode::ShareUpdateExclusive
                )
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableLockSnapshotRow {
    pub rel: RelFileLocator,
    pub client_id: ClientId,
    pub mode: TableLockMode,
    pub granted: bool,
    pub waitstart: Option<TimestampTzADT>,
}

struct TableLockEntry {
    mode: TableLockMode,
    holder: ClientId,
}

struct TableLockWaiter {
    holder: ClientId,
    mode: TableLockMode,
    waitstart: TimestampTzADT,
}

#[derive(Default)]
struct TableLockState {
    locks: HashMap<RelFileLocator, Vec<TableLockEntry>>,
    waiters: HashMap<RelFileLocator, Vec<TableLockWaiter>>,
}

pub struct TableLockManager {
    state: Mutex<TableLockState>,
    cv: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockError {
    Interrupted(InterruptReason),
}

impl TableLockManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TableLockState::default()),
            cv: Condvar::new(),
        }
    }

    pub fn lock_table(&self, rel: RelFileLocator, mode: TableLockMode, client_id: ClientId) {
        let mut state = self.state.lock();
        loop {
            let entries = state.locks.entry(rel).or_default();
            let has_conflict = entries.iter().any(|e| {
                e.holder != client_id && e.mode.conflicts_with(mode_for_holder(e, client_id, mode))
            });
            if !has_conflict {
                grant_table_lock(entries, client_id, mode);
                return;
            }
            self.cv.wait(&mut state);
        }
    }

    pub fn lock_table_interruptible(
        &self,
        rel: RelFileLocator,
        mode: TableLockMode,
        client_id: ClientId,
        interrupts: &InterruptState,
    ) -> Result<(), TableLockError> {
        let mut state = self.state.lock();
        let mut waiting = false;
        loop {
            let has_conflict = state.locks.entry(rel).or_default().iter().any(|e| {
                e.holder != client_id && e.mode.conflicts_with(mode_for_holder(e, client_id, mode))
            });
            if !has_conflict {
                if waiting {
                    remove_table_waiter(&mut state.waiters, rel, client_id, mode);
                }
                let entries = state.locks.entry(rel).or_default();
                grant_table_lock(entries, client_id, mode);
                return Ok(());
            }
            if !waiting {
                state.waiters.entry(rel).or_default().push(TableLockWaiter {
                    holder: client_id,
                    mode,
                    waitstart: now_timestamptz(),
                });
                waiting = true;
            }
            if let Err(reason) = check_for_interrupts(interrupts) {
                remove_table_waiter(&mut state.waiters, rel, client_id, mode);
                self.cv.notify_all();
                return Err(TableLockError::Interrupted(reason));
            }
            self.cv.wait_for(&mut state, Duration::from_millis(10));
        }
    }

    pub fn unlock_table(&self, rel: RelFileLocator, client_id: ClientId) {
        let mut state = self.state.lock();
        if let Some(entries) = state.locks.get_mut(&rel) {
            if let Some(idx) = entries.iter().rposition(|e| e.holder == client_id) {
                entries.remove(idx);
            }
            if entries.is_empty() {
                state.locks.remove(&rel);
            }
        }
        self.cv.notify_all();
    }

    pub fn unlock_all_for_client(&self, client_id: ClientId) {
        let mut state = self.state.lock();
        let mut released_any = false;
        state.locks.retain(|_, entries| {
            let before = entries.len();
            entries.retain(|entry| entry.holder != client_id);
            released_any |= entries.len() != before;
            !entries.is_empty()
        });
        state.waiters.retain(|_, entries| {
            let before = entries.len();
            entries.retain(|entry| entry.holder != client_id);
            released_any |= entries.len() != before;
            !entries.is_empty()
        });
        if released_any {
            self.cv.notify_all();
        }
    }

    pub fn snapshot(&self) -> Vec<TableLockSnapshotRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (rel, entries) in &state.locks {
            for entry in entries {
                rows.push(TableLockSnapshotRow {
                    rel: *rel,
                    client_id: entry.holder,
                    mode: entry.mode,
                    granted: true,
                    waitstart: None,
                });
            }
        }
        for (rel, entries) in &state.waiters {
            for entry in entries {
                rows.push(TableLockSnapshotRow {
                    rel: *rel,
                    client_id: entry.holder,
                    mode: entry.mode,
                    granted: false,
                    waitstart: Some(entry.waitstart),
                });
            }
        }
        rows.sort_by_key(|row| {
            (
                row.rel,
                row.client_id,
                row.mode,
                !row.granted,
                row.waitstart,
            )
        });
        rows
    }

    #[cfg(test)]
    pub(crate) fn has_locks_for_client(&self, client_id: ClientId) -> bool {
        self.state
            .lock()
            .locks
            .values()
            .any(|entries| entries.iter().any(|entry| entry.holder == client_id))
    }
}

pub(crate) fn unlock_relations(
    table_locks: &TableLockManager,
    client_id: ClientId,
    rels: &[RelFileLocator],
) {
    for rel in rels {
        table_locks.unlock_table(*rel, client_id);
    }
}

pub(crate) fn lock_relations_interruptible(
    table_locks: &TableLockManager,
    client_id: ClientId,
    rels: &[RelFileLocator],
    interrupts: &InterruptState,
) -> Result<(), TableLockError> {
    lock_tables_interruptible(
        table_locks,
        client_id,
        rels,
        TableLockMode::AccessShare,
        interrupts,
    )
}

pub(crate) fn lock_tables_interruptible(
    table_locks: &TableLockManager,
    client_id: ClientId,
    rels: &[RelFileLocator],
    mode: TableLockMode,
    interrupts: &InterruptState,
) -> Result<(), TableLockError> {
    let mut locked = Vec::new();
    for rel in rels {
        if let Err(err) = table_locks.lock_table_interruptible(*rel, mode, client_id, interrupts) {
            unlock_relations(table_locks, client_id, &locked);
            return Err(err);
        }
        locked.push(*rel);
    }
    Ok(())
}

pub(crate) fn lock_table_requests_interruptible(
    table_locks: &TableLockManager,
    client_id: ClientId,
    requests: &[(RelFileLocator, TableLockMode)],
    interrupts: &InterruptState,
) -> Result<(), TableLockError> {
    let mut locked = Vec::new();
    for (rel, mode) in requests {
        if let Err(err) = table_locks.lock_table_interruptible(*rel, *mode, client_id, interrupts) {
            unlock_relations(table_locks, client_id, &locked);
            return Err(err);
        }
        locked.push(*rel);
    }
    Ok(())
}

fn grant_table_lock(entries: &mut Vec<TableLockEntry>, client_id: ClientId, mode: TableLockMode) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.holder == client_id) {
        entry.mode = entry.mode.strongest(mode);
    } else {
        entries.push(TableLockEntry {
            mode,
            holder: client_id,
        });
    }
}

fn remove_table_waiter(
    waiters: &mut HashMap<RelFileLocator, Vec<TableLockWaiter>>,
    rel: RelFileLocator,
    client_id: ClientId,
    mode: TableLockMode,
) {
    let remove_key = if let Some(entries) = waiters.get_mut(&rel) {
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.holder == client_id && entry.mode == mode)
        {
            entries.remove(index);
        }
        entries.is_empty()
    } else {
        false
    };
    if remove_key {
        waiters.remove(&rel);
    }
}

fn mode_for_holder(
    entry: &TableLockEntry,
    client_id: ClientId,
    requested: TableLockMode,
) -> TableLockMode {
    if entry.holder == client_id {
        entry.mode.strongest(requested)
    } else {
        requested
    }
}
