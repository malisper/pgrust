use std::collections::HashMap;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::backend::utils::misc::interrupts::{InterruptReason, InterruptState, check_for_interrupts};
use crate::{ClientId, RelFileLocator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockMode {
    AccessShare,
    RowExclusive,
    ShareUpdateExclusive,
    AccessExclusive,
}

impl TableLockMode {
    fn conflicts_with(self, other: TableLockMode) -> bool {
        matches!(
            (self, other),
            (TableLockMode::AccessExclusive, _)
                | (_, TableLockMode::AccessExclusive)
                | (TableLockMode::ShareUpdateExclusive, TableLockMode::ShareUpdateExclusive)
                | (TableLockMode::ShareUpdateExclusive, TableLockMode::RowExclusive)
                | (TableLockMode::RowExclusive, TableLockMode::ShareUpdateExclusive)
        )
    }
}

struct TableLockEntry {
    mode: TableLockMode,
    holder: ClientId,
}

pub struct TableLockManager {
    locks: Mutex<HashMap<RelFileLocator, Vec<TableLockEntry>>>,
    cv: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockError {
    Interrupted(InterruptReason),
}

impl TableLockManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            cv: Condvar::new(),
        }
    }

    pub fn lock_table(&self, rel: RelFileLocator, mode: TableLockMode, client_id: ClientId) {
        let mut locks = self.locks.lock();
        loop {
            let entries = locks.entry(rel).or_default();
            let dominated_by_self = entries
                .iter()
                .any(|e| e.holder == client_id && !e.mode.conflicts_with(mode));
            let has_conflict = entries
                .iter()
                .any(|e| e.holder != client_id && e.mode.conflicts_with(mode));
            if !has_conflict || dominated_by_self {
                entries.push(TableLockEntry {
                    mode,
                    holder: client_id,
                });
                return;
            }
            self.cv.wait(&mut locks);
        }
    }

    pub fn lock_table_interruptible(
        &self,
        rel: RelFileLocator,
        mode: TableLockMode,
        client_id: ClientId,
        interrupts: &InterruptState,
    ) -> Result<(), TableLockError> {
        let mut locks = self.locks.lock();
        loop {
            let entries = locks.entry(rel).or_default();
            let dominated_by_self = entries
                .iter()
                .any(|e| e.holder == client_id && !e.mode.conflicts_with(mode));
            let has_conflict = entries
                .iter()
                .any(|e| e.holder != client_id && e.mode.conflicts_with(mode));
            if !has_conflict || dominated_by_self {
                entries.push(TableLockEntry {
                    mode,
                    holder: client_id,
                });
                return Ok(());
            }
            if let Err(reason) = check_for_interrupts(interrupts) {
                return Err(TableLockError::Interrupted(reason));
            }
            self.cv.wait_for(&mut locks, Duration::from_millis(10));
        }
    }

    pub fn unlock_table(&self, rel: RelFileLocator, client_id: ClientId) {
        let mut locks = self.locks.lock();
        if let Some(entries) = locks.get_mut(&rel) {
            if let Some(idx) = entries.iter().rposition(|e| e.holder == client_id) {
                entries.remove(idx);
            }
            if entries.is_empty() {
                locks.remove(&rel);
            }
        }
        self.cv.notify_all();
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
