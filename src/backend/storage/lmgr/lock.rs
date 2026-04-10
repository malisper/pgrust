use std::collections::HashMap;

use parking_lot::{Condvar, Mutex};

use crate::{ClientId, RelFileLocator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockMode {
    AccessShare,
    RowExclusive,
    AccessExclusive,
}

impl TableLockMode {
    fn conflicts_with(self, other: TableLockMode) -> bool {
        matches!(
            (self, other),
            (TableLockMode::AccessExclusive, _) | (_, TableLockMode::AccessExclusive)
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

pub(crate) fn lock_relations(table_locks: &TableLockManager, client_id: ClientId, rels: &[RelFileLocator]) {
    for rel in rels {
        table_locks.lock_table(*rel, TableLockMode::AccessShare, client_id);
    }
}

pub(crate) fn unlock_relations(table_locks: &TableLockManager, client_id: ClientId, rels: &[RelFileLocator]) {
    for rel in rels {
        table_locks.unlock_table(*rel, client_id);
    }
}
