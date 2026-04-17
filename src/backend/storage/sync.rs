use std::collections::BTreeSet;

use parking_lot::Mutex;

use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, SmgrError, StorageManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SyncTag {
    pub rel: RelFileLocator,
    pub fork: ForkNumber,
}

#[derive(Default)]
pub struct SyncQueue {
    pending: Mutex<BTreeSet<SyncTag>>,
}

impl SyncQueue {
    pub fn register(&self, rel: RelFileLocator, fork: ForkNumber) {
        self.pending.lock().insert(SyncTag { rel, fork });
    }

    pub fn cancel_relation(&self, rel: RelFileLocator, fork: Option<ForkNumber>) {
        let mut pending = self.pending.lock();
        pending.retain(|tag| match fork {
            Some(fork) => !(tag.rel == rel && tag.fork == fork),
            None => tag.rel != rel,
        });
    }

    pub fn cancel_database(&self, db_oid: u32) {
        self.pending.lock().retain(|tag| tag.rel.db_oid != db_oid);
    }

    pub fn register_truncated_relation(&self, rel: RelFileLocator, fork: ForkNumber) {
        let mut pending = self.pending.lock();
        pending.retain(|tag| !(tag.rel == rel && tag.fork == fork));
        pending.insert(SyncTag { rel, fork });
    }

    pub fn drain(&self) -> Vec<SyncTag> {
        std::mem::take(&mut *self.pending.lock())
            .into_iter()
            .collect()
    }

    pub fn process_pending_syncs<S: StorageManager>(&self, smgr: &mut S) -> Result<u64, SmgrError> {
        let mut synced = 0;
        for tag in self.drain() {
            if !smgr.exists(tag.rel, tag.fork) {
                continue;
            }
            match smgr.immedsync(tag.rel, tag.fork) {
                Ok(()) => synced += 1,
                Err(err) if sync_target_is_missing(&err) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(synced)
    }
}

fn sync_target_is_missing(err: &SmgrError) -> bool {
    match err {
        SmgrError::RelationNotFound { .. } => true,
        SmgrError::Io(err) => err.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use crate::backend::storage::smgr::MdStorageManager;

    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!("pgrust_sync_queue_{label}_{nanos}"))
    }

    fn test_rel(rel_number: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number,
        }
    }

    #[test]
    fn queue_deduplicates_relation_forks() {
        let queue = SyncQueue::default();
        let rel = test_rel(100);

        queue.register(rel, ForkNumber::Main);
        queue.register(rel, ForkNumber::Main);
        queue.register(rel, ForkNumber::VisibilityMap);

        assert_eq!(
            queue.drain(),
            vec![
                SyncTag {
                    rel,
                    fork: ForkNumber::Main
                },
                SyncTag {
                    rel,
                    fork: ForkNumber::VisibilityMap
                }
            ]
        );
    }

    #[test]
    fn cancel_relation_removes_pending_entries() {
        let queue = SyncQueue::default();
        let rel = test_rel(101);

        queue.register(rel, ForkNumber::Main);
        queue.register(rel, ForkNumber::VisibilityMap);
        queue.cancel_relation(rel, Some(ForkNumber::Main));

        assert_eq!(
            queue.drain(),
            vec![SyncTag {
                rel,
                fork: ForkNumber::VisibilityMap
            }]
        );
    }

    #[test]
    fn truncate_re_registers_relation_fork() {
        let queue = SyncQueue::default();
        let rel = test_rel(102);

        queue.register(rel, ForkNumber::Main);
        queue.register_truncated_relation(rel, ForkNumber::Main);

        assert_eq!(
            queue.drain(),
            vec![SyncTag {
                rel,
                fork: ForkNumber::Main
            }]
        );
    }

    #[test]
    fn processing_pending_syncs_ignores_missing_relations() {
        let base = temp_dir("missing_rel");
        let queue = Arc::new(SyncQueue::default());
        let mut smgr = MdStorageManager::new_with_sync_queue(&base, Arc::clone(&queue));
        let rel = test_rel(103);

        queue.register(rel, ForkNumber::Main);
        assert_eq!(queue.process_pending_syncs(&mut smgr).unwrap(), 0);
    }
}
