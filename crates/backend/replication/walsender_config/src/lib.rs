use std::sync::atomic::{AtomicI32, Ordering};

use guc_tables::{vars, GucVarAccessors};

static MAX_WAL_SENDERS: AtomicI32 = AtomicI32::new(10);
static MAX_REPLICATION_SLOTS: AtomicI32 = AtomicI32::new(10);

pub fn max_wal_senders() -> i32 {
    MAX_WAL_SENDERS.load(Ordering::Relaxed)
}

pub fn max_replication_slots() -> i32 {
    MAX_REPLICATION_SLOTS.load(Ordering::Relaxed)
}

pub fn init_seams() {
    vars::max_wal_senders.install(GucVarAccessors {
        get: max_wal_senders,
        set: |v| MAX_WAL_SENDERS.store(v, Ordering::Relaxed),
    });
    vars::max_replication_slots.install(GucVarAccessors {
        get: max_replication_slots,
        set: |v| MAX_REPLICATION_SLOTS.store(v, Ordering::Relaxed),
    });
}
