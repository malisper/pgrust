use std::sync::atomic::{AtomicI32, Ordering};

use guc_tables::{vars, GucVarAccessors};

static MAX_PREPARED_XACTS: AtomicI32 = AtomicI32::new(0);

pub fn max_prepared_xacts() -> i32 {
    MAX_PREPARED_XACTS.load(Ordering::Relaxed)
}

pub fn init_seams() {
    vars::max_prepared_xacts.install(GucVarAccessors {
        get: max_prepared_xacts,
        set: |v| MAX_PREPARED_XACTS.store(v, Ordering::Relaxed),
    });
}
