use std::sync::atomic::{AtomicI32, Ordering};

use guc_tables::consts::{IOMETHOD_SYNC, IOMETHOD_WORKER};
use guc_tables::{option_sets, vars, GucHookExtra, GucVarAccessors};
use types_guc::config_enum_entry;

// io_uring is #ifdef'd out on this target in C too.
pub const IO_METHOD_OPTIONS: &[config_enum_entry] = &[
    config_enum_entry { name: "sync", val: IOMETHOD_SYNC, hidden: false },
    config_enum_entry { name: "worker", val: IOMETHOD_WORKER, hidden: false },
];

static IO_METHOD: AtomicI32 = AtomicI32::new(IOMETHOD_WORKER);
static IO_WORKERS: AtomicI32 = AtomicI32::new(3);

pub fn io_method() -> i32 {
    IO_METHOD.load(Ordering::Relaxed)
}

pub fn io_workers() -> i32 {
    IO_WORKERS.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IoMethodOps {
    Sync,
}

pub fn pgaio_method_ops() -> IoMethodOps {
    match io_method() {
        IOMETHOD_SYNC => IoMethodOps::Sync,
        m => panic!("pgaio_method_ops: io_method {m} unported (backend-storage-aio-core)"),
    }
}

fn assign_io_method(newval: i32, _extra: Option<&GucHookExtra>) {
    IO_METHOD.store(newval, Ordering::Relaxed);
}

pub fn init_seams() {
    option_sets::io_method_options.install(IO_METHOD_OPTIONS);
    guc_tables::hooks::assign_io_method.install(assign_io_method);
    vars::io_method.install(GucVarAccessors {
        get: io_method,
        set: |v| IO_METHOD.store(v, Ordering::Relaxed),
    });
    vars::io_workers.install(GucVarAccessors {
        get: io_workers,
        set: |v| IO_WORKERS.store(v, Ordering::Relaxed),
    });
}
