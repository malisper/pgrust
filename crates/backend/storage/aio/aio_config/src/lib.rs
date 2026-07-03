#![allow(non_snake_case)]

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

thread_local! {
    // C's pgaio_my_backend slot; no handle can be issued (pgaio_io_acquire unported).
    static MY_BACKEND_ATTACHED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn pgaio_init_backend() {
    debug_assert!(!MY_BACKEND_ATTACHED.get());

    if miscinit::GetMyBackendType() == types_core::BackendType::IoWorker {
        return;
    }

    if lmgr_proc::MyProc().is_none() {
        panic!("aio requires a normal PGPROC");
    }

    MY_BACKEND_ATTACHED.set(true);

    match pgaio_method_ops() {
        IoMethodOps::Sync => {}
    }

    ipc_seams::before_shmem_exit::call(pgaio_shutdown, datum::Datum::from_usize(0))
        .expect("pgaio_init_backend: before_shmem_exit");
}

fn pgaio_shutdown(code: i32, _arg: datum::Datum) -> types_error::PgResult<()> {
    debug_assert!(MY_BACKEND_ATTACHED.get());
    AtEOXact_Aio(code == 0);
    MY_BACKEND_ATTACHED.set(false);
    Ok(())
}

pub fn AtEOXact_Aio(_is_commit: bool) {}

pub fn pgaio_error_cleanup() {}

// C submits staged IOs and (per method) waits on fd users; none can be staged.
pub fn pgaio_closing_fd(_fd: i32) {
    debug_assert!(!MY_BACKEND_ATTACHED.get() || matches!(pgaio_method_ops(), IoMethodOps::Sync));
}

pub fn init_seams() {
    aio_seams::pgaio_init_backend::set(pgaio_init_backend);
    aio_seams::at_eoxact_aio::set(AtEOXact_Aio);
    aio_seams::pgaio_error_cleanup::set(pgaio_error_cleanup);
    aio_seams::pgaio_closing_fd::set(pgaio_closing_fd);
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
