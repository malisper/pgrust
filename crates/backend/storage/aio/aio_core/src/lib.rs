//! storage/aio: PgAioHandle engine, shmem model, io_method dispatch.
//!
//! Thread-model divergence (the one structural note, see the GL-AIO-1
//! letter): C's IO worker PROCESSES are postmaster-child THREADS of kind
//! BackendType::IoWorker — pmchild-tracked, PGPROC-owning, covered by the
//! postmaster state machine (PM_WAIT_IO_WORKERS). Everything cross-process
//! in C (shmem handle table, wrefs, worker submission ring) is cross-THREAD
//! here with identical protocols; fd reopen parity is kept (workers reopen
//! the smgr target through their own per-thread vfd cache, never reusing
//! the issuer's raw fd).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicPtr, AtomicU64, AtomicU8, Ordering};

use guc_tables::consts::{IOMETHOD_SYNC, IOMETHOD_WORKER};
use guc_tables::{option_sets, vars, GucHookExtra, GucVarAccessors};
use types_error::PgResult;
use types_guc::config_enum_entry;
use types_storage::aio::{
    PgAioOpDataRw, PgAioResult, PgAioReturn, PgAioTargetData, PGAIO_HANDLE_MAX_CALLBACKS,
    PGAIO_SUBMIT_BATCH_SIZE,
};

mod callback;
mod handle;
mod init;
mod io;
mod method_worker;
mod target;
#[cfg(test)]
mod tests;

pub use callback::{
    pgaio_io_get_handle_data, pgaio_io_register_callbacks, pgaio_io_set_handle_data_32,
    pgaio_result_report,
};
pub use handle::{
    pgaio_closing_fd, pgaio_enter_batchmode, pgaio_error_cleanup, pgaio_exit_batchmode,
    pgaio_have_staged, pgaio_io_acquire, pgaio_io_acquire_nb, pgaio_io_get_id,
    pgaio_io_get_owner, pgaio_io_get_wref, pgaio_io_release, pgaio_io_set_flag,
    pgaio_submit_staged, pgaio_wref_check_done, pgaio_wref_clear, pgaio_wref_valid,
    pgaio_wref_wait, AtEOXact_Aio,
};
pub use init::{pgaio_init_backend, AioShmemInit, AioShmemResetAfterCrash, AioShmemSize};
pub use io::{pgaio_io_current, pgaio_io_set_iovec_pages, pgaio_io_start_readv_current};
pub use method_worker::{
    pgaio_worker_cycle, pgaio_worker_executed_count, pgaio_worker_register,
    pgaio_workers_enabled,
};
pub use target::{pgaio_io_get_target_data, pgaio_io_set_target_smgr};

pub const IO_METHOD_OPTIONS: &[config_enum_entry] = &[
    // io_uring stays unlisted until inc-2 (C compile-gates it the same way on
    config_enum_entry { name: "sync", val: IOMETHOD_SYNC, hidden: false },
    config_enum_entry { name: "worker", val: IOMETHOD_WORKER, hidden: false },
];

// Boot default diverges from C (DEFAULT_IO_METHOD = worker) until the worker
static IO_METHOD: AtomicI32 = AtomicI32::new(IOMETHOD_SYNC);
static IO_WORKERS: AtomicI32 = AtomicI32::new(3);
static IO_MAX_CONCURRENCY: AtomicI32 = AtomicI32::new(-1);

pub fn io_method() -> i32 {
    IO_METHOD.load(Ordering::Relaxed)
}

pub fn io_workers() -> i32 {
    IO_WORKERS.load(Ordering::Relaxed)
}

pub fn io_max_concurrency() -> i32 {
    IO_MAX_CONCURRENCY.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoMethodKind {
    Sync,
    Worker,
}

pub fn pgaio_method_kind() -> IoMethodKind {
    match io_method() {
        IOMETHOD_SYNC => IoMethodKind::Sync,
        IOMETHOD_WORKER => IoMethodKind::Worker,
        m => panic!("pgaio_method_kind: io_method {m} unported (backend-storage-aio-core)"),
    }
}


pub const PGAIO_HS_IDLE: u8 = 0;
pub const PGAIO_HS_HANDED_OUT: u8 = 1;
pub const PGAIO_HS_DEFINED: u8 = 2;
pub const PGAIO_HS_STAGED: u8 = 3;
pub const PGAIO_HS_SUBMITTED: u8 = 4;
pub const PGAIO_HS_COMPLETED_IO: u8 = 5;
pub const PGAIO_HS_COMPLETED_SHARED: u8 = 6;
pub const PGAIO_HS_COMPLETED_LOCAL: u8 = 7;

pub(crate) const NO_HANDLE: u32 = u32::MAX;

pub(crate) struct AioCell<T>(std::cell::UnsafeCell<T>);

// SAFETY: access serialized by the documented handle/backend ownership
unsafe impl<T> Sync for AioCell<T> {}

impl<T> AioCell<T> {
    pub const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    pub fn get(&self) -> *mut T {
        self.0.get()
    }
}

pub(crate) struct HandleData {
    pub target: u8,
    pub op: u8,
    pub num_callbacks: u8,
    pub callbacks: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    pub callbacks_data: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    pub handle_data_len: u8,
    pub resowner: Option<types_resowner::ResourceOwner>,
    // Raw pointer into the issuer's ReadBuffersOperation (C report_return).
    // C contract: only the OWNER dereferences report_return; resowner
    // cleanup clears it before the referenced storage can go away.
    pub report_return: *mut PgAioReturn,
    pub distilled_result: PgAioResult,
    pub op_data: PgAioOpDataRw,
    pub target_data: PgAioTargetData,
}

pub(crate) struct PgAioHandle {
    pub state: AtomicU8,
    // Read by cross-thread waiters: atomic where C reads a plain byte.
    pub flags: AtomicU8,
    pub owner_procno: i32,
    pub iovec_off: u32,
    pub generation: AtomicU64,
    pub result: AtomicI32,
    pub cv: condition_variable::ConditionVariable,
    pub d: AioCell<HandleData>,
    pub node: AioCell<ListNode>,
}

impl PgAioHandle {
    pub(crate) fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    pub(crate) fn set_state(&self, s: u8) {
        self.state.store(s, Ordering::Release);
    }

    /// SAFETY: caller is on the state-machine edge that owns `d` (see the
    /// HandleData publication contract above).
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn data(&self) -> &mut HandleData {
        &mut *self.d.get()
    }
}

// SAFETY: field access follows the aio.c ownership protocol documented on
unsafe impl Sync for PgAioHandle {}

#[derive(Clone, Copy)]
pub(crate) struct ListNode {
    pub prev: u32,
    pub next: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct Dclist {
    pub head: u32,
    pub tail: u32,
    pub count: u32,
}

impl Dclist {
    pub const fn new() -> Self {
        Dclist { head: NO_HANDLE, tail: NO_HANDLE, count: 0 }
    }
}

pub(crate) struct BackendData {
    pub idle_ios: Dclist,
    pub in_flight_ios: Dclist,
    pub handed_out_io: u32,
    pub in_batchmode: bool,
    pub num_staged_ios: u16,
    pub staged_ios: [u32; PGAIO_SUBMIT_BATCH_SIZE],
}

pub(crate) struct PgAioBackend {
    pub io_handle_off: u32,
    pub b: AioCell<BackendData>,
}

// SAFETY: BackendData is accessed only by the thread whose MyProcNumber owns
unsafe impl Sync for PgAioBackend {}

static HANDLES: AtomicPtr<PgAioHandle> = AtomicPtr::new(std::ptr::null_mut());
static HANDLE_COUNT: AtomicI64 = AtomicI64::new(0);
static BACKENDS: AtomicPtr<PgAioBackend> = AtomicPtr::new(std::ptr::null_mut());
static BACKEND_COUNT: AtomicI64 = AtomicI64::new(0);
static IOVECS: AtomicPtr<libc::iovec> = AtomicPtr::new(std::ptr::null_mut());
static HANDLE_DATA: AtomicPtr<u64> = AtomicPtr::new(std::ptr::null_mut());

pub(crate) fn handle_count() -> usize {
    HANDLE_COUNT.load(Ordering::Relaxed) as usize
}

pub(crate) fn ioh(index: u32) -> &'static PgAioHandle {
    debug_assert!((index as usize) < handle_count());
    // SAFETY: AioShmemInit published a table of handle_count() initialized
    unsafe { &*HANDLES.load(Ordering::Relaxed).add(index as usize) }
}

pub(crate) fn backend_slot(procno: i32) -> &'static PgAioBackend {
    debug_assert!(procno >= 0 && (procno as i64) < BACKEND_COUNT.load(Ordering::Relaxed));
    // SAFETY: as ioh().
    unsafe { &*BACKENDS.load(Ordering::Relaxed).add(procno as usize) }
}

/// SAFETY contract: written by the owner while defining the IO, read by the
pub(crate) unsafe fn iovec_region(iovec_off: u32) -> *mut libc::iovec {
    IOVECS.load(Ordering::Relaxed).add(iovec_off as usize)
}

pub(crate) unsafe fn handle_data_region(iovec_off: u32) -> *mut u64 {
    HANDLE_DATA.load(Ordering::Relaxed).add(iovec_off as usize)
}

thread_local! {
    // C pgaio_my_backend: this thread's procno slot, set by pgaio_init_backend.
    pub(crate) static MY_BACKEND: Cell<Option<i32>> = const { Cell::new(None) };
}

pub(crate) fn my_backend_procno() -> i32 {
    MY_BACKEND.get().expect("pgaio_my_backend is NULL (pgaio_init_backend not called)")
}

/// SAFETY: owner-thread-only by the pgaio_init_backend contract; callers must
/// aio.c reentrancy shape: stage -> submit -> prepare_submit).
#[allow(clippy::mut_from_ref)]
pub(crate) unsafe fn my_backend() -> &'static mut BackendData {
    let slot = backend_slot(my_backend_procno());
    &mut *slot.b.get()
}


pub(crate) fn dclist_push_head(list: &mut Dclist, index: u32) {
    // SAFETY: owner-only node access (list membership is owner-driven).
    unsafe {
        let n = &mut *ioh(index).node.get();
        n.prev = NO_HANDLE;
        n.next = list.head;
        if list.head != NO_HANDLE {
            (*ioh(list.head).node.get()).prev = index;
        } else {
            list.tail = index;
        }
    }
    list.head = index;
    list.count += 1;
}

pub(crate) fn dclist_push_tail(list: &mut Dclist, index: u32) {
    // SAFETY: as dclist_push_head.
    unsafe {
        let n = &mut *ioh(index).node.get();
        n.next = NO_HANDLE;
        n.prev = list.tail;
        if list.tail != NO_HANDLE {
            (*ioh(list.tail).node.get()).next = index;
        } else {
            list.head = index;
        }
    }
    list.tail = index;
    list.count += 1;
}

pub(crate) fn dclist_pop_head(list: &mut Dclist) -> u32 {
    debug_assert!(list.head != NO_HANDLE);
    let index = list.head;
    dclist_delete_from(list, index);
    index
}

pub(crate) fn dclist_delete_from(list: &mut Dclist, index: u32) {
    // SAFETY: as dclist_push_head.
    unsafe {
        let n = *ioh(index).node.get();
        if n.prev != NO_HANDLE {
            (*ioh(n.prev).node.get()).next = n.next;
        } else {
            debug_assert!(list.head == index);
            list.head = n.next;
        }
        if n.next != NO_HANDLE {
            (*ioh(n.next).node.get()).prev = n.prev;
        } else {
            debug_assert!(list.tail == index);
            list.tail = n.prev;
        }
    }
    debug_assert!(list.count > 0);
    list.count -= 1;
}

// GUC hooks (aio.c)

fn assign_io_method(newval: i32, _extra: Option<&GucHookExtra>) {
    IO_METHOD.store(newval, Ordering::Relaxed);
}

// pgrust-only: C compile-gates unavailable methods out of io_method_options;
// here unported methods are refused at the GUC gate instead (inert-fixes
fn check_io_method(
    newval: &mut i32,
    _extra: &mut Option<GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    if *newval != IOMETHOD_SYNC && *newval != IOMETHOD_WORKER {
        if guc_seams::guc_check_errdetail::is_installed() {
            let name = IO_METHOD_OPTIONS
                .iter()
                .find(|e| e.val == *newval)
                .map_or("?", |e| e.name);
            guc_seams::guc_check_errdetail::call(format!(
                "io_method=\"{name}\" is not yet supported by pgrust; use \"sync\" or \"worker\"."
            ));
        }
        return Ok(false);
    }
    Ok(true)
}

fn check_io_max_concurrency(
    newval: &mut i32,
    _extra: &mut Option<GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    if *newval == -1 {
        return Ok(true);
    }
    if *newval == 0 {
        if guc_seams::guc_check_errdetail::is_installed() {
            guc_seams::guc_check_errdetail::call(
                "Only -1 or values bigger than 0 are valid.".to_string(),
            );
        }
        return Ok(false);
    }
    Ok(true)
}

pub fn init_seams() {
    aio_seams::pgaio_init_backend::set(pgaio_init_backend);
    aio_seams::at_eoxact_aio::set(AtEOXact_Aio);
    aio_seams::pgaio_error_cleanup::set(pgaio_error_cleanup);
    aio_seams::pgaio_closing_fd::set(pgaio_closing_fd);
    aio_seams::pgaio_io_start_readv::set(pgaio_io_start_readv_current);
    aio_seams::pgaio_io_release_resowner::set(|node, on_error| {
        handle::pgaio_io_release_resowner(node as u32, on_error)
    });
    option_sets::io_method_options.install(IO_METHOD_OPTIONS);
    guc_tables::hooks::assign_io_method.install(assign_io_method);
    guc_tables::hooks::check_io_method.install(check_io_method);
    guc_tables::hooks::check_io_max_concurrency.install(check_io_max_concurrency);
    vars::io_method.install(GucVarAccessors {
        get: io_method,
        set: |v| IO_METHOD.store(v, Ordering::Relaxed),
    });
    vars::io_workers.install(GucVarAccessors {
        get: io_workers,
        set: |v| IO_WORKERS.store(v, Ordering::Relaxed),
    });
    vars::io_max_concurrency.install(GucVarAccessors {
        get: io_max_concurrency,
        set: |v| IO_MAX_CONCURRENCY.store(v, Ordering::Relaxed),
    });
}
