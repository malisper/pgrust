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
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU64, AtomicU8, Ordering};

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
    pgaio_result_report, pgaio_result_status_string,
};
pub use handle::{
    pgaio_closing_fd, pgaio_enter_batchmode, pgaio_error_cleanup, pgaio_exit_batchmode,
    pgaio_have_staged, pgaio_io_acquire, pgaio_io_acquire_nb, pgaio_io_get_id,
    pgaio_io_get_owner, pgaio_io_get_wref, pgaio_io_release, pgaio_io_set_flag,
    pgaio_submit_staged, pgaio_wref_check_done, pgaio_wref_clear, pgaio_wref_valid,
    pgaio_wref_wait, AtEOXact_Aio,
};
pub use init::{pgaio_init_backend, AioShmemInit, AioShmemResetAfterCrash, AioShmemSize};
pub use io::{
    pgaio_io_current, pgaio_io_op_name, pgaio_io_set_iovec_pages, pgaio_io_start_readv_current,
};
pub use method_worker::{
    pgaio_worker_cycle, pgaio_worker_executed_count, pgaio_worker_register,
    pgaio_workers_enabled,
};
pub use target::{pgaio_io_get_target_data, pgaio_io_set_target_smgr, pgaio_io_target_name};

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

/// aio.h `PgAioCtl`: the shared control block AioShmemInit registers as
/// "AioCtl" in the shmem index (aio_init.c:157). C keeps the table pointers
/// and counts here and every process finds them through `pgaio_ctl`; this is
/// the same block, reached through `PGAIO_CTL` (the process-global
/// `pgaio_ctl` pointer). `backend_count` is pgrust-only: the boot-time
/// AioProcs() the geometry was laid out for, so scan bounds never depend on
/// a live GUC read (see `io_handles_per_backend`).
pub(crate) struct PgAioCtl {
    pub io_handle_count: u64,
    pub iovec_count: u64,
    pub backend_count: u64,
    pub backend_state: *mut PgAioBackend,
    pub io_handles: *mut PgAioHandle,
    pub iovecs: *mut libc::iovec,
    pub handle_data: *mut u64,
}

// SAFETY: written once at AioShmemInit (single-threaded boot) and read-only
// afterwards; the tables it points to follow the handle/backend protocols.
unsafe impl Sync for PgAioCtl {}

// C `pgaio_ctl`: null until AioShmemInit has run.
static PGAIO_CTL: AtomicPtr<PgAioCtl> = AtomicPtr::new(std::ptr::null_mut());

pub(crate) fn pgaio_ctl_opt() -> Option<&'static PgAioCtl> {
    let p = PGAIO_CTL.load(Ordering::Acquire);
    // SAFETY: the block is placement-initialized before it is published
    // (Release store in AioShmemInit) and never freed.
    unsafe { p.as_ref() }
}

pub(crate) fn pgaio_ctl() -> &'static PgAioCtl {
    pgaio_ctl_opt().expect("pgaio_ctl is NULL (AioShmemInit not called)")
}

pub(crate) fn publish_ctl(ctl: *mut PgAioCtl) {
    PGAIO_CTL.store(ctl, Ordering::Release);
}

pub(crate) fn handle_count() -> usize {
    pgaio_ctl_opt().map_or(0, |c| c.io_handle_count as usize)
}

/// C `pgaio_ctl->io_handle_count` for readers outside the crate (pg_get_aios
/// walks the whole table).
pub fn pgaio_io_handle_count() -> usize {
    handle_count()
}

pub(crate) fn backend_count() -> usize {
    pgaio_ctl_opt().map_or(0, |c| c.backend_count as usize)
}

/// Immutable per-backend handle count, fixed at AioShmemInit from the boot-time
/// table geometry. Unlike the live io_max_concurrency GUC (which child-launch
/// republication can clobber back to its -1 sentinel), this is the true length
/// of each backend's handle range and is safe to use as a scan bound.
pub(crate) fn io_handles_per_backend() -> usize {
    let procs = backend_count();
    debug_assert!(procs > 0, "AioShmemInit must run before io_handles_per_backend");
    handle_count() / procs
}

pub(crate) fn ioh(index: u32) -> &'static PgAioHandle {
    let ctl = pgaio_ctl();
    debug_assert!((index as u64) < ctl.io_handle_count);
    // SAFETY: AioShmemInit published a table of io_handle_count initialized
    unsafe { &*ctl.io_handles.add(index as usize) }
}

pub(crate) fn backend_slot(procno: i32) -> &'static PgAioBackend {
    let ctl = pgaio_ctl();
    debug_assert!(procno >= 0 && (procno as u64) < ctl.backend_count);
    // SAFETY: as ioh().
    unsafe { &*ctl.backend_state.add(procno as usize) }
}

/// SAFETY contract: written by the owner while defining the IO, read by the
pub(crate) unsafe fn iovec_region(iovec_off: u32) -> *mut libc::iovec {
    let ctl = pgaio_ctl();
    debug_assert!((iovec_off as u64) < ctl.iovec_count);
    ctl.iovecs.add(iovec_off as usize)
}

pub(crate) unsafe fn handle_data_region(iovec_off: u32) -> *mut u64 {
    let ctl = pgaio_ctl();
    debug_assert!((iovec_off as u64) < ctl.iovec_count);
    ctl.handle_data.add(iovec_off as usize)
}

/// Placement value for one handle slot (AioShmemInit's per-handle init,
/// aio_init.c:196-206): IDLE, generation 1, owned by `procno`.
pub(crate) fn new_handle(procno: i32, iovec_off: u32) -> PgAioHandle {
    PgAioHandle {
        state: AtomicU8::new(PGAIO_HS_IDLE),
        flags: AtomicU8::new(0),
        owner_procno: procno,
        iovec_off,
        generation: AtomicU64::new(1),
        result: AtomicI32::new(0),
        cv: condition_variable::ConditionVariable::new(),
        d: AioCell::new(HandleData {
            target: types_storage::aio::PGAIO_TID_INVALID,
            op: types_storage::aio::PGAIO_OP_INVALID,
            num_callbacks: 0,
            callbacks: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            callbacks_data: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            handle_data_len: 0,
            resowner: None,
            report_return: std::ptr::null_mut(),
            distilled_result: PgAioResult {
                status: types_storage::aio::PgAioResultStatus::Unknown,
                ..Default::default()
            },
            op_data: Default::default(),
            target_data: Default::default(),
        }),
        node: AioCell::new(ListNode { prev: NO_HANDLE, next: NO_HANDLE }),
    }
}

/// Placement value for one backend slot (aio_init.c:183-189).
pub(crate) fn new_backend(io_handle_off: u32) -> PgAioBackend {
    PgAioBackend {
        io_handle_off,
        b: AioCell::new(BackendData {
            idle_ios: Dclist::new(),
            in_flight_ios: Dclist::new(),
            handed_out_io: NO_HANDLE,
            in_batchmode: false,
            num_staged_ios: 0,
            staged_ios: [NO_HANDLE; PGAIO_SUBMIT_BATCH_SIZE],
        }),
    }
}

/// aio.c pgaio_io_state_get_name.
pub fn pgaio_io_state_name(state: u8) -> &'static str {
    match state {
        PGAIO_HS_IDLE => "IDLE",
        PGAIO_HS_HANDED_OUT => "HANDED_OUT",
        PGAIO_HS_DEFINED => "DEFINED",
        PGAIO_HS_STAGED => "STAGED",
        PGAIO_HS_SUBMITTED => "SUBMITTED",
        PGAIO_HS_COMPLETED_IO => "COMPLETED_IO",
        PGAIO_HS_COMPLETED_SHARED => "COMPLETED_SHARED",
        PGAIO_HS_COMPLETED_LOCAL => "COMPLETED_LOCAL",
        _ => "?",
    }
}

/// One handle rendered for pg_get_aios (aio_funcs.c:56-146): the fields C
/// copies out of the live handle, taken under C's no-lock protocol.
#[derive(Clone, Copy, Debug)]
pub struct PgAioHandleSnapshot {
    pub id: i32,
    pub generation: u64,
    pub state: u8,
    pub owner_procno: i32,
    pub op: u8,
    pub op_offset: u64,
    /// iov_byte_length(iov, iov_length): bytes the op covers.
    pub iov_bytes: i64,
    pub target: u8,
    pub handle_data_len: u8,
    pub result: i32,
    pub distilled_status: types_storage::aio::PgAioResultStatus,
    pub target_data: PgAioTargetData,
    pub flags: u8,
}

/// aio_funcs.c:71-136 — there is no lock that could prevent the IO from
/// advancing concurrently, so: 1) note state + generation, 2) copy the
/// handle (and its iovecs) to local memory, 3) re-check: a generation change
/// means the IO was recycled (don't display it); a state change means retry.
/// `None` for IDLE or recycled handles.
pub fn pgaio_io_snapshot(index: u32) -> Option<PgAioHandleSnapshot> {
    use std::sync::atomic::fence;

    let h = ioh(index);
    let start_generation = h.generation.load(Ordering::Relaxed);
    let combine = guc_tables::vars::io_max_combine_limit.read().max(0) as usize;

    loop {
        // pg_read_barrier()
        fence(Ordering::Acquire);
        let start_state = h.state();
        if start_state == PGAIO_HS_IDLE {
            return None;
        }

        // 2) C memcpy's the live handle while the owner/completer may still
        // be writing it; the generation + state re-check below is what
        // validates the copy. Volatile reads keep the copy a plain byte
        // copy like C's, with no assumption of exclusive access.
        // SAFETY: the pointers address the handle's own storage and its
        // reserved iovec region (per_backend_iovecs covers io_max_combine_limit
        // entries per handle).
        let (d, iov_len_sum) = unsafe {
            let d = std::ptr::read_volatile(h.d.get());
            let iov = iovec_region(h.iovec_off);
            let n = (d.op_data.iov_length as usize).min(combine);
            let mut sum: i64 = 0;
            for i in 0..n {
                sum += std::ptr::read_volatile(iov.add(i)).iov_len as i64;
            }
            (d, sum)
        };
        let flags = h.flags.load(Ordering::Relaxed);
        let result = h.result.load(Ordering::Relaxed);
        let owner_procno = h.owner_procno;

        // 3) pg_read_barrier()
        fence(Ordering::Acquire);
        if h.generation.load(Ordering::Relaxed) != start_generation {
            return None;
        }
        if h.state() != start_state {
            continue;
        }

        return Some(PgAioHandleSnapshot {
            id: index as i32,
            generation: start_generation,
            state: start_state,
            owner_procno,
            op: d.op,
            op_offset: d.op_data.offset,
            iov_bytes: iov_len_sum,
            target: d.target,
            handle_data_len: d.handle_data_len,
            result,
            distilled_status: d.distilled_result.status,
            target_data: d.target_data,
            flags,
        });
    }
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
