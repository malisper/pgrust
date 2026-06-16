#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned `Err`; the un-boxed return is the project error
// contract.
#![allow(clippy::result_large_err)]

//! The boot-critical subset of the AIO subsystem:
//! `storage/aio/aio_init.c` (subsystem initialization) + the synchronous IO
//! method (`storage/aio/method_sync.c`), plus the two AIO GUC variables aio.c
//! owns (`int io_method` / `int io_max_concurrency`) and the
//! `assign_io_method` / `check_io_max_concurrency` hooks.
//!
//! This is the slice that `CreateSharedMemoryAndSemaphores` (ipci.c) reaches
//! through `AioShmemSize` / `AioShmemInit` and that `pgaio_init_backend`
//! (aio_init.c) reaches on per-backend setup. Landing it stops
//! `aio_shmem_init` from panicking on the boot path under `io_method = sync`.
//!
//! ## Shared-memory model
//!
//! In PostgreSQL the AIO control struct, the per-backend states, the io-handle
//! array, the iovec array and the handle-data array all live in one contiguous
//! shared-memory region wired together with raw pointers
//! (`PgAioCtl.backend_state`, `.io_handles`, …) and intrusive circular
//! doubly-linked lists (`dclist`) whose nodes are embedded in each
//! `PgAioHandle`. This crate expresses the same structures field-for-field, but
//! replaces the raw-pointer sub-allocations with owned vectors and the
//! intrusive-list membership with handle-*index* lists carrying the same
//! ordering and `count`. The control struct lives in a process-global
//! [`OnceLock`] — the same idiom every ported shmem subsystem in this workspace
//! uses (e.g. `ProcSignal`) — so the C `ShmemInitStruct`/`found` handshake
//! becomes `get_or_init` (a second caller attaches; the first builds).
//!
//! ## What is NOT here (seam-and-panic, by design — task #15)
//!
//! The AIO *engine* (`aio.c`: `pgaio_io_acquire`/`_nb`, `pgaio_io_release`,
//! the `pgaio_io_start_*` staging path, `pgaio_io_wait`/`pgaio_wref_wait`,
//! `pgaio_submit_staged`, `pgaio_io_reclaim`, the callback dispatch in
//! `aio_callback.c`, the per-op synchronous execution in `aio_io.c`, the target
//! reopen in `aio_target.c`), and the asynchronous IO methods
//! (`method_worker.c`, `method_io_uring.c`) are unported. They are deeply
//! entangled with the resource-owner AIO integration, the pgstat AIO counters,
//! the interrupt machinery and the buffer manager's AIO completion callbacks —
//! genuinely bufmgr/engine-blocked. The synchronous method makes every IO
//! execute inline in the issuing backend via `pgaio_io_perform_synchronously`
//! (aio_io.c), which is part of that unported engine; so `pgaio_sync_submit`
//! here faithfully reproduces the C `elog(ERROR, "IO should have been executed
//! synchronously")` (it is never reached because `needs_synchronous_execution`
//! returns true and the engine that would call `submit` is unported anyway).
//!
//! The `aio-seams` engine decls (`pgaio_error_cleanup`, `pgaio_closing_fd`,
//! `at_eoxact_aio`, `pgaio_io_start_readv`) stay uninstalled — calling them
//! panics with the C symbol name until the engine lands.

extern crate alloc;

use alloc::string::ToString;
use alloc::vec::Vec;

use types_condvar::ConditionVariable;
use types_core::primitive::Size;
use types_error::{PgError, PgResult};
use types_storage::storage::NUM_AUXILIARY_PROCS;

// ===========================================================================
// io_method enum (storage/aio.h)
// ===========================================================================

/// `IOMETHOD_SYNC = 0` (`storage/aio.h`).
pub const IOMETHOD_SYNC: i32 = 0;
/// `IOMETHOD_WORKER = 1` (`storage/aio.h`).
pub const IOMETHOD_WORKER: i32 = 1;
/// `IOMETHOD_IO_URING = 2` (`storage/aio.h`, when `IOMETHOD_IO_URING_ENABLED`).
pub const IOMETHOD_IO_URING: i32 = 2;

/// `#define DEFAULT_IO_METHOD IOMETHOD_WORKER` (`storage/aio.h`).
pub const DEFAULT_IO_METHOD: i32 = IOMETHOD_WORKER;

/// `#define PGAIO_SUBMIT_BATCH_SIZE 32` (`storage/aio_internal.h`).
pub const PGAIO_SUBMIT_BATCH_SIZE: usize = 32;

/// `#define PGAIO_HANDLE_MAX_CALLBACKS 4` (`storage/aio.h`).
pub const PGAIO_HANDLE_MAX_CALLBACKS: usize = 4;

// ===========================================================================
// The shared-memory data model (storage/aio_internal.h, storage/aio_types.h)
// ===========================================================================

/// `enum PgAioHandleState` (`storage/aio_internal.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PgAioHandleState {
    /// `PGAIO_HS_IDLE = 0` — not in use.
    #[default]
    Idle = 0,
    /// `PGAIO_HS_HANDED_OUT` — returned by `pgaio_io_acquire()`.
    HandedOut,
    /// `PGAIO_HS_DEFINED` — `pgaio_io_start_*()` called, not yet staged.
    Defined,
    /// `PGAIO_HS_STAGED` — staged, ready to submit.
    Staged,
    /// `PGAIO_HS_SUBMITTED` — submitted to the IO method.
    Submitted,
    /// `PGAIO_HS_COMPLETED_IO` — IO finished, result unprocessed.
    CompletedIo,
    /// `PGAIO_HS_COMPLETED_SHARED` — shared completion done.
    CompletedShared,
    /// `PGAIO_HS_COMPLETED_LOCAL` — local completion done.
    CompletedLocal,
}

/// `enum PgAioResultStatus` (`storage/aio_types.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PgAioResultStatus {
    /// `PGAIO_RS_UNKNOWN` — not yet completed / uninitialized (the zero value).
    #[default]
    Unknown = 0,
    /// `PGAIO_RS_OK`.
    Ok,
    /// `PGAIO_RS_PARTIAL` — partial success, no warning/error.
    Partial,
    /// `PGAIO_RS_WARNING` — [partially] succeeded with a warning.
    Warning,
    /// `PGAIO_RS_ERROR` — failed entirely.
    Error,
}

/// `struct iovec` (`<sys/uio.h>`, via `port/pg_iovec.h`). `aio_init.c` only
/// allocates an array of these and never reads/writes the fields here.
#[derive(Clone, Copy, Debug, Default)]
pub struct Iovec {
    /// `void *iov_base` — the buffer region base (an integer cookie here; the
    /// engine populates it during `pgaio_io_set_iovec`).
    pub iov_base: usize,
    /// `size_t iov_len` — length of the buffer region.
    pub iov_len: usize,
}

/// `struct PgAioResult` (`storage/aio_types.h`) — packed into 8 bytes via
/// bitfields in C; plain fields here. `aio_init.c` only writes `status`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioResult {
    /// `uint32 id:6` — `PgAioHandleCallbackID`.
    pub id: u32,
    /// `uint32 status:3` — a [`PgAioResultStatus`].
    pub status: PgAioResultStatus,
    /// `uint32 error_data:23` — callback-defined error data.
    pub error_data: u32,
    /// `int32 result`.
    pub result: i32,
}

/// `union PgAioTargetData` (`storage/aio_types.h`) — currently a single `smgr`
/// arm. `aio_init.c` never reads or writes its fields; modeled as opaque bytes
/// sized for the engine to populate later.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioTargetData {
    /// `smgr` arm fields, opaque to the init/sync slice.
    pub rlocator_spc_oid: u32,
    pub rlocator_db_oid: u32,
    pub rlocator_rel_number: u32,
    pub block: u32,
    pub nblocks: u16,
    pub fork: u8,
    pub is_temp: bool,
    pub skip_fsync: bool,
}

/// `union PgAioOpData` (`storage/aio.h`) — read/write arms share a layout.
/// `aio_init.c` never reads or writes it.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioOpData {
    /// `int fd`.
    pub fd: i32,
    /// `uint16 iov_length`.
    pub iov_length: u16,
    /// `uint64 offset`.
    pub offset: u64,
}

/// `struct PgAioReturn` (`storage/aio_types.h`). `aio_init.c` only stores
/// "no return location" (`report_return = NULL`), modeled as `Option`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgAioReturn {
    /// `PgAioResult result`.
    pub result: PgAioResult,
    /// `PgAioTargetData target_data`.
    pub target_data: PgAioTargetData,
}

/// `struct ResourceOwnerData *` (`utils/resowner.c`) — opaque to `aio_init.c`,
/// which only ever stores the null pointer (`None`). The resowner AIO
/// integration (`pgaio_io_resowner_register`) belongs to the unported engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceOwnerId(pub u64);

/// Head of the intrusive idle/in-flight handle lists (`dclist_head`,
/// `lib/ilist.h`). The owned model carries the same `count` plus the ordered
/// handle indices the intrusive list would chain (each index is an offset into
/// [`PgAioCtl::io_handles`]), preserving idle/submission ordering.
#[derive(Clone, Debug, Default)]
pub struct DclistHead {
    /// Ordered membership: the io-handle indices currently linked, tail-last,
    /// exactly as `dclist_push_tail` appends them.
    pub members: Vec<usize>,
    /// `uint32 count` — number of elements, kept in lockstep with `members`.
    pub count: u32,
}

/// `struct PgAioHandle` (`storage/aio_internal.h`). Field order mirrors C.
///
/// The intrusive `dlist_node node`/`resowner_node` membership is expressed by
/// [`DclistHead`] index lists on the owning [`PgAioBackend`], so the embedded
/// node fields carry no raw pointers. `cv` is the workspace's real
/// [`ConditionVariable`] (shmem-resident, `!Copy`/`!Clone`), so this struct is
/// neither `Copy` nor `Clone` — handles are constructed in place by
/// `AioShmemInit`, exactly as C memsets and fills each one.
#[derive(Debug)]
pub struct PgAioHandle {
    /// `uint8 state` — a [`PgAioHandleState`].
    pub state: PgAioHandleState,
    /// `uint8 target` — a `PgAioTargetID`.
    pub target: u8,
    /// `uint8 op` — which IO operation.
    pub op: u8,
    /// `uint8 flags` — bitfield of `PgAioHandleFlags`.
    pub flags: u8,
    /// `uint8 num_callbacks`.
    pub num_callbacks: u8,
    /// `uint8 callbacks[PGAIO_HANDLE_MAX_CALLBACKS]`.
    pub callbacks: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    /// `uint8 callbacks_data[PGAIO_HANDLE_MAX_CALLBACKS]`.
    pub callbacks_data: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    /// `uint8 handle_data_len`.
    pub handle_data_len: u8,
    /// `int32 owner_procno`.
    pub owner_procno: i32,
    /// `int32 result` — raw result of the IO operation.
    pub result: i32,
    /// `struct ResourceOwnerData *resowner` — `None` until an owner is set.
    pub resowner: Option<ResourceOwnerId>,
    /// `uint64 generation` — incremented every time the handle is reused.
    pub generation: u64,
    /// `ConditionVariable cv`.
    pub cv: ConditionVariable,
    /// `PgAioResult distilled_result`.
    pub distilled_result: PgAioResult,
    /// `uint32 iovec_off` — index into `PgAioCtl.iovecs`/`.handle_data`.
    pub iovec_off: u32,
    /// `PgAioReturn *report_return` — `None` until a return location is set.
    pub report_return: Option<PgAioReturn>,
    /// `PgAioOpData op_data`.
    pub op_data: PgAioOpData,
    /// `PgAioTargetData target_data`.
    pub target_data: PgAioTargetData,
}

impl PgAioHandle {
    /// A freshly zeroed handle (the C `memset(pgaio_ctl, 0, ...)` baseline for
    /// one handle, before `AioShmemInit` fills generation/owner/iovec_off/cv).
    fn zeroed() -> Self {
        PgAioHandle {
            state: PgAioHandleState::Idle,
            target: 0,
            op: 0,
            flags: 0,
            num_callbacks: 0,
            callbacks: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            callbacks_data: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            handle_data_len: 0,
            owner_procno: 0,
            result: 0,
            resowner: None,
            generation: 0,
            cv: ConditionVariable::new(),
            distilled_result: PgAioResult::default(),
            iovec_off: 0,
            report_return: None,
            op_data: PgAioOpData::default(),
            target_data: PgAioTargetData::default(),
        }
    }
}

/// `typedef struct PgAioBackend` (`storage/aio_internal.h`). Field order mirrors
/// C.
#[derive(Clone, Debug, Default)]
pub struct PgAioBackend {
    /// `uint32 io_handle_off` — index into `PgAioCtl.io_handles`.
    pub io_handle_off: u32,
    /// `dclist_head idle_ios` — handles currently not in use.
    pub idle_ios: DclistHead,
    /// `PgAioHandle *handed_out_io` — the single handed-out-but-undefined
    /// handle, by io-handle index; `None` when none is handed out.
    pub handed_out_io: Option<usize>,
    /// `bool in_batchmode`.
    pub in_batchmode: bool,
    /// `uint16 num_staged_ios`.
    pub num_staged_ios: u16,
    /// `PgAioHandle *staged_ios[PGAIO_SUBMIT_BATCH_SIZE]` — defined-but-unsubmitted
    /// handles, by io-handle index (`None` for empty slots).
    pub staged_ios: [Option<usize>; PGAIO_SUBMIT_BATCH_SIZE],
    /// `dclist_head in_flight_ios` — in-flight (or completed-elsewhere) handles,
    /// ordered by submission time.
    pub in_flight_ios: DclistHead,
}

/// `typedef struct PgAioCtl` (`storage/aio_internal.h`). The C raw-pointer
/// sub-allocations become owned vectors; the counts are preserved verbatim.
#[derive(Debug, Default)]
pub struct PgAioCtl {
    /// `int backend_state_count`.
    pub backend_state_count: i32,
    /// `PgAioBackend *backend_state`.
    pub backend_state: Vec<PgAioBackend>,
    /// `uint32 iovec_count`.
    pub iovec_count: u32,
    /// `struct iovec *iovecs`.
    pub iovecs: Vec<Iovec>,
    /// `uint64 *handle_data`.
    pub handle_data: Vec<u64>,
    /// `uint32 io_handle_count`.
    pub io_handle_count: u32,
    /// `PgAioHandle *io_handles`.
    pub io_handles: Vec<PgAioHandle>,
}

// ===========================================================================
// IO method ops table (storage/aio_internal.h)
// ===========================================================================

/// `typedef struct IoMethodOps` (`storage/aio_internal.h`) — the vtable each IO
/// method exposes. `aio_init.c` reads `wait_on_fd_before_close`, `shmem_size`,
/// `shmem_init`, `init_backend`; `aio.c` also reads
/// `needs_synchronous_execution`, `submit`, `wait_one`. Optional callbacks are
/// `None` (a NULL C function pointer).
///
/// The engine-side callbacks (`submit`/`wait_one`) take `PgAioHandle`s; the
/// sync method's `submit` is the only one the boot-critical slice supplies, and
/// it faithfully reproduces the C `elog(ERROR, ...)`.
pub struct IoMethodOps {
    /// `bool wait_on_fd_before_close`.
    pub wait_on_fd_before_close: bool,
    /// `size_t (*shmem_size)(void)` — additional shmem this method reserves.
    pub shmem_size: Option<fn() -> PgResult<Size>>,
    /// `void (*shmem_init)(bool first_time)`.
    pub shmem_init: Option<fn(first_time: bool) -> PgResult<()>>,
    /// `void (*init_backend)(void)`.
    pub init_backend: Option<fn() -> PgResult<()>>,
    /// `bool (*needs_synchronous_execution)(PgAioHandle *ioh)`.
    pub needs_synchronous_execution: Option<fn(&PgAioHandle) -> bool>,
    /// `int (*submit)(uint16 num_staged_ios, PgAioHandle **staged_ios)`.
    pub submit: Option<fn(num_staged_ios: u16) -> PgResult<i32>>,
    /// `void (*wait_one)(PgAioHandle *ioh, uint64 ref_generation)`.
    pub wait_one: Option<fn(&PgAioHandle, u64)>,
}

// --- method_sync.c ---------------------------------------------------------

/// `static bool pgaio_sync_needs_synchronous_execution(PgAioHandle *ioh)`
/// (method_sync.c) — always true.
fn pgaio_sync_needs_synchronous_execution(_ioh: &PgAioHandle) -> bool {
    true
}

/// `static int pgaio_sync_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)`
/// (method_sync.c) — `elog(ERROR, "IO should have been executed synchronously")`.
/// Never reached: `needs_synchronous_execution` returns true, so the engine
/// executes the IO inline (`pgaio_io_perform_synchronously`) and never calls
/// `submit`. Faithful to the C error nonetheless.
fn pgaio_sync_submit(_num_staged_ios: u16) -> PgResult<i32> {
    Err(PgError::error("IO should have been executed synchronously"))
}

/// `const IoMethodOps pgaio_sync_ops` (method_sync.c).
fn pgaio_sync_ops() -> IoMethodOps {
    IoMethodOps {
        wait_on_fd_before_close: false,
        shmem_size: None,
        shmem_init: None,
        init_backend: None,
        needs_synchronous_execution: Some(pgaio_sync_needs_synchronous_execution),
        submit: Some(pgaio_sync_submit),
        wait_one: None,
    }
}

/// `const IoMethodOps pgaio_worker_ops` (method_worker.c) — the worker IO method
/// is unported (task #15, F4). Its `shmem_size`/`shmem_init`/`init_backend`/
/// `submit`/`wait_one` all reach the worker-process queue machinery, so they
/// panic with the C symbol until method_worker.c lands. A cluster running the
/// default `io_method = worker` reaches these from `AioShmemSize`/`AioShmemInit`
/// (the worker method reserves a shmem queue); that is the precise, correct
/// "worker method unported" boundary. Under `io_method = sync` the boot path
/// never selects this entry.
fn pgaio_worker_ops() -> IoMethodOps {
    IoMethodOps {
        // method_worker.c: `.wait_on_fd_before_close = true`.
        wait_on_fd_before_close: true,
        shmem_size: Some(|| {
            panic!(
                "pgaio_worker_shmem_size: the worker IO method (method_worker.c) \
                 is not yet ported (task #15 F4); run io_method = sync"
            )
        }),
        shmem_init: Some(|_first_time| {
            panic!(
                "pgaio_worker_shmem_init: the worker IO method (method_worker.c) \
                 is not yet ported (task #15 F4); run io_method = sync"
            )
        }),
        init_backend: Some(|| {
            panic!(
                "pgaio_worker_init_backend: the worker IO method (method_worker.c) \
                 is not yet ported (task #15 F4); run io_method = sync"
            )
        }),
        needs_synchronous_execution: None,
        submit: Some(|_n| {
            panic!(
                "pgaio_worker_submit: the worker IO method (method_worker.c) is \
                 not yet ported (task #15 F4); run io_method = sync"
            )
        }),
        wait_one: Some(|_ioh, _gen| {
            panic!(
                "pgaio_worker_wait_one: the worker IO method (method_worker.c) is \
                 not yet ported (task #15 F4); run io_method = sync"
            )
        }),
    }
}

/// `static const IoMethodOps *const pgaio_method_ops_table[]` (aio.c) — indexed
/// by `io_method`. `io_uring` is compiled out here (no
/// `IOMETHOD_IO_URING_ENABLED`), matching a build without liburing.
fn pgaio_method_ops_for(io_method: i32) -> IoMethodOps {
    match io_method {
        IOMETHOD_SYNC => pgaio_sync_ops(),
        IOMETHOD_WORKER => pgaio_worker_ops(),
        // io_uring is unported (task #15, F4) and gated behind
        // IOMETHOD_IO_URING_ENABLED in C; selecting it is a config the boot
        // path under sync never takes.
        IOMETHOD_IO_URING => panic!(
            "io_method = io_uring: the io_uring IO method (method_io_uring.c) is \
             not yet ported (task #15 F4); run io_method = sync"
        ),
        other => panic!("assign_io_method: invalid io_method value {other}"),
    }
}

/// The active `const IoMethodOps *pgaio_method_ops` (aio.c). `assign_io_method`
/// sets this from `io_method`; here it is resolved on demand from the live GUC
/// store (the C global is written by the assign hook, which runs at GUC apply
/// time; reading the resolved table entry on demand is equivalent and avoids a
/// stale process-global before the hook fires).
fn pgaio_method_ops() -> IoMethodOps {
    pgaio_method_ops_for(current_io_method())
}

// ===========================================================================
// GUC ownership: int io_method / int io_max_concurrency (aio.c globals)
// ===========================================================================

/// Read `io_method` from the live GUC store (the C `*conf->variable`). At boot,
/// before any SET, this returns the compiled-in boot value
/// (`DEFAULT_IO_METHOD`).
fn current_io_method() -> i32 {
    backend_utils_misc_guc::get_enum("io_method").unwrap_or(DEFAULT_IO_METHOD)
}

/// Read `io_max_concurrency` from the live GUC store (the C `io_max_concurrency`
/// global). `-1` (the boot value) means "auto-tune later".
fn io_max_concurrency() -> i32 {
    backend_utils_misc_guc::get_int("io_max_concurrency").unwrap_or(-1)
}

/// Read `io_max_combine_limit` from the live GUC store. This GUC is owned by
/// bufmgr.c; `AioShmemSize`/`AioShmemInit` read it to size the iovec and
/// handle-data arrays. The live store returns its boot value even before
/// bufmgr installs its variable accessor.
fn io_max_combine_limit() -> i32 {
    backend_utils_misc_guc::get_int("io_max_combine_limit")
        .expect("io_max_combine_limit GUC not registered (initialize_guc_options not called)")
}

/// `void assign_io_method(int newval, void *extra)` (aio.c) — set
/// `pgaio_method_ops` from the table. Here `pgaio_method_ops` is resolved on
/// demand, so the assign hook only validates the index (the C asserts the table
/// entry is non-NULL).
fn assign_io_method(newval: i32, _extra: Option<&backend_utils_misc_guc_tables::GucHookExtra>) {
    // Assert(newval < lengthof(pgaio_method_ops_table)) +
    // Assert(pgaio_method_ops_table[newval] != NULL): validate by resolving.
    debug_assert!((IOMETHOD_SYNC..=IOMETHOD_IO_URING).contains(&newval));
    // The on-demand resolver IS the table; nothing to cache.
    let _ = newval;
}

/// `bool check_io_max_concurrency(int *newval, void **extra, GucSource source)`
/// (aio.c).
fn check_io_max_concurrency(
    newval: &mut i32,
    _extra: &mut Option<backend_utils_misc_guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    if *newval == -1 {
        // Auto-tuning will be applied later during startup.
        return Ok(true);
    } else if *newval == 0 {
        backend_utils_misc_guc_seams::guc_check_errdetail::call(alloc::string::String::from(
            "Only -1 or values bigger than 0 are valid.",
        ));
        return Ok(false);
    }
    Ok(true)
}

/// `const struct config_enum_entry io_method_options[]` (aio.c). `io_uring` is
/// omitted (no `IOMETHOD_IO_URING_ENABLED`).
const IO_METHOD_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry { name: "sync", val: IOMETHOD_SYNC, hidden: false },
    types_guc::config_enum_entry { name: "worker", val: IOMETHOD_WORKER, hidden: false },
];

// ===========================================================================
// aio_init.c — subsystem initialization
// ===========================================================================

/// The process-global `PgAioCtl *pgaio_ctl` (aio.c). Built once by the first
/// `AioShmemInit` caller (the C `!found` branch); later callers attach.
static PGAIO_CTL: std::sync::OnceLock<PgAioCtl> = std::sync::OnceLock::new();

/// `static Size AioCtlShmemSize(void)` (aio_init.c) — `sizeof(PgAioCtl)`.
fn AioCtlShmemSize() -> Size {
    core::mem::size_of::<PgAioCtl>()
}

/// `static uint32 AioProcs(void)` (aio_init.c) — `MaxBackends + NUM_AUXILIARY_PROCS`.
fn AioProcs() -> u32 {
    (backend_utils_init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS) as u32
}

/// `static Size AioBackendShmemSize(void)` (aio_init.c).
fn AioBackendShmemSize() -> PgResult<Size> {
    backend_storage_ipc_shmem_seams::mul_size::call(
        AioProcs() as Size,
        core::mem::size_of::<PgAioBackend>(),
    )
}

/// `static Size AioHandleShmemSize(void)` (aio_init.c).
fn AioHandleShmemSize() -> PgResult<Size> {
    // Assert(io_max_concurrency > 0) — AioChooseMaxConcurrency ran already.
    debug_assert!(io_max_concurrency() > 0);
    let inner = backend_storage_ipc_shmem_seams::mul_size::call(
        io_max_concurrency() as Size,
        core::mem::size_of::<PgAioHandle>(),
    )?;
    backend_storage_ipc_shmem_seams::mul_size::call(AioProcs() as Size, inner)
}

/// `static Size AioHandleIOVShmemSize(void)` (aio_init.c).
fn AioHandleIOVShmemSize() -> PgResult<Size> {
    // each IO handle can have up to io_max_combine_limit iovec objects
    let combine_x_procs = backend_storage_ipc_shmem_seams::mul_size::call(
        io_max_combine_limit() as Size,
        AioProcs() as Size,
    )?;
    let inner =
        backend_storage_ipc_shmem_seams::mul_size::call(combine_x_procs, io_max_concurrency() as Size)?;
    backend_storage_ipc_shmem_seams::mul_size::call(core::mem::size_of::<Iovec>(), inner)
}

/// `static Size AioHandleDataShmemSize(void)` (aio_init.c).
fn AioHandleDataShmemSize() -> PgResult<Size> {
    // each buffer referenced by an iovec can have associated data
    let combine_x_procs = backend_storage_ipc_shmem_seams::mul_size::call(
        io_max_combine_limit() as Size,
        AioProcs() as Size,
    )?;
    let inner =
        backend_storage_ipc_shmem_seams::mul_size::call(combine_x_procs, io_max_concurrency() as Size)?;
    backend_storage_ipc_shmem_seams::mul_size::call(core::mem::size_of::<u64>(), inner)
}

/// `static int AioChooseMaxConcurrency(void)` (aio_init.c).
fn AioChooseMaxConcurrency() -> i32 {
    // Similar logic to LimitAdditionalPins().
    let max_backends: u32 =
        (backend_utils_init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS) as u32;
    let mut max_proportional_pins: i32 =
        backend_utils_init_small_seams::nbuffers::call() / max_backends as i32;
    max_proportional_pins = core::cmp::max(max_proportional_pins, 1);
    // apply upper limit
    core::cmp::min(max_proportional_pins, 64)
}

/// `Size AioShmemSize(void)` (aio_init.c).
pub fn AioShmemSize() -> PgResult<Size> {
    let mut sz: Size = 0;

    // If the DBA left io_max_concurrency = -1, force its dynamic default (and,
    // if that fails to override an explicit -1, force it harder). The C builds
    // the decimal value with snprintf; here SetConfigOption takes the rendered
    // string.
    if io_max_concurrency() == -1 {
        let buf = AioChooseMaxConcurrency().to_string();
        backend_utils_misc_guc_seams::set_config_option::call(
            "io_max_concurrency",
            &buf,
            types_guc::GucContext::PGC_POSTMASTER,
            types_guc::GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;
        if io_max_concurrency() == -1 {
            // failed to apply it?
            backend_utils_misc_guc_seams::set_config_option::call(
                "io_max_concurrency",
                &buf,
                types_guc::GucContext::PGC_POSTMASTER,
                types_guc::GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }

    sz = backend_storage_ipc_shmem_seams::add_size::call(sz, AioCtlShmemSize())?;
    sz = backend_storage_ipc_shmem_seams::add_size::call(sz, AioBackendShmemSize()?)?;
    sz = backend_storage_ipc_shmem_seams::add_size::call(sz, AioHandleShmemSize()?)?;
    sz = backend_storage_ipc_shmem_seams::add_size::call(sz, AioHandleIOVShmemSize()?)?;
    sz = backend_storage_ipc_shmem_seams::add_size::call(sz, AioHandleDataShmemSize()?)?;

    // Reserve space for method specific resources.
    let ops = pgaio_method_ops();
    if let Some(method_size) = ops.shmem_size {
        sz = backend_storage_ipc_shmem_seams::add_size::call(sz, method_size()?)?;
    }

    Ok(sz)
}

/// `void AioShmemInit(void)` (aio_init.c). Builds the process-global
/// [`PgAioCtl`] on first call (the C `!found` branch) and runs the IO method's
/// `shmem_init`; a later call attaches (the `found` short-circuit).
pub fn AioShmemInit() -> PgResult<()> {
    // ShmemInitStruct("AioCtl", ...): first caller builds, later callers attach.
    let found = PGAIO_CTL.get().is_some();

    if found {
        // goto out:
        return aio_shmem_init_out(found);
    }

    // Build the full control struct (the !found branch). Computed before the
    // OnceLock set so an error (size overflow) surfaces without a half-built
    // struct landing in the global.
    let ctl = build_pgaio_ctl()?;

    // pgaio_ctl = ShmemInitStruct(...) — publish the built struct. A racing
    // initializer would lose here; AIO shmem init is single-threaded at
    // postmaster startup, so set() always succeeds on the first call.
    let _ = PGAIO_CTL.set(ctl);

    aio_shmem_init_out(found)
}

/// The body of `AioShmemInit`'s `!found` branch (aio_init.c L162-209).
fn build_pgaio_ctl() -> PgResult<PgAioCtl> {
    let aio_procs = AioProcs();
    let imc = io_max_concurrency();
    let imcl = io_max_combine_limit();
    let per_backend_iovecs: u32 = (imc * imcl) as u32;

    // memset(pgaio_ctl, 0, ...) — start from a zeroed struct.
    let mut pgaio_ctl = PgAioCtl::default();

    pgaio_ctl.io_handle_count = aio_procs * imc as u32;
    pgaio_ctl.iovec_count = aio_procs * per_backend_iovecs;

    // The C sub-allocates each region via ShmemInitStruct (whose byte sizing we
    // mirror above for AioShmemSize); here they are owned vectors of the same
    // element count.
    grow_with(&mut pgaio_ctl.backend_state, aio_procs as usize, PgAioBackend::default)?;
    grow_with(
        &mut pgaio_ctl.io_handles,
        pgaio_ctl.io_handle_count as usize,
        PgAioHandle::zeroed,
    )?;
    grow_with(&mut pgaio_ctl.iovecs, pgaio_ctl.iovec_count as usize, Iovec::default)?;
    grow_with(&mut pgaio_ctl.handle_data, pgaio_ctl.iovec_count as usize, u64::default)?;

    let mut io_handle_off: u32 = 0;
    let mut iovec_off: u32 = 0;

    for procno in 0..aio_procs as i32 {
        let bs_io_handle_off = io_handle_off;
        io_handle_off += imc as u32;

        let mut idle_ios = DclistHead::default();
        dclist_init(&mut idle_ios);
        // memset(bs->staged_ios, 0, ...): all slots cleared.
        let staged_ios: [Option<usize>; PGAIO_SUBMIT_BATCH_SIZE] = [None; PGAIO_SUBMIT_BATCH_SIZE];
        let mut in_flight_ios = DclistHead::default();
        dclist_init(&mut in_flight_ios);

        // initialize per-backend IOs
        for i in 0..imc {
            let ioh_index = (bs_io_handle_off + i as u32) as usize;
            let ioh = &mut pgaio_ctl.io_handles[ioh_index];

            ioh.generation = 1;
            ioh.owner_procno = procno;
            ioh.iovec_off = iovec_off;
            ioh.handle_data_len = 0;
            ioh.report_return = None;
            ioh.resowner = None;
            ioh.num_callbacks = 0;
            ioh.distilled_result.status = PgAioResultStatus::Unknown;
            ioh.flags = 0;

            // ConditionVariableInit(&ioh->cv) — re-initialize in place.
            ConditionVariableInit(&mut ioh.cv);

            // dclist_push_tail(&bs->idle_ios, &ioh->node);
            dclist_push_tail(&mut idle_ios, ioh_index)?;
            iovec_off += imcl as u32;
        }

        let bs = &mut pgaio_ctl.backend_state[procno as usize];
        bs.io_handle_off = bs_io_handle_off;
        bs.idle_ios = idle_ios;
        bs.staged_ios = staged_ios;
        bs.in_flight_ios = in_flight_ios;
    }

    Ok(pgaio_ctl)
}

/// The `out:` label tail of `AioShmemInit` (aio_init.c L211-215).
fn aio_shmem_init_out(found: bool) -> PgResult<()> {
    // Initialize IO method specific resources.
    let ops = pgaio_method_ops();
    if let Some(shmem_init) = ops.shmem_init {
        shmem_init(!found)?;
    }
    Ok(())
}

/// `void pgaio_init_backend(void)` (aio_init.c L217-235).
pub fn pgaio_init_backend() -> PgResult<()> {
    // Assert(!pgaio_my_backend) — shouldn't be initialized twice.

    if backend_utils_init_small_seams::my_backend_type::call()
        == types_core::init::BackendType::IoWorker
    {
        return Ok(());
    }

    // C: `if (MyProc == NULL || MyProcNumber >= AioProcs())`. `MyProcNumber`
    // reads `INVALID_PROC_NUMBER` (-1) when `MyProc == NULL`, so the NULL test
    // is the invalid-procnumber test plus the upper bound.
    let my_proc_number = backend_utils_init_small_seams::my_proc_number::call();
    if my_proc_number == types_core::primitive::INVALID_PROC_NUMBER
        || my_proc_number >= AioProcs() as i32
    {
        return Err(PgError::error("aio requires a normal PGPROC"));
    }

    // pgaio_my_backend = &pgaio_ctl->backend_state[MyProcNumber]; the C global
    // is a per-backend pointer into shmem. Validate the index exists (the
    // engine, when ported, will hold the resolved per-backend reference).
    let my_backend = my_proc_number as usize;
    let ctl = pgaio_ctl();
    let _ = &ctl.backend_state[my_backend];

    let ops = pgaio_method_ops();
    if let Some(init_backend) = ops.init_backend {
        init_backend()?;
    }

    // before_shmem_exit(pgaio_shutdown, 0).
    backend_storage_ipc_dsm_core_seams::before_shmem_exit::call(
        pgaio_shutdown,
        types_tuple::Datum::null(),
    )?;

    Ok(())
}

/// `void pgaio_shutdown(int code, Datum arg)` (aio.c L1311) — registered as the
/// `before_shmem_exit` callback. Its body (`AtEOXact_Aio` + draining in-flight
/// IOs + per-method shutdown) is part of the unported engine; registering the
/// callback is correct, firing it panics with the precise boundary.
fn pgaio_shutdown(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    panic!(
        "pgaio_shutdown: the AIO engine (aio.c AtEOXact_Aio + in-flight drain) \
         is not yet ported (task #15); only aio_init.c + the sync method are"
    )
}

// ===========================================================================
// The `pgaio_ctl` global accessor + intrusive-list helpers (lib/ilist.h)
// ===========================================================================

/// The `pgaio_ctl != NULL` dereference: C would crash on use before
/// `AioShmemInit`; here it is a loud panic.
fn pgaio_ctl() -> &'static PgAioCtl {
    PGAIO_CTL
        .get()
        .expect("pgaio_ctl shared memory not initialized (AioShmemInit not called)")
}

/// `dclist_init(dclist_head *head)` (`lib/ilist.h`).
fn dclist_init(head: &mut DclistHead) {
    head.members.clear();
    head.count = 0;
}

/// `dclist_push_tail(dclist_head *head, dlist_node *node)` (`lib/ilist.h`).
fn dclist_push_tail(head: &mut DclistHead, node_index: usize) -> PgResult<()> {
    head.members
        .try_reserve(1)
        .map_err(|_| oom_error("dclist_push_tail"))?;
    head.members.push(node_index);
    head.count += 1;
    debug_assert!(head.count > 0); // count overflow check
    Ok(())
}

/// `ConditionVariableInit(ConditionVariable *cv)` (`condition_variable.c`):
/// `SpinLockInit(&cv->mutex)` + `proclist_init(&cv->wakeup)` — re-initialize in
/// place.
fn ConditionVariableInit(cv: &mut ConditionVariable) {
    *cv = ConditionVariable::new();
}

/// Grow `vec` to exactly `n` elements built by `make`, OOM-safely. `n` is one of
/// the AIO sizing products (each computed through the overflow-checked `mul_size`
/// seam), so it is a validated bound.
fn grow_with<T>(vec: &mut Vec<T>, n: usize, mut make: impl FnMut() -> T) -> PgResult<()> {
    vec.try_reserve(n).map_err(|_| oom_error("AioShmemInit"))?;
    for _ in 0..n {
        vec.push(make());
    }
    Ok(())
}

/// Build the out-of-memory error the shmem allocator would `ereport`.
fn oom_error(where_: &str) -> PgError {
    PgError::error(alloc::format!(
        "out of memory while initializing AIO shared memory ({where_})"
    ))
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seams (the AIO shmem-init + per-backend-init
/// entry points ipci.c/postinit reach) and the `io_method`/`io_max_concurrency`
/// GUC variables + hooks this unit owns (mirroring aio.c).
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{hooks, option_sets, vars, GucVarAccessors};

    backend_storage_aio_seams::aio_shmem_size::set(AioShmemSize);
    backend_storage_aio_seams::aio_shmem_init::set(AioShmemInit);
    backend_storage_aio_seams::pgaio_init_backend::set(pgaio_init_backend);

    // io_method_options[] + the io_method enum variable accessor + assign hook.
    option_sets::io_method_options.install(IO_METHOD_OPTIONS);
    vars::io_method.install(GucVarAccessors {
        get: current_io_method,
        set: |_v| { /* the resolver reads the live store; nothing to cache. */ },
    });
    hooks::assign_io_method.install(assign_io_method);

    // io_max_concurrency variable accessor + check hook.
    vars::io_max_concurrency.install(GucVarAccessors {
        get: io_max_concurrency,
        set: |_v| { /* read through the live store. */ },
    });
    hooks::check_io_max_concurrency.install(check_io_max_concurrency);
}

#[cfg(test)]
mod tests;
