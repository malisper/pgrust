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
//! ## The engine + its aio-owned satellite files (now ported)
//!
//! The AIO *engine* (`aio.c`'s full `PgAioHandle` state machine) plus its
//! aio-owned satellite source files are ported here as submodules:
//!  * [`aio`] — `aio.c`: `pgaio_io_acquire`/`_nb`, `pgaio_io_release`,
//!    `pgaio_io_release_resowner`, `pgaio_io_stage`, `pgaio_io_prepare_submit`,
//!    `pgaio_io_process_completion`, `pgaio_io_wait`/`pgaio_wref_wait`,
//!    `pgaio_io_reclaim`, `pgaio_io_wait_for_free`, the wref + batch-mode +
//!    `pgaio_submit_staged` + `pgaio_error_cleanup` + `AtEOXact_Aio` +
//!    `pgaio_closing_fd` + `pgaio_shutdown` surfaces;
//!  * [`aio_callback`] — `aio_callback.c`: callback registration + the
//!    stage/complete-shared/complete-local/report dispatch loops;
//!  * [`aio_target`] — `aio_target.c`: the target registry + dispatch;
//!  * [`aio_io`] — `aio_io.c`: the per-op start routines, the synchronous
//!    executor, and the op/fd helpers;
//!  * [`aio_funcs`] — `aio_funcs.c`: the `pg_get_aios()` SQL set-returning
//!    function that introspects every in-flight AIO handle.
//!
//! These engine entry points are installed across the three per-consumer seam
//! crates the VFD / xact / resowner call sites reach (`-seams`, `-aio-seams`,
//! `-core-seams`), clearing the `pgaio_closing_fd` boot wall.
//!
//! ## Crate ↔ c2rust-unit correspondence (relocation deferred)
//!
//! The c2rust canonical layout places `aio.c` / `aio_callback.c` / `aio_io.c` /
//! `aio_funcs.c` in a `backend-storage-aio-core` crate and `aio_init.c` /
//! `aio_target.c` / `method_sync.c` / `method_worker.c` in this
//! `backend-storage-aio-methods` crate. This port keeps the engine files
//! (`aio` / `aio_callback` / `aio_io` / `aio_funcs`) here alongside the
//! `aio_internal.h` shared-memory model (`aio_init.c`) **deliberately**: a true
//! crate split would form an *unbreakable circular crate dependency*. The
//! engine (`aio.c`) is the primary mutator of the shared-memory model that lives
//! with `aio_init.c` (`pgaio_ctl`, `pgaio_my_backend`, the `dclist_*` helpers,
//! `PgAioCtl` / `PgAioHandle` / `PgAioBackend`, `pgaio_method_ops`,
//! `io_max_concurrency`, `clear_pgaio_my_backend`) → `-core` would depend on
//! `-methods`. But `aio_init.c`'s `pgaio_init_backend` registers
//! `aio::pgaio_shutdown` as a `before_shmem_exit` hook and this crate's
//! `init_seams()` installs ~7 engine entry points (`pgaio_closing_fd`,
//! `pgaio_error_cleanup`, `AtEOXact_Aio`, `pgaio_io_release_resowner`,
//! `pgaio_io_start_readv`) → `-methods` would depend on `-core`. In C these
//! share one `aio_internal.h` header; across a Rust crate boundary the two
//! directions are a cycle Cargo rejects. Breaking it would require extracting
//! the `aio_internal.h` shmem model into a *third* crate below both and
//! relocating the GUC-table ownership + seam-install orchestration — high-churn
//! structural surgery touching the data model shared by `aio_init.c` /
//! `method_sync.c` / `aio_target.c` and the engine, plus tests. That is out of
//! scope for a fidelity pass, so the engine files stay co-located with the
//! model they mutate. (`read_stream.c` and `method_io_uring.c`, the other
//! c2rust `-core` members, are already separate crates / seam-and-panic stubs.)
//!
//! ## What is NOT here (seam-and-panic into genuinely-unported owners)
//!
//! The leaves that bottom out in subsystems unported *for AIO* are seamed (see
//! [`completion_seams`]): the buffer-manager / md.c
//! completion callbacks (`pgaio_cb_*`), the smgr/fd synchronous read/write
//! syscall (`pgaio_perform_io_syscall`), the smgr target reopen
//! (`pgaio_io_reopen`), and the resource-owner AIO-handle registry
//! (`resource_owner_*_aio_handle`). They are reached only on the async /
//! buffered-IO completion path, never on the `io_method = sync` boot path.
//!
//! The **worker IO method** (`method_worker.c`, PG 18's *default* `io_method =
//! worker`) is now ported in [`method_worker`]: the `AioWorkerSubmissionQueue`
//! ring + `AioWorkerControl` block in real cross-process `ShmemInitStruct`
//! shmem, the `pgaio_worker_submit` enqueue-and-wake path, and the
//! `IoWorkerMain` aux-process loop (the postmaster already spawns/reaps the
//! `io_workers` children; this installs the loop they run via the
//! `io_worker_main` seam). `method_io_uring.c` remains seam-and-panic (no
//! liburing FFI binding crate). The synchronous method (`method_sync.c`) makes
//! every IO execute inline via `pgaio_io_perform_synchronously`;
//! `pgaio_sync_submit` faithfully reproduces the C `elog(ERROR, "IO should have
//! been executed synchronously")` (never reached, as
//! `needs_synchronous_execution` returns true).

extern crate alloc;

use alloc::string::ToString;
use alloc::vec::Vec;

use core::cell::Cell;
use core::cell::RefCell;
use core::sync::atomic::{AtomicI32, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

use ::condvar::ConditionVariable;
use ::types_core::primitive::Size;
use ::types_error::{PgError, PgResult};
use ::types_resowner::ResourceOwner;
use ::types_storage::storage::NUM_AUXILIARY_PROCS;

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

/// `PGAIO_HF_SYNCHRONOUS = 1 << 0` (`storage/aio.h`) — IO will run synchronously.
pub const PGAIO_HF_SYNCHRONOUS: u8 = 1 << 0;
/// `PGAIO_HF_REFERENCES_LOCAL = 1 << 1` (`storage/aio.h`).
pub const PGAIO_HF_REFERENCES_LOCAL: u8 = 1 << 1;
/// `PGAIO_HF_BUFFERED = 1 << 2` (`storage/aio.h`).
pub const PGAIO_HF_BUFFERED: u8 = 1 << 2;

/// `PGAIO_OP_INVALID = 0` (`storage/aio.h`).
pub const PGAIO_OP_INVALID: u8 = 0;
/// `PGAIO_OP_READV` (`storage/aio.h`).
pub const PGAIO_OP_READV: u8 = 1;
/// `PGAIO_OP_WRITEV` (`storage/aio.h`).
pub const PGAIO_OP_WRITEV: u8 = 2;

/// `PGAIO_TID_INVALID = 0` (`storage/aio.h`).
pub const PGAIO_TID_INVALID: u8 = 0;
/// `PGAIO_TID_SMGR` (`storage/aio.h`).
pub const PGAIO_TID_SMGR: u8 = 1;

/// `PG_UINT32_MAX` — the invalid `aio_index` sentinel for a cleared wait ref.
pub const PG_UINT32_MAX: u32 = u32::MAX;

/// `PG_IOV_MAX` (`port/pg_iovec.h`) — `Min(IOV_MAX, 128)`. The maximum number of
/// iovec entries a single handle's scatter/gather array can hold; returned by
/// `pgaio_io_get_iovec` as the capacity of the handle's iovec sub-range. On every
/// supported platform `IOV_MAX >= 128`, so this evaluates to 128.
pub const PG_IOV_MAX: usize = 128;

/// `WAIT_EVENT_AIO_IO_COMPLETION` (`utils/wait_event_names.txt`) — the wait
/// event reported while sleeping on a handle's completion condition variable.
pub const WAIT_EVENT_AIO_IO_COMPLETION: u32 = 0x0B00_0006;

/// `typedef struct PgAioWaitRef` (`storage/aio_types.h`) — a process-portable
/// reference to a specific IO handle + the generation that referenced it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAioWaitRef {
    /// `uint32 aio_index` — index into `pgaio_ctl->io_handles`.
    pub aio_index: u32,
    /// `uint32 generation_upper` — high 32 bits of the handle generation.
    pub generation_upper: u32,
    /// `uint32 generation_lower` — low 32 bits of the handle generation.
    pub generation_lower: u32,
}

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

impl PgAioHandleState {
    /// Decode the `repr(u8)` stored in the handle's atomic `state` field.
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => PgAioHandleState::Idle,
            1 => PgAioHandleState::HandedOut,
            2 => PgAioHandleState::Defined,
            3 => PgAioHandleState::Staged,
            4 => PgAioHandleState::Submitted,
            5 => PgAioHandleState::CompletedIo,
            6 => PgAioHandleState::CompletedShared,
            7 => PgAioHandleState::CompletedLocal,
            other => panic!("invalid PgAioHandleState discriminant {other}"),
        }
    }
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

/// `struct ResourceOwnerData *resowner` (`utils/resowner.c`) — the value-typed
/// resource-owner handle (`::types_resowner::ResourceOwner`), `None` until an
/// owner is set. The resowner AIO integration (`pgaio_io_resowner_register`)
/// threads this through the `resource_owner_remember/forget_aio_handle` seams.
pub type ResourceOwnerId = ResourceOwner;

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
/// In PostgreSQL the AIO control struct lives in shared memory and the engine
/// mutates handle fields through a `PgAioHandle *` while every backend can read
/// the cross-visible fields (`state`, `generation`, `result`) concurrently with
/// `pg_read_barrier`/`pg_write_barrier`. This workspace models shared memory as
/// a single process-global [`OnceLock`] reached through `&'static`; mutation is
/// therefore expressed as *interior mutability* on the handle fields (the same
/// idiom `procsignal.c`/`pmsignal.c` use). The three cross-backend-visible
/// scalar fields (`state`, `result`, `generation`) become atomics — their
/// `Acquire`/`Release` orderings reproduce the C read/write barriers exactly —
/// and the remaining single-owner mutable fields live behind one per-handle
/// [`Mutex`] ([`PgAioHandleData`]), the same shmem-faithful guard `procsignal.c`
/// uses for its per-slot `pss_mutex` (a bare `Cell` would make the handle
/// `!Sync`, but the process-global `OnceLock<PgAioCtl>` must be `Sync`).
#[derive(Debug)]
pub struct PgAioHandle {
    /// `uint8 state` — a [`PgAioHandleState`]. Cross-backend-visible: written
    /// `Release` (the C `pg_write_barrier(); ioh->state = new_state`), read
    /// `Acquire`.
    pub state: AtomicU8,
    /// `int32 owner_procno` — set once at init, never mutated afterwards.
    pub owner_procno: i32,
    /// `int32 result` — raw result of the IO operation. Cross-backend-visible.
    pub result: AtomicI32,
    /// `uint64 generation` — incremented every time the handle is reused.
    /// Cross-backend-visible: read `Acquire` after the `state` load.
    pub generation: AtomicU64,
    /// `ConditionVariable cv` — already interior-mutable.
    pub cv: ConditionVariable,
    /// `uint32 iovec_off` — index into `PgAioCtl.iovecs`/`.handle_data`. Set once
    /// at init.
    pub iovec_off: u32,
    /// The single-owner mutable handle fields (`target`/`op`/`flags`/
    /// `num_callbacks`/`callbacks`/`callbacks_data`/`handle_data_len`/`resowner`/
    /// `distilled_result`/`report_return`/`op_data`/`target_data`). Only the
    /// owning backend mutates these, but `&'static` sharing requires `Sync`, so
    /// they sit behind one per-handle [`Mutex`].
    pub data: Mutex<PgAioHandleData>,
}

/// The single-owner mutable portion of a [`PgAioHandle`] (see its `data` field).
#[derive(Clone, Debug)]
pub struct PgAioHandleData {
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
    /// `struct ResourceOwnerData *resowner` — `None` until an owner is set.
    pub resowner: Option<ResourceOwnerId>,
    /// `PgAioResult distilled_result`.
    pub distilled_result: PgAioResult,
    /// `PgAioReturn *report_return` — `None` until a return location is set.
    pub report_return: Option<PgAioReturn>,
    /// `PgAioOpData op_data`.
    pub op_data: PgAioOpData,
    /// `PgAioTargetData target_data`.
    pub target_data: PgAioTargetData,
}

impl Default for PgAioHandleData {
    fn default() -> Self {
        PgAioHandleData {
            target: 0,
            op: 0,
            flags: 0,
            num_callbacks: 0,
            callbacks: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            callbacks_data: [0; PGAIO_HANDLE_MAX_CALLBACKS],
            handle_data_len: 0,
            resowner: None,
            distilled_result: PgAioResult::default(),
            report_return: None,
            op_data: PgAioOpData::default(),
            target_data: PgAioTargetData::default(),
        }
    }
}

impl PgAioHandle {
    /// A freshly zeroed handle (the C `memset(pgaio_ctl, 0, ...)` baseline for
    /// one handle, before `AioShmemInit` fills generation/owner/iovec_off/cv).
    fn zeroed() -> Self {
        PgAioHandle {
            state: AtomicU8::new(PgAioHandleState::Idle as u8),
            owner_procno: 0,
            result: AtomicI32::new(0),
            generation: AtomicU64::new(0),
            cv: ConditionVariable::new(),
            iovec_off: 0,
            data: Mutex::new(PgAioHandleData::default()),
        }
    }

    /// Load `state` with `Acquire` (the C read-barrier-before-state-use).
    pub fn state(&self) -> PgAioHandleState {
        PgAioHandleState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// `pgaio_io_update_state`: `pg_write_barrier(); ioh->state = new_state`.
    pub fn set_state(&self, new: PgAioHandleState) {
        self.state.store(new as u8, Ordering::Release);
    }

    /// Lock and access the single-owner mutable portion of the handle.
    pub fn data(&self) -> std::sync::MutexGuard<'_, PgAioHandleData> {
        self.data.lock().unwrap()
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
    /// `PgAioBackend *backend_state`. Each `PgAioBackend` is single-owner in C
    /// (only the owning backend touches its lists / `num_staged_ios` /
    /// `handed_out_io` / `in_batchmode`), so the mutable state lives behind a
    /// per-slot [`Mutex`] — the same shmem-faithful guard `procsignal.c` uses for
    /// its per-slot `pss_mutex`.
    pub backend_state: Vec<Mutex<PgAioBackend>>,
    /// `uint32 iovec_count`.
    pub iovec_count: u32,
    /// `struct iovec *iovecs`. Each handle's iovec sub-range (`[iovec_off ..
    /// iovec_off+io_max_combine_limit]`) is filled by its owning backend before
    /// staging; `&'static` sharing requires `Sync`, so the array sits behind a
    /// [`Mutex`].
    pub iovecs: Mutex<Vec<Iovec>>,
    /// `uint64 *handle_data`. Same single-owner sub-range model as `iovecs`.
    pub handle_data: Mutex<Vec<u64>>,
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
    /// `int (*submit)(uint16 num_staged_ios, PgAioHandle **staged_ios)` — the
    /// staged io-handle indices (`pgaio_my_backend->staged_ios[0..num]`).
    pub submit: Option<fn(staged_ios: &[usize]) -> PgResult<i32>>,
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
fn pgaio_sync_submit(_staged_ios: &[usize]) -> PgResult<i32> {
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

/// `const IoMethodOps pgaio_worker_ops` (method_worker.c) — the worker IO
/// method, now ported in [`method_worker`]. A cluster running the default
/// `io_method = worker` reaches its `shmem_size`/`shmem_init` from
/// `AioShmemSize`/`AioShmemInit` (the worker method reserves the shmem
/// submission queue + control block) and its `submit` /
/// `needs_synchronous_execution` from the engine staging path.
fn pgaio_worker_ops() -> IoMethodOps {
    method_worker::pgaio_worker_ops()
}

/// The `IoMethodOps.submit` trampoline for the worker method — the ops table
/// holds a `fn` pointer, so this thin free function bridges to the module's
/// `pgaio_worker_submit`.
pub(crate) fn pgaio_worker_submit_bridge(staged_ios: &[usize]) -> PgResult<i32> {
    method_worker::pgaio_worker_submit(staged_ios)
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
pub(crate) fn pgaio_method_ops() -> IoMethodOps {
    pgaio_method_ops_for(current_io_method())
}

// ===========================================================================
// GUC ownership: int io_method / int io_max_concurrency (aio.c globals)
// ===========================================================================

/// Read `io_method` from the live GUC store (the C `*conf->variable`). At boot,
/// before any SET, this returns the compiled-in boot value
/// (`DEFAULT_IO_METHOD`).
fn current_io_method() -> i32 {
    misc_guc::get_enum("io_method").unwrap_or(DEFAULT_IO_METHOD)
}

/// Read `io_max_concurrency` from the live GUC store (the C `io_max_concurrency`
/// global). `-1` (the boot value) means "auto-tune later".
pub(crate) fn io_max_concurrency() -> i32 {
    misc_guc::get_int("io_max_concurrency").unwrap_or(-1)
}

/// Read `io_max_combine_limit` from the live GUC store. This GUC is owned by
/// bufmgr.c; `AioShmemSize`/`AioShmemInit` read it to size the iovec and
/// handle-data arrays. The live store returns its boot value even before
/// bufmgr installs its variable accessor.
fn io_max_combine_limit() -> i32 {
    misc_guc::get_int("io_max_combine_limit")
        .expect("io_max_combine_limit GUC not registered (initialize_guc_options not called)")
}

/// Read `io_workers` from the live GUC store. In C this is the plain global
/// `int io_workers = 3` (method_worker.c) which is *itself* the storage cell the
/// `io_workers` GUC's `&io_workers` points at, so reading the GUC slot is reading
/// the variable. The worker IO method (method_worker.c) that consumes it is now
/// ported ([`method_worker`]); this accessor owns the variable so the GUC
/// machinery (SIGHUP reload, SHOW) and the postmaster's `maybe_adjust_io_workers`
/// read/write the right cell. The boot value is `3`.
fn io_workers() -> i32 {
    misc_guc::get_int("io_workers").unwrap_or(3)
}

/// `void assign_io_method(int newval, void *extra)` (aio.c) — set
/// `pgaio_method_ops` from the table. Here `pgaio_method_ops` is resolved on
/// demand, so the assign hook only validates the index (the C asserts the table
/// entry is non-NULL).
fn assign_io_method(newval: i32, _extra: Option<&::guc_tables::GucHookExtra>) {
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
    _extra: &mut Option<::guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    if *newval == -1 {
        // Auto-tuning will be applied later during startup.
        return Ok(true);
    } else if *newval == 0 {
        guc_seams::guc_check_errdetail::call(alloc::string::String::from(
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

thread_local! {
    /// The process-local `PgAioBackend *pgaio_my_backend` (aio.c) — the index
    /// into `pgaio_ctl->backend_state` for this backend, or `None` before
    /// `pgaio_init_backend` runs / in subprocesses that don't use AIO. Modeled
    /// as a `thread_local` [`Cell`], the same idiom procsignal uses for
    /// `MyProcSignalSlot`.
    static PGAIO_MY_BACKEND: Cell<Option<usize>> = const { Cell::new(None) };

    /// The issuer-owned `PgAioReturn`s for IOs this backend has issued whose
    /// result has been published by `pgaio_io_reclaim` but not yet consumed by
    /// the issuer's `pgaio_wref_wait`.
    ///
    /// In C, `pgaio_io_acquire(resowner, ret)` records the caller's
    /// `PgAioReturn *ret` (e.g. `&operation->io_return`) on the handle's
    /// `report_return`; the completion path (`pgaio_io_reclaim`) writes the
    /// distilled result through that pointer into the caller's own storage,
    /// which then outlives the handle's recycle. Crucially, each in-flight IO
    /// instance owns a DISTINCT `io_return` (e.g. `read_stream.c` keeps a
    /// separate `ReadBuffersOperation` — hence a separate `io_return` — for
    /// every `InProgressIO` slot), so several completed-but-not-yet-waited
    /// results can coexist when reads are issued ahead (the read-ahead
    /// pipeline). Under `io_method = sync` each read completes and is reclaimed
    /// inline within `start_read_buffers`, publishing its result before the
    /// NEXT read is started, so multiple unconsumed results pile up here until
    /// each operation's `WaitReadBuffers` claims its own.
    ///
    /// The value-typed model stores `report_return` by value on the handle and
    /// clears it on recycle, so the completed result would be lost to the
    /// issuer. This backend-local map mirrors C's per-operation slots: each
    /// completion publishes under the handle instance's globally-unique
    /// `generation` (the same generation carried in the issuer's `PgAioWaitRef`
    /// at submit time, before `pgaio_io_reclaim` bumps it), and the buffer-read
    /// `wait` seam consumes by that exact generation. A single `aio_index`-keyed
    /// cell is INCORRECT: the handle is recycled to the same index across reads,
    /// so a later read's result would clobber an earlier, still-unwaited read's
    /// result (observed as `process_read_buffers_result` advancing
    /// `nblocks_done` past `operation.buffers.len()` during VACUUM/scan of a
    /// large relation).
    static PGAIO_PENDING_RETURNS: RefCell<Vec<(u32, u64, PgAioReturn)>> =
        const { RefCell::new(Vec::new()) };
}

/// Publish the issuer-owned `PgAioReturn` for a just-completed IO instance,
/// keyed by `(aio_index, generation)` (`pgaio_io_reclaim` writing through C's
/// `report_return` pointer into the caller's slot). See [`PGAIO_PENDING_RETURNS`].
pub(crate) fn set_pgaio_last_return(aio_index: u32, generation: u64, ret: PgAioReturn) {
    PGAIO_PENDING_RETURNS.with(|c| {
        let mut v = c.borrow_mut();
        // Each completed IO instance has a UNIQUE (aio_index, generation), so this
        // is normally a fresh push. We must NOT prune other entries on the same
        // index here: with read-ahead, an earlier read on this (now-recycled)
        // index can still be sitting completed-but-not-yet-waited while this later
        // read on the same index completes — dropping it would lose the earlier
        // read's result (the very bug this map fixes). Entries are removed only by
        // `take` when their issuer waits; the map size is bounded by the live
        // read-ahead depth (a handful of in-flight ReadStream IOs). The find guards
        // against a (re)publish of the same instance.
        if let Some(slot) = v.iter_mut().find(|(i, g, _)| *i == aio_index && *g == generation) {
            slot.2 = ret;
        } else {
            v.push((aio_index, generation, ret));
        }
    });
}

/// Read back (and remove) the issuer-owned `PgAioReturn` published for the IO
/// instance identified by `(aio_index, generation)`, or `None` if no completion
/// has been recorded for it. See [`PGAIO_PENDING_RETURNS`].
pub(crate) fn take_pgaio_last_return(aio_index: u32, generation: u64) -> Option<PgAioReturn> {
    PGAIO_PENDING_RETURNS.with(|c| {
        let mut v = c.borrow_mut();
        if let Some(pos) = v.iter().position(|(idx, g, _)| *idx == aio_index && *g == generation) {
            Some(v.swap_remove(pos).2)
        } else {
            None
        }
    })
}

/// `pgaio_my_backend` accessor returning the per-backend index, or `None` when
/// AIO isn't initialized for this process (`!pgaio_my_backend` in C).
pub(crate) fn pgaio_my_backend() -> Option<usize> {
    PGAIO_MY_BACKEND.with(|c| c.get())
}

/// Clear `pgaio_my_backend` (the C `pgaio_my_backend = NULL` at shutdown).
pub(crate) fn clear_pgaio_my_backend() {
    PGAIO_MY_BACKEND.with(|c| c.set(None));
}

/// `static Size AioCtlShmemSize(void)` (aio_init.c) — `sizeof(PgAioCtl)`.
fn AioCtlShmemSize() -> Size {
    core::mem::size_of::<PgAioCtl>()
}

/// `static uint32 AioProcs(void)` (aio_init.c) — `MaxBackends + NUM_AUXILIARY_PROCS`.
fn AioProcs() -> u32 {
    (init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS) as u32
}

/// `static Size AioBackendShmemSize(void)` (aio_init.c).
fn AioBackendShmemSize() -> PgResult<Size> {
    ipc_shmem_seams::mul_size::call(
        AioProcs() as Size,
        core::mem::size_of::<PgAioBackend>(),
    )
}

/// `static Size AioHandleShmemSize(void)` (aio_init.c).
fn AioHandleShmemSize() -> PgResult<Size> {
    // Assert(io_max_concurrency > 0) — AioChooseMaxConcurrency ran already.
    debug_assert!(io_max_concurrency() > 0);
    let inner = ipc_shmem_seams::mul_size::call(
        io_max_concurrency() as Size,
        core::mem::size_of::<PgAioHandle>(),
    )?;
    ipc_shmem_seams::mul_size::call(AioProcs() as Size, inner)
}

/// `static Size AioHandleIOVShmemSize(void)` (aio_init.c).
fn AioHandleIOVShmemSize() -> PgResult<Size> {
    // each IO handle can have up to io_max_combine_limit iovec objects
    let combine_x_procs = ipc_shmem_seams::mul_size::call(
        io_max_combine_limit() as Size,
        AioProcs() as Size,
    )?;
    let inner =
        ipc_shmem_seams::mul_size::call(combine_x_procs, io_max_concurrency() as Size)?;
    ipc_shmem_seams::mul_size::call(core::mem::size_of::<Iovec>(), inner)
}

/// `static Size AioHandleDataShmemSize(void)` (aio_init.c).
fn AioHandleDataShmemSize() -> PgResult<Size> {
    // each buffer referenced by an iovec can have associated data
    let combine_x_procs = ipc_shmem_seams::mul_size::call(
        io_max_combine_limit() as Size,
        AioProcs() as Size,
    )?;
    let inner =
        ipc_shmem_seams::mul_size::call(combine_x_procs, io_max_concurrency() as Size)?;
    ipc_shmem_seams::mul_size::call(core::mem::size_of::<u64>(), inner)
}

/// `static int AioChooseMaxConcurrency(void)` (aio_init.c).
fn AioChooseMaxConcurrency() -> i32 {
    // Similar logic to LimitAdditionalPins().
    let max_backends: u32 =
        (init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS) as u32;
    let mut max_proportional_pins: i32 =
        init_small_seams::nbuffers::call() / max_backends as i32;
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
        guc_seams::set_config_option::call(
            "io_max_concurrency",
            &buf,
            types_guc::GucContext::PGC_POSTMASTER,
            types_guc::GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;
        if io_max_concurrency() == -1 {
            // failed to apply it?
            guc_seams::set_config_option::call(
                "io_max_concurrency",
                &buf,
                types_guc::GucContext::PGC_POSTMASTER,
                types_guc::GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }

    sz = ipc_shmem_seams::add_size::call(sz, AioCtlShmemSize())?;
    sz = ipc_shmem_seams::add_size::call(sz, AioBackendShmemSize()?)?;
    sz = ipc_shmem_seams::add_size::call(sz, AioHandleShmemSize()?)?;
    sz = ipc_shmem_seams::add_size::call(sz, AioHandleIOVShmemSize()?)?;
    sz = ipc_shmem_seams::add_size::call(sz, AioHandleDataShmemSize()?)?;

    // Reserve space for method specific resources.
    let ops = pgaio_method_ops();
    if let Some(method_size) = ops.shmem_size {
        sz = ipc_shmem_seams::add_size::call(sz, method_size()?)?;
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
    grow_with(&mut pgaio_ctl.backend_state, aio_procs as usize, || {
        Mutex::new(PgAioBackend::default())
    })?;
    grow_with(
        &mut pgaio_ctl.io_handles,
        pgaio_ctl.io_handle_count as usize,
        PgAioHandle::zeroed,
    )?;
    grow_with(
        pgaio_ctl.iovecs.get_mut().unwrap(),
        pgaio_ctl.iovec_count as usize,
        Iovec::default,
    )?;
    grow_with(
        pgaio_ctl.handle_data.get_mut().unwrap(),
        pgaio_ctl.iovec_count as usize,
        u64::default,
    )?;

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

            ioh.generation = AtomicU64::new(1);
            ioh.owner_procno = procno;
            ioh.iovec_off = iovec_off;
            {
                let d = ioh.data.get_mut().unwrap();
                d.handle_data_len = 0;
                d.report_return = None;
                d.resowner = None;
                d.num_callbacks = 0;
                d.distilled_result.status = PgAioResultStatus::Unknown;
                d.flags = 0;
            }

            // ConditionVariableInit(&ioh->cv) — re-initialize in place.
            ConditionVariableInit(&mut ioh.cv);

            // dclist_push_tail(&bs->idle_ios, &ioh->node);
            dclist_push_tail(&mut idle_ios, ioh_index)?;
            iovec_off += imcl as u32;
        }

        let mut bs = pgaio_ctl.backend_state[procno as usize].lock().unwrap();
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

    if init_small_seams::my_backend_type::call()
        == ::types_core::init::BackendType::IoWorker
    {
        return Ok(());
    }

    // C: `if (MyProc == NULL || MyProcNumber >= AioProcs())`. `MyProcNumber`
    // reads `INVALID_PROC_NUMBER` (-1) when `MyProc == NULL`, so the NULL test
    // is the invalid-procnumber test plus the upper bound.
    let my_proc_number = init_small_seams::my_proc_number::call();
    if my_proc_number == ::types_core::primitive::INVALID_PROC_NUMBER
        || my_proc_number >= AioProcs() as i32
    {
        return Err(PgError::error("aio requires a normal PGPROC"));
    }

    // pgaio_my_backend = &pgaio_ctl->backend_state[MyProcNumber]; the C global
    // is a per-backend pointer into shmem. Here it is the process-local index
    // into `pgaio_ctl->backend_state`, stashed in the thread-local exactly as
    // procsignal stashes `MyProcSignalSlot`.
    let my_backend = my_proc_number as usize;
    let ctl = pgaio_ctl();
    let _ = &ctl.backend_state[my_backend];
    PGAIO_MY_BACKEND.with(|c| c.set(Some(my_backend)));

    let ops = pgaio_method_ops();
    if let Some(init_backend) = ops.init_backend {
        init_backend()?;
    }

    // before_shmem_exit(pgaio_shutdown, 0).
    dsm_core_seams::before_shmem_exit::call(
        aio::pgaio_shutdown,
        types_tuple::Datum::null(),
    )?;

    Ok(())
}

// ===========================================================================
// The `pgaio_ctl` global accessor + intrusive-list helpers (lib/ilist.h)
// ===========================================================================

/// The `pgaio_ctl != NULL` dereference: C would crash on use before
/// `AioShmemInit`; here it is a loud panic.
pub(crate) fn pgaio_ctl() -> &'static PgAioCtl {
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
pub(crate) fn dclist_push_tail(head: &mut DclistHead, node_index: usize) -> PgResult<()> {
    head.members
        .try_reserve(1)
        .map_err(|_| oom_error("dclist_push_tail"))?;
    head.members.push(node_index);
    head.count += 1;
    debug_assert!(head.count > 0); // count overflow check
    Ok(())
}

/// `dclist_push_head(dclist_head *head, dlist_node *node)` (`lib/ilist.h`).
pub(crate) fn dclist_push_head(head: &mut DclistHead, node_index: usize) -> PgResult<()> {
    head.members
        .try_reserve(1)
        .map_err(|_| oom_error("dclist_push_head"))?;
    head.members.insert(0, node_index);
    head.count += 1;
    debug_assert!(head.count > 0);
    Ok(())
}

/// `dclist_pop_head_node(dclist_head *head)` (`lib/ilist.h`) — remove and return
/// the head member index. Panics if empty (the C `Assert(!dclist_is_empty)`).
pub(crate) fn dclist_pop_head(head: &mut DclistHead) -> usize {
    let node = head.members.remove(0);
    head.count -= 1;
    node
}

/// `dclist_delete_from(dclist_head *head, dlist_node *node)` (`lib/ilist.h`) —
/// remove a specific member by its io-handle index.
pub(crate) fn dclist_delete_from(head: &mut DclistHead, node_index: usize) {
    let pos = head
        .members
        .iter()
        .position(|&x| x == node_index)
        .expect("dclist_delete_from: node not on list");
    head.members.remove(pos);
    head.count -= 1;
}

/// `dclist_is_empty(dclist_head *head)` (`lib/ilist.h`).
pub(crate) fn dclist_is_empty(head: &DclistHead) -> bool {
    head.count == 0
}

/// `dclist_count(dclist_head *head)` (`lib/ilist.h`). Part of the faithful
/// ilist surface; the C uses it only in `pgaio_debug*` log lines (elided here).
#[allow(dead_code)]
pub(crate) fn dclist_count(head: &DclistHead) -> u32 {
    head.count
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
    use ::guc_tables::{hooks, option_sets, vars, GucVarAccessors};

    aio_seams_2::aio_shmem_size::set(AioShmemSize);
    aio_seams_2::aio_shmem_init::set(AioShmemInit);
    aio_seams_2::pgaio_init_backend::set(pgaio_init_backend);

    // The postmaster's IO-worker scheduler (maybe_adjust_io_workers) reads
    // `pgaio_workers_enabled()` (`io_method == IOMETHOD_WORKER`), an aio.c
    // predicate owned here. (`io_workers` itself is read through the GUC slot by
    // the postmaster's own read seam.)
    postmaster_seams::pgaio_workers_enabled::set(|| {
        current_io_method() == IOMETHOD_WORKER
    });

    // The IO-worker aux-process entry point the postmaster's
    // `postmaster_child_launch` dispatches `B_IO_WORKER` children to
    // (`IoWorkerMain` in method_worker.c). The postmaster already spawns/reaps
    // io_workers; this installs the loop they run.
    methods_seams::io_worker_main::set(method_worker::io_worker_main);

    // The aio.c engine entry points the VFD / xact / resowner call sites reach.
    // The three per-consumer seam crates (`-seams` via vfd_core.rs, `-aio-seams`
    // via vfd_io.rs, `-core-seams` via allocated_desc.rs) each declare their own
    // `pgaio_closing_fd` / `pgaio_error_cleanup`; the single engine
    // implementation is installed into all of them.
    aio_seams_2::pgaio_closing_fd::set(aio::pgaio_closing_fd);
    aio_seams_2::pgaio_error_cleanup::set(aio::pgaio_error_cleanup);
    aio_seams_2::at_eoxact_aio::set(aio::AtEOXact_Aio);
    aio_seams::pgaio_closing_fd::set(aio::pgaio_closing_fd);
    aio_seams::pgaio_error_cleanup::set(aio::pgaio_error_cleanup);
    aio_seams::pgaio_io_start_readv::set(aio_io::pgaio_io_start_readv);
    aio_seams::pgaio_io_release_resowner::set(aio::pgaio_io_release_resowner);
    aio_core_seams::pgaio_closing_fd::set(aio::pgaio_closing_fd);
    aio_core_seams::pgaio_error_cleanup::set(aio::pgaio_error_cleanup);

    // The buffer manager's explicit multi-block read pipeline AIO handle seams
    // (bufmgr.c AsyncReadBuffers / WaitReadBuffers). The engine owns the
    // PgAioHandle lifecycle; the per-buffer page verification + TerminateBufferIO
    // completion callbacks + the synchronous read syscall live in the buffer
    // manager (installed there into the aio-completion seams).
    bufmgr_seams::pgaio_io_acquire::set(
        aio_buffer_read::pgaio_io_acquire_for_buffer_read,
    );
    bufmgr_seams::pgaio_register_callbacks::set(
        aio_buffer_read::pgaio_register_callbacks_for_buffer_read,
    );
    bufmgr_seams::start_read_buffers::set(
        aio_buffer_read::start_read_buffers_aio,
    );
    bufmgr_seams::wait_read_buffers::set(
        aio_buffer_read::wait_read_buffers_aio,
    );
    bufmgr_seams::wref_check_done::set(aio_buffer_read::wref_check_done_aio);

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

    // io_workers variable accessor (PGC_SIGHUP int, no hooks — guc_tables.c
    // lists check/assign/show all NULL). Owned by method_worker.c's `int
    // io_workers` global; resolved through the live store like the other AIO
    // GUCs.
    vars::io_workers.install(GucVarAccessors {
        get: io_workers,
        set: |_v| { /* read through the live store. */ },
    });
}

// The AIO engine + its aio-owned satellite source files.
pub mod aio;
pub mod aio_buffer_read;
pub mod aio_callback;
pub mod aio_funcs;
pub mod aio_io;
pub mod aio_target;
pub mod method_worker;

#[cfg(test)]
mod tests;
