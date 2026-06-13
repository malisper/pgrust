//! The shared `LogicalRepCtxStruct` control block and `LogicalRepWorker[]`
//! slot array.
//!
//! In C these live in shared memory carved by `ShmemInitStruct` and are
//! interlocked by `LWLock(LogicalRepWorkerLock)`. Per AGENTS.md "Backend-global
//! state", genuinely cross-backend state is ported as an explicitly shared,
//! synchronized type. The launcher owns it, so it lives here as a process
//! global behind a `Mutex`: the LWLock seam still provides cross-backend
//! exclusion at the same points C acquires it, and the `Mutex` gives Rust-level
//! data-race freedom for the single-process model. The slot array is sized once
//! by [`initialize`], matching C's flexible-member layout from
//! `ApplyLauncherShmemSize()`.

use std::cell::Cell;
use std::sync::{Mutex, OnceLock};

use types_storage::storage::{
    dsa_handle, dshash_table_handle, DSM_HANDLE_INVALID, INVALID_DSA_POINTER,
};
use types_replication_launcher::{LogicalRepWorker, LogicalRepWorkerType};

/// `DSA_HANDLE_INVALID` (`utils/dsa.h`, `(dsa_handle) DSM_HANDLE_INVALID`).
const DSA_HANDLE_INVALID: dsa_handle = DSM_HANDLE_INVALID;
/// `DSHASH_HANDLE_INVALID` (`lib/dshash.h`, `(dshash_table_handle) InvalidDsaPointer`).
const DSHASH_HANDLE_INVALID: dshash_table_handle = INVALID_DSA_POINTER;

/// `LogicalRepCtxStruct` (launcher.c) — the fixed shmem header.
pub struct LogicalRepCtx {
    /// Supervisor process PID (`launcher_pid`).
    pub launcher_pid: i32,
    /// `dsa_handle last_start_dsa` — handle for the last-start-times DSA area.
    pub last_start_dsa: dsa_handle,
    /// `dshash_table_handle last_start_dsh` — handle for the dshash table.
    pub last_start_dsh: dshash_table_handle,
    /// `LogicalRepWorker workers[FLEXIBLE_ARRAY_MEMBER]` — the slot array.
    pub workers: Vec<LogicalRepWorker>,
}

impl LogicalRepCtx {
    fn new(nworkers: usize) -> Self {
        LogicalRepCtx {
            launcher_pid: 0,
            last_start_dsa: DSA_HANDLE_INVALID,
            last_start_dsh: DSHASH_HANDLE_INVALID,
            workers: (0..nworkers).map(|_| zeroed_worker()).collect(),
        }
    }
}

/// A `memset(worker, 0, sizeof(LogicalRepWorker))` slot — all-zero / not in
/// use, exactly as `ApplyLauncherShmemInit` initializes each slot.
fn zeroed_worker() -> LogicalRepWorker {
    LogicalRepWorker {
        wtype: LogicalRepWorkerType::Unknown,
        launch_time: 0,
        in_use: false,
        generation: 0,
        proc_pid: None,
        dbid: 0,
        userid: 0,
        subid: 0,
        relid: 0,
        relstate: 0,
        relstate_lsn: 0,
        leader_pid: 0,
        parallel_apply: false,
        last_lsn: 0,
        last_send_time: 0,
        last_recv_time: 0,
        reply_lsn: 0,
        reply_time: 0,
    }
}

static LOGICAL_REP_CTX: OnceLock<Mutex<LogicalRepCtx>> = OnceLock::new();

/// `ApplyLauncherShmemInit` — create the control block with `nworkers` slots.
/// Idempotent (C's `ShmemInitStruct` returns `found` on a second call).
pub fn initialize(nworkers: usize) {
    let _ = LOGICAL_REP_CTX.set(Mutex::new(LogicalRepCtx::new(nworkers)));
}

/// Lazily access the shared control block. The launcher always initializes it
/// via `ApplyLauncherShmemInit` before any worker operation; if a caller races
/// ahead, create an empty (zero-slot) block so reads are well-defined rather
/// than panicking — mirroring C reading an all-zero shmem region.
fn lock_ctx() -> std::sync::MutexGuard<'static, LogicalRepCtx> {
    LOGICAL_REP_CTX
        .get_or_init(|| Mutex::new(LogicalRepCtx::new(0)))
        .lock()
        .expect("LogicalRepCtx mutex poisoned")
}

/// Run `f` with mutable access to the control-block header + slots.
pub fn with_ctx<R>(f: impl FnOnce(&mut LogicalRepCtx) -> R) -> R {
    f(&mut lock_ctx())
}

/// Run `f` with mutable access to the worker slot array.
pub fn with_workers<R>(f: impl FnOnce(&mut [LogicalRepWorker]) -> R) -> R {
    let mut ctx = lock_ctx();
    f(&mut ctx.workers)
}

thread_local! {
    /// `LogicalRepWorker *MyLogicalRepWorker` — the slot index this backend
    /// attached to (set in `logicalrep_worker_attach`), or `None` (C NULL).
    /// Per-backend, so thread_local.
    static MY_LOGICAL_REP_WORKER_SLOT: Cell<Option<i32>> = const { Cell::new(None) };
}

/// Accessor for the per-backend `MyLogicalRepWorker` slot index cell.
pub fn my_logical_rep_worker_slot() -> &'static std::thread::LocalKey<Cell<Option<i32>>> {
    &MY_LOGICAL_REP_WORKER_SLOT
}
