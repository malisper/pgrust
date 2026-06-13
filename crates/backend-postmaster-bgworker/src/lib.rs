//! Background-worker registration and slot lifecycle
//! (`src/backend/postmaster/bgworker.c`).
//!
//! Background workers are processes the postmaster forks on behalf of an
//! extension, registered either statically (an extension's `_PG_init` calling
//! [`RegisterBackgroundWorker`] during `shared_preload_libraries` processing,
//! appended to the process-local [`BackgroundWorkerList`]) or dynamically (a
//! running backend calling [`RegisterDynamicBackgroundWorker`], which claims a
//! free slot in the shared `BackgroundWorkerArray` and signals the postmaster).
//!
//! ## Shared-state residency
//!
//! Two pieces of state differ in residency:
//!
//!   * **`BackgroundWorkerList`** — the postmaster's process-local registration
//!     list, built before shared memory exists and inherited at fork. Modeled
//!     as a `thread_local!` `Vec<RegisteredBgWorker>` (the Vec *is* the C
//!     `dlist`; the intrusive `rw_lnode` link is unused). Mutated only by the
//!     single-threaded postmaster (the per-backend rule: a C postmaster global
//!     is backend-private state).
//!   * **`BackgroundWorkerData`** — the shared `BackgroundWorkerArray` (header
//!     counters + flexible `BackgroundWorkerSlot slot[]`). It lives in real
//!     shared memory in C, accessed by both the postmaster (lock-free) and
//!     regular backends (under `BackgroundWorkerLock`). Modeled here as an
//!     explicitly shared, synchronized type (`Mutex<Option<…>>`, AGENTS.md
//!     "the only legitimately cross-thread state is what C keeps in shared
//!     memory — port that as explicitly shared, synchronized types"); the
//!     `Mutex` carries the `BackgroundWorkerLock` serialization the backends
//!     rely on. Sizing for `ShmemInitStruct` still goes through the shmem seam.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::sync::Mutex;

use backend_utils_error::emit_error_report_for;
use backend_utils_error_seams::ereport;
use types_error::ErrorLocation;
use types_bgworker::{
    BackgroundWorker, BackgroundWorkerHandle, BgWorkerStartTime, BgwHandleStatus,
    RegisteredBgWorker, BGWORKER_BACKEND_DATABASE_CONNECTION, BGWORKER_BYPASS_ALLOWCONN,
    BGWORKER_BYPASS_ROLELOGINCHECK, BGWORKER_CLASS_PARALLEL, BGWORKER_SHMEM_ACCESS, BGW_EXTRALEN,
    BGW_MAXLEN, BGW_NEVER_RESTART, INVALID_PID,
};
use types_core::{pid_t, uint32, uint64, Size, MAXPGPATH};
use types_error::{
    PgError, PgResult, DEBUG1, ERROR, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    FATAL, LOG,
};
use types_pgstat::wait_event::{WAIT_EVENT_BGWORKER_SHUTDOWN, WAIT_EVENT_BGWORKER_STARTUP};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

#[cfg(test)]
mod tests;

const FILE: &str = "bgworker.c";

fn loc(lineno: i32, funcname: &str) -> ErrorLocation {
    ErrorLocation::new(FILE, lineno, funcname)
}

/// `USECS_PER_DAY` (`datatype/timestamp.h`).
const USECS_PER_DAY: i64 = 86_400_000_000;

/// `INIT_PG_OVERRIDE_ALLOW_CONNS` (`miscadmin.h`) — `InitPostgres` flag bit.
const INIT_PG_OVERRIDE_ALLOW_CONNS: u32 = 0x0002;
/// `INIT_PG_OVERRIDE_ROLE_LOGIN` (`miscadmin.h`) — `InitPostgres` flag bit.
const INIT_PG_OVERRIDE_ROLE_LOGIN: u32 = 0x0004;

/// `offsetof(BackgroundWorkerArray, slot)` — the header preceding the flexible
/// slot array (`int total_slots; uint32 parallel_register_count; uint32
/// parallel_terminate_count;`) padded to the slot alignment: 16 bytes,
/// matching the C2Rust translation's literal `size = 16`.
const BGW_ARRAY_HEADER_SIZE: Size = 16;

/// `sizeof(BackgroundWorkerSlot)`: `bool in_use; bool terminate; pid_t pid;
/// uint64 generation; BackgroundWorker worker;`. Computed from field sizes to
/// stay faithful to the C `sizeof` (the substrate owns the real layout).
const BACKGROUND_WORKER_SLOT_SIZE: Size = {
    let worker = BGW_MAXLEN          // bgw_name
        + BGW_MAXLEN                 // bgw_type
        + 4                          // bgw_flags
        + 4                          // bgw_start_time
        + 4                          // bgw_restart_time
        + MAXPGPATH                  // bgw_library_name
        + BGW_MAXLEN                 // bgw_function_name
        + 8                          // bgw_main_arg (Datum)
        + BGW_EXTRALEN               // bgw_extra
        + 4; // bgw_notify_pid
    // Slot prefix: in_use(1) terminate(1) pad(2) pid(4) generation(8) = 16.
    16 + worker
};

// ---------------------------------------------------------------------------
// BackgroundWorkerList — the postmaster's process-local registration list.
// C: `dlist_head BackgroundWorkerList = DLIST_STATIC_INIT(...)`.
// ---------------------------------------------------------------------------

thread_local! {
    static BACKGROUND_WORKER_LIST: RefCell<Vec<RegisteredBgWorker>> = const { RefCell::new(Vec::new()) };
    /// `static int numworkers = 0;` inside `RegisterBackgroundWorker`.
    static NUMWORKERS: RefCell<i32> = const { RefCell::new(0) };
}

// ---------------------------------------------------------------------------
// BackgroundWorkerData — the shared BackgroundWorkerArray.
// ---------------------------------------------------------------------------

/// `BackgroundWorkerSlot` (`bgworker.c`).
#[derive(Clone, Copy)]
struct BackgroundWorkerSlot {
    in_use: bool,
    terminate: bool,
    /// `InvalidPid` = not started yet; 0 = dead.
    pid: pid_t,
    /// incremented when slot is recycled.
    generation: uint64,
    worker: BackgroundWorker,
}

impl BackgroundWorkerSlot {
    const fn empty() -> Self {
        BackgroundWorkerSlot {
            in_use: false,
            terminate: false,
            pid: 0,
            generation: 0,
            worker: BackgroundWorker::zeroed(),
        }
    }
}

/// `BackgroundWorkerArray` (`bgworker.c`): header counters plus the slot array.
struct BackgroundWorkerArray {
    total_slots: i32,
    parallel_register_count: uint32,
    parallel_terminate_count: uint32,
    slot: Vec<BackgroundWorkerSlot>,
}

/// `static BackgroundWorkerArray *BackgroundWorkerData;` — the shared array,
/// `None` until `BackgroundWorkerShmemInit` runs. The `Mutex` is the
/// `BackgroundWorkerLock` serialization (the data is genuinely cross-thread).
static BACKGROUND_WORKER_DATA: Mutex<Option<BackgroundWorkerArray>> = Mutex::new(None);

// ---------------------------------------------------------------------------
// Internal background-worker entry-point names (library "postgres").
// C: `static const struct { const char *fn_name; bgworker_main_type fn_addr; }
//     InternalBGWorkers[]`. The fn_addresses are resolved by the loader seam;
// what is portable here is the name table used to decide whether a
// "postgres"-library function name is a known internal entry point.
// ---------------------------------------------------------------------------

const INTERNAL_BGWORKER_NAMES: [&str; 5] = [
    "ParallelWorkerMain",
    "ApplyLauncherMain",
    "ApplyWorkerMain",
    "ParallelApplyWorkerMain",
    "TablesyncWorkerMain",
];

// ---------------------------------------------------------------------------
// BackgroundWorkerShmemSize / BackgroundWorkerShmemInit
// ---------------------------------------------------------------------------

/// `BackgroundWorkerShmemSize(void)` — compute size of our shared memory area.
pub fn BackgroundWorkerShmemSize() -> Size {
    // size = offsetof(BackgroundWorkerArray, slot);
    // size = add_size(size, mul_size(max_worker_processes,
    //                                sizeof(BackgroundWorkerSlot)));
    let max_worker_processes = backend_utils_init_small_seams::max_worker_processes::call();
    BGW_ARRAY_HEADER_SIZE + (max_worker_processes as Size) * BACKGROUND_WORKER_SLOT_SIZE
}

/// `BackgroundWorkerShmemInit(void)` — allocate and initialize our shared
/// memory area. When this process is the postmaster (`!IsUnderPostmaster`),
/// copy `BackgroundWorkerList` into the slot array and mark the rest free.
pub fn BackgroundWorkerShmemInit() -> PgResult<()> {
    // BackgroundWorkerData = ShmemInitStruct("Background Worker Data",
    //                                        BackgroundWorkerShmemSize(), &found);
    let size = BackgroundWorkerShmemSize();
    let (_addr, _found) =
        backend_storage_ipc_shmem_seams::shmem_init_struct::call("Background Worker Data", size)?;

    let max_worker_processes = backend_utils_init_small_seams::max_worker_processes::call();

    if !backend_utils_init_small_seams::is_under_postmaster::call() {
        let mut slots: Vec<BackgroundWorkerSlot> =
            vec![BackgroundWorkerSlot::empty(); max_worker_processes as usize];

        BACKGROUND_WORKER_LIST.with(|list| {
            let mut list = list.borrow_mut();
            // Copy contents of worker list into shared memory, recording the
            // shared slot assigned to each worker (1-to-1 correspondence).
            for (slotno, rw) in list.iter_mut().enumerate() {
                debug_assert!((slotno as i32) < max_worker_processes);
                let slot = &mut slots[slotno];
                slot.in_use = true;
                slot.terminate = false;
                slot.pid = INVALID_PID;
                slot.generation = 0;
                rw.rw_shmem_slot = slotno as i32;
                rw.rw_worker.bgw_notify_pid = 0; // might be reinit after crash
                slot.worker = rw.rw_worker;
            }
            // Remaining slots already constructed as not in use.
        });

        *BACKGROUND_WORKER_DATA.lock().unwrap() = Some(BackgroundWorkerArray {
            total_slots: max_worker_processes,
            parallel_register_count: 0,
            parallel_terminate_count: 0,
            slot: slots,
        });
    } else {
        // Under postmaster: the array already exists (Assert(found)).
        debug_assert!(_found);
    }

    Ok(())
}

/// Whether `BackgroundWorkerData != NULL` (the shmem region is attached).
fn bgworker_data_is_initialized() -> bool {
    BACKGROUND_WORKER_DATA.lock().unwrap().is_some()
}

// ---------------------------------------------------------------------------
// FindRegisteredWorkerBySlotNumber
// ---------------------------------------------------------------------------

/// `FindRegisteredWorkerBySlotNumber(int slotno)` — return the list index of
/// the worker occupying shared slot `slotno`, the idiomatic stand-in for the C
/// `RegisteredBgWorker *`.
fn FindRegisteredWorkerBySlotNumber(list: &[RegisteredBgWorker], slotno: i32) -> Option<usize> {
    list.iter().position(|rw| rw.rw_shmem_slot == slotno)
}

// ---------------------------------------------------------------------------
// BackgroundWorkerStateChange
// ---------------------------------------------------------------------------

/// `BackgroundWorkerStateChange(bool allow_new_workers)` — notice changes to
/// shared memory made by other backends. Runs in the postmaster, so it must
/// not crash even on corrupted shared memory (log and bail rather than Assert).
pub fn BackgroundWorkerStateChange(allow_new_workers: bool) -> PgResult<()> {
    let max_worker_processes = backend_utils_init_small_seams::max_worker_processes::call();

    // The total number of slots stored in shared memory should match our
    // notion of max_worker_processes. If it does not, something is very wrong.
    {
        let total_slots = {
            let data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_ref().map(|d| d.total_slots).unwrap_or(0)
        };
        if max_worker_processes != total_slots {
            ereport::call(
                PgError::new(
                    LOG,
                    format!(
                        "inconsistent background worker state (\"max_worker_processes\"={max_worker_processes}, total slots={total_slots})"
                    ),
                )
                .with_error_location(loc(284, "BackgroundWorkerStateChange")),
            )?;
            return Ok(());
        }
    }

    // Iterate through slots, looking for newly-registered workers or workers
    // who must die.
    let mut slotno = 0;
    while slotno < max_worker_processes {
        let in_use = {
            let data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_ref().unwrap().slot[slotno as usize].in_use
        };
        if !in_use {
            slotno += 1;
            continue;
        }

        // Make sure we don't see the in_use flag before the updated slot
        // contents. (The Mutex acquire/release is the barrier.)

        // See whether we already know about this worker.
        let rw_index =
            BACKGROUND_WORKER_LIST.with(|l| FindRegisteredWorkerBySlotNumber(&l.borrow(), slotno));

        if let Some(rw_index) = rw_index {
            // Someone can set the terminate flag.
            let (slot_terminate, rw_terminate) = {
                let data = BACKGROUND_WORKER_DATA.lock().unwrap();
                let st = data.as_ref().unwrap().slot[slotno as usize].terminate;
                let rt = BACKGROUND_WORKER_LIST.with(|l| l.borrow()[rw_index].rw_terminate);
                (st, rt)
            };
            if slot_terminate && !rw_terminate {
                let rw_pid = BACKGROUND_WORKER_LIST.with(|l| {
                    let mut list = l.borrow_mut();
                    list[rw_index].rw_terminate = true;
                    list[rw_index].rw_pid
                });
                if rw_pid != 0 {
                    backend_postmaster_postmaster_seams::signal_child_sigterm::call(rw_pid);
                } else {
                    // Report never-started, now-terminated worker as dead.
                    ReportBackgroundWorkerPID(rw_index);
                }
            }
            slotno += 1;
            continue;
        }

        // If we aren't allowing new workers, mark it for termination; the next
        // stanza cleans it up and wakes anyone waiting.
        if !allow_new_workers {
            let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_mut().unwrap().slot[slotno as usize].terminate = true;
        }

        // Found a slot without a corresponding RegisteredBgWorker.
        let slot_terminate = {
            let data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_ref().unwrap().slot[slotno as usize].terminate
        };
        if slot_terminate {
            // Slot already terminating: free it and notify the registrant.
            let notify_pid = {
                let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
                let d = data.as_mut().unwrap();
                let notify_pid = d.slot[slotno as usize].worker.bgw_notify_pid;
                if (d.slot[slotno as usize].worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0 {
                    d.parallel_terminate_count = d.parallel_terminate_count.wrapping_add(1);
                }
                d.slot[slotno as usize].pid = 0;
                // pg_memory_barrier(): the Mutex release/acquire is the barrier.
                d.slot[slotno as usize].in_use = false;
                notify_pid
            };
            if notify_pid != 0 {
                backend_postmaster_postmaster_seams::signal_child_sigusr1::call(notify_pid);
            }
            slotno += 1;
            continue;
        }

        // Copy the registration data into the registered workers list.
        //
        // C: rw = MemoryContextAllocExtended(PostmasterContext,
        //         sizeof(RegisteredBgWorker), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
        //     if (rw == NULL) { ereport(LOG, OUT_OF_MEMORY, "out of memory"); return; }
        //
        // The no-OOM allocator is deliberate: this runs in the postmaster, which
        // must survive OOM rather than crash. We reserve the list node's space
        // fallibly (try_reserve) and, on failure, LOG and return without
        // aborting — preserving the postmaster's survive-OOM contract.
        if let Err(_e) =
            BACKGROUND_WORKER_LIST.with(|l| l.borrow_mut().try_reserve(1))
        {
            ereport::call(
                PgError::new(LOG, "out of memory".to_string())
                    .with_sqlstate(types_error::ERRCODE_OUT_OF_MEMORY)
                    .with_message_id("out of memory")
                    .with_error_location(loc(356, "BackgroundWorkerStateChange")),
            )?;
            return Ok(());
        }

        let slot_worker = {
            let data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_ref().unwrap().slot[slotno as usize].worker
        };

        // Copy strings in a paranoid way (shared memory might be corrupted and
        // not NUL-terminated).
        let mut rw = RegisteredBgWorker {
            rw_worker: BackgroundWorker {
                bgw_name: ascii_safe_strlcpy(&slot_worker.bgw_name, BGW_MAXLEN),
                bgw_type: ascii_safe_strlcpy(&slot_worker.bgw_type, BGW_MAXLEN),
                bgw_library_name: ascii_safe_strlcpy(&slot_worker.bgw_library_name, MAXPGPATH),
                bgw_function_name: ascii_safe_strlcpy(&slot_worker.bgw_function_name, BGW_MAXLEN),
                bgw_flags: slot_worker.bgw_flags,
                bgw_start_time: slot_worker.bgw_start_time,
                bgw_restart_time: slot_worker.bgw_restart_time,
                bgw_main_arg: slot_worker.bgw_main_arg,
                bgw_extra: slot_worker.bgw_extra,
                bgw_notify_pid: slot_worker.bgw_notify_pid,
            },
            rw_pid: 0,
            rw_crashed_at: 0,
            rw_shmem_slot: slotno,
            rw_terminate: false,
        };

        // Copy the notify PID, but only if the postmaster knows a backend with
        // it; log at a high debug level if not.
        if !backend_postmaster_postmaster_seams::postmaster_mark_pid_for_worker_notify::call(
            rw.rw_worker.bgw_notify_pid,
        ) {
            ereport::call(
                PgError::new(
                    DEBUG1,
                    format!(
                        "worker notification PID {} is not valid",
                        rw.rw_worker.bgw_notify_pid
                    ),
                )
                .with_message_id("worker notification PID %d is not valid")
                .with_error_location(loc(422, "BackgroundWorkerStateChange")),
            )?;
            rw.rw_worker.bgw_notify_pid = 0;
        }

        ereport::call(
            PgError::new(
                DEBUG1,
                format!(
                    "registering background worker \"{}\"",
                    cstr_lossy(&rw.rw_worker.bgw_name)
                ),
            )
            .with_message_id("registering background worker \"%s\"")
            .with_error_location(loc(435, "BackgroundWorkerStateChange")),
        )?;

        // dlist_push_head: prepend to keep C's head-insertion order.
        BACKGROUND_WORKER_LIST.with(|l| l.borrow_mut().insert(0, rw));

        slotno += 1;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ForgetBackgroundWorker
// ---------------------------------------------------------------------------

/// `ForgetBackgroundWorker(RegisteredBgWorker *rw)` — forget a worker that's no
/// longer needed. `rw_index` indexes [`BackgroundWorkerList`]. Postmaster-only.
/// Caller notifies `bgw_notify_pid` if appropriate.
pub fn ForgetBackgroundWorker(rw_index: usize) -> PgResult<()> {
    let (slotno, parallel, name) = BACKGROUND_WORKER_LIST.with(|l| {
        let list = l.borrow();
        let rw = &list[rw_index];
        (
            rw.rw_shmem_slot,
            (rw.rw_worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0,
            cstr_lossy(&rw.rw_worker.bgw_name),
        )
    });

    {
        let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
        let d = data.as_mut().unwrap();
        debug_assert!(d.slot[slotno as usize].in_use);
        // Update of parallel_terminate_count completes before the store to
        // in_use (the Mutex release is the memory barrier).
        if parallel {
            d.parallel_terminate_count = d.parallel_terminate_count.wrapping_add(1);
        }
        d.slot[slotno as usize].in_use = false;
    }

    ereport::call(
        PgError::new(DEBUG1, format!("unregistering background worker \"{name}\""))
            .with_message_id("unregistering background worker \"%s\"")
            .with_error_location(loc(472, "ForgetBackgroundWorker")),
    )?;

    // dlist_delete + pfree(rw).
    BACKGROUND_WORKER_LIST.with(|l| {
        l.borrow_mut().remove(rw_index);
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// ReportBackgroundWorkerPID
// ---------------------------------------------------------------------------

/// `ReportBackgroundWorkerPID(RegisteredBgWorker *rw)` — publish a launched
/// worker's PID in shared memory. Postmaster-only.
pub fn ReportBackgroundWorkerPID(rw_index: usize) {
    let (slotno, rw_pid, notify_pid) = BACKGROUND_WORKER_LIST.with(|l| {
        let list = l.borrow();
        let rw = &list[rw_index];
        (rw.rw_shmem_slot, rw.rw_pid, rw.rw_worker.bgw_notify_pid)
    });

    {
        let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
        data.as_mut().unwrap().slot[slotno as usize].pid = rw_pid;
    }

    if notify_pid != 0 {
        backend_postmaster_postmaster_seams::signal_child_sigusr1::call(notify_pid);
    }
}

// ---------------------------------------------------------------------------
// ReportBackgroundWorkerExit
// ---------------------------------------------------------------------------

/// `ReportBackgroundWorkerExit(RegisteredBgWorker *rw)` — report a worker's PID
/// now zero because it exited. Postmaster-only.
pub fn ReportBackgroundWorkerExit(rw_index: usize) -> PgResult<()> {
    let (slotno, rw_pid, rw_terminate, restart_time, notify_pid) =
        BACKGROUND_WORKER_LIST.with(|l| {
            let list = l.borrow();
            let rw = &list[rw_index];
            (
                rw.rw_shmem_slot,
                rw.rw_pid,
                rw.rw_terminate,
                rw.rw_worker.bgw_restart_time,
                rw.rw_worker.bgw_notify_pid,
            )
        });

    {
        let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
        data.as_mut().unwrap().slot[slotno as usize].pid = rw_pid;
    }

    // If this worker is slated for deregistration, do that before notifying the
    // process which started it, so a quick slot reuse is less likely to race.
    if rw_terminate || restart_time == BGW_NEVER_RESTART {
        ForgetBackgroundWorker(rw_index)?;
    }

    if notify_pid != 0 {
        backend_postmaster_postmaster_seams::signal_child_sigusr1::call(notify_pid);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// BackgroundWorkerStopNotifications
// ---------------------------------------------------------------------------

/// `BackgroundWorkerStopNotifications(pid_t pid)` — cancel SIGUSR1
/// notifications for an exiting backend's PID. Postmaster-only.
pub fn BackgroundWorkerStopNotifications(pid: pid_t) {
    BACKGROUND_WORKER_LIST.with(|l| {
        for rw in l.borrow_mut().iter_mut() {
            if rw.rw_worker.bgw_notify_pid == pid {
                rw.rw_worker.bgw_notify_pid = 0;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// ForgetUnstartedBackgroundWorkers
// ---------------------------------------------------------------------------

/// `ForgetUnstartedBackgroundWorkers(void)` — cancel not-yet-started worker
/// requests whose waiters need to be kicked at shutdown. Postmaster-only.
pub fn ForgetUnstartedBackgroundWorkers() -> PgResult<()> {
    // dlist_foreach_modify: walk by index; ForgetBackgroundWorker removes the
    // entry, so re-examine the same index when it does.
    let mut i = 0;
    loop {
        let len = BACKGROUND_WORKER_LIST.with(|l| l.borrow().len());
        if i >= len {
            break;
        }

        let (slotno, notify_pid) = BACKGROUND_WORKER_LIST.with(|l| {
            let list = l.borrow();
            (list[i].rw_shmem_slot, list[i].rw_worker.bgw_notify_pid)
        });

        let slot_pid = {
            let data = BACKGROUND_WORKER_DATA.lock().unwrap();
            data.as_ref().unwrap().slot[slotno as usize].pid
        };

        // If it's not yet started, and there's someone waiting ...
        if slot_pid == INVALID_PID && notify_pid != 0 {
            // ... then zap it, and notify the waiter.
            ForgetBackgroundWorker(i)?;
            if notify_pid != 0 {
                backend_postmaster_postmaster_seams::signal_child_sigusr1::call(notify_pid);
            }
            // The entry was removed; the next entry now occupies index i.
            continue;
        }

        i += 1;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ResetBackgroundWorkerCrashTimes
// ---------------------------------------------------------------------------

/// `ResetBackgroundWorkerCrashTimes(void)` — reset crash state before resuming
/// after a crash. Workers marked `BGW_NEVER_RESTART` are forgotten; others get
/// their crash time / pid / notify pid cleared. Postmaster-only.
pub fn ResetBackgroundWorkerCrashTimes() -> PgResult<()> {
    let mut i = 0;
    loop {
        let len = BACKGROUND_WORKER_LIST.with(|l| l.borrow().len());
        if i >= len {
            break;
        }

        let restart_time =
            BACKGROUND_WORKER_LIST.with(|l| l.borrow()[i].rw_worker.bgw_restart_time);

        if restart_time == BGW_NEVER_RESTART {
            // Forget BGW_NEVER_RESTART workers so they aren't relaunched (and so
            // a parallel worker can't bump parallel_terminate_count after the
            // register count was zeroed).
            ForgetBackgroundWorker(i)?;
            // The entry was removed; the next entry now occupies index i.
            continue;
        } else {
            BACKGROUND_WORKER_LIST.with(|l| {
                let mut list = l.borrow_mut();
                // All non-never-restart survivors must be non-parallel.
                debug_assert!((list[i].rw_worker.bgw_flags & BGWORKER_CLASS_PARALLEL) == 0);
                // Allow immediate restart and drop any waiter.
                list[i].rw_crashed_at = 0;
                list[i].rw_pid = 0;
                list[i].rw_worker.bgw_notify_pid = 0;
            });
        }

        i += 1;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SanityCheckBackgroundWorker
// ---------------------------------------------------------------------------

/// `SanityCheckBackgroundWorker(BackgroundWorker *worker, int elevel)` —
/// validate a registration. Returns `Ok(true)` if ok, `Ok(false)` if not
/// (when `elevel < ERROR`); when `elevel == ERROR`/`FATAL` the report
/// propagates as `Err`. A valid `bgw_type` is defaulted from `bgw_name`.
fn SanityCheckBackgroundWorker(
    worker: &mut BackgroundWorker,
    elevel: types_error::ErrorLevel,
) -> PgResult<bool> {
    // BGWORKER_SHMEM_ACCESS is a required flag.
    if (worker.bgw_flags & BGWORKER_SHMEM_ACCESS) == 0 {
        ereport::call(
            PgError::new(
                elevel,
                format!(
                    "background worker \"{}\": background workers without shared memory access are not supported",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_error_location(loc(670, "SanityCheckBackgroundWorker")),
        )?;
        return Ok(false);
    }

    if (worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) != 0
        && worker.bgw_start_time == BgWorkerStartTime::PostmasterStart
    {
        ereport::call(
            PgError::new(
                elevel,
                format!(
                    "background worker \"{}\": cannot request database access if starting at postmaster start",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_error_location(loc(681, "SanityCheckBackgroundWorker")),
        )?;
        return Ok(false);
        // XXX other checks?
    }

    if (worker.bgw_restart_time < 0 && worker.bgw_restart_time != BGW_NEVER_RESTART)
        || (worker.bgw_restart_time as i64 > USECS_PER_DAY / 1000)
    {
        ereport::call(
            PgError::new(
                elevel,
                format!(
                    "background worker \"{}\": invalid restart interval",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_error_location(loc(695, "SanityCheckBackgroundWorker")),
        )?;
        return Ok(false);
    }

    // Parallel workers may not be configured for restart (the
    // register/terminate accounting can't survive a crash-and-restart cycle).
    if worker.bgw_restart_time != BGW_NEVER_RESTART
        && (worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0
    {
        ereport::call(
            PgError::new(
                elevel,
                format!(
                    "background worker \"{}\": parallel workers may not be configured for restart",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_error_location(loc(710, "SanityCheckBackgroundWorker")),
        )?;
        return Ok(false);
    }

    // If bgw_type is not filled in, use bgw_name.
    if cstr_len(&worker.bgw_type) == 0 {
        strcpy(&mut worker.bgw_type, &worker.bgw_name);
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// bgworker_die — the SIGTERM handler body.
// ---------------------------------------------------------------------------

/// `bgworker_die(SIGNAL_ARGS)` — standard SIGTERM handler for background
/// workers: block all signals, then `ereport(FATAL)`. The block-signals and
/// the actual FATAL longjmp/exit are owned externally; the `Err(PgError)` is
/// the FATAL report (the handler is installed via the signal-handler seam,
/// which carries this body).
pub fn bgworker_die(bgw_type: &[u8]) -> PgResult<()> {
    backend_libpq_pqsignal_seams::block_signals::call();
    Err(PgError::new(
        FATAL,
        format!(
            "terminating background worker \"{}\" due to administrator command",
            cstr_lossy(bgw_type)
        ),
    )
    .with_sqlstate(types_error::ERRCODE_ADMIN_SHUTDOWN)
    .with_error_location(loc(734, "bgworker_die")))
}

// ---------------------------------------------------------------------------
// BackgroundWorkerMain — the worker process body.
// ---------------------------------------------------------------------------

thread_local! {
    /// `BackgroundWorker *MyBgworkerEntry;` — the running worker's registry
    /// entry (per-backend; bgworker.c owns it).
    static MY_BGWORKER_ENTRY: RefCell<Option<BackgroundWorker>> = const { RefCell::new(None) };
}

/// `BackgroundWorkerMain(const void *startup_data, size_t startup_data_len)` —
/// entry point for a background worker process. `noreturn` (`proc_exit`).
pub fn BackgroundWorkerMain(startup_data: &StartupData) -> ! {
    let my_pid = backend_utils_init_small_seams::my_proc_pid::call();

    // if (startup_data == NULL) elog(FATAL, "unable to find bgworker entry");
    // Assert(startup_data_len == sizeof(BackgroundWorker));
    let worker = match startup_data {
        StartupData::BgWorker(w) => *w,
        _ => {
            // elog(FATAL, "unable to find bgworker entry"): emit then exit.
            emit_error_report_for(
                &PgError::new(FATAL, "unable to find bgworker entry")
                    .with_message_id("unable to find bgworker entry")
                    .with_error_location(loc(749, "BackgroundWorkerMain")),
            );
            backend_storage_ipc_seams::proc_exit::call(1, my_pid)
        }
    };

    // worker = MemoryContextAlloc(TopMemoryContext, ...); memcpy; publish as
    // MyBgworkerEntry; then release the inherited PostmasterContext.
    MY_BGWORKER_ENTRY.with(|e| *e.borrow_mut() = Some(worker));
    backend_postmaster_postmaster_seams::delete_postmaster_context::call();

    // MyBackendType = B_BG_WORKER;
    backend_utils_init_small_seams::set_my_backend_type::call(
        types_core::init::BackendType::BgWorker,
    );

    // init_ps_display(worker->bgw_name);
    backend_utils_misc_ps_status_seams::init_ps_display::call(&worker.bgw_name);

    // Assert(GetProcessingMode() == InitProcessing);

    // Apply PostAuthDelay.
    let post_auth_delay = backend_utils_init_small_seams::post_auth_delay::call();
    if post_auth_delay > 0 {
        port_pgsleep_seams::pg_usleep::call(post_auth_delay as i64 * 1_000_000);
    }

    // Set up signal handlers (connection vs non-connection variant), the
    // bgworker_die SIGTERM handler, the SIG_IGN/SIG_DFL dispositions, and
    // InitializeTimeouts().
    let db_connection = (worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) != 0;
    backend_tcop_postgres_seams::install_bgworker_signal_handlers::call(db_connection);

    // We can now handle ereport(ERROR). C arms the sigsetjmp here; the
    // idiomatic model is `?` propagation: run the body and, on Err, clean up,
    // report, and proc_exit(1).
    match run_worker_body(worker) {
        Ok(()) => {
            // If the background worker exits without an error, exit cleanly.
            backend_storage_ipc_seams::proc_exit::call(0, my_pid)
        }
        Err(_edata) => {
            // Since not using PG_TRY, must reset error stack by hand:
            // error_context_stack = NULL (handled by the error crate on
            // propagation). Prevent interrupts while cleaning up: HOLD_INTERRUPTS.
            backend_utils_init_small_seams::hold_interrupts::call();
            // sigsetjmp blocked all signals; once held it is safe to unblock.
            BackgroundWorkerUnblockSignals();
            // Report the error to the parallel leader and the server log.
            emit_error_report_for(&_edata);
            backend_storage_ipc_seams::proc_exit::call(1, my_pid)
        }
    }
}

/// The post-sigsetjmp body of `BackgroundWorkerMain`: create the PGPROC,
/// early-init, look up the entry point, and invoke the worker code.
fn run_worker_body(worker: BackgroundWorker) -> PgResult<()> {
    // Create a per-backend PGPROC struct in shared memory (before LWLock use).
    backend_storage_lmgr_proc_seams::init_process::call()?;

    // Early initialization.
    backend_utils_init_postinit_seams::base_init::call()?;

    // Look up the entry point function, loading its library if necessary.
    // LookupBackgroundWorkerFunction's internal-name decision is in-crate; the
    // resolution + call is the loader's job.
    let _ = LookupBackgroundWorkerFunction(&worker.bgw_library_name, &worker.bgw_function_name)?;

    // Note: a normal backend would InitPostgres here; a worker waits for user
    // code to call BackgroundWorkerInitializeConnection().

    // Invoke the user-defined worker code: entrypt(worker->bgw_main_arg).
    backend_utils_fmgr_fmgr_seams::call_bgworker_entrypoint::call(worker, worker.bgw_main_arg)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// BackgroundWorkerInitializeConnection{,ByOid}
// ---------------------------------------------------------------------------

/// `BackgroundWorkerInitializeConnection(dbname, username, flags)` — connect a
/// background worker to a database by name, using the current role.
pub fn BackgroundWorkerInitializeConnection(
    dbname: Option<&str>,
    username: Option<&str>,
    flags: u32,
) -> PgResult<()> {
    let worker = my_bgworker_entry();
    let mut init_flags: u32 = 0; // never honor session_preload_libraries

    // ignore datallowconn and ACL_CONNECT?
    if (flags & BGWORKER_BYPASS_ALLOWCONN) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ALLOW_CONNS;
    }
    // ignore rolcanlogin?
    if (flags & BGWORKER_BYPASS_ROLELOGINCHECK) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ROLE_LOGIN;
    }

    // XXX is this the right errcode?
    if (worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) == 0 {
        return Err(PgError::new(
            FATAL,
            "database connection requirement not indicated during registration",
        )
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_error_location(loc(891, "BackgroundWorkerInitializeConnection")));
    }

    backend_utils_init_postinit_seams::init_postgres_by_name::call(dbname, username, init_flags)?;

    // it had better not have gotten out of "init" mode yet (the seam owner
    // performs the IsInitProcessingMode check + SetProcessingMode(Normal)).
    Ok(())
}

/// `BackgroundWorkerInitializeConnectionByOid(dboid, useroid, flags)` — connect
/// a background worker to a database by OID using the role `useroid`.
pub fn BackgroundWorkerInitializeConnectionByOid(
    dboid: types_core::Oid,
    useroid: types_core::Oid,
    flags: u32,
) -> PgResult<()> {
    let worker = my_bgworker_entry();
    let mut init_flags: u32 = 0;

    if (flags & BGWORKER_BYPASS_ALLOWCONN) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ALLOW_CONNS;
    }
    if (flags & BGWORKER_BYPASS_ROLELOGINCHECK) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ROLE_LOGIN;
    }

    if (worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) == 0 {
        return Err(PgError::new(
            FATAL,
            "database connection requirement not indicated during registration",
        )
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_error_location(loc(925, "BackgroundWorkerInitializeConnectionByOid")));
    }

    backend_utils_init_postinit_seams::init_postgres_by_oid::call(dboid, useroid, init_flags)?;

    Ok(())
}

/// `BackgroundWorker *worker = MyBgworkerEntry;`.
fn my_bgworker_entry() -> BackgroundWorker {
    MY_BGWORKER_ENTRY.with(|e| e.borrow().expect("MyBgworkerEntry not set"))
}

// ---------------------------------------------------------------------------
// BackgroundWorkerBlockSignals / BackgroundWorkerUnblockSignals
// ---------------------------------------------------------------------------

/// `BackgroundWorkerBlockSignals(void)`.
pub fn BackgroundWorkerBlockSignals() {
    backend_libpq_pqsignal_seams::block_signals::call();
}

/// `BackgroundWorkerUnblockSignals(void)`.
pub fn BackgroundWorkerUnblockSignals() {
    backend_libpq_pqsignal_seams::unblock_signals::call();
}

// ---------------------------------------------------------------------------
// RegisterBackgroundWorker
// ---------------------------------------------------------------------------

/// `RegisterBackgroundWorker(BackgroundWorker *worker)` — register a static
/// background worker. Only effective from the postmaster (or a
/// shared_preload_libraries `_PG_init`).
pub fn RegisterBackgroundWorker(worker: &BackgroundWorker) -> PgResult<()> {
    // Static background workers can only be registered in the postmaster.
    if backend_utils_init_small_seams::is_under_postmaster::call()
        || !backend_utils_init_small_seams::is_postmaster_environment::call()
    {
        // Tolerate duplicate registration during shared_preload_libraries
        // processing (an extension may be loaded both ways).
        if backend_utils_init_miscinit_seams::process_shared_preload_libraries_in_progress::call() {
            return Ok(());
        }
        ereport::call(
            PgError::new(
                LOG,
                format!(
                    "background worker \"{}\": must be registered in \"shared_preload_libraries\"",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_error_location(loc(990, "RegisterBackgroundWorker")),
        )?;
        return Ok(());
    }

    // Cannot register after BackgroundWorkerShmemInit() (should not happen).
    if bgworker_data_is_initialized() {
        return Err(PgError::new(
            ERROR,
            format!(
                "cannot register background worker \"{}\" after shmem init",
                cstr_lossy(&worker.bgw_name)
            ),
        )
        .with_message_id("cannot register background worker \"%s\" after shmem init")
        .with_error_location(loc(1000, "RegisterBackgroundWorker")));
    }

    ereport::call(
        PgError::new(
            DEBUG1,
            format!(
                "registering background worker \"{}\"",
                cstr_lossy(&worker.bgw_name)
            ),
        )
        .with_message_id("registering background worker \"%s\"")
        .with_error_location(loc(1004, "RegisterBackgroundWorker")),
    )?;

    let mut worker = *worker;
    if !SanityCheckBackgroundWorker(&mut worker, LOG)? {
        return Ok(());
    }

    if worker.bgw_notify_pid != 0 {
        ereport::call(
            PgError::new(
                LOG,
                format!(
                    "background worker \"{}\": only dynamic background workers can request notification",
                    cstr_lossy(&worker.bgw_name)
                ),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_error_location(loc(1013, "RegisterBackgroundWorker")),
        )?;
        return Ok(());
    }

    // Enforce maximum number of workers.
    let max_worker_processes = backend_utils_init_small_seams::max_worker_processes::call();
    let too_many = NUMWORKERS.with(|n| {
        let mut n = n.borrow_mut();
        *n += 1;
        *n > max_worker_processes
    });
    if too_many {
        let detail = if max_worker_processes == 1 {
            format!("Up to {max_worker_processes} background worker can be registered with the current settings.")
        } else {
            format!("Up to {max_worker_processes} background workers can be registered with the current settings.")
        };
        ereport::call(
            PgError::new(LOG, "too many background workers")
                .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .with_detail(detail)
                .with_hint(
                    "Consider increasing the configuration parameter \"max_worker_processes\".",
                )
                .with_error_location(loc(1026, "RegisterBackgroundWorker")),
        )?;
        return Ok(());
    }

    // Copy the registration data into the registered workers list.
    //
    // C: rw = MemoryContextAllocExtended(PostmasterContext,
    //         sizeof(RegisteredBgWorker), MCXT_ALLOC_NO_OOM);
    //     if (rw == NULL) { ereport(LOG, OUT_OF_MEMORY, "out of memory"); return; }
    //
    // No-OOM is deliberate (this runs in the postmaster). Reserve the node's
    // space fallibly; on failure LOG and return without aborting.
    if let Err(_e) = BACKGROUND_WORKER_LIST.with(|l| l.borrow_mut().try_reserve(1)) {
        ereport::call(
            PgError::new(LOG, "out of memory".to_string())
                .with_sqlstate(types_error::ERRCODE_OUT_OF_MEMORY)
                .with_message_id("out of memory")
                .with_error_location(loc(1021, "RegisterBackgroundWorker")),
        )?;
        return Ok(());
    }

    let rw = RegisteredBgWorker {
        rw_worker: worker,
        rw_pid: 0,
        rw_crashed_at: 0,
        rw_shmem_slot: 0,
        rw_terminate: false,
    };

    BACKGROUND_WORKER_LIST.with(|l| l.borrow_mut().insert(0, rw));
    Ok(())
}

// ---------------------------------------------------------------------------
// RegisterDynamicBackgroundWorker
// ---------------------------------------------------------------------------

/// `RegisterDynamicBackgroundWorker(BackgroundWorker *worker,
/// BackgroundWorkerHandle **handle)` — register a worker from a regular
/// backend. Returns `Ok(None)` on failure, `Ok(Some(handle))` on success.
pub fn RegisterDynamicBackgroundWorker(
    worker: &BackgroundWorker,
) -> PgResult<Option<BackgroundWorkerHandle>> {
    let mut success = false;
    let mut generation: uint64 = 0;

    // We can't register dynamic background workers from the postmaster, and a
    // standalone backend is the only process.
    if !backend_utils_init_small_seams::is_under_postmaster::call() {
        return Ok(None);
    }

    let mut worker = *worker;
    if !SanityCheckBackgroundWorker(&mut worker, ERROR)? {
        return Ok(None);
    }

    let parallel = (worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0;

    // LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE) — held across the
    // unused-slot search; the guard's Drop is C's LWLockReleaseAll abort path.
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        BACKGROUND_WORKER_LWLOCK_OFFSET,
        types_storage::LWLockMode::LW_EXCLUSIVE,
    )?;

    let mut slotno = 0;
    {
        let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
        let d = data.as_mut().unwrap();

        // Too many parallel workers already? Our view of
        // parallel_terminate_count may be slightly stale; that's fine.
        if parallel
            && d.parallel_register_count.wrapping_sub(d.parallel_terminate_count)
                >= backend_utils_init_small_seams::max_parallel_workers::call() as uint32
        {
            debug_assert!(
                d.parallel_register_count.wrapping_sub(d.parallel_terminate_count)
                    <= types_bgworker::MAX_PARALLEL_WORKER_LIMIT as uint32
            );
            drop(data);
            guard.release()?;
            return Ok(None);
        }

        // Look for an unused slot. If we find one, grab it.
        while slotno < d.total_slots {
            if !d.slot[slotno as usize].in_use {
                d.slot[slotno as usize].worker = worker;
                d.slot[slotno as usize].pid = INVALID_PID; // not started yet
                d.slot[slotno as usize].generation =
                    d.slot[slotno as usize].generation.wrapping_add(1);
                d.slot[slotno as usize].terminate = false;
                generation = d.slot[slotno as usize].generation;
                if parallel {
                    d.parallel_register_count = d.parallel_register_count.wrapping_add(1);
                }
                // pg_write_barrier(): the in_use store is the last write, and
                // the Mutex release publishes the new contents before in_use is
                // observed (postmaster reads under the same Mutex).
                d.slot[slotno as usize].in_use = true;
                success = true;
                break;
            }
            slotno += 1;
        }
    }

    guard.release()?;

    // If we found a slot, tell the postmaster to notice the change.
    if success {
        backend_storage_ipc_pmsignal_seams::send_postmaster_signal_bgworker_change::call();
    }

    // If we found a slot, initialize the handle.
    if success {
        Ok(Some(BackgroundWorkerHandle {
            slot: slotno,
            generation,
        }))
    } else {
        Ok(None)
    }
}

/// `&MainLWLockArray[32].lock` — `BackgroundWorkerLock` is `PG_LWLOCK(33,
/// BackgroundWorker)` (`lwlocklist.h`), the 33rd individual lock, so offset 32.
const BACKGROUND_WORKER_LWLOCK_OFFSET: usize = 32;

// ---------------------------------------------------------------------------
// GetBackgroundWorkerPid
// ---------------------------------------------------------------------------

/// `GetBackgroundWorkerPid(BackgroundWorkerHandle *handle, pid_t *pidp)` — get
/// the PID of a dynamically-registered worker. Returns the status and (for
/// `Started`) the PID.
pub fn GetBackgroundWorkerPid(handle: &BackgroundWorkerHandle) -> (BgwHandleStatus, pid_t) {
    let slotno = handle.slot;

    // Keep it simple and grab the lock (contention is unlikely).
    // GetBackgroundWorkerPid is infallible in C; the shared-lock acquire
    // cannot fail there. The guard is held across the slot read; its Drop is
    // C's LWLockReleaseAll abort path.
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        BACKGROUND_WORKER_LWLOCK_OFFSET,
        types_storage::LWLockMode::LW_SHARED,
    )
    .expect("BackgroundWorkerLock shared acquire cannot fail");

    let pid = {
        let data = BACKGROUND_WORKER_DATA.lock().unwrap();
        let slot = &data.as_ref().unwrap().slot[slotno as usize];
        // generation can't change under the lock; pid (postmaster-updated)
        // may be stale but won't be garbage.
        if handle.generation != slot.generation || !slot.in_use {
            0
        } else {
            slot.pid
        }
    };

    // LWLockRelease(BackgroundWorkerLock).
    let _ = guard.release();

    if pid == 0 {
        (BgwHandleStatus::Stopped, 0)
    } else if pid == INVALID_PID {
        (BgwHandleStatus::NotYetStarted, 0)
    } else {
        (BgwHandleStatus::Started, pid)
    }
}

// ---------------------------------------------------------------------------
// WaitForBackgroundWorkerStartup
// ---------------------------------------------------------------------------

/// `WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *handle, pid_t
/// *pidp)` — sleep until the worker leaves `NotYetStarted` (or the postmaster
/// dies). Returns the terminal status and the PID for `Started`.
pub fn WaitForBackgroundWorkerStartup(
    handle: &BackgroundWorkerHandle,
) -> PgResult<(BgwHandleStatus, pid_t)> {
    let mut status;
    let mut out_pid = 0;

    loop {
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        let (s, pid) = GetBackgroundWorkerPid(handle);
        status = s;
        if status == BgwHandleStatus::Started {
            out_pid = pid;
        }
        if status != BgwHandleStatus::NotYetStarted {
            break;
        }

        let rc = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_STARTUP,
        )?;

        if (rc & WL_POSTMASTER_DEATH) != 0 {
            status = BgwHandleStatus::PostmasterDied;
            break;
        }

        backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
    }

    Ok((status, out_pid))
}

// ---------------------------------------------------------------------------
// WaitForBackgroundWorkerShutdown
// ---------------------------------------------------------------------------

/// `WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *)` — sleep until
/// the worker reaches `Stopped` (or the postmaster dies).
pub fn WaitForBackgroundWorkerShutdown(
    handle: &BackgroundWorkerHandle,
) -> PgResult<BgwHandleStatus> {
    let mut status;

    loop {
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        let (s, _pid) = GetBackgroundWorkerPid(handle);
        status = s;
        if status == BgwHandleStatus::Stopped {
            break;
        }

        let rc = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        )?;

        if (rc & WL_POSTMASTER_DEATH) != 0 {
            status = BgwHandleStatus::PostmasterDied;
            break;
        }

        backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
    }

    Ok(status)
}

// ---------------------------------------------------------------------------
// TerminateBackgroundWorker
// ---------------------------------------------------------------------------

/// `TerminateBackgroundWorker(BackgroundWorkerHandle *handle)` — instruct the
/// postmaster to terminate a worker. Safe regardless of the worker's state.
pub fn TerminateBackgroundWorker(handle: &BackgroundWorkerHandle) -> PgResult<()> {
    let slotno = handle.slot;
    let mut signal_postmaster = false;

    // LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE) — held across the
    // generation check / terminate store; Drop is C's LWLockReleaseAll path.
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        BACKGROUND_WORKER_LWLOCK_OFFSET,
        types_storage::LWLockMode::LW_EXCLUSIVE,
    )?;

    // Only act if the generation matches (guards against slot reuse).
    {
        let mut data = BACKGROUND_WORKER_DATA.lock().unwrap();
        let slot = &mut data.as_mut().unwrap().slot[slotno as usize];
        if handle.generation == slot.generation {
            slot.terminate = true;
            signal_postmaster = true;
        }
    }

    guard.release()?;

    if signal_postmaster {
        backend_storage_ipc_pmsignal_seams::send_postmaster_signal_bgworker_change::call();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// LookupBackgroundWorkerFunction
// ---------------------------------------------------------------------------

/// `LookupBackgroundWorkerFunction(libraryname, funcname)` — the in-crate part
/// of the entry-point lookup: for library "postgres" the function name must be
/// one of [`INTERNAL_BGWORKER_NAMES`] (an unknown internal name is FATAL).
/// Returns the index into the internal table for an internal function, or
/// `None` for an external one (loaded by the entrypoint-call seam).
fn LookupBackgroundWorkerFunction(libraryname: &[u8], funcname: &[u8]) -> PgResult<Option<usize>> {
    // If loaded from postgres itself, search the InternalBGWorkers array.
    if cstr_eq(libraryname, b"postgres") {
        let fname = cstr_str(funcname);
        for (i, &name) in INTERNAL_BGWORKER_NAMES.iter().enumerate() {
            if fname == name {
                return Ok(Some(i));
            }
        }
        // We can only reach this by programming error.
        return Err(PgError::new(
            ERROR,
            format!("internal function \"{}\" not found", cstr_str(funcname)),
        )
        .with_message_id("internal function \"%s\" not found")
        .with_error_location(loc(1379, "LookupBackgroundWorkerFunction")));
    }

    // Otherwise load from external library (the entrypoint-call seam performs
    // the actual load_external_function + dispatch).
    Ok(None)
}

// ---------------------------------------------------------------------------
// GetBackgroundWorkerTypeByPid
// ---------------------------------------------------------------------------

/// `GetBackgroundWorkerTypeByPid(pid_t pid)` — the `bgw_type` of the worker
/// with PID `pid`, or `None`. (C returns a pointer into a static buffer; here
/// an owned `String`.)
pub fn GetBackgroundWorkerTypeByPid(pid: pid_t) -> Option<String> {
    let mut result: Option<String> = None;

    // LWLockAcquire(BackgroundWorkerLock, LW_SHARED) — infallible in C; the
    // guard is held across the slot scan; Drop is C's LWLockReleaseAll path.
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        BACKGROUND_WORKER_LWLOCK_OFFSET,
        types_storage::LWLockMode::LW_SHARED,
    )
    .expect("BackgroundWorkerLock shared acquire cannot fail");

    {
        let data = BACKGROUND_WORKER_DATA.lock().unwrap();
        let d = data.as_ref().unwrap();
        let mut slotno = 0;
        while slotno < d.total_slots {
            let slot = &d.slot[slotno as usize];
            if slot.pid > 0 && slot.pid == pid {
                result = Some(cstr_lossy(&slot.worker.bgw_type));
                break;
            }
            slotno += 1;
        }
    }

    // LWLockRelease(BackgroundWorkerLock).
    let _ = guard.release();

    result
}

// ---------------------------------------------------------------------------
// Small string helpers mirroring the C ops on fixed-size char arrays.
// ---------------------------------------------------------------------------

/// NUL-terminated length of a fixed-size C char array.
fn cstr_len(buf: &[u8]) -> usize {
    buf.iter().position(|&b| b == 0).unwrap_or(buf.len())
}

/// View a NUL-terminated fixed-size C char array as `&str`.
fn cstr_str(buf: &[u8]) -> &str {
    core::str::from_utf8(&buf[..cstr_len(buf)]).unwrap_or("")
}

/// `strcmp(buf, lit) == 0` against a fixed-size C char array (lit has no NUL).
fn cstr_eq(buf: &[u8], lit: &[u8]) -> bool {
    let n = cstr_len(buf);
    &buf[..n] == lit
}

/// Owned lossy copy of a NUL-terminated fixed-size C char array (for `%s`).
fn cstr_lossy(buf: &[u8]) -> String {
    String::from_utf8_lossy(&buf[..cstr_len(buf)]).into_owned()
}

/// `strcpy(dst, src)` between two fixed-size C char arrays: copy `src` up to
/// its NUL, zero the remainder.
fn strcpy<const N: usize>(dst: &mut [u8; N], src: &[u8; N]) {
    let n = cstr_len(src);
    dst[..n].copy_from_slice(&src[..n]);
    for b in dst[n..].iter_mut() {
        *b = 0;
    }
}

/// `ascii_safe_strlcpy(dst, src, destsiz)` (`utils/adt/ascii.c:174`): copy at
/// most `destsiz - 1` bytes of NUL-terminated `src` into a fresh `[u8; N]`,
/// keeping printable ASCII (`32 <= ch <= 127`) and the whitespace bytes
/// `'\n'`/`'\r'`/`'\t'`, replacing every other byte with `'?'`, then
/// NUL-terminate. Must never `ereport(ERROR)` — it runs in the postmaster.
fn ascii_safe_strlcpy<const N: usize>(src: &[u8; N], destsiz: usize) -> [u8; N] {
    let mut dst = [0u8; N];
    let limit = destsiz.saturating_sub(1).min(N - 1);
    let mut i = 0;
    while i < limit {
        let ch = src[i];
        if ch == 0 {
            break;
        }
        dst[i] = if (32..=127).contains(&ch) {
            // Keep printable ASCII characters.
            ch
        } else if ch == b'\n' || ch == b'\r' || ch == b'\t' {
            // White-space is also OK.
            ch
        } else {
            // Everything else is replaced with '?'.
            b'?'
        };
        i += 1;
    }
    dst[i] = 0;
    dst
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this unit's seams (`backend-postmaster-bgworker-seams`).
pub fn init_seams() {
    backend_postmaster_bgworker_seams::background_worker_main::set(background_worker_main_seam);
    backend_postmaster_bgworker_seams::get_background_worker_pid::set(get_background_worker_pid_seam);
    backend_postmaster_bgworker_seams::register_background_worker::set(register_background_worker_seam);
    backend_postmaster_bgworker_seams::register_dynamic_background_worker::set(
        register_dynamic_background_worker_seam,
    );
    backend_postmaster_bgworker_seams::background_worker_initialize_connection::set(
        background_worker_initialize_connection_seam,
    );
    backend_postmaster_bgworker_seams::background_worker_unblock_signals::set(
        background_worker_unblock_signals_seam,
    );
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    backend_postmaster_bgworker_seams::background_worker_shmem_init::set(BackgroundWorkerShmemInit);
}

/// Marshal for the `background_worker_main` inward seam.
fn background_worker_main_seam(startup_data: &StartupData) -> ! {
    BackgroundWorkerMain(startup_data)
}

/// Marshal for the `get_background_worker_pid` inward seam.
fn get_background_worker_pid_seam(handle: BackgroundWorkerHandle) -> (BgwHandleStatus, i32) {
    GetBackgroundWorkerPid(&handle)
}

/// Marshal for the `register_background_worker` inward seam.
fn register_background_worker_seam(worker: &BackgroundWorker) -> PgResult<()> {
    RegisterBackgroundWorker(worker)
}

/// Marshal for the `register_dynamic_background_worker` inward seam.
fn register_dynamic_background_worker_seam(
    worker: &BackgroundWorker,
) -> PgResult<Option<BackgroundWorkerHandle>> {
    RegisterDynamicBackgroundWorker(worker)
}

/// Marshal for the `background_worker_initialize_connection` inward seam.
fn background_worker_initialize_connection_seam(
    dbname: Option<&str>,
    username: Option<&str>,
    flags: u32,
) -> PgResult<()> {
    BackgroundWorkerInitializeConnection(dbname, username, flags)
}

/// Marshal for the `background_worker_unblock_signals` inward seam.
fn background_worker_unblock_signals_seam() {
    BackgroundWorkerUnblockSignals()
}
