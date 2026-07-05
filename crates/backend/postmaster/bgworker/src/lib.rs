//! bgworker.c under the thread model, dynamic workers only. Divergences:
//! bgw_library_name/bgw_function_name collapse to a direct `bgw_main` fn
//! pointer (one address space, no cross-process symbol lookup); C's lockless
//! postmaster/backend slot protocol (read/write barriers, in_use handoff)
//! collapses into one std Mutex (cold supervisor path, like pmchild); static
//! RegisterBackgroundWorker and restart scheduling (bgw_restart_time >= 0)
//! are loud panics this phase.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::sync::Mutex;

use elog::{elog as report, ereport};
use init_small::globals as g;
use types_core::{pid_t, BackendType, InvalidOid, ProcessingMode};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, ERRCODE_ADMIN_SHUTDOWN,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, FATAL, LOG,
};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

#[cfg(test)]
mod tests;

pub const BGWORKER_SHMEM_ACCESS: i32 = 0x0001;
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: i32 = 0x0002;
pub const BGWORKER_CLASS_PARALLEL: i32 = 0x0010;

pub const BGW_DEFAULT_RESTART_INTERVAL: i32 = 60;
pub const BGW_NEVER_RESTART: i32 = -1;
pub const BGW_MAXLEN: usize = 96;
pub const BGW_EXTRALEN: usize = 128;
pub const MAX_PARALLEL_WORKER_LIMIT: u32 = 1024;

pub const BGWORKER_BYPASS_ALLOWCONN: u32 = 0x0001;
pub const BGWORKER_BYPASS_ROLELOGINCHECK: u32 = 0x0002;

const InvalidPid: pid_t = -1;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BGWORKER_SHUTDOWN: u32 = PG_WAIT_IPC + 5;
const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC + 6;

const SRC: &str = "src/backend/postmaster/bgworker.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

pub type BgworkerMainFn = fn(u64) -> PgResult<()>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BgWorkerStartTime {
    PostmasterStart,
    ConsistentState,
    RecoveryFinished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BgwHandleStatus {
    BGWH_STARTED,
    BGWH_NOT_YET_STARTED,
    BGWH_STOPPED,
    BGWH_POSTMASTER_DIED,
}

#[derive(Clone)]
pub struct BackgroundWorker {
    pub bgw_name: String,
    pub bgw_type: String,
    pub bgw_flags: i32,
    pub bgw_start_time: BgWorkerStartTime,
    pub bgw_restart_time: i32,
    pub bgw_main: BgworkerMainFn,
    pub bgw_main_arg: u64,
    pub bgw_extra: [u8; BGW_EXTRALEN],
    pub bgw_notify_pid: pid_t,
}

#[derive(Clone, Copy, Debug)]
pub struct BackgroundWorkerHandle {
    pub slot: i32,
    pub generation: u64,
}

struct BackgroundWorkerSlot {
    in_use: bool,
    terminate: bool,
    pid: pid_t, // InvalidPid = not started yet; 0 = dead
    generation: u64,
    worker: Option<BackgroundWorker>,
}

struct RegisteredBgWorker {
    worker: BackgroundWorker,
    pid: pid_t,
    crashed_at: i64,
    shmem_slot: i32,
    terminate: bool,
}

struct Registry {
    total_slots: usize,
    parallel_register_count: u32,
    parallel_terminate_count: u32,
    slots: Vec<BackgroundWorkerSlot>,
    registered: Vec<Option<RegisteredBgWorker>>,
}

static REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);

fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let reg = guard
        .as_mut()
        .unwrap_or_else(|| panic!("BackgroundWorkerData accessed before BackgroundWorkerShmemInit"));
    f(reg)
}

fn send_signal(pid: pid_t, signo: i32) {
    let _ = procsignal::SendThreadSignal(pid, signo);
}

pub fn BackgroundWorkerShmemInit() {
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let total_slots = g::max_worker_processes() as usize;
    let mut registered = guard.take().map(|r| r.registered).unwrap_or_default();
    let mut slots: Vec<BackgroundWorkerSlot> = (0..total_slots)
        .map(|_| BackgroundWorkerSlot {
            in_use: false,
            terminate: false,
            pid: 0,
            generation: 0,
            worker: None,
        })
        .collect();
    let mut slotno = 0usize;
    for rw in registered.iter_mut().flatten() {
        assert!(slotno < total_slots);
        rw.worker.bgw_notify_pid = 0; // might be reinit after crash
        slots[slotno] = BackgroundWorkerSlot {
            in_use: true,
            terminate: false,
            pid: InvalidPid,
            generation: 0,
            worker: Some(rw.worker.clone()),
        };
        rw.shmem_slot = slotno as i32;
        slotno += 1;
    }
    *guard = Some(Registry {
        total_slots,
        parallel_register_count: 0,
        parallel_terminate_count: 0,
        slots,
        registered,
    });
}

fn find_rw_by_slot(reg: &Registry, slotno: i32) -> Option<usize> {
    reg.registered
        .iter()
        .position(|rw| rw.as_ref().is_some_and(|rw| rw.shmem_slot == slotno))
}

fn rw_ref(reg: &Registry, idx: usize) -> &RegisteredBgWorker {
    reg.registered[idx]
        .as_ref()
        .unwrap_or_else(|| panic!("bgworker: stale registered-worker index {idx}"))
}

fn rw_mut(reg: &mut Registry, idx: usize) -> &mut RegisteredBgWorker {
    reg.registered[idx]
        .as_mut()
        .unwrap_or_else(|| panic!("bgworker: stale registered-worker index {idx}"))
}

pub fn BackgroundWorkerStateChange(allow_new_workers: bool) {
    let mut notifies: Vec<(pid_t, i32)> = Vec::new();
    with_registry(|reg| {
        if g::max_worker_processes() != reg.total_slots as i32 {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "inconsistent background worker state (\"max_worker_processes\"={}, total slots={})",
                    g::max_worker_processes(),
                    reg.total_slots
                ))
                .finish(loc(284, "BackgroundWorkerStateChange"));
            return;
        }

        for slotno in 0..reg.total_slots {
            if !reg.slots[slotno].in_use {
                continue;
            }

            if let Some(idx) = find_rw_by_slot(reg, slotno as i32) {
                let slot_terminate = reg.slots[slotno].terminate;
                let rw = rw_mut(reg, idx);
                if slot_terminate && !rw.terminate {
                    rw.terminate = true;
                    if rw.pid != 0 {
                        notifies.push((rw.pid, libc::SIGTERM));
                    } else {
                        // Report never-started, now-terminated worker as dead.
                        let pid = rw.pid;
                        let notify_pid = rw.worker.bgw_notify_pid;
                        let shmem_slot = rw.shmem_slot;
                        reg.slots[shmem_slot as usize].pid = pid;
                        if notify_pid != 0 {
                            notifies.push((notify_pid, libc::SIGUSR1));
                        }
                    }
                }
                continue;
            }

            if !allow_new_workers {
                reg.slots[slotno].terminate = true;
            }

            if reg.slots[slotno].terminate {
                let slot = &mut reg.slots[slotno];
                let worker = slot.worker.take();
                let notify_pid = worker.as_ref().map_or(0, |w| w.bgw_notify_pid);
                if worker.as_ref().is_some_and(|w| w.bgw_flags & BGWORKER_CLASS_PARALLEL != 0) {
                    reg.parallel_terminate_count = reg.parallel_terminate_count.wrapping_add(1);
                }
                slot.pid = 0;
                slot.in_use = false;
                if notify_pid != 0 {
                    notifies.push((notify_pid, libc::SIGUSR1));
                }
                continue;
            }

            // C re-copies strings ascii_safe against corrupted shmem; intact
            // Rust values make the plain clone equivalent.
            let mut worker = reg.slots[slotno].worker.clone().expect("in-use slot has a worker");
            if worker.bgw_notify_pid != 0
                && pmchild_seams::find_postmaster_child_by_pid::call(worker.bgw_notify_pid)
                    .is_none()
            {
                let _ = report(
                    DEBUG1,
                    format!("worker notification PID {} is not valid", worker.bgw_notify_pid),
                );
                worker.bgw_notify_pid = 0;
            }

            let _ = report(
                DEBUG1,
                format!("registering background worker \"{}\"", worker.bgw_name),
            );

            let rw = RegisteredBgWorker {
                worker,
                pid: 0,
                crashed_at: 0,
                shmem_slot: slotno as i32,
                terminate: false,
            };
            match reg.registered.iter_mut().find(|e| e.is_none()) {
                Some(hole) => *hole = Some(rw),
                None => reg.registered.push(Some(rw)),
            }
        }
    });
    for (pid, signo) in notifies {
        send_signal(pid, signo);
    }
}

fn forget_locked(reg: &mut Registry, idx: usize) {
    let rw = reg.registered[idx]
        .take()
        .unwrap_or_else(|| panic!("bgworker: stale registered-worker index {idx}"));
    let slot = &mut reg.slots[rw.shmem_slot as usize];
    debug_assert!(slot.in_use);
    if rw.worker.bgw_flags & BGWORKER_CLASS_PARALLEL != 0 {
        reg.parallel_terminate_count = reg.parallel_terminate_count.wrapping_add(1);
    }
    slot.in_use = false;
    slot.worker = None;
    let _ = report(
        DEBUG1,
        format!("unregistering background worker \"{}\"", rw.worker.bgw_name),
    );
}

pub fn ForgetBackgroundWorker(idx: usize) {
    with_registry(|reg| forget_locked(reg, idx));
}

pub fn ReportBackgroundWorkerPID(idx: usize) {
    let notify_pid = with_registry(|reg| {
        let rw = rw_ref(reg, idx);
        let (pid, shmem_slot, notify_pid) = (rw.pid, rw.shmem_slot, rw.worker.bgw_notify_pid);
        reg.slots[shmem_slot as usize].pid = pid;
        notify_pid
    });
    if notify_pid != 0 {
        send_signal(notify_pid, libc::SIGUSR1);
    }
}

pub fn ReportBackgroundWorkerExit(idx: usize) {
    let notify_pid = with_registry(|reg| {
        let rw = rw_ref(reg, idx);
        let (pid, shmem_slot, notify_pid) = (rw.pid, rw.shmem_slot, rw.worker.bgw_notify_pid);
        let (terminate, restart_time) = (rw.terminate, rw.worker.bgw_restart_time);
        reg.slots[shmem_slot as usize].pid = pid;
        // Deregister before notifying so the waiter can reuse the slot (C
        // narrows the same window).
        if terminate || restart_time == BGW_NEVER_RESTART {
            forget_locked(reg, idx);
        }
        notify_pid
    });
    if notify_pid != 0 {
        send_signal(notify_pid, libc::SIGUSR1);
    }
}

// GetBackgroundWorkerTypeByPid (bgworker.c): bgw_type from the shmem slot
// whose live pid matches.
pub fn GetBackgroundWorkerTypeByPid(pid: pid_t) -> Option<String> {
    with_registry(|reg| {
        reg.slots
            .iter()
            .find(|slot| slot.pid > 0 && slot.pid == pid)
            .and_then(|slot| slot.worker.as_ref().map(|w| w.bgw_type.clone()))
    })
}

pub fn BackgroundWorkerStopNotifications(pid: pid_t) {
    with_registry(|reg| {
        for rw in reg.registered.iter_mut().flatten() {
            if rw.worker.bgw_notify_pid == pid {
                rw.worker.bgw_notify_pid = 0;
            }
        }
    });
}

pub fn ForgetUnstartedBackgroundWorkers() {
    let mut notifies: Vec<pid_t> = Vec::new();
    with_registry(|reg| {
        for idx in 0..reg.registered.len() {
            let Some(rw) = reg.registered[idx].as_ref() else { continue };
            let notify_pid = rw.worker.bgw_notify_pid;
            if reg.slots[rw.shmem_slot as usize].pid == InvalidPid && notify_pid != 0 {
                forget_locked(reg, idx);
                notifies.push(notify_pid);
            }
        }
    });
    for pid in notifies {
        send_signal(pid, libc::SIGUSR1);
    }
}

pub fn ResetBackgroundWorkerCrashTimes() {
    with_registry(|reg| {
        for idx in 0..reg.registered.len() {
            let Some(rw) = reg.registered[idx].as_mut() else { continue };
            if rw.worker.bgw_restart_time == BGW_NEVER_RESTART {
                forget_locked(reg, idx);
            } else {
                // Parallel workers are always BGW_NEVER_RESTART: the
                // register/terminate accounting can't survive a crash cycle.
                debug_assert!(rw.worker.bgw_flags & BGWORKER_CLASS_PARALLEL == 0);
                rw.crashed_at = 0;
                rw.pid = 0;
                rw.worker.bgw_notify_pid = 0;
            }
        }
    });
}

fn SanityCheckBackgroundWorker(worker: &mut BackgroundWorker) -> PgResult<()> {
    if worker.bgw_flags & BGWORKER_SHMEM_ACCESS == 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "background worker \"{}\": background workers without shared memory access are not supported",
                worker.bgw_name
            ))
            .finish(loc(670, "SanityCheckBackgroundWorker"));
    }

    if worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION != 0
        && worker.bgw_start_time == BgWorkerStartTime::PostmasterStart
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "background worker \"{}\": cannot request database access if starting at postmaster start",
                worker.bgw_name
            ))
            .finish(loc(681, "SanityCheckBackgroundWorker"));
    }

    // USECS_PER_DAY / 1000
    if (worker.bgw_restart_time < 0 && worker.bgw_restart_time != BGW_NEVER_RESTART)
        || worker.bgw_restart_time > 86_400_000
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "background worker \"{}\": invalid restart interval",
                worker.bgw_name
            ))
            .finish(loc(695, "SanityCheckBackgroundWorker"));
    }

    if worker.bgw_restart_time != BGW_NEVER_RESTART
        && worker.bgw_flags & BGWORKER_CLASS_PARALLEL != 0
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "background worker \"{}\": parallel workers may not be configured for restart",
                worker.bgw_name
            ))
            .finish(loc(710, "SanityCheckBackgroundWorker"));
    }

    if worker.bgw_type.is_empty() {
        worker.bgw_type = worker.bgw_name.clone();
    }

    Ok(())
}

fn bgw_truncate(s: &str) -> String {
    // BGW_MAXLEN counts C's terminating NUL.
    let max = BGW_MAXLEN - 1;
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

pub fn RegisterBackgroundWorker(_worker: &BackgroundWorker) {
    panic!("RegisterBackgroundWorker: static bgworker registration unported (shared_preload_libraries path, including its \"too many background workers\" error)");
}

pub fn RegisterDynamicBackgroundWorker(
    mut worker: BackgroundWorker,
) -> PgResult<Option<BackgroundWorkerHandle>> {
    if !g::IsUnderPostmaster() {
        return Ok(None);
    }

    worker.bgw_name = bgw_truncate(&worker.bgw_name);
    worker.bgw_type = bgw_truncate(&worker.bgw_type);
    SanityCheckBackgroundWorker(&mut worker)?;

    if worker.bgw_restart_time != BGW_NEVER_RESTART {
        panic!("RegisterDynamicBackgroundWorker: bgworker restart machinery unported (bgw_restart_time >= 0)");
    }

    let parallel = worker.bgw_flags & BGWORKER_CLASS_PARALLEL != 0;

    let handle = with_registry(|reg| {
        if parallel
            && reg.parallel_register_count.wrapping_sub(reg.parallel_terminate_count)
                >= g::max_parallel_workers() as u32
        {
            debug_assert!(
                reg.parallel_register_count.wrapping_sub(reg.parallel_terminate_count)
                    <= MAX_PARALLEL_WORKER_LIMIT
            );
            return None;
        }

        for slotno in 0..reg.total_slots {
            let slot = &mut reg.slots[slotno];
            if !slot.in_use {
                slot.worker = Some(worker.clone());
                slot.pid = InvalidPid;
                slot.generation += 1;
                slot.terminate = false;
                let generation = slot.generation;
                if parallel {
                    reg.parallel_register_count = reg.parallel_register_count.wrapping_add(1);
                }
                slot.in_use = true;
                return Some(BackgroundWorkerHandle { slot: slotno as i32, generation });
            }
        }
        None
    });

    if handle.is_some() {
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE);
    }

    Ok(handle)
}

pub fn GetBackgroundWorkerPid(handle: &BackgroundWorkerHandle) -> (BgwHandleStatus, pid_t) {
    let pid = with_registry(|reg| {
        debug_assert!((handle.slot as usize) < reg.total_slots);
        let slot = &reg.slots[handle.slot as usize];
        if handle.generation != slot.generation || !slot.in_use {
            0
        } else {
            slot.pid
        }
    });

    if pid == 0 {
        (BgwHandleStatus::BGWH_STOPPED, 0)
    } else if pid == InvalidPid {
        (BgwHandleStatus::BGWH_NOT_YET_STARTED, 0)
    } else {
        (BgwHandleStatus::BGWH_STARTED, pid)
    }
}

pub fn WaitForBackgroundWorkerStartup(
    handle: &BackgroundWorkerHandle,
) -> PgResult<(BgwHandleStatus, pid_t)> {
    loop {
        postgres::check_for_interrupts()?;

        let (status, pid) = GetBackgroundWorkerPid(handle);
        if status != BgwHandleStatus::BGWH_NOT_YET_STARTED {
            return Ok((status, pid));
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_STARTUP,
        )?;
        if rc & WL_POSTMASTER_DEATH != 0 {
            return Ok((BgwHandleStatus::BGWH_POSTMASTER_DIED, 0));
        }
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }
    }
}

pub fn WaitForBackgroundWorkerShutdown(handle: &BackgroundWorkerHandle) -> PgResult<BgwHandleStatus> {
    loop {
        postgres::check_for_interrupts()?;

        let (status, _pid) = GetBackgroundWorkerPid(handle);
        if status == BgwHandleStatus::BGWH_STOPPED {
            return Ok(status);
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        )?;
        if rc & WL_POSTMASTER_DEATH != 0 {
            return Ok(BgwHandleStatus::BGWH_POSTMASTER_DIED);
        }
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }
    }
}

pub fn TerminateBackgroundWorker(handle: &BackgroundWorkerHandle) {
    let signal_postmaster = with_registry(|reg| {
        debug_assert!((handle.slot as usize) < reg.total_slots);
        let slot = &mut reg.slots[handle.slot as usize];
        if handle.generation == slot.generation {
            slot.terminate = true;
            true
        } else {
            false
        }
    });
    if signal_postmaster {
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE);
    }
}

pub fn GetBackgroundWorkerTypeByPid(pid: pid_t) -> Option<String> {
    with_registry(|reg| {
        reg.slots
            .iter()
            .find(|s| s.pid > 0 && s.pid == pid)
            .and_then(|s| s.worker.as_ref().map(|w| w.bgw_type.clone()))
    })
}

pub fn find_registered_worker_by_pid(pid: pid_t) -> Option<usize> {
    with_registry(|reg| {
        reg.registered
            .iter()
            .position(|rw| rw.as_ref().is_some_and(|rw| rw.pid == pid && pid != 0))
    })
}

pub fn registered_indexes() -> Vec<usize> {
    with_registry(|reg| {
        (0..reg.registered.len()).filter(|&i| reg.registered[i].is_some()).collect()
    })
}

pub fn rw_pid(idx: usize) -> pid_t {
    with_registry(|reg| rw_ref(reg, idx).pid)
}

pub fn set_rw_pid(idx: usize, pid: pid_t) {
    with_registry(|reg| rw_mut(reg, idx).pid = pid);
}

pub fn rw_crashed_at(idx: usize) -> i64 {
    with_registry(|reg| rw_ref(reg, idx).crashed_at)
}

pub fn set_rw_crashed_at(idx: usize, ts: i64) {
    with_registry(|reg| rw_mut(reg, idx).crashed_at = ts);
}

pub fn rw_terminate(idx: usize) -> bool {
    with_registry(|reg| rw_ref(reg, idx).terminate)
}

pub fn set_rw_terminate(idx: usize, terminate: bool) {
    with_registry(|reg| rw_mut(reg, idx).terminate = terminate);
}

pub fn rw_restart_time(idx: usize) -> i32 {
    with_registry(|reg| rw_ref(reg, idx).worker.bgw_restart_time)
}

pub fn rw_start_time(idx: usize) -> BgWorkerStartTime {
    with_registry(|reg| rw_ref(reg, idx).worker.bgw_start_time)
}

pub fn rw_notify_pid(idx: usize) -> pid_t {
    with_registry(|reg| rw_ref(reg, idx).worker.bgw_notify_pid)
}

pub fn rw_shmem_slot(idx: usize) -> i32 {
    with_registry(|reg| rw_ref(reg, idx).shmem_slot)
}

pub fn rw_name(idx: usize) -> String {
    with_registry(|reg| rw_ref(reg, idx).worker.bgw_name.clone())
}

pub fn rw_type(idx: usize) -> String {
    with_registry(|reg| rw_ref(reg, idx).worker.bgw_type.clone())
}

pub fn slot_generation(slotno: i32) -> u64 {
    with_registry(|reg| reg.slots[slotno as usize].generation)
}

thread_local! {
    static MY_BGWORKER_ENTRY: RefCell<Option<BackgroundWorker>> = const { RefCell::new(None) };
}

pub fn MyBgworkerEntry() -> Option<BackgroundWorker> {
    MY_BGWORKER_ENTRY.with(|e| e.borrow().clone())
}

pub fn bgworker_die() -> PgResult<()> {
    let bgw_type = MY_BGWORKER_ENTRY
        .with(|e| e.borrow().as_ref().map(|w| w.bgw_type.clone()))
        .unwrap_or_default();
    ereport(FATAL)
        .errcode(ERRCODE_ADMIN_SHUTDOWN)
        .errmsg(format!(
            "terminating background worker \"{bgw_type}\" due to administrator command"
        ))
        .finish(loc(732, "bgworker_die"))
}

fn install_signal_handlers(db_connection: bool) {
    use procsignal::ThreadSignalHandler::{Fallible, Ignore, Simple};

    if db_connection {
        procsignal::pqsignal_thread(libc::SIGINT, Simple(postgres::StatementCancelHandler));
        procsignal::pqsignal_thread(libc::SIGUSR1, Simple(procsignal::procsignal_sigusr1_handler));
        procsignal::pqsignal_thread(libc::SIGFPE, Fallible(postgres::FloatExceptionHandler));
    } else {
        procsignal::pqsignal_thread(libc::SIGINT, Ignore);
        procsignal::pqsignal_thread(libc::SIGUSR1, Ignore);
        procsignal::pqsignal_thread(libc::SIGFPE, Ignore);
    }
    procsignal::pqsignal_thread(libc::SIGTERM, Fallible(bgworker_die));
    // SIGQUIT disposition installed by the launch path.
    procsignal::pqsignal_thread(libc::SIGHUP, Ignore);

    timeout_seams::initialize_timeouts::call();

    procsignal::pqsignal_thread(libc::SIGPIPE, Ignore);
    procsignal::pqsignal_thread(libc::SIGUSR2, Ignore);
    procsignal::pqsignal_thread(libc::SIGCHLD, Ignore);
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn BackgroundWorkerMain(startup_data: &StartupData) -> ! {
    let my_pid = g::MyProcPid();

    let StartupData::BgWorker(d) = startup_data else {
        fatal_exit(&PgError::new(FATAL, "unable to find bgworker entry"));
    };
    let worker = with_registry(|reg| {
        let slot = &reg.slots[d.slot as usize];
        if slot.in_use && slot.generation == d.generation {
            slot.worker.clone()
        } else {
            None
        }
    });
    let Some(worker) = worker else {
        fatal_exit(&PgError::new(FATAL, "unable to find bgworker entry"));
    };
    MY_BGWORKER_ENTRY.with(|e| *e.borrow_mut() = Some(worker.clone()));

    miscinit::SetMyBackendType(BackendType::BgWorker);

    debug_assert!(miscinit::IsInitProcessingMode());

    let post_auth_delay = guc_tables::vars::PostAuthDelay.read();
    if post_auth_delay > 0 {
        std::thread::sleep(std::time::Duration::from_secs(post_auth_delay as u64));
    }

    install_signal_handlers(worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION != 0);

    // sigsetjmp(local_sigjmp_buf) equivalent: an Err out of the body reports
    // to the log (and the parallel leader, once attached) then exits(1).
    match run_worker_body(&worker) {
        Ok(()) => ipc::proc_exit(0, my_pid),
        Err(e) => {
            g::HoldInterrupts();
            BackgroundWorkerUnblockSignals();
            elog::emit_error_report_for(&e);
            ipc::proc_exit(1, my_pid)
        }
    }
}

fn run_worker_body(worker: &BackgroundWorker) -> PgResult<()> {
    lmgr_proc::InitProcess(BackendType::BgWorker)?;
    postinit::BaseInit()?;
    (worker.bgw_main)(worker.bgw_main_arg)
}

fn initialize_connection(
    dbname: Option<&str>,
    dboid: types_core::Oid,
    username: Option<&str>,
    useroid: types_core::Oid,
    flags: u32,
    line: i32,
    func: &'static str,
) -> PgResult<()> {
    let worker = MyBgworkerEntry().expect("MyBgworkerEntry is not set");

    let mut init_flags: u32 = 0; // never honor session_preload_libraries
    if flags & BGWORKER_BYPASS_ALLOWCONN != 0 {
        init_flags |= postinit::INIT_PG_OVERRIDE_ALLOW_CONNS;
    }
    if flags & BGWORKER_BYPASS_ROLELOGINCHECK != 0 {
        init_flags |= postinit::INIT_PG_OVERRIDE_ROLE_LOGIN;
    }

    if worker.bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION == 0 {
        return ereport(FATAL)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("database connection requirement not indicated during registration")
            .finish(loc(line, func));
    }

    let top = mcx::MemoryContext::new("BackgroundWorkerInit");
    postinit::InitPostgres(top.mcx(), dbname, dboid, username, useroid, init_flags, None)?;

    if !miscinit::IsInitProcessingMode() {
        return ereport(ERROR)
            .errmsg("invalid processing mode in background worker")
            .finish(loc(line, func));
    }
    miscinit::SetProcessingMode(ProcessingMode::NormalProcessing);
    Ok(())
}

pub fn BackgroundWorkerInitializeConnection(
    dbname: Option<&str>,
    username: Option<&str>,
    flags: u32,
) -> PgResult<()> {
    initialize_connection(
        dbname,
        InvalidOid,
        username,
        InvalidOid,
        flags,
        893,
        "BackgroundWorkerInitializeConnection",
    )
}

pub fn BackgroundWorkerInitializeConnectionByOid(
    dboid: types_core::Oid,
    useroid: types_core::Oid,
    flags: u32,
) -> PgResult<()> {
    initialize_connection(
        None,
        dboid,
        None,
        useroid,
        flags,
        927,
        "BackgroundWorkerInitializeConnectionByOid",
    )
}

pub fn BackgroundWorkerBlockSignals() {
    libpq_pqsignal::block_signals();
}

pub fn BackgroundWorkerUnblockSignals() {
    libpq_pqsignal::unblock_signals();
}

pub fn init_seams() {
    bgworker_seams::get_background_worker_type_by_pid::set(GetBackgroundWorkerTypeByPid);
    bgworker_seams::background_worker_stopped::set(|slot, generation| {
        let handle = BackgroundWorkerHandle { slot, generation };
        matches!(
            GetBackgroundWorkerPid(&handle).0,
            BgwHandleStatus::BGWH_STOPPED | BgwHandleStatus::BGWH_POSTMASTER_DIED
        )
    });
}
