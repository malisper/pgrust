#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{
    AtomicBool, AtomicU32, AtomicU8,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::OnceLock;

use ::elog::elog;
use init_small::globals as g;
use types_error::{PgResult, FATAL};

pub const PM_CHILD_UNUSED: u8 = 0;
pub const PM_CHILD_ASSIGNED: u8 = 1;
pub const PM_CHILD_ACTIVE: u8 = 2;
pub const PM_CHILD_WALSENDER: u8 = 3;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PMSignalReason {
    PMSIGNAL_RECOVERY_STARTED = 0,
    PMSIGNAL_RECOVERY_CONSISTENT = 1,
    PMSIGNAL_BEGIN_HOT_STANDBY = 2,
    PMSIGNAL_ROTATE_LOGFILE = 3,
    PMSIGNAL_START_AUTOVAC_LAUNCHER = 4,
    PMSIGNAL_START_AUTOVAC_WORKER = 5,
    PMSIGNAL_BACKGROUND_WORKER_CHANGE = 6,
    PMSIGNAL_START_WALRECEIVER = 7,
    PMSIGNAL_ADVANCE_STATE_MACHINE = 8,
    PMSIGNAL_XLOG_IS_SHUTDOWN = 9,
}

pub const NUM_PMSIGNALS: usize = PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN as usize + 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuitSignalReason {
    PMQUIT_NOT_SENT = 0,
    PMQUIT_FOR_CRASH = 1,
    PMQUIT_FOR_STOP = 2,
}

impl QuitSignalReason {
    fn from_u32(value: u32) -> QuitSignalReason {
        match value {
            1 => QuitSignalReason::PMQUIT_FOR_CRASH,
            2 => QuitSignalReason::PMQUIT_FOR_STOP,
            _ => QuitSignalReason::PMQUIT_NOT_SENT,
        }
    }
}

// C's volatile sig_atomic_t fields carry no explicit barriers (the kill(2)
// round trip orders them); Release stores / Acquire loads keep that
// publication sound across backend threads without full fences.
struct PMSignalData {
    PMSignalFlags: [AtomicBool; NUM_PMSIGNALS],
    sigquit_reason: AtomicU32,
    num_child_flags: i32,
    PMChildFlags: &'static [AtomicU8],
}

const _: () = assert!(!core::mem::needs_drop::<[AtomicU8; 4]>());

static PM_SIGNAL_STATE: OnceLock<PMSignalData> = OnceLock::new();

thread_local! {
    // Postmaster's untrusting local copy of num_child_flags (C static).
    static NUM_CHILD_FLAGS: Cell<i32> = const { Cell::new(0) };
}

fn state() -> &'static PMSignalData {
    PM_SIGNAL_STATE
        .get()
        .unwrap_or_else(|| panic!("PMSignalState not initialized (PMSignalShmemInit not called)"))
}

pub fn PMSignalShmemSize(max_live_children: i32) -> PgResult<usize> {
    shmem_seams::add_size::call(
        core::mem::size_of::<PMSignalData>(),
        shmem_seams::mul_size::call(max_live_children as usize, 1)?,
    )
}

// C sizes the child-flag array with MaxLivePostmasterChildren() (pmchild.c,
// unported); the caller passes the value, as with lmgr_proc's config.
pub fn PMSignalShmemInit(max_live_children: i32) {
    assert!(max_live_children > 0, "max_live_children not initialized");
    let state = PM_SIGNAL_STATE.get_or_init(|| PMSignalData {
        PMSignalFlags: std::array::from_fn(|_| AtomicBool::new(false)),
        sigquit_reason: AtomicU32::new(QuitSignalReason::PMQUIT_NOT_SENT as u32),
        num_child_flags: max_live_children,
        PMChildFlags: (0..max_live_children)
            .map(|_| AtomicU8::new(PM_CHILD_UNUSED))
            .collect::<Vec<_>>()
            .leak(),
    });
    NUM_CHILD_FLAGS.set(state.num_child_flags);
}

pub fn SendPostmasterSignal(reason: PMSignalReason) {
    // Standalone backend: nothing to signal.
    if !g::IsUnderPostmaster() {
        return;
    }
    state().PMSignalFlags[reason as usize].store(true, Release);
    // C: kill(PostmasterPid, SIGUSR1). The postmaster's handler half (drain
    // via CheckPostmasterSignal) belongs to its event loop; delivery here is
    // the wait-loop kick for the postmaster thread's pid.
    waiteventset_seams::wakeup_other_proc::call(g::PostmasterPid());
}

pub fn CheckPostmasterSignal(reason: PMSignalReason) -> bool {
    let flag = &state().PMSignalFlags[reason as usize];
    // Don't clear a flag we haven't seen set.
    if flag.load(Acquire) {
        flag.store(false, Relaxed);
        return true;
    }
    false
}

pub fn SetQuitSignalReason(reason: QuitSignalReason) {
    state().sigquit_reason.store(reason as u32, Release);
}

pub fn GetQuitSignalReason() -> QuitSignalReason {
    // Signal-handler-reachable: be extra paranoid, as in C.
    if !g::IsUnderPostmaster() {
        return QuitSignalReason::PMQUIT_NOT_SENT;
    }
    match PM_SIGNAL_STATE.get() {
        None => QuitSignalReason::PMQUIT_NOT_SENT,
        Some(state) => QuitSignalReason::from_u32(state.sigquit_reason.load(Acquire)),
    }
}

// Postmaster-only, so no locking (as in C).
pub fn MarkPostmasterChildSlotAssigned(slot: i32) -> PgResult<()> {
    debug_assert!(slot > 0 && slot <= NUM_CHILD_FLAGS.get());
    let flag = &state().PMChildFlags[(slot - 1) as usize];
    if flag.load(Acquire) != PM_CHILD_UNUSED {
        return elog(FATAL, "postmaster child slot is already in use");
    }
    flag.store(PM_CHILD_ASSIGNED, Release);
    Ok(())
}

pub fn MarkPostmasterChildSlotUnassigned(slot: i32) -> bool {
    debug_assert!(slot > 0 && slot <= NUM_CHILD_FLAGS.get());
    let flag = &state().PMChildFlags[(slot - 1) as usize];
    // May legitimately already be UNUSED: postmaster.c can call this twice
    // for a crashed child, so no state assertion.
    let result = flag.load(Acquire) == PM_CHILD_ASSIGNED;
    flag.store(PM_CHILD_UNUSED, Release);
    result
}

pub fn IsPostmasterChildWalSender(slot: i32) -> bool {
    debug_assert!(slot > 0 && slot <= NUM_CHILD_FLAGS.get());
    state().PMChildFlags[(slot - 1) as usize].load(Acquire) == PM_CHILD_WALSENDER
}

pub fn RegisterPostmasterChildActive() {
    let state = state();
    let slot = g::MyPMChildSlot();
    debug_assert!(slot > 0 && slot <= state.num_child_flags);
    let flag = &state.PMChildFlags[(slot - 1) as usize];
    debug_assert_eq!(flag.load(Acquire), PM_CHILD_ASSIGNED);
    flag.store(PM_CHILD_ACTIVE, Release);

    ipc_seams::on_shmem_exit::call(MarkPostmasterChildInactive, 0);
}

// C asserts am_walsender; walsender.c is unported, so the assert is dropped.
pub fn MarkPostmasterChildWalSender() {
    let state = state();
    let slot = g::MyPMChildSlot();
    debug_assert!(slot > 0 && slot <= state.num_child_flags);
    let flag = &state.PMChildFlags[(slot - 1) as usize];
    debug_assert_eq!(flag.load(Acquire), PM_CHILD_ACTIVE);
    flag.store(PM_CHILD_WALSENDER, Release);
}

fn MarkPostmasterChildInactive(_code: i32, _arg: usize) {
    let state = state();
    let slot = g::MyPMChildSlot();
    debug_assert!(slot > 0 && slot <= state.num_child_flags);
    let flag = &state.PMChildFlags[(slot - 1) as usize];
    debug_assert!(matches!(
        flag.load(Acquire),
        PM_CHILD_ACTIVE | PM_CHILD_WALSENDER
    ));
    flag.store(PM_CHILD_ASSIGNED, Release);
}

// C watches the postmaster through a parent-death signal and/or the
// postmaster_alive_fds pipe; neither survives one-process-many-threads (a
// thread's exit closes no pipe and raises no PDEATHSIG). Death observation
// must be redesigned by the postmaster/waiteventset port, so these panic
// loudly instead of stubbing "alive".
pub fn PostmasterIsAlive() -> bool {
    PostmasterIsAliveInternal()
}

pub fn PostmasterIsAliveInternal() -> bool {
    panic!("postmaster death monitoring is not ported (threaded-model owner: postmaster)");
}

pub fn PostmasterDeathSignalInit() {
    panic!("postmaster death signaling is not ported (threaded-model owner: postmaster)");
}

pub fn init_seams() {
    pmsignal_seams::register_postmaster_child_active::set(RegisterPostmasterChildActive);
}

#[cfg(test)]
mod tests;
