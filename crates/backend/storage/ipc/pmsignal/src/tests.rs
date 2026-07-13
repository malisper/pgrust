use super::*;
use std::sync::{Mutex, Once};

static WAKEUPS: Mutex<Vec<i32>> = Mutex::new(Vec::new());
static EXIT_CALLBACKS: Mutex<Vec<(fn(i32, usize), usize)>> = Mutex::new(Vec::new());

const MAX_LIVE_CHILDREN: i32 = 8;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("mul_size overflow")));
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("add_size overflow")));
        ipc_seams::on_shmem_exit::set(|f, arg| EXIT_CALLBACKS.lock().unwrap().push((f, arg)));
        waiteventset_seams::wakeup_postmaster::set(|| WAKEUPS.lock().unwrap().push(0));
        PMSignalShmemInit(MAX_LIVE_CHILDREN);
        init_seams();
    });
    NUM_CHILD_FLAGS.set(MAX_LIVE_CHILDREN);
}

#[test]
fn shmem_shape_matches_c() {
    setup();
    assert_eq!(state().num_child_flags, MAX_LIVE_CHILDREN);
    assert_eq!(state().PMChildFlags.len(), MAX_LIVE_CHILDREN as usize);
    assert_eq!(
        PMSignalShmemSize(MAX_LIVE_CHILDREN).unwrap(),
        core::mem::size_of::<PMSignalData>() + MAX_LIVE_CHILDREN as usize
    );
    assert_eq!(NUM_PMSIGNALS, 10);
}

#[test]
fn send_and_check_postmaster_signal() {
    setup();
    let _guard = serial();
    g::SetIsUnderPostmaster(true);
    g::SetPostmasterPid(4321);

    WAKEUPS.lock().unwrap().clear();
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER);
    // The wake seam is pid-less now (the postmaster is the one well-known
    // route); the assertion is that exactly one PM kick was delivered.
    assert_eq!(WAKEUPS.lock().unwrap().len(), 1);
    assert!(CheckPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER));
    assert!(!CheckPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER));
    assert!(!CheckPostmasterSignal(PMSignalReason::PMSIGNAL_ROTATE_LOGFILE));
}

#[test]
fn standalone_send_is_a_noop() {
    setup();
    let _guard = serial();
    g::SetIsUnderPostmaster(false);
    let wakeups_before = WAKEUPS.lock().unwrap().len();
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_ROTATE_LOGFILE);
    assert_eq!(WAKEUPS.lock().unwrap().len(), wakeups_before);
    assert_eq!(
        state().PMSignalFlags[PMSignalReason::PMSIGNAL_ROTATE_LOGFILE as usize].load(Acquire),
        false
    );
}

#[test]
fn quit_signal_reason_roundtrip() {
    setup();
    let _guard = serial();
    g::SetIsUnderPostmaster(false);
    assert_eq!(GetQuitSignalReason(), QuitSignalReason::PMQUIT_NOT_SENT);

    g::SetIsUnderPostmaster(true);
    SetQuitSignalReason(QuitSignalReason::PMQUIT_FOR_CRASH);
    assert_eq!(GetQuitSignalReason(), QuitSignalReason::PMQUIT_FOR_CRASH);
    SetQuitSignalReason(QuitSignalReason::PMQUIT_FOR_STOP);
    assert_eq!(GetQuitSignalReason(), QuitSignalReason::PMQUIT_FOR_STOP);
    SetQuitSignalReason(QuitSignalReason::PMQUIT_NOT_SENT);
}

#[test]
fn child_slot_lifecycle() {
    setup();
    let _guard = serial();
    let slot = 3;
    g::SetMyPMChildSlot(slot);

    MarkPostmasterChildSlotAssigned(slot).unwrap();
    assert!(MarkPostmasterChildSlotAssigned(slot).is_err());
    assert!(!IsPostmasterChildWalSender(slot));

    let before = EXIT_CALLBACKS.lock().unwrap().len();
    RegisterPostmasterChildActive();
    assert_eq!(EXIT_CALLBACKS.lock().unwrap().len(), before + 1);
    assert_eq!(
        state().PMChildFlags[(slot - 1) as usize].load(Acquire),
        PM_CHILD_ACTIVE
    );

    MarkPostmasterChildWalSender();
    assert!(IsPostmasterChildWalSender(slot));

    let (f, arg) = *EXIT_CALLBACKS.lock().unwrap().last().unwrap();
    f(0, arg);
    assert_eq!(
        state().PMChildFlags[(slot - 1) as usize].load(Acquire),
        PM_CHILD_ASSIGNED
    );

    assert!(MarkPostmasterChildSlotUnassigned(slot));
    assert!(!MarkPostmasterChildSlotUnassigned(slot));
}

#[test]
fn register_seam_delegates() {
    setup();
    let _guard = serial();
    assert!(pmsignal_seams::register_postmaster_child_active::is_installed());
    let slot = 5;
    g::SetMyPMChildSlot(slot);
    MarkPostmasterChildSlotAssigned(slot).unwrap();
    pmsignal_seams::register_postmaster_child_active::call();
    assert_eq!(
        state().PMChildFlags[(slot - 1) as usize].load(Acquire),
        PM_CHILD_ACTIVE
    );
}

#[test]
fn reset_after_crash_restores_boot_image() {
    setup();
    let _guard = serial();
    g::SetIsUnderPostmaster(true);
    g::SetPostmasterPid(4321);

    SendPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_STARTED);
    SetQuitSignalReason(QuitSignalReason::PMQUIT_FOR_CRASH);
    let slot = 7;
    MarkPostmasterChildSlotAssigned(slot).unwrap();

    PMSignalShmemResetAfterCrash();

    assert!(!CheckPostmasterSignal(PMSignalReason::PMSIGNAL_RECOVERY_STARTED));
    assert_eq!(GetQuitSignalReason(), QuitSignalReason::PMQUIT_NOT_SENT);
    for flag in state().PMChildFlags {
        assert_eq!(flag.load(Acquire), PM_CHILD_UNUSED);
    }
    MarkPostmasterChildSlotAssigned(slot).unwrap();
    assert!(MarkPostmasterChildSlotUnassigned(slot));
}

#[test]
fn postmaster_death_probes_panic_loudly() {
    let alive = std::panic::catch_unwind(PostmasterIsAlive);
    assert!(alive.is_err());
    let init = std::panic::catch_unwind(PostmasterDeathSignalInit);
    assert!(init.is_err());
}
