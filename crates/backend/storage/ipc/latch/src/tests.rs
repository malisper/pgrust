use super::*;
use init_small::globals::{SetIsUnderPostmaster, SetMyLatch, SetMyProcPid};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Mutex, Once};
use types_storage::waiteventset::WaitEvent;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Create { nevents: i32 },
    CreateCurrentOwner { nevents: i32 },
    Add { set: usize, events: u32, fd: pgsocket, latch: Option<LatchHandle> },
    Modify { set: usize, pos: i32, events: u32, latch: Option<LatchHandle> },
    Wait { set: usize, timeout: i64, wait_event_info: u32 },
    Free { set: usize },
    WakeupMyProc,
    WakeupOtherProc { pid: i32 },
}

struct MockState {
    calls: Vec<Call>,
    next_set: usize,
    add_pos: i32,
    wait_result: Result<Option<WaitEvent>, String>,
}

static MOCK: Mutex<Option<MockState>> = Mutex::new(None);

fn with_mock<R>(f: impl FnOnce(&mut MockState) -> R) -> R {
    let mut guard = MOCK.lock().unwrap();
    f(guard.as_mut().expect("mock not installed"))
}

fn install_mock() {
    *MOCK.lock().unwrap() = Some(MockState {
        calls: Vec::new(),
        next_set: 1,
        add_pos: 0,
        wait_result: Ok(None),
    });

    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        wes::create_wait_event_set::set(|nevents| {
            with_mock(|m| {
                m.calls.push(Call::Create { nevents });
                let h = WaitEventSetHandle::new(m.next_set);
                m.next_set += 1;
                Ok(h)
            })
        });
        wes::create_wait_event_set_current_owner::set(|nevents| {
            with_mock(|m| {
                m.calls.push(Call::CreateCurrentOwner { nevents });
                let h = WaitEventSetHandle::new(m.next_set);
                m.next_set += 1;
                Ok(h)
            })
        });
        wes::add_wait_event_to_set::set(|set, events, fd, latch, _user_data| {
            with_mock(|m| {
                m.calls.push(Call::Add { set: set.as_usize(), events, fd, latch });
                let pos = m.add_pos;
                m.add_pos += 1;
                Ok(pos)
            })
        });
        wes::modify_wait_event::set(|set, pos, events, latch| {
            with_mock(|m| {
                m.calls.push(Call::Modify { set: set.as_usize(), pos, events, latch });
                Ok(())
            })
        });
        wes::wait_event_set_wait_one::set(|set, timeout, wait_event_info| {
            with_mock(|m| {
                m.calls.push(Call::Wait { set: set.as_usize(), timeout, wait_event_info });
                m.wait_result
                    .clone()
                    .map_err(|msg| Box::new(PgError::error(msg)))
            })
        });
        wes::free_wait_event_set::set(|set| {
            with_mock(|m| m.calls.push(Call::Free { set: set.as_usize() }));
        });
        wes::wakeup_my_proc::set(|| with_mock(|m| m.calls.push(Call::WakeupMyProc)));
        wes::wakeup_other_proc::set(|pid| {
            with_mock(|m| m.calls.push(Call::WakeupOtherProc { pid }));
        });
        init_seams();
    });
}

fn calls() -> Vec<Call> {
    with_mock(|m| m.calls.clone())
}

fn fresh_latch() -> LatchHandle {
    allocate_local_latch()
}

#[test]
fn init_latch_owned_by_me_not_shared() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    let l = latch_ref(h);
    assert_eq!(l.is_set.load(SeqCst), 0);
    assert_eq!(l.maybe_sleeping.load(SeqCst), 0);
    assert_eq!(l.owner_pid.load(SeqCst), 42);
    assert!(!l.is_shared.load(SeqCst));
}

#[test]
fn shared_latch_own_disown_round_trip() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitSharedLatch(h);
    let l = latch_ref(h);
    assert!(l.is_shared.load(SeqCst));
    assert_eq!(l.owner_pid.load(SeqCst), 0);

    OwnLatch(h).unwrap();
    assert_eq!(l.owner_pid.load(SeqCst), 42);

    DisownLatch(h);
    assert_eq!(l.owner_pid.load(SeqCst), 0);
}

#[test]
fn own_latch_already_owned_is_panic_level() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitSharedLatch(h);
    latch_ref(h).owner_pid.store(7, SeqCst);

    let err = OwnLatch(h).unwrap_err();
    assert_eq!(err.level, PANIC);
    assert_eq!(err.message, "latch already owned by PID 7");
}

#[test]
fn set_latch_no_wake_when_not_sleeping() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    SetLatch(h);

    assert_eq!(latch_ref(h).is_set.load(SeqCst), 1);
    assert!(calls().is_empty());
}

#[test]
fn set_latch_quick_exit_when_already_set() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    let l = latch_ref(h);
    l.is_set.store(1, SeqCst);
    l.maybe_sleeping.store(1, SeqCst);

    SetLatch(h);
    assert!(calls().is_empty());
}

#[test]
fn set_latch_wakes_own_proc() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    latch_ref(h).maybe_sleeping.store(1, SeqCst);

    SetLatch(h);
    assert_eq!(calls(), vec![Call::WakeupMyProc]);
}

#[test]
fn set_latch_wakes_other_proc() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitSharedLatch(h);
    let l = latch_ref(h);
    l.owner_pid.store(7, SeqCst);
    l.maybe_sleeping.store(1, SeqCst);

    SetLatch(h);
    assert_eq!(calls(), vec![Call::WakeupOtherProc { pid: 7 }]);
}

#[test]
fn set_latch_no_wake_when_unowned() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitSharedLatch(h);
    latch_ref(h).maybe_sleeping.store(1, SeqCst);

    SetLatch(h);
    assert_eq!(latch_ref(h).is_set.load(SeqCst), 1);
    assert!(calls().is_empty());
}

#[test]
fn set_latch_from_other_thread_wakes_owner() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitSharedLatch(h);
    OwnLatch(h).unwrap();
    latch_ref(h).maybe_sleeping.store(1, SeqCst);

    std::thread::spawn(move || {
        SetMyProcPid(7);
        SetLatch(h);
    })
    .join()
    .unwrap();

    assert_eq!(latch_ref(h).is_set.load(SeqCst), 1);
    assert_eq!(calls(), vec![Call::WakeupOtherProc { pid: 42 }]);
    DisownLatch(h);
}

#[test]
fn reset_latch_clears_is_set() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    latch_ref(h).is_set.store(1, SeqCst);

    ResetLatch(h);
    assert_eq!(latch_ref(h).is_set.load(SeqCst), 0);
}

#[test]
fn initialize_latch_wait_set_and_wait_latch() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);
    SetIsUnderPostmaster(true);

    let h = fresh_latch();
    InitLatch(h);
    SetMyLatch(Some(h));

    InitializeLatchWaitSet().unwrap();
    assert_eq!(
        calls(),
        vec![
            Call::Create { nevents: 2 },
            Call::Add { set: 1, events: WL_LATCH_SET, fd: PGINVALID_SOCKET, latch: Some(h) },
            Call::Add { set: 1, events: WL_EXIT_ON_PM_DEATH, fd: PGINVALID_SOCKET, latch: None },
        ]
    );

    with_mock(|m| {
        m.calls.clear();
        m.wait_result = Ok(Some(WaitEvent {
            pos: 0,
            events: WL_LATCH_SET,
            fd: PGINVALID_SOCKET,
            user_data: None,
        }));
    });

    let rc = WaitLatch(Some(h), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 123, 99).unwrap();
    assert_eq!(rc, WL_LATCH_SET);
    assert_eq!(
        calls(),
        vec![
            Call::Modify { set: 1, pos: 0, events: WL_LATCH_SET, latch: Some(h) },
            Call::Modify { set: 1, pos: 1, events: WL_EXIT_ON_PM_DEATH, latch: None },
            // no WL_TIMEOUT => timeout forced to -1
            Call::Wait { set: 1, timeout: -1, wait_event_info: 99 },
        ]
    );

    with_mock(|m| {
        m.calls.clear();
        m.wait_result = Ok(None);
    });

    // No WL_LATCH_SET => latch stripped; WL_TIMEOUT honored; timeout return.
    let rc = WaitLatch(Some(h), WL_EXIT_ON_PM_DEATH | WL_TIMEOUT, 10, 99).unwrap();
    assert_eq!(rc, WL_TIMEOUT);
    assert_eq!(
        calls()[0],
        Call::Modify { set: 1, pos: 0, events: WL_LATCH_SET, latch: None }
    );
    assert_eq!(calls()[2], Call::Wait { set: 1, timeout: 10, wait_event_info: 99 });

    SetIsUnderPostmaster(false);
    SetMyLatch(None);
}

#[test]
fn wait_latch_or_socket_builds_frees_throwaway_set() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);
    SetIsUnderPostmaster(false);

    let h = fresh_latch();
    InitLatch(h);
    with_mock(|m| {
        m.wait_result = Ok(Some(WaitEvent {
            pos: 1,
            events: types_storage::waiteventset::WL_SOCKET_READABLE,
            fd: 5,
            user_data: None,
        }));
    });

    let rc = WaitLatchOrSocket(
        Some(h),
        WL_LATCH_SET | types_storage::waiteventset::WL_SOCKET_READABLE,
        5,
        0,
        77,
    )
    .unwrap();
    assert_eq!(rc, types_storage::waiteventset::WL_SOCKET_READABLE);

    let recorded = calls();
    let set = match recorded[0] {
        Call::CreateCurrentOwner { nevents: 3 } => 1usize,
        ref other => panic!("unexpected first call: {other:?}"),
    };
    assert_eq!(
        recorded[1],
        Call::Add { set, events: WL_LATCH_SET, fd: PGINVALID_SOCKET, latch: Some(h) }
    );
    assert_eq!(
        recorded[2],
        Call::Add {
            set,
            events: types_storage::waiteventset::WL_SOCKET_READABLE,
            fd: 5,
            latch: None
        }
    );
    assert_eq!(*recorded.last().unwrap(), Call::Free { set });
}

#[test]
fn wait_latch_or_socket_frees_set_on_error() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);
    SetIsUnderPostmaster(false);

    let h = fresh_latch();
    InitLatch(h);
    with_mock(|m| m.wait_result = Err(String::from("epoll_wait() failed")));

    let err = WaitLatchOrSocket(Some(h), WL_LATCH_SET | WL_TIMEOUT, PGINVALID_SOCKET, 1, 77)
        .unwrap_err();
    assert_eq!(err.message, "epoll_wait() failed");
    assert!(matches!(calls().last().unwrap(), Call::Free { .. }));
}

#[test]
fn set_latch_my_latch_seam_installed_and_allocation_shape() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(42);

    let h = fresh_latch();
    InitLatch(h);
    SetMyLatch(Some(h));

    assert!(latch_seams::set_latch_my_latch::is_installed());
    latch_seams::set_latch_my_latch::call();
    assert_eq!(latch_ref(h).is_set.load(SeqCst), 1);

    SetMyLatch(None);
}

#[test]
fn set_latch_my_latch_panics_when_null() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyLatch(None);

    let result = catch_unwind(AssertUnwindSafe(|| latch_seams::set_latch_my_latch::call()));
    assert!(result.is_err());
}

#[test]
fn recovery_wakeup_handle_panics_loudly() {
    let _g = TEST_LOCK.lock().unwrap();
    let result = catch_unwind(|| latch_ref(LatchHandle::recovery_wakeup()));
    assert!(result.is_err());
}

#[test]
fn free_local_latch_recycles_slot() {
    let _g = TEST_LOCK.lock().unwrap();

    let h = allocate_local_latch();
    let high = local_latch_high_water();
    free_local_latch(h);
    assert_eq!(allocate_local_latch(), h);
    assert_eq!(local_latch_high_water(), high);
    free_local_latch(h);
}

#[test]
fn free_list_bounds_high_water_across_churn() {
    let _g = TEST_LOCK.lock().unwrap();

    let h = allocate_local_latch();
    free_local_latch(h);
    let high = local_latch_high_water();
    for _ in 0..3 * LOCAL_LATCH_CAP {
        let h = allocate_local_latch();
        free_local_latch(h);
    }
    assert_eq!(local_latch_high_water(), high);
}

#[test]
fn free_local_latch_rejects_proc_handle() {
    let _g = TEST_LOCK.lock().unwrap();
    let result = catch_unwind(|| free_local_latch(LatchHandle::proc(3)));
    assert!(result.is_err());
}

static DRAINS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static DRAIN_FAIL_NEXT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[test]
fn wait_latch_runs_thread_signal_drain() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mock();
    SetMyProcPid(43);
    SetIsUnderPostmaster(false);

    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        procsignal_seams::drain_thread_signals::set(|| {
            DRAINS.fetch_add(1, SeqCst);
            if DRAIN_FAIL_NEXT.swap(false, SeqCst) {
                return Err(Box::new(types_error::PgError::new(
                    types_error::FATAL,
                    "terminating connection due to administrator command",
                )));
            }
            Ok(())
        });
    });

    let h = fresh_latch();
    InitLatch(h);
    SetMyLatch(Some(h));
    InitializeLatchWaitSet().unwrap();

    with_mock(|m| {
        m.wait_result = Ok(Some(WaitEvent {
            pos: 0,
            events: WL_LATCH_SET,
            fd: PGINVALID_SOCKET,
            user_data: None,
        }));
    });
    let before = DRAINS.load(SeqCst);
    assert_eq!(WaitLatch(Some(h), WL_LATCH_SET, 0, 0).unwrap(), WL_LATCH_SET);
    assert_eq!(DRAINS.load(SeqCst), before + 1);

    DRAIN_FAIL_NEXT.store(true, SeqCst);
    let err = WaitLatch(Some(h), WL_LATCH_SET, 0, 0).unwrap_err();
    assert_eq!(err.level(), types_error::FATAL);

    with_mock(|m| m.wait_result = Ok(None));
    let rc = WaitLatchOrSocket(Some(h), WL_LATCH_SET | WL_TIMEOUT, PGINVALID_SOCKET, 1, 0).unwrap();
    assert_eq!(rc, WL_TIMEOUT);
    assert_eq!(DRAINS.load(SeqCst), before + 3);

    SetMyLatch(None);
}
