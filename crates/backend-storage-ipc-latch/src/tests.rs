//! latch.c is a layer over waiteventset.c (unported, reached through its
//! seam crate). These tests install a deterministic per-thread mock of the
//! wait-event-set seams to exercise latch.c's own logic: position
//! bookkeeping, `WL_LATCH_SET` masking, the modify-then-wait sequence, and
//! `SetLatch`'s owner-PID wakeup dispatch. Seam slots are process-global
//! `OnceLock`s, so each is installed exactly once with a dispatcher reading
//! the per-thread [`Mock`]; the test harness runs each test on its own
//! thread.

use std::cell::RefCell;
use std::sync::Once;

use super::*;

const TEST_PID: i32 = 424242;
const OTHER_PID: i32 = 424243;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Create { nevents: i32 },
    Add { set: usize, events: u32, fd: pgsocket, latch: Option<LatchHandle> },
    Modify { set: usize, pos: i32, events: u32, latch: Option<LatchHandle> },
    Wait { set: usize, timeout: i64 },
    Free { set: usize },
    WakeupMyProc,
    WakeupOtherProc { pid: i32 },
}

#[derive(Default)]
struct Mock {
    calls: Vec<Call>,
    next_handle: usize,
    next_add_pos: i32,
    /// What the next `wait_event_set_wait` reports (None = timeout).
    wait_result: Option<WaitEvent>,
    is_under_postmaster: bool,
}

thread_local! {
    static MOCK: RefCell<Mock> = RefCell::new(Mock { next_handle: 1, ..Default::default() });
}

fn with_mock<R>(f: impl FnOnce(&mut Mock) -> R) -> R {
    MOCK.with(|m| f(&mut m.borrow_mut()))
}

fn calls() -> Vec<Call> {
    with_mock(|m| std::mem::take(&mut m.calls))
}

fn install_seams_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        super::init_seams();

        backend_utils_init_small_seams::my_proc_pid::set(|| TEST_PID);
        backend_utils_init_small_seams::is_under_postmaster::set(|| {
            with_mock(|m| m.is_under_postmaster)
        });

        wes_seams::create_wait_event_set::set(|nevents| {
            with_mock(|m| {
                m.calls.push(Call::Create { nevents });
                let h = m.next_handle;
                m.next_handle += 1;
                m.next_add_pos = 0;
                Ok(types_storage::waiteventset::WaitEventSetHandle::new(h))
            })
        });
        wes_seams::add_wait_event_to_set::set(|set, events, fd, latch, _user_data| {
            with_mock(|m| {
                m.calls.push(Call::Add { set: set.as_usize(), events, fd, latch });
                let pos = m.next_add_pos;
                m.next_add_pos += 1;
                Ok(pos)
            })
        });
        wes_seams::modify_wait_event::set(|set, pos, events, latch| {
            with_mock(|m| {
                m.calls.push(Call::Modify { set: set.as_usize(), pos, events, latch });
                Ok(())
            })
        });
        wes_seams::wait_event_set_wait::set(|set, timeout, occurred, _wait_event_info| {
            with_mock(|m| {
                m.calls.push(Call::Wait { set: set.as_usize(), timeout });
                match m.wait_result.take() {
                    Some(event) => {
                        occurred[0] = event;
                        Ok(1)
                    }
                    None => Ok(0),
                }
            })
        });
        wes_seams::free_wait_event_set::set(|set| {
            with_mock(|m| m.calls.push(Call::Free { set: set.as_usize() }));
        });
        wes_seams::wakeup_my_proc::set(|| with_mock(|m| m.calls.push(Call::WakeupMyProc)));
        wes_seams::wakeup_other_proc::set(|pid| {
            with_mock(|m| m.calls.push(Call::WakeupOtherProc { pid }))
        });
    });
}

#[test]
fn init_latch_owns_current_process() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    let latch = lookup_latch(h);
    assert!(!latch.is_set());
    assert!(!latch.is_shared.load(SeqCst));
    assert_eq!(latch.owner_pid(), TEST_PID);
}

#[test]
fn shared_latch_own_disown() {
    install_seams_once();
    let h = allocate_latch();
    InitSharedLatch(h);
    let latch = lookup_latch(h);
    assert!(latch.is_shared.load(SeqCst));
    assert_eq!(latch.owner_pid(), 0);

    OwnLatch(h).unwrap();
    assert_eq!(latch.owner_pid(), TEST_PID);

    // Owning an already-owned latch is the C elog(PANIC).
    let err = OwnLatch(h).unwrap_err();
    assert_eq!(err.level(), PANIC);
    assert!(err.message().contains(&TEST_PID.to_string()));

    DisownLatch(h);
    assert_eq!(latch.owner_pid(), 0);
}

#[test]
fn set_latch_quick_exit_when_already_set() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    lookup_latch(h).is_set.store(1, SeqCst);
    SetLatch(h);
    assert_eq!(calls(), vec![]);
}

#[test]
fn set_latch_skips_wakeup_unless_maybe_sleeping() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    SetLatch(h);
    assert!(lookup_latch(h).is_set());
    assert_eq!(calls(), vec![]);
}

#[test]
fn set_latch_wakes_own_process() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    lookup_latch(h).set_maybe_sleeping(true);
    SetLatch(h);
    assert_eq!(calls(), vec![Call::WakeupMyProc]);
}

#[test]
fn set_latch_signals_other_owner() {
    install_seams_once();
    let h = allocate_latch();
    InitSharedLatch(h);
    let latch = lookup_latch(h);
    latch.owner_pid.store(OTHER_PID, SeqCst);
    latch.set_maybe_sleeping(true);
    SetLatch(h);
    assert_eq!(calls(), vec![Call::WakeupOtherProc { pid: OTHER_PID }]);
}

#[test]
fn set_latch_unowned_wakes_no_one() {
    install_seams_once();
    let h = allocate_latch();
    InitSharedLatch(h);
    lookup_latch(h).set_maybe_sleeping(true);
    SetLatch(h);
    assert!(lookup_latch(h).is_set());
    assert_eq!(calls(), vec![]);
}

#[test]
fn reset_latch_clears() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    SetLatch(h);
    assert!(lookup_latch(h).is_set());
    ResetLatch(h);
    assert!(!lookup_latch(h).is_set());
}

#[test]
fn seam_shapes_resolve_my_latch() {
    install_seams_once();
    let h = allocate_latch();
    InitLatch(h);
    set_my_latch(Some(h));
    backend_storage_ipc_latch_seams::set_latch_my_latch::call();
    assert!(lookup_latch(h).is_set());
    backend_storage_ipc_latch_seams::reset_latch::call(h);
    assert!(!lookup_latch(h).is_set());
    set_my_latch(None);
}

#[test]
fn wait_latch_modify_then_wait_sequence() {
    install_seams_once();
    let my = allocate_latch();
    InitLatch(my);
    set_my_latch(Some(my));
    with_mock(|m| m.is_under_postmaster = true);

    InitializeLatchWaitSet().unwrap();
    let set = 1; // first handle this thread minted
    assert_eq!(
        calls(),
        vec![
            Call::Create { nevents: 2 },
            Call::Add { set, events: WL_LATCH_SET, fd: PGINVALID_SOCKET, latch: Some(my) },
            Call::Add { set, events: WL_EXIT_ON_PM_DEATH, fd: PGINVALID_SOCKET, latch: None },
        ]
    );

    // Latch wake-up: the wait reports WL_LATCH_SET back.
    with_mock(|m| {
        m.wait_result = Some(WaitEvent {
            pos: 0,
            events: WL_LATCH_SET,
            fd: PGINVALID_SOCKET,
            user_data: None,
        })
    });
    let rc = WaitLatch(Some(my), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, 0).unwrap();
    assert_eq!(rc, WL_LATCH_SET);
    assert_eq!(
        calls(),
        vec![
            Call::Modify { set, pos: 0, events: WL_LATCH_SET, latch: Some(my) },
            Call::Modify { set, pos: 1, events: WL_EXIT_ON_PM_DEATH, latch: None },
            // No WL_TIMEOUT in wakeEvents -> timeout forced to -1.
            Call::Wait { set, timeout: -1 },
        ]
    );

    // Without WL_LATCH_SET the latch argument is nulled out; with
    // WL_TIMEOUT the caller's timeout is honored and 0 fired events report
    // WL_TIMEOUT.
    let rc = WaitLatch(Some(my), WL_TIMEOUT | WL_POSTMASTER_DEATH, 123, 0).unwrap();
    assert_eq!(rc, WL_TIMEOUT);
    assert_eq!(
        calls(),
        vec![
            Call::Modify { set, pos: 0, events: WL_LATCH_SET, latch: None },
            Call::Modify { set, pos: 1, events: WL_POSTMASTER_DEATH, latch: None },
            Call::Wait { set, timeout: 123 },
        ]
    );
    set_my_latch(None);
}

#[test]
fn wait_latch_or_socket_builds_and_frees_throwaway_set() {
    install_seams_once();
    let my = allocate_latch();
    InitLatch(my);
    with_mock(|m| m.is_under_postmaster = true);

    let sock: pgsocket = 7;
    with_mock(|m| {
        m.wait_result = Some(WaitEvent {
            pos: 2,
            events: types_storage::waiteventset::WL_SOCKET_READABLE,
            fd: sock,
            user_data: None,
        })
    });
    let rc = WaitLatchOrSocket(
        Some(my),
        WL_LATCH_SET
            | WL_EXIT_ON_PM_DEATH
            | types_storage::waiteventset::WL_SOCKET_READABLE,
        sock,
        0,
        0,
    )
    .unwrap();
    assert_eq!(rc, types_storage::waiteventset::WL_SOCKET_READABLE);

    let recorded = calls();
    let set = match recorded[0] {
        Call::Create { nevents: 3 } => match recorded[1] {
            Call::Add { set, .. } => set,
            _ => panic!("expected Add, got {recorded:?}"),
        },
        _ => panic!("expected Create(3), got {recorded:?}"),
    };
    assert_eq!(
        recorded,
        vec![
            Call::Create { nevents: 3 },
            Call::Add { set, events: WL_LATCH_SET, fd: PGINVALID_SOCKET, latch: Some(my) },
            Call::Add { set, events: WL_EXIT_ON_PM_DEATH, fd: PGINVALID_SOCKET, latch: None },
            Call::Add {
                set,
                events: types_storage::waiteventset::WL_SOCKET_READABLE,
                fd: sock,
                latch: None,
            },
            Call::Wait { set, timeout: -1 },
            // The guard's Drop is the C's unconditional FreeWaitEventSet.
            Call::Free { set },
        ]
    );

    // Timeout path: 0 fired events -> WL_TIMEOUT, set still freed.
    let rc = WaitLatchOrSocket(None, WL_TIMEOUT | WL_POSTMASTER_DEATH, PGINVALID_SOCKET, 55, 0)
        .unwrap();
    assert_eq!(rc, WL_TIMEOUT);
    let recorded = calls();
    assert!(matches!(recorded[0], Call::Create { nevents: 3 }));
    assert!(recorded
        .iter()
        .any(|c| matches!(c, Call::Wait { timeout: 55, .. })));
    assert!(matches!(recorded.last(), Some(Call::Free { .. })));
}
