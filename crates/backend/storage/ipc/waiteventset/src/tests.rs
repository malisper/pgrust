use super::*;
use init_small::globals as g;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::Once;
use types_storage::waiteventset::{WL_SOCKET_CLOSED, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};

static NEXT_PID: AtomicI32 = AtomicI32::new(7100);

fn setup_backend() -> i32 {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        init_seams();
        latch::init_seams();
    });
    let pid = NEXT_PID.fetch_add(1, SeqCst);
    g::SetMyProcPid(pid);
    fd::vfd::set_max_safe_fds_value(1000);
    InitializeWaitEventSupport().unwrap();
    pid
}

fn owned_latch() -> LatchHandle {
    let h = latch::allocate_local_latch();
    latch::InitLatch(h);
    h
}

fn socketpair() -> (i32, i32) {
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(rc, 0);
    (fds[0], fds[1])
}

#[test]
fn latch_event_reports_immediately_and_after_reset_times_out() {
    setup_backend();
    let latch = owned_latch();
    let set = CreateWaitEventSet(2).unwrap();
    let pos = AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, Some(latch), Some(9)).unwrap();
    assert_eq!(pos, 0);
    assert_eq!(GetNumRegisteredWaitEvents(set), 1);

    latch::SetLatch(latch);
    let mut occurred = [WaitEvent::default(); 2];
    let n = WaitEventSetWait(set, 1000, &mut occurred, 0).unwrap();
    assert_eq!(n, 1);
    assert_eq!(occurred[0].events, WL_LATCH_SET);
    assert_eq!(occurred[0].pos, 0);
    assert_eq!(occurred[0].user_data, Some(9));
    assert_eq!(occurred[0].fd, PGINVALID_SOCKET);

    latch::ResetLatch(latch);
    let start = std::time::Instant::now();
    let n = WaitEventSetWait(set, 50, &mut occurred, 0).unwrap();
    assert_eq!(n, 0);
    assert!(start.elapsed().as_millis() >= 40);

    FreeWaitEventSet(set);
}

#[test]
fn socket_readiness_and_modify_to_closed() {
    setup_backend();
    let (a, b) = socketpair();
    let set = CreateWaitEventSet(2).unwrap();
    let pos = AddWaitEventToSet(
        set,
        WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE,
        a,
        None,
        Some(42),
    )
    .unwrap();

    let mut occurred = [WaitEvent::default(); 2];
    let n = WaitEventSetWait(set, 1000, &mut occurred, 0).unwrap();
    assert_eq!(n, 1);
    assert_eq!(occurred[0].events, WL_SOCKET_WRITEABLE);
    assert_eq!(occurred[0].fd, a);
    assert_eq!(occurred[0].user_data, Some(42));

    let payload = [7u8; 3];
    assert_eq!(
        unsafe { libc::write(b, payload.as_ptr().cast(), payload.len()) },
        3
    );
    // kqueue reports read/write as two kevents (two occurred entries, as in
    // C's kqueue arm); epoll merges them into one.
    let n = WaitEventSetWait(set, 1000, &mut occurred, 0).unwrap();
    assert!(n >= 1);
    let union: u32 = occurred[..n as usize].iter().map(|e| e.events).fold(0, |a, e| a | e);
    assert_eq!(union, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
    assert!(occurred[..n as usize].iter().all(|e| e.fd == a));

    // Read->write mask switches are the ModifyWaitEvent fast/slow paths.
    ModifyWaitEvent(set, pos, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE, None).unwrap();
    ModifyWaitEvent(set, pos, WL_SOCKET_CLOSED, None).unwrap();
    assert_eq!(unsafe { libc::close(b) }, 0);
    let n = WaitEventSetWait(set, 1000, &mut occurred, 0).unwrap();
    assert_eq!(n, 1);
    assert_eq!(occurred[0].events, WL_SOCKET_CLOSED);

    FreeWaitEventSet(set);
    unsafe { libc::close(a) };
}

#[test]
fn timeout_expires_without_events() {
    setup_backend();
    let (a, b) = socketpair();
    let set = CreateWaitEventSet(1).unwrap();
    AddWaitEventToSet(set, WL_SOCKET_READABLE, a, None, None).unwrap();

    let mut occurred = [WaitEvent::default(); 1];
    let start = std::time::Instant::now();
    let n = WaitEventSetWait(set, 60, &mut occurred, 0).unwrap();
    assert_eq!(n, 0);
    assert!(start.elapsed().as_millis() >= 50);

    FreeWaitEventSet(set);
    unsafe {
        libc::close(a);
        libc::close(b);
    }
}

#[test]
fn cross_backend_set_latch_wakes_blocked_wait() {
    setup_backend();
    let (send, recv) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        setup_backend();
        let latch = owned_latch();
        let set = CreateWaitEventSet(1).unwrap();
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, Some(latch), None).unwrap();
        send.send(latch).unwrap();
        let mut occurred = [WaitEvent::default(); 1];
        let n = WaitEventSetWait(set, 10_000, &mut occurred, 0).unwrap();
        FreeWaitEventSet(set);
        (n, occurred[0].events)
    });

    let latch = recv.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(30));
    // Cross-backend SetLatch routes through wakeup_other_proc -> the waiter's
    // wakeup pipe (C: kill(owner_pid, SIGURG)).
    latch::SetLatch(latch);

    let (n, events) = waiter.join().unwrap();
    assert_eq!(n, 1);
    assert_eq!(events, WL_LATCH_SET);
}

#[test]
fn seam_wait_one_roundtrip() {
    setup_backend();
    let latch = owned_latch();
    let set = waiteventset_seams::create_wait_event_set_current_owner::call(1).unwrap();
    waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        Some(latch),
        None,
    )
    .unwrap();

    let none = waiteventset_seams::wait_event_set_wait_one::call(set, 0, 0).unwrap();
    assert!(none.is_none());

    latch::SetLatch(latch);
    let got = waiteventset_seams::wait_event_set_wait_one::call(set, 1000, 0)
        .unwrap()
        .expect("latch event");
    assert_eq!(got.events, WL_LATCH_SET);

    waiteventset_seams::modify_wait_event::call(set, 0, WL_LATCH_SET, None).unwrap();
    latch::ResetLatch(latch);
    latch::SetLatch(latch);
    let none = waiteventset_seams::wait_event_set_wait_one::call(set, 0, 0).unwrap();
    assert!(none.is_none(), "disabled latch must not report");

    waiteventset_seams::free_wait_event_set::call(set);
}

#[test]
fn multiple_events_reported_together() {
    setup_backend();
    let latch = owned_latch();
    let (a, b) = socketpair();
    let set = CreateWaitEventSet(2).unwrap();
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, Some(latch), None).unwrap();
    AddWaitEventToSet(set, WL_SOCKET_READABLE, a, None, Some(1)).unwrap();

    let payload = [1u8; 1];
    assert_eq!(unsafe { libc::write(b, payload.as_ptr().cast(), 1) }, 1);
    latch::SetLatch(latch);

    let mut occurred = [WaitEvent::default(); 2];
    let n = WaitEventSetWait(set, 1000, &mut occurred, 0).unwrap();
    assert_eq!(n, 2);
    assert_eq!(occurred[0].events, WL_LATCH_SET);
    assert_eq!(occurred[1].events, WL_SOCKET_READABLE);
    assert_eq!(occurred[1].user_data, Some(1));

    FreeWaitEventSet(set);
    unsafe {
        libc::close(a);
        libc::close(b);
    }
}

#[test]
#[should_panic(expected = "invalid WaitEventSetHandle")]
fn freed_handle_is_invalid() {
    setup_backend();
    let set = CreateWaitEventSet(1).unwrap();
    FreeWaitEventSet(set);
    GetNumRegisteredWaitEvents(set);
}
