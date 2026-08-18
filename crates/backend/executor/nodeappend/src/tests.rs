use super::*;
use ::executils::AsyncRequest;
use ::mcx::{Mcx, MemoryContext};
use ::types_nodes::Node;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::Once;

fn leaked_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("nodeappend test"))).mcx()
}

fn mk_append_plan(mcx: Mcx<'static>, plan_node_id: i32) -> &'static Append<'static> {
    let mut b = Node::build::<Append<'static>>(mcx).unwrap();
    b.plan.plan_node_id = plan_node_id;
    let node = b.seal();
    node.as_append().unwrap()
}

fn bms(mcx: Mcx<'static>, members: &[i32]) -> Bitmapset<'static> {
    let mut b = Bitmapset::empty();
    for &m in members {
        b.add_member(mcx, m).unwrap();
    }
    b
}

// Slot ids encode (child, row) so the test can verify provenance without
// real tuple slots (nodeappend treats ExecSlotId as opaque).
fn slot(child: usize, row: i32) -> ExecSlotId {
    ExecSlotId((child as u32) << 16 | row as u32)
}

#[derive(Clone, Copy, PartialEq)]
enum ChildMode {
    Sync,
    /// Completes every request in the request callback itself.
    AsyncImmediate,
    /// Goes callback-pending; completes in notify once its fd polls readable.
    AsyncViaEvent(i32),
}

struct MockChild {
    mode: ChildMode,
    rows_left: i32,
    emitted: i32,
}

struct MockDriver {
    children: Vec<MockChild>,
    configure_calls: usize,
    notify_calls: usize,
}

impl MockDriver {
    fn produce(child: &mut MockChild, id: usize, areq: &mut AsyncRequest) {
        if child.rows_left > 0 {
            child.rows_left -= 1;
            child.emitted += 1;
            areq.request_complete = true;
            areq.result = Some(slot(id, child.emitted));
        } else {
            areq.request_complete = true;
            areq.result = None;
        }
    }
}

impl<'mcx> AppendAsyncDriver<'mcx> for MockDriver {
    fn fetch_subplan(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        i: usize,
    ) -> PgResult<Option<ExecSlotId>> {
        let child = &mut self.children[i];
        assert!(child.mode == ChildMode::Sync, "sync pull hit an async child");
        if child.rows_left > 0 {
            child.rows_left -= 1;
            child.emitted += 1;
            Ok(Some(slot(i, child.emitted)))
        } else {
            Ok(None)
        }
    }
    fn async_request(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
    ) -> PgResult<()> {
        let i = areq.request_index as usize;
        let child = &mut self.children[i];
        match child.mode {
            ChildMode::Sync => panic!("async request to a sync child"),
            ChildMode::AsyncImmediate => MockDriver::produce(child, i, areq),
            ChildMode::AsyncViaEvent(_) => {
                areq.callback_pending = true;
                areq.request_complete = false;
                areq.result = None;
            }
        }
        Ok(())
    }
    fn async_configure_wait(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
        wait: &AsyncWaitCtx,
    ) -> PgResult<()> {
        assert!(areq.callback_pending);
        self.configure_calls += 1;
        let ChildMode::AsyncViaEvent(fd) = self.children[areq.request_index as usize].mode
        else {
            panic!("configure_wait on a non-event child")
        };
        waiteventset::AddWaitEventToSet(
            wait.set,
            WL_SOCKET_READABLE,
            fd,
            None,
            Some(areq.request_index),
        )?;
        Ok(())
    }
    fn async_notify(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
    ) -> PgResult<()> {
        assert!(!areq.callback_pending);
        self.notify_calls += 1;
        let i = areq.request_index as usize;
        MockDriver::produce(&mut self.children[i], i, areq);
        Ok(())
    }
}

fn init_state(
    mcx: Mcx<'static>,
    estate: &mut EStateData<'static>,
    nplans: usize,
    async_members: &[i32],
) -> AppendState<'static> {
    let plan = mk_append_plan(mcx, 7);
    exec_init_append(
        plan,
        estate,
        0,
        nplans,
        nplans as i32,
        None,
        bms(mcx, async_members),
        async_members.len() as i32,
    )
    .unwrap()
}

fn drain<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> Vec<ExecSlotId> {
    let mut out = Vec::new();
    while let Some(s) = exec_append(node, estate, driver).unwrap() {
        out.push(s);
        assert!(out.len() < 1000, "append never terminated");
    }
    out
}

#[test]
fn all_async_immediate_children_drain_and_finish() {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let mut node = init_state(mcx, &mut estate, 3, &[0, 1, 2]);
    let mut driver = MockDriver {
        children: vec![
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 2, emitted: 0 },
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 0, emitted: 0 },
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 3, emitted: 0 },
        ],
        configure_calls: 0,
        notify_calls: 0,
    };
    let out = drain(&mut node, &mut estate, &mut driver);
    assert_eq!(out.len(), 5);
    let mut sorted: Vec<u32> = out.iter().map(|s| s.0).collect();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![slot(0, 1).0, slot(0, 2).0, slot(2, 1).0, slot(2, 2).0, slot(2, 3).0]
    );
    assert_eq!(driver.configure_calls, 0);
}

#[test]
fn mixed_sync_and_async_children() {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let mut node = init_state(mcx, &mut estate, 2, &[1]);
    let mut driver = MockDriver {
        children: vec![
            MockChild { mode: ChildMode::Sync, rows_left: 2, emitted: 0 },
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 2, emitted: 0 },
        ],
        configure_calls: 0,
        notify_calls: 0,
    };
    let out = drain(&mut node, &mut estate, &mut driver);
    let mut sorted: Vec<u32> = out.iter().map(|s| s.0).collect();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![slot(0, 1).0, slot(0, 2).0, slot(1, 1).0, slot(1, 2).0]
    );
}

#[test]
fn rescan_resets_async_state_and_replays() {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let mut node = init_state(mcx, &mut estate, 2, &[0, 1]);
    let mut driver = MockDriver {
        children: vec![
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 1, emitted: 0 },
            MockChild { mode: ChildMode::AsyncImmediate, rows_left: 1, emitted: 0 },
        ],
        configure_calls: 0,
        notify_calls: 0,
    };
    assert_eq!(drain(&mut node, &mut estate, &mut driver).len(), 2);
    exec_rescan_append(&mut node);
    driver.children[0].rows_left = 1;
    driver.children[1].rows_left = 1;
    assert_eq!(drain(&mut node, &mut estate, &mut driver).len(), 2);
}

// ---- event-wait integration (socketpair-backed notify) ----

static NEXT_PID: AtomicI32 = AtomicI32::new(8300);

fn setup_wait_backend() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waiteventset::init_seams();
        latch::init_seams();
    });
    let pid = NEXT_PID.fetch_add(1, SeqCst);
    init_small::globals::SetMyProcPid(pid);
    fd::vfd::set_max_safe_fds_value(1000);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let latch = latch::allocate_local_latch();
    latch::InitLatch(latch);
    init_small::globals::SetMyLatch(Some(latch));
}

fn socketpair() -> (i32, i32) {
    let mut fds = [0i32; 2];
    // SAFETY: plain socketpair; both fds owned by the test.
    let rc =
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(rc, 0);
    (fds[0], fds[1])
}

#[test]
fn pending_children_complete_through_event_wait() {
    setup_wait_backend();
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let mut node = init_state(mcx, &mut estate, 2, &[0, 1]);
    let (r0, w0) = socketpair();
    let (r1, w1) = socketpair();
    // Make both fds readable up front so the -1 timeout wait returns.
    // SAFETY: writing one byte to test-owned sockets.
    unsafe {
        assert_eq!(libc::write(w0, b"x".as_ptr().cast(), 1), 1);
        assert_eq!(libc::write(w1, b"x".as_ptr().cast(), 1), 1);
    }
    let mut driver = MockDriver {
        children: vec![
            MockChild { mode: ChildMode::AsyncViaEvent(r0), rows_left: 2, emitted: 0 },
            MockChild { mode: ChildMode::AsyncViaEvent(r1), rows_left: 1, emitted: 0 },
        ],
        configure_calls: 0,
        notify_calls: 0,
    };
    let out = drain(&mut node, &mut estate, &mut driver);
    let mut sorted: Vec<u32> = out.iter().map(|s| s.0).collect();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![slot(0, 1).0, slot(0, 2).0, slot(1, 1).0]);
    assert!(driver.configure_calls >= 2);
    assert!(driver.notify_calls >= 2);
    // SAFETY: closing test-owned fds.
    unsafe {
        libc::close(r0);
        libc::close(w0);
        libc::close(r1);
        libc::close(w1);
    }
}
