use super::*;
use std::sync::{Mutex, Once};
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        use init_small::globals as g;
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        lmgr_proc_seams::proc_latch::set(|p| &lmgr_proc::GetPGProcByNumber(p).procLatch);
        g::SetIsUnderPostmaster(false);
        g::SetMaxConnections(4);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(4 + 3 + 2 + 2 + NUM_SPECIAL_WORKER_PROCS);
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        waiteventset::init_seams();
        latch::init_seams();
    });
}

fn become_backend(procno: ProcNumber, pid: i32) {
    use init_small::globals as g;
    g::SetMyProcNumber(procno);
    g::SetMyProcPid(pid);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let h = LatchHandle::proc(procno);
    // Tests reuse proc slots across serialized test threads; drop stale owners.
    lmgr_proc::GetPGProcByNumber(procno)
        .procLatch
        .owner_pid
        .store(0, std::sync::atomic::Ordering::SeqCst);
    latch::OwnLatch(h).unwrap();
    g::SetMyLatch(Some(h));
    latch::InitializeLatchWaitSet().unwrap();
}

fn recv_success(handle: &mut ShmMqHandle, nowait: bool) -> Vec<u8> {
    match handle.receive(nowait).unwrap() {
        ShmMqRecv::Success(data) => data.to_vec(),
        other => panic!("expected Success, got {other:?}"),
    }
}

fn msg_body(i: usize, len: usize) -> Vec<u8> {
    (0..len).map(|j| (i.wrapping_mul(31).wrapping_add(j)) as u8).collect()
}

#[test]
fn nowait_roundtrip_single_thread() {
    let _s = serial();
    setup();
    become_backend(0, 7100);

    let mq = shm_mq_create(128);
    mq.set_receiver(0);
    mq.set_sender(0);
    assert_eq!(mq.get_sender(), Some(0));
    assert_eq!(mq.get_receiver(), Some(0));

    let mut tx = shm_mq_attach(Arc::clone(&mq));
    let mut rx = shm_mq_attach(Arc::clone(&mq));

    let msg = b"hello shared memory queue";
    assert_eq!(tx.send(msg, true, true).unwrap(), ShmMqResult::Success);
    assert_eq!(recv_success(&mut rx, true), msg);

    assert_eq!(tx.send(&[], true, true).unwrap(), ShmMqResult::Success);
    assert_eq!(recv_success(&mut rx, true), Vec::<u8>::new());

    match rx.receive(true).unwrap() {
        ShmMqRecv::WouldBlock => {}
        other => panic!("expected WouldBlock, got {other:?}"),
    }
}

#[test]
fn nowait_wraparound_message_resumes() {
    let _s = serial();
    setup();
    become_backend(0, 7110);

    let mq = shm_mq_create(64);
    mq.set_receiver(0);
    mq.set_sender(0);
    let mut tx = shm_mq_attach(Arc::clone(&mq));
    let mut rx = shm_mq_attach(Arc::clone(&mq));

    let msg = msg_body(1, 100);
    let mut sent = tx.send(&msg, true, false).unwrap();
    let mut received: Option<Vec<u8>> = None;
    let mut spins = 0;
    while received.is_none() {
        match rx.receive(true).unwrap() {
            ShmMqRecv::Success(data) => received = Some(data.to_vec()),
            ShmMqRecv::WouldBlock => {}
            ShmMqRecv::Detached => panic!("unexpected detach"),
        }
        if sent == ShmMqResult::WouldBlock {
            sent = tx.send(&msg, true, false).unwrap();
        }
        spins += 1;
        assert!(spins < 1000, "wrap-around message never completed");
    }
    assert_eq!(sent, ShmMqResult::Success);
    assert_eq!(received.unwrap(), msg);
}

#[test]
fn sender_detach_lets_receiver_drain() {
    let _s = serial();
    setup();
    become_backend(0, 7120);

    let mq = shm_mq_create(256);
    mq.set_receiver(0);
    mq.set_sender(0);
    let mut tx = shm_mq_attach(Arc::clone(&mq));
    let mut rx = shm_mq_attach(Arc::clone(&mq));

    let a = msg_body(1, 20);
    let b = msg_body(2, 40);
    assert_eq!(tx.send(&a, true, false).unwrap(), ShmMqResult::Success);
    assert_eq!(tx.send(&b, true, false).unwrap(), ShmMqResult::Success);
    tx.detach();

    assert_eq!(recv_success(&mut rx, true), a);
    assert_eq!(recv_success(&mut rx, true), b);
    match rx.receive(true).unwrap() {
        ShmMqRecv::Detached => {}
        other => panic!("expected Detached, got {other:?}"),
    }
}

#[test]
fn receiver_detach_fails_sends() {
    let _s = serial();
    setup();
    become_backend(0, 7130);

    let mq = shm_mq_create(128);
    mq.set_receiver(0);
    mq.set_sender(0);
    let mut tx = shm_mq_attach(Arc::clone(&mq));
    let rx = shm_mq_attach(Arc::clone(&mq));
    drop(rx);

    assert_eq!(
        tx.send(b"too late", true, true).unwrap(),
        ShmMqResult::Detached
    );
}

#[test]
fn mid_message_detach_reports_detached() {
    let _s = serial();
    setup();
    become_backend(0, 7140);

    let mq = shm_mq_create(64);
    mq.set_receiver(0);
    mq.set_sender(0);
    let mut tx = shm_mq_attach(Arc::clone(&mq));
    let mut rx = shm_mq_attach(Arc::clone(&mq));

    let msg = msg_body(3, 200);
    assert_eq!(tx.send(&msg, true, false).unwrap(), ShmMqResult::WouldBlock);
    tx.detach();

    let mut saw_detached = false;
    for _ in 0..100 {
        match rx.receive(true).unwrap() {
            ShmMqRecv::Detached => {
                saw_detached = true;
                break;
            }
            ShmMqRecv::WouldBlock => {}
            ShmMqRecv::Success(_) => panic!("partial message must not complete"),
        }
    }
    assert!(saw_detached);
}

#[test]
fn oversize_message_errors() {
    let _s = serial();
    setup();
    become_backend(0, 7150);

    let mq = shm_mq_create(128);
    mq.set_receiver(0);
    mq.set_sender(0);
    let mut tx = shm_mq_attach(Arc::clone(&mq));

    let big = vec![0u8; MAX_ALLOC_SIZE + 1];
    let err = tx.send(&big, true, false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        err.message,
        format!(
            "cannot send a message of size {} via shared memory queue",
            MAX_ALLOC_SIZE + 1
        )
    );
}

fn two_thread_stress(ring_size: usize, n: usize, max_len: usize, procs: (ProcNumber, ProcNumber), pids: (i32, i32)) {
    let mq = shm_mq_create(ring_size);
    mq.set_receiver(procs.0);
    mq.set_sender(procs.1);

    let tx_mq = Arc::clone(&mq);
    let sender = std::thread::spawn(move || {
        become_backend(procs.1, pids.1);
        let mut tx = shm_mq_attach(tx_mq);
        for i in 0..n {
            let len = if i % 97 == 0 { 0 } else { (i * 37) % max_len };
            let msg = msg_body(i, len);
            assert_eq!(tx.send(&msg, false, false).unwrap(), ShmMqResult::Success);
        }
    });

    let rx_mq = Arc::clone(&mq);
    let receiver = std::thread::spawn(move || {
        become_backend(procs.0, pids.0);
        let mut rx = shm_mq_attach(rx_mq);
        for i in 0..n {
            let len = if i % 97 == 0 { 0 } else { (i * 37) % max_len };
            let expected = msg_body(i, len);
            assert_eq!(recv_success(&mut rx, false), expected, "message {i}");
        }
        match rx.receive(false).unwrap() {
            ShmMqRecv::Detached => {}
            other => panic!("expected Detached at end of stream, got {other:?}"),
        }
    });

    sender.join().unwrap();
    receiver.join().unwrap();
}

#[test]
fn two_thread_stress_blocking() {
    let _s = serial();
    setup();
    two_thread_stress(4096, 10_000, 1000, (0, 2), (7200, 7201));
}

// Tiny ring: every message wraps and reuses ring bytes — pressure on the
// inc_bytes_read release edge that licenses sender overwrite.
#[test]
fn two_thread_stress_small_ring() {
    let _s = serial();
    setup();
    two_thread_stress(64, 10_000, 200, (0, 2), (7210, 7211));
}
