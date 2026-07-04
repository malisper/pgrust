use super::*;
use shm_mq::{shm_mq_attach, shm_mq_create};
use std::sync::{Arc, Mutex, Once};
use types_core::ProcNumber;
use types_storage::latch::LatchHandle;
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

fn tuple_image(i: usize) -> Vec<u8> {
    let len = 16 + (i * 53) % 512;
    (0..len).map(|j| (i.wrapping_mul(7).wrapping_add(j)) as u8).collect()
}

#[test]
fn leader_worker_tuple_stream() {
    let _s = serial();
    setup();

    const N: usize = 1000;
    let mq = shm_mq_create(PARALLEL_TUPLE_QUEUE_SIZE);
    mq.set_receiver(0);
    mq.set_sender(2);

    let worker_mq = Arc::clone(&mq);
    let worker = std::thread::spawn(move || {
        become_backend(2, 7301);
        let mut queue = shm_mq_attach(worker_mq);
        for i in 0..N {
            assert!(tqueue_send_bytes(&mut queue, &tuple_image(i)).unwrap());
        }
    });

    let leader_mq = Arc::clone(&mq);
    let leader = std::thread::spawn(move || {
        become_backend(0, 7300);
        let mut reader = TupleQueueReader::new(shm_mq_attach(leader_mq));
        let mut got = 0usize;
        let mut done = false;
        while !done {
            match reader.next(true, &mut done).unwrap() {
                Some(tuple) => {
                    assert_eq!(tuple, tuple_image(got), "tuple {got}");
                    got += 1;
                }
                None => std::thread::yield_now(),
            }
        }
        assert_eq!(got, N);
    });

    worker.join().unwrap();
    leader.join().unwrap();
}

#[test]
fn send_after_reader_detach_returns_false() {
    let _s = serial();
    setup();
    become_backend(0, 7310);

    let mq = shm_mq_create(PARALLEL_TUPLE_QUEUE_SIZE);
    mq.set_receiver(0);
    mq.set_sender(0);

    let mut queue = shm_mq_attach(Arc::clone(&mq));
    let reader = TupleQueueReader::new(shm_mq_attach(Arc::clone(&mq)));
    drop(reader);

    assert!(!tqueue_send_bytes(&mut queue, &tuple_image(0)).unwrap());
}
