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

fn batched_pair(
    receiver: ProcNumber,
    sender: ProcNumber,
) -> (Arc<shm_mq::ShmMq>, Arc<ChunkLedger>) {
    let mq = shm_mq_create(PARALLEL_TUPLE_QUEUE_SIZE);
    mq.set_receiver(receiver);
    mq.set_sender(sender);
    (mq, Arc::new(ChunkLedger::new()))
}

fn batched_dr(mq: &Arc<shm_mq::ShmMq>, ledger: &Arc<ChunkLedger>) -> DrTqueue {
    tqueue_create_DR_batched(shm_mq_attach(Arc::clone(mq)), Arc::clone(ledger))
}

fn batched_reader(mq: &Arc<shm_mq::ShmMq>, ledger: &Arc<ChunkLedger>) -> TupleQueueReader {
    TupleQueueReader::new_batched(shm_mq_attach(Arc::clone(mq)), Arc::clone(ledger))
}

// Varied sizes, some larger than a whole chunk, so the stream crosses many
// chunk boundaries and exercises the oversized-single-tuple path.
fn batch_tuple_image(i: usize) -> Vec<u8> {
    let len = match i % 97 {
        0 => CHUNK_CAPACITY + 1 + i % 300,
        m => 16 + (m * 211) % 2048,
    };
    (0..len).map(|j| (i.wrapping_mul(31).wrapping_add(j)) as u8).collect()
}

#[test]
fn batched_stream_preserves_order_and_backpressure() {
    let _s = serial();
    setup();

    const N: usize = 3000;
    let (mq, ledger) = batched_pair(0, 2);

    let (wmq, wledger) = (Arc::clone(&mq), Arc::clone(&ledger));
    let worker = std::thread::spawn(move || {
        become_backend(2, 7401);
        let mut dr = batched_dr(&wmq, &wledger);
        for i in 0..N {
            assert!(dr.push_tuple_bytes(&batch_tuple_image(i)).unwrap());
        }
        dr.shutdown().unwrap();
    });

    let (lmq, lledger) = (Arc::clone(&mq), Arc::clone(&ledger));
    let leader = std::thread::spawn(move || {
        become_backend(0, 7400);
        let mut reader = batched_reader(&lmq, &lledger);
        let mut got = 0usize;
        let mut done = false;
        while !done {
            match reader.next(true, &mut done).unwrap() {
                Some(tuple) => {
                    assert_eq!(tuple.as_ptr() as usize % 8, 0, "tuple {got} is 8-aligned");
                    assert_eq!(tuple, batch_tuple_image(got), "tuple {got}");
                    got += 1;
                    // Slow consumer stretch: force the ledger-full sender wait.
                    if got % 512 == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
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
fn batched_blocking_receive_drains_then_done() {
    let _s = serial();
    setup();

    const N: usize = 40;
    let (mq, ledger) = batched_pair(0, 2);

    let (wmq, wledger) = (Arc::clone(&mq), Arc::clone(&ledger));
    let worker = std::thread::spawn(move || {
        become_backend(2, 7411);
        let mut dr = batched_dr(&wmq, &wledger);
        for i in 0..N {
            assert!(dr.push_tuple_bytes(&batch_tuple_image(i)).unwrap());
        }
        dr.shutdown().unwrap();
    });

    let (lmq, lledger) = (Arc::clone(&mq), Arc::clone(&ledger));
    let leader = std::thread::spawn(move || {
        become_backend(0, 7410);
        // GatherMerge's nowait=false arm: block for each tuple.
        let mut reader = batched_reader(&lmq, &lledger);
        let mut done = false;
        for i in 0..N {
            let tuple = reader.next(false, &mut done).unwrap().expect("stream has N tuples");
            assert_eq!(tuple, batch_tuple_image(i), "tuple {i}");
        }
        assert!(reader.next(false, &mut done).unwrap().is_none());
        assert!(done);
    });

    worker.join().unwrap();
    leader.join().unwrap();
}

// Worker death mid-batch: the queue detaches without a flush (drop path). The
// leader must see every flushed chunk, then done — never a hang or a torn
// tuple.
#[test]
fn batched_worker_death_mid_batch() {
    let _s = serial();
    setup();
    become_backend(0, 7420);

    let (mq, ledger) = batched_pair(0, 0);
    let mut dr = batched_dr(&mq, &ledger);

    // Two full chunks' worth flushed, then a partial chunk abandoned.
    let big = vec![7u8; CHUNK_CAPACITY - 16];
    assert!(dr.push_tuple_bytes(&big).unwrap());
    assert!(dr.flush().unwrap());
    assert!(dr.push_tuple_bytes(&big).unwrap());
    assert!(dr.flush().unwrap());
    assert!(dr.push_tuple_bytes(&[42u8; 100]).unwrap());
    drop(dr); // detach without flush — worker died mid-batch

    let mut reader = batched_reader(&mq, &ledger);
    let mut done = false;
    for i in 0..2 {
        let tuple = reader.next(true, &mut done).unwrap().unwrap_or_else(|| {
            panic!("flushed chunk {i} is drained after sender death")
        });
        assert_eq!(tuple, big.as_slice());
    }
    assert!(reader.next(true, &mut done).unwrap().is_none());
    assert!(done, "detach reported after the drain");
}

// Leader detach mid-batch: the sender fails open (returns false) whether it is
// mid-chunk, flushing, or waiting for a ledger slot; in-flight chunks are
// reclaimed when the ledger drops.
#[test]
fn batched_send_after_reader_detach_returns_false() {
    let _s = serial();
    setup();
    become_backend(0, 7430);

    let (mq, ledger) = batched_pair(0, 0);
    let mut dr = batched_dr(&mq, &ledger);

    assert!(dr.push_tuple_bytes(&batch_tuple_image(1)).unwrap());
    assert!(dr.flush().unwrap());

    let mut reader = batched_reader(&mq, &ledger);
    let mut done = false;
    assert!(reader.next(true, &mut done).unwrap().is_some());
    drop(reader);

    // Buffered puts still succeed (worker-local), but the handoff reports the
    // detach: either at chunk-boundary flush or explicit flush.
    assert!(dr.push_tuple_bytes(&batch_tuple_image(2)).unwrap());
    assert!(!dr.flush().unwrap(), "flush after leader detach");
    assert!(dr.push_tuple_bytes(&batch_tuple_image(3)).unwrap());
    assert!(
        !dr.push_tuple_bytes(&vec![1u8; CHUNK_CAPACITY]).unwrap(),
        "chunk-boundary flush after leader detach"
    );
    dr.shutdown().unwrap();
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
