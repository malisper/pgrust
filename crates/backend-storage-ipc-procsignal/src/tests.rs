//! Tests drive the real shared slot array. Seam slots are process-global
//! `OnceLock`s, so every outward seam is installed exactly once with a
//! dispatcher that reads a per-thread fixture [`Env`]; a process-global
//! mutex serializes tests because the `PROC_SIGNAL` header is shared.
//! Fake PIDs are far beyond any live pid range so the real `kill(2)` calls
//! fail with ESRCH instead of signaling anything.

use std::cell::{Cell, RefCell};
use std::sync::{Mutex, MutexGuard, Once};

use types_storage::{ProcSignalBarrierType, ProcSignalReason, NUM_AUXILIARY_PROCS};

use super::*;

const TEST_MAX_BACKENDS: i32 = 4;

thread_local! {
    static ENV: Env = Env::default();
}

#[derive(Default)]
pub(crate) struct Env {
    interrupt_pending: Cell<bool>,
    latch_set_count: Cell<u32>,
    notify_calls: Cell<u32>,
    catchup_calls: Cell<u32>,
    recovery_conflicts: RefCell<Vec<ProcSignalReason>>,
    smgr_release_result: Cell<Option<bool>>, // None => Err
    smgr_release_calls: Cell<u32>,
    cv_broadcasts: Cell<u32>,
    #[allow(clippy::type_complexity)]
    #[allow(clippy::type_complexity)]
    shmem_exit_callback: RefCell<
        Option<(
            fn(i32, types_tuple::backend_access_common_heaptuple::Datum<'static>) -> types_error::PgResult<()>,
            types_tuple::backend_access_common_heaptuple::Datum<'static>,
        )>,
    >,
}

fn install_seams_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        super::init_seams();

        backend_utils_init_small_seams::max_backends::set(|| TEST_MAX_BACKENDS);
        backend_utils_init_small_seams::set_interrupt_pending::set(|v| {
            ENV.with(|e| e.interrupt_pending.set(v))
        });

        backend_storage_ipc_shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).unwrap()));
        backend_storage_ipc_shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).unwrap()));

        // Mock `ShmemInitStruct`: allocate a fresh zeroed, 16-byte-aligned
        // host buffer and leak it (tests run serially and never free shmem).
        // `found = false` so `ProcSignalShmemInit` runs its initializer arm.
        // Each `setup()` re-allocates, giving every test a pristine slot array
        // (the real postmaster allocates exactly once; the test isolation we
        // want is fresh-per-test, so a fresh leak is the faithful stand-in).
        backend_storage_ipc_shmem_seams::shmem_init_struct::set(|_name, size| {
            use std::alloc::{alloc_zeroed, Layout};
            let layout = Layout::from_size_align(size.max(1), 16).unwrap();
            // SAFETY: nonzero size, valid alignment; the leak is intentional.
            let ptr = unsafe { alloc_zeroed(layout) };
            assert!(!ptr.is_null(), "test shmem alloc failed");
            Ok((ptr, false))
        });

        backend_storage_ipc_dsm_core_seams::on_shmem_exit::set(|f, arg| {
            ENV.with(|e| *e.shmem_exit_callback.borrow_mut() = Some((f, arg)));
            Ok(())
        });

        backend_storage_ipc_latch_seams::set_latch_my_latch::set(|| {
            ENV.with(|e| e.latch_set_count.set(e.latch_set_count.get() + 1))
        });

        backend_storage_lmgr_condition_variable_seams::condition_variable_timed_sleep::set(
            |_cv, timeout_ms, _wait_event| {
                // Single-threaded tests re-check the predicate after every
                // (spurious) wakeup; burn a slice of the timeout and return.
                std::thread::sleep(std::time::Duration::from_millis(
                    timeout_ms.clamp(0, 10) as u64
                ));
                Ok(false)
            },
        );
        backend_storage_lmgr_condition_variable_seams::condition_variable_cancel_sleep::set(
            || false,
        );
        backend_storage_lmgr_condition_variable_seams::condition_variable_broadcast::set(|_cv| {
            ENV.with(|e| e.cv_broadcasts.set(e.cv_broadcasts.get() + 1));
        });

        backend_storage_smgr_seams::process_barrier_smgr_release::set(|| {
            ENV.with(|e| {
                e.smgr_release_calls.set(e.smgr_release_calls.get() + 1);
                match e.smgr_release_result.get() {
                    Some(ok) => Ok(ok),
                    None => Err(types_error::PgError::new(
                        ERROR,
                        "smgr release failed".to_string(),
                    )),
                }
            })
        });

        backend_storage_ipc_sinval_seams::handle_catchup_interrupt::set(|| {
            ENV.with(|e| e.catchup_calls.set(e.catchup_calls.get() + 1))
        });
        backend_commands_async_seams::handle_notify_interrupt::set(|| {
            ENV.with(|e| e.notify_calls.set(e.notify_calls.get() + 1))
        });
        backend_replication_walsender_seams::handle_wal_snd_init_stopping::set(|| {});
        backend_utils_mmgr_mcxt_seams::handle_log_memory_context_interrupt::set(|| {});
        backend_replication_logical_applyparallelworker_seams::handle_parallel_apply_message_interrupt::set(|| {});
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::set(|reason| {
            ENV.with(|e| e.recovery_conflicts.borrow_mut().push(reason))
        });
    });
}

/// Serialize tests (the slot array is process-global), install seams, init
/// the shared array, and register this thread in `slot`.
fn setup(slot: i32, pid: i32, cancel_key: &[u8]) -> MutexGuard<'static, ()> {
    static TEST_LOCK: Mutex<()> = Mutex::new(());
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    install_seams_once();
    ProcSignalShmemInit().unwrap();

    ENV.with(|e| {
        e.interrupt_pending.set(false);
        e.smgr_release_result.set(Some(true));
        e.smgr_release_calls.set(0);
        e.cv_broadcasts.set(0);
        *e.shmem_exit_callback.borrow_mut() = None;
        e.recovery_conflicts.borrow_mut().clear();
    });
    SetProcSignalBarrierPending(false);
    MY_PROC_SIGNAL_SLOT.set(None);

    ProcSignalInit(slot, pid, cancel_key).unwrap();
    guard
}

/// Run the registered on_shmem_exit callback (CleanupProcSignalState), as
/// shmem_exit would.
fn teardown() {
    let (f, arg) = ENV.with(|e| e.shmem_exit_callback.borrow().clone()).unwrap();
    f(0, arg).unwrap();
}

/// `&ProcSignal->psh_slot[i]` as a process-lifetime ref (the slot array lives
/// in the leaked test "shmem" buffer, valid until process exit), so test
/// assertions can hold it across `teardown()`.
fn slot_ptr(i: usize) -> &'static ProcSignalSlot {
    let base = PROC_SIGNAL_SLOTS.load(std::sync::atomic::Ordering::Relaxed);
    assert!(!base.is_null());
    // SAFETY: `base` addresses the leaked slot array; `i` is in bounds for the
    // test's NumProcSignalSlots and the buffer outlives the process.
    unsafe { &*base.add(i) }
}

/// Read the spinlock-protected cancel-key length of slot `i`.
fn slot_cancel_key_len(i: usize) -> i32 {
    slot_ptr(i).with_mutex(|key| key.pss_cancel_key_len)
}

/// A fake pid no real process can have (macOS caps pids at 99998; Linux
/// pid_max defaults far lower), so `kill(2)` returns ESRCH.
fn fake_pid(slot: i32) -> i32 {
    9_000_000 + slot
}

#[test]
fn shmem_size_counts_all_slots_plus_header() {
    let _guard = setup(0, fake_pid(0), &[]);
    let nslots = (TEST_MAX_BACKENDS + NUM_AUXILIARY_PROCS) as usize;
    assert_eq!(
        ProcSignalShmemSize().unwrap(),
        nslots * core::mem::size_of::<ProcSignalSlot>() + core::mem::size_of::<AtomicU64>()
    );
    teardown();
}

#[test]
fn init_registers_slot_and_cleanup_releases_it() {
    let pid = fake_pid(1);
    let _guard = setup(1, pid, &[7; 8]);

    let slot = slot_ptr(1);
    assert_eq!(slot.pss_pid.load(SeqCst), pid as u32);
    slot.with_mutex(|key| {
        assert_eq!(key.pss_cancel_key_len, 8);
        assert_eq!(&key.pss_cancel_key[..8], &[7; 8]);
    });
    // Our generation caught up to the shared one.
    assert_eq!(
        slot.pss_barrierGeneration.load(SeqCst),
        proc_signal().psh_barrierGeneration.load(SeqCst)
    );

    teardown();
    assert_eq!(slot.pss_pid.load(SeqCst), 0);
    assert_eq!(slot.pss_barrierGeneration.load(SeqCst), u64::MAX);
    assert_eq!(slot_cancel_key_len(1), 0);
    assert!(ENV.with(|e| e.cv_broadcasts.get()) >= 1);
}

#[test]
fn send_proc_signal_sets_flag_and_handler_dispatches() {
    let pid = fake_pid(2);
    let _guard = setup(2, pid, &[]);

    // kill(2) on the fake pid fails with ESRCH, but the flag is already set
    // (exactly the C ordering).
    let rc = SendProcSignal(pid, ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT, 2);
    assert_eq!(rc, -1);
    let slot = slot_ptr(2);
    assert!(slot.pss_signalFlags[ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT as usize].load(SeqCst));

    // Search-by-pid path (INVALID_PROC_NUMBER).
    let rc = SendProcSignal(
        pid,
        ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT,
        INVALID_PROC_NUMBER,
    );
    assert_eq!(rc, -1);

    // Unknown pid: no slot matched -> -1 / ESRCH.
    let rc = SendProcSignal(1, ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT, INVALID_PROC_NUMBER);
    assert_eq!(rc, -1);

    let before_latches = ENV.with(|e| e.latch_set_count.get());
    procsignal_sigusr1_handler();
    ENV.with(|e| {
        assert_eq!(e.notify_calls.get(), 1);
        assert_eq!(e.catchup_calls.get(), 1);
        assert_eq!(e.latch_set_count.get(), before_latches + 1);
    });
    // Flags were test-and-cleared.
    assert!(!slot.pss_signalFlags[ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT as usize].load(SeqCst));

    // Recovery-conflict arm carries the reason through.
    SendProcSignal(pid, ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK, 2);
    procsignal_sigusr1_handler();
    ENV.with(|e| {
        assert_eq!(
            *e.recovery_conflicts.borrow(),
            vec![ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK]
        );
    });

    teardown();
}

#[test]
fn barrier_roundtrip_advances_generation() {
    let pid = fake_pid(3);
    let _guard = setup(3, pid, &[]);
    let slot = slot_ptr(3);

    let generation =
        EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    assert_eq!(generation, proc_signal().psh_barrierGeneration.load(SeqCst));
    assert_eq!(slot.pss_barrierCheckMask.load(SeqCst), 1);
    // Emit set our PROCSIG_BARRIER flag even though kill failed.
    assert!(slot.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize].load(SeqCst));

    // Receive the "signal".
    procsignal_sigusr1_handler();
    assert!(ProcSignalBarrierPending());
    assert!(ENV.with(|e| e.interrupt_pending.get()));

    // Absorb it.
    ProcessProcSignalBarrier().unwrap();
    assert!(!ProcSignalBarrierPending());
    assert_eq!(slot.pss_barrierGeneration.load(SeqCst), generation);
    assert_eq!(slot.pss_barrierCheckMask.load(SeqCst), 0);
    assert_eq!(ENV.with(|e| e.smgr_release_calls.get()), 1);
    assert!(ENV.with(|e| e.cv_broadcasts.get()) >= 1);

    // Now that every registered slot is current, the wait completes.
    WaitForProcSignalBarrier(generation).unwrap();

    // Re-processing with no pending flag is a no-op.
    ProcessProcSignalBarrier().unwrap();
    assert_eq!(ENV.with(|e| e.smgr_release_calls.get()), 1);

    teardown();
}

#[test]
fn unabsorbed_barrier_rearms_and_retries() {
    let pid = fake_pid(0);
    let _guard = setup(0, pid, &[]);
    let slot = slot_ptr(0);

    let generation =
        EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    procsignal_sigusr1_handler();

    // Barrier-processing function says "can't absorb right now".
    ENV.with(|e| e.smgr_release_result.set(Some(false)));
    ENV.with(|e| e.interrupt_pending.set(false));
    ProcessProcSignalBarrier().unwrap();
    assert!(ProcSignalBarrierPending());
    assert!(ENV.with(|e| e.interrupt_pending.get()));
    assert_eq!(slot.pss_barrierCheckMask.load(SeqCst), 1);
    assert!(slot.pss_barrierGeneration.load(SeqCst) < generation);

    // An ERROR from the processing function re-arms too, and propagates.
    ENV.with(|e| e.smgr_release_result.set(None));
    assert!(ProcessProcSignalBarrier().is_err());
    assert!(ProcSignalBarrierPending());
    assert_eq!(slot.pss_barrierCheckMask.load(SeqCst), 1);

    // Finally absorb.
    ENV.with(|e| e.smgr_release_result.set(Some(true)));
    ProcessProcSignalBarrier().unwrap();
    assert!(!ProcSignalBarrierPending());
    assert_eq!(slot.pss_barrierGeneration.load(SeqCst), generation);

    teardown();
}

#[test]
fn cancel_request_checks_key() {
    let pid = fake_pid(1);
    let _guard = setup(1, pid, &[5, 6, 7, 8]);

    // All arms are LOG/DEBUG only; the function must not panic.
    SendCancelRequest(0, &[5, 6, 7, 8]); // PID 0
    SendCancelRequest(pid, &[5, 6, 7, 8]); // match: SIGINT to dead group
    SendCancelRequest(pid, &[5, 6, 7, 9]); // wrong key
    SendCancelRequest(pid, &[5, 6, 7]); // wrong length
    SendCancelRequest(1, &[5, 6, 7, 8]); // no matching backend

    teardown();
}

#[test]
fn timingsafe_bcmp_matches_c() {
    assert_eq!(timingsafe_bcmp(&[], &[]), 0);
    assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 3]), 0);
    assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 4]), 1);
    assert_eq!(pg_rightmost_one_pos32(0b1010_0100), 2);
    assert_eq!(pg_rightmost_one_pos32(1), 0);
}
