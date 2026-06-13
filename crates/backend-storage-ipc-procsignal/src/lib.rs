//! Port of `src/backend/storage/ipc/procsignal.c` (PostgreSQL 18.3):
//! interprocess signaling.
//!
//! The SIGUSR1 signal is multiplexed to support signaling multiple event
//! types. The specific reason is communicated via flags in shared memory.
//! We keep a boolean flag for each possible "reason", so that different
//! reasons can be signaled to a process concurrently. (However, if the same
//! reason is signaled more than once nearly simultaneously, the process may
//! observe it only once.)
//!
//! Each process that wants to receive signals registers its process ID in
//! the ProcSignalSlots array, indexed by ProcNumber. The fields in each slot
//! are protected by a spinlock, `pss_mutex`; `pss_pid` can also be read
//! without holding the spinlock as a quick preliminary check.
//!
//! `pss_signalFlags` carry fire-and-forget reasons. For global state changes
//! that need confirmation from every backend there is the barrier mechanism:
//! a bit in `pss_barrierCheckMask` plus a bumped "barrier generation"; when
//! the new generation appears in every process's `pss_barrierGeneration`,
//! the change has been absorbed everywhere.
//!
//! # Model notes (audit against these)
//!
//! - The C `ProcSignalHeader` lives in shared memory via
//!   `ShmemInitStruct("ProcSignal", size, &found)`. Here a backend is a
//!   thread and shared memory is explicitly shared, synchronized state, so
//!   the header is a process-global [`OnceLock`]: first
//!   `ProcSignalShmemInit` constructs it (the C `!found` arm — every per-slot
//!   initialization value is preserved), later calls find it. The byte-size
//!   handshake with `ShmemInitStruct` has no analogue, so
//!   `ProcSignalShmemInit` does not consume [`ProcSignalShmemSize`]; the
//!   size function itself stays ported (its `mul_size`/`add_size` overflow
//!   ereports reach shmem.c through `backend-storage-ipc-shmem-seams`).
//! - `slock_t pss_mutex` becomes a host `Mutex` owning exactly the fields C
//!   documents it as protecting that are not independently atomic
//!   (`pss_cancel_key_len`/`pss_cancel_key`); `pss_pid` and
//!   `pss_signalFlags` stay lock-free-readable atomics exactly as in C, with
//!   their writes performed while holding the guard, mirroring the C lock
//!   discipline. The atomics use `SeqCst`, at least as strong as the
//!   `pg_atomic_*` full-barrier RMW ops the C relies on (and the c2rust
//!   object normalized the same ops to SeqCst).
//! - `pss_barrierCV` is a [`types_condvar::ConditionVariable`]; the sleep /
//!   broadcast protocol is `condition_variable.c`'s and is reached through
//!   `backend-storage-lmgr-condition-variable-seams`.
//! - `MyProcSignalSlot` (a per-process static) is a thread-local slot index,
//!   stored together with the registering backend's pid (C's `MyProcPid`)
//!   so the fixed-signature exit callback needs no ambient getter.
//! - `ProcSignalBarrierPending` is C-defined in `globals.c`, but procsignal
//!   owns its whole lifecycle (set by `HandleProcSignalBarrierInterrupt` /
//!   `ResetProcSignalBarrierBits`, cleared by `ProcessProcSignalBarrier`),
//!   so it lives here as a thread-local, published through this unit's seam
//!   crate (`proc_signal_barrier_pending`).
//! - `kill(2)` / `errno` are the OS boundary and are called via `libc`
//!   directly, like the other signal-layer ports. `timingsafe_bcmp` is the
//!   12-line libpgport helper with no catalog unit of its own; it is ported
//!   in-crate.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Mutex, OnceLock};

use backend_utils_error::{elog, ereport};
use types_condvar::ConditionVariable;
use types_core::{ProcNumber, INVALID_PROC_NUMBER, MAX_CANCEL_KEY_LENGTH};
use types_error::{ErrorLocation, PgResult, DEBUG1, DEBUG2, ERROR, LOG};
use types_storage::{
    ProcSignalBarrierType, ProcSignalReason, NUM_AUXILIARY_PROCS, NUM_PROCSIGNALS,
};

/// `WAIT_EVENT_PROC_SIGNAL_BARRIER` (generated `utils/wait_event_types.h`):
/// `PG_WAIT_IPC | 0x2A` — `PROC_SIGNAL_BARRIER` is at 0-based index 42 (the
/// 43rd name) in the alphabetized `WaitEventIPC` section of
/// `wait_event_names.txt`; the generator assigns the first name the class
/// value `PG_WAIT_IPC` itself.
const WAIT_EVENT_PROC_SIGNAL_BARRIER: u32 = 0x0800_0000 | 0x2A;

/// The fields of a slot protected by `pss_mutex` that are not independently
/// atomic: the cancel key. `pss_cancel_key_len == 0` means no cancellation
/// is possible.
struct CancelKey {
    pss_cancel_key_len: i32,
    pss_cancel_key: [u8; MAX_CANCEL_KEY_LENGTH],
}

/// `ProcSignalSlot` (procsignal.c).
struct ProcSignalSlot {
    pss_pid: AtomicU32,
    /// `slock_t pss_mutex` plus the plain fields it protects.
    pss_mutex: Mutex<CancelKey>,
    /// `volatile sig_atomic_t pss_signalFlags[NUM_PROCSIGNALS]`.
    pss_signalFlags: [AtomicBool; NUM_PROCSIGNALS],

    // Barrier-related fields (not protected by pss_mutex)
    pss_barrierGeneration: AtomicU64,
    pss_barrierCheckMask: AtomicU32,
    pss_barrierCV: ConditionVariable,
}

/// `ProcSignalHeader` (procsignal.c). `psh_barrierGeneration` is the highest
/// barrier generation in existence.
struct ProcSignalHeader {
    psh_barrierGeneration: AtomicU64,
    psh_slot: Box<[ProcSignalSlot]>,
}

/// `NON_EXEC_STATIC ProcSignalHeader *ProcSignal = NULL;`
static PROC_SIGNAL: OnceLock<ProcSignalHeader> = OnceLock::new();

thread_local! {
    /// `static ProcSignalSlot *MyProcSignalSlot = NULL;` — our index into
    /// `psh_slot`, paired with the registering backend's pid (the C reads
    /// `MyProcPid` again in `CleanupProcSignalState`; the fixed-signature
    /// `on_shmem_exit` callback has no parameter source, so the pid is
    /// stashed here at registration instead of going through a getter seam).
    static MY_PROC_SIGNAL_SLOT: Cell<Option<(usize, i32)>> = const { Cell::new(None) };

    /// `volatile sig_atomic_t ProcSignalBarrierPending = false;`
    static PROC_SIGNAL_BARRIER_PENDING: Cell<bool> = const { Cell::new(false) };
}

/// Read `ProcSignalBarrierPending`.
#[inline]
pub fn ProcSignalBarrierPending() -> bool {
    PROC_SIGNAL_BARRIER_PENDING.get()
}

/// Write `ProcSignalBarrierPending`.
#[inline]
pub fn SetProcSignalBarrierPending(value: bool) {
    PROC_SIGNAL_BARRIER_PENDING.set(value);
}

/// The `ProcSignal != NULL` dereference: C would crash on use before
/// `ProcSignalShmemInit`; here it is a loud panic.
fn proc_signal() -> &'static ProcSignalHeader {
    PROC_SIGNAL
        .get()
        .expect("ProcSignal shared memory not initialized (ProcSignalShmemInit not called)")
}

/// `NumProcSignalSlots` — `MaxBackends + NUM_AUXILIARY_PROCS`. Used where the
/// slot array does not exist yet; once built, the array length is the same
/// value (MaxBackends is fixed at postmaster startup).
fn num_proc_signal_slots() -> i32 {
    backend_utils_init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS
}

/// `ProcSignalShmemSize` — compute space needed for ProcSignal's shared
/// memory. The `Err` is the C overflow `ereport(ERROR)` inside
/// `mul_size`/`add_size`.
pub fn ProcSignalShmemSize() -> PgResult<usize> {
    let mut size = backend_storage_ipc_shmem_seams::mul_size::call(
        num_proc_signal_slots() as usize,
        core::mem::size_of::<ProcSignalSlot>(),
    )?;
    // add_size(size, offsetof(ProcSignalHeader, psh_slot))
    size = backend_storage_ipc_shmem_seams::add_size::call(
        size,
        core::mem::size_of::<AtomicU64>(),
    )?;
    Ok(size)
}

/// `ProcSignalShmemInit` — allocate and initialize ProcSignal's shared
/// memory. First caller creates and initializes the array (the C `!found`
/// branch); later callers just attach. The C failure surface is
/// `ShmemInitStruct`'s out-of-shared-memory `ereport(ERROR)`; the host
/// allocation backing the `OnceLock` has no recoverable failure, so this
/// always returns `Ok` today.
pub fn ProcSignalShmemInit() -> PgResult<()> {
    PROC_SIGNAL.get_or_init(|| {
        let nslots = num_proc_signal_slots() as usize;
        let mut slots = Vec::with_capacity(nslots);
        for _ in 0..nslots {
            slots.push(ProcSignalSlot {
                pss_pid: AtomicU32::new(0),
                pss_mutex: Mutex::new(CancelKey {
                    pss_cancel_key_len: 0,
                    pss_cancel_key: [0; MAX_CANCEL_KEY_LENGTH],
                }),
                pss_signalFlags: std::array::from_fn(|_| AtomicBool::new(false)),
                pss_barrierGeneration: AtomicU64::new(u64::MAX),
                pss_barrierCheckMask: AtomicU32::new(0),
                pss_barrierCV: ConditionVariable::new(),
            });
        }
        ProcSignalHeader {
            psh_barrierGeneration: AtomicU64::new(0),
            psh_slot: slots.into_boxed_slice(),
        }
    });
    Ok(())
}

/// `ProcSignalInit(const uint8 *cancel_key, int cancel_key_len)` — register
/// the current process in the ProcSignal array. The `Err`s are the two
/// `elog(ERROR)`s on a bad `MyProcNumber` and `on_shmem_exit`'s FATAL slot
/// overflow. The C reads the `MyProcNumber`/`MyProcPid` globals; here the
/// caller passes its own values explicitly.
pub fn ProcSignalInit(
    my_proc_number: ProcNumber,
    my_proc_pid: i32,
    cancel_key: &[u8],
) -> PgResult<()> {
    let cancel_key_len = cancel_key.len() as i32;

    // Assert(cancel_key_len >= 0 && cancel_key_len <= MAX_CANCEL_KEY_LENGTH)
    // (>= 0 is implicit in a slice length).
    debug_assert!(cancel_key.len() <= MAX_CANCEL_KEY_LENGTH);

    if my_proc_number < 0 {
        return elog(ERROR, "MyProcNumber not set");
    }
    let header = proc_signal();
    let num_slots = header.psh_slot.len() as i32;
    if my_proc_number >= num_slots {
        return elog(
            ERROR,
            format!(
                "unexpected MyProcNumber {} in ProcSignalInit (max {})",
                my_proc_number, num_slots
            ),
        );
    }
    let slot = &header.psh_slot[my_proc_number as usize];

    // SpinLockAcquire(&slot->pss_mutex)
    let mut key = slot.pss_mutex.lock().unwrap();

    // Value used for sanity check below
    let old_pss_pid = slot.pss_pid.load(SeqCst);

    // Clear out any leftover signal reasons
    // MemSet(slot->pss_signalFlags, 0, NUM_PROCSIGNALS * sizeof(sig_atomic_t))
    for flag in &slot.pss_signalFlags {
        flag.store(false, SeqCst);
    }

    // Initialize barrier state. Since we're a brand-new process, there
    // shouldn't be any leftover backend-private state that needs to be
    // updated. Therefore, we can broadcast the latest barrier generation
    // and disregard any previously-set check bits.
    //
    // NB: This only works if this initialization happens early enough in the
    // startup sequence that we haven't yet cached any state that might need
    // to be invalidated.
    slot.pss_barrierCheckMask.store(0, SeqCst);
    let barrier_generation = header.psh_barrierGeneration.load(SeqCst);
    slot.pss_barrierGeneration.store(barrier_generation, SeqCst);

    if cancel_key_len > 0 {
        key.pss_cancel_key[..cancel_key.len()].copy_from_slice(cancel_key);
    }
    key.pss_cancel_key_len = cancel_key_len;
    slot.pss_pid.store(my_proc_pid as u32, SeqCst);

    // SpinLockRelease(&slot->pss_mutex)
    drop(key);

    // Spinlock is released, do the check
    if old_pss_pid != 0 {
        elog(
            LOG,
            format!(
                "process {} taking over ProcSignal slot {}, but it's not empty",
                my_proc_pid, my_proc_number
            ),
        )?;
    }

    // Remember slot location for CheckProcSignal (and our pid for
    // CleanupProcSignalState).
    MY_PROC_SIGNAL_SLOT.set(Some((my_proc_number as usize, my_proc_pid)));

    // Set up to release the slot on process exit
    backend_storage_ipc_seams::on_shmem_exit::call(
        CleanupProcSignalState,
        types_datum::Datum::from_usize(0),
    )?;

    Ok(())
}

/// `CleanupProcSignalState(int status, Datum arg)` — remove current process
/// from the ProcSignal mechanism. Called via `on_shmem_exit()` during
/// backend shutdown.
fn CleanupProcSignalState(_status: i32, _arg: types_datum::Datum) -> PgResult<()> {
    // Clear MyProcSignalSlot, so that a SIGUSR1 received after this point
    // won't try to access it after it's no longer ours.
    let (slot_index, my_proc_pid) = MY_PROC_SIGNAL_SLOT
        .get()
        .expect("CleanupProcSignalState called without a ProcSignal slot");
    MY_PROC_SIGNAL_SLOT.set(None);

    let header = proc_signal();
    let slot = &header.psh_slot[slot_index];

    // sanity check
    let mut key = slot.pss_mutex.lock().unwrap();
    let old_pid = slot.pss_pid.load(SeqCst);
    if old_pid != my_proc_pid as u32 {
        // don't ERROR here. We're exiting anyway, and don't want to get into
        // infinite loop trying to exit
        drop(key);
        // LOG never raises; ignore the Ok.
        let _ = elog(
            LOG,
            format!(
                "process {} releasing ProcSignal slot {}, but it contains {}",
                my_proc_pid, slot_index, old_pid as i32
            ),
        );
        return Ok(()); /* XXX better to zero the slot anyway? */
    }

    // Mark the slot as unused
    slot.pss_pid.store(0, SeqCst);
    key.pss_cancel_key_len = 0;

    // Make this slot look like it's absorbed all possible barriers, so that
    // no barrier waits block on it.
    slot.pss_barrierGeneration.store(u64::MAX, SeqCst);

    drop(key);

    backend_storage_lmgr_condition_variable_seams::condition_variable_broadcast::call(
        &slot.pss_barrierCV,
    );

    Ok(())
}

/// `SendProcSignal(pid, reason, procNumber)` — send a signal to a Postgres
/// process. Providing `proc_number` is optional, but it will speed up the
/// operation.
///
/// On success (a signal was sent), zero is returned. On error, -1 is
/// returned, and errno is set (typically to ESRCH or EPERM).
///
/// Not to be confused with ProcSendSignal.
pub fn SendProcSignal(pid: i32, reason: ProcSignalReason, proc_number: ProcNumber) -> i32 {
    let header = proc_signal();

    if proc_number != INVALID_PROC_NUMBER {
        debug_assert!(proc_number < header.psh_slot.len() as i32);
        let slot = &header.psh_slot[proc_number as usize];

        let key = slot.pss_mutex.lock().unwrap();
        if slot.pss_pid.load(SeqCst) == pid as u32 {
            // Atomically set the proper flag
            slot.pss_signalFlags[reason as usize].store(true, SeqCst);
            drop(key);
            // Send signal
            return unsafe { libc::kill(pid, libc::SIGUSR1) };
        }
        drop(key);
    } else {
        // procNumber not provided, so search the array using pid. We search
        // the array back to front so as to reduce search overhead. Passing
        // INVALID_PROC_NUMBER means that the target is most likely an
        // auxiliary process, which will have a slot near the end of the
        // array.
        for slot in header.psh_slot.iter().rev() {
            if slot.pss_pid.load(SeqCst) == pid as u32 {
                let key = slot.pss_mutex.lock().unwrap();
                if slot.pss_pid.load(SeqCst) == pid as u32 {
                    // Atomically set the proper flag
                    slot.pss_signalFlags[reason as usize].store(true, SeqCst);
                    drop(key);
                    // Send signal
                    return unsafe { libc::kill(pid, libc::SIGUSR1) };
                }
                drop(key);
            }
        }
    }

    set_errno(libc::ESRCH);
    -1
}

/// `EmitProcSignalBarrier(type)` — send a signal to every Postgres process.
///
/// The return value is the barrier "generation" created by this operation,
/// to pass to [`WaitForProcSignalBarrier`]. Callers are entitled to assume
/// that this function will not throw ERROR or FATAL.
pub fn EmitProcSignalBarrier(type_: ProcSignalBarrierType) -> u64 {
    let flagbit: u32 = 1 << (type_ as u32);
    let header = proc_signal();

    // Set all the flags.
    //
    // Note that pg_atomic_fetch_or_u32 has full barrier semantics, so this
    // is totally ordered with respect to anything the caller did before, and
    // anything that we do afterwards. (This is also true of the later call
    // to pg_atomic_add_fetch_u64.)
    for slot in header.psh_slot.iter() {
        slot.pss_barrierCheckMask.fetch_or(flagbit, SeqCst);
    }

    // Increment the generation counter.
    let generation = header.psh_barrierGeneration.fetch_add(1, SeqCst) + 1;

    // Signal all the processes, so that they update their advertised
    // barrier generation.
    //
    // Concurrency is not a problem here. Backends that have exited don't
    // matter, and new backends that have joined since we entered this
    // function must already have current state, since the caller is
    // responsible for making sure that the relevant state is entirely
    // visible before calling this function in the first place. We still
    // have to wake them up - because we can't distinguish between such
    // backends and older backends that need to update state - but they
    // won't actually need to change any state.
    for slot in header.psh_slot.iter().rev() {
        let mut pid = slot.pss_pid.load(SeqCst) as i32;
        if pid != 0 {
            let key = slot.pss_mutex.lock().unwrap();
            pid = slot.pss_pid.load(SeqCst) as i32;
            if pid != 0 {
                // see SendProcSignal for details
                slot.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize]
                    .store(true, SeqCst);
                drop(key);
                unsafe { libc::kill(pid, libc::SIGUSR1) };
            } else {
                drop(key);
            }
        }
    }

    generation
}

/// `WaitForProcSignalBarrier(generation)` — wait until it is guaranteed that
/// all changes requested by a specific call to [`EmitProcSignalBarrier`]
/// have taken effect. The `Err` surface is `ConditionVariableTimedSleep`'s
/// `CHECK_FOR_INTERRUPTS()`.
pub fn WaitForProcSignalBarrier(generation: u64) -> PgResult<()> {
    let header = proc_signal();

    debug_assert!(generation <= header.psh_barrierGeneration.load(SeqCst));

    elog(
        DEBUG1,
        format!(
            "waiting for all backends to process ProcSignalBarrier generation {generation}"
        ),
    )?;

    for slot in header.psh_slot.iter().rev() {
        // It's important that we check only pss_barrierGeneration here and
        // not pss_barrierCheckMask. Bits in pss_barrierCheckMask get
        // cleared before the barrier is actually absorbed, but
        // pss_barrierGeneration is updated only afterward.
        let mut oldval = slot.pss_barrierGeneration.load(SeqCst);
        while oldval < generation {
            if backend_storage_lmgr_condition_variable_seams::condition_variable_timed_sleep::call(
                &slot.pss_barrierCV,
                5000,
                WAIT_EVENT_PROC_SIGNAL_BARRIER,
            )? {
                ereport(LOG)
                    .errmsg(format!(
                        "still waiting for backend with PID {} to accept ProcSignalBarrier",
                        slot.pss_pid.load(SeqCst) as i32
                    ))
                    .finish(ErrorLocation::new(
                        "procsignal.c",
                        461,
                        "WaitForProcSignalBarrier",
                    ))?;
            }
            oldval = slot.pss_barrierGeneration.load(SeqCst);
        }
        backend_storage_lmgr_condition_variable_seams::condition_variable_cancel_sleep::call();
    }

    elog(
        DEBUG1,
        format!(
            "finished waiting for all backends to process ProcSignalBarrier generation {generation}"
        ),
    )?;

    // The caller is probably calling this function because it wants to read
    // the shared state or perform further writes to shared state once all
    // backends are known to have absorbed the barrier. However, the read of
    // pss_barrierGeneration was performed unlocked; insert a memory barrier
    // to separate it from whatever follows.
    fence(SeqCst);

    Ok(())
}

/// `HandleProcSignalBarrierInterrupt()` — handle receipt of an interrupt
/// indicating a global barrier event.
///
/// All the actual work is deferred to [`ProcessProcSignalBarrier`], because
/// we cannot safely access the barrier generation inside the signal handler
/// (64bit atomics might use spinlock based emulation, even for reads).
fn HandleProcSignalBarrierInterrupt() {
    backend_utils_init_small_seams::set_interrupt_pending::call(true);
    SetProcSignalBarrierPending(true);
    // latch will be set by procsignal_sigusr1_handler
}

/// `ProcessProcSignalBarrier()` — perform global barrier related interrupt
/// checking.
///
/// Any backend that participates in ProcSignal signaling must arrange to
/// call this function periodically. It is called from
/// `CHECK_FOR_INTERRUPTS()`, which is enough for normal backends, but not
/// necessarily for all types of background processes.
pub fn ProcessProcSignalBarrier() -> PgResult<()> {
    // Assert(MyProcSignalSlot)
    debug_assert!(MY_PROC_SIGNAL_SLOT.get().is_some());

    // Exit quickly if there's no work to do.
    if !ProcSignalBarrierPending() {
        return Ok(());
    }
    SetProcSignalBarrierPending(false);

    let header = proc_signal();
    let my_slot = &header.psh_slot[MY_PROC_SIGNAL_SLOT
        .get()
        .expect("ProcessProcSignalBarrier called without a ProcSignal slot")
        .0];

    // It's not unlikely to process multiple barriers at once, before the
    // signals for all the barriers have arrived. To avoid unnecessary work
    // in response to subsequent signals, exit early if we already have
    // processed all of them.
    let local_gen = my_slot.pss_barrierGeneration.load(SeqCst);
    let shared_gen = header.psh_barrierGeneration.load(SeqCst);

    debug_assert!(local_gen <= shared_gen);

    if local_gen == shared_gen {
        return Ok(());
    }

    // Get and clear the flags that are set for this backend. Note that
    // pg_atomic_exchange_u32 is a full barrier, so we're guaranteed that
    // the read of the barrier generation above happens before we atomically
    // extract the flags, and that any subsequent state changes happen
    // afterward.
    //
    // NB: In order to avoid race conditions, we must zero
    // pss_barrierCheckMask first and only afterwards try to do barrier
    // processing. If we did it in the other order, someone could send us
    // another barrier of some type right after we called the
    // barrier-processing function but before we cleared the bit. We would
    // have no way of knowing that the bit needs to stay set in that case,
    // so the need to call the barrier-processing function again would just
    // get forgotten. So instead, we tentatively clear all the bits and then
    // put back any for which we don't manage to successfully absorb the
    // barrier.
    let mut flags = my_slot.pss_barrierCheckMask.swap(0, SeqCst);

    // If there are no flags set, then we can skip doing any real work.
    // Otherwise, the C establishes a PG_TRY block so it doesn't lose track
    // of which types of barrier processing are needed if an ERROR occurs;
    // here the loop's Err propagates after the PG_CATCH re-arm.
    if flags != 0 {
        let mut success = true;

        let result = (|| -> PgResult<()> {
            // Process each type of barrier. The barrier-processing
            // functions should normally return true, but may return false
            // if the barrier can't be absorbed at the current time. This
            // should be rare, because it's pretty expensive. Every single
            // CHECK_FOR_INTERRUPTS() will return here until we manage to
            // absorb the barrier, and that cost will add up in a hurry.
            //
            // NB: It ought to be OK to call the barrier-processing
            // functions unconditionally, but it's more efficient to call
            // only the ones that might need us to do something based on the
            // flags.
            while flags != 0 {
                let type_ = pg_rightmost_one_pos32(flags) as u32;
                // The C switch's only arm; an unknown bit (impossible from
                // EmitProcSignalBarrier) would fall through with
                // processed = true.
                let processed =
                    if type_ == ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE as u32 {
                        backend_storage_smgr_seams::process_barrier_smgr_release::call()?
                    } else {
                        true
                    };

                // To avoid an infinite loop, we must always unset the bit
                // in flags.
                flags &= !(1u32 << type_);

                // If we failed to process the barrier, reset the shared bit
                // so we try again later, and set a flag so that we don't
                // bump our generation.
                if !processed {
                    ResetProcSignalBarrierBits(1u32 << type_);
                    success = false;
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            // PG_CATCH: if an ERROR occurred, we'll need to try again later
            // to handle that barrier type and any others that haven't been
            // handled yet or weren't successfully absorbed. (`flags` still
            // contains the failing type's bit: the error propagated before
            // BARRIER_CLEAR_BIT.)
            ResetProcSignalBarrierBits(flags);
            return Err(e);
        }

        // If some barrier types were not successfully absorbed, we will
        // have to try again later.
        if !success {
            return Ok(());
        }
    }

    // State changes related to all types of barriers that might have been
    // emitted have now been handled, so we can update our notion of the
    // generation to the one we observed before beginning the updates. If
    // things have changed further, it'll get fixed up when this function is
    // next called.
    my_slot.pss_barrierGeneration.store(shared_gen, SeqCst);
    backend_storage_lmgr_condition_variable_seams::condition_variable_broadcast::call(
        &my_slot.pss_barrierCV,
    );

    Ok(())
}

/// `ResetProcSignalBarrierBits(flags)` — if it turns out that we couldn't
/// absorb one or more barrier types, either because the barrier-processing
/// functions returned false or due to an error, arrange for processing to
/// be retried later.
fn ResetProcSignalBarrierBits(flags: u32) {
    let header = proc_signal();
    let my_slot = &header.psh_slot[MY_PROC_SIGNAL_SLOT
        .get()
        .expect("ResetProcSignalBarrierBits called without a ProcSignal slot")
        .0];
    my_slot.pss_barrierCheckMask.fetch_or(flags, SeqCst);
    SetProcSignalBarrierPending(true);
    backend_utils_init_small_seams::set_interrupt_pending::call(true);
}

/// `CheckProcSignal(reason)` — check to see if a particular reason has been
/// signaled, and clear the signal flag. Should be called after receiving
/// SIGUSR1.
fn CheckProcSignal(reason: ProcSignalReason) -> bool {
    if let Some((index, _)) = MY_PROC_SIGNAL_SLOT.get() {
        let slot = &proc_signal().psh_slot[index];

        // Careful here --- don't clear flag if we haven't seen it set.
        // pss_signalFlags is of type "volatile sig_atomic_t" to allow us to
        // read it here safely, without holding the spinlock.
        if slot.pss_signalFlags[reason as usize].load(SeqCst) {
            slot.pss_signalFlags[reason as usize].store(false, SeqCst);
            return true;
        }
    }

    false
}

/// `procsignal_sigusr1_handler(SIGNAL_ARGS)` — handle SIGUSR1 signal.
///
/// The C handler runs in signal context with errno saved/restored by the
/// SIGNAL_ARGS prologue; here it is an ordinary function for the signal
/// trampoline to call.
pub fn procsignal_sigusr1_handler() {
    use ProcSignalReason::*;

    if CheckProcSignal(PROCSIG_CATCHUP_INTERRUPT) {
        backend_storage_ipc_sinval_seams::handle_catchup_interrupt::call();
    }

    if CheckProcSignal(PROCSIG_NOTIFY_INTERRUPT) {
        backend_commands_async_seams::handle_notify_interrupt::call();
    }

    if CheckProcSignal(PROCSIG_PARALLEL_MESSAGE) {
        backend_access_transam_parallel_seams::handle_parallel_message_interrupt::call();
    }

    if CheckProcSignal(PROCSIG_WALSND_INIT_STOPPING) {
        backend_replication_walsender_seams::handle_wal_snd_init_stopping::call();
    }

    if CheckProcSignal(PROCSIG_BARRIER) {
        HandleProcSignalBarrierInterrupt();
    }

    if CheckProcSignal(PROCSIG_LOG_MEMORY_CONTEXT) {
        backend_utils_mmgr_mcxt_seams::handle_log_memory_context_interrupt::call();
    }

    if CheckProcSignal(PROCSIG_PARALLEL_APPLY_MESSAGE) {
        backend_replication_logical_applyparallelworker_seams::handle_parallel_apply_message_interrupt::call();
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_DATABASE) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_DATABASE,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_TABLESPACE) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOCK) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_LOCK,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
        );
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN) {
        backend_tcop_postgres_seams::handle_recovery_conflict_interrupt::call(
            PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
        );
    }

    backend_storage_ipc_latch_seams::set_latch_my_latch::call();
}

/// `SendCancelRequest(backendPID, cancel_key, cancel_key_len)` — send a
/// query cancellation signal to backend.
///
/// Note: This is called from a backend process before authentication. We
/// cannot take LWLocks yet, but that's OK; we rely on atomic reads of the
/// fields in the ProcSignal slots. Every report in here is LOG/DEBUG level,
/// so the function is infallible, as in C.
pub fn SendCancelRequest(backend_pid: i32, cancel_key: &[u8]) {
    let cancel_key_len = cancel_key.len() as i32;

    if backend_pid == 0 {
        log_never_raises(
            ereport(LOG)
                .errmsg("invalid cancel request with PID 0")
                .finish(ErrorLocation::new("procsignal.c", 743, "SendCancelRequest")),
        );
        return;
    }

    // See if we have a matching backend. Reading the pss_pid and
    // pss_cancel_key fields is racy, a backend might die and remove itself
    // from the array at any time. The probability of the cancellation key
    // matching wrong process is miniscule, however, so we can live with
    // that. PIDs are reused too, so sending the signal based on PID is
    // inherently racy anyway, although OS's avoid reusing PIDs too soon.
    let header = proc_signal();
    for slot in header.psh_slot.iter() {
        if slot.pss_pid.load(SeqCst) != backend_pid as u32 {
            continue;
        }

        // Acquire the spinlock and re-check
        let key = slot.pss_mutex.lock().unwrap();
        if slot.pss_pid.load(SeqCst) != backend_pid as u32 {
            drop(key);
            continue;
        }

        let match_ = key.pss_cancel_key_len == cancel_key_len
            && timingsafe_bcmp(
                &key.pss_cancel_key[..cancel_key.len()],
                cancel_key,
            ) == 0;

        drop(key);

        if match_ {
            // Found a match; signal that backend to cancel current op
            log_never_raises(
                ereport(DEBUG2)
                    .errmsg_internal(format!(
                        "processing cancel request: sending SIGINT to process {backend_pid}"
                    ))
                    .finish(ErrorLocation::new("procsignal.c", 781, "SendCancelRequest")),
            );

            // If we have setsid(), signal the backend's whole process group
            // (HAVE_SETSID holds on every supported unix).
            unsafe { libc::kill(-backend_pid, libc::SIGINT) };
        } else {
            // Right PID, wrong key: no way, Jose
            log_never_raises(
                ereport(LOG)
                    .errmsg(format!(
                        "wrong key in cancel request for process {backend_pid}"
                    ))
                    .finish(ErrorLocation::new("procsignal.c", 798, "SendCancelRequest")),
            );
        }
        return;
    }

    // No matching backend
    log_never_raises(
        ereport(LOG)
            .errmsg(format!(
                "PID {backend_pid} in cancel request did not match any process"
            ))
            .finish(ErrorLocation::new("procsignal.c", 807, "SendCancelRequest")),
    );
}

/// `pg_rightmost_one_pos32(word)` (`port/pg_bitutils.h` inline) — index of
/// the least-significant set bit; `word` must not be 0.
fn pg_rightmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    word.trailing_zeros() as i32
}

/// `timingsafe_bcmp(b1, b2, n)` (`src/port/timingsafe_bcmp.c`, the
/// non-OpenSSL arm) — constant-time comparison; the slices must be the same
/// length (the C caller passes one `n`).
fn timingsafe_bcmp(b1: &[u8], b2: &[u8]) -> i32 {
    let mut ret: i32 = 0;
    for (p1, p2) in b1.iter().zip(b2.iter()) {
        ret |= (p1 ^ p2) as i32;
    }
    (ret != 0) as i32
}

/// `errno = value` — the OS errno cell (`SendProcSignal` sets ESRCH when no
/// slot matched, so the caller's `%m` reports it).
fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    unsafe {
        *libc::__errno_location() = value;
    }
}

/// Discard the `Ok` from a LOG/DEBUG-level report, which never raises (only
/// ERROR and above travel the `Err` channel).
fn log_never_raises(result: PgResult<()>) {
    debug_assert!(result.is_ok());
}

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    backend_storage_ipc_procsignal_seams::proc_signal_barrier_pending::set(
        ProcSignalBarrierPending,
    );
    backend_storage_ipc_procsignal_seams::process_proc_signal_barrier::set(
        ProcessProcSignalBarrier,
    );
    // `procsignal_sigusr1_handler(SIGNAL_ARGS)`: the C handler ignores the
    // `postgres_signal_arg` (it only dispatches on the multiplexed proc-signal
    // flags), so the adapter drops it onto the parameterless implementation.
    backend_storage_ipc_procsignal_seams::procsignal_sigusr1_handler::set(
        |_postgres_signal_arg| procsignal_sigusr1_handler(),
    );
    backend_storage_ipc_procsignal_seams::send_proc_signal::set(SendProcSignal);
    // `types_core::Size` is `usize`, so `ProcSignalShmemSize`'s
    // `PgResult<usize>` matches the seam's `PgResult<types_core::Size>`.
    backend_storage_ipc_procsignal_seams::proc_signal_shmem_size::set(ProcSignalShmemSize);
    backend_storage_ipc_procsignal_seams::proc_signal_shmem_init::set(ProcSignalShmemInit);
}

#[cfg(test)]
mod tests;
