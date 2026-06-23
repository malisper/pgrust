//! Port of `src/backend/port/sysv_sema.c` (PostgreSQL 18.3): `PGSemaphore`
//! support implemented with SysV semaphore facilities.
//!
//! PostgreSQL identifies a semaphore by a `(semId, semNum)` pair — `semId` is
//! a kernel SysV semaphore *set* id and `semNum` is the index within that set.
//! C returns a `PGSemaphore` (a pointer into a shared-memory array of
//! `PGSemaphoreData`) from `PGSemaphoreCreate()` and stashes it in
//! `PGPROC.sem`; lock/unlock/reset then operate on that two-`int` value.
//!
//! # Model notes (audit against these)
//!
//! - The repo's `backend-port-pg-sema-seams` inward contract keys the
//!   per-process operations (`PGSemaphoreReset`/`Lock`/`Unlock`) by
//!   `ProcNumber` rather than by a `PGSemaphore` pointer the consumer holds.
//!   This unit therefore owns the per-procno `PGSemaphoreData` assignment.
//!   In C, `PGSemaphoreCreate()` is called from `InitProcGlobal()` once per
//!   PGPROC, in PGPROC index (= ProcNumber) order, for the first
//!   `MaxBackends + NUM_AUXILIARY_PROCS` slots. We fold those per-proc
//!   creations into `PGReserveSemaphores(maxSemas)`: it eagerly creates all
//!   `maxSemas` semaphores in procno order, so `sharedSemas[procno]` is the
//!   semaphore the C `GetPGProcByNumber(procno)->sem` would point at. This is
//!   behaviour-preserving (same kernel sets, same allocation/key order, same
//!   final state) — eager vs. lazy creation is the only difference, and the
//!   reserve-time `maxSemas` is exactly `ProcGlobalSemas()`.
//! - `sharedSemas` is a process-global `Vec<PGSemaphoreData>` rather than a
//!   raw pointer into the SysV shim segment. The values are two `int`s that
//!   every postmaster child inherits identically across `fork()`; the real
//!   shared object is the kernel semaphore set keyed by `(semId, semNum)`, so
//!   carrying the descriptor by value is faithful (lock/unlock/reset only read
//!   those two ints to issue `semop`/`semctl`). We still reserve the shim
//!   region with `ShmemAllocUnlocked(PGSemaphoreShmemSize(maxSemas))` for
//!   faithful sizing/accounting, matching the C, and register the
//!   `on_shmem_exit(ReleaseSemaphores)` callback.
//! - The postmaster-local bookkeeping statics
//!   (`numSharedSemas`/`maxSharedSemas`/`mySemaSets`/`numSemaSets`/
//!   `maxSemaSets`/`nextSemaKey`/`nextSemaNumber`) live in a process-global
//!   `Mutex<SemaState>`, mirroring the C `static` variables.
//! - The OS primitives (`semget`/`semctl`/`semop`/`stat`/`kill`/`getpid`) are
//!   direct `libc` calls — the genuine OS boundary, not a Rust dependency, so
//!   per AGENTS.md no seam is introduced for them.
//! - `ereport(FATAL)`/`elog(PANIC)` become `PgError` with the matching level;
//!   `elog(LOG)` non-fatal failures are swallowed (best-effort) as in C.
//! - WIN32/Solaris `union semun` quirks: we build the `semun` union as C does.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::sync::Mutex;

use ::types_core::{ProcNumber, Size};
use ::types_error::{PgError, PgResult, FATAL, PANIC};
use ::types_storage::storage::PGSemaphoreData;

/// `SEMAS_PER_SET` — number of useful semaphores in each set we allocate. It
/// must be *less than* the kernel's SEMMSL, because we allocate one extra sema
/// in each set for identification purposes.
const SEMAS_PER_SET: i32 = 16;

/// `IPCProtection` — access/modify by user only.
const IPC_PROTECTION: libc::c_int = 0o600;

/// `PGSemaMagic` — must be less than SEMVMX.
const PG_SEMA_MAGIC: libc::c_int = 537;

type IpcSemaphoreKey = libc::key_t;
type IpcSemaphoreId = libc::c_int;

/// The `union semun` argument to `semctl(2)` (C builds it locally; on
/// platforms without `HAVE_UNION_SEMUN` the union is declared in-file).
#[repr(C)]
#[derive(Clone, Copy)]
union Semun {
    val: libc::c_int,
    buf: *mut libc::semid_ds,
    array: *mut libc::c_ushort,
}

/// Postmaster-local semaphore bookkeeping (the C `static` variables).
struct SemaState {
    /// `static PGSemaphore sharedSemas` — array of `PGSemaphoreData`. Modeled
    /// as an owned `Vec` (see module docs); index = `ProcNumber`.
    shared_semas: Vec<PGSemaphoreData>,
    /// `numSharedSemas` — number of `PGSemaphoreData`s used so far.
    num_shared_semas: i32,
    /// `maxSharedSemas` — allocated size of the `PGSemaphoreData` array.
    max_shared_semas: i32,
    /// `mySemaSets` — IDs of sema sets acquired so far.
    my_sema_sets: Vec<IpcSemaphoreId>,
    /// `numSemaSets` — number of sema sets acquired so far.
    num_sema_sets: i32,
    /// `maxSemaSets` — allocated size of the `mySemaSets` array.
    max_sema_sets: i32,
    /// `nextSemaKey` — next key to try using.
    next_sema_key: IpcSemaphoreKey,
    /// `nextSemaNumber` — next free sem num in the last sema set.
    next_sema_number: i32,
}

static SEMA_STATE: Mutex<SemaState> = Mutex::new(SemaState {
    shared_semas: Vec::new(),
    num_shared_semas: 0,
    max_shared_semas: 0,
    my_sema_sets: Vec::new(),
    num_sema_sets: 0,
    max_sema_sets: 0,
    next_sema_key: 0,
    next_sema_number: 0,
});

/// `InternalIpcSemaphoreCreate(semKey, numSems, retry_ok)`.
///
/// Attempt to create a new semaphore set with the specified key. Will fail
/// (return `Ok(-1)`) if such a set already exists and `retry_ok`. If we fail
/// with a failure code other than collision-with-existing-set, return `Err`
/// (the C `ereport(FATAL)`).
fn internal_ipc_semaphore_create(
    sem_key: IpcSemaphoreKey,
    num_sems: i32,
    retry_ok: bool,
) -> PgResult<IpcSemaphoreId> {
    // SAFETY: semget is a syscall; arguments are plain integers.
    let sem_id = unsafe {
        libc::semget(
            sem_key,
            num_sems,
            libc::IPC_CREAT | libc::IPC_EXCL | IPC_PROTECTION,
        )
    };

    if sem_id < 0 {
        let saved_errno = errno();

        // Fail quietly if error suggests a collision with an existing set and
        // our caller has not lost patience.
        //
        // One would expect EEXIST, given that we said IPC_EXCL, but perhaps we
        // could get a permission violation instead. On some platforms EINVAL
        // will be reported if the existing set has too few semaphores. Also,
        // EIDRM might occur if an old set is slated for destruction but not
        // gone yet.
        if retry_ok
            && (saved_errno == libc::EEXIST
                || saved_errno == libc::EACCES
                || saved_errno == libc::EINVAL
                || saved_errno == libc::EIDRM)
        {
            return Ok(-1);
        }

        // Else complain and abort.
        let hint = if saved_errno == libc::ENOSPC {
            "\nThis error does *not* mean that you have run out of disk space.  \
             It occurs when either the system limit for the maximum number of \
             semaphore sets (SEMMNI), or the system wide maximum number of \
             semaphores (SEMMNS), would be exceeded.  You need to raise the \
             respective kernel parameter.  Alternatively, reduce PostgreSQL's \
             consumption of semaphores by reducing its \"max_connections\" parameter."
        } else {
            ""
        };
        return Err(PgError::new(
            FATAL,
            format!(
                "could not create semaphores: {}\nFailed system call was semget({}, {}, 0{:o}).{}",
                os_error_string(saved_errno),
                sem_key as u64,
                num_sems,
                libc::IPC_CREAT | libc::IPC_EXCL | IPC_PROTECTION,
                hint,
            ),
        ));
    }

    Ok(sem_id)
}

/// `IpcSemaphoreInitialize(semId, semNum, value)` — initialize a semaphore to
/// the specified value.
fn ipc_semaphore_initialize(sem_id: IpcSemaphoreId, sem_num: i32, value: i32) -> PgResult<()> {
    let semun = Semun { val: value };
    // SAFETY: semctl with SETVAL takes the `union semun` by value.
    let rc = unsafe { libc::semctl(sem_id, sem_num, libc::SETVAL, semun) };
    if rc < 0 {
        let saved_errno = errno();
        let hint = if saved_errno == libc::ERANGE {
            format!(
                "\nYou possibly need to raise your kernel's SEMVMX value to be at least \
                 {value}.  Look into the PostgreSQL documentation for details."
            )
        } else {
            String::new()
        };
        return Err(PgError::new(
            FATAL,
            format!(
                "semctl({sem_id}, {sem_num}, SETVAL, {value}) failed: {}{hint}",
                os_error_string(saved_errno),
            ),
        ));
    }
    Ok(())
}

/// `IpcSemaphoreKill(semId)` — removes a semaphore set.
fn ipc_semaphore_kill(sem_id: IpcSemaphoreId) {
    let semun = Semun { val: 0 }; // unused, but keep compiler quiet
                                  // SAFETY: semctl with IPC_RMID.
    let rc = unsafe { libc::semctl(sem_id, 0, libc::IPC_RMID, semun) };
    if rc < 0 {
        // elog(LOG, ...) — best effort, ignore.
    }
}

/// `IpcSemaphoreGetValue(semId, semNum)` — get the current value (semval).
fn ipc_semaphore_get_value(sem_id: IpcSemaphoreId, sem_num: i32) -> i32 {
    let dummy = Semun { val: 0 }; // unused
                                  // SAFETY: semctl with GETVAL.
    unsafe { libc::semctl(sem_id, sem_num, libc::GETVAL, dummy) }
}

/// `IpcSemaphoreGetLastPID(semId, semNum)` — get the PID of the last process
/// to do `semop()` on the semaphore.
fn ipc_semaphore_get_last_pid(sem_id: IpcSemaphoreId, sem_num: i32) -> libc::pid_t {
    let dummy = Semun { val: 0 }; // unused
                                  // SAFETY: semctl with GETPID.
    unsafe { libc::semctl(sem_id, sem_num, libc::GETPID, dummy) }
}

/// `IpcSemaphoreCreate(numSems)` — create a semaphore set with the given
/// number of useful semaphores (an additional sema is allocated to serve as
/// identifier). Dead Postgres sema sets are recycled if found.
fn ipc_semaphore_create(state: &mut SemaState, num_sems: i32) -> PgResult<IpcSemaphoreId> {
    let mut num_tries = 0;

    // Loop till we find a free IPC key.
    loop {
        state.next_sema_key += 1;

        // Try to create new semaphore set. Give up after trying 1000 distinct
        // IPC keys.
        let mut sem_id = internal_ipc_semaphore_create(
            state.next_sema_key,
            num_sems + 1,
            num_tries < 1000,
        )?;
        if sem_id >= 0 {
            // successful create — finalize below.
            return finalize_created_set(sem_id, num_sems);
        }

        // See if it looks to be leftover from a dead Postgres process.
        // SAFETY: semget probe with no creation flags.
        sem_id = unsafe { libc::semget(state.next_sema_key, num_sems + 1, 0) };
        if sem_id < 0 {
            num_tries += 1;
            continue; // failed: must be some other app's
        }
        if ipc_semaphore_get_value(sem_id, num_sems) != PG_SEMA_MAGIC {
            num_tries += 1;
            continue; // sema belongs to a non-Postgres app
        }

        // If the creator PID is my own PID or does not belong to any extant
        // process, it's safe to zap it.
        let creator_pid = ipc_semaphore_get_last_pid(sem_id, num_sems);
        if creator_pid <= 0 {
            num_tries += 1;
            continue; // oops, GETPID failed
        }
        if creator_pid != getpid() {
            // SAFETY: kill(pid, 0) probes existence without sending a signal.
            if unsafe { libc::kill(creator_pid, 0) } == 0 || errno() != libc::ESRCH {
                num_tries += 1;
                continue; // sema belongs to a live process
            }
        }

        // The sema set appears to be from a dead Postgres process, or from a
        // previous cycle of life in this same process. Zap it, if possible.
        // This probably shouldn't fail, but if it does, assume the sema set
        // belongs to someone else after all, and continue quietly.
        let semun = Semun { val: 0 };
        // SAFETY: semctl IPC_RMID.
        if unsafe { libc::semctl(sem_id, 0, libc::IPC_RMID, semun) } < 0 {
            num_tries += 1;
            continue;
        }

        // Now try again to create the sema set.
        sem_id = internal_ipc_semaphore_create(state.next_sema_key, num_sems + 1, true)?;
        if sem_id >= 0 {
            return finalize_created_set(sem_id, num_sems);
        }

        // Can only get here if some other process managed to create the same
        // sema key before we did. Let him have that one, loop around to try
        // next key.
        num_tries += 1;
    }
}

/// The tail of `IpcSemaphoreCreate`: mark the freshly-created set as ours by
/// initializing the spare semaphore to `PGSemaMagic - 1` and incrementing it,
/// leaving value `PGSemaMagic` and `sempid` referencing this process.
fn finalize_created_set(sem_id: IpcSemaphoreId, num_sems: i32) -> PgResult<IpcSemaphoreId> {
    ipc_semaphore_initialize(sem_id, num_sems, PG_SEMA_MAGIC - 1)?;
    let mysema = PGSemaphoreData {
        semId: sem_id,
        semNum: num_sems,
    };
    semaphore_unlock(&mysema);
    Ok(sem_id)
}

/// `PGSemaphoreShmemSize(int maxSemas)` — report amount of shared memory
/// needed for semaphores.
pub fn PGSemaphoreShmemSize(max_semas: i32) -> PgResult<Size> {
    // mul_size(maxSemas, sizeof(PGSemaphoreData)); the C uses the overflow-
    // checked helper, but PGSemaphoreData is 8 bytes and maxSemas is bounded,
    // so a plain product matches (mul_size only ereports on size_t overflow).
    Ok((max_semas as usize) * core::mem::size_of::<PGSemaphoreData>())
}

/// `PGReserveSemaphores(int maxSemas)` — initialize semaphore support.
///
/// See module docs: we eagerly create all `maxSemas` semaphores here, in
/// procno order, folding C's per-PGPROC `PGSemaphoreCreate()` calls.
pub fn PGReserveSemaphores(max_semas: i32) -> PgResult<()> {
    // We use the data directory's inode number to seed the search for free
    // semaphore keys.
    let data_dir = init_small_seams::data_dir::call().unwrap_or_default();
    let statbuf = match stat(&data_dir) {
        Ok(s) => s,
        Err(e) => {
            return Err(PgError::new(
                FATAL,
                format!("could not stat data directory \"{data_dir}\": {}", os_error_string(e)),
            ))
        }
    };

    // We must use ShmemAllocUnlocked(), since the spinlock protecting
    // ShmemAlloc() won't be ready yet. (The returned address is the SysV shim
    // region; our `sharedSemas` Vec is the by-value mirror — see module docs.)
    let _shim = ipc_shmem_seams::shmem_alloc_unlocked::call(
        PGSemaphoreShmemSize(max_semas)?,
    )?;

    let mut state = SEMA_STATE.lock().unwrap();
    state.shared_semas = vec![PGSemaphoreData { semId: 0, semNum: 0 }; max_semas as usize];
    state.num_shared_semas = 0;
    state.max_shared_semas = max_semas;

    state.max_sema_sets = (max_semas + SEMAS_PER_SET - 1) / SEMAS_PER_SET;
    state.my_sema_sets = Vec::with_capacity(state.max_sema_sets as usize);
    state.num_sema_sets = 0;
    state.next_sema_key = statbuf.st_ino as IpcSemaphoreKey;
    state.next_sema_number = SEMAS_PER_SET; // force sema set alloc on 1st call

    // Eagerly create all maxSemas semaphores in procno order (folds the
    // per-PGPROC PGSemaphoreCreate() calls InitProcGlobal would make).
    for _ in 0..max_semas {
        pg_semaphore_create_locked(&mut state)?;
    }

    drop(state);

    // on_shmem_exit(ReleaseSemaphores, 0).
    dsm_core_seams::on_shmem_exit::call(
        release_semaphores,
        types_tuple::Datum::from_i32(0),
    )?;

    Ok(())
}

/// `ReleaseSemaphores(status, arg)` — release semaphores at shutdown or shmem
/// reinitialization (an `on_shmem_exit` callback).
///
/// DIVERGENCE FROM C, tied to this tree's shared-memory model (mirrors the same
/// reasoning as sysv_shmem.c's `anonymous_shmem_detach`): in C, crash reinit
/// runs `shmem_exit(1)` — which fires this callback and `semctl(IPC_RMID)`s every
/// SysV semaphore set — and then immediately re-runs
/// `CreateSharedMemoryAndSemaphores()` → `PGReserveSemaphores()`, which creates a
/// fresh batch of sets. This tree reuses a single segment / semaphore batch for
/// the cluster's lifetime: `PGReserveSemaphores` eagerly stashes the
/// `PGSemaphoreData` mirror in process-static state that the re-forked children
/// inherit, and crash reinit deliberately skips the re-create. If we killed the
/// sets here in the postmaster, the very next re-forked auxiliary process'
/// `PGSemaphoreReset` (`semctl(..., SETVAL, 0)`) would hit a removed set and fail
/// `EINVAL`, aborting startup. So in the postmaster process we keep the sets:
/// they must outlive every reinit, and at genuine postmaster exit the kernel
/// reclaims the SysV sets on the postmaster's death anyway. Forked children
/// inherit a *populated* `my_sema_sets` via copy-on-write (`PGReserveSemaphores`
/// runs once in the postmaster, before the first fork), so a child running this
/// callback on its own exit would `IPC_RMID` the still-shared sets and break the
/// surviving postmaster — therefore no process under the postmaster removes the
/// sets either. Only a true standalone backend reclaims the sets it created.
fn release_semaphores(_status: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    // The postmaster owns the persistent semaphore sets that every re-forked
    // child inherits across crash reinit; they must never be removed while the
    // postmaster lives (see the divergence note above).
    //
    // In this tree `PGReserveSemaphores` runs exactly once, in the postmaster,
    // before the first fork — so every child inherits a *non-empty*
    // `my_sema_sets` mirror via fork's copy-on-write (the old "children find
    // num_sema_sets == 0, so the kill loop is a no-op" assumption is false).
    // The sets are real, shared OS objects keyed for the whole cluster; if any
    // child ran this `IPC_RMID` loop on its own `on_shmem_exit` — including the
    // children the postmaster SIGQUITs during crash reinit — it would destroy
    // the sets out from under the surviving postmaster, and the very next
    // re-forked auxiliary process' `PGSemaphoreReset` (`semctl(.., SETVAL, 0)`)
    // would fault `EINVAL` and abort crash recovery.
    //
    // So no process in a postmaster environment (the postmaster itself OR any
    // of its forked children) may remove the sets: they must outlive every
    // reinit, and the kernel reclaims the SysV sets when the postmaster finally
    // dies. Only a genuine standalone backend (`--single`,
    // `is_postmaster_environment() == false`) actually created its own sets and
    // must reclaim them here on exit.
    if init_small_seams::is_postmaster_environment::call() {
        return Ok(());
    }

    let mut state = SEMA_STATE.lock().unwrap();
    for i in 0..state.num_sema_sets as usize {
        let id = state.my_sema_sets[i];
        ipc_semaphore_kill(id);
    }
    // free(mySemaSets) — drop the Vec.
    state.my_sema_sets.clear();
    Ok(())
}

/// `PGSemaphoreCreate(void)` — allocate a `PGSemaphoreData` with initial count
/// 1 and return its assigned slot index (= ProcNumber). Called only under the
/// state lock (postmaster, never a backend in C: `Assert(!IsUnderPostmaster)`).
fn pg_semaphore_create_locked(state: &mut SemaState) -> PgResult<()> {
    if state.next_sema_number >= SEMAS_PER_SET {
        // Time to allocate another semaphore set.
        if state.num_sema_sets >= state.max_sema_sets {
            return Err(PgError::new(PANIC, "too many semaphores created".to_string()));
        }
        let new_set = ipc_semaphore_create(state, SEMAS_PER_SET)?;
        state.my_sema_sets.push(new_set);
        state.num_sema_sets += 1;
        state.next_sema_number = 0;
    }
    // Use the next shared PGSemaphoreData.
    if state.num_shared_semas >= state.max_shared_semas {
        return Err(PgError::new(PANIC, "too many semaphores created".to_string()));
    }
    let idx = state.num_shared_semas as usize;
    state.num_shared_semas += 1;
    // Assign the next free semaphore in the current set.
    let set_id = state.my_sema_sets[(state.num_sema_sets - 1) as usize];
    let sem_num = state.next_sema_number;
    state.next_sema_number += 1;
    state.shared_semas[idx] = PGSemaphoreData {
        semId: set_id,
        semNum: sem_num,
    };
    // Initialize it to count 1.
    ipc_semaphore_initialize(set_id, sem_num, 1)?;
    Ok(())
}

/// Resolve a `ProcNumber` to its assigned `PGSemaphoreData` (the value C
/// reaches as `GetPGProcByNumber(procno)->sem`).
fn sema_for_procno(procno: ProcNumber) -> PGSemaphoreData {
    let state = SEMA_STATE.lock().unwrap();
    state.shared_semas[procno as usize]
}

/// `PGSemaphoreReset(sema)` — reset a previously-initialized semaphore to have
/// count 0.
pub fn PGSemaphoreReset(procno: ProcNumber) {
    let sema = sema_for_procno(procno);
    // C is void; on the (FATAL) semctl failure path we panic, since the seam
    // contract is infallible and a SETVAL failure here is unrecoverable.
    ipc_semaphore_initialize(sema.semId, sema.semNum, 0).expect("PGSemaphoreReset: semctl failed");
}

/// `PGSemaphoreLock(sema)` — lock a semaphore (decrement count), blocking if
/// count would be < 0.
pub fn PGSemaphoreLock(procno: ProcNumber) {
    let sema = sema_for_procno(procno);
    semaphore_op(&sema, -1, 0).expect("PGSemaphoreLock: semop failed");
}

/// `PGSemaphoreUnlock(sema)` — unlock a semaphore (increment count).
pub fn PGSemaphoreUnlock(procno: ProcNumber) {
    let sema = sema_for_procno(procno);
    semaphore_unlock(&sema);
}

/// The shared body of `PGSemaphoreUnlock`, also used by `IpcSemaphoreCreate`'s
/// finalization (which holds a local `PGSemaphoreData`, not a procno).
fn semaphore_unlock(sema: &PGSemaphoreData) {
    semaphore_op(sema, 1, 0).expect("PGSemaphoreUnlock: semop failed");
}

/// `PGSemaphoreTryLock(sema)` — lock a semaphore only if able to do so without
/// blocking. Returns `true` if the lock was acquired.
pub fn PGSemaphoreTryLock(procno: ProcNumber) -> bool {
    let sema = sema_for_procno(procno);
    match semaphore_op(&sema, -1, libc::IPC_NOWAIT as libc::c_short) {
        Ok(()) => true,
        Err(TryLockBlocked) => false,
    }
}

/// Marker for the `EAGAIN`/`EWOULDBLOCK` non-blocking-failure path.
#[derive(Debug)]
struct TryLockBlocked;

/// Shared `semop()` loop for lock/unlock/trylock. `sem_flg` is `0` (blocking)
/// or `IPC_NOWAIT`. On `EINTR` we retry (matching the C `do/while`). A non-
/// `EINTR` failure is `FATAL`: for the blocking path we panic; for the
/// `IPC_NOWAIT` path an `EAGAIN`/`EWOULDBLOCK` returns `Err(TryLockBlocked)`.
fn semaphore_op(
    sema: &PGSemaphoreData,
    sem_op: libc::c_short,
    sem_flg: libc::c_short,
) -> Result<(), TryLockBlocked> {
    let mut sops = libc::sembuf {
        sem_num: sema.semNum as libc::c_ushort,
        sem_op,
        sem_flg,
    };
    loop {
        // SAFETY: semop on a single sembuf.
        let err_status = unsafe { libc::semop(sema.semId, &mut sops as *mut libc::sembuf, 1) };
        if err_status < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            if sem_flg & (libc::IPC_NOWAIT as libc::c_short) != 0 {
                // Expect EAGAIN or EWOULDBLOCK (platform-dependent).
                if e == libc::EAGAIN || e == libc::EWOULDBLOCK {
                    return Err(TryLockBlocked);
                }
            }
            // Otherwise we got trouble: elog(FATAL).
            panic!("semop(id={}) failed: {}", sema.semId, os_error_string(e));
        }
        return Ok(());
    }
}

// ---- OS helpers (direct libc; the genuine syscall boundary) ----

fn errno() -> libc::c_int {
    // SAFETY: __errno_location()/__error() via the libc errno accessor.
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn os_error_string(e: libc::c_int) -> String {
    std::io::Error::from_raw_os_error(e).to_string()
}

fn getpid() -> libc::pid_t {
    // SAFETY: getpid is always safe.
    unsafe { libc::getpid() }
}

/// `stat(path, &statbuf)` returning the `stat` struct or the errno.
fn stat(path: &str) -> Result<libc::stat, libc::c_int> {
    let c_path = match std::ffi::CString::new(path) {
        Ok(c) => c,
        Err(_) => return Err(libc::ENOENT),
    };
    // SAFETY: stat into a zeroed struct.
    let mut statbuf: libc::stat = unsafe { core::mem::zeroed() };
    let rc = unsafe { libc::stat(c_path.as_ptr(), &mut statbuf as *mut libc::stat) };
    if rc < 0 {
        Err(errno())
    } else {
        Ok(statbuf)
    }
}

/// Install the inward `PGSemaphore*` seams consumed by ipci/proc.
pub fn init_seams() {
    pg_sema_seams::pg_semaphore_shmem_size::set(PGSemaphoreShmemSize);
    pg_sema_seams::pg_reserve_semaphores::set(PGReserveSemaphores);
    pg_sema_seams::pg_semaphore_reset::set(PGSemaphoreReset);
    pg_sema_seams::pg_semaphore_lock::set(PGSemaphoreLock);
    pg_sema_seams::pg_semaphore_unlock::set(PGSemaphoreUnlock);
}
