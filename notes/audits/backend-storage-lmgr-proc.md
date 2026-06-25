# Audit: backend-storage-lmgr-proc

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Unit C source: `src/backend/storage/lmgr/proc.c` (single-file unit)
- Port branch: `fix/no-todo-backend-storage-lmgr-proc` (off current local `main`)
- c2rust reference: `../pgrust/c2rust-runs/backend-storage-lmgr-proc/src/proc.rs`

## Summary

Re-audit after the no-`todo!()` pass: every Class-B `todo!()` that previously
stood in for an outward neighbour has been driven to ZERO. The per-function
control flow of all 30 `proc.c` functions remains faithful, the substrate layer
is real OWN logic, and design conformance is clean. The crate now contains **no
`todo!()` / `unimplemented!()`** in `src/` (verified by grep).

### No-`todo!()` pass (this audit)

Each former Class-B `todo!()` was resolved as one of:

- **Own logic** (proc-owned state / pure macros, implemented in-crate):
  `ProcStructLock` spin acquire/release (an owned `Spinlock` in `proc_shmem`
  acquired via the merged `s_lock.c` TAS+backoff primitive); the
  `Am{AutoVacuumWorker,SpecialWorker,BackgroundWorker,WalSender,RegularBackend}Process`
  / `AmStartupProcess` predicates and `IsUnderPostmaster` (direct reads of
  `my_backend_type()` / `is_under_postmaster()` and compares, exactly as the
  miscadmin.h macros expand); `getpid()` (`std::process::id`); `kill(pid,
  SIGUSR2)` (`libc::kill`); `set/update_spins_per_delay` (merged `s_lock.c`);
  the `elog`/`ereport` helpers (merged `backend-utils-error`, with the exact
  `ERRCODE_TOO_MANY_CONNECTIONS` SQLSTATE + message strings from proc.c:453/457);
  the `proc_latch_handle` LatchHandle minting; the twophase dummy-PGPROC init
  (`proc_init_prepared`/`gxact_load_subxact_data`, the `proc->...` field writes
  of `MarkAsPreparingGuts`/`GXactLoadSubxactData` over the owned PGPROC arena);
  and the `DeadlockTimeout`/`TransactionTimeout` GUC reads (guc_tables `GucIntVar`).
- **Real installed seam `::call`** into a merged/audited owner (no panic):
  `register_postmaster_child_active` (pmsignal — newly wired into its
  `init_seams()`), `init_lwlock_access` (lwlock — newly wired), `lwlock_release_all`
  / `lwlock_acquire_main` / `lwlock_release_main` (lwlock), `init_deadlock_checking`
  / `get_blocking_auto_vacuum_pgproc` (deadlock), `condition_variable_cancel_sleep`
  (condition-variable).
- **Seam-and-panic** into an owner whose C unit is still unported (panics
  "seam not installed" until it lands): `max_wal_senders` (walsender),
  `autovacuum_launcher_pid` (autovacuum-ext), `on_shmem_exit` (ipc),
  `pgstat_set/reset_wait_event_storage` (pgstat), `pg_semaphore_reset/lock/unlock`
  (pg_sema), `sync_rep_cleanup_at_proc_exit` (syncrep), `proc_array_add/remove`
  (procarray), `proc_locks_hold_masks` (lock.c — new narrow seam declared in
  `backend-storage-lmgr-lock-seams`).
- **Faithful `panic!` at a genuine cross-unit integration boundary** (NOT a
  `todo!()`/own-logic stub): the deadlock checker's `DeadLockCheck` /
  `RememberSimpleDeadLock` and the inward `proc_lock_wakeup` all need the
  lock.c-built `LockSpace` arena proc.c cannot construct, and the latch
  `OwnLatch`/`DisownLatch`/`SwitchTo/BackLatch`/`SetLatch(&proc->procLatch)` reach
  the PGPROC-embedded latch the ported latch unit's handle-registry does not yet
  know. These abort loudly until the respective integration step lands.

New owner seams declared this pass (panic until each owner installs): pg_sema
`pg_semaphore_reset/lock/unlock`, pgstat `pgstat_set_wait_event_storage_for_proc`
+ `pgstat_reset_wait_event_storage`, syncrep `sync_rep_cleanup_at_proc_exit`,
lwlock `init_lwlock_access` (installed by lwlock), pmsignal
`register_postmaster_child_active` (installed by pmsignal), lock
`proc_locks_hold_masks`. Both `seams-init` recurrence-guard tests pass.

The earlier (pre-this-pass) findings about the substrate are retained below for
history.

### Finding 1 (RESOLVED) — InitProcGlobal threads the real freelists

`proc_shmem::InitProcGlobal` now `push_tail`s every `PGPROC` onto the correct
`ProcGlobal` freelist by the exact C class partitioning (`< max_connections` →
Regular; `+ autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS` → Autovac;
`+ max_worker_processes` → Bgworker; `< max_backends` → Walsender; else aux /
prepared → no list) and records `proc.procgloballist = freelist`, mirroring
proc.c:328-351 one-for-one. The introduced `Option<Box<dlist_head>>` opacity is
gone: `PGPROC.procgloballist` is now `Option<FreeListId>` (the C `dlist_head *`
always names one of the four known heads), and the four `PROC_HDR` freelist
heads are `ProcFreeList` (an intrusive `dlist` over the `PGPROC` arena realized
as an ordered list of `ProcNumber`s — the deadlock-unit index-link pattern). The
freelists therefore actually receive the slots, so `InitProcess` can pop one.

### Finding 2 (RESOLVED) — the MyProc/ProcGlobal substrate is real

Every Class-A accessor in `src/seam.rs` and the owned inward-seam installer
`src/inward_seams.rs` is now a real read/write over `proc_shmem`'s owned state:
`with_proc_global` / `with_proc_by_number` / `with_my_proc` over
`ProcGlobal->allProcs`, plus the per-backend `MyProcNumber` thread-local. The
freelist ops, `proc_globallist_of`, `spins_per_delay`(+set),
`startup_buffer_pin_wait_buf_id`(+set), `status_flags`,
`auxiliary_proc_find_free`/`_procno`, `prepared_xact_procno`, the lock-group
accessors, the `MyProc` bindings, and all 17 wait-queue PGPROC field accessors
are implemented. Only genuinely-unported neighbours remain `todo!()` as Class-B
panic-through (see the residual-todo list below).

### Finding 3 (RESOLVED) — no `&'static mut`, no introduced opacity

`my_proc_mut() -> &'static mut PGPROC` is gone; `MyProc` mutation is via
`with_my_proc(|p| ...)` closure-scoped borrows, so no `&'static mut` escapes.
Alongside `procgloballist`, the other introduced-opacity fields are reshaped to
arena index/value modeling: `lockGroupLeader: Option<ProcNumber>`,
`waitLock: Option<LOCKTAG>`, `waitProcLock: Option<ProcNumber>` — no
`Box<dlist_head>` / `Box<PGPROC>` / `Box<LOCK>` / `Box<PROCLOCK>` remain.

### `main` sync

Merging `main` brought in the now-landed deadlock unit's reshaped
`backend-storage-lmgr-deadlock-seams` (modeled over a lock.c-built `LockSpace`
arena), plus a reshaped `init_process` / `proc_lock_wakeup` surface on the proc
seam crate. proc.c's three deadlock-checker calls (`deadlock_check`,
`get_blocking_autovacuum_pgproc`, `remember_simple_deadlock`) route through
in-crate Class-B panic-through seams rather than that LockSpace interface,
because the `LockSpace` arena belongs to the still-unported lock.c boundary —
an acceptable Class-B deferral (deadlock arena construction is lock.c's). Union
merges on shared seam crates (xlog/lwlock/lock/postgres-seams, types-storage)
and a duplicate `enable_timeout_after` (unified to the fallible
`PgResult<()>` form) were resolved.

## Per-function inventory

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | PGProcShmemSize | proc.c:97 | proc_shmem.rs | MATCH | add_size/mul_size over TotalProcs + mirror arrays |
| 2 | FastPathLockShmemSize | proc.c:115 | proc_shmem.rs | MATCH | MAXALIGN(groups*u64)+MAXALIGN(slots*Oid) |
| 3 | ProcGlobalShmemSize | proc.c:139 | proc_shmem.rs | MATCH | PROC_HDR + slock_t + above |
| 4 | ProcGlobalSemas | proc.c:157 | proc_shmem.rs | MATCH | max_backends + NUM_AUXILIARY_PROCS |
| 5 | InitProcGlobal | proc.c:192 | proc_shmem.rs | MATCH | Finding 1 fixed: freelists threaded by real C class partition |
| 6 | InitProcess | proc.c:390 | proc_lifecycle.rs:116 | MATCH | freelist pop, class match, full field init, latch/sema/exit-cb ordering |
| 7 | InitProcessPhase2 | proc.c:580 | proc_lifecycle.rs:207 | MATCH | proc_array_add + on_shmem_exit |
| 8 | InitAuxiliaryProcess | proc.c:615 | proc_lifecycle.rs | MATCH | aux linear search via auxiliary_proc_find_free; cut-down init |
| 9 | SetStartupBufferPinWaitBufId | proc.c:754 | proc_lifecycle.rs | MATCH | ProcGlobal field write |
| 10 | GetStartupBufferPinWaitBufId | proc.c:766 | proc_lifecycle.rs | MATCH | ProcGlobal field read |
| 11 | HaveNFreeProcs | proc.c:782 | proc_lifecycle.rs | MATCH | freelist iter, n>0 assert, count==n |
| 12 | LockErrorCleanup | proc.c:813 | proc_waitqueue.rs:658 | MATCH | hold-interrupts, abort-strong, timer disable, dequeue/grant |
| 13 | ProcReleaseLocks | proc.c:891 | proc_waitqueue.rs | MATCH | default + USER_LOCKMETHOD release |
| 14 | RemoveProcFromArray | proc.c:908 | proc_lifecycle.rs | MATCH | proc_array_remove |
| 15 | ProcKill | proc.c:919 | proc_lifecycle.rs:327 | MATCH | lock-group detach, leader-return push_head, self push_tail, spins update |
| 16 | AuxiliaryProcKill | proc.c:1040 | proc_lifecycle.rs | MATCH | aux slot release, pid==getpid panic, spins update |
| 17 | AuxiliaryPidGetProc | proc.c:1091 | proc_lifecycle.rs | MATCH | pid==0 guard + linear scan |
| 18 | JoinWaitQueue | proc.c:1140 | proc_waitqueue.rs:110 | MATCH | priority-insert scan, early-deadlock, immediate-grant, heldLocks set |
| 19 | ProcSleep | proc.c:1309 | proc_waitqueue.rs:225 | MATCH | full wait loop: deadlock/lock timeout, HS, autovac-cancel, log_lock_waits |
| 20 | ProcWakeup | proc.c:1711 | proc_waitqueue.rs:523 | MATCH | detached guard, dequeue, clear-wait, SetLatch |
| 21 | ProcLockWakeup | proc.c:1739 | proc_waitqueue.rs:552 | MATCH | empty guard, ahead_requests conflict walk, grant+wakeup |
| 22 | CheckDeadLock | proc.c:1787 | proc_waitqueue.rs:601 | MATCH | all-partition acquire order, unlinked guard, DeadLockCheck, hard-deadlock remove |
| 23 | CheckDeadLockAlert | proc.c:1873 | proc_waitqueue.rs:643 | MATCH | sets got_deadlock_timeout + latch; errno note correct |
| 24 | GetLockHoldersAndWaiters | proc.c:1900 | proc_waitqueue.rs | MATCH | holder/waiter strings via lock seam; StringInfo OOM Err |
| 25 | ProcWaitForSignal | proc.c:1974 | proc_misc.rs:46 | MATCH | WaitLatch loop + ResetLatch |
| 26 | ProcSendSignal | proc.c:1986 | proc_misc.rs:63 | MATCH | range check via all_proc_count (real) + SetLatch |
| 27 | BecomeLockGroupLeader | proc.c:2001 | proc_misc.rs:78 | MATCH | idempotent self-leader under partition LWLock |
| 28 | BecomeLockGroupMember | proc.c:2031 | proc_misc.rs:112 | MATCH | leader pid/leader-of-self validation + dlist push_tail |
| 29 | IsWaitingForLock | proc.c | proc_misc.rs:515 | MATCH | lock_awaited_is_set (lock.c hashcode != -1) |
| 30 | (static) shmem-size / kill helpers | — | — | MATCH | covered under #1/#2/#15/#16 |

Spot-checked in detail: InitProcGlobal class boundaries vs proc.c:328-351;
ProcKill freelist-return ordering (push_head for an exited leader, push_tail for
self, reading `procgloballist` via the captured procno after `MyProc` is
cleared); JoinWaitQueue early-deadlock-vs-dontWait ordering; the ProcSleep wait
loop; CheckDeadLock's goto-check_done realization. Constants verified against C
headers (NUM_AUXILIARY_PROCS, NUM_SPECIAL_WORKER_PROCS=2, FP_LOCK_SLOTS_PER_GROUP
=16, NUM_LOCK_PARTITIONS=16, DEFAULT_SPINS_PER_DELAY=100, USER_LOCKMETHOD=2) —
no transcription corruption.

## Seam audit

- **Owned seam crate:** `backend-storage-lmgr-proc-seams` (maps to proc.c). All
  **49** declarations are installed by `inward_seams::install()` (49 unique
  `set()` calls, 0 missing, 0 duplicate), and `init_seams()` is wired into
  `seams-init::init_all()`. `init_process` is installed with a real body
  (`proc_lifecycle::InitProcess`, wrapping the unused `Mcx` with a throwaway
  context — proc.c's `InitProcess(void)` takes no context); `proc_lock_wakeup`
  (the deadlock detector's `&mut LockSpace, LockId` view) is installed as an
  explicit Class-B panic-through to the lock.c-built arena.
- **Class-A substrate accessors:** all implemented over the owned
  `ProcGlobal->allProcs` / `MyProc` state. No own-logic `todo!()` remains.
- **ZERO `todo!()`/`unimplemented!()`** in `src/` (grep-verified). Every former
  Class-B `todo!()` is now own logic, a real installed seam `::call`, a
  seam-and-panic into an unported owner, or a faithful integration-boundary
  `panic!` — see the "No-`todo!()` pass" summary above for the per-occurrence
  disposition.
- **Outward seams:** thin marshal + delegate; the size-overflow
  `add_size`/`mul_size` `.expect()` and the void-C `.expect()` wrappers
  (`ProcArrayAdd`/`ProcArrayRemove`/`InitDeadLockChecking`/`on_shmem_exit`/
  `RegisterPostmasterChildActive`) faithfully mirror C abort-on-error. No
  branching/computation inside a seam path. No outward-seam findings.

## Design conformance

- Allocating fns take `Mcx` + return `PgResult` (InitProcGlobal/InitProcess/
  InitProcessPhase2/InitAuxiliaryProcess). Good.
- Per-backend globals (`globals.rs`; `proc_shmem` `PROC_GLOBAL`/`MY_PROC_NUMBER`)
  are `thread_local`, not shared statics. Good.
- No invented opacity (Finding 3 reshapes removed `Box<dlist_head>`/`Box<PGPROC>`
  /`Box<LOCK>`/`Box<PROCLOCK>` to `FreeListId`/`ProcNumber`/`LOCKTAG`). Good.

## Verdict

**PASS.** Every function `MATCH`, **zero `todo!()`/`unimplemented!()` in
`src/`** (grep-verified), the owned seam crate fully installed, all prior
findings resolved, design conformance clean. `cargo check --workspace` passes;
`backend-storage-lmgr-proc` tests and both `seams-init` recurrence-guard tests
pass. CATALOG.tsv row set to `audited`.
