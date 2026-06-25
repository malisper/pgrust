# Audit: backend-storage-ipc-procsignal

- **C source**: `src/backend/storage/ipc/procsignal.c` (PostgreSQL 18.3, 809 lines)
- **c2rust**: `c2rust-runs/backend-storage-ipc-procsignal/src/procsignal.rs`
- **Port**: `crates/backend-storage-ipc-procsignal/src/lib.rs`
- **Companion crates audited**: `backend-storage-ipc-procsignal-seams`,
  `backend-storage-ipc-shmem-seams`, `backend-storage-smgr-seams`,
  `backend-storage-ipc-sinval-seams`, `backend-commands-async-seams`,
  `backend-replication-walsender-seams`,
  `backend-replication-logical-applyparallelworker-seams`,
  `backend-storage-lmgr-condition-variable-seams`, `types-condvar`
- **Auditor basis**: independent re-derivation from the C source and headers,
  cross-checked function-by-function against the c2rust rendering.

## Function inventory (every definition in procsignal.c, plus in-crate helpers)

| # | Function | C location | Port location | Verdict | Notes |
|---|----------|-----------|---------------|---------|-------|
| 1 | `ProcSignalShmemSize` | procsignal.c:126 | lib.rs `ProcSignalShmemSize` | MATCH | `mul_size(NumProcSignalSlots, sizeof(ProcSignalSlot))` then `add_size(size, offsetof(ProcSignalHeader, psh_slot))` via shmem seams; offsetof = 8 = `size_of::<AtomicU64>()` (header is one `pg_atomic_uint64` then 8-aligned flexible array). Overflow ereports travel as `Err` exactly as C's `ereport(ERROR)`. Rust slot size differs from C's, but the function is a budget computation; logic identical. |
| 2 | `ProcSignalShmemInit` | procsignal.c:140 | lib.rs `ProcSignalShmemInit` | MATCH | `ShmemInitStruct` found/!found becomes `OnceLock::get_or_init`. Every `!found` initialization preserved per slot: pid=0, cancel_key_len=0, signalFlags zeroed, `pss_barrierGeneration = PG_UINT64_MAX` (`u64::MAX`), checkMask=0, CV init; header `psh_barrierGeneration = 0`. |
| 3 | `ProcSignalInit` | procsignal.c:175 | lib.rs `ProcSignalInit` | MATCH | Assert on key length -> debug_assert (>=0 implicit in slice). Both `elog(ERROR)`s present with exact messages (`MyProcNumber not set`; `unexpected MyProcNumber %d ... (max %d)` with max = slot count = MaxBackends+NUM_AUXILIARY_PROCS). Under the slot mutex: read old pid, clear all signal flags, checkMask=0, copy `psh_barrierGeneration` into slot generation, copy cancel key iff len>0, set len, set pid — same order. Post-release LOG "taking over" on `old_pss_pid != 0`. Sets thread-local slot, registers `CleanupProcSignalState` via `on_shmem_exit` seam with Datum 0. |
| 4 | `CleanupProcSignalState` (static) | procsignal.c:238 | lib.rs `CleanupProcSignalState` | MATCH | Clears `MyProcSignalSlot` first (signal-safety comment preserved); under mutex re-checks pid; mismatch path releases lock, LOGs `process %d releasing ProcSignal slot %d, but it contains %d` (slot index = `slot - psh_slot` = stored index) and returns without zeroing — exactly the C "don't ERROR while exiting" path. Match path: pid=0, key len=0, generation=`u64::MAX`, release, `ConditionVariableBroadcast` via CV seam. |
| 5 | `SendProcSignal` | procsignal.c:293 | lib.rs `SendProcSignal` | MATCH | procNumber arm: lock, compare pid under lock, set flag, unlock, `kill(pid, SIGUSR1)` return value passed through. INVALID_PROC_NUMBER arm: back-to-front scan, unlocked pre-check then locked re-check, identical. Fallthrough sets `errno = ESRCH` (via `libc::__error` on macOS) and returns -1. pid compare uses `pid as u32` = C's usual-arithmetic-conversion of `int` vs `uint32`. |
| 6 | `EmitProcSignalBarrier` | procsignal.c:365 | lib.rs `EmitProcSignalBarrier` | MATCH | `flagbit = 1 << type`; forward `fetch_or` loop over all slots; `pg_atomic_add_fetch_u64` = `fetch_add(1)+1`; reverse loop with unlocked pid pre-check, locked re-check, sets `PROCSIG_BARRIER` flag, kills with SIGUSR1; returns generation. SeqCst >= the full-barrier semantics C documents. |
| 7 | `WaitForProcSignalBarrier` | procsignal.c:433 | lib.rs `WaitForProcSignalBarrier` | MATCH | Assert -> debug_assert; DEBUG1 entry/exit messages verbatim; reverse slot loop; while `oldval < generation` calls `ConditionVariableTimedSleep(cv, 5000, WAIT_EVENT_PROC_SIGNAL_BARRIER)` via CV seam, timeout=true emits the LOG "still waiting for backend with PID %d..."; `ConditionVariableCancelSleep` per slot; final `pg_memory_barrier()` -> `fence(SeqCst)`. WAIT_EVENT_PROC_SIGNAL_BARRIER = `0x08000000|0x2A` = 134217770, verified against c2rust constant and wait_event_names.txt ordinal. Sleep's CHECK_FOR_INTERRUPTS error surfaces as `Err`. |
| 8 | `HandleProcSignalBarrierInterrupt` (static) | procsignal.c:492 | lib.rs `HandleProcSignalBarrierInterrupt` | MATCH | `InterruptPending = true` (init-small seam) then `ProcSignalBarrierPending = true`, same order. |
| 9 | `ProcessProcSignalBarrier` | procsignal.c:508 | lib.rs `ProcessProcSignalBarrier` | MATCH | Assert(MyProcSignalSlot); early return if `!ProcSignalBarrierPending`, then clears it; local/shared gen reads with `Assert(local<=shared)`; equal -> return; `swap(0)` = `pg_atomic_exchange_u32`. PG_TRY block becomes a closure: per-bit loop via `pg_rightmost_one_pos32`, switch with the single SMGRRELEASE arm (other bits: `processed` stays true, same as C's empty default), `BARRIER_CLEAR_BIT` always runs after the arm, `!processed` -> `ResetProcSignalBarrierBits(1<<type)` + `success=false`. PG_CATCH: on `Err`, `flags` still contains the failing bit (error propagates before the clear, matching C where the longjmp skips BARRIER_CLEAR_BIT), `ResetProcSignalBarrierBits(flags)` then re-throw (`return Err`). `!success` -> return without bumping generation. Success: store `shared_gen`, broadcast CV. Verified against c2rust sigsetjmp rendering. |
| 10 | `ResetProcSignalBarrierBits` (static) | procsignal.c:645 | lib.rs `ResetProcSignalBarrierBits` | MATCH | `fetch_or(flags)` on own checkMask; `ProcSignalBarrierPending = true`; `InterruptPending = true` — same order. |
| 11 | `CheckProcSignal` (static) | procsignal.c:658 | lib.rs `CheckProcSignal` | MATCH | Reads `MyProcSignalSlot` once (NULL -> `None` -> false); only clears the flag if seen set; no spinlock, as in C. |
| 12 | `procsignal_sigusr1_handler` | procsignal.c:683 | lib.rs `procsignal_sigusr1_handler` | MATCH | All 14 `CheckProcSignal` arms present, in C's exact order (CATCHUP, NOTIFY, PARALLEL_MESSAGE, WALSND_INIT_STOPPING, BARRIER, LOG_MEMORY_CONTEXT, PARALLEL_APPLY_MESSAGE, then recovery conflicts DATABASE, TABLESPACE, LOCK, SNAPSHOT, LOGICALSLOT, STARTUP_DEADLOCK, BUFFERPIN), each delegating to the owning unit's seam; BARRIER handled in-crate. Trailing `SetLatch(MyLatch)` via latch seam. SIGNAL_ARGS errno save/restore is the C trampoline's job; here the function is called by the signal dispatch layer. |
| 13 | `SendCancelRequest` | procsignal.c:738 | lib.rs `SendCancelRequest` | MATCH | PID==0 LOG + return; forward scan; unlocked pid pre-check, locked re-check (continue on mismatch); match = `len == cancel_key_len && timingsafe_bcmp(...) == 0` (Rust `&&` short-circuits so the bcmp/slice only runs on equal lengths, same as C); match -> DEBUG2 errmsg_internal then `kill(-pid, SIGINT)` (HAVE_SETSID arm — defined on all supported Unix, matches build config); mismatch -> LOG "wrong key..."; `return` after the locked-match slot either way; loop exhausted -> LOG "PID %d ... did not match any process". All messages verbatim; all reports LOG/DEBUG so the function stays infallible like the C. |
| 14 | `pg_rightmost_one_pos32` (inline, pg_bitutils.h) | pg_bitutils.h | lib.rs `pg_rightmost_one_pos32` | MATCH | `__builtin_ctz` -> `trailing_zeros()`, Assert(word!=0) -> debug_assert. |
| 15 | `timingsafe_bcmp` (libpgport, no catalog unit) | src/port/timingsafe_bcmp.c | lib.rs `timingsafe_bcmp` | MATCH | Non-OpenSSL arm: OR of byte XORs, returns `(ret != 0)`. Equal-length precondition guaranteed by the only caller. |

### Globals / data model

- `ProcSignalHeader`/`ProcSignalSlot`: shared memory -> process-global
  `OnceLock<ProcSignalHeader>` with `Box<[ProcSignalSlot]>`; `slock_t pss_mutex`
  -> `Mutex<CancelKey>` owning exactly the non-atomic fields it protects;
  `pss_pid`/`pss_signalFlags`/barrier words stay atomics (SeqCst, >= the C
  full-barrier RMW ops; c2rust normalized the same ops to SeqCst).
  `pss_barrierCV` is `types_condvar::ConditionVariable` (data shape only).
- `MyProcSignalSlot` (C static) -> thread-local `Cell<Option<usize>>` index.
- `ProcSignalBarrierPending` (C globals.c, lifecycle wholly owned by
  procsignal) -> thread-local, exposed via getter/setter and the unit's seam.
- Constants verified against headers: `ProcSignalReason` 0..13 ordering and
  `NUM_PROCSIGNALS = 14` (procsignal.h:32-52), `PROCSIGNAL_BARRIER_SMGRRELEASE
  = 0` (procsignal.h:56), `MAX_CANCEL_KEY_LENGTH = 32` (procsignal.h:67),
  `NUM_AUXILIARY_PROCS = 6 + MAX_IO_WORKERS(32)` (proc.h:460-461),
  `PG_UINT64_MAX` -> `u64::MAX`, `WAIT_EVENT_PROC_SIGNAL_BARRIER = 134217770`
  (c2rust constant + wait_event_names.txt).

## Seam audit

Inward (owned, `backend-storage-ipc-procsignal-seams`):
- `proc_signal_barrier_pending() -> bool`, `process_proc_signal_barrier() ->
  PgResult<()>` — signatures match the C declarations
  (`ProcSignalBarrierPending` read; `ProcessProcSignalBarrier(void)` whose
  PG_TRY re-throws). Both installed in `init_seams()`, which contains only the
  two `set()` calls; `seams-init::init_all()` calls
  `backend_storage_ipc_procsignal::init_seams()` (lib.rs:16). Consumer:
  `backend-postmaster-interrupt` (already merged) — real cycle. No `set()`
  outside the owner except test-local stubs in `#[cfg(test)]` modules.

Outward (all thin marshal+delegate, one call each, no logic in seam paths):
- `backend-utils-init-small-seams`: `max_backends`, `my_proc_number`,
  `my_proc_pid`, `set_interrupt_pending` — C globals owned by an unported unit.
- `backend-storage-ipc-seams::on_shmem_exit` — ipc.c unported.
- `backend-storage-ipc-shmem-seams::mul_size/add_size` — shmem.c unported;
  declarations carry the C overflow-ereport as `Err`.
- `backend-storage-smgr-seams::process_barrier_smgr_release` — smgr.c
  unported; `PgResult<bool>` models return-false and ereport separately,
  matching the C PG_TRY contract.
- `backend-storage-lmgr-condition-variable-seams`
  (`condition_variable_timed_sleep/cancel_sleep/broadcast`) —
  condition_variable.c unported; signatures match C (`long timeout` ms,
  returns timed-out bool; CancelSleep returns bool, discarded as in C).
- `backend-storage-ipc-latch-seams::set_latch_my_latch` — latch.c unported.
- SIGUSR1 arms: `backend-storage-ipc-sinval-seams`,
  `backend-commands-async-seams`, `backend-access-transam-parallel-seams`,
  `backend-replication-walsender-seams`,
  `backend-replication-logical-applyparallelworker-seams`,
  `backend-utils-mmgr-mcxt-seams`, `backend-tcop-postgres-seams`
  (`handle_recovery_conflict_interrupt(ProcSignalReason)`) — each handler is a
  one-line delegate to the unit that owns it in C; none of procsignal's own
  logic crosses outward.

No function body was replaced by a seam call; all 13 procsignal.c functions'
logic lives in this crate. `kill(2)`/`errno` go to `libc` directly (OS
boundary), consistent with the repo's signal-layer convention.

## Build/tests

`cargo build -p backend-storage-ipc-procsignal -p seams-init` clean;
`cargo test -p backend-storage-ipc-procsignal`: 7 passed, 0 failed.

## Verdict

**PASS** — all 15 inventory rows MATCH (none SEAMED-away, none
MISSING/PARTIAL/DIVERGES); zero seam findings.
