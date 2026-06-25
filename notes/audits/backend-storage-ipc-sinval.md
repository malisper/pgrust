# Audit: backend-storage-ipc-sinval

- **Date:** 2026-06-13
- **Auditor model:** Claude Fable 5
- **Branch:** port/backend-storage-ipc-sinval
- **Verdict:** PASS

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Sources re-derived from:
- C: `../pgrust/postgres-18.3/src/backend/storage/ipc/sinval.c` (203 lines),
  `.../sinvaladt.c` (713 lines)
- c2rust oracle: `../pgrust/c2rust-runs/backend-storage-ipc-sinval/src/{sinval,sinvaladt}.rs`
- Port: `crates/backend-storage-ipc-sinval/src/lib.rs`

## 1. Function inventory (completeness oracle)

The c2rust run kept exactly the build's functions. Cross-checked against the C.
All 12 function definitions across both files are present in the port; no
inline/static helper is dropped.

| # | C function | C loc | Port loc (lib.rs) | Verdict |
|---|-----------|-------|-------------------|---------|
| 1 | `SendSharedInvalidMessages` | sinval.c:46 | `SendSharedInvalidMessages` :152 | MATCH |
| 2 | `ReceiveSharedInvalidMessages` | sinval.c:68 | :176 | MATCH |
| 3 | `HandleCatchupInterrupt` | sinval.c:153 | :252 | MATCH |
| 4 | `ProcessCatchupInterrupt` | sinval.c:173 | :266 | MATCH |
| 5 | `SharedInvalShmemSize` | sinvaladt.c:217 | :551 | MATCH |
| 6 | `SharedInvalShmemInit` | sinvaladt.c:233 | :570 (+`initialize_sinval_memory`) | MATCH |
| 7 | `SharedInvalBackendInit` | sinvaladt.c:271 | :633 | MATCH |
| 8 | `CleanupInvalidationState` | sinvaladt.c:327 | :704 (+`cleanup_invalidation_state_callback`) | MATCH |
| 9 | `SIInsertDataEntries` | sinvaladt.c:369 | :761 (+`insert_data_entries_chunk`) | MATCH |
| 10 | `SIGetDataEntries` | sinvaladt.c:472 | :832 | MATCH |
| 11 | `SICleanupQueue` | sinvaladt.c:576 | :905 (+`cleanup_queue_locked`) | MATCH |
| 12 | `GetNextLocalTransactionId` | sinvaladt.c:700 | :1048 | MATCH |

Globals: `SharedInvalidMessageCounter` (sinval.c:24), `catchupInterruptPending`
(sinval.c:39), the static `messages[]`/`nextmsg`/`nummsgs` receive buffer
(sinval.c:73-80), `shmInvalBuffer` (sinvaladt.c:206), `nextLocalTransactionId`
(sinvaladt.c:209) all modelled as `thread_local!` per-backend cells (single-
threaded backend), except `shmInvalBuffer` which is a handle to genuine
byte-addressed shared memory (`InSegmentSISegHeader`). Conforms to memory rule
"shared statics for per-backend globals" — these are per-backend, not shared.

## 2. Per-function detail (re-derived spot-checks)

- **ReceiveSharedInvalidMessages**: outer pending-drain loop, then the C
  `do { ... } while (nummsgs == MAXINVALMSGS)` rendered as `loop { ... }` with
  break-on-reset (`getResult < 0` -> elog DEBUG4 "cache state reset", bump,
  reset_function, break) and break-when-not-full (`NUMMSGS != MAXINVALMSGS`).
  Counter bumped once per message and once per reset, matching C. Final catchup
  arm (DEBUG4 "sinval catchup complete, cleaning queue" + `SICleanupQueue(false,0)`)
  preserved. Static-buffer recursion semantics preserved via `thread_local!`.
  MAXINVALMSGS=32 verified against sinval.c:72.
- **SIInsertDataEntries**: WRITE_QUANTUM=64 chunking; full-or-threshold cleanup
  loop with recheck (`numMsgs + nthistime > MAXNUMMESSAGES || numMsgs >= nextThreshold`),
  circular-buffer write at `max % MAXNUMMESSAGES`, maxMsgNum publish under the
  in-segment `msgnumLock` spinlock, then the per-proc `hasMessages=true` kick,
  then `LWLockRelease`. Cleanup is invoked with callerHasWriteLock=true and the
  write-lock guard is threaded through so the lock survives the call, matching C.
- **SIGetDataEntries**: unlocked `hasMessages` fast path (return 0); SInvalReadLock
  SHARED; `hasMessages=false` before reading; maxMsgNum read under spinlock;
  resetState branch sets nextMsgNum=max, clears resetState+signaled, returns -1;
  drain loop bounded by `n < datasize && nextMsgNum < max`; caught-up clears
  `signaled` else sets `hasMessages`. Return values 0 / n / -1 all match.
- **SICleanupQueue / cleanup_queue_locked**: min/minsig/lowbound init
  (`min=maxMsgNum; minsig=min-SIG_THRESHOLD; lowbound=min-MAXNUMMESSAGES+minFree`),
  sendOnly/resetState skip, force-reset when `n < lowbound`, global-min tracking,
  furthest-unsignaled selection, MSGNUMWRAPAROUND decrement of all counters,
  nextThreshold recompute (`numMsgs < CLEANUP_MIN ? CLEANUP_MIN :
  (numMsgs/CLEANUP_QUANTUM + 1)*CLEANUP_QUANTUM`), and the release-before-signal /
  re-acquire-iff-callerHasWriteLock dance preserved exactly. The C
  `his_procNumber = needSig - &procState[0]` (ProcState array index) equals the
  tracked pgprocno since procState[] is indexed by pgprocno — correct.
- **SharedInvalBackendInit / CleanupInvalidationState**: MyProcNumber<0 ->
  ERROR, >=NumProcStateSlots -> PANIC; pgprocnos append/numProcs++; nextLXID
  copy-in/out; in-use-slot ERROR (lock released first, matching sinval slot
  re-use path); on_shmem_exit registration; dense-array swap-remove on cleanup;
  not-found -> PANIC. All control flow and severities match.
- **GetNextLocalTransactionId**: do/while skipping InvalidLocalTransactionId,
  post-increment with wrap. MATCH.

## 3. Constants verified against C headers (not from memory)

- `MAXNUMMESSAGES=4096`, `MSGNUMWRAPAROUND=4096*262144`, `CLEANUP_MIN=2048`,
  `CLEANUP_QUANTUM=256`, `SIG_THRESHOLD=2048`, `WRITE_QUANTUM=64`,
  `MAXINVALMSGS=32` — all match sinvaladt.c:129-134 / sinval.c:72.
- `NumProcStateSlots = MaxBackends + NUM_AUXILIARY_PROCS`; `NUM_AUXILIARY_PROCS
  = 6 + MAX_IO_WORKERS = 6 + 32 = 38` verified against
  `src/include/storage/proc.h:460-461` (`types_storage::NUM_AUXILIARY_PROCS`).
- `SINVAL_READ_LOCK=5`, `SINVAL_WRITE_LOCK=6` verified against
  `src/include/storage/lwlocklist.h:38-39` (PG_LWLOCK(5,SInvalRead),
  PG_LWLOCK(6,SInvalWrite)).
- Layout: `proc_state_offset() = offsetof(numProcs)+sizeof(int)` mirrors
  `offsetof(SISeg, procState)`; pgprocnos array implicit immediately after
  procState[slots], matching C's `pgprocnos = &procState[NumProcStateSlots]`.

## 4. Seam & wiring audit

Owned seam crate (by C-source coverage): `crates/backend-storage-ipc-sinval-seams`
(maps to sinval.c / sinvaladt.c). It declares 8 seams; all 8 are installed by
the crate's `init_seams()` (lib.rs:1065-1078):
send_shared_invalid_messages, receive_shared_invalid_messages,
handle_catchup_interrupt, shared_inval_backend_init,
get_next_local_transaction_id, shared_invalid_message_counter,
shared_inval_shmem_size, shared_inval_shmem_init. No uninstalled declaration, no
`set()` outside the owner. `init_seams()` contains nothing but `set()` calls.
`seams-init::init_all()` calls `backend_storage_ipc_sinval::init_seams();`
(seams-init/src/lib.rs:108) and the Cargo dep is present (line 112).
Both recurrence guards pass.

Outward seam calls (all thin marshal+delegate, no branching/computation in seam
path): xact (is_transaction_or_transaction_block, start/commit_transaction_command),
inval (accept_invalidation_messages), latch (set_latch_my_latch), procsignal
(send_proc_signal), ipc (on_shmem_exit), shmem (shmem_init_struct/add_size/
mul_size), init_small (my_proc_number/my_proc_pid/max_backends). Spinlock via the
ported `s_lock`/`s_unlock` (direct dep, in-segment slock_t — not a seam).
No function body was replaced by a "somewhere else" delegate; all 12 cores live
in this crate.

## 3b. Design conformance

- Allocating/fallible fns return `PgResult`; ereport ERROR/PANIC -> `Err(PgError)`
  with matching severity. No locks held across `?` without a guard
  (`SinvalLwLockGuard` Drop backstop + explicit `release()` at C's release points;
  `with_msgnum_lock` brackets the spinlock without `?` inside).
- No invented opacity: `SISeg`/`ProcState`/`InSegmentSISegHeader` are real repr(C)
  structs over real shared memory, not stand-in handles.
- No shared statics for per-backend globals; no ambient-global seams; no
  registry-shaped side tables; no unledgered divergence markers.

## Gate

- `cargo check --workspace`: clean (only pre-existing unrelated warnings).
- `cargo test --workspace`: pass (no failures; the 2 known timeout flakes did
  not fire).
- `backend-storage-ipc-sinval` unit tests: 5 passed.
- `seams-init` recurrence guards: 2 passed.

## Verdict: PASS

Every function MATCH; zero seam findings; design-conformance clean. Catalog row
set to `audited`.
