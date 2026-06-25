# Audit: backend-postmaster-startup

- Unit: `backend-postmaster-startup` (`src/backend/postmaster/startup.c`, PostgreSQL 18.3)
- Port: `crates/backend-postmaster-startup` (+ `crates/backend-postmaster-startup-seams`)
- C source: `../pgrust/postgres-18.3/src/backend/postmaster/startup.c` (378 lines)
- c2rust: `../pgrust/c2rust-runs/backend-postmaster-startup/src/startup.rs`
- Auditor: independent re-derivation from C + c2rust; constants re-verified
  against headers (`utils/timeout.h`, `miscadmin.h`, `access/xlogutils.h`,
  `utils/guc.h`, `src/include/port.h`, `src/port/pqsignal.c`).

## Function inventory and verdicts

Every function definition in startup.c (statics included), cross-checked
against the c2rust rendering (which kept all 16 of them; no `#if` branches
outside the build config exist in this file other than the
`USE_POSTMASTER_DEATH_SIGNAL` split, covered below):

| # | C function (startup.c) | Port location (crates/backend-postmaster-startup/src/lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `StartupProcTriggerHandler` (l.92) | `StartupProcTriggerHandler` (l.91) | MATCH | Sets promote flag, `WakeupRecovery()` via xlogrecovery seam. |
| 2 | `StartupProcSigHupHandler` (l.100) | `StartupProcSigHupHandler` (l.98) | MATCH | Sets got_SIGHUP, wakes recovery. |
| 3 | `StartupProcShutdownHandler` (l.108) | `StartupProcShutdownHandler` (l.104) | MATCH | `in_restore_command` → `proc_exit(1)` (seam returns `!`, so the trailing `WakeupRecovery()` runs only on the else branch, same as C). |
| 4 | `StartupRereadConfig` (l.124) | `StartupRereadConfig` (l.117) | MATCH | conninfo/slotname/tempSlot snapshotted before `ProcessConfigFile(PGC_SIGHUP)`; tempSlot change only considered when slotname unchanged and the *new* `PrimarySlotName` is empty; restart requested iff any of the three changed. `pstrdup`/`pfree` become owned `String` snapshots. `PGC_SIGHUP = 2` verified against guc.h enum order. |
| 5 | `ProcessStartupProcInterrupts` (l.153) | `ProcessStartupProcInterrupts` (l.169) | MATCH | Order preserved: SIGHUP reload → shutdown `proc_exit(1)` → postmaster-death probe → barrier → memory-context log. Death probe: short-circuit `&&` increments `postmaster_poll_count` only when under postmaster, `wrapping_add` + `% 1024` matches `uint32` semantics (POSTMASTER_POLL_RATE_LIMIT = 1024 verified). C's `#ifndef USE_POSTMASTER_DEATH_SIGNAL` rate-limit becomes `cfg(not(linux/freebsd))`, matching pmsignal.h's `PR_SET_PDEATHSIG`/`PROC_PDEATHSIG_CTL` definition sites; on linux/freebsd the probe runs every call, as in C. Dead-postmaster path is `libc::exit(1)`, deliberately not `proc_exit`, as in C. |
| 6 | `StartupProcExit` (l.202) | `StartupProcExit` (l.209) | MATCH | `standbyState != STANDBY_DISABLED` → `ShutdownRecoveryTransactionEnvironment()`. `STANDBY_DISABLED = 0` verified against xlogutils.h. |
| 7 | `StartupProcessMain` (l.215) | `StartupProcessMain` (l.227) | MATCH | `assert_eq!(len, 0)` = C `Assert`; `MyBackendType = B_STARTUP` (= 13, verified against the miscadmin.h enum order); full sequence preserved: AuxiliaryProcessMainCommon → on_shmem_exit(StartupProcExit, 0) → pqsignal SIGHUP/SIGINT(IGN)/SIGTERM → InitializeTimeouts → SIGPIPE(IGN)/SIGUSR1(procsignal)/SIGUSR2 → SIGCHLD(DFL) → RegisterTimeout x3 (ids 4/5/6 verified) → sigprocmask(SIG_SETMASK, UnBlockSig) → StartupXLOG → proc_exit(0). `proc_exit` seam is `-> !`, so the function cannot fall through, matching `pg_noreturn`. |
| 8 | `PreRestoreCommand` (l.267) | `PreRestoreCommand` (l.299) | MATCH | Flag set before the shutdown_requested re-check, same order as C. |
| 9 | `PostRestoreCommand` (l.281) | `PostRestoreCommand` (l.311) | MATCH | |
| 10 | `IsPromoteSignaled` (l.287) | `IsPromoteSignaled` (l.316) | MATCH | |
| 11 | `ResetPromoteSignaled` (l.293) | `ResetPromoteSignaled` (l.321) | MATCH | |
| 12 | `startup_progress_timeout_handler` (l.302) | `startup_progress_timeout_handler` (l.328) | MATCH | |
| 13 | `disable_startup_progress_timeout` (l.308) | `disable_startup_progress_timeout` (l.333) | MATCH | interval==0 early return; `disable_timeout(STARTUP_PROGRESS_TIMEOUT /*=12*/, false)`; flag cleared after. |
| 14 | `enable_startup_progress_timeout` (l.322) | `enable_startup_progress_timeout` (l.345) | MATCH | interval==0 early return; `fin_time = start + ms * (int64)1000` (`TimestampTzPlusMilliseconds` reproduced inline, verified against utils/timestamp.h); `enable_timeout_every(12, fin_time, interval)`. |
| 15 | `begin_startup_progress_phase` (l.342) | `begin_startup_progress_phase` (l.364) | MATCH | interval==0 early return, then disable + enable. |
| 16 | `has_startup_progress_timeout_expired` (l.358) | `has_startup_progress_timeout_expired` (l.378) | MATCH | Returns `None` when not expired; otherwise `TimestampDifference(start, now)` via seam, flag reset, `Some((secs, usecs))` replaces the C out-params + bool. |

File-scope state: the four `volatile sig_atomic_t` flags,
`startup_progress_phase_start_time`, `startup_progress_timer_expired`, the
`log_startup_progress_interval` GUC (default 10000, matching C l.76), and the
function-local `static uint32 postmaster_poll_count` are all per-process
backend state and are thread-locals with the same initial values. The GUC gets
get/set accessors exposed through the unit's own seam crate for the GUC
machinery.

Error mapping: this file has no `ereport`/`elog` of its own; the `PgResult`
plumbing on `ProcessStartupProcInterrupts`/`StartupRereadConfig`/
`StartupProcessMain` only propagates errors thrown by callees
(`ProcessConfigFile`, `ProcessProcSignalBarrier`,
`ProcessLogMemoryContextInterrupt`, `AuxiliaryProcessMainCommon`,
`StartupXLOG`), matching C's longjmp escape. `got_SIGHUP` is cleared before
`StartupRereadConfig` runs, so an error escaping mid-reload leaves the same
flag state as C.

## Seam audit

Inward (`crates/backend-postmaster-startup-seams`): 11 declarations
(`process_startup_proc_interrupts`, `pre_restore_command`,
`post_restore_command`, `is_promote_signaled`, `reset_promote_signaled`,
`begin_startup_progress_phase`, `disable_startup_progress_timeout`,
`has_startup_progress_timeout_expired`, `startup_progress_timeout_handler`,
`log_startup_progress_interval`, `set_log_startup_progress_interval`) — all 11
installed by `backend_postmaster_startup::init_seams()`, which contains
nothing but `set()` calls; `seams-init::init_all()` calls it
(crates/seams-init/src/lib.rs:16). Justified by real cycles: xlog.c /
xlogrecovery.c / xlogarchive.c / fd.c / reinit.c and the GUC tables all call
back into startup.c.

Outward seam calls (all thin marshal + delegate; every target unit is
unported, so a direct dependency cannot exist):

| Seam crate | Calls used | Owner unit (unported) | Thin? |
|---|---|---|---|
| backend-access-transam-xlog-seams | `startup_xlog` | xlog.c | yes |
| backend-access-transam-xlogrecovery-seams | `wakeup_recovery`, `primary_conninfo`, `primary_slot_name`, `wal_receiver_create_temp_slot`, `startup_request_wal_receiver_restart` | xlogrecovery.c | yes (GUC reads return owned snapshots = the C `pstrdup`) |
| backend-access-transam-xlogutils-seams | `standby_state` | xlogutils.c | yes |
| backend-storage-ipc-pmsignal-seams | `postmaster_is_alive` | pmsignal.c (owner carries the `PostmasterIsAlive()` fast-path/`PostmasterIsAliveInternal` split from pmsignal.h — that logic belongs to the owner, not here) | yes |
| backend-storage-ipc-standby-seams | `shutdown_recovery_transaction_environment`, `standby_dead_lock_handler`, `standby_timeout_handler`, `standby_lock_timeout_handler` | standby.c | yes |
| backend-postmaster-auxprocess-seams | `auxiliary_process_main_common` | auxprocess.c | yes |
| backend-utils-misc-timeout-seams | `initialize_timeouts`, `register_timeout`, `enable_timeout_every`, `disable_timeout` | timeout.c | yes |
| port-pqsignal-seams | `pqsignal` | src/port/pqsignal.c (`pqsignal_be`) | yes (post-fix, see findings) |
| pre-existing seams | `proc_exit`, `on_shmem_exit` (ipc), `is_under_postmaster`, `set_my_backend_type` (init), procsignal trio, mcxt pair, `process_config_file` (guc), timestamp pair | — | yes |

`UnBlockSig` comes from the already-ported `backend-libpq-pqsignal` crate
(direct dependency, no cycle) — correct, since `pqinitmask()`/the mask globals
live in `src/backend/libpq/pqsignal.c`.

No `set()` of any foreign unit's seam exists in production code (test-only
mocks excepted, as permitted).

Constants re-verified against headers: TimeoutId 0–13/MAX_TIMEOUTS=23
(`utils/timeout.h`), BackendType 0–17 incl. B_STARTUP=13 (`miscadmin.h`),
HotStandbyState 0–3 (`access/xlogutils.h`), PGC_SIGHUP=2 (`utils/guc.h`),
POSTMASTER_POLL_RATE_LIMIT=1024 (startup.c), default interval 10000. All match
the c2rust rendering and the type crates.

## Findings and fixes

1. **(round 1, FAIL → fixed)** `port-pqsignal-seams::pqsignal` was declared
   `fn pqsignal(signo: i32, func: SigDisposition) -> SigDisposition`,
   documented as returning the previous disposition. PostgreSQL 18.3's
   `src/port/pqsignal.c` is `void pqsignal(int signo, pqsigfunc func)` (the
   value-returning signature exists only in the frozen
   `interfaces/libpq/legacy-pqsignal.c`, which has its own crate). The seam
   contract would have forced the future owner to fabricate a return value.
   Fixed: declaration now returns nothing; doc corrected; the test mock
   updated. startup.c discards the result at every call site, so no caller
   behavior changed.

Re-audit of the fix from scratch: declaration now mirrors the 18.3 signature
(`void`, verified at `src/port/pqsignal.c` l.122 and `port.h` l.530
`#define pqsignal pqsignal_be`); all call sites unchanged; workspace builds
and all tests pass.

## Verdict

**PASS** — 16/16 functions MATCH (callee delegation via seams per the rules),
zero outstanding seam findings. Spot-checks re-derived in full detail:
`StartupRereadConfig` (snapshot-before-reload ordering, empty-slotname
predicate on the new value), `ProcessStartupProcInterrupts` (short-circuit
increment, wrapping uint32 modulo, exit-vs-proc_exit), `StartupProcessMain`
(exact 15-step call sequence vs the c2rust rendering).
