# Audit: backend-postmaster-bgworker

- **Unit:** `backend-postmaster-bgworker`
- **C source:** `src/backend/postmaster/bgworker.c` (PostgreSQL 18.3)
- **Port crate:** `crates/backend-postmaster-bgworker/src/lib.rs`
- **Owned seam crate:** `crates/backend-postmaster-bgworker-seams`
- **c2rust reference:** `../pgrust/c2rust-runs/backend-postmaster-bgworker/src/bgworker.rs`
- **Branch:** `port/backend-postmaster-bgworker`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Top-line verdict: **PASS** (independent from-scratch re-audit; one new seam-wiring FAIL found and fixed this round)

This is an independent re-derivation from the C, c2rust, and port sources — not a
re-read of the prior report. Results:

- The three previously-failing findings (LWLock guard discarded, `ascii_safe_strlcpy`
  divergence, NO_OOM survive-OOM dropped) are **confirmed resolved** by
  re-derivation against the C; details under "Previously-failing findings".
- The prior audit's seam section declared seam installation "clean" after
  inspecting only the two installed seams. Re-enumerating the owned seam crate
  surfaced a **new merge-blocking finding**: `backend-postmaster-bgworker-seams`
  declares six seams but `init_seams()` installed only two. Fixed this round
  (one re-audit cycle) by wiring the remaining four; re-verified clean.

The workspace gate (`cargo check --workspace`, `cargo test -p
backend-postmaster-bgworker`) is green after the fix.

## Function inventory and verdicts

Every function definition in `bgworker.c` is listed (statics included). The three
`c2rust_pg_*_barrier` inlines (lines 38-49) and the `dlist_*` inlines are c2rust
transpile substrate, not unit functions — correctly absent from the port.

| # | C function (bgworker.c) | C line | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `BackgroundWorkerShmemSize` | 170 | lib.rs:169 | MATCH | header(16) + max_worker_processes·sizeof(slot); header literal 16 matches c2rust `size = 16`; slot size from field-sum const; max_worker_processes via init-small seam. |
| 2 | `BackgroundWorkerShmemInit` | 186 | lib.rs:180 | MATCH | shmem_init_struct seam; `!IsUnderPostmaster` copies list→slots (in_use=true/terminate=false/pid=InvalidPid/gen=0, rw_shmem_slot set, bgw_notify_pid=0, slot.worker=rw_worker), rest free; else `Assert(found)`. |
| 3 | `FindRegisteredWorkerBySlotNumber` (static) | 245 | lib.rs:237 | MATCH | linear scan by `rw_shmem_slot`, returns `Option<usize>` index (idiomatic for `RegisteredBgWorker*`). |
| 4 | `BackgroundWorkerStateChange` | 270 | lib.rs:248 | MATCH | total_slots mismatch → LOG+return; per-slot in_use skip; find; terminate/notify handling; parallel_terminate_count++; pid=0/in_use=false ordering preserved (signal deferred after store, same observable effect as C kill after in_use=false); NO_OOM try_reserve+LOG+return (Finding 3 fix); paranoid `ascii_safe_strlcpy` copies (Finding 2 fix); notify-PID validity DEBUG1; head insertion via `insert(0,..)`. |
| 5 | `ForgetBackgroundWorker` | 453 | lib.rs:445 | MATCH | parallel→parallel_terminate_count++, in_use=false, DEBUG1 unregister, dlist_delete+pfree → `Vec::remove`. Barrier modeled by Mutex. |
| 6 | `ReportBackgroundWorkerPID` | 485 | lib.rs:487 | MATCH | slot.pid = rw_pid; if notify_pid!=0 → SIGUSR1. |
| 7 | `ReportBackgroundWorkerExit` | 507 | lib.rs:510 | MATCH | slot.pid=rw_pid; Forget if rw_terminate‖restart==NEVER before notify; then SIGUSR1. Ordering preserved (notify_pid latched before Forget). |
| 8 | `BackgroundWorkerStopNotifications` | 538 | lib.rs:548 | MATCH | zero bgw_notify_pid on every entry with notify_pid==pid. |
| 9 | `ForgetUnstartedBackgroundWorkers` | 565 | lib.rs:564 | MATCH | dlist_foreach_modify → index walk; re-examine same index after Forget removes it; zap+notify when slot.pid==InvalidPid && notify_pid!=0. |
| 10 | `ResetBackgroundWorkerCrashTimes` | 603 | lib.rs:608 | MATCH | NEVER_RESTART→Forget (re-examine index); else debug_assert non-parallel, clear rw_crashed_at/rw_pid/notify_pid. |
| 11 | `SanityCheckBackgroundWorker` (static) | 656 | lib.rs:652 | MATCH | All four ereport predicates with ERRCODE_INVALID_PARAMETER_VALUE; restart bound `>USECS_PER_DAY/1000` (i64 promotion matches C int64 constant) and `<0 && !=NEVER`; parallel+restart check; default bgw_type←bgw_name when empty. USECS_PER_DAY=86_400_000_000 verified. Returns Ok(false)/Err per elevel. |
| 12 | `bgworker_die` (static) | 728 | lib.rs:743 | MATCH | block_signals then FATAL ERRCODE_ADMIN_SHUTDOWN with the "terminating background worker …" message. sigaction/longjmp owned externally. |
| 13 | `BackgroundWorkerMain` | 742 | lib.rs:768 | MATCH | NULL startup→FATAL "unable to find bgworker entry"+proc_exit(1); publish MyBgworkerEntry; delete PostmasterContext; B_BG_WORKER; init_ps_display; PostAuthDelay sleep; signal-handler install; sigsetjmp→`?`: on Err HOLD_INTERRUPTS, Unblock, EmitErrorReport, proc_exit(1); body InitProcess/BaseInit/Lookup/entrypt then proc_exit(0). Signal-handler disposition split bundled in `install_bgworker_signal_handlers` seam (see seam audit — defensible). |
| 14 | `BackgroundWorkerInitializeConnection` | 877 | lib.rs:864 | MATCH | init_flags from BYPASS_ALLOWCONN→0x2, BYPASS_ROLELOGINCHECK→0x4; FATAL+PROGRAM_LIMIT_EXCEEDED if no DB-conn flag; init_postgres_by_name seam; Init→Normal mode delegated to postinit owner. |
| 15 | `BackgroundWorkerInitializeConnectionByOid` | 911 | lib.rs:900 | MATCH | same as 14 by OID/role. |
| 16 | `BackgroundWorkerBlockSignals` | 945 | lib.rs:939 | MATCH | block_signals seam. |
| 17 | `BackgroundWorkerUnblockSignals` | 951 | lib.rs:944 | MATCH | unblock_signals seam. |
| 18 | `RegisterBackgroundWorker` | 964 | lib.rs:955 | MATCH | IsUnderPostmaster‖!IsPostmasterEnvironment→preload-tolerate or LOG; data-initialized→ERROR; DEBUG1; Sanity(LOG); notify_pid!=0→LOG; numworkers++ > max→LOG with errdetail_plural+errhint; NO_OOM try_reserve+LOG+return (Finding 3 fix); head insert. `numworkers` modeled thread_local (postmaster-private). |
| 19 | `RegisterDynamicBackgroundWorker` | 1070 | lib.rs:1086 | MATCH | !IsUnderPostmaster→None; Sanity(ERROR); parallel limit `reg-term >= max_parallel_workers` with MAX_PARALLEL_WORKER_LIMIT debug_assert; unused-slot scan, memcpy/pid=InvalidPid/gen++/terminate=false/parallel_register_count++/write-barrier(in_use=true); guard held across critical section, released at C LWLockRelease (Finding 1 fix); SendPostmasterSignal; handle init. |
| 20 | `GetBackgroundWorkerPid` | 1182 | lib.rs:1184 | MATCH | LW_SHARED guard held across slot read, `.expect()` on infallible acquire, released at C point (Finding 1 fix); gen mismatch‖!in_use→pid0; pid0→Stopped, InvalidPid→NotYetStarted, else Started+pid. |
| 21 | `WaitForBackgroundWorkerStartup` | 1237 | lib.rs:1228 | MATCH | CHECK_FOR_INTERRUPTS; GetPid; set pid on Started; break unless NotYetStarted; WaitLatch(WL_LATCH_SET\|WL_POSTMASTER_DEATH,0,BGWORKER_STARTUP); POSTMASTER_DEATH→PostmasterDied; ResetLatch. |
| 22 | `WaitForBackgroundWorkerShutdown` | 1282 | lib.rs:1269 | MATCH | same loop, break on Stopped, BGWORKER_SHUTDOWN wait event. |
| 23 | `TerminateBackgroundWorker` | 1321 | lib.rs:1306 | MATCH | LW_EXCLUSIVE guard held; terminate=true + signal_postmaster only on generation match; release at C point (Finding 1 fix); SendPostmasterSignal if signalled. |
| 24 | `LookupBackgroundWorkerFunction` (static) | 1362 | lib.rs:1345 | MATCH | library "postgres"→search InternalBGWorkers names (5 entries in C order: ParallelWorkerMain, ApplyLauncherMain, ApplyWorkerMain, ParallelApplyWorkerMain, TablesyncWorkerMain — verified against C lines 145-159), unknown→ERROR "internal function … not found"; else None (external load = entrypoint seam). Name decision in-crate; address resolution is the loader's job — acceptable split, not a hollowed SEAMED. |
| 25 | `GetBackgroundWorkerTypeByPid` | 1396 | lib.rs:1375 | MATCH | LW_SHARED guard held across scan (Finding 1 fix); `pid>0 && pid==pid`→bgw_type else None; static buffer→owned String. |

## Previously-failing findings (re-derived independently; all RESOLVED)

**Finding 1 — LWLock RAII guard discarded (was merge-blocking). RESOLVED.**
Re-checked all four lock-using functions against C `LWLockAcquire`/`LWLockRelease`
pairs. Each now binds `let guard = lwlock_acquire_main(...)` and calls
`guard.release()?` (or `let _ = guard.release()` for infallible callers) at the C
release site; the early-return path in `RegisterDynamicBackgroundWorker` drops the
data lock then `guard.release()?` before returning. The seam guard
(`crates/backend-storage-lmgr-lwlock-seams/src/lib.rs:130-143`) genuinely holds:
`release()` consumes self and releases once; `Drop` releases only if not already
released — so the lock is held for the full critical section and released exactly
once. No bare `lwlock_release_main(offset)` calls remain in this crate.

**Finding 2 — `ascii_safe_strlcpy` reimplementation (was DIVERGES). RESOLVED.**
Verified byte-for-byte against `src/backend/utils/adt/ascii.c:174-199`: keep
`32 <= ch <= 127`, keep `\n`/`\r`/`\t`, else `'?'`. The port (lib.rs:1446) now uses
`(32..=127).contains(&ch)` plus the whitespace arm, dropping control bytes to
`'?'`. (Edge case: C returns without writing the NUL when `destsiz == 0`; the port
would write `dst[0]=0`. Unreachable here — `destsiz` is always BGW_MAXLEN/MAXPGPATH,
nonzero constants — so no behavioral divergence.)

**Finding 3 — NO_OOM postmaster survive-OOM dropped (was PARTIAL/MISSING). RESOLVED.**
Both `BackgroundWorkerStateChange` (C:372, MCXT_ALLOC_NO_OOM|ZERO) and
`RegisterBackgroundWorker` (C:1040-ish, MCXT_ALLOC_NO_OOM) now reserve the list
node fallibly via `Vec::try_reserve(1)` and, on failure, `ereport(LOG,
ERRCODE_OUT_OF_MEMORY, "out of memory")` and `return Ok(())` without aborting —
preserving the postmaster's survive-OOM contract.

## New finding this round

**Finding 5 (merge-blocking — design/wiring): owned seam crate only partially installed. FIXED this round.**
`backend-postmaster-bgworker-seams` (the only `X-seams` mapping to bgworker.c, so
the sole owned seam crate) declares **six** seams:
`background_worker_main`, `get_background_worker_pid`, `register_background_worker`,
`register_dynamic_background_worker`, `background_worker_initialize_connection`,
`background_worker_unblock_signals`. The prior `init_seams()` installed only the
first two. The remaining four are real cyclic consumers' entry points — all four are
called by `crates/backend-replication-logical-launcher/src/lib.rs` — so at runtime a
launcher call would hit the panic-stub. Per audit-crate SKILL step 3 ("Every
declaration in every owned seam crate must be installed by the crate's
`init_seams()`"), an uninstalled owned seam is an automatic FAIL.

Fix: wired the four missing seams in `init_seams()` (lib.rs:1476) with thin
marshal+delegate stubs (`register_background_worker_seam`,
`register_dynamic_background_worker_seam`,
`background_worker_initialize_connection_seam`,
`background_worker_unblock_signals_seam`) — each does one call to the in-crate port
function, no branching or computation. Re-verified: `cargo check -p
backend-postmaster-bgworker` and `cargo test -p backend-postmaster-bgworker` (9
passed) green; `cargo check --workspace` green. No `set()` of any of these seams
occurs outside the owner except a `#[cfg(test)]` fixture override in
`backend-storage-ipc-shm-mq` (test-local, allowed). Seam installation now clean.

## Finding 4 (advisory / modeling; carried forward): postmaster acquires the data Mutex

C keeps `BackgroundWorkerData` lock-free for the postmaster by design (the
postmaster cannot take locks). The port models the array as
`Mutex<Option<BackgroundWorkerArray>>` and the postmaster-only functions take that
Mutex. State transitions are identical and the lib doc acknowledges the
shared-synchronized-state modeling choice (AGENTS.md: "the only legitimately
cross-thread state is what C keeps in shared memory — port that as explicitly
shared, synchronized types"). Not a logic divergence; recorded as advisory because
it changes the postmaster's no-lock invariant. Not independently merge-blocking.

## Seam audit

**Owned seam crate:** `backend-postmaster-bgworker-seams` — six declarations, all
now installed by `init_seams()` (lib.rs:1476-1489) via thin marshal+delegate stubs;
`seams-init::init_all()` calls `backend_postmaster_bgworker::init_seams()`
(seams-init/src/lib.rs:33). Clean after Finding 5 fix.

**Outward seam calls** (each a real cross-unit dependency, thin marshal+delegate):
init-small (max_worker_processes, is_under_postmaster, is_postmaster_environment,
my_proc_pid, post_auth_delay, max_parallel_workers, set_my_backend_type,
hold_interrupts), miscinit (process_shared_preload_libraries_in_progress), shmem
(shmem_init_struct), postmaster (signal_child_sigterm/sigusr1,
postmaster_mark_pid_for_worker_notify, delete_postmaster_context), pmsignal
(send_postmaster_signal_bgworker_change), lwlock (acquire/release_main), proc
(init_process), postinit (base_init, init_postgres_by_{name,oid}), ipc (proc_exit),
latch (wait_latch_my_latch, reset_latch_my_latch), tcop (check_for_interrupts),
ps_status (init_ps_display), pqsignal (block/unblock,
install_bgworker_signal_handlers), pgsleep (pg_usleep), fmgr
(call_bgworker_entrypoint). All leaf marshal+delegate; no outward seam carries
branching or node-construction.

Borderline (advisory, not a finding): `install_bgworker_signal_handlers(db_connection)`
bundles the C connection-vs-non-connection `pqsignal` disposition split plus
`InitializeTimeouts`. The branch selects which non-portable handler addresses
(StatementCancelHandler / procsignal_sigusr1_handler / FloatExceptionHandler vs
SIG_IGN) to install — these are owned by other subsystems and are not values
bgworker can name, so delegating the disposition is defensible. The `bgworker_die`
SIGTERM-handler *body* (the FATAL ereport) is ported in-crate, so no logic is
hollowed.

## Conclusion

**PASS.** Findings 1-3 are independently confirmed resolved against the C. Finding 5
(four uninstalled owned seams) was found and fixed this round in a single re-audit
cycle; seam installation is now complete and verified. Finding 4 remains an accepted
advisory. Workspace gate green. The crate may merge.
