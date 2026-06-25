# Audit: backend-storage-ipc-pmsignal

- **Unit:** `backend-storage-ipc-pmsignal`
  (C: `src/backend/storage/ipc/{barrier,ipc,ipci,pmsignal,signalfuncs}.c`, PostgreSQL 18.3)
- **Branch:** `port/backend-storage-ipc-pmsignal`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Re-derived from the C sources, the c2rust renderings
(`../pgrust/c2rust-runs/backend-storage-ipc-pmsignal/src/{barrier,pmsignal,signalfuncs}.rs`),
and the Rust port. The port's own commit message / self-review was ignored.

## 1. Function inventory and scope

The catalog row's `c_sources` lists five files. Two are out of scope for this
crate and correctly excluded:

- `ipc.c` (10 functions: proc_exit, proc_exit_prepare, shmem_exit,
  atexit_callback, on_proc_exit, before_shmem_exit, on_shmem_exit,
  cancel_before_shmem_exit, on_exit_reset, check_on_shmem_exit_lists_are_empty)
  — already ported in `backend-storage-ipc-dsm-core`. Confirmed `on_shmem_exit`
  is consumed here through `backend_storage_ipc_seams::on_shmem_exit`.
- `ipci.c` (6 functions: RequestAddinShmemSpace, CalculateShmemSize,
  AttachSharedMemoryStructs, CreateSharedMemoryAndSemaphores,
  CreateOrAttachShmemStructs, InitializeShmemGUCs) — its own unit
  `backend-storage-ipc-ipci` (owns `backend-storage-ipc-ipci-seams`). Not this
  crate's obligation.

The three in-scope files define **26** functions total; every one gets a row.
c2rust renders all 26 — no `#if`-gated functions outside the build config. The
build uses `USE_POSTMASTER_DEATH_SIGNAL` (verified: c2rust pmsignal.rs has
`postmaster_possibly_dead` + the death-pipe `read` path), so the port's
platform assumptions are correct, and the WIN32 arm of
`PostmasterIsAliveInternal` is legitimately absent.

### barrier.c (8 functions + 1 static inline)

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `BarrierInit` | barrier.c:100 | barrier.rs:51 | MATCH | SpinLockInit + field init + ConditionVariableInit inlined (SpinLockInit on cv.mutex + proclist_init), exact. |
| `BarrierArriveAndWait` | barrier.c:125 | barrier.rs:77 | MATCH | spinlock region, election logic, prepare/sleep/cancel CV loop, `phase==start||next` assert, `elected != next_phase` re-election — all 1:1. CV `Result`s discarded (C is `void`; interrupt exits via longjmp inside the CV impl). |
| `BarrierArriveAndDetach` | barrier.c:203 | barrier.rs:142 | MATCH | delegates `BarrierDetachImpl(_, true)`. |
| `BarrierArriveAndDetachExceptLast` | barrier.c:213 | barrier.rs:152 | MATCH | participants>1 early-return false; else assert==1, ++phase, true. |
| `BarrierAttach` | barrier.c:236 | barrier.rs:173 | MATCH | assert !static_party, ++participants, return phase. |
| `BarrierDetach` | barrier.c:256 | barrier.rs:191 | MATCH | delegates `BarrierDetachImpl(_, false)`. |
| `BarrierPhase` | barrier.c:265 | barrier.rs:204 | MATCH | lock-free phase read (takes `&Barrier`). |
| `BarrierParticipants` | barrier.c:281 | barrier.rs:214 | MATCH | spinlocked participants read. |
| `BarrierDetachImpl` (static inline) | barrier.c:300 | barrier.rs:232 | MATCH | `(arrive || participants>0) && arrived==participants` release predicate, ++phase, `last = participants==0`, broadcast outside lock — exact. |

### pmsignal.c (12 functions, incl. 1 static handler + 1 static callback)

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `postmaster_death_handler` (static) | pmsignal.c:99 | pmsignal.rs:471 | MATCH | sets `postmaster_possibly_dead=true`; installed by `PostmasterDeathSignalInit` via `pqsignal`. |
| `PMSignalShmemSize` | pmsignal.c:129 | pmsignal.rs:181 | MATCH | `offsetof(PMSignalData,PMChildFlags)` = (NUM_PMSIGNALS+2)*sizeof(sig_atomic_t) header + mul_size/add_size over MaxLivePostmasterChildren(); checked-arith ereports preserved. |
| `PMSignalShmemInit` | pmsignal.c:144 | pmsignal.rs:211 | MATCH | `!found` arm: zero-init, publish num_child_flags=MaxLivePostmasterChildren(). OnceLock get_or_init models shmem find-or-create; ShmemInitStruct OOM ereport has no host analogue (documented). |
| `SendPostmasterSignal` | pmsignal.c:164 | pmsignal.rs:239 | MATCH | !IsUnderPostmaster early return; set flag; kill(PostmasterPid, SIGUSR1). |
| `CheckPostmasterSignal` | pmsignal.c:181 | pmsignal.rs:269 | MATCH | test-then-clear, no clear if unset. |
| `SetQuitSignalReason` | pmsignal.c:201 | pmsignal.rs:289 | MATCH | store reason. |
| `GetQuitSignalReason` | pmsignal.c:212 | pmsignal.rs:305 | MATCH | signal-handler-paranoid: !IsUnderPostmaster or unattached state → PMQUIT_NOT_SENT; unknown value decodes to NOT_SENT. |
| `MarkPostmasterChildSlotAssigned` | pmsignal.c:229 | pmsignal.rs:329 | MATCH | assert bounds, slot--, `!= UNUSED → elog(FATAL)`, set ASSIGNED. FATAL→PgResult Err. |
| `MarkPostmasterChildSlotUnassigned` | pmsignal.c:248 | pmsignal.rs:355 | MATCH | result = (==ASSIGNED), set UNUSED, no state assert (may be called twice). |
| `IsPostmasterChildWalSender` | pmsignal.c:270 | pmsignal.rs:372 | MATCH | == WALSENDER. |
| `RegisterPostmasterChildActive` | pmsignal.c:289 | pmsignal.rs:391 | MATCH | MyPMChildSlot, asserts ASSIGNED, set ACTIVE, on_shmem_exit(MarkPostmasterChildInactive,0) via seam. |
| `MarkPostmasterChildWalSender` | pmsignal.c:308 | pmsignal.rs:419 | MATCH | asserts ACTIVE, set WALSENDER. `Assert(am_walsender)` retained at callers (walsender-local flag) — documented. |
| `MarkPostmasterChildInactive` (static) | pmsignal.c:325 | pmsignal.rs:446 | MATCH | asserts ACTIVE||WALSENDER, set ASSIGNED; pub so on_shmem_exit re-enters. |
| `PostmasterIsAliveInternal` | pmsignal.c:345 | pmsignal.rs:512 | MATCH | reset flag, death-watch read via postmaster seam returning (rc,errno); EAGAIN/EWOULDBLOCK→true; else re-arm flag + FATAL on rc<0 / rc>0 (panic mirrors longjmp), return false. WIN32 arm out of build config. |
| `PostmasterDeathSignalInit` | pmsignal.c:407 | pmsignal.rs:561 | MATCH | pqsignal(handler), request_parent_death_signal seam (prctl/procctl ERROR→Err), seed possibly_dead=true. |

Note: `PostmasterIsAlive()` (the `pmsignal.h` inline fast path) is ported at
pmsignal.rs:485 and installed into the `postmaster_is_alive` seam — header
inline, correctly placed with the .c logic.

### signalfuncs.c (6 functions)

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `pg_signal_backend` (static) | signalfuncs.c:50 | signalfuncs.rs:72 | MATCH | NULL-proc WARNING+ERROR code; `!OidIsValid(roleId) || superuser_arg` branch; B_AUTOVAC_WORKER→pg_signal_autovacuum_worker priv; else superuser; else has_privs(roleId)||has_privs(pg_signal_backend). HAVE_SETSID → `kill(-pid,sig)`; kill-fail WARNING. SIGNAL_BACKEND_* codes 0-4 verified. |
| `pg_cancel_backend` | signalfuncs.c:135 | signalfuncs.rs:156 | MATCH | SIGINT; NOSUPERUSER/NOAUTOVAC/NOPERMISSION → ERRCODE_INSUFFICIENT_PRIVILEGE with exact errdetail strings; bool == SUCCESS. |
| `pg_wait_until_termination` (static) | signalfuncs.c:168 | signalfuncs.rs:193 | MATCH | waittime=100, do/while modeled as loop+`remainingtime<=0 break`; kill(pid,0)/ESRCH→true else ERROR; CHECK_FOR_INTERRUPTS; WaitLatch(WL_LATCH_SET\|WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH, WAIT_EVENT_BACKEND_TERMINATION); ResetLatch; timeout WARNING errmsg_plural. |
| `pg_terminate_backend` | signalfuncs.c:236 | signalfuncs.rs:273 | MATCH | timeout<0 → ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE; SIGTERM; same priv ladder w/ terminate strings; wait only on SUCCESS && timeout>0. |
| `pg_reload_conf` | signalfuncs.c:287 | signalfuncs.rs:320 | MATCH | kill(PostmasterPid,SIGHUP); fail→WARNING+false. |
| `pg_rotate_logfile` | signalfuncs.c:307 | signalfuncs.rs:343 | MATCH | !Logging_collector→WARNING+false; else SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE)+true. |

## 2. Constants verified

- `PM_CHILD_{UNUSED,ASSIGNED,ACTIVE,WALSENDER}` = 0/1/2/3 (pmsignal.c:66-69). ✓
- `PMSignalReason` 10 variants 0-9, `NUM_PMSIGNALS`=10 (storage/pmsignal.h). ✓
- `QuitSignalReason` PMQUIT_NOT_SENT/FOR_CRASH/FOR_STOP = 0/1/2. ✓
- `SIGNAL_BACKEND_*` = 0..4 (signalfuncs.c:45-49). ✓
- `ROLE_PG_SIGNAL_BACKEND`=4200, `ROLE_PG_SIGNAL_AUTOVACUUM_WORKER`=6392
  (pg_authid.dat). ✓
- `WAIT_EVENT_BACKEND_TERMINATION` = `PG_WAIT_IPC + 3`: IPC class is sorted
  alphabetically — APPEND_READY(0), ARCHIVE_CLEANUP_COMMAND(1),
  ARCHIVE_COMMAND(2), BACKEND_TERMINATION(3). ✓
- `POSTMASTER_DEATH_SIGNAL` = SIGINFO where available else SIGPWR
  (pmsignal.c:113-116). ✓
- Unit-test asserts (`tests.rs`) cross-check PM_CHILD/PMSignalReason/
  SIGNAL_BACKEND values against the headers — all pass.

## 3. Seam audit

Owned seam crates (by C-source coverage): `backend-storage-ipc-barrier-seams`
and `backend-storage-ipc-pmsignal-seams`. (`backend-storage-ipc-ipci-seams`
belongs to the separate `ipci` unit.)

- `barrier-seams`: 8 declarations, all 8 installed by `init_seams()`. ✓
- `pmsignal-seams`: declares 6 — `postmaster_is_alive`,
  `postmaster_death_signal_init`, `set_postmaster_death_watch_cloexec`,
  `send_postmaster_signal_bgworker_change`, `pm_signal_shmem_size`,
  `pm_signal_shmem_init`. `init_seams()` installs 5; it does **not** install
  `set_postmaster_death_watch_cloexec`.

`set_postmaster_death_watch_cloexec` wraps `fcntl(postmaster_alive_fds[…],
F_SETFD, FD_CLOEXEC)` at **miscinit.c:162** — not a function in this unit's
`c_sources`. It was declared into this crate by the earlier `miscinit` port
(commit `936bfcac`, predating this port) and is consumed by miscinit
(`process.rs:70`). Its body is OS-fd/postmaster-layer logic, not pmsignal logic,
so installing it here would import foreign logic — the wrong fix. It is an
orphaned/misfiled inward declaration whose implementer is the not-yet-ported
platform layer; it currently panics if called (faithful seam-and-panic for an
unported callee). This is **not** a logic gap of any function this unit ports
and does not block the audit; it is recorded as a cross-unit wiring observation
for the eventual postmaster/platform port to install. The `recurrence_guard`
test (`every_seam_installing_crate_is_wired_into_init_all`) passes — `init_all`
calls this crate's `init_seams()`.

- `init_seams()` is pure `set()` calls (lib.rs:122-145); no logic. ✓
- `init_all()` calls `backend_storage_ipc_pmsignal::init_seams()`
  (seams-init/src/lib.rs:97). ✓
- Every outward seam is thin marshal+delegate over a real dependency boundary
  (procarray BackendPidGetProc/role, activity pgstat_get_backend_type,
  superuser/acl, miscinit get_user_id, latch WaitLatch/ResetLatch, postgres
  CHECK_FOR_INTERRUPTS, pmchild MaxLivePostmasterChildren, postmaster
  read_postmaster_death_watch/request_parent_death_signal, syslogger
  Logging_collector, init-small PostmasterPid/MyPMChildSlot/IsUnderPostmaster,
  ipc on_shmem_exit, port pqsignal, condition-variable broadcast/sleep). No
  branching/node-construction/computation found in any seam path. ✓

## 3b. Design conformance

- No invented opacity: `Barrier`/`ConditionVariable` are real `types-condvar`
  structs; the slock_t is a real `Spinlock` over `s-lock`. ✓
- `PMSignalState` is a process-global `OnceLock<PMSignalState>` modeling shared
  memory (valid in postmaster + children), with `volatile sig_atomic_t` fields
  as atomics — the C lockless discipline, not a per-backend thread_local; this
  is correct because the C object is genuinely shared shmem. ✓
- Allocating/fallible surfaces (`PMSignalShmemSize/Init`,
  `MarkPostmasterChildSlotAssigned`, signalfuncs) return `PgResult`; FATAL/ERROR
  ereports map to Err (or loud panic where C longjmps out of a `bool`-returning
  fn). ✓
- No locks held across `?`: the barrier spinlock is released before any CV `?`
  call; signalfuncs hold no lock across the seam `?`s. ✓
- No registry-shaped side tables; no unledgered divergence markers. ✓

## 4. Verdict

**PASS.** All 26 in-scope functions MATCH; `ipc.c`/`ipci.c` correctly out of
scope. All owned seam declarations that map to this unit's C are installed;
`init_all` wiring + `recurrence_guard` pass. The single uninstalled
`pmsignal-seams` declaration maps to miscinit.c logic outside this unit and is a
recorded cross-unit observation, not a logic finding. Gate: `cargo check
--workspace` clean (warnings only, unrelated crate); `cargo test --workspace`
exit 0, no failures.

Spot-re-derivation of `BarrierDetachImpl`, `pg_signal_backend`'s privilege
ladder, and the `WAIT_EVENT_BACKEND_TERMINATION` ordinal confirmed against the C
and headers line-by-line.
