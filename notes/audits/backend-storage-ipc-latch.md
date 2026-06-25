# Audit: backend-storage-ipc-latch

- **C source**: `src/backend/storage/ipc/latch.c` (PostgreSQL 18.3, 399 lines)
- **c2rust**: `c2rust-runs/backend-storage-ipc-latch/src/latch.rs`
- **Port**: `crates/backend-storage-ipc-latch/src/lib.rs`
- **Companion crates audited**: `backend-storage-ipc-latch-seams`,
  `backend-storage-ipc-waiteventset-seams` (extended by this branch),
  `types-storage::latch` / `types-storage::waiteventset` (pre-existing),
  `crates/backend-libpq-pqcomm` (one-line call-site update for the widened
  `modify_event` signature)
- **Auditor basis**: independent re-derivation from the C source and headers
  (`storage/latch.h`, `storage/waiteventset.h`, `port.h`, `elog.h`),
  cross-checked function-by-function against the c2rust rendering.

In PG 18, `latch.c` is the thin latch layer; the WaitEventSet machinery lives
in `waiteventset.c` (separate catalog unit, unported). The full inventory of
function definitions in `latch.c` is exactly nine; c2rust kept all nine (its
`c2rust_pg_memory_barrier` is a transpile shim for `pg_memory_barrier()`, not
a source function).

## Function inventory (every definition in latch.c, plus in-crate helpers)

| # | Function | C location | Port location | Verdict | Notes |
|---|----------|-----------|---------------|---------|-------|
| 1 | `InitializeLatchWaitSet` | latch.c:44 | lib.rs `InitializeLatchWaitSet` | MATCH | `Assert(LatchWaitSet == NULL)` -> `debug_assert!(slot.is_none())`. `CreateWaitEventSet(NULL, 2)` -> `WaitEventSet::create(2)` (seam marshals the NULL resowner). `AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL)` -> `add_event(WL_LATCH_SET, PGINVALID_SOCKET, my_latch())`; position asserted `== 0`. `IsUnderPostmaster` branch adds `WL_EXIT_ON_PM_DEATH` at position 1, asserted. `latch_pos` is `PG_USED_FOR_ASSERTS_ONLY` in C; the port keeps the assertions. Error path: an `elog(ERROR)` from `AddWaitEventToSet` propagates as `Err`; the partially built set is freed by the guard's `Drop` where C (NULL resowner) would leak it until exit — strictly tidier, no observable difference. |
| 2 | `InitLatch` | latch.c:72 | lib.rs `InitLatch` | MATCH | `is_set=false; maybe_sleeping=false; owner_pid=MyProcPid; is_shared=false` — same fields, same order, `MyProcPid` via init-small seam. WIN32 `CreateEvent` arm out of build config (matches c2rust, which has no such arm). |
| 3 | `InitSharedLatch` | latch.c:102 | lib.rs `InitSharedLatch` | MATCH | `is_set=false; maybe_sleeping=false; owner_pid=0; is_shared=true`. WIN32 `SECURITY_ATTRIBUTES`/`CreateEvent` arm out of build config. |
| 4 | `OwnLatch` | latch.c:135 | lib.rs `OwnLatch` | MATCH | `Assert(latch->is_shared)` -> debug_assert. Reads `owner_pid` once; nonzero -> `elog(PANIC, "latch already owned by PID %d", owner_pid)` = `Err(PgError::new(PANIC, ...))`, identical message; PANIC = level 23 verified against `types_error::PANIC` and the c2rust `errstart(23, ...)`. Success stores `MyProcPid`. The C `elog(PANIC)` longjmps so `owner_pid = MyProcPid` never runs on the error path; the port returns `Err` before the store — same. |
| 5 | `DisownLatch` | latch.c:153 | lib.rs `DisownLatch` | MATCH | Both Asserts (`is_shared`, `owner_pid == MyProcPid`) as debug_asserts; `owner_pid = 0`. |
| 6 | `WaitLatch` | latch.c:181 | lib.rs `WaitLatch` | MATCH | Assert `!IsUnderPostmaster \|\| wakeEvents & (WL_EXIT_ON_PM_DEATH \| WL_POSTMASTER_DEATH)` preserved. `latch = NULL` when `!(wakeEvents & WL_LATCH_SET)` -> `Option` cleared. `ModifyWaitEvent(LatchWaitSet, 0, WL_LATCH_SET, latch)` then, under postmaster, `ModifyWaitEvent(set, 1, wakeEvents & (WL_EXIT_ON_PM_DEATH\|WL_POSTMASTER_DEATH), NULL)` — same positions (0/1 constants verified), same masks, NULL latch -> `None`. `WaitEventSetWait(set, (wakeEvents & WL_TIMEOUT) ? timeout : -1, &event, 1, wait_event_info)`; `== 0` -> `WL_TIMEOUT`, else `event.events` — identical. A NULL `LatchWaitSet` deref (C crash) is an `expect` panic. `ModifyWaitEvent`'s `elog(ERROR)` propagates as `Err` via `?`, same surface. |
| 7 | `WaitLatchOrSocket` | latch.c:232 | lib.rs `WaitLatchOrSocket` | MATCH | Re-derived line by line. `CreateWaitEventSet(CurrentResourceOwner, 3)` first (as in C, where the call is in the declaration); `WL_TIMEOUT ? Assert(timeout >= 0) : timeout = -1`; conditional adds in C order: latch (`WL_LATCH_SET`), pm-death assert, `WL_POSTMASTER_DEATH && IsUnderPostmaster`, `WL_EXIT_ON_PM_DEATH && IsUnderPostmaster`, `wakeEvents & WL_SOCKET_MASK` with `ev = wakeEvents & WL_SOCKET_MASK` and the real `sock`. Wait with `nevents=1`; `rc == 0` -> `ret \|= WL_TIMEOUT`, else `ret \|= event.events & (WL_LATCH_SET \| WL_POSTMASTER_DEATH \| WL_SOCKET_MASK)`. C's `CurrentResourceOwner` registration exists so an `ereport(ERROR)` between create and the trailing `FreeWaitEventSet` still releases the set; here the guard's `Drop` is that release on every path including `?` — behaviorally identical, and the success path drop == the explicit `FreeWaitEventSet`. Over-subscription (both PM-death flags + latch + socket = 4 > 3) errors inside `AddWaitEventToSet` in both. |
| 8 | `SetLatch` | latch.c:299 | lib.rs `SetLatch` / `set_latch` | MATCH | `pg_memory_barrier()` -> `fence(SeqCst)` (same mapping as the c2rust shim), plus SeqCst field atomics — at least as strong as C's plain accesses bracketed by barriers. Quick exit if `is_set`; `is_set = true`; barrier; return if `!maybe_sleeping`; fetch `owner_pid` once (the C single-fetch comment is exactly why the field is an atomic here); `0` -> return; `== MyProcPid` -> `WakeupMyProc()` seam; else `WakeupOtherProc(owner_pid)` seam. Non-WIN32 arm only, matching build config. Infallible (no error path), as required for signal-handler/critical-section callers. |
| 9 | `ResetLatch` | latch.c:383 | lib.rs `ResetLatch` | MATCH | Both Asserts (`owner_pid == MyProcPid`, `maybe_sleeping == false`) as debug_asserts; `is_set = false`; trailing `pg_memory_barrier()` -> `fence(SeqCst)`. |

### In-crate helpers (no C counterpart; model plumbing)

| Helper | Role | Verdict |
|---|---|---|
| `Latch` struct + `is_set()`/`set_maybe_sleeping()`/`owner_pid()` | `struct Latch` (latch.h:113): `is_set`, `maybe_sleeping`, `is_shared`, `owner_pid` — all four C fields present (WIN32 `event` handle excluded with the WIN32 code); accessors are the field reads/writes waiteventset.c performs on a registered latch. | OK |
| `allocate_latch` / `lookup_latch` / `LATCHES` | The C caller's `Latch` storage declaration (globals.c `LocalLatchData`, `PGPROC.procLatch` in shmem). Process-global, synchronized, append-only — the shared-memory analogue, not a registry-shaped side table onto someone else's object; C never frees a latch, registry never removes. Growth is std allocation standing in for static/shmem storage, not a palloc path, so no `Mcx`. | OK |
| `set_my_latch` / `my_latch` (thread-local) | `Latch *MyLatch` (globals.c, owner unported); per-backend slot held thread-locally here, same documented convention procsignal used for `ProcSignalBarrierPending`/`MyProcSignalSlot`. Not a shared static. | OK |
| `set_latch_my_latch` | Seam-install shim: resolve `MyLatch` at call time, `expect` on NULL (C would crash on the deref), one `SetLatch` call. Thin. | OK |
| `init_seams` | Two `set()` calls, nothing else. | OK |

## Seam audit

- **Outward: `backend-storage-ipc-waiteventset-seams`** — owner
  (`waiteventset.c`, catalog `backend-storage-ipc-wait`) is unported, and the
  dependency is genuinely cyclic (waiteventset.c reads `Latch` fields /
  latch.c drives WaitEventSets), so seam use is justified. This branch widened
  `modify_wait_event` to carry the C `Latch *` argument (previously a
  latchless shape — the reconciliation is *toward* the C signature) and added
  `wakeup_my_proc` / `wakeup_other_proc`, both verified against
  `waiteventset.h` (`WakeupMyProc(void)`, `WakeupOtherProc(int pid)`,
  infallible). The one pre-existing consumer of `modify_event`
  (`pqcomm::pq_check_connection`) was updated to pass `None`, matching its C
  call `ModifyWaitEvent(FeBeWaitSet, ..., NULL)`. All calls are thin
  marshal + delegate via the owned `WaitEventSet` guard (Drop =
  `FreeWaitEventSet`); no logic hides in the seam path.
- **Outward: `backend-utils-init-small-seams`** (`my_proc_pid`,
  `is_under_postmaster`) — pre-existing declarations, same usage as the
  merged sibling ipc ports (procsignal, pqcomm). `SetLatch` must read
  `MyProcPid` from a parameterless signal-handler shape, so an explicit
  parameter is not available there; accepted per established precedent.
- **Owned: `backend-storage-ipc-latch-seams`** — declares
  `set_latch_my_latch()` (signal-handler shape, no parameter possible) and
  `reset_latch(LatchHandle)`. Both pre-existed on main with these exact
  signatures (consumers: interrupt, procsignal, syslogger, pqcomm); both are
  installed by `init_seams()`, which contains only `set()` calls;
  `seams-init::init_all()` calls `backend_storage_ipc_latch::init_seams()`
  (seams-init/src/lib.rs:28). No `set()` outside the owner (test fakes in
  consumer test modules excepted, as everywhere). No seam body computes
  anything beyond `MyLatch` resolution + delegate.

## Constants verified against headers

- `WL_LATCH_SET`=1<<0, `WL_SOCKET_READABLE`=1<<1, `WL_SOCKET_WRITEABLE`=1<<2,
  `WL_TIMEOUT`=1<<3, `WL_POSTMASTER_DEATH`=1<<4, `WL_EXIT_ON_PM_DEATH`=1<<5,
  non-WIN32 `WL_SOCKET_CONNECTED`=`WL_SOCKET_WRITEABLE`,
  `WL_SOCKET_CLOSED`=1<<7, non-WIN32 `WL_SOCKET_ACCEPT`=`WL_SOCKET_READABLE`,
  `WL_SOCKET_MASK` = OR of the five — all match `storage/waiteventset.h`.
- `LatchWaitSetLatchPos`=0, `LatchWaitSetPostmasterDeathPos`=1 (latch.c:41-42).
- `PGINVALID_SOCKET` = -1 (`port.h`, non-WIN32).
- `PANIC` = 23 (`elog.h`; c2rust `errstart(23, ...)` confirms).

## Design conformance

- No invented opacity: `Latch` is the real four-field struct;
  `LatchHandle` (pre-existing in `types-storage`) is the identity token for
  owner-side synchronized storage, the documented analogue of `Latch *` into
  shmem in the threads-as-backends model.
- No allocating seams without `Mcx`: the unit's only allocation is registry
  growth standing in for static/shared-memory storage declarations.
- Per-backend globals (`MyLatch`, `LatchWaitSet`) are thread-locals, not
  shared statics; the shared `LATCHES` registry models genuinely shared
  (cross-process-settable) objects.
- No locks held across `?` without guards: `LATCHES` read/write locks are
  scoped to lookup/grow; the `LATCH_WAIT_SET` `RefCell` borrow across the
  blocking wait is documented and non-reentrant (`SetLatch` does not touch
  it).
- `elog(PANIC)` -> `Err(PgError)` at PANIC, per AGENTS.md convention.

## Build / tests

- `cargo build -p backend-storage-ipc-latch -p backend-storage-ipc-latch-seams`: clean.
- `cargo test -p backend-storage-ipc-latch`: 11 passed.
- Seam consumers (`pqcomm`, `syslogger`, `procsignal`, `interrupt`,
  `seams-init`) `cargo check`: clean.

## Verdict

**PASS** — all 9 latch.c functions `MATCH`; zero seam findings; zero design
findings. Spot-checks re-derived in full detail: `WaitLatchOrSocket` (event
add order, timeout/assert arms, result masking, free-on-error semantics) and
`SetLatch` (barrier placement, early exits, single owner_pid fetch, wakeup
dispatch).
