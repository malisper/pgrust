# Audit: backend-storage-lmgr-condition-variable

- C source: `src/backend/storage/lmgr/condition_variable.c` (PostgreSQL 18.3)
- Headers verified: `storage/condition_variable.h`, `storage/proclist.h`,
  `storage/proclist_types.h`, `storage/procnumber.h`,
  `storage/waiteventset.h`, `portability/instr_time.h`
- c2rust rendering: `c2rust-runs/backend-storage-lmgr-condition-variable/src/condition_variable.rs`
- Port: `crates/backend-storage-lmgr-condition-variable/src/lib.rs`, data shape
  in `crates/types-condvar/src/condition_variable.rs`, seam decls in
  `crates/backend-storage-lmgr-condition-variable-seams/src/lib.rs`
- Audit commit base: 430ffbf

The c2rust rendering confirms the audited build config: assertions compiled
out (no `Assert` bodies), `SpinLockAcquire` rendered as
`tas() != 0 ? s_lock(file, line, func) : ;` and `SpinLockRelease` as a
Release store of 0 — exactly what the port's `spin_lock_acquire` Drop guard
does via `s_lock_macro` / `Spinlock::unlock`. `CHECK_FOR_INTERRUPTS()` is
rendered as `if InterruptPending { ProcessInterrupts() }`; the port delegates
the whole macro through `backend-tcop-postgres-seams::check_for_interrupts`
(`PgResult` stands in for the ereport longjmp).

## Function inventory — condition_variable.c (7 functions, all present)

| # | C function (line) | Port | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ConditionVariableInit` (:35) | `ConditionVariableInit` | MATCH | `SpinLockInit` = `s_init_lock` (store 0), `proclist_init` (head=tail=INVALID_PROC_NUMBER). |
| 2 | `ConditionVariablePrepareToSleep` (:56) | `ConditionVariablePrepareToSleep` | MATCH | `pgprocno = MyProcNumber` read first (seam); cancel any prior prepared sleep; record `cv_sleep_target` (thread-local `Cell<*const ConditionVariable>`); push self onto `cv->wakeup` tail under the spinlock guard. Order of operations identical to C. |
| 3 | `ConditionVariableSleep` (:96) | `ConditionVariableSleep` | MATCH | Delegates to `ConditionVariableTimedSleep(cv, -1, wei)`, discards the bool; `PgResult<()>` propagates the ereport-shaped errors that in C longjmp out of WaitLatch/CHECK_FOR_INTERRUPTS. |
| 4 | `ConditionVariableTimedSleep` (:112) | `ConditionVariableTimedSleep` | MATCH | `cv_sleep_target != cv` → prepare + return false (pointer equality via `ptr::eq`). `timeout >= 0`: `INSTR_TIME_SET_CURRENT(start_time)`, `cur_timeout = timeout`, `wait_events = WL_LATCH_SET\|WL_TIMEOUT\|WL_EXIT_ON_PM_DEATH` (1\|8\|32, verified against waiteventset.h); else `WL_LATCH_SET\|WL_EXIT_ON_PM_DEATH`. Loop: WaitLatch(MyLatch) (seam, result ignored as in C, `?` = the C proc_exit/ERROR escape), ResetLatch(MyLatch), spinlock-guarded `!proclist_contains` → `done = true` + re-push-tail, CHECK_FOR_INTERRUPTS (seam, `?`), `cv != cv_sleep_target` → `done = true`, `done` → return false; timeout recompute `timeout - (long) INSTR_TIME_GET_MILLISEC(cur_time)` = `timeout - (ticks as f64 / NS_PER_MS) as i64` (same truncation as the C double→long cast; NS_PER_MS = 1_000_000 verified), `cur_timeout <= 0` → return true. `Assert(timeout >= 0 && timeout <= INT_MAX)` kept as debug_assert (compiled out in audited C build). |
| 5 | `ConditionVariableCancelSleep` (:230) | `ConditionVariableCancelSleep` | MATCH | NULL target → false; under spinlock: contained → delete, else `signaled = true`; clear target after unlock; return signaled. Same order as C. |
| 6 | `ConditionVariableSignal` (:259) | `ConditionVariableSignal` | MATCH | Pop head under spinlock if non-empty; outside the lock `SetLatch(&proc->procLatch)` = `set_proc_latch(procno)` seam (the C PGPROC* and the procno name the same proc; `proclist_pop_head_node` returns `list->head`'s proc in both). |
| 7 | `ConditionVariableBroadcast` (:282) | `ConditionVariableBroadcast` | MATCH | `pgprocno = MyProcNumber` first; cancel own prepared sleep if any; first spinlock section: pop head, and if still non-empty push self as sentinel (`have_sentinel`); wake first waiter outside the lock; sentinel loop: pop head if non-empty, `have_sentinel = contains(self)`, wake popped proc unless it is self (`proc != MyProc` ⇔ `procno != pgprocno`, MyProc/MyProcNumber name the same backend). `Assert(!proclist_contains(...))` kept as debug_assert under the lock, as in C. |

## proclist.h inline helpers (specialized to `cvWaitLink`, in-crate)

Same pattern as the audited-and-merged lwlock crate (`lwWaitLink` helpers);
each `PGPROC.cvWaitLink` node is read/written through
`backend-storage-lmgr-proc-seams` exactly as the C
`proclist_node_get(procno, offsetof(PGPROC, cvWaitLink))` does.

| C helper | Port | Verdict | Notes |
|---|---|---|---|
| `proclist_init` | `proclist_init` | MATCH | head = tail = INVALID_PROC_NUMBER (-1, verified procnumber.h). |
| `proclist_is_empty` | `proclist_is_empty` | MATCH | `head == INVALID_PROC_NUMBER`. |
| `proclist_push_tail_offset` | `proclist_push_tail` | MATCH | Empty: prev=next=INVALID, head=tail=procno; else: prev=old tail, old tail's next=procno, next=INVALID, tail=procno. Node written back through the seam after both fields are set (no aliasing hazard: C asserts head≠procno, tail≠procno). `Assert(node->next==0 && node->prev==0)` kept as debug_assert; the remaining C Asserts are debug-only sanity checks, compiled out in the audited build (c2rust confirms no assert bodies). |
| `proclist_delete_offset` | `proclist_delete` | MATCH | prev==INVALID → head=next else prev.next=next; next==INVALID → tail=prev else next.prev=prev; node zeroed (0,0 = "not in any list"). |
| `proclist_contains_offset` | `proclist_contains` | MATCH | `prev==0 && next==0` → false, else true. Release behavior identical; the O(1) head/tail debug_asserts are stated in the converse direction from C's (head==procno → prev==INVALID vs. C's prev==INVALID → head==procno) — debug-only, compiled out in the audited build, both vacuous on well-formed lists. Noted, not a behavioral divergence. |
| `proclist_pop_head_node_offset` | `proclist_pop_head_node` | MATCH | Returns `list->head`'s procno (C returns the PGPROC*; callers only use it for `SetLatch(&proc->procLatch)` / `proc != MyProc`, both procno-equivalent). |

`proclist_push_head_offset`, `proclist_node_get`, and
`proclist_foreach_modify` have no caller in condition_variable.c and are not
ported here (proclist.h is header-only; each owner ports the helpers it
uses — lwlock precedent).

## Data shape (`types-condvar`)

- `ConditionVariable { mutex: Spinlock, wakeup: proclist_head }` — faithful to
  the C `{ slock_t; proclist_head; }`; `Spinlock` is `#[repr(transparent)]`
  over `AtomicI32` (int-width slock_t), moved into `types-storage` verbatim
  from the audited s-lock crate (which now re-exports it; no logic change —
  diff is a pure move).
- `CV_MINIMAL_SIZE` = 16 (sizeof CV = 12 ≤ 16, matches the C conditional) and
  `ConditionVariableMinimallyPadded` `#[repr(align(16))]` with a compile-time
  size assert == CV_MINIMAL_SIZE — reproduces the union's size/placement.
- `cv_sleep_target` is a per-backend C static → thread-local `Cell` (rule:
  per-backend globals must not be shared statics — satisfied).
- `wakeup_mut` `&` → `&mut` cast under the spinlock: identical to the merged
  lwlock `waiters_mut` precedent; mutation only inside `spin_lock_acquire`
  guards (plus `ConditionVariableInit`, pre-publication, per the C contract).
- `instr_time::subtract` / `get_millisec` / `NS_PER_MS` added to types-core:
  verified against instr_time.h (`x.ticks -= y.ticks`;
  `(double) ticks / NS_PER_MS`; 1_000_000).

## Seam audit

Outward (all marshal + delegate, one call, no logic in any seam path):

- `backend-storage-lmgr-proc-seams::proc_cv_wait_link` /
  `set_proc_cv_wait_link` / `set_proc_latch` (new decls): PGPROC array owner
  (proc.c) unported; mirrors the lwlock `proc_lw_wait_link` precedent.
- `backend-storage-ipc-latch-seams::wait_latch_my_latch` /
  `reset_latch_my_latch` (new decls): latch.c owner unported; `MyLatch`
  resolved by the owner at install time (matches existing
  `set_latch_my_latch` precedent in the same seams crate).
- `backend-tcop-postgres-seams::check_for_interrupts`: tcop cycle, existing decl.
- `backend-utils-init-small-seams::my_proc_number`: globals.c owner, existing decl.
- Direct deps (no seam needed): `backend-storage-lmgr-s-lock` (SpinLock
  acquire/release behind a Drop guard — lock never held across `?`; the two
  fallible calls in TimedSleep are outside the guard scope),
  `portability-instr-time`, `types-condvar`, `types-storage`, `types-core`.

Owner seams: `backend-storage-lmgr-condition-variable-seams` declares exactly
`condition_variable_timed_sleep`, `condition_variable_cancel_sleep`,
`condition_variable_broadcast` (signatures match the C prototypes in
condition_variable.h). `init_seams()` contains nothing but the three `set()`
calls, installs every declaration, and `seams-init::init_all()` calls it.
No `set()` outside the owner anywhere in `crates/` (procsignal's test-only
fakes excepted, which is the established pattern).

Design conformance: no invented opacity (CV shape is the faithful C layout in
a types crate); no allocation in the unit (no Mcx needed); per-backend global
is a thread-local; no ambient-global seams (MyLatch/MyProcNumber resolution
lives with their owners); no registry side tables; no divergence markers
needed.

## Spot-check

`ConditionVariableTimedSleep` and `ConditionVariableBroadcast` re-derived
line-by-line against both the C and the c2rust rendering (including the
sentinel-removal/extra-wakeup semantics and the timeout truncation cast);
`proclist_push_tail`/`proclist_delete` re-derived against
`proclist.h` and the c2rust `*_offset` bodies.

## Tests / build

- `cargo test -p backend-storage-lmgr-condition-variable` — 15/15 pass
  (prepare/sleep/cancel state machine, signal ordering, sentinel broadcast,
  timeout path, re-queue-on-signal path).
- `cargo test -p backend-storage-ipc-procsignal` — pass (consumer of the CV
  seams, fakes updated for the new shape).
- Full workspace builds clean (pre-existing warnings only).

## Verdict

**PASS** — all 7 condition_variable.c functions and all 6 used proclist
helpers MATCH; zero seam findings; zero design-conformance findings.
