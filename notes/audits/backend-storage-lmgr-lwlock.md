# Audit: backend-storage-lmgr-lwlock

- **Unit**: `backend-storage-lmgr-lwlock` — `src/backend/storage/lmgr/lwlock.c` (2053 lines)
- **C source**: `../pgrust/postgres-18.3/src/backend/storage/lmgr/lwlock.c`
- **c2rust**: `../pgrust/c2rust-runs/backend-storage-lmgr-lwlock/src/lwlock.rs`
- **Port**: `crates/backend-storage-lmgr-lwlock/src/lib.rs`
- **Auditor basis**: independent re-derivation from the C source, cross-checked
  against the c2rust rendering and the headers (`storage/lwlock.h`,
  `storage/lwlocklist.h`, `storage/proclist.h`, `storage/proclist_types.h`,
  `storage/procnumber.h`, `utils/wait_classes.h`, `pg_config_manual.h`).

## Build-config note

The c2rust rendering confirms the audited build config: assertions compiled
out (no `Assert` bodies in the rendering), `LWLOCK_STATS` / `LOCK_DEBUG` /
dtrace probes absent, and `pg_write_barrier` rendered as a release fence via
the `C2RUST_TRANSPILE` shim. The port keeps the C `Assert`s as
`debug_assert!`s (extra fidelity relative to the production build) and ports
no `LWLOCK_STATS`/`LOCK_DEBUG` surface, matching the build.

## Constants verified against headers

- `LW_FLAG_HAS_WAITERS`/`RELEASE_OK`/`LOCKED` = 1<<31 / 1<<30 / 1<<29;
  `LW_FLAG_BITS`=3, `LW_FLAG_MASK` formula — match lwlock.c:104-108.
- `MAX_BACKENDS` = 2^18 − 1 (`procnumber.h:38-39`); `LW_VAL_EXCLUSIVE` =
  MAX_BACKENDS+1, `LW_VAL_SHARED`=1, `LW_SHARED_MASK`, `LW_LOCK_MASK` — match.
  All three C `StaticAssertDecl`s reproduced as `const _: ()` asserts.
- `MAX_SIMUL_LWLOCKS` = 200; `NAMEDATALEN` = 64; `INVALID_PROC_NUMBER` = −1;
  `PG_WAIT_LWLOCK` = 0x01000000 (`wait_classes.h:18`);
  `LWLOCK_PADDED_SIZE` = `PG_CACHE_LINE_SIZE` = 128 (size-asserted on
  `LWLockPadded`, `repr(align(128))`).
- `LWLockMode` LW_EXCLUSIVE=0/LW_SHARED=1/LW_WAIT_UNTIL_FREE=2 and
  `LWLockWaitState` LW_WS_NOT_WAITING=0/WAITING=1/PENDING_WAKEUP=2 — match the
  lwlock.h enums.
- `NUM_INDIVIDUAL_LWLOCKS`=54 (lwlocklist.h max id 53 + 1), partition counts
  128/16/16, the four offset constants, `NUM_FIXED_LWLOCKS` — match
  lwlock.h:93-110.
- The `LWTRANCHE_*` enum (types-storage) re-derived against lwlock.h:183-224
  value by value, **including the enum-order subtlety that
  `LWTRANCHE_MULTIXACTMEMBER_SLRU` precedes `LWTRANCHE_MULTIXACTOFFSET_SLRU`**;
  `LWTRANCHE_FIRST_USER_DEFINED` = 95.
- `BUILTIN_TRANCHE_NAMES` (95 entries) checked entry-by-entry against the
  c2rust-materialized designated-initializer array: every name and every NULL
  gap slot (ids 0, 10, 11, 12, 14, 15, 26, 31, 38, 42, 45) is in the right
  position, and the SLRU group names land on the right ids despite the
  declaration-order/enum-order mismatch in the C source.

## Function inventory and verdicts

| # | C function (lwlock.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `c2rust_pg_write_barrier` (:91, transpile shim) | `fence(Ordering::Release)` at use sites | MATCH | Not a Postgres function; the real `pg_write_barrier` is a write barrier — the port's release fences at both use sites (LWLockWakeup, LWLockUpdateVar) are ≥ the C barrier. |
| 2 | `PRINT_LWDEBUG` (:284, LOCK_DEBUG) | — | — | Compiled out of the build (c2rust confirms); correctly not ported. |
| 3 | `LOG_LWDEBUG` (:306, LOCK_DEBUG) | — | — | Compiled out; correctly not ported. |
| 4 | `init_lwlock_stats` (:331, LWLOCK_STATS) | — | — | Compiled out; correctly not ported. |
| 5 | `print_lwlock_stats` (:366, LWLOCK_STATS) | — | — | Compiled out; correctly not ported. |
| 6 | `get_lwlock_stats_entry` (:390, LWLOCK_STATS) | — | — | Compiled out; correctly not ported. |
| 7 | `NumLWLocksForNamedTranches` (:427) | `NumLWLocksForNamedTranches` | MATCH | Sum of `num_lwlocks` over the request array. |
| 8 | `LWLockShmemSize` (:442) | `LWLockShmemSize` | MATCH | Same formula via the shmem `mul_size`/`add_size` seams (overflow `ereport(ERROR)` surfaces as `Err`); `sizeof(NamedLWLockTranche)`/`strlen+1` use the Rust struct/string sizes — sizes are inherently layout-dependent in C too (owned-model note). |
| 9 | `CreateLWLocks` (:472) | `CreateLWLocks` | MATCH (documented owned-model adaptation) | `!IsUnderPostmaster`: size-check, allocate, reset tranche counter to `LWTRANCHE_FIRST_USER_DEFINED`, `InitializeLWLocks` — same sequence. Under-postmaster: C attaches to fork-inherited shmem; with no shmem segment yet the port rebuilds the array and re-derives the placement ranges (IDs sequential from `LWTRANCHE_FIRST_USER_DEFINED` in request order — exactly what `InitializeLWLocks`' `LWLockNewTrancheId` calls produce after the counter reset). Both branches then register every named tranche, as C does from `NamedLWLockTrancheArray`. Documented in the port and CATALOG. |
| 10 | `InitializeLWLocks` (:512) | `InitializeLWLocks` | MATCH | Four init loops with the verified offsets/tranche ids; named-tranche loop assigns `LWLockNewTrancheId()` per request and initializes `num_lwlocks` locks sequentially from `NUM_FIXED_LWLOCKS`; placement metadata is the owned stand-in for the shmem `NamedLWLockTrancheArray` + name copies. |
| 11 | `InitLWLockAccess` (:579) | `InitLWLockAccess` | MATCH | Empty without LWLOCK_STATS (c2rust: `{}`). |
| 12 | `GetNamedLWLockTranche` (:595) | `GetNamedLWLockTranche` | MATCH | First name match wins; base position = `NUM_FIXED_LWLOCKS` + cumulative preceding `num_lwlocks` (precomputed as `range.start`, equal to C's running `lock_pos`); miss → `elog(ERROR, "requested tranche is not registered")`. |
| 13 | `LWLockNewTrancheId` (:625) | `LWLockNewTrancheId` | MATCH | Post-increment of the counter under the `ShmemLock` spinlock seam; counter is a process-wide atomic (shmem stand-in), Relaxed under the spinlock exactly as C's plain int. |
| 14 | `LWLockRegisterTranche` (:650) | `LWLockRegisterTranche` | MATCH | `< LWTRANCHE_FIRST_USER_DEFINED` → no-op return; growth `pg_nextpower2_32(Max(8, tranche_id+1))` == `(index+1).max(8).next_power_of_two()` (both return num itself when already a power of 2); zero-filled growth == `MemoryContextAllocZero`/`repalloc0_array`; stores an owned copy instead of the caller's pointer (drops C's backend-lifetime obligation, strictly safer). |
| 15 | `RequestNamedLWLockTranche` (:692) | `RequestNamedLWLockTranche` | MATCH (after fix round 1) | `!process_shmem_requests_in_progress` → `elog(FATAL)` (returns `Err`, same non-return). **Fix round 1**: the original port clamped the name to `NAMEDATALEN-1` *chars*; C's `strlcpy` truncates at `NAMEDATALEN-1` *bytes*. Fixed to byte-wise truncation (backed off to the nearest UTF-8 boundary). The C `Assert(strlen+1 <= NAMEDATALEN)` is kept as a `debug_assert!`. Vec growth replaces the 16-then-doubling realloc (capacity strategy is not observable). |
| 16 | `LWLockInitialize` (:729) | `LWLockInitialize` | MATCH | `state := LW_FLAG_RELEASE_OK` (plain init store on a `&mut`, == `pg_atomic_init_u32`), tranche stored as uint16, waiters proclist-initialized. LOCK_DEBUG `nwaiters` init compiled out. |
| 17 | `LWLockReportWaitStart` (:747) | `LWLockReportWaitStart` | SEAMED | `pgstat_report_wait_start(PG_WAIT_LWLOCK \| tranche)` via waitevent seam — thin delegate. |
| 18 | `LWLockReportWaitEnd` (:756) | `LWLockReportWaitEnd` | SEAMED | `pgstat_report_wait_end()` via waitevent seam. |
| 19 | `GetLWTrancheName` (:765) | `GetLWTrancheName` | MATCH | Builtin range → table lookup; user range → registered name or `"extension"` (covers both the `>= allocated` and `NULL` cases via `Option`). For the builtin NULL gap slots (never assigned to any lock; C would return a NULL pointer) the port returns `"unknown"` — defensive on an unreachable input. |
| 20 | `GetLWLockIdentifier` (:789) | `GetLWLockIdentifier` | MATCH | `Assert(classId == PG_WAIT_LWLOCK)` as debug_assert; event id is the tranche number. |
| 21 | `LWLockAttemptLock` (:806) | `LWLockAttemptLock` | MATCH | Read-once-then-CAS loop; exclusive: free iff `(state & LW_LOCK_MASK)==0`, `+= LW_VAL_EXCLUSIVE`; shared: free iff `(state & LW_VAL_EXCLUSIVE)==0`, `+= LW_VAL_SHARED`; always swaps (barrier) even when not free; returns `!lock_free` on CAS success. `compare_exchange_weak` inside the unconditional retry loop is behaviorally identical to the C strong CAS (spurious failure just re-loops with the refreshed value). LOCK_DEBUG owner-tracking compiled out. |
| 22 | `LWLockWaitListLock` (:877) | `LWLockWaitListLock` | MATCH (spin-delay timing adaptation) | `fetch_or(LOCKED)` try, then read-only spin while locked, then retry — exact control flow. C's inner delay is `perform_spin_delay`/`finish_spin_delay` from the **unported s_lock.c unit**; the port substitutes `core::hint::spin_loop()` until that unit lands (documented in port + CATALOG). The delay affects only backoff timing and s_lock's adaptive `spins_per_delay` bookkeeping, not any lwlock.c-observable state; a panicking seam here would make the lock unusable under contention. lwlock.c-local logic is complete. |
| 23 | `LWLockWaitListUnlock` (:929) | `LWLockWaitListUnlock` | MATCH | `fetch_and(!LOCKED)` (Release), `Assert(old & LOCKED)` as debug_assert. |
| 24 | `LWLockWakeup` (:942) | `LWLockWakeup` | MATCH | Same collect-under-waitlist-lock walk: skip exclusive waiters once somebody woken; move to local wakeup list; non-`LW_WAIT_UNTIL_FREE` waiter clears `new_release_ok` and sets `wokeup_somebody`; mark `LW_WS_PENDING_WAKEUP`; break after waking an exclusive waiter. CAS loop sets/clears `RELEASE_OK`, clears `HAS_WAITERS` iff list empty (re-checked per iteration, as C), clears `LOCKED`. Wake loop: delete, release fence (== `pg_write_barrier`), `LW_WS_NOT_WAITING`, semaphore unlock — same order. |
| 25 | `LWLockQueueSelf` (:1058) | `LWLockQueueSelf` | MATCH | No-PGPROC and already-waiting `elog(PANIC)` paths (PANIC aborts per the elog port); waitlist lock; `fetch_or(HAS_WAITERS)` under it; set lwWaiting/lwWaitMode; `LW_WAIT_UNTIL_FREE` → push_head else push_tail; unlock. LOCK_DEBUG nwaiters compiled out. |
| 26 | `LWLockDequeueSelf` (:1101) | `LWLockDequeueSelf` | MATCH | `on_waitlist` test under the waitlist lock; conditional delete; clear `HAS_WAITERS` iff list empty and flag set; unlock; then either clear lwWaiting, or (already dequeued by someone else) `fetch_or(RELEASE_OK)` + absorb the scheduled wakeup on the semaphore and repay extra waits — exact branch structure. |
| 27 | `LWLockAcquire` (:1190) | `LWLockAcquire` | MATCH | Room check → `elog(ERROR, "too many LWLocks taken")`; HOLD_INTERRUPTS (seam); attempt / queue / re-attempt / dequeue-on-success / sleep loop with `result=false` after a sleep; `fetch_or(RELEASE_OK)` after waking, before ReportWaitEnd (same order as C); record held lock; repay extraWaits; return result. The C `Assert(!(proc == NULL && IsUnderPostmaster))` is compiled out in the audited build (c2rust confirms). dtrace probes compiled out. |
| 28 | `LWLockConditionalAcquire` (:1361) | `LWLockConditionalAcquire` | MATCH | Room check, HOLD, one attempt; mustwait → RESUME + false; else record + true. |
| 29 | `LWLockAcquireOrWait` (:1418) | `LWLockAcquireOrWait` | MATCH | Twice-in-a-row protocol queuing as `LW_WAIT_UNTIL_FREE`; on sleep, NO `RELEASE_OK` re-set (deliberate C difference from LWLockAcquire — preserved); repay extraWaits, then mustwait → RESUME + false, else record + true. |
| 30 | `LWLockConflictsWithVar` (:1545) | `LWLockConflictsWithVar` | MATCH | `mustwait = state & LW_VAL_EXCLUSIVE`; free → `(false, result=true)`; else read u64 (relaxed, like `pg_atomic_read_u64`), mismatch → store `*newval`, `(false, false)`; match → `(true, false)`. Tuple return == the C out-params. |
| 31 | `LWLockWaitForVar` (:1606) | `LWLockWaitForVar` | MATCH | HOLD; loop: conflict check / queue `LW_WAIT_UNTIL_FREE` / `fetch_or(RELEASE_OK)` / re-check / dequeue-on-no-conflict / sleep with report start+end; repay extraWaits; RESUME; return result. |
| 32 | `LWLockUpdateVar` (:1742) | `LWLockUpdateVar` | MATCH | `swap(SeqCst)` == full-barrier `pg_atomic_exchange_u64`; under waitlist lock, pop only the front-of-queue `LW_WAIT_UNTIL_FREE` waiters (break on first non-UNTIL_FREE), mark PENDING_WAKEUP; unlock; wake loop identical to LWLockWakeup's (fence == pg_write_barrier). `Assert(state & LW_VAL_EXCLUSIVE)` kept as debug_assert. |
| 33 | `LWLockDisownInternal` (:1816) | `LWLockDisownInternal` + `HeldLWLocks::disown` | MATCH | Backwards search (`rposition`), shift-down removal, `elog(ERROR, "lock %s is not held")` with the tranche name on miss. No RESUME (caller's job), as C. |
| 34 | `LWLockReleaseInternal` (:1846) | `LWLockReleaseInternal` | MATCH | `sub_fetch` of EXCLUSIVE/SHARED (fetch_sub + wrapping_sub reproduces the *new* value C names `oldstate`); `Assert(!(new & LW_VAL_EXCLUSIVE))` as debug_assert; `check_waiters` iff `HAS_WAITERS|RELEASE_OK` both set in the new value AND `LW_LOCK_MASK` clear; then LWLockWakeup. dtrace compiled out. |
| 35 | `LWLockDisown` (:1899) | `LWLockDisown` | MATCH | DisownInternal + RESUME_INTERRUPTS. |
| 36 | `LWLockRelease` (:1910) | `LWLockRelease` | MATCH | DisownInternal → ReleaseInternal → RESUME. |
| 37 | `LWLockReleaseDisowned` (:1930) | `LWLockReleaseDisowned` | MATCH | ReleaseInternal only. |
| 38 | `LWLockReleaseClearVar` (:1939) | `LWLockReleaseClearVar` | MATCH | Full-barrier swap then LWLockRelease. |
| 39 | `LWLockReleaseAll` (:1965) | `LWLockReleaseAll` | MATCH | While held: HOLD (to balance Release's RESUME, keeping InterruptHoldoffCount net-unchanged) + release the last-held lock; `release_held_by_addr`'s `disown(last addr)` finds the last entry == C's backwards search hitting index n−1. Safe with zero locks held. |
| 40 | `ForEachLWLockHeldByMe` (:1984) | `ForEachLWLockHeldByMe` | MATCH (closure adaptation) | Iterates held entries in order, invoking the callback with (lock identity, mode); C's `void *context` is subsumed by closure capture. Fixed-size stack snapshot — no allocation, like C. Debug support only. |
| 41 | `LWLockHeldByMe` (:1999) | `LWLockHeldByMe` | MATCH | Any-entry address equality. |
| 42 | `LWLockAnyHeldByMe` (:2017) | `LWLockAnyHeldByMe` | MATCH (slice adaptation) | C's `(lock, nlocks, stride)` window-with-stride-alignment predicate expressed over `&[LWLockPadded]`: a held address satisfies C's `begin <= p < end && (p-begin) % stride == 0` exactly when it equals `&slot.lock` for some slot. Callers with a non-LWLockPadded stride (e.g. buffer descriptors) will need a wrapper when ported; the predicate logic itself is equivalent. Debug support only. |
| 43 | `LWLockHeldByMeInMode` (:2043) | `LWLockHeldByMeInMode` | MATCH | Address + mode equality. |

### proclist.h inline helpers (ported in-crate, as the C inlines are)

Verified line-by-line against `storage/proclist.h` / `proclist_types.h` and
the c2rust `proclist_*_offset` renderings:

| C inline | Port | Verdict | Notes |
|---|---|---|---|
| `proclist_init` | `proclist_init` | MATCH | head = tail = INVALID_PROC_NUMBER. |
| `proclist_is_empty` | `proclist_is_empty` | MATCH | head == INVALID. |
| `proclist_node_get(procno, offsetof(PGPROC,lwWaitLink))` | `proc_s::proc_lw_wait_link` / `set_proc_lw_wait_link` seams | SEAMED | PGPROC belongs to proc.c; read/write-whole-node marshal. |
| `proclist_push_head_offset` | `proclist_push_head` | MATCH | Empty: next=prev=INVALID, head=tail=procno; else: next=old head, old head's prev=procno, prev=INVALID, head=procno. `Assert(node->next==0 && node->prev==0)` kept as debug_assert. |
| `proclist_push_tail_offset` | `proclist_push_tail` | MATCH | Mirror image, verified. |
| `proclist_delete_offset` | `proclist_delete` | MATCH | prev/next relinking with head/tail fixups; node reset to {0,0} after unlink. |
| `proclist_foreach_modify` | `proclist_foreach_modify` | MATCH | `next` cached before the body runs, so the body may delete `cur` (both the waiters walk and the wakeup-drain walk depend on this); `ControlFlow::Break` == C `break`. |

## Seam audit

Owned seam crate **`backend-storage-lmgr-lwlock-seams`** (pre-existing,
declared by backend-utils-activity-small's port): 3 declarations
(`lwlock_initialize`, `lwlock_acquire`, `lwlock_release`), signatures match
the C prototypes (Err = the C error surface). All 3 — and nothing else — are
installed by this crate's `init_seams()`, which contains only `set()` calls;
`seams-init::init_all()` invokes it (crates/seams-init/src/lib.rs:16). No
non-owner `set()` of these seams outside `#[cfg(test)]` fakes (verified by
grep; the only other setters are activity-small's test_seams).

Outward seams, each justified (callee unit unported; direct dep impossible)
and each a thin marshal + delegate with no branching/computation:

- **`backend-storage-ipc-shmem-seams`** (new): `add_size`, `mul_size`
  (→ `PgResult<Size>` carrying shmem.c's overflow ereport), and
  `shmem_lock_acquire`/`shmem_lock_release` (= `SpinLockAcquire/Release(ShmemLock)`;
  ShmemLock is owned by shmem.c). Used by LWLockShmemSize / LWLockNewTrancheId
  exactly where C uses them.
- **`backend-storage-lmgr-proc-seams`** (new): PGPROC field accessors
  (`lwWaiting`, `lwWaitMode`, `lwWaitLink` get/set by procno — the
  `GetPGProcByNumber` idiom) and `pg_semaphore_lock`/`pg_semaphore_unlock`
  (`PGSemaphore` ops on `proc->sem`). Pure field/semaphore marshal.
- **`backend-utils-activity-waitevent-seams`** (new):
  `pgstat_report_wait_start`/`_end`.
- **`backend-utils-init-miscinit-seams`** (new):
  `process_shmem_requests_in_progress`.
- **`backend-utils-init-small-seams`** (extended): `is_under_postmaster`,
  `my_proc_number` (the `MyProc`/`MyProcNumber` globals — `MyProc == NULL` ↔
  `INVALID_PROC_NUMBER`), `hold_interrupts`/`resume_interrupts`
  (HOLD/RESUME_INTERRUPTS).

No seam contains logic; no function body was replaced by a seam call (the
SEAMED rows above are genuine one-line delegations to other units' state).

## Findings and fix rounds

**Round 1 — 1 finding, fixed in this audit:**

1. `RequestNamedLWLockTranche` (DIVERGES → fixed): the port truncated the
   tranche name to `NAMEDATALEN-1` **chars** (`chars().take(63)`), while C's
   `strlcpy(request->tranche_name, tranche_name, NAMEDATALEN)` truncates at
   `NAMEDATALEN-1` **bytes**; a >63-byte multibyte name would have stored more
   bytes than C keeps. Fixed to byte-wise truncation backed off to the nearest
   UTF-8 char boundary (a Rust `String` cannot hold a split sequence; C's
   split-byte tail is not a representable name). Re-audited from scratch
   post-fix: the FATAL guard, the kept `Assert` as `debug_assert!`, the
   truncation, and the append all match.

**Accepted adaptations (documented, no behavioral divergence on the C-parity
surface):**

- `LWLockWaitListLock`: `perform_spin_delay` (unported s_lock.c) replaced by
  `core::hint::spin_loop()` — timing/backoff only (see table row 22).
- `CreateLWLocks` under-postmaster branch rebuilds the owned table instead of
  attaching to fork-inherited shmem (no shmem segment exists yet); tranche
  registration identical (row 9).
- Owned strings replace C's fixed `char[NAMEDATALEN]` / stored `const char *`
  (rows 14, 19); shmem sizing uses Rust layout sizes (row 8).
- `GetLWTrancheName` returns `"unknown"` for the builtin NULL gap slots, an
  input no lock can carry (C would return a NULL `char *`).
- `ForEachLWLockHeldByMe` / `LWLockAnyHeldByMe` signature adaptations
  (rows 40, 42).
- `compare_exchange_weak` inside unconditional CAS retry loops (rows 21, 24).
- The three identical sleep loops are factored into `wait_until_awakened`;
  expansion at each site is exactly the C loop.

## Verdict

**PASS** (after fix round 1). Every compiled-in function is MATCH or SEAMED
per the rules above; the compiled-out LWLOCK_STATS/LOCK_DEBUG surface is
correctly absent; constants verified against the headers; seam wiring clean
(owner-only installation, `init_all` coverage, thin delegations).
`cargo test -p backend-storage-lmgr-lwlock`: 24 passed. Workspace builds.
