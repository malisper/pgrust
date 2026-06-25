# Audit: backend-utils-misc-timeout (timeout.c)

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Branch: `port/backend-utils-misc-timeout`
- Unit: `backend-utils-misc-more2` (this branch ports only `src/backend/utils/misc/timeout.c`)
- C source: `src/backend/utils/misc/timeout.c` (PostgreSQL 18.3)
- c2rust: `c2rust-runs/backend-utils-misc-more2/src/timeout.rs`
- Port: `crates/backend-utils-misc-timeout/src/lib.rs`

The audit was re-derived from the C and c2rust sources independently. One
merge-blocking seam-wiring defect was found on the first pass (FAIL), fixed on
the branch, and re-audited clean (PASS). All logic verdicts are MATCH.

## 1. Function inventory and verdicts

Enumerated from `timeout.c` (every definition, including file statics). c2rust
kept all of them; no `#if`-gated functions exist in this file under the build
config (the only conditional content is the `MemSet` open-coded loop, which is
behavior-equivalent to zeroing the `itimerval`).

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|------------|-------|-------------------|---------|-------|
| 1 | `find_active_timeout` (static) | 95 | `find_active_timeout` :128 | MATCH | Linear scan over `active_timeouts[0..num]`, compares `.index == id`, returns position or `None` (C `-1`). Port stores slot indices instead of pointers — equivalent for a fixed array. |
| 2 | `insert_timeout` (static) | 113 | `insert_timeout` :138 | MATCH | Range check `index > num` → FATAL (C also checks `< 0`, impossible for `usize`). Sets `active=true`, shifts `[index..num]` up by one, writes slot, `num++`. Shift loop transcribed faithfully. FATAL → `Err(PgError(FATAL))`. |
| 3 | `remove_timeout_index` (static) | 137 | `remove_timeout_index` :161 | MATCH | Range check `index >= num` → FATAL (msg `0..num-1`, port computes `num as isize - 1`, matching C `%d` with `num-1`). Clears `active`, shifts `[index+1..num]` down, `num--`. |
| 4 | `enable_timeout` (static) | 157 | `enable_timeout` :181 | MATCH | Asserts initialized + handler set (debug_assert). If already active, `remove_timeout_index(find_active_timeout(id))`. Insertion-point scan sorts by `fin_time` then by `id < old.index` for ties — identical predicates. Resets indicator, sets start/fin/interval, `insert_timeout`. |
| 5 | `schedule_alarm` (static) | 209 | `schedule_alarm` :223 | MATCH | Guarded by `num>0`. Lost-signal recovery: `signal_pending && now > signal_due_at + 10*1000` clears pending. `nearest = active[0].fin_time`; if `now > nearest` clear pending and force `secs=0,usecs=1`; else `TimestampDifference` (seam) and bump 0/0 → usecs=1. `enable_alarm()` before the early-return check `signal_pending && nearest >= signal_due_at`. Then set `signal_due_at`/`signal_pending`, `setitimer(ITIMER_REAL,...)`; on failure clear pending + FATAL `could not enable SIGALRM timer: %m` (port renders `%m` via `last_os_error()`). All ordering and boundary comparisons (`>` vs `>=`) preserved. |
| 6 | `handle_sig_alarm` (static) | 363 | `handle_sig_alarm` :303 | MATCH | `HOLD_INTERRUPTS` (seam), `SetLatch(MyLatch)` (seam), unconditional `signal_pending=false`. If `alarm_enabled`: `disable_alarm`, then while `num>0 && now >= active[0].fin_time`: snapshot+remove front, set indicator, run handler, if `interval>0` recompute `new_fin_time` (drift guard: intended time, fall back to `now` if a cycle missed), `enable_timeout`, refresh `now`. Then `schedule_alarm(now)`. `RESUME_INTERRUPTS` (seam). The borrow is dropped before each handler call (handler may re-enter the public API, e.g. CheckDeadLock) — faithful to C running the handler outside any lock. |
| 7 | `InitializeTimeouts` | 469 | `InitializeTimeouts` :397 | MATCH | `disable_alarm`, `num=0`, init all `MAX_TIMEOUTS` slots (index=i, flags false, handler None, times 0), `all_timeouts_initialized=true`, `pqsignal(SIGALRM, handle_sig_alarm)` (seam). |
| 8 | `RegisterTimeout` | 504 | `RegisterTimeout` :425 | MATCH | If `id >= USER_TIMEOUT`, scan `[USER_TIMEOUT..MAX_TIMEOUTS)` for a free slot; if none → `ereport(FATAL, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, "cannot add more timeout reasons")` (port `panic!` carrying the PgError, since the seam returns a bare `TimeoutId`). Stores handler, returns id. |
| 9 | `reschedule_timeouts` | 539 | `reschedule_timeouts` :468 | MATCH | Early-return if not initialized; `disable_alarm`; if `num>0` `schedule_alarm(GetCurrentTimestamp())`. |
| 10 | `enable_timeout_after` | 559 | `enable_timeout_after` :488 | MATCH | `disable_alarm`, `now=GetCurrentTimestamp`, `fin=now+delay`, `enable_timeout(..,0)`, `schedule_alarm(now)`. |
| 11 | `enable_timeout_every` | 583 | `enable_timeout_every` :503 | MATCH | `disable_alarm`, now, `enable_timeout(id,now,fin_time,delay)`, `schedule_alarm`. |
| 12 | `enable_timeout_at` | 606 | `enable_timeout_at` :520 | MATCH | `disable_alarm`, now, `enable_timeout(..,0)`, `schedule_alarm`. |
| 13 | `enable_timeouts` | 629 | `enable_timeouts` :532 | MATCH | `disable_alarm`, single `now`; per entry switch on type: AFTER/EVERY compute `fin=now+delay` (EVERY keeps interval=delay), AT uses entry `fin_time`. Default arm in C is `elog(ERROR, "unrecognized timeout type")` — unreachable in the port because `TimeoutType` is a closed 3-variant enum, so the exhaustive `match` is behavior-equivalent (C's `int` switch can't receive any other value either, given a valid enum). `schedule_alarm(now)`. |
| 14 | `disable_timeout` | 684 | `disable_timeout` :567 | MATCH | Asserts; `disable_alarm`; if active remove; clear indicator unless `keep_indicator`; if `num>0` `schedule_alarm(GetCurrentTimestamp())`. |
| 15 | `disable_timeouts` | 717 | `disable_timeouts` :610 | MATCH | Asserts; `disable_alarm`; per entry: if active remove, clear indicator unless its `keep_indicator`; then if `num>0` one `schedule_alarm(GetCurrentTimestamp())`. |
| 16 | `disable_all_timeouts` | 750 | `disable_all_timeouts` :644 | MATCH | `disable_alarm`; `num=0`; clear every slot's `active` and (unless `keep_indicators`) `indicator`. Does not touch the kernel timer (matches the comment). |
| 17 | `get_timeout_active` | 779 | `get_timeout_active` :665 | MATCH | Returns `all_timeouts[id].active`. |
| 18 | `get_timeout_indicator` | 792 | `get_timeout_indicator` :672 | MATCH | If indicator set: reset when `reset_indicator`, return true; else return false (never resets on false — race-safe per comment). |
| 19 | `get_timeout_start_time` | 812 | `get_timeout_start_time` :688 | MATCH | Returns `start_time`. |
| 20 | `get_timeout_finish_time` | 826 | `get_timeout_finish_time` :694 | MATCH | Returns `fin_time`. |

Helper macros `disable_alarm`/`enable_alarm` (C `#define`) → `fn disable_alarm`/
`enable_alarm` (lib.rs :108/:114). `TimestampTzPlusMilliseconds` →
`timestamptz_plus_milliseconds` :29 — `(tz)+((ms)*(int64)1000)`, i64 multiply,
matches header `utils/timestamp.h:85`.

## Constants verified against headers (not from memory)

- `MAX_TIMEOUTS = USER_TIMEOUT + 10 = 23`, `USER_TIMEOUT = 13` —
  `utils/timeout.h`; `types-timeout` defines `USER_TIMEOUT=13` and
  `MAX_TIMEOUTS = USER_TIMEOUT + 10`. c2rust agrees (`MAX_TIMEOUTS=23`).
- `TimeoutId` enum order 0..13 — matches `utils/timeout.h` exactly
  (`types_timeout::TimeoutId` and `types_core::TimeoutId` both transcribe it).
- `ERRCODE_CONFIGURATION_LIMIT_EXCEEDED = "53400"` —
  `src/backend/utils/errcodes.txt:411`; `types-error` `make_sqlstate(b"53400")`;
  c2rust's char-arithmetic decode yields the same.
- `SIGALRM`, `ITIMER_REAL` — via `libc` (platform constants, not transcribed).
- `FATAL`/`ERROR` severities preserved on every `elog`/`ereport` path.

## State / typing notes

- C's `static volatile` per-process state → `thread_local!` (`TIMEOUT_DATA` in a
  `RefCell`; `alarm_enabled`/`signal_pending`/`signal_due_at`/
  `all_timeouts_initialized` as `Cell`). Correct: each PG backend is a process;
  this is per-backend state, not shared. The `alarm_enabled`/`disable_alarm`
  borrow discipline mirrors C's mutual-exclusion contract; handler clears the
  flag before borrowing, so handler and mainline never overlap.
- `active_timeouts` stores slot indices rather than raw pointers — the faithful
  safe equivalent for a fixed-size array; no invented opacity (types.md 6–7
  clean).

## 3 / 3b. Seam audit and design conformance

**Owned seam crates** (by C-file coverage: every `X-seams` whose `X` maps to
`timeout.c`):

1. `backend-utils-misc-timeout-seams` — 9 declarations
   (`enable_timeouts`, `disable_all_timeouts`, `initialize_timeouts`,
   `register_timeout`, `enable_timeout_every`, `disable_timeout`,
   `enable_timeout_after`, `disable_timeouts`, `get_timeout_start_time`).
   **All 9 installed** by `init_seams()`.
2. `backend-utils-misc-more2-seams` — 3 timeout declarations
   (`enable_timeout_after`, `disable_timeout`, `reschedule_timeouts`),
   created earlier by the xact port and **actually consumed** by
   `backend-access-transam-xact` (engine.rs, via the `timeout_seams` alias) and
   `backend-utils-init-postinit`. **All 3 installed** by `init_seams()`.

**Finding F1 (FAIL on first pass — FIXED).** The original port installed only
the 9 declarations of `backend-utils-misc-timeout-seams` and created that crate
as a *parallel duplicate* of the pre-existing `backend-utils-misc-more2-seams`.
The `more2-seams` timeout declarations — the ones the real consumers (xact's
`StartTransaction`/`CommitTransaction`/`AbortTransaction` transaction-timeout
arming and `reschedule_timeouts`, and postinit's statement-timeout) call — were
left **uninstalled**. Since both seam crates map to `timeout.c`, this unit owns
both; an uninstalled declaration in an owned seam crate is an automatic FAIL
(SKILL §3). At runtime every transaction-timeout arm/disarm and the postinit
statement-timeout path would have panicked with "seam not installed".

*Fix:* `init_seams()` now also installs the three `more2-seams` declarations
(`crates/backend-utils-misc-timeout/src/lib.rs`). Because `more2-seams` types
its id as `types_core::TimeoutId` (an identical-discriminant copy of the enum)
and wraps the void `disable_timeout`/`reschedule_timeouts` in `PgResult`, three
thin adapter fns convert the id by index and wrap the infallible results in
`Ok` — marshal-and-delegate only, no branching or computation in the seam path.
`init_seams()` remains nothing but `set()` calls. Crate dependency on
`backend-utils-misc-more2-seams` added to `Cargo.toml`.

**Note N1 (not a blocker; debt belongs to another unit).**
`backend-utils-init-miscinit-seams` declares a stray `initialize_timeouts() ->
PgResult<()>` (for `timeout.c`'s `InitializeTimeouts`). That seam crate maps to
`miscinit.c`, not `timeout.c`, so by the ownership rule it is **not** an owned
seam crate of this unit — it is a mis-placed cross-unit declaration owned by the
miscinit unit. It has zero `call` sites (dead) and is harmless. Left for the
miscinit unit to relocate/install; not gating this audit.

**Outward seams used by the port** — all thin marshal+delegate, each justified
by a real dependency cycle:
- `backend-utils-adt-timestamp-seams::{get_current_timestamp, timestamp_difference}`
- `backend-storage-ipc-latch-seams::set_latch_my_latch`
- `backend-utils-init-small-seams::{hold_interrupts, resume_interrupts}`
- `port-pqsignal-seams::pqsignal`
- `libc::setitimer` (direct syscall; not a PG dependency)

No node construction, branching, or computation lives in any seam path. No
shared statics for per-backend globals; no ambient-global seams; no locks held
across `?`; FATAL/ERROR surfaces carried on `PgResult`/`PgError` where the seam
allows, `panic!`/`.expect()` only where the C is `void` and longjmps on FATAL
(seam-and-panic per "Mirror PG and panic"). Allocation rules N/A (no `Mcx`
allocation in this file).

## Build / test

- `cargo build -p backend-utils-misc-timeout` — clean.
- `cargo test -p backend-utils-misc-timeout` — 7/7 pass (ordering, enable/
  disable single+batched, disable_all, indicator reset, one-shot + periodic
  firing via handler, user-timeout allocation).
- `cargo build -p seams-init -p backend-utils-init-postinit
  -p backend-access-transam-xact` — clean (consumers of the now-installed
  `more2-seams` declarations build).

## Verdict

**PASS.** All 20 functions + helpers MATCH; every declaration in both owned
seam crates is installed by `init_seams()`; zero outstanding seam findings;
design-conformance clean. CATALOG row set to `audited`.
