# Audit: backend-postmaster-interrupt

Unit: `backend-postmaster-interrupt` (`src/backend/postmaster/interrupt.c`,
109 lines, PostgreSQL 18.3).
Crates audited: `crates/backend-postmaster-interrupt`, plus the new seam
crates it introduced (`backend-storage-ipc-seams`,
`backend-storage-ipc-latch-seams`, `backend-storage-ipc-procsignal-seams`,
`backend-utils-misc-guc-seams`, `backend-utils-mmgr-mcxt-seams`) and the new
types crate (`types-guc`).
Cross-checked against
`../pgrust/c2rust-runs/backend-postmaster-interrupt/src/interrupt.rs`.
Auditor: independent re-derivation from the C sources and headers
(`postmaster/interrupt.h`, `utils/guc.h`, `storage/procsignal.h`,
`utils/memutils.h`, `storage/ipc.h`, `storage/latch.h`, `miscadmin.h`).

## Function inventory (every definition in interrupt.c)

The C file defines exactly four functions and two file-scope globals; the
c2rust rendering (`interrupt.rs`) contains the same four `pub unsafe extern
"C" fn`s and the same two `static mut` flags, confirming nothing else
survived preprocessing. `postmaster/interrupt.h` declares no additional
functions.

| # | C function (interrupt.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ProcessMainLoopInterrupts` (:33) | `lib.rs::ProcessMainLoopInterrupts` | MATCH | All four arms present, in C order: (a) `ProcSignalBarrierPending` → `ProcessProcSignalBarrier()`; (b) `ConfigReloadPending` → clear flag **then** `ProcessConfigFile(PGC_SIGHUP)` (clear-before-call order preserved; PGC_SIGHUP == 2 verified against guc.h enum order); (c) `ShutdownRequestPending` → `proc_exit(0)` (seam declared `-> !`, matching C's no-return); (d) `LogMemoryContextPending` → `ProcessLogMemoryContextInterrupt()`. In C, callees in arms (a), (b), (d) can `ereport(ERROR)` and longjmp out; the port's `?` early-return reproduces the same "skip all later arms" behavior, including leaving `ConfigReloadPending` already cleared when `ProcessConfigFile` fails — identical to C. Reads of the foreign per-backend flags `ProcSignalBarrierPending` (procsignal.c) and `LogMemoryContextPending` (mcxt.c) go through their owners' seam getters. |
| 2 | `SignalHandlerForConfigReload` (:60) | `lib.rs::SignalHandlerForConfigReload` | MATCH | `ConfigReloadPending = true; SetLatch(MyLatch);` — flag set then latch, exactly two statements, no extras. `SetLatch(MyLatch)` delegated via `set_latch_my_latch` (latch.c owns both `SetLatch` and the `MyLatch` resolution). The C `SIGNAL_ARGS` parameter (`int postgres_signal_arg`) is unused in the body (confirmed in C and c2rust) so its omission is behavior-preserving; registration plumbing belongs to the callers/pqsignal. |
| 3 | `SignalHandlerForCrashExit` (:72) | `lib.rs::SignalHandlerForCrashExit` | MATCH | `_exit(2)` — direct `libc::_exit(2)`, declared `-> !`. No proc_exit/atexit callbacks run, matching the C comment's intent exactly; exit code 2 (not 0) preserved. |
| 4 | `SignalHandlerForShutdownRequest` (:103) | `lib.rs::SignalHandlerForShutdownRequest` | MATCH | `ShutdownRequestPending = true; SetLatch(MyLatch);` — mirror of #2 with the other flag. |

## File-scope state

| C global | Port | Verdict | Notes |
|---|---|---|---|
| `volatile sig_atomic_t ConfigReloadPending = false` (:27) | `CONFIG_RELOAD_PENDING: thread_local Cell<bool>` + `ConfigReloadPending()` / `SetConfigReloadPending()` | MATCH | Per-backend (per-process in C) state → thread-local, initialized `false`. Exposed read/write accessors replace the `extern` global; the `volatile sig_atomic_t` semantics (async handler ↔ main loop in the same backend) are modeled per the repo convention for per-backend flags. |
| `volatile sig_atomic_t ShutdownRequestPending = false` (:28) | `SHUTDOWN_REQUEST_PENDING: thread_local Cell<bool>` + accessors | MATCH | Same. |

## Constants verified against headers

- `PGC_SIGHUP == 2`: `utils/guc.h` enum `GucContext` order is
  `PGC_INTERNAL(0), PGC_POSTMASTER(1), PGC_SIGHUP(2), PGC_SU_BACKEND(3),
  PGC_BACKEND(4), PGC_SUSET(5), PGC_USERSET(6)` — `crates/types-guc/src/guc.rs`
  matches all seven, and matches the c2rust constants exactly.
- `_exit(2)` and `proc_exit(0)` exit codes match the C literals.

## Seam audit

This unit declares **no inward seams** (nothing depends back on interrupt.c
through a cycle yet); `backend_postmaster_interrupt::init_seams()` is
correspondingly empty and is invoked by `seams-init::init_all()` (sorted,
one line). Correct.

Outward seam crates introduced (all declaration-only; owners unported, so a
direct cargo dependency cannot exist — each call is one thin delegate, no
marshalling logic, no branching):

| Seam | Owner unit | Used by | Thin? |
|---|---|---|---|
| `backend-storage-ipc-seams::proc_exit(i32) -> !` | ipc.c | fn #1 | yes — single call, literal arg |
| `backend-storage-ipc-latch-seams::set_latch_my_latch()` | latch.c (+ `MyLatch` from globals.c, resolved by the owner at install time) | fns #2, #4 | yes — zero-arg delegate; bundling `MyLatch` resolution into the owner is the correct seam shape for signal-handler callers |
| `backend-storage-ipc-procsignal-seams::proc_signal_barrier_pending() -> bool` / `process_proc_signal_barrier() -> PgResult<()>` | procsignal.c | fn #1 | yes — flag read + delegate; error type mirrors C's PG_TRY/re-throw |
| `backend-utils-misc-guc-seams::process_config_file(GucContext) -> PgResult<()>` | guc-file.l / guc.c | fn #1 | yes |
| `backend-utils-mmgr-mcxt-seams::log_memory_context_pending() -> bool` / `process_log_memory_context_interrupt() -> PgResult<()>` | mcxt.c (interrupt-logging surface excluded from `crates/mcx` by design — see `docs/mctx-design.md`) | fn #1 | yes |

No `set()` calls exist anywhere in the workspace for these seams outside the
crate's own `#[cfg(test)]` mocks (verified by grep). No logic lives in any
seam path.

## Tests / build

`cargo build --workspace` clean; `cargo test -p backend-postmaster-interrupt`
10/10 pass, covering flag round-trips, both handler bodies (flag + latch
order), each main-loop arm in isolation, arm ordering when all fire, the
clear-before-reload ordering, and `proc_exit(0)` short-circuiting the
memory-context arm.

## Spot-check of MATCH verdicts

Re-derived #1 statement-by-statement against both the C (interrupt.c:33-51)
and the c2rust rendering (interrupt.rs:36-53): four `if`s, no `else`
chaining (an arm can run after a previous arm ran), flag cleared before
`ProcessConfigFile`, `proc_exit(0)` before the memctx check. Re-derived #3
against interrupt.rs:64-69 (`_exit(2)`). Port agrees on every point.

## Verdict

**PASS** — all 4 functions MATCH (with the three foreign calls per function
SEAMED per the rules above), both globals modeled faithfully, constants
header-verified, seams thin and correctly wired, zero findings. Fix rounds:
0.
