# Audit: port-pqsignal

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** claude-opus-4-8[1m]
- **Unit:** `port-pqsignal` (catalog row `probe-port-srv-pqsignal`)
- **C source:** `src/port/pqsignal.c` (server build: `FRONTEND` undefined, not `WIN32`)
- **Crate:** `crates/port-pqsignal`; owned seam crate `crates/port-pqsignal-seams`

This audit is independent: the inventory and logic were re-derived from the C
source and the system headers, not from the port's comments or build status.
(`../pgrust/c2rust-runs/` does not exist in this checkout, so the c2rust
rendering was unavailable; the C is simple enough — two functions, no macro
machinery beyond `PG_NSIG` and the `StaticAssertDecl`s — that a faithful
re-derivation from the C plus the headers is conclusive.)

## 1. Function inventory

`src/port/pqsignal.c` defines exactly two functions, plus the compile-time
machinery (`PG_NSIG` macro, four `StaticAssertDecl`s, the `pqsignal_handlers`
static array). The `WIN32`/`FRONTEND` paths are compiled out on this target.

| # | C entity | C loc | Port loc | Verdict | Notes |
|---|----------|-------|----------|---------|-------|
| 1 | `wrapper_handler(SIGNAL_ARGS)` (static) | pqsignal.c:85 | lib.rs:58 `wrapper_handler` | MATCH | see §2.1 |
| 2 | `pqsignal(int, pqsigfunc)` → symbol `pqsignal_be` | pqsignal.c:122 | lib.rs:104 `pqsignal_be` | MATCH | see §2.2 |
| — | `PG_NSIG` macro (`NSIG`/`PG_SIGNAL_COUNT`/64) | pqsignal.c:58 | lib.rs:36 `PG_NSIG` | MATCH | Linux `NSIG`/`_NSIG`=65 → `PG_NSIG=65`; macOS `NSIG`=32 → `PG_NSIG=32` (verified `cc` probe on host: `NSIG=32`). WIN32 `PG_SIGNAL_COUNT` branch and the `=64` wild-guess fallback are not reachable on the build targets. |
| — | 4× `StaticAssertDecl(SIG* < PG_NSIG)` | pqsignal.c:67-70 | lib.rs:42-45 `const _: () = assert!(...)` | MATCH | Same four signals (SIGUSR2/SIGHUP/SIGTERM/SIGALRM), same predicate, evaluated at compile time. |
| — | `static volatile pqsigfunc pqsignal_handlers[PG_NSIG]` | pqsignal.c:72 | lib.rs:51 `PQSIGNAL_HANDLERS: [AtomicUsize; PG_NSIG]` | MATCH | Process-static (not thread_local): dispositions are process-wide and the array must be reachable from async-signal context. Atomic stores model the C "assumed atomic" pointer write; `0` is the "none" sentinel. Correct modeling per memory note "shared statics only for genuinely process-wide state". |

### 2.1 `wrapper_handler` — MATCH

- `save_errno = errno` at entry; `errno = save_errno` at exit. Port reads
  `errno()` first and `set_errno(save_errno)` last. **MATCH**, including the
  subtle detail that the **fork-child branch returns before restoring errno**
  (C: `return;` at line 106, before the line-112 restore; port: `return;` at
  lib.rs:81 before the lib.rs:92 restore). Identical.
- `Assert(postgres_signal_arg > 0)` / `< PG_NSIG` → `debug_assert!` (lib.rs:63-64). MATCH (debug-only in both).
- `#ifndef FRONTEND` block (always present on this server build):
  - `Assert(MyProcPid)` → `debug_assert!(my_proc_pid() != 0)` (lib.rs:70). MATCH.
  - `Assert(MyProcPid != PostmasterPid || !IsUnderPostmaster)` — **not
    reproduced**; `PostmasterPid` has no seam. This is a debug-only assert
    (no-op in release) requiring an unported global; dropping it changes no
    production behavior. Ledgered in the crate doc and CATALOG note. Acceptable
    (debug-assert over an unported global, not absent logic).
  - `if (unlikely(MyProcPid != (int) getpid()))` fork guard: `pqsignal(arg,
    SIG_DFL); raise(arg); return;`. Port (lib.rs:73-82) compares
    `my_proc_pid()` to `getpid()`, reinstalls `SigHandler::Default`, `raise`s,
    returns. MATCH (the C `unlikely` hint has no behavioral effect).
- Dispatch: C `(*pqsignal_handlers[arg])(arg)` unconditionally (no NULL check).
  Port loads the slot, `debug_assert!(slot != 0)`, transmutes to `fn(i32)`,
  calls. MATCH — the wrapper is only ever installed when a concrete handler was
  registered, so the slot is populated; the C makes the same assumption.
- The `MyProcPid` reference is a real cross-unit dependency (`MyProcPid` is
  owned by `backend-utils-init-small`, which installs the `my_proc_pid` seam),
  routed through that owner's seam as a thin getter. Legitimate seam.

### 2.2 `pqsignal_be` — MATCH

- `Assert(signo > 0)` / `< PG_NSIG` → `debug_assert!` (lib.rs:105-106). MATCH.
- `if (func != SIG_IGN && func != SIG_DFL)`: register the concrete handler in
  `pqsignal_handlers[signo]` and substitute `wrapper_handler`. Port's `match
  func`: `Handler(f)` stores `f as usize` then installs `wrapper_handler`;
  `Default→SIG_DFL`, `Ignore→SIG_IGN` go straight to the kernel (lib.rs:110-117).
  MATCH — same partition of the `pqsigfunc` sentinel space; the `SigHandler`
  enum encodes exactly C's `SIG_DFL`/`SIG_IGN`/concrete trichotomy.
- `sigaction` setup: `act.sa_handler = func; sigemptyset(&act.sa_mask);
  act.sa_flags = SA_RESTART; if (signo == SIGCHLD) act.sa_flags |=
  SA_NOCLDSTOP;`. Port zeroes the struct, sets `act.sa_sigaction =
  disposition` (the union member exposed by `libc`; no `SA_SIGINFO`, so this is
  exactly C's `sa_handler` write), `sigemptyset`, `SA_RESTART`, and
  `+SA_NOCLDSTOP` for SIGCHLD (lib.rs:121-128). MATCH. C guards SA_NOCLDSTOP
  with `#ifdef`; the macro is unconditionally defined on Linux/macOS, so the
  port's unconditional add is behavior-identical on the targets.
- `if (sigaction(...) < 0) Assert(false)` → `debug_assert!(false, ...)`
  (lib.rs:129-132). MATCH (a failed `sigaction` is a coding-error assert in
  both; no ereport, so the seam stays infallible — see §3).

## 3. Seam audit

**Owned seam crate (by C-source coverage):** `crates/port-pqsignal-seams` maps
to `src/port/pqsignal.c` = this unit. After the fix below it declares exactly
one seam, `pqsignal`, which mirrors this unit's `pqsignal_be` and is installed
by `port_pqsignal::init_seams()` (lib.rs:165-167). `seams-init/src/lib.rs:77`
calls `port_pqsignal::init_seams()`. `init_seams()` contains only the one
`set()`. **Clean.**

- Seam signature: `pqsignal(signo: i32, func: SigHandler)` returns `void`. The
  C `pqsignal_be` returns void and its only failure is `Assert(false)`, never
  an `ereport`. Infallible signature is correct (matches the failure surface).
- Outward call from `wrapper_handler`: `backend_utils_init_small_seams::
  my_proc_pid` — real cross-unit dependency on a global owned elsewhere, thin
  getter, no logic in the seam path. OK.

### Finding 1 (FIXED this round): three foreign declarations in the owned seam crate

`port-pqsignal-seams` originally declared four seams. Three of them —
`install_bgworker_signal_handlers`, `block_signals`, `unblock_signals` — do
**not** mirror any function in `src/port/pqsignal.c`; they back code owned by
other C files:

- `block_signals`/`unblock_signals` wrap `sigprocmask(SIG_SETMASK,
  &BlockSig/&UnBlockSig, NULL)`. `BlockSig`/`UnBlockSig` are owned by
  `src/backend/libpq/pqsignal.c` (`pqinitmask`), crate
  `backend-libpq-pqsignal` (merged).
- `install_bgworker_signal_handlers` is the `BackgroundWorkerMain`
  signal-handler block (`src/backend/postmaster/bgworker.c`), whose dispatched
  handler addresses (`StatementCancelHandler` etc.) are owned by
  `src/backend/tcop/postgres.c` (crate `backend-tcop-postgres`, unported).

None were installed by `port_pqsignal::init_seams()` (it cannot — the logic is
not pqsignal.c's). An uninstalled declaration in this unit's owned seam crate is
a seam finding (AGENTS.md "Declarations for X's functions live only in
X-seams"; audit-crate §3 "every declaration in every owned seam crate must be
installed by the crate's `init_seams()`").

**Fix (relocate to true owners, no logic moved):**
- Created `crates/backend-libpq-pqsignal-seams` declaring `block_signals` /
  `unblock_signals`; `backend_libpq_pqsignal::init_seams()` now installs them
  with thin `sigprocmask` bodies over its owned `signal_masks().block_sig()` /
  `unblock_sig()`. (Previously these were panic-stubs; the owner is merged and
  owns the masks, so installing the real thin body is strictly better and still
  marshal+delegate.)
- Moved `install_bgworker_signal_handlers(db_connection)` into the existing
  `crates/backend-tcop-postgres-seams` (owner `backend-tcop-postgres`,
  unported) — it correctly stays uninstalled / panic-until-owner-lands.
- `crates/backend-postmaster-bgworker` (the sole consumer) updated to call the
  relocated seams (`backend_libpq_pqsignal_seams::{block,unblock}_signals`,
  `backend_tcop_postgres_seams::install_bgworker_signal_handlers`); its Cargo
  dep on `port-pqsignal-seams` swapped for `backend-libpq-pqsignal-seams`.
  bgworker's ported logic (the `bgworker_die` FATAL body, the
  block/unblock wrappers) is unchanged.

After the fix `port-pqsignal-seams` contains only `pqsignal`, which the unit
installs. `cargo check --workspace` green; `cargo test` green for
`port-pqsignal` (2), `backend-libpq-pqsignal` (9), `backend-postmaster-bgworker`
(2); `seams-init` builds (no duplicate-install).

## 3b. Design conformance

- **No invented opacity** (types.md 6-7): `SigHandler` is a faithful split of
  C's `pqsigfunc` (input half), defined in `types-signal`; concrete handlers
  are real `fn(i32)`. No stand-in handles. OK.
- **Allocation/Mcx/PgResult:** the unit allocates nothing and has no
  error-returning path (sigaction failure is a coding-error assert, not an
  ereport), so no `Mcx`/`PgResult` is owed. OK.
- **Shared statics for per-backend globals:** `PQSIGNAL_HANDLERS` is a process
  static modeling C's process-wide `static volatile` array — genuinely
  process-wide state reachable from async-signal context, not a per-backend
  global. Correct (not a shared-static-for-per-backend violation). The
  per-backend `MyProcPid` is read through its owner's seam, not duplicated here.
- **No ambient-global seams of our own:** this unit declares only `pqsignal`
  (explicit args). The `my_proc_pid` getter it *consumes* is owned/justified by
  `backend-utils-init-small` (pre-existing). OK.
- **No locks across `?`, no registry side tables, no unledgered divergence:**
  the one divergence (dropped `PostmasterPid` debug-assert) is ledgered in the
  crate doc and CATALOG. OK.

## 4. Conclusion

All compile-time machinery and both functions are **MATCH**. The sole seam
finding (three foreign declarations misfiled in this unit's owned seam crate)
was fixed this round by relocating them to their true owners; the owned seam
crate now declares and installs exactly its one function. Zero seam findings
remain. **PASS.** CATALOG row `probe-port-srv-pqsignal` set to `audited`.
