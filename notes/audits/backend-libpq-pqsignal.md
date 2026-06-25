# Audit: backend-libpq-pqsignal (+ bundled interfaces-libpq-legacy-pqsignal)

- Catalog unit: `backend-libpq-pqsignal` — `src/backend/libpq/pqsignal.c`
  (pqinitmask only; `src/port/pqsignal.c` with `pqsignal_be`/`pqsignal_fe` is a
  *different* unit, port-batch2x, and is correctly absent here). The catalog
  row bundles `src/interfaces/libpq/legacy-pqsignal.c` (no other catalog row),
  ported as `crates/interfaces-libpq-legacy-pqsignal`.
- C sources: postgres-18.3 `src/backend/libpq/pqsignal.c`,
  `src/interfaces/libpq/legacy-pqsignal.c`
- c2rust reference: `c2rust-runs/backend-libpq-pqsignal/src/pqsignal.rs`.
  No c2rust run exists for `legacy-pqsignal.c` (it was never a standalone
  c2rust unit); that file was audited directly against the C.
- Ports: `crates/backend-libpq-pqsignal/src/lib.rs`,
  `crates/interfaces-libpq-legacy-pqsignal/src/lib.rs`
- Audit date: 2026-06-12

## Function inventory

`pqsignal.c` defines exactly one function (`pqinitmask`) plus three global
`sigset_t` variables. `legacy-pqsignal.c` defines exactly one function
(`pqsignal`). The c2rust rendering of the backend file contains `pqinitmask`,
the three globals, and `__sigbits` — a Darwin *header* inline expanded by the
preprocessor, not unit logic (the port's `libc::sigdelset` covers it). No other
statics, helpers, or build-config `#if` branches.

| C definition (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `pqinitmask` (pqsignal.c:41) | `backend-libpq-pqsignal/src/lib.rs::pqinitmask` + `SignalMasks::new` | MATCH | Detail below. |
| globals `UnBlockSig`, `BlockSig`, `StartupBlockSig` (pqsignal.c:22) | `SignalMasks` fields + `RwLock` global snapshot (`global_masks`/`signal_masks`) | MATCH | Per-process globals set once at startup in C; owned value + snapshot is behavior-preserving. |
| `pqsignal` (legacy-pqsignal.c:41) | `interfaces-libpq-legacy-pqsignal/src/lib.rs::pqsignal` | MATCH | Detail below. |

## Detail: pqinitmask

C logic: `sigemptyset(&UnBlockSig)`; `sigfillset(&BlockSig)`;
`sigfillset(&StartupBlockSig)`; then `sigdelset` of SIGTRAP, SIGABRT, SIGILL,
SIGFPE, SIGSEGV, SIGBUS, SIGSYS, SIGCONT from **both** BlockSig and
StartupBlockSig; then `sigdelset` of SIGQUIT, SIGTERM, SIGALRM from
StartupBlockSig **only**. Every `#ifdef SIG*` guard is satisfied on the
platforms this tree builds for (all eleven signals exist on Darwin and Linux).

Port: `NEVER_BLOCK_SIGNALS = [SIGTRAP, SIGABRT, SIGILL, SIGFPE, SIGSEGV,
SIGBUS, SIGSYS, SIGCONT]` deleted from both filled sets;
`STARTUP_UNBLOCKED_SIGNALS = [SIGQUIT, SIGTERM, SIGALRM]` deleted from
`startup_block_sig` only; `unblock_sig` from `sigemptyset`. Identical set
operations (order is irrelevant for set deletion, and matches anyway).

Constants verified against headers, not memory: the c2rust rendering deletes
bits 5, 6, 4, 8, 11, 10, 12, 19 from both sets and 3, 15, 14 from
StartupBlockSig; the macOS SDK `sys/signal.h` confirms SIGTRAP=5, SIGABRT=6,
SIGILL=4, SIGFPE=8, SIGSEGV=11, SIGBUS=10, SIGSYS=12, SIGCONT=19, SIGQUIT=3,
SIGTERM=15, SIGALRM=14 — exactly the named signals. The port uses `libc::SIG*`
constants, so it is also correct on Linux where SIGBUS/SIGSYS/SIGCONT differ
numerically. MATCH.

## Detail: pqsignal (legacy)

C logic (non-WIN32 — the only path in this tree's build config):
`act.sa_handler = func`; `sigemptyset(&act.sa_mask)`; `act.sa_flags = 0`;
`|= SA_RESTART` iff `signo != SIGALRM` (the frozen 9.2 SIGALRM semantics);
`|= SA_NOCLDSTOP` iff `signo == SIGCHLD` (`#ifdef SA_NOCLDSTOP` always
satisfied on Darwin/Linux); `sigaction(signo, &act, &oact) < 0` → return
`SIG_ERR`; else return `oact.sa_handler`.

Port: same flags computation, same predicates, same single `sigaction` call,
`Error` returned exactly when `sigaction` fails, previous handler returned
mapped `SIG_DFL`→`Default`, `SIG_IGN`→`Ignore`, else `Handler(addr)`.
`SigDisposition` is the owned stand-in for `pqsigfunc`; `SIG_ERR` is
return-only in C and the port panics if a caller tries to *install* `Error` —
unrepresentable input in C practice, behavior identical on all real inputs.
Zeroing `act` before filling it (C leaves padding uninitialized) is
behavior-preserving. Tests confirm SIG_ERR on invalid signo and round-trip of
the previous disposition. MATCH.

Spot-check re-derivation: both MATCH details above were re-derived line-by-line
from the C (flags predicates, signal lists, return mapping) and cross-checked
against the c2rust constants where a rendering exists.

## Seam audit

- No outward seam calls: the only external dependency is `libc` (OS calls),
  a direct cargo dependency — correct for a leaf unit. The src-idiomatic
  `pqsignal_sigaction` OS seam was reclaimed as a direct `libc::sigaction`
  call; no logic was left behind in any wire file (checked old src-idiomatic
  seam usage — it was pure marshal/delegate).
- No inward seam declarations: no `crates/<unit>-seams` crates exist, and none
  are needed (no cycle partners call into these units yet).
- Both crates expose an empty `pub fn init_seams()` and are wired into
  `seams-init::init_all()` (one line each, sorted) with matching dependency
  lines in `crates/seams-init/Cargo.toml`. No `set()` calls anywhere — there
  is nothing to set. No findings.

Fix round 1: the initial port omitted the empty `init_seams()` and the
`seams-init` registration required by repo convention ("calls every ported
crate's init_seams()"). Added in this audit and re-verified from scratch:
both `init_seams()` bodies are empty, `init_all()` calls both, and
`cargo test --workspace` is green.

## Verdict

**PASS** — 3/3 inventory rows MATCH; zero seam findings after the wiring fix.
`cargo test --workspace` green (10 tests across the two crates).
