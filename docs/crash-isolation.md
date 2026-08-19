# Crash isolation under thread-per-backend

pgrust runs one OS thread per backend inside a single process, where C
PostgreSQL forks one process per backend. That choice changes the crash
isolation contract, and the difference is deliberate, bounded, and — for one
crash class — strictly weaker than C's. This document states the contract
explicitly so operators and contributors don't have to reverse-engineer it
from `launch_backend`, `postmaster/statemachine.rs`, and `crash_signals.rs`.
(Raised as upstream issue #67.)

## The C baseline

When a C backend dies abnormally, the postmaster — a separate process whose
private memory the backend could never touch — reaps it via `SIGCHLD`,
assumes shared memory *may* be corrupt, `SIGQUIT`s every other child
(`quickdie`: no cleanup, by design), waits for quiescence, discards and
rebuilds shared memory, and restarts. Two properties do the heavy lifting:

1. **The supervisor survives by construction.** No backend fault can touch
   the postmaster's address space.
2. **The blast radius is exactly shared memory.** A crashing backend can
   have torn only shared structures; those are unconditionally rebuilt, and
   sibling *private* memory was never reachable.

## pgrust's two crash classes

### 1. Caught panics (the catchable class) — C-equivalent choreography

Every backend thread body runs under `std::panic::catch_unwind`
(`launch_backend`). A panic — `unwrap()`, assertion, explicit `panic!`,
including every "unported path" named panic — unwinds to the wrapper, which
translates it into a synthetic crash-exit announcement. The postmaster
thread then runs the same ladder C does: `HandleFatalError` marks the
fatal state, `SIGQUIT`s every other backend thread (which observe it at
their quickdie-equivalent points and exit without cleanup), waits for
quiescence, and runs the reinit walk — `shmem_exit(1)`,
`ipci::ResetShmemAfterCrash` (every shared structure back to its boot
image: LWLocks re-armed, transam variables re-seeded, proc arrays and
buffer state reset), `BackgroundWorkerShmemInit` — before re-entering
`PM_STARTUP` with a fresh startup child driving crash recovery from WAL.

Because locking is C-style (no RAII guards), a panicking thread releases
nothing on unwind: LWLocks stay held, half-applied mutations stay torn.
That is the same state C's `quickdie` leaves behind, and the same answer
applies — nothing consults the torn state before the reset walk rebuilds
it. The postmaster thread itself takes no LWLocks during the choreography,
so a crash-held lock cannot deadlock the ladder.

Tested (both in `postmaster/tests/`):

- `crash_restart.rs` — the orchestration: fan-out, quiesce reason,
  reset walk, `PM_STARTUP` re-entry, fresh startup child.
- `crash_restart_torn.rs` — the torn-state arm this document exists for:
  a real panic unwinds a backend thread while it **holds an LWLock
  mid-mutation of shared transam state**; the test proves the unwind
  released nothing (the tear is real), then that the ladder clears both
  the crash-held lock and the torn store, and that the lock is usable
  again after reinit.

**The honest limit of this class**: the choreography assumes the panic's
*cause* was logical, not memory unsafety. Between the panic and sibling
quiesce there is a window where other threads still run against shared
structures the victim may have left mid-mutation — identical in kind to
C's window between a child crash and `SIGQUIT` delivery, but wider in
scope, because here *all* memory is shared-fault-domain, not just shmem.
Structures are designed so that torn-but-unconsulted state is safe (locks
gate readers; the reset walk rebuilds before reuse), but this is an
invariant maintained by discipline, not enforced by the type system.

### 2. Fatal signals (SIGSEGV / SIGBUS / SIGILL / SIGABRT) — process-fatal by design

A genuine memory-safety violation (a bug in `unsafe` code, a wild write, a
stack overflow) raises a fatal signal, and **backends are threads: the
signal kills the whole server**. `crash_signals.rs` emulates C's
"terminated by signal N" log line, restores the previous disposition, and
re-raises. It does not — structurally cannot — run the reinit ladder: the
process that would run it is the process that is dying.

This is the deliberately weaker half of the contract:

- **C**: postmaster survives, resets, restarts children. Self-healing.
- **pgrust**: the server exits. Recovery is a fresh process start —
  **an external supervisor (systemd `Restart=`, a container restart
  policy, an orchestrator) is a required part of a production
  deployment**, not an optional nicety. Durability is unaffected (crash
  recovery replays WAL exactly as after a power cut); availability
  depends on the supervisor's restart latency.

There is an argument this is the *safer* rendering of that crash class:
after UB has demonstrably occurred in a shared address space, resetting
shmem-shaped structures and continuing (as C does after a child SIGSEGV)
trusts that the corruption stopped at a boundary the dying process can no
longer vouch for. pgrust instead abandons the whole address space. The
cost is availability; the benefit is never running on memory a UB event
may have poisoned.

## Operator summary

| event | what happens | what you must provide |
|---|---|---|
| backend panic (assert, unported path, OOM-as-error) | in-process restart: SIGQUIT fan-out, shmem reset, WAL crash recovery | nothing |
| SIGSEGV/SIGBUS/SIGILL/SIGABRT anywhere | log line, whole-process death, core per disposition | an external supervisor that restarts the binary |

## Prior art

The trade-off is the classic multi-threaded-PostgreSQL question; see the
pgconf.eu 2023 session "Multi-threaded PostgreSQL" and the pgsql-hackers
thread `31cc6df9-53fe-3cd9-af5b-ac0d801163f4@iki.fi` (both linked from
issue #67).
