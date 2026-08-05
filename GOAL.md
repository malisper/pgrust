# Goal

Build a from-scratch, idiomatic-Rust rewrite of PostgreSQL 18.3 that is
**measurably faster than C Postgres** — not a port that merely matches it,
and not a compatible-but-different database. Same wire protocol, same SQL
semantics, same on-disk format (a C 18.3 binary can boot our data directory
and vice versa), byte-identical error codes and messages — but faster.

## What "faster" means

Every performance claim passes a dual gate on the target hardware
(AWS Graviton 4 / Neoverse V2, the fleet's c8gd instances):

1. **≤ 1.0× C instructions** — instruction count at most that of C Postgres
   built with clang-16 `-O3 -mcpu=neoverse-v2`, and
2. **≤ 1.0× C wall time** — nanoseconds at most C's on the same machine,
   same run.

Both, not either: wall-time wins that ride on instruction bloat are fragile
across microarchitectures, and instruction wins that lose wall time are not
wins. Rust side is pinned rustc, fat LTO, `codegen-units=1`,
`-Ctarget-cpu=neoverse-v2`.

Claims are made at three levels, each with its own evidence:

- **Per-unit** — every hot ported unit gets a microbenchmark against a
  verbatim C lift of the same code, run on the fleet, with an asm diff when
  the numbers disagree. Records live in `docs/benchmarks/`.
- **Composed gates** — end-to-end workloads against real C Postgres in the
  same pod: M1 (`SELECT 1` — full protocol/parse/plan/execute round trip),
  M2 (indexed point selects), M3 (aggregates, sorts, joins).
- **Correctness floor** — a differential corpus replayed against live
  PostgreSQL must stay byte-identical, and C↔pgrust interop on a shared
  data directory (boot, crash recovery, vacuum WAL) must hold.

## How

- Port PostgreSQL unit by unit from the C source, C-exact in semantics,
  tracked in `CATALOG.tsv` (~970 units). Hot query path first; periphery
  last or never.
- Idiomatic Rust with day-one performance architecture: bump arenas for
  no-drop node trees, enum dispatch instead of function pointers,
  resolve-once carriers, thread-per-backend instead of process-per-backend.
  Deviations from C's shape are taken only when they are the *reason* we
  can be faster, and are proven by benchmark.
- Anything unported fails loudly with a named panic — no silent wrong
  answers, ever. The frontier of named panics is the work queue.
- When a unit loses to C, the loss is attributed at the assembly level and
  either fixed or recorded with a named lever. No unexplained regressions
  ride along.

## Status shorthand

First official M1 verdict: wall time **0.998× — faster than C** — with a
1.22× instruction gap under active attribution. Roughly half the server's
code (by C source lines) is ported; `SELECT 1` through DML, joins,
aggregates, DDL, and VACUUM run end-to-end.
