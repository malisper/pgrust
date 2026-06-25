# Audit: backend-executor-instrument

Unit: `backend-executor-instrument` (src/backend/executor/instrument.c)
Crate: `crates/backend-executor-instrument`
Branch: `port/backend-executor-instrument` (audited at d4fbfab)
C sources: `postgres-18.3/src/backend/executor/instrument.c` (293 lines), plus
the `static inline` helpers/macros it expands from
`src/include/portability/instr_time.h` and the type/flag definitions from
`src/include/executor/instrument.h`.
c2rust cross-check: `c2rust-runs/backend-executor-instrument/src/instrument.rs`.

## Function inventory and per-function verdicts

Every function definition in instrument.c (10 extern, 2 static), plus the
instr_time.h inline/macros the file expands, cross-checked against the c2rust
rendering (which contains exactly the same 13 function symbols, including
`pg_clock_gettime_ns`). No `#if` branches in instrument.c; instr_time.h has a
WIN32 branch (QueryPerformanceCounter) that is outside the build config and
correctly not ported.

| C function (instrument.c) | Port location (crates/backend-executor-instrument/src/lib.rs) | Verdict | Notes |
|---|---|---|---|
| `InstrAlloc` (L30) | `InstrAlloc` (L91) | MATCH | `palloc0(n * sizeof)` -> zeroed `PgVec` in caller's `Mcx`; the option-bits gate `(BUFFERS\|TIMER\|WAL)` and the per-element flag fill (incl. `async_mode`) match. Negative/oversized `n` reproduces palloc's MaxAllocSize gate ("invalid memory alloc request size", `MAX_ALLOC_SIZE = 0x3FFFFFFF` verified against memutils.h). |
| `InstrInit` (L57) | `InstrInit` (L129) | MATCH | memset-to-zero -> `Instrumentation::default()`; sets only the three `need_*` flags, leaves `async_mode` zeroed, exactly as C. |
| `InstrStartNode` (L67) | `InstrStartNode` (L137) | MATCH | `need_timer && !INSTR_TIME_SET_CURRENT_LAZY(starttime)` -> elog(ERROR) "InstrStartNode called twice in a row"; bufusage/walusage snapshots gated identically. Lazy macro semantics (set only if zero, return whether set) verified against instr_time.h L174. |
| `InstrStopNode` (L83) | `InstrStopNode` (L155) | MATCH | `save_tuplecount` captured before `tuplecount += nTuples`; zero-starttime check -> elog(ERROR) "InstrStopNode called without start"; ACCUM_DIFF then SET_ZERO of starttime; bufusage/walusage delta accumulation; first-tuple logic incl. async branch (`async_mode && save_tuplecount < 1.0`) all identical. |
| `InstrUpdateTupleCount` (L131) | `InstrUpdateTupleCount` (L198) | MATCH | `tuplecount += nTuples`. |
| `InstrEndLoop` (L139) | `InstrEndLoop` (L204) | MATCH | early return when `!running`; nonzero-starttime -> elog(ERROR) "InstrEndLoop called on running node"; totals accumulation (startup, total, ntuples, nloops += 1) and the full reset block match. |
| `InstrAggNode` (L168) | `InstrAggNode` (L234) | MATCH | first-tuple merge (both branches, `>` comparison direction), counter ADD, all eight double-field sums, and `need_bufusage`/`need_walusage`-gated Add calls match. |
| `InstrStartParallelQuery` (L199) | `InstrStartParallelQuery` (L265) | MATCH | snapshots `pgBufferUsage`/`pgWalUsage` into the save statics (thread-locals here). |
| `InstrEndParallelQuery` (L207) | `InstrEndParallelQuery` (L271) | MATCH | zeroes both out-params, then AccumDiff(current, save) for each. |
| `InstrAccumParallelQuery` (L217) | `InstrAccumParallelQuery` (L285) | MATCH | adds worker usage into the backend globals via `BufferUsageAdd`/`WalUsageAdd`. |
| `BufferUsageAdd` (static, L225) | `BufferUsageAdd` (private, L291) | MATCH | all 10 int64 counters and all 6 instr_time ADDs, field-by-field against instrument.h. |
| `BufferUsageAccumDiff` (L247) | `BufferUsageAccumDiff` (L311) | MATCH | all 10 `+= add - sub` counters and all 6 ACCUM_DIFFs, field-by-field. |
| `WalUsageAdd` (static, L277) | `WalUsageAdd` (private, L355) | MATCH | 4 fields; `wal_bytes` is `uint64` in C (instrument.h) so the port uses `wrapping_add` — same modular semantics. |
| `WalUsageAccumDiff` (L286) | `WalUsageAccumDiff` (L364) | MATCH | `wal_bytes` uses `wrapping_add(wrapping_sub(..))` matching C unsigned arithmetic; signed fields plain. |
| `pg_clock_gettime_ns` (instr_time.h L110, static inline) | `pg_clock_gettime_ns` (L381) | MATCH | `PG_INSTR_CLOCK` = `CLOCK_MONOTONIC_RAW` on darwin, `CLOCK_MONOTONIC` elsewhere (instr_time.h L101-104); `ticks = tv_sec * NS_PER_S + tv_nsec`; return code ignored as in C. |
| instr_time macros (`IS_ZERO`, `SET_ZERO`, `SET_CURRENT_LAZY`, `ADD`, `ACCUM_DIFF`, `GET_DOUBLE`) | helper fns L402-434 | MATCH | verified one-for-one against instr_time.h L168-189; `NS_PER_S = 1000000000` verified at instr_time.h L77. |

## Types and constants

- `Instrumentation`, `BufferUsage`, `WalUsage`, `instr_time` in
  `types-core/src/instrument.rs` checked field-by-field (names, order, signed
  vs. unsigned: `wal_bytes: u64`) against instrument.h — match.
- Flag bits `INSTRUMENT_TIMER=1<<0`, `INSTRUMENT_BUFFERS=1<<1`,
  `INSTRUMENT_ROWS=1<<2`, `INSTRUMENT_WAL=1<<3`, `INSTRUMENT_ALL=PG_INT32_MAX`
  — verified against instrument.h.
- All three `elog(ERROR, ...)` messages map to `PgError::error` (level ERROR,
  default sqlstate `ERRCODE_INTERNAL_ERROR` = XX000, matching elog semantics).

## Globals

`pgBufferUsage`/`pgWalUsage` (extern) and `save_pgBufferUsage`/`save_pgWalUsage`
(file-static) are per-backend state; ported as thread-locals with public
accessors for the extern pair and module-private cells for the save pair —
faithful to C linkage.

## Seams

The crate declares no seams and installs none; `init_seams()` is empty and is
called by `seams-init::init_all()` (seams-init/src/lib.rs L12). No outward seam
calls exist; all callees are in-crate or in types crates. No findings.

## Build/tests

`cargo test -p backend-executor-instrument`: 18 passed. `cargo check -p
seams-init` clean.

## Verdict

**PASS** — 15/15 rows MATCH, zero seam findings.
