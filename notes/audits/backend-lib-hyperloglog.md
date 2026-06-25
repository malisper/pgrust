# Audit: backend-lib-hyperloglog

Unit C sources: `src/backend/lib/hyperloglog.c` (+ `src/include/lib/hyperloglog.h`,
`src/include/port/pg_bitutils.h` for the `pg_leftmost_one_pos32` helper).

Audited independently from the C source (PG 18.3) and the c2rust contract; the
port's comments and green build were not trusted. Constants were re-derived from
the C headers, not memory.

## Function inventory and verdicts

| C function (file:line) | Kind | Port location | Verdict | Notes |
|---|---|---|---|---|
| `initHyperLogLog` (hyperloglog.c:67) | extern | `lib.rs hyperLogLogState::init` + `pub fn initHyperLogLog` | MATCH | `bwidth<4 || bwidth>16` -> `elog(ERROR, "bit width must be between 4 and 16 inclusive")?` (same text). `nRegisters = 1<<bwidth`; `arrSize = sizeof(uint8)*nRegisters + 1`; `palloc0(arrSize)` -> fallible `mcx::vec_with_capacity_in` + `resize(0)`. alpha switch arms 16/32/64 -> 0.673/0.697/0.709, default `0.7213/(1.0+1.079/nRegisters)`; `alphaMM = alpha*nRegisters*nRegisters`. All values verified vs C lines 54-55,70-108. |
| `initHyperLogLogError` (hyperloglog.c:128) | extern | `pub fn initHyperLogLogError` | MATCH | `bwidth=4`; `while bwidth<16 { m=(Size)1<<bwidth; if 1.04/sqrt(m)<error break; bwidth++ }`; then `initHyperLogLog`. `(Size)1<<bwidth` then to double == `(1usize<<bwidth) as f64`. Loop bound `< 16` and break predicate identical. |
| `freeHyperLogLog` (hyperloglog.c:148) | extern | `pub fn freeHyperLogLog` (+ `McxOwned` drop) | MATCH | C `Assert(hashesArr != NULL); pfree(hashesArr)`. Port consumes the owned bundle; dropping releases the register array's charge (state-before-context drop order). The owned `PgVec` is always present for an initialized state, so the assert is structurally guaranteed. |
| `addHyperLogLog` (hyperloglog.c:160) | extern | `hyperLogLogState::addHyperLogLog` + `pub fn addHyperLogLog` | MATCH | `index = hash >> (BITS_PER_BYTE*sizeof(uint32) - registerWidth)` (8*4 - rw). `count = rho(hash << registerWidth, 8*4 - registerWidth)`. `hashesArr[index] = Max(count, hashesArr[index])`. `BITS_PER_BYTE=8`, `sizeof(uint32)=4` verified; `Max` -> `max_u8`. |
| `estimateHyperLogLog` (hyperloglog.c:175) | extern | `hyperLogLogState::estimateHyperLogLog` + `pub fn estimateHyperLogLog` | MATCH | Sum over `i in 0..nRegisters` of `1.0/pow(2.0, hashesArr[i])` (slice `[..nRegisters]`, NOT arrSize). `result = alphaMM/sum`. Branch 1: `result <= (5.0/2.0)*nRegisters` -> count zeros over the same range, if `zero_count!=0` then `result = nRegisters * log(nRegisters/zero_count)`. Branch 2 (else if): `result > (1.0/30.0)*POW_2_32` -> `result = NEG_POW_2_32 * log(1.0 - result/POW_2_32)`. `pow`->`powf`, C `log`(natural)->`.ln()`. POW_2_32=4294967296.0, NEG=-4294967296.0 verified vs C:54-55. Branch order and else-if structure preserved. |
| `rho` (hyperloglog.c:240, static inline) | static | `fn rho` | MATCH | `if x==0 return b+1; j = 32 - pg_leftmost_one_pos32(x); if j>b return b+1; return j`. The C `uint8 j = 1` initializer is dead (overwritten before use); port omits the dead init, behavior identical. |
| `pg_leftmost_one_pos32` (pg_bitutils.h:41) | extern inline (helper) | `fn pg_leftmost_one_pos32` | MATCH | C `Assert(word != 0); return 31 - __builtin_clz(word)`. Port `debug_assert!(word!=0); 31 - word.leading_zeros()`. `leading_zeros() == __builtin_clz` for nonzero u32. `rho` guarantees `x != 0` before the call. |

Every function defined in the unit's C sources has a row; none are MISSING,
PARTIAL, or DIVERGES.

## Constants re-derived from C (not memory)

- `POW_2_32 = 4294967296.0`, `NEG_POW_2_32 = -4294967296.0` (hyperloglog.c:54-55) — match.
- alpha arms `0.673 / 0.697 / 0.709`, default `0.7213 / (1.0 + 1.079 / nRegisters)` (lines 91-101) — match.
- range gate `< 4 || > 16`, error-loop bound `< 16`, error formula `1.04 / sqrt(m)` (lines 70, 132, 136) — match.
- `(5.0/2.0)` small-range threshold, `(1.0/30.0)*POW_2_32` large-range threshold (lines 200, 215) — match.
- `BITS_PER_BYTE=8` (c.h), `sizeof(uint32)=4`, `arrSize = nRegisters + 1` (line 75) — match.
- elog message text "bit width must be between 4 and 16 inclusive" (line 71) — match verbatim.

## Seam / wiring audit

Owned seam crate: `crates/backend-lib-hyperloglog-seams` (maps to this unit's
sole C file `hyperloglog.c`). It declares four seams; all four are installed by
`init_seams()` and nothing else:

- `init_hyper_log_log(bwidth:u8)->usize`, `add_hyper_log_log(usize,u32)`,
  `estimate_hyper_log_log(usize)->f64`, `free_hyper_log_log(usize)` — all
  `set()` in `crate::registry::init_seams`, called from `crate::init_seams`.
- `seams-init::init_all()` calls `backend_lib_hyperloglog::init_seams()`
  (verified; `seams-init` recurrence-guard tests pass: every declared seam
  installed by its owner + owner wired into init_all).

No outward seam calls (a pure leaf owns no inward dependencies; this crate only
*installs* its own seams for `nodeAgg`'s spill consumer). The seam bodies are
thin: each resolves the opaque `usize` handle to a real owned `HyperLogLog`
through the per-thread registry and makes exactly one call into ported logic —
no branching, node construction, or computation in the seam path. The function
*bodies* (the actual HLL math) live in this crate, not behind a seam, so this is
ownership, not MISSING-by-seam.

Opacity: the `hyperLogLogState *` that `nodeAgg` already held as an opaque
`usize` word resolves here to the **real** `hyperLogLogState` struct (opacity
inherited, not introduced). The handle registry is the mandated mechanism for
crossing a pointer the consumer modeled as `usize` across the seam, not an
invented side table — it is keyed by, and only by, the handle words the seam
contract defines.

## Design conformance

- Allocation (`palloc0`) goes through `Mcx` + `PgResult` (`vec_with_capacity_in`);
  OOM surfaces a `PgError` exactly where C's `palloc0` would `ereport`. PASS.
- `elog(ERROR)` -> `elog(ERROR, ...)?` early return with matching text. PASS.
- No shared statics for per-backend state: the handle registry is `thread_local`
  (a `MemoryContext` is `!Send`/`!Sync`, matching PG's process-per-backend
  model; `hyperLogLogState *` never crosses threads in C). No ambient-global
  seam, no lock held across `?`. PASS.
- `#![forbid(unsafe_code)]`; raw `uint8 *hashesArr` -> owned context-charged
  `PgVec<u8>` bundled via `mcx::McxOwned` (no raw pointer, no invented handle
  type leaked to callers). PASS.

## Spot-check (auditor self-check)

Re-derived `addHyperLogLog` and `estimateHyperLogLog` bit-for-bit, including the
`hash << registerWidth` / `hash >> (32 - registerWidth)` split, the `Max` update,
the `[..nRegisters]` (not `arrSize`) iteration bound, and the else-if ordering of
the small-/large-range corrections. Re-derived the error-loop integer-to-double
conversion. All confirmed identical to C.

## Verdict

**PASS.** Every function MATCH; zero seam findings; design rules satisfied.
Gate: `cargo check --workspace` clean; `cargo test -p backend-lib-hyperloglog`
16/16; `cargo test -p seams-init` 2/2 (recurrence guards).
