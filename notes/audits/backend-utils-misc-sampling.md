# Audit: backend-utils-misc-sampling

- **Unit:** `backend-utils-misc-sampling`
- **C source:** `src/backend/utils/misc/sampling.c` (305 lines, PostgreSQL 18.3)
- **Header:** `src/include/utils/sampling.h` (struct layouts), `src/common/pg_prng.c`
  / `src/include/common/pg_prng.h` (PRNG callee semantics)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-utils-misc-sampling/src/sampling.rs`
- **Port:** `crates/backend-utils-misc-sampling/src/lib.rs`
- **Auditor:** independent re-derivation from the C source; every function
  cross-checked against the c2rust rendering.

## Function inventory (every definition in sampling.c — 10 functions)

| # | C function (sampling.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `BlockSampler_Init` (:38) | `lib.rs::BlockSampler_Init` | MATCH | Field assignments in C order (`N`, `n`, `t=0`, `m=0`), then `sampler_random_init_state(randseed, &randstate)`. `Min(bs->n, bs->N)` mixes `int` with `uint32`; C's usual arithmetic conversions compare in unsigned space (c2rust confirms: `(*bs).n as BlockNumber < (*bs).N`); port does `(bs.n as BlockNumber).min(bs.N)` — identical, including the negative-`samplesize` wraparound case. |
| 2 | `BlockSampler_HasMore` (:57) | `lib.rs::BlockSampler_HasMore` | MATCH | `(t < N) && (m < n)`, same operand types (u32/u32, i32/i32). |
| 3 | `BlockSampler_Next` (:63) | `lib.rs::BlockSampler_Next` | MATCH | `K = N - t` as wrapping u32 sub, `k = n - m` as i32, `Assert(HasMore)` → `debug_assert!`. Take-all branch `(BlockNumber) k >= K` with post-increment `t++` return value modeled exactly. Skip loop: single `V = sampler_random_fract`, `p = 1.0 - k/K`, `while V < p { t++; K--; p *= 1.0 - k/K }`, then `m++`, return `t++`. All float casts (`k as f64 / K as f64`) and wrapping u32 arithmetic match the C/c2rust exactly. |
| 4 | `reservoir_init_selection_state` (:132) | `lib.rs::reservoir_init_selection_state` | MATCH | Seeds from `pg_prng_uint32(&pg_global_prng_state)` → `global_prng(PgPrng::next_u32)` (pg-prng crate holds the shared global state; `next_u32` = upper 32 bits of xoroshiro128**, verified against pg_prng.c:227). `W = exp(-log(fract)/n)` → `(-fract.ln() / n as f64).exp()`. |
| 5 | `reservoir_get_next_S` (:146) | `lib.rs::reservoir_get_next_S` | MATCH | Branch predicate `t <= 22.0 * n` exact. Algorithm X arm: `V` drawn once, `S=0`, `t+=1`, `quot=(t-n)/t`, `while quot > V { S+=1; t+=1; quot *= (t-n)/t }` — identical, `t` is a by-value copy in both. Algorithm Z arm re-derived term by term: `term = t - n + 1`; per-iteration `U`, `X = t*(W-1)`, `S = floor(X)`, `tmp=(t+1)/term`, `lhs = exp(log((U*tmp*tmp*(term+S))/(t+X))/n)`, `rhs = ((t+X)/(term+S))*term/t`; first break sets `W = rhs/lhs`; `y = ((U*(t+1))/term)*(t+S+1)/(t+X)`; `n < S` selects `(denom, numer_lim) = (t, term+S)` else `(t-n+S, t+1)`; descending product loop `for numer = t+S; numer >= numer_lim; numer -= 1 { y *= numer/denom; denom -= 1 }` matches the port's while-loop with end-of-body decrements (c2rust agrees); `W` regenerated in advance; second break on `exp(log(y)/n) <= (t+X)/t`; `rs->W = W` written only in the Z arm, after the loop. Port's `S = s` at each break point is equivalent to C's per-iteration `S = floor(X)` since `S` is only read after exit. |
| 6 | `sampler_random_init_state` (:233) | `lib.rs::sampler_random_init_state` | MATCH | `pg_prng_seed(randstate, (uint64) seed)` → `randstate.seed(seed as u64)`; pg-prng's `seed` is double splitmix64 + zero-state check, verified against pg_prng.c:89. |
| 7 | `sampler_random_fract` (:240) | `lib.rs::sampler_random_fract` | MATCH | do/while rejecting exactly `0.0` → loop with `!= 0.0` return; `pg_prng_double` → `next_f64` (`(v >> 12) as f64 * 2^-52`, verified against pg_prng.c:267 `ldexp((double)(v >> (64-52)), -52)`). |
| 8 | `anl_random_fract` (:265) | `lib.rs::anl_random_fract` | MATCH | First-time init guard (seed `oldrs.randstate` from the global PRNG, set flag) then fract from `oldrs.randstate`. The C static pair `(oldrs, oldrs_initialized)` is modeled as one `Mutex<(ReservoirStateData, bool)>` (`OLD_RESERVOIR_STATE`) with the guard in `with_old_reservoir_state`; predicate and seeding source identical. |
| 9 | `anl_init_selection_state` (:280) | `lib.rs::anl_init_selection_state` | MATCH | Same init guard, then returns `exp(-log(fract(oldrs.randstate))/n)`; identical to C, byte-for-byte the same formula as #4's W computation. |
| 10 | `anl_get_next_S` (:295) | `lib.rs::anl_get_next_S` | MATCH (after fix round 1) | C has **no** `oldrs_initialized` guard: it writes `oldrs.W = *stateptr`, calls `reservoir_get_next_S(&oldrs, t, n)`, writes back `*stateptr = oldrs.W`. Initial port routed this through `with_old_reservoir_state`, which seeds the PRNG and sets the flag — a divergence (see findings). Fixed to lock the state directly with no initialization; now exactly the C body, including spinning in `sampler_random_fract` on an unseeded all-zero state just as C does. |

Struct layouts: `BlockSamplerData {N, n, t, m, randstate}` and
`ReservoirStateData {W, randstate}` carry the same fields/types as
`utils/sampling.h` (BlockNumber=u32, int=i32, pg_prng_state = two u64 words via
`PgPrng`). The `from_parts`/accessor helpers are pure field plumbing with no
logic.

## Seam audit

- The crate declares **no** seams and makes **no** seam calls. Its only
  dependencies (`pg-prng`, `types-core`) are direct crate deps, which is
  correct — no cycle exists.
- `init_seams()` is an empty no-op and is wired into
  `crates/seams-init/src/lib.rs` (`backend_utils_misc_sampling::init_seams()`),
  with the matching Cargo dependency. No `set()` calls outside an owner; no
  uninstalled declarations.
- The C file-static globals `oldrs`/`oldrs_initialized` become a single
  module-private `Mutex`; mutual exclusion is a superset of the single-threaded
  C backend's behavior and introduces no observable difference for any caller.

## Findings and fixes

**Round 1 (FAIL):** `anl_get_next_S` used the shared `with_old_reservoir_state`
helper, which performs first-use seeding of `oldrs.randstate` and sets
`oldrs_initialized`. In C this function has no such guard. Divergent input:
`anl_get_next_S` called before any other `anl_*` function — C runs
`reservoir_get_next_S` on the zero-initialized PRNG state (and
`sampler_random_fract` then loops forever on the all-zero xoroshiro state),
while the port seeded the state, returned a value, and suppressed the later
first-use seeding in `anl_random_fract`/`anl_init_selection_state`. Fixed by
making `anl_get_next_S` lock the state without initializing, matching the C
body exactly. Re-audited from scratch after the fix: MATCH.

## Verdict

**PASS** — all 10 functions MATCH; no seam findings. `cargo test
-p backend-utils-misc-sampling` (6 tests) and the workspace build pass.
