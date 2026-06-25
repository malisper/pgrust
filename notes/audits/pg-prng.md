# Audit: pg-prng

- Unit: `pg-prng` (`src/common/pg_prng.c`)
- C source: `postgres-18.3/src/common/pg_prng.c` (header `src/include/common/pg_prng.h`)
- c2rust: `c2rust-runs/pg-prng/src/pg_prng.rs`
- Port: `crates/pg-prng/src/lib.rs`
- Auditor: independent re-derivation from C + c2rust; constants verified against
  the C source/headers; test vectors re-derived with an independent Python
  implementation of the C algorithm.

## Function inventory and per-function comparison

Every function definition in `pg_prng.c` (including statics/inlines pulled in
from headers) has a row. No `#if` branches exist in `pg_prng.c` other than the
`M_PI` fallback define (a constant, verified below).

| C function (pg_prng.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `rotl` (static inline, L40) | `u64::rotate_left` (used at L134, L136-137) | MATCH | C `(x<<b)\|(x>>(64-b))` identical to `rotate_left` for b in {7, 24, 37}; never called with b=0/64 so no UB-edge divergence. |
| `xoroshiro128ss` (static, L53) | `PgPrng::xoroshiro128ss` (L131) | MATCH | `val = rotl(s0*5,7)*9`; `s0' = rotl(s0,24)^sx^(sx<<16)`; `s1' = rotl(sx,37)`; wrapping mul matches C unsigned overflow. Verified against c2rust and independent reimplementation (3-value vector for seed 42 matches: 0x69e85b3631381baa, 0x3bc32c541d626e1d, 0x3e35de64b3b378d8). |
| `splitmix64` (static, L71) | `splitmix64` (L154) | MATCH | Constants 0x9E3779B97F4A7C15, 0xBF58476D1CE4E5B9, 0x94D049BB133111EB and shifts 30/27/31 verified against C source. State update order (increment first, then extract) matches. |
| `pg_prng_seed` (L88) | `PgPrng::seed` (L47) | MATCH | Two splitmix64 draws from the same evolving seed, then seed-check. `seeded()` ctor is a convenience wrapper over the same path. |
| `pg_prng_fseed` (L101) | `PgPrng::seed_from_f64` (L53) | MATCH | `((2^52 - 1) as f64 * fseed) as i64 as u64`. For the documented [-1.0, 1.0] domain, Rust `as` and C conversion agree exactly; outside it C is UB while Rust saturates (defined-behavior superset, not a divergence). |
| `pg_prng_seed_check` (L113) | `PgPrng::ensure_seeded` (L58) | MATCH | All-zero state replaced with Knuth LCG constants 0x5851F42D4C957F2D / 0x14057B7EF767814F (verified against C L122-123); always returns true. |
| `pg_prng_uint64` (L133) | `PgPrng::next_u64` (L67) | MATCH | Direct delegate. |
| `pg_prng_uint64_range` (L143) | `PgPrng::u64_range` (L71) | MATCH | Same bitmask-rejection: `rshift = 63 - leftmost_one_pos64(range)`; do-while rendered as `loop { ...; if value <= range { break } }` — executes at least once, identical predicate. Empty range (`rmax <= rmin`) yields offset 0 → returns rmin. `leftmost_one_pos64` = `63 - leading_zeros`, matching `pg_bitutils.h`'s `63 - __builtin_clzll`; range >= 1 here so the word!=0 precondition holds. `min.wrapping_add` matches C unsigned wrap (C callers never overflow, but bit-identical anyway). |
| `pg_prng_int64` (L172) | `PgPrng::next_i64` (L89) | MATCH | `as i64` = C cast (two's complement). |
| `pg_prng_int64p` (L181) | `PgPrng::next_nonnegative_i64` (L93) | MATCH | Mask 0x7FFFFFFFFFFFFFFF verified. |
| `pg_prng_int64_range` (L191) | `PgPrng::i64_range` (L97) | MATCH | C computes `uval = (u64)rmin + u64_range(0, (u64)rmax - (u64)rmin)` then carefully wraps u64→i64. Port does `min.wrapping_add(offset as i64)`. Proven bit-identical (two's complement add is sign-agnostic; C's `uval > PG_INT64_MAX` branch is exactly wrapping reinterpretation) and exhaustively spot-checked over 100k random (rmin, offset) pairs. Empty range returns rmin. |
| `pg_prng_uint32` (L226) | `PgPrng::next_u32` (L105) | MATCH | Upper 32 bits, `v >> 32`. |
| `pg_prng_int32` (L242) | `PgPrng::next_i32` (L109) | MATCH | `(v >> 32) as i32` wraps like the C cast. |
| `pg_prng_int32p` (L253) | `PgPrng::next_nonnegative_i32` (L113) | MATCH | `v >> 33`, fits in 31 bits, always nonnegative. |
| `pg_prng_double` (L267) | `PgPrng::next_f64` (L117) | MATCH | C `ldexp((double)(v >> 12), -52)`; port `(v >> 12) as f64 * 2f64.powi(-52)`. v>>12 < 2^52 so the f64 conversion is exact; multiplication by the exact power of two 2^-52 is exact and equals ldexp. Result in [0, 1). |
| `pg_prng_double_normal` (L289) | `PgPrng::normal_f64` (L121) | MATCH | `u1 = 1 - d`, `u2 = 1 - d`, Box-Muller `sqrt(-2 ln u1) * sin(2 PI u2)`; draw order preserved; `core::f64::consts::PI` matches the M_PI literal 3.14159265358979323846 to f64 precision. ln/sqrt/sin lower to the same libm calls as C. |
| `pg_prng_bool` (L312) | `PgPrng::next_bool` (L127) | MATCH | Top bit, `v >> 63 != 0`. |
| global `pg_global_prng_state` (L34) | `GLOBAL_PRNG: Mutex<PgPrng>` + `global_prng()` (L143-148) | MATCH | Zero-initialized like the C global; mutex wrapper is the idiomatic rendering of mutable process-wide state, no behavior change. |

Header note: `pg_prng_strong_seed` in `pg_prng.h` is a macro over
`pg_strong_random` (a different unit, `port/pg_strong_random.c`) — not a
function of this unit; correctly absent.

Cosmetic note (non-finding): `from_raw`/`raw` and `from_raw_state`/`raw_state`
are duplicate accessor pairs; harmless API redundancy, no logic involved.

## Seam audit

- No seams: pg-prng is a pure leaf. There is no `pg-prng-seams` crate, no
  `seam-core` dependency, and `Cargo.toml` has zero dependencies — correct.
- `seams-init::init_all()` contains no entry for pg-prng — correct, since the
  crate declares no seams and has nothing to install.
- No outward seam calls anywhere in the crate; nothing to justify.

## Verification

- `cargo test -p pg-prng`: 8/8 pass.
- `cargo clippy -p pg-prng`: clean.
- Test vector for seed 42 independently re-derived from the C algorithm
  (Python reimplementation): matches the port's hardcoded expected values.
- `i64_range` wrap-conversion equivalence with the C two-branch conversion
  verified over 100k randomized cases.

## Verdict

**PASS** — all 17 inventory rows MATCH; zero seam findings.
