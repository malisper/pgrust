# Audit: common-prng-base64

Independent function-by-function audit against the C sources
(`src/common/base64.c`, `src/common/pg_prng.c`) and the c2rust rendering
(`../pgrust/c2rust-runs/common-prng-base64/src/{base64,pg_prng}.rs`).

Unit composition: `base64.c` ported in-crate (`crates/common-prng-base64/src/base64.rs`);
`pg_prng.c` already ported + audited PASS as the standalone `pg-prng` crate
(`crates/pg-prng/src/lib.rs`) and re-exported here as `prng` so the combined
catalog unit presents one surface. This audit re-derives BOTH from the C.

## base64.c — 4 functions

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `pg_b64_encode` | base64.c:48 | base64.rs:37 | MATCH | Index-based mirror of pointer walk. `buf |= src[s] as u32 << (pos<<3)`; uint8 widen is non-negative so u32-vs-int OR is identical. Overflow guard `p+4 > dstlen` -> `error_encode` (zeroes dst, -1). Tail block `pos!=2` with `(pos==0)?_base64:'='` then `'='`. `Assert((p-dst)<=dstlen)` -> `debug_assert!`. |
| `pg_b64_decode` | base64.c:111 | base64.rs:100 | MATCH | `c = *s++` rendered `src[s] as i8 as i32` (faithful to signed `char`); whitespace early-error; `'='` end-seq logic (pos==2->end=1, pos==3->end=2, else error; `end` set only on first '='); `b64lookup` guarded by `0<c<127`; `buf=(buf<<6)+b` -> `wrapping_add`; per-quad emit gated by `end==0||end>1` / `>2`; trailing `pos!=0` error. All overflow checks present. |
| `pg_b64_enc_len` | base64.c:201 | base64.rs:205 | MATCH | `(srclen+2)/3*4`. |
| `pg_b64_dec_len` | base64.c:213 | base64.rs:213 | MATCH | `(srclen*3)>>2`. |

Static tables `_base64[64]` and `b64lookup[128]` transcribed byte-for-byte
against the C source (verified row-by-row). `error:` label -> `error_encode` /
`error_decode` (`dst[..dstlen].fill(0); -1`), matching `memset(dst,0,dstlen)`.

## pg_prng.c — 17 functions + 2 static helpers (in pg-prng crate)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `rotl` | pg_prng.c:41 | rotate_left intrinsic | MATCH | `(x<<bits)|(x>>(64-bits))` == `u64::rotate_left`. |
| `xoroshiro128ss` | pg_prng.c:54 | lib.rs:123 | MATCH | `rotl(s0*5,7)*9`; s0/s1 update identical (rotl 24/37, `sx<<16`). wrapping arithmetic. |
| `splitmix64` | pg_prng.c:72 | lib.rs:151 | MATCH | consts 0x9E3779B97f4A7C15 / 0xBF58476D1CE4E5B9 / 0x94D049BB133111EB; shifts 30/27/31. |
| `pg_prng_seed` | pg_prng.c:89 | lib.rs:39 | MATCH | two splitmix64 + seed_check. |
| `pg_prng_fseed` | pg_prng.c:102 | lib.rs:45 | MATCH | `((1<<52)-1) as f64 * fseed) as i64` -> seed as u64. |
| `pg_prng_seed_check` | pg_prng.c:114 | lib.rs:50 (`ensure_seeded`) | MATCH | fallback 0x5851F42D4C957F2D / 0x14057B7EF767814F, returns true. |
| `pg_prng_uint64` | pg_prng.c:134 | lib.rs:59 | MATCH | |
| `pg_prng_uint64_range` | pg_prng.c:144 | lib.rs:63 | MATCH | rshift=63-pg_leftmost_one_pos64(range) (`leftmost_one_pos64`=63-leading_zeros); bitmask-reject loop `val>range`; empty->0; `rmin+val` wrapping. |
| `pg_prng_int64` | pg_prng.c:173 | lib.rs:81 | MATCH | |
| `pg_prng_int64p` | pg_prng.c:182 | lib.rs:85 | MATCH | mask 0x7FFFFFFFFFFFFFFF. |
| `pg_prng_int64_range` | pg_prng.c:192 | lib.rs:89 | MATCH | `min.wrapping_add(u64_range(0,(max as u64)-(min as u64)) as i64)` == C's `(u64)rmin + uval` careful-cast (wrapping-add commutes with i64 cast). empty->rmin. |
| `pg_prng_uint32` | pg_prng.c:227 | lib.rs:97 | MATCH | v>>32. |
| `pg_prng_int32` | pg_prng.c:243 | lib.rs:101 | MATCH | v>>32 as i32. |
| `pg_prng_int32p` | pg_prng.c:254 | lib.rs:105 | MATCH | v>>33. |
| `pg_prng_double` | pg_prng.c:268 | lib.rs:109 | MATCH | `ldexp((v>>12) as f64,-52)` == `(v>>12) as f64 * 2^-52`. |
| `pg_prng_double_normal` | pg_prng.c:290 | lib.rs:113 | MATCH | Box-Muller; `1.0-double`; `sqrt(-2 ln u1)*sin(2pi u2)`, M_PI=core::f64::consts::PI. |
| `pg_prng_bool` | pg_prng.c:313 | lib.rs:119 | MATCH | v>>63. |

`pg_global_prng_state` (process-wide static) -> `thread_local! GLOBAL_PRNG`
with a `global_prng(|s| ...)` accessor. Faithful: the C global is backend-private
memory (reseeded per backend), so per-thread is the correct repo model, not a
shared static.

## Seams / wiring

Pure leaf. No C file in this unit's c_sources has an owned `X-seams` crate
(no `base64-seams`, no `pg-prng-seams`/`prng-seams` exist). No outward seam
calls. `init_seams()` is a correct no-op; wired into
`seams-init::init_all()` (seams-init/src/lib.rs:220) and listed in its Cargo.toml.

## Gates

- `cargo test -p common-prng-base64`: 7 passed (encode/decode reference vectors,
  whitespace/bad-padding rejection, `Zm=9` edge case, overflow-zeroes-dst, len helpers).
- `cargo test -p seams-init`: both `recurrence_guard` tests pass
  (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`).
- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-access-common-printtup).

## Verdict: PASS

All 21 functions MATCH. No MISSING / PARTIAL / DIVERGES, no own-logic stubs,
no deferred/SEAMED escapes, no design-rule violations.
