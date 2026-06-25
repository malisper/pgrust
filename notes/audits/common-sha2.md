# Audit: common-sha2

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) (claude-opus-4-8[1m])
- Branch: port/common-sha2
- Unit C source: `src/common/sha2.c` (PostgreSQL 18.3), headers
  `src/include/common/sha2.h`, `src/common/sha2_int.h`.
- Port: `crates/common-sha2/src/lib.rs` (+ `tests.rs`).

## Scope / method

Independent re-derivation from the C source. The build config does **not**
define `SHA2_UNROLL_TRANSFORM`, so the active transform bodies are the `#else`
(rolled) branches; the unrolled `#ifdef` macro variants are not in the build and
are correctly not ported. No c2rust run exists for this `common` leaf
(`c2rust-runs` has only `sha1` probes), so comparison is C-direct plus the
FIPS-180 known-answer vectors in `tests.rs` (all 17 pass, incl. the 1,000,000-`a`
SHA-256 vector and incremental-vs-oneshot checks).

This crate is a pure `#![no_std]`, `#![forbid(unsafe_code)]` arithmetic leaf:
no allocation, no outward calls, no seam declarations.

## Function inventory & verdicts

Every definition in `sha2.c` (functions, statics, and the macro-helpers that
the C realizes as inline expressions):

| C symbol (sha2.c) | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| `REVERSE32` (macro) | 95 | folded into BE load/store | MATCH | endianness handled by explicit big-endian byte ops; net effect identical on PG's LE hosts |
| `REVERSE64` (macro) | 100 | folded into BE store | MATCH | same |
| `ADDINC128` (macro) | 115 | `addinc128` 127 | MATCH | wrapping add + carry on `w[0] < n` |
| `R` (macro) | 132 | `r32`/`r64` 137/141 | MATCH | shift-right |
| `S32` (macro) | 134 | `s32` 146 | MATCH | 32-bit rotate-right |
| `S64` (macro) | 136 | `s64` 151 | MATCH | 64-bit rotate-right |
| `Ch` | 139 | `ch32`/`ch64` 157/165 | MATCH | |
| `Maj` | 140 | `maj32`/`maj64` 161/169 | MATCH | |
| `Sigma0_256` | 143 | `big_sigma0_256` 175 | MATCH | S32(2,13,22) |
| `Sigma1_256` | 144 | `big_sigma1_256` 179 | MATCH | S32(6,11,25) |
| `sigma0_256` | 145 | `small_sigma0_256` 183 | MATCH | S32(7,18) ^ R(3) |
| `sigma1_256` | 146 | `small_sigma1_256` 187 | MATCH | S32(17,19) ^ R(10) |
| `Sigma0_512` | 149 | `big_sigma0_512` 193 | MATCH | S64(28,34,39) |
| `Sigma1_512` | 150 | `big_sigma1_512` 197 | MATCH | S64(14,18,41) |
| `sigma0_512` | 151 | `small_sigma0_512` 201 | MATCH | S64(1,8) ^ R(7) |
| `sigma1_512` | 152 | `small_sigma1_512` 205 | MATCH | S64(19,61) ^ R(6) |
| `K256[64]` | 165 | `K256` 211 | MATCH | all 64 words verified against C, byte-for-byte |
| `sha224_initial_hash_value[8]` | 185 | 223 | MATCH | verified |
| `sha256_initial_hash_value[8]` | 197 | 228 | MATCH | verified |
| `K512[80]` | 209 | `K512` 233 | MATCH | all 80 words verified against C |
| `sha384_initial_hash_value[8]` | 253 | 257 | MATCH | verified |
| `sha512_initial_hash_value[8]` | 265 | 269 | MATCH | verified |
| `pg_sha256_init` | 278 | 282 | MATCH | NULL guard idiomatic-elided (`&mut`); state/buffer/bitcount reset identical |
| `SHA256_Transform` (rolled) | 386 | `sha256_transform` 295 | MATCH | 0..15 BE load + compress; 16..63 schedule expansion with `&0x0f` indexing; T1/T2 and a..h rotation identical; final state add identical |
| `pg_sha256_update` | 476 | 380 | MATCH | len==0 early return; usedspace/freespace fill+transform; full-block loop; leftover save; bitcount += len<<3 each step |
| `SHA256_Last` | 528 | `sha256_last` 431 | MATCH | 0x80 pad, short/long-block branches, bitcount stored BE at SHORT_BLOCK_LENGTH, final transform |
| `pg_sha256_final` | 576 | 486 | MATCH | NULL-digest guard via `is_empty()`; 8 words emitted BE = 32 bytes; ctx zeroed |
| `pg_sha512_init` | 604 | 503 | MATCH | bitcount[0]=bitcount[1]=0 |
| `SHA512_Transform` (rolled) | 711 | `sha512_transform` 511 | MATCH | 64-bit analogue; 0..15 / 16..79; identical structure |
| `pg_sha512_update` | 801 | 600 | MATCH | uses bitcount[0] for usedspace; `addinc128` for the 128-bit count |
| `SHA512_Last` | 854 | `sha512_last` 649 | MATCH | long-block branch clears `BLOCK_LENGTH-2` (126) bytes exactly as C; stores bitcount[1] then bitcount[0] BE at offsets 112/120 |
| `pg_sha512_final` | 904 | 712 | MATCH | 8 words BE = 64 bytes |
| `pg_sha384_init` | 933 | 729 | MATCH | sha384 IV; 128-byte buffer |
| `pg_sha384_update` | 943 | 737 | MATCH | delegates to sha512_update (typedef-shared ctx) |
| `pg_sha384_final` | 949 | 742 | MATCH | sha512_last + 6 words BE = 48 bytes (C reverses 6 then memcpy 48) |
| `pg_sha224_init` | 977 | 759 | MATCH | sha224 IV; 64-byte buffer |
| `pg_sha224_update` | 987 | 766 | MATCH | delegates to sha256_update |
| `pg_sha224_final` | 994 | 771 | MATCH | sha256_last + 7 words BE = 28 bytes (C reverses 8 then memcpy 28; word 8's reversal is dead, byte-identical) |

### Edge cases re-verified

- **`usedspace > SHORT_BLOCK_LENGTH` second-to-last transform path** (256 & 512):
  buffer zero-fill ranges match the C `memset` extents exactly, including the
  deliberate `PG_SHA512_BLOCK_LENGTH - 2` (= 126) clear in `SHA512_Last`
  (the last 2 bytes are overwritten by the bitcount store).
- **Bit-count endianness**: the C does host-order `REVERSE64` then a host-order
  `uint64` store, then re-reads BE in the transform. The port stores the count
  big-endian directly — provably the same byte image on PG's little-endian hosts
  and endianness-independent. Confirmed by the multi-block KAT vectors.
- **`len == 0`** update is a no-op (test `update_zero_len_is_noop`).
- **NULL context / NULL digest**: C `context == NULL` guards become unrepresentable
  (`&mut` reference); NULL-digest guard preserved as `digest.is_empty()`.
- **Word index `& 0x0f` wraparound** in the schedule expansion: identical.
- **`wrapping_add`** used everywhere the C relies on unsigned overflow.

## Seam audit

- Owned seam crates = every `crates/X-seams` where `X` maps to a C file in
  `c_sources` (`*/sha2.c`). **None exist** (no `common-sha2-seams`), and no other
  seam crate declares any sha2 symbol. Nothing to install.
- The crate makes **zero outward seam calls** — it is a pure leaf with no
  dependency cycle to break.
- `init_seams()` is empty (correct: no owned declarations) and is invoked by
  `seams-init::init_all()` (`crates/seams-init/src/lib.rs:86`,
  dep wired in its Cargo.toml).
- No `set()` outside an owner, no uninstalled seam, no body replaced by an
  outward call. **Zero seam findings.**

## Design conformance (§3b)

- No allocation anywhere → no `Mcx`/`PgResult` obligation; contexts are
  fixed-size owned values, not palloc'd blobs.
- No invented opacity: `pg_sha256_ctx`/`pg_sha512_ctx` are real structs mirroring
  the C layout (state/bitcount/buffer); `pg_sha224_ctx`/`pg_sha384_ctx` are the
  same typedefs the C uses.
- The `static K256/K512/*_initial_hash_value` tables are immutable read-only
  constants (not per-backend mutable globals) → no shared-static violation.
- No ambient-global seams, no locks, no registry side tables, no divergence
  markers.

## Conclusion

All 38 inventoried symbols (functions + statics + macro-helpers) are **MATCH**.
Zero seam findings. Design-conformance clean. Tests green. **PASS.**
