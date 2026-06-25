# Audit: common-hashfn

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context)
- **Branch:** port/common-hashfn
- **C source:** `src/common/hashfn.c` (+ inlines/macros from `src/include/common/hashfn.h`)
- **c2rust:** `c2rust-runs/common-batch2/src/hashfn.rs` (and identical copies in other batches)
- **Port:** `crates/common-hashfn/src/lib.rs`, seams in `crates/common-hashfn-seams/src/lib.rs`

This is an independent re-derivation. The hashfn output is keyed to the
little-endian byte order PostgreSQL produces; the port targets LE (as the
catalog row records) and the test oracles are LE. On a little-endian host the C
aligned and unaligned source paths produce identical results, so the port's
single byte-slice path is behavior-preserving against both. The big-endian
`WORDS_BIGENDIAN` branches are out of the build config and are intentionally not
mirrored (LE-only target).

## Independent oracle check

A standalone harness compiled directly from the unmodified `hashfn.c` body
(`cc -O2`, LE host) emits, for `hash_bytes`/`hash_bytes_extended`:

| input | C oracle | Rust test asserts |
|---|---|---|
| `""` | a7ea466d | a7ea466d |
| `"a"` | 401370b1 | 401370b1 |
| `"abc"` | d12feb97 | d12feb97 |
| `"PostgreSQL"` (len 10) | 9ae1fe84 | 9ae1fe84 |
| `"abc..xyz"` (len 26) | 0a00e7bb | 0a00e7bb |
| `"123456789"` (len 9) | 3c7347a8 | 3c7347a8 |
| `"1234567890"` (len 10) | e9c1ee42 | e9c1ee42 |
| `"12345678901"` (len 11) | 6259ed3a | 6259ed3a |
| `hash_bytes_extended("PostgreSQL", 0x0123456789abcdef)` | 271d3a06c807e270 | 271d3a06c807e270 |

Byte-exact, including the len 9/10/11 cases where `c`'s low byte is reserved for
the length, and the seed-perturbation path. `cargo test -p common-hashfn`: 7/7 pass.

## Per-function table

| C function (hashfn.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `mix(a,b,c)` macro | `HashState::mix` | MATCH | All six reversible rounds; rotate constants 4/6/8/16/19/4; wrapping arithmetic. Identical to c2rust expansion. |
| `final(a,b,c)` macro | `HashState::final_mix` | MATCH | Seven rounds; rotate constants 14/11/25/16/4/14/24. |
| `rot(x,k)` macro = `pg_rotate_left32` | `u32::rotate_left` | MATCH | Native rotate; identical to C `(word<<n)|(word>>(32-n))` for n in 1..31 (all uses). |
| `hash_bytes` | `hash_bytes` / `hash_bytes_state` / `hash_bytes_into_state` (finish32) | MATCH | Init `a=b=c=0x9e3779b9+len+3923095`; 12-byte main loop; LE tail re-derived case-by-case for len 0..11 (see tail analysis below); `final`; returns `c`. assert!(len<=i32::MAX) mirrors the C `int keylen`. |
| `hash_bytes_extended` | `hash_bytes_extended` (finish64) | MATCH | Seed mix-before-loop (`a+=seed>>32; b+=seed; mix`) when seed!=0; same main loop + tail; returns `(b<<32)\|c`. |
| `hash_bytes_uint32` | `hash_bytes_uint32` | MATCH | `a=b=c=0x9e3779b9+sizeof(u32)+3923095; a+=k; final; return c`. |
| `hash_bytes_uint32_extended` | `hash_bytes_uint32_extended` | MATCH | Same init; seed mix when seed!=0; `a+=k`; `final`; `(b<<32)\|c`. |
| `string_hash` | `string_hash` | MATCH | strlen via NUL scan; `Min(s_len, keysize-1)` with `wrapping_sub(1)` reproducing the C unsigned wrap when keysize==0 (full string hashed); then `hash_bytes`. |
| `tag_hash` | `tag_hash` | MATCH | `hash_bytes(key[..keysize])`. assert! that the slice covers keysize. |
| `uint32_hash` | `uint32_hash` | MATCH | `hash_bytes_uint32(k)`. C `Assert(keysize==sizeof(uint32))` is a caller contract; the Rust signature takes `u32` directly so the assert is structurally enforced. |
| `ROTATE_HIGH_AND_LOW_32BITS` macro (hashfn.h) | `rotate_high_and_low_32bits` | MATCH | `((v<<1)&0xfffffffefffffffe)\|((v>>31)&0x100000001)`. |
| `hash_combine` inline (hashfn.h) | `hash_combine` | MATCH | `a ^= b+0x9e3779b9+(a<<6)+(a>>2)`. |
| `hash_combine64` inline (hashfn.h) | `hash_combine64` | MATCH | `a ^= b+0x49a0f4dd15e5a8e3+(a<<54)+(a>>7)`. |
| `murmurhash32` inline (hashfn.h) | `murmurhash32` | MATCH | shifts 16/13/16, muls 0x85ebca6b/0xc2b2ae35. |
| `murmurhash64` inline (hashfn.h) | `murmurhash64` | MATCH | shifts 33/33/33, muls 0xff51afd7ed558ccd/0xc4ceb9fe1a85ec53. |
| `hash_any` inline (hashfn.h, FRONTEND-gated) | n/a | OUT-OF-UNIT | `UInt32GetDatum(hash_bytes(k,len))` — a header convenience that only Datum-wraps `hash_bytes`. The hash core lives here; the Datum wrap is an fmgr concern done at the consumer. No absent logic. |
| `hash_any_extended` inline | n/a | OUT-OF-UNIT | `UInt64GetDatum(hash_bytes_extended(...))`; same rationale. |
| `hash_uint32` inline | n/a | OUT-OF-UNIT | `UInt32GetDatum(hash_bytes_uint32(k))`; consumers (rangetypes, acl) call `hash_bytes_uint32` and wrap locally. |
| `hash_uint32_extended` inline | n/a | OUT-OF-UNIT | `UInt64GetDatum(hash_bytes_uint32_extended(...))`; same. |

No statics, no inline helpers beyond the macros above. c2rust's `hashfn.rs`
contains exactly the four no_mangle byte/int hashers plus the three key-helper
wrappers — all accounted for.

## Tail-handling re-derivation (LE)

After the 12-byte main loop, `len` is 0..11. The port collapses the C
switch/fallthrough into three guarded `wrapping_add`s:
`a += read_tail_word(bytes[0..min(4)])` always;
`b += read_tail_word(bytes[4..min(8)])` if len>4;
`c += read_tail_word_c(bytes[8..])` if len>8.
`read_tail_word` packs byte i at shift 8*i; `read_tail_word_c` packs byte i at
shift 8*(i+1) (the reserved-low-byte rule). Verified equal to the C LE switch
for every len:

- 0: a+=0 (no-op) = C case 0.
- 1/2/3: a += k[0] (+k[1]<<8 (+k[2]<<16)) = C cases 1/2/3.
- 4: a += word(k0..3) = C case 4 (`a+=ka[0]`).
- 5/6/7: also b += k[4] (+k[5]<<8 (+k[6]<<16)) = C cases 5/6/7.
- 8: a+=ka[0], b+=ka[1] (full b word) = C case 8.
- 9/10/11: also c += k[8]<<8 (+k[9]<<16 (+k[10]<<24)) = C cases 9/10/11, low byte of c left for length.

`read_tail_word_c` uses non-wrapping `+`, safe: at most 3 distinct byte
positions at shifts 8/16/24, never overflows u32.

## Seam audit

Owned seam crates by C-source coverage (`*/hashfn.c`): **`common-hashfn-seams`** only.

- Declarations: `hash_bytes_uint32`, `hash_bytes_uint32_extended`, `tag_hash`,
  `string_hash` (4).
- `common_hashfn::init_seams()` installs all four via `set()` and contains
  nothing but `set()` calls. No uninstalled declaration.
- `seams-init/src/lib.rs:155` calls `common_hashfn::init_seams()`.
- No outward seam calls: the crate is pure computation with no dependency on any
  unported neighbor, so no cross-seam delegation exists to justify.
- No `set()` outside the owner.

## Design conformance (§3b)

- No allocation: pure value math, no `Mcx`/`PgResult` needed (seams are
  infallible, matching the C which "must never throw elog(ERROR)" — the
  ResourceOwner contract at hashfn.c:138).
- No statics / no per-backend globals / no ambient-global seams.
- No invented opacity: keys cross as `&[u8]` (the C `const void *key` over the
  first `keysize` bytes) — a real byte image, not a stand-in handle.
- No locks, no registry side tables, no unledgered divergence markers.
- LE-only narrowing is recorded in the catalog row and matches PostgreSQL's
  on-disk/in-memory LE hash semantics; not a silent divergence.

## Conclusion

Every C function MATCH (the four header inlines are Datum-wrapping conveniences
whose hash core is present and exported), zero seam findings, zero design
findings. **PASS.**
