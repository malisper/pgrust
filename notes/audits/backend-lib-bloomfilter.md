# Audit: backend-lib-bloomfilter (SAFE re-port)

Unit C source: `src/backend/lib/bloomfilter.c` (PostgreSQL 18.3) + header
`src/include/lib/bloomfilter.h`.
References: `../pgrust/c2rust-runs/backend-lib-no-ilist/src/bloomfilter.rs`
(c2rust), `../pgrust/src-idiomatic/crates/backend-lib-bloomfilter/src/lib.rs`
(the SAFE idiomatic source this re-port is based on),
`crates/backend-lib-bloomfilter/src/lib.rs` (the port).

Independent re-derivation from C + c2rust; the port's comments were not trusted.

## Re-port rationale

The previous port was flagged HIGH by the raw-pointer-overuse audit: it mirrored
the C `struct bloom_filter` byte-for-byte as a `#[repr(C)]` header with a
`FLEXIBLE_ARRAY_MEMBER bitset` (`[u8; 0]`), allocated the whole object as a
single `alloc_zeroed` block, freed it with `dealloc`, and crossed the seam as an
opaque `*mut BloomFilter`. That is the raw-pointer shape (13 `unsafe` keyword
uses) the SAFE src-idiomatic version avoids.

`bloom_filter` is NOT intrinsically intrusive/caller-allocated (unlike rbtree):
it is a self-contained object PostgreSQL `palloc0`s and owns, and the sole
in-repo consumer (`backend-utils-adt-acl::role_membership`) only holds an opaque
pointer + a `NULL` sentinel. That makes a clean swap to an owned value possible.

## Port shape (after re-port)

Following the SAFE src-idiomatic version, the filter is now the owned
`BloomFilter` value defined in the dependency-free seam crate
(`backend-lib-bloomfilter-seams`): three plain control fields (`k_hash_funcs`,
`seed`, `m`) and the bitset as an owned `Vec<u8>`. No `FLEXIBLE_ARRAY_MEMBER`,
no raw pointer; the impl crate is `#![forbid(unsafe_code)]` and has **0** real
`unsafe`. Bit addressing (`hash >> 3`, `hash & 7`) is unchanged from C and now
indexes a safe slice.

### Sanctioned divergences (audit against these)

1. **`palloc0` -> fallible `Vec<u8>`.** Like the rbtree re-port, this port drops
   the memory-context charge model the src-idiomatic version used (`MemoryContext`
   + `PgVec`) — this repo's bloomfilter has no `backend-utils-mctx` dep — and
   uses a plain `Vec<u8>` allocated OOM-safely via `try_reserve_exact` +
   `resize(.,0)`. The bitset is byte-identical (`m / 8` zero bytes). C's `palloc0`
   `ereport(ERROR)` OOM exit becomes `Err(PgError::error("out of memory")
   .with_sqlstate(ERRCODE_OUT_OF_MEMORY))`, preserving the `PgResult` seam
   signature.
2. **`pfree`/`bloom_free` -> Drop.** The C `bloom_free(filter)` `pfree` is the
   owned value being dropped. The `bloom_free` seam declaration is **removed**
   (Rust ownership releases the bitset on drop); the consumer drops its
   `Option<BloomFilter>`. No declared-but-uninstalled seam results (confirmed by
   the seams-init recurrence guards).
3. **`hash_any_extended` -> `hash_bytes_extended` via `common-hashfn-seams`.**
   Same as before; the fmgr wrapper is bypassed straight to the ported Bob
   Jenkins hash. SEAMED (real dep cycle, thin delegate).
4. **`pg_popcount` -> per-byte `count_ones()`.** Identical value on every input.

## Function inventory and verdicts

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `bloom_create` | 86-120 | lib.rs `bloom_create` | MATCH | `Min(work_mem*1024, total_elems*2)`, `Max(1MB,.)`, `my_bloom_power(bytes*8)`, `1<<power`, `bytes=bits/8`, fields set. `palloc0` -> fallible zeroed `Vec`. Integer widths/wrapping identical (work_mem cast to u64 then *1024; total_elems*2 in i64 then `as u64`). |
| `bloom_free` | 125-129 | (removed; Drop) | MATCH | `pfree` -> owned value dropped. Behavior-identical release. |
| `bloom_add_element` | 134-147 | lib.rs `bloom_add_element` | MATCH | `bitset[h>>3] |= 1<<(h&7)` for i in 0..k. `.iter().take(k)` == C loop. |
| `bloom_lacks_element` | 156-172 | lib.rs `bloom_lacks_element` | MATCH | early-return true if any addressed bit clear; else false. |
| `bloom_prop_bits_set` | 186-193 | lib.rs `bloom_prop_bits_set` | MATCH | `pg_popcount(bitset, m/8) / (double) m`. |
| `my_bloom_power` | 209-221 | lib.rs `my_bloom_power` | MATCH | identical `while bits>0 && power<32` loop. |
| `optimal_k` | 228-234 | lib.rs `optimal_k` | MATCH | `rint(log(2.0)*bits/total)` -> `(LN_2*bits/total).round_ties_even()`; `Max(1,Min(k,10))`. |
| `k_hashes` | 249-276 | lib.rs `k_hashes` | MATCH | `x=(u32)hash`, `y=(u32)(hash>>32)`, `mod_m`, then `x=mod_m(x+y)`, `y=mod_m(y+i)` with `wrapping_add` (C uint32 overflow wraps). Loop from 1. |
| `mod_m` | 287-294 | lib.rs `mod_m` | MATCH | `val & (m-1)`; `debug_assert!` mirror C `Assert`. |

All functions **MATCH** (with `hash_any_extended` SEAMED per rule 3).

## Seam audit

Owned seam crate: `backend-lib-bloomfilter-seams` (maps to `bloomfilter.c`).

- Declarations: `bloom_create`, `bloom_add_element`, `bloom_lacks_element`,
  `bloom_prop_bits_set`. All four installed by `init_seams()` (nothing but
  `set()` calls). `init_seams()` wired into `seams-init::init_all`
  (`crates/seams-init/src/lib.rs:105`). Verified by the two seams-init
  recurrence-guard tests (every owned seam installed; crate wired into init_all).
- The `bloom_free` seam was **removed** (Drop replaces it); no orphan
  declaration remains.
- Seam bodies are thin pass-through (`set(bloom_create)` etc.) — no branching or
  computation in the seam path.
- Outward call `hash_bytes_extended` via `common-hashfn-seams`: justified
  dependency-cycle delegate, marshal-only.

## Design conformance

- No invented opacity — the inherited `*mut BloomFilter` opaque view was
  *removed* in favor of a real owned struct (strictly improves on
  opacity-inherited-never-introduced).
- Allocating function returns `PgResult` for the OOM (`palloc0` ereport) path.
- No raw pointers, no shared statics, no ambient-global seams, no locks across
  `?`. `#![forbid(unsafe_code)]` holds (0 real `unsafe`).

## Consumer rewire

`backend-utils-adt-acl::role_membership`: the holder `*mut BloomFilter` +
`ptr::null_mut()` sentinel becomes `Option<BloomFilter>`; element pointers
(`&Oid` cast to `*const u8`, `size_of::<Oid>()`) become `&role.to_ne_bytes()`
(native-endian, byte-identical to the C `&role` cast). `bloom_free(bf)` becomes
`drop(bf)`. Logic unchanged (same predicates, same threshold, same populate
loop). Compiles and its build is clean.

## Verdict

**PASS.** Every function MATCH (one SEAMED delegate). Zero seam findings.
`unsafe_before` = 13, `unsafe_after` = 0 (`forbid(unsafe_code)` in force).
Workspace `cargo check` clean; `cargo test -p backend-lib-bloomfilter` (10
tests incl. the test_bloomfilter.c golden FP-rate test) and
`cargo test -p seams-init` pass; `backend-utils-adt-acl` builds.
