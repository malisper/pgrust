# Audit: common-checksum-helper

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- Branch: `port/common-checksum-helper`
- Unit C source: `src/common/checksum_helper.c`
- Port crate: `crates/common-checksum-helper`

Audit is independent: re-derived from the C source (`postgres-18.3`), the C
headers, and the c2rust rendering of `checksum_helper.c` (taken from
`c2rust-runs/common-batch9/src/checksum_helper.rs`; no dedicated c2rust run
exists for this unit, it only appears inside `common-*batch*` units). Constants
verified against headers, not from memory.

## 1. Function inventory

`checksum_helper.c` defines exactly five functions; it has no statics and no
inline helpers. The c2rust rendering confirms the same five. Every one gets a
row.

| # | C function (C loc) | Port (lib.rs) | Verdict | Notes |
|---|--------------------|---------------|---------|-------|
| 1 | `pg_checksum_parse_type` (27-50) | `pg_checksum_parse_type` (146-162) | MATCH | Six `pg_strcasecmp`-guarded arms in identical order (none/crc32c/sha224/256/384/512); else returns `None`. C's `pg_strcasecmp` routes to real owner `port-pgstrcasecmp` (`== 0`). C's `*type = CHECKSUM_TYPE_NONE; return false` on the unknown arm is preserved as `None` (idiomatic `Option`; the out-param NONE write is observationally folded into the `None` return). NUL-terminated byte slices match the C `char *` contract. |
| 2 | `pg_checksum_type_name` (55-76) | `pg_checksum_type_name` (178-187) | MATCH | All six arms return the exact strings NONE/CRC32C/SHA224/SHA256/SHA384/SHA512. The C `Assert(false); return "???"` fall-through is provably unreachable: `pg_checksum_type` is a closed Rust enum, so no out-of-range value exists. |
| 3 | `pg_checksum_init` (82-138) | `pg_checksum_init` + `create_sha2` (198-232) | MATCH | NONE→no-op; CRC32C→`INIT_CRC32C` = `0xFFFFFFFF` (verified vs `pg_crc32c.h:41`); each SHA-2 arm: `pg_cryptohash_create(selector)`, NULL→err(-1), then `pg_cryptohash_init`, on `<0` free+err(-1). The NULL/init/free control flow lives in-crate (`create_sha2`); only the three `pg_cryptohash_*` primitive calls cross the seam. Selectors `PG_SHA224/256/384/512` = 2/3/4/5, verified vs `cryptohash.h`. C `0`/`-1` → `Result`. |
| 4 | `pg_checksum_update` (144-166) | `pg_checksum_update` (242-266) | MATCH | NONE→Ok; CRC32C→`COMP_CRC32C` via `port_crc32c::pg_comp_crc32c_sb8`; SHA-2 (all four arms share one body in C)→`pg_cryptohash_update`, `<0`→err. C switches on `context->type`; port switches on `raw_context`, equivalent because init constructs `type`/`raw_context` in lockstep. |
| 5 | `pg_checksum_final` (175-232) | `pg_checksum_final` + helpers (283-346) | MATCH | NONE→0; CRC32C→`FIN_CRC32C` (XOR `0xFFFFFFFF`, verified vs `pg_crc32c.h:54`), `retval = sizeof(pg_crc32c) = 4`, host-endian `memcpy` rendered as `to_ne_bytes` copy; SHA-2→`retval = PG_SHA*_DIGEST_LENGTH` (28/32/48/64, verified vs `sha2.h:20-30`), `pg_cryptohash_final` `<0`→err (no free on failure, matching C), then `pg_cryptohash_free`. The `StaticAssertDecl` digest-fit checks and the trailing `Assert(retval <= PG_CHECKSUM_MAX_LENGTH)` are compile-time/debug-only invariants, satisfied by construction (all lengths ≤ 64 = `PG_CHECKSUM_MAX_LENGTH`). |

### Constants cross-checked against headers

- `PG_SHA224/256/384/512_DIGEST_LENGTH` = 28/32/48/64 — `common/sha2.h:20-30`. ✓
- `PG_CHECKSUM_MAX_LENGTH` = `PG_SHA512_DIGEST_LENGTH` = 64 — `checksum_helper.h`. ✓
- `pg_checksum_type` NONE..SHA512 = 0..5 — `checksum_helper.h`. ✓
- `pg_cryptohash_type` PG_MD5..PG_SHA512 = 0..5 — `cryptohash.h`. ✓
- `INIT_CRC32C` = `0xFFFFFFFF`, `FIN` = `^= 0xFFFFFFFF`, `sizeof(pg_crc32c)` = 4
  (`typedef uint32 pg_crc32c`) — `port/pg_crc32c.h:38,41,54`. ✓

Note on CRC implementation selection: original C uses the `pg_comp_crc32c`
dispatch pointer; the sampled c2rust run resolved it to `pg_comp_crc32c_armv8`;
the port calls `port_crc32c::pg_comp_crc32c_sb8`. All three compute the same
CRC-32C (Castagnoli) over identical input — interchangeable implementations of
one function. Not a divergence.

## 2. Seams and wiring (§3)

**Owned seam crates: none.** Ownership is by C-source coverage. This unit's
only `c_source` is `checksum_helper.c`. The seam crate it touches,
`common-cryptohash-seams`, maps to `common/cryptohash.c` /
`cryptohash_openssl.c` — a *different* unit's C. So this unit owns zero seam
crates, and its empty `init_seams()` is correct (nothing to install). The
crate's `init_seams()` is wired into `seams-init::init_all()`
(`common_checksum_helper::init_seams();`). ✓

**Outward seam calls** (`pg_cryptohash_create/init/update/final/free`):
justified by a real dependency — the cryptohash provider
(`cryptohash.c`/`cryptohash_openssl.c`, an OpenSSL `EVP_*` / in-tree `sha2.c`
primitive) is unported. The consumer correctly *introduces* the seam crate but
does not own/install it; the cryptohash unit installs the real implementation
when it lands, and calls panic loudly until then (no silent fallback). In
tests, mocks are `set()` only within the test module. Each seam path is a thin
delegate: argument pass-through, one call, result mapping; all branching
(NULL/`<0`/free) lives in-crate, never in a seam. ✓

Seam signature shape: `pg_cryptohash_*` report failure via the C `int`
return (`<0`) / NULL pointer, never `ereport`. Per the failure-surface rule the
seams therefore return raw `i32` / `*mut pg_cryptohash_ctx`, not `PgResult`,
and need no `Mcx` (allocation occurs behind the seam in the provider). ✓

## 3. Design conformance (§3b)

- **Inherited opacity, not invented (types.md 6-7):** `pg_cryptohash_ctx` is a
  genuinely opaque C typedef (`struct pg_cryptohash_ctx` with no exposed
  definition). `types-crypto` renders it as a zero-field, never-constructed
  struct held only by `*mut`. Real type, inherited opacity — conforms. ✓
- **No allocating function/seam lacks Mcx+PgResult:** this crate allocates
  nothing; the SHA-2 allocation is the provider's, behind the seam. ✓
- No shared statics for per-backend globals; no ambient-global seams; no locks
  across `?`; no registry-shaped side tables. ✓
- **One intentional, benign stricture:** `pg_checksum_final` takes `&mut [u8]`
  and returns `ChecksumError::OutputTooSmall` for an undersized buffer where C
  writes unchecked. C's documented contract requires the caller to supply a
  `PG_CHECKSUM_MAX_LENGTH` buffer; on every contract-conforming input the
  behavior is byte-identical. A bounds-carrying slice cannot reproduce C's
  unchecked write, so surfacing an error is the only sound rendering and is
  strictly stricter than C, never yielding a different valid-input result. This
  is idiomatic rendering of an unsafe C contract, not a logic divergence; no
  ledger entry required.

## 4. Verdict

**PASS.** All five functions MATCH; the genuinely-external SHA-2 primitive is
properly seamed across a non-owned neighbor seam crate with all decision logic
retained in-crate; zero seam findings; design-conformance clean. `cargo build`
and `cargo test -p common-checksum-helper` both green (11 tests pass).

Auditor spot-check: re-derived #3 (`pg_checksum_init`) and #5
(`pg_checksum_final`) line-by-line against the C, including the free-on-error
asymmetry (init frees on `init<0`; final does *not* free on `final<0`) — both
match the C exactly. CRC seed/finalize constants and all four digest lengths
re-verified against headers above.
