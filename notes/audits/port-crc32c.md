# Audit: port-crc32c

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Unit:** `port-crc32c` (`src/port/pg_crc32c_sb8.c`)
- **Branch:** `port/port-crc32c`

## 1. Function inventory

`src/port/pg_crc32c_sb8.c` defines exactly one function and one file-static
data object:

| # | C symbol | Kind | C location | Port location | Verdict |
|---|----------|------|-----------|---------------|---------|
| 1 | `pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len)` | function | pg_crc32c_sb8.c:34-99 | `crates/port-crc32c/src/lib.rs:25` `pg_comp_crc32c_sb8` | MATCH |
| 2 | `pg_crc32c_table[8][256]` | static const data | pg_crc32c_sb8.c:25,109-1169 | `crates/port-crc32c/src/table.rs` `PG_CRC32C_TABLE` | MATCH |
| — | `CRC8(x)` | macro (not a function) | pg_crc32c_sb8.c:28-32 | `crates/port-crc32c/src/lib.rs:18` `crc8()` inline helper | MATCH |

There is no c2rust rendering for this unit (`c2rust-runs/` has only the armv8
and ltree crc variants, not `pg_crc32c_sb8`). The C source was the sole
reference; cross-check was performed value-by-value against the C table (below).

`INIT_CRC32C` / `FIN_CRC32C` / `EQ_CRC32C` (from `port/pg_crc32c.h`) are trivial
macros (`= 0xFFFFFFFF`, `^= 0xFFFFFFFF`, `==`) performed at call sites, not
functions of this `.c` file; correctly out of scope and not stubbed.

## 2. Function-by-function comparison

### `pg_comp_crc32c_sb8` — MATCH

Verified branch by branch against pg_crc32c_sb8.c:34-99 (little-endian build
config, which is the only path the port carries — see §3b):

- **Unaligned prefix loop** (C 44-48): `while (len > 0 && ((uintptr_t)p & 3))`
  consumes 0-3 leading bytes via `CRC8`. Rust (lib.rs:30-33):
  `while !p.is_empty() && (p.as_ptr() as usize & 3) != 0` — same predicate, same
  per-byte `crc8`, advances the slice by one. (The alignment test is a
  performance optimization; CRC accumulation is byte-associative so the result
  is independent of where the split falls — the port preserves it faithfully
  regardless.)
- **8-byte slicing loop** (C 54-86): reads two native u32 words `a = *p4++ ^
  crc`, `b = *p4++`. On little-endian `*p4` is a LE load → Rust uses
  `u32::from_le_bytes` for both (lib.rs:38-39). Byte extraction matches the
  `#else` (LE) arm exactly: `c0=b>>24, c1=b>>16, c2=b>>8, c3=b, c4=a>>24,
  c5=a>>16, c6=a>>8, c7=a` (C 69-76 vs lib.rs:41-48). The XOR fold over
  `pg_crc32c_table[0..8][c0..c7]` is byte-for-byte the same table-index mapping
  (C 79-83 vs lib.rs:50-58). Loop bound `len >= 8` ↔ `p.len() >= 8`; advance by
  8 each iteration.
- **Trailing-byte loop** (C 91-96): remaining `< 8` bytes via `CRC8` one at a
  time. Rust (lib.rs:61-64) identical.
- **Return** `crc` (C 98 / lib.rs:66).
- **`crc8` helper**: matches the LE `CRC8` macro
  `pg_crc32c_table[0][(crc ^ x) & 0xFF] ^ (crc >> 8)` (C 31) exactly
  (lib.rs:19).

No allocation, no failure path, no `ereport`/`elog`, no early returns. Pure
arithmetic over the input slice. Logic is provably identical for all inputs on
little-endian.

### `PG_CRC32C_TABLE` — MATCH (verified mechanically)

Extracted all `0x........` literals from the little-endian table block of the C
source (pg_crc32c_sb8.c:111-638, the `#ifndef WORDS_BIGENDIAN` arm) — 2048
values — and from `table.rs` — 2048 values (after excluding the polynomial
constant `0x1EDC6F41` that appears only in the doc comment). A normalized
(upper-cased) line-by-line `diff` of the two sequences is **empty**: all 2048
entries match in value and order across all 8 sub-tables. This is the
constants-transcription check the SKILL flags as a recurring silent-corruption
risk; it is clean.

## 3. Seam audit

**Owned seam crates** (every `crates/X-seams` whose `X` maps to a C file in this
unit's `c_sources`, `src/port/pg_crc32c*.c`):

- `crates/port-crc32c-seams` — declares `comp_crc32c(crc: u32, data: &[u8]) -> u32`.
- `crates/port-pg-crc32c-seams` — declares `pg_comp_crc32c(crc: u32, data: &[u8]) -> u32`.

Both are the only crc32c seam crates in the tree. Both declarations are
installed by this crate's `init_seams()` (lib.rs:69-72):

```rust
pub fn init_seams() {
    port_crc32c_seams::comp_crc32c::set(pg_comp_crc32c_sb8);
    port_pg_crc32c_seams::pg_comp_crc32c::set(pg_comp_crc32c_sb8);
}
```

- `init_seams()` contains nothing but `set()` calls. No uninstalled
  declarations remain; no empty installer.
- `seams-init::init_all()` calls `port_crc32c::init_seams()`
  (seams-init/src/lib.rs:37); the Cargo dep was added.
- **Outward seam calls:** none. This is a leaf primitive; it does not reach
  across any seam. No marshal/delegate logic, no branching in a seam path.
- No function body was replaced by a seam call.

**Seam findings: zero.**

## 3b. Design conformance

- **Failure surface (seam signatures mirror C):** the C function cannot
  `ereport`; it is pure arithmetic. The seam returns plain `u32`, not
  `PgResult` — correct, no manufactured failure surface.
- **Mcx / allocation:** no allocation anywhere; no `Mcx` threading needed.
- **Opacity:** no invented handles or stand-in types; inputs are `u32` + `&[u8]`.
- **Globals:** the lookup table is a `static` of immutable `const` data
  (read-only, identical for every backend) — not a per-backend mutable global,
  so the shared-statics rule does not apply. No ambient-global seams.
- **Big-endian omission:** the C file carries both a LE and a BE table (the BE
  arm stores byte-reversed values) selected by `WORDS_BIGENDIAN`. The port
  carries the LE table and LE byte-extraction only. This is justified and
  ledgered in the module doc and the CATALOG note: every consumer in this tree
  is little-endian and the seam signature is a LE `u32`. The dropped `#ifdef
  WORDS_BIGENDIAN` branch is a build-config branch absent from any LE build, not
  missing logic. Documented divergence; not a silent stub.

**Design findings: zero.**

## 4. Verdict

**PASS.** The single function `MATCH`es exactly (control flow, byte
extraction, loop bounds, the LE `CRC8` macro), the 2048-entry lookup table is
mechanically verified identical to the C source, both owned seam crates are
fully installed by `init_seams()` and wired through `seams-init`, and there are
zero seam findings and zero design-conformance findings. Tests pass, including
the canonical CRC-32C("123456789") = `0xE3069283` check vector, empty input,
and split-call equivalence (exercising the unaligned-prefix and tail paths).
