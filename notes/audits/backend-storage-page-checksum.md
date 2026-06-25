# Audit: backend-storage-page-checksum

- **Unit:** `backend-storage-page-checksum`
- **C sources:** `src/backend/storage/page/checksum.c` (implementation lives in
  `src/include/storage/checksum_impl.h`, included by checksum.c)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-storage-page-checksum/src/checksum.rs`
- **Port:** `crates/backend-storage-page-checksum/src/lib.rs`
- **Auditor:** independent re-derivation from C sources, 2026-06-12

## Function inventory

`checksum.c` defines no functions of its own; it only includes
`storage/checksum_impl.h`. The complete inventory from `checksum_impl.h`
(cross-checked against the c2rust rendering, which contains the same three
items: `checksum_comp`, `PageRef::checksum_block`, `pg_checksum_page`):

| C function / macro | C location | Port location | Verdict | Notes |
|---|---|---|---|---|
| `CHECKSUM_COMP` (macro) | checksum_impl.h:135-139 | `checksum_comp`, lib.rs:114-117 | MATCH | `(tmp ^ value)`, then `tmp * FNV_PRIME ^ (tmp >> 17)`. C `*` binds tighter than `^`; port `mixed.wrapping_mul(FNV_PRIME) ^ (mixed >> 17)` is the same grouping. C unsigned multiply wraps; port uses `wrapping_mul`. FNV_PRIME = 16777619, verified against header line 108. |
| `pg_checksum_block` (static) | checksum_impl.h:145-174 | `checksum_block`, lib.rs:87-110 | MATCH | sums init from `checksumBaseOffsets` (all 32 constants diffed mechanically against header lines 121-130: identical, same order); main loop iterates `data[i][j]` row-major over 64 rows x 32 columns, port walks the page bytes in the same memory order via `chunks_exact(4)` + `from_ne_bytes` (same as a native-endian `uint32` load); two zero-mixing rounds; xor-fold of all 32 sums starting from 0. `Assert(sizeof(PGChecksummablePage) == BLCKSZ)` is a debug no-op in C; the port enforces it statically via the `[u8; BLCKSZ]` parameter type. |
| `pg_checksum_page` | checksum_impl.h:186-215 | `pg_checksum_page`, lib.rs:56-72 | MATCH | Saves `pd_checksum`, zeroes it, computes block checksum, restores it; `checksum ^= blkno`; returns `(uint16)((checksum % 65535) + 1)` — port `(checksum % 65_535 + 1) as u16` is identical (`%` binds tighter than `+`; max value 65535 fits u16). `pd_checksum` byte offset 8 verified against `bufpage.h`: `pd_lsn` is `PageXLogRecPtr` = two `uint32` (`xlogid`, `xrecoff`) = 8 bytes, `pd_checksum` is the next field; native-endian u16 read/write at offset 8 equals the C struct access. `Assert(!PageIsNew(page))` is a debug no-op; omission does not change production behavior. |

Constants verified against headers (not from memory): `N_SUMS = 32`,
`FNV_PRIME = 16777619`, `BLCKSZ = 8192` (`crates/types`, matching the build
config used by c2rust), `CHECKSUM_ROWS = 8192/(4*32) = 64`, and all 32
`checksumBaseOffsets` values (mechanical diff of hex literals: 32/32 equal,
same order).

API note: the C function takes `char *page` with a 4-byte alignment
requirement; the port takes `&mut [u8; BLCKSZ]` and uses unaligned
native-endian byte reads, which is behaviorally identical on every input
that is valid for the C version (and additionally well-defined for
unaligned buffers).

## Differential verification

Compiled the verbatim C implementation (`checksum_impl.h` logic) standalone
and compared against the port on 50 LCG-generated pseudo-random 8 KB pages
with pseudo-random block numbers: all 50 checksums identical. The port's own
5 unit tests (determinism, blkno dependence, pd_checksum restoration,
pd_checksum exclusion, nonzero result) also pass.

## Seam audit

- This unit is a pure computation leaf. No `backend-storage-page-checksum-seams`
  crate exists and none is needed: the only external references are the
  `BlockNumber` type and `BLCKSZ` constant from `crates/types` (a direct
  dependency, no cycle).
- No outward seam calls anywhere in the crate; no logic hidden behind seams.
- `init_seams()` is an empty no-op (nothing to install) and is invoked by
  `seams-init::init_all()` (crates/seams-init/src/lib.rs:9). No `set()` calls
  exist outside seam-core's own doc/test code.
- Seam findings: none.

## Verdict

**PASS** — 3/3 functions MATCH, zero seam findings, differential test against
the compiled C reference passes 50/50.
