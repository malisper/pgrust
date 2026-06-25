# Audit: backend-utils-adt-ascii

C source: `src/backend/utils/adt/ascii.c` (PostgreSQL 18.3, 199 LOC).
c2rust reference and src-idiomatic `crates/backend-utils-adt-ascii` consulted.

Scope note: the porting prompt named `chr()`/`ascii()` builtins, but those live
in `varlena.c`, not `ascii.c`. The unit manifest (`*/ascii.c`) is authoritative.
`ascii.c` exports exactly the six functions below; all are ported here.

## Function-by-function

| C function (line) | Status | Notes |
|---|---|---|
| `pg_to_ascii` (28) | MATCH | Table-driven transliteration. The 4 per-encoding tables (LATIN1/LATIN2/LATIN9/WIN1250) are byte-for-byte identical to the C string literals (verified programmatically). `RANGE_160` for the three Latin encodings, `RANGE_128` for WIN1250. Three-way mapping: `<128` pass-through, `[128,range)`→`' '`, `[range,256)`→`ascii[byte-range]`. Unsupported enc → `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, "encoding conversion from %s to ASCII not supported")`. C rewrites in place; the port returns a fresh result `Vec` of identical length (PG_GETARG_TEXT_P_COPY's copy is implicit). |
| `encode_to_ascii` (103) | MATCH | Thin wrapper delegating to `pg_to_ascii`. C's "overwrite in place, length-preserving" invariant is honored by the 1:1 byte mapping. |
| `to_ascii_encname` (119) | MATCH | `pg_char_to_encoding(encname)` via `common-encnames-seams` (owner `common/encnames.c` unported → seam); `enc < 0` → `ERRCODE_UNDEFINED_OBJECT` "%s is not a valid encoding name". |
| `to_ascii_enc` (138) | MATCH | `PG_VALID_ENCODING(enc)` via `types_wchar::encoding::pg_valid_encoding`; invalid → `ERRCODE_UNDEFINED_OBJECT` "%d is not a valid encoding code". |
| `to_ascii_default` (156) | MATCH | `GetDatabaseEncoding()` via `backend-utils-mb-mbutils-seams::get_database_encoding` (owner mbutils unported → seam). |
| `ascii_safe_strlcpy` (174) | MATCH | `destsiz==0` → no-op; writes ≤ `destsiz-1` bytes; stops at first NUL in src; keeps printable ASCII `32..=127` and `\n`/`\r`/`\t`, all else → `'?'`; NUL-terminates. Infallible (must not ereport — runs in postmaster). |

## Constants / SQLSTATEs

- `RANGE_128 = 128`, `RANGE_160 = 160` — match `ascii.c:38-39`.
- `ERRCODE_FEATURE_NOT_SUPPORTED` (0A000), `ERRCODE_UNDEFINED_OBJECT` (42704),
  `ERRCODE_OUT_OF_MEMORY` (53200) — all match the C `ereport` errcodes.
- Encoding ids `PG_LATIN1=8`, `PG_LATIN2=9`, `PG_LATIN9=16`, `PG_WIN1250=29`
  from `types-wchar` (verified vs `mb/pg_wchar.h`).

## Memory / failure surface

The only allocation is the owned `text` result (`palloc`-into-caller analog),
grown OOM-safely with `try_reserve_exact` against the validated input length;
OOM → `ERRCODE_OUT_OF_MEMORY`. The fmgr-boundary `RefPayload::Varlena` is a
global-allocator `Vec<u8>`, so no `Mcx` handle is threaded.

## Seams

- Consumes `common-encnames-seams::{pg_char_to_encoding, pg_encoding_to_char}`
  and `backend-utils-mb-mbutils-seams::get_database_encoding` (all owners
  unported; loud panic until they land).
- Owns NO inward seam crate. `ascii_safe_strlcpy` is the only externally-used
  symbol; its sole consumer (`backend-postmaster-bgworker`) keeps a private,
  signature-matched const-generic copy (pre-existing, faithful to C) rather than
  calling across a cycle — so no `backend-utils-adt-ascii-seams` crate and no
  `init_seams()`/seams-init line is required.
- The SQL `to_ascii_*` PGFunction registry wiring is deferred (workspace-wide
  bare-word PGFunction registry gate), mirroring other adt crates.

## Result

PASS. No `todo!`/`unimplemented!`. 13 unit tests pass. Workspace `cargo check`,
`no-todo-guard`, `seams-init` all green; full `cargo test --workspace` clean
except the allowlisted flakes (range_pair_*, gram-core LALR).
