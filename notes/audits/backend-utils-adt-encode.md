# Audit: backend-utils-adt-encode

Crate: `crates/backend-utils-adt-encode`
C source: `src/backend/utils/adt/encode.c` (PostgreSQL 18.3)
Reference: `../pgrust/src-idiomatic/crates/backend-utils-adt-encode`
Date: 2026-06-15
Result: PASS (no logic divergences)

## Scope

Owner crate created (the repo previously had only `backend-utils-adt-encode-seams`).
Function-by-function comparison against the C and the src-idiomatic port.

## Function-by-function

| C function | Rust | Verdict |
|---|---|---|
| `pg_find_encoding` | `pg_find_encoding` | MATCH — scans the `enclist[]` static dispatch table (`ENCLIST`, the faithful C static array) with `pg_strcasecmp` modeled by `pg_strcasecmp_ascii` (NUL-terminated ASCII case-insensitive; identical to `pg_strcasecmp` for the fixed ASCII names `hex`/`base64`/`escape`). |
| `binary_encode` (PG_FUNCTION_ARGS) | `binary_encode_bytes(mcx, dataptr, name)` | MATCH — `pg_find_encoding` -> `unrecognized_encoding` (22023); `encode_len`; `resultlen > MaxAllocSize - VARHDRSZ` -> `conversion_too_large` (54000); palloc-analog `PgVec` zeroed in caller `mcx`; `encode`; `res > resultlen` -> FATAL `overflow - encode estimate too small`; truncate to true length. The `PG_GETARG_*`/`SET_VARSIZE`/`PG_RETURN_*` fmgr envelope is the deferred bare-word PGFunction registry (project-wide). |
| `binary_decode` (PG_FUNCTION_ARGS) | `binary_decode_bytes(mcx, dataptr, name)` | MATCH — symmetric; `decode_len` is fallible so a lone-backslash escape error surfaces from the length call before allocation, exactly as the C `esc_dec_len` `ereport` longjmps before `palloc`. |
| `hex_encode` | `hex_encode` | MATCH — `hextbl[512]` table copied verbatim; returns `len*2`. |
| `get_hex` (static inline) | `get_hex` | MATCH — `hexlookup[128]` verbatim; `c < 127` guard; `Some`/`None` mirror `*out`/`true`/`false`. |
| `hex_decode` | `hex_decode` | MATCH — wraps `hex_decode_safe(.., None)`. |
| `hex_decode_safe` | `hex_decode_safe` | MATCH — whitespace skip; `ereturn(escontext, 0, ...)` for invalid digit / odd digits (22023) via `SoftErrorContext` + `ereturn`; `(v1<<4)|v2`. |
| `hex_enc_len` (static) | `hex_enc_len` | MATCH — `srclen << 1`. |
| `hex_dec_len` (static) | `hex_dec_len` | MATCH — `srclen >> 1`. |
| `pg_base64_encode` (static) | `pg_base64_encode` | MATCH — `_base64[64]` alphabet verbatim; `pos`/`buf` bit packing; 76-column newline wrap; `=` padding. |
| `pg_base64_decode` (static) | `pg_base64_decode` | MATCH — `b64lookup[128]` verbatim; `c > 0 && c < 127` signed-char guard; `=` end-sequence states; 22023 unexpected `=` / invalid symbol / invalid end (+ hint). |
| `pg_base64_enc_len` (static) | `pg_base64_enc_len` | MATCH — `(srclen+2)/3*4 + srclen/(76*3/4)` byte-for-byte integer division. |
| `pg_base64_dec_len` (static) | `pg_base64_dec_len` | MATCH — `(srclen*3) >> 2`. |
| `esc_encode` (static) | `esc_encode` | MATCH — `\0`/high-bit -> `\nnn` octal, `\` -> `\\`, else passthrough; `DIG(VAL)`/`VAL(CH)`. |
| `esc_decode` (static) | `esc_decode` | MATCH — `\###` octal decode, `\\` -> `\`, lone backslash -> 22P02 `invalid input syntax for type bytea`. |
| `esc_enc_len` (static) | `esc_enc_len` | MATCH — counts 4/2/1 per byte. |
| `esc_dec_len` (static) | `esc_dec_len` | MATCH — same scan as `esc_decode`, errors 22P02 on a lone backslash before allocation. |

## Tables / constants verified against C

- `hextbl[512]`, `hexlookup[128]` — byte-for-byte.
- `_base64[64]` alphabet, `b64lookup[128]` — byte-for-byte.
- SQLSTATEs: `ERRCODE_INVALID_PARAMETER_VALUE` (22023), `ERRCODE_PROGRAM_LIMIT_EXCEEDED` (54000), `ERRCODE_INVALID_TEXT_REPRESENTATION` (22P02).
- Error message strings match the C source verbatim (incl. the base64 end-sequence hint).
- `VARHDRSZ = 4`; `MaxAllocSize` -> `mcx::MAX_ALLOC_SIZE` (0x3FFFFFFF).

## Reconciliation notes (model, not logic, divergences)

- Error construction uses the repo `PgError::error(..).with_sqlstate(..)` builder and `PgError::new(FATAL, ..)` (per CLAUDE.md: `elog(FATAL)` in owned logic -> `Err` at FATAL level), in place of src-idiomatic's `ereport(..).errcode()..into_error()` chain. Same level/sqlstate/text.
- `binary_*` return a context-charged `PgVec<'mcx, u8>` allocated in the caller's `Mcx` (the `palloc` analog), replacing src-idiomatic's per-call MemoryContext + `FmgrArg`/`FmgrOut` Option-4 boundary. Truncation to the true length replaces `SET_VARSIZE`.
- `pg_mblen_range` crosses `backend-utils-mb-mbutils-seams`. The repo seam is infallible (`fn(&[u8]) -> i32`, clamped to the slice end), so `mb_snippet` is infallible — there is no fallible arg-evaluation path where a truncated multibyte lead byte's encoding error "wins" over the hex/base64 message. This matches the repo's seam contract; the offending byte is always present at the error site so the clamp never truncates the snippet improperly.

## Seams

- Installed (owned by this unit): `hex_encode`, `hex_decode_safe`
  (`backend-utils-adt-encode-seams`), both allocate-and-return `PgVec` in the
  caller `Mcx`. Consumed by `backend-utils-adt-varlena` (`byteain`/`byteaout`).
  Wired via `init_seams()` into `seams-init`.
- Outward: `pg_mblen_range` (`backend-utils-mb-mbutils-seams`).

## No todo!/unimplemented! / no own-logic stubs

Confirmed: every function fully implemented; the only deferral is the bare-word
PGFunction fmgr registry (project-wide), exposed as the `*_bytes` cores ready for
the fmgr layer.

## Tests

20 unit tests pass (hex/base64/escape round-trips and error paths, dispatch
case-insensitivity, binary_encode/decode cores, soft-error collection, the two
installed seam bodies).
