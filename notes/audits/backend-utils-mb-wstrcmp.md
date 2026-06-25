# Audit: backend-utils-mb-wstrcmp

- C source: `src/backend/utils/mb/wstrcmp.c` (postgres-18.3)
- c2rust reference: `c2rust-runs/backend-utils-mb-wstrcmp/src/wstrcmp.rs`
- Port: `crates/backend-utils-mb-wstrcmp/src/lib.rs`
- Audit date: 2026-06-12

## Function inventory

`wstrcmp.c` defines exactly one function; the c2rust rendering contains the
same single function plus the `pg_wchar` typedef. No statics, no inline
helpers, no `#if` branches.

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `pg_char_and_wchar_strcmp` (wstrcmp.c:41) | `lib.rs::pg_char_and_wchar_strcmp` | MATCH | See detail below. |

## Detail: pg_char_and_wchar_strcmp

C logic: advance while `(pg_wchar) *s1 == *s2++`; if the matching `*s1` is
NUL return 0; on mismatch return `*(const unsigned char *) s1 - *(s2 - 1)`.

Subtleties verified against the C, the header, and the c2rust rendering:

- `pg_wchar` is `unsigned int` (`src/include/mb/pg_wchar.h:28`); port uses
  `PgWChar = u32` from the `types` crate. MATCH.
- The loop comparison casts the **signed** `char` to `pg_wchar`
  (sign-extending; c2rust: `*s1 as pg_wchar` with `c_char = i8` on the
  reference darwin target). Port replicates with `(byte as i8) as PgWChar`.
  MATCH (e.g. byte 0xE9 equals wchar 0xFFFFFFE9; covered by test
  `high_byte_matches_sign_extended_wchar`).
- The mismatch return uses the **unsigned** byte (`*(unsigned char *) s1`),
  promoted to `int`, converted to `unsigned int` for the subtraction with
  `pg_wchar`, result truncated to `int`. Port: `PgWChar::from(byte)
  .wrapping_sub(wchar) as i32`. MATCH (test
  `high_byte_mismatch_returns_unsigned_difference` checks 0xE9 vs 0xEA = -1).
- Termination: the C loop ends only at a mismatch (returns difference) or at
  a matching NUL in `s1` (returns 0). The port zips `s1`'s bytes-with-nul with
  `s2.as_slice_with_nul()`. Both inputs are guaranteed to contain a NUL
  (`CStr`; `PgWCharStr::from_slice` requires a zero), and at the index of the
  first NUL in either input the loop necessarily resolves (matching NULs
  return 0; otherwise a mismatch returns the difference), so the zip can
  never be exhausted first — the trailing `0` is dead code, and the port can
  never read past where the C would. MATCH.

## Seam audit

- The crate declares no seams; `wstrcmp.c` calls no external functions
  (leaf unit), so none are needed.
- `init_seams()` exists, contains nothing, and is invoked by
  `seams-init::init_all()` (crates/seams-init/src/lib.rs:9). No `set()`
  calls anywhere. No findings.

## Verdict

**PASS** — 1/1 functions MATCH; no seam findings. `cargo test --workspace`
green (7 tests in this crate).
