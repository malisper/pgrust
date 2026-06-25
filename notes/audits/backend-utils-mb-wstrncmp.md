# Audit: backend-utils-mb-wstrncmp

- C source: `src/backend/utils/mb/wstrncmp.c` (postgres-18.3)
- c2rust reference: `c2rust-runs/backend-utils-mb-wstrncmp/src/wstrncmp.rs`
- Port: `crates/backend-utils-mb-wstrncmp/src/lib.rs`
- Audit date: 2026-06-12

## Function inventory

`wstrncmp.c` defines exactly three functions; the c2rust rendering contains
the same three. No statics, no inline helpers, no `#if` branches.

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `pg_wchar_strncmp` (wstrncmp.c:41) | `lib.rs::pg_wchar_strncmp` | MATCH | |
| `pg_char_and_wchar_strncmp` (wstrncmp.c:56) | `lib.rs::pg_char_and_wchar_strncmp` | MATCH | |
| `pg_wchar_strlen` (wstrncmp.c:71) | `lib.rs::pg_wchar_strlen` | MATCH | |

## Detail

### pg_wchar_strncmp

C: early `return 0` for `n == 0`; do-while comparing up to `n` wchars;
mismatch returns `*s1 - *(s2 - 1)` (unsigned subtraction, truncated to int);
a matching zero wchar breaks with 0; loop runs at most `n` iterations.

Port: `n == 0` guard, then a zip over both `as_slice_with_nul()` slices with
`enumerate`; mismatch returns `wchar1.wrapping_sub(wchar2) as i32` (identical
to the C `unsigned int` subtraction narrowed to `int`); `wchar1 == 0` returns
0; `idx + 1 == n` returns 0 after the n-th matching pair — exactly the C
do-while's `--n != 0` exit. Both slices are guaranteed to contain a zero
(`PgWCharStr::from_slice`), and the loop resolves no later than the index of
the earlier zero, so zip exhaustion is unreachable. MATCH.

### pg_char_and_wchar_strncmp

Same shape with `const char *s1`. The C casts through `unsigned char` on
**both** the comparison and the returned difference (zero-extension — note
the deliberate asymmetry with `wstrcmp.c`, which sign-extends in its
comparison). c2rust confirms: `*s1 as c_uchar as pg_wchar` in both places.
Port uses `PgWChar::from(byte)` for both, i.e. zero-extension for both.
NUL break (`*s1++ == 0`) maps to `byte == 0 => return 0`; the `n` bound maps
to `idx + 1 == n`. MATCH.

### pg_wchar_strlen

C: scan to the first zero wchar, return the count. Port:
`PgWCharStr::len()`, which is `position(|&w| w == 0)` over the slice — the
index of the first zero, identical to the C pointer difference. The
`unwrap_or(len)` fallback is unreachable for a validly constructed
`PgWCharStr` (constructor requires a zero). Verified by test
`wchar_strlen_counts_before_nul` (interior zero, trailing data). MATCH.

## Seam audit

- The crate declares no seams; `wstrncmp.c` calls no external functions
  (leaf unit), so none are needed.
- `init_seams()` exists, contains nothing, and is invoked by
  `seams-init::init_all()` (crates/seams-init/src/lib.rs:10). No `set()`
  calls anywhere. No findings.

## Verdict

**PASS** — 3/3 functions MATCH; no seam findings. `cargo test --workspace`
green (6 tests in this crate).
