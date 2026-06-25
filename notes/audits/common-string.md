# Audit: common-string (`src/common/string.c`)

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** claude-opus-4-8[1m]
- **Branch:** port/common-string-wt
- **Crate:** `crates/common-string` (+ owned seam crate `crates/common-string-seams`)

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Sources read: C `../pgrust/postgres-18.3/src/common/string.c`; c2rust backend
rendering `../pgrust/c2rust-runs/common-batch23/src/string.rs` (non-FRONTEND
build — the `#ifdef FRONTEND` `malloc` branch is compiled out; backend uses
`palloc_extended`); port `crates/common-string/src/lib.rs`.

This audit found and fixed a `DIVERGES` on `strtoint`, then re-derived the fixed
function from scratch against libc `strtol`.

## Function inventory

`string.c` defines exactly 5 functions (no statics, no inline helpers). All 5
appear in the c2rust rendering and the port. No file in this unit's `c_sources`
other than `string.c`, so the only owned seam crate is `common-string-seams`.

| C function | C loc (string.c) | Port loc (lib.rs) | Verdict |
|---|---|---|---|
| `pg_str_endswith` | 29 | `pg_str_endswith` (20) | MATCH |
| `strtoint` | 46 | `strtoint` + internal `strtol` (75) | MATCH (after fix) |
| `pg_clean_ascii` | 81 | `pg_clean_ascii` (35) | MATCH / SEAMED (owner installs) |
| `pg_is_ascii` | 127 | `pg_is_ascii` (62) | MATCH |
| `pg_strip_crlf` | 148 | `pg_strip_crlf` (71) | MATCH |

## Per-function notes

### pg_str_endswith — MATCH
C: `strlen(str)`, `strlen(end)`, `if (elen > slen) return false`, `str += slen -
elen`, `strcmp(str, end) == 0`. Port: `s.as_bytes().ends_with(end.as_bytes())`.
Byte-for-byte equivalent for NUL-terminated inputs (empty `end` → true on both:
C does `strcmp("","")==0`, Rust `ends_with(b"")`→true). Longer-suffix early
return covered by `ends_with`'s length check.

### strtoint — MATCH (after fix; was DIVERGES)
C is `val = strtol(str, endptr, base); if (val != (int) val) errno = ERANGE;
return (int) val;` — a **total** function: it never reports a "no digits" or
"invalid base" *error*; on no conversion `strtol` returns 0 with `endptr = str`
(offset 0) and no `errno`, and the only out-of-band signal is `errno = ERANGE`
when the `long` does not fit `int`. Real PG callers (`nodes/read.c`,
`utils/adt/datetime.c`, `utils/adt/jsonfuncs.c`, …) rely on this: they read the
returned value and inspect `endptr`/`errno` themselves.

**Finding (fixed):** the original port returned `Result<i32, StrToIntError>`
with `NoDigits` / `InvalidBase` / `OutOfRange` error variants. A differential
test against libc `strtol` confirmed the port returned `Err(NoDigits)` for
`"xyz"`, `""`, `"-"`, `"  "`, `"z"`, `"  -"` where C `strtoint` returns `0`
(value), `endptr = str`, no ERANGE — a behavioral divergence (the C function is
total; the port turned non-error outcomes into errors). The error `.end` for the
no-digit case also pointed past leading whitespace instead of at offset 0 as
`strtol` does.

**Fix:** rewrote `strtoint` to mirror C exactly — an internal `strtol(text,
base) -> ParsedLong` emulation (64-bit `long`, clamps overflow to
`i64::MIN`/`i64::MAX` and sets `erange`, no-conversion returns `value 0` /
`end 0`, invalid base returns the `strtol(EINVAL)` outcome `0`/`end 0`), and
`strtoint` returns `StrToInt { value: i32, end: usize, erange: bool }` where
`value` is the wrapping `(int) val` cast and `erange = parsed.erange || (int)val
!= val`. Verified the wrapping cast matches C `(int)` truncation:
`(int)0x80000000 == i32::MIN`, `(int)LONG_MAX == -1`, `(int)LONG_MIN == 0`,
`(int)0x2147483648 == 0x47483648`.

A 35-case differential harness (every prefix/sign/octal/hex-autodetect/overflow/
no-conversion/whitespace/invalid-base edge) now matches libc `strtol` on
`(value, endptr offset, ERANGE)` with **0 mismatches**. Number base handling
(0 auto-detect: `0x`→hex, leading `0`→octal, else decimal; explicit base 16
optional `0x` prefix; C-isspace skip including vertical tab `\x0b`/form feed
`\x0c` which Rust's `is_ascii_whitespace` omits) all verified against libc.

### pg_clean_ascii — MATCH / SEAMED
C worst-case alloc `strlen(str)*4 + 1`; for each byte, if `*p < 32 || *p > 126`
emit `\xXX` via `snprintf("\\x%02x", (unsigned char)*p)` (i += 4) else copy
(i += 1); NUL-terminate. Port iterates `s.as_bytes()`, same predicate
`c < 32 || c > 126`, emits `\`, `x`, two **lowercase** hex nibbles
(`hex_digit(c >> 4)`, `hex_digit(c & 0x0f)` — matches `%02x` lowercase, always
2 digits for a byte) else pushes the byte; builds into a `PgString`. Verified:
`"a\nb\rc\t"` → `a\x0ab\x0dc\x09`, `"~\u{7f}"` → `~\x7f` (0x7f > 126 escaped),
`"\u{80}"` → `\xc2\x80` (UTF-8 bytes escaped individually, matching C's
byte-by-byte pass), empty → empty.

This is the crate's **allocating seam** (`palloc_extended` in the C backend
build). The fallible `mcx` allocation surfaces an allocator refusal as `Err`
(the C `if (!dst) return NULL` / `MCXT_ALLOC_NO_OOM` path); `alloc_flags`
accepted to match the C surface. Allocation in an allocating function with
`Mcx` + `PgResult` — design-conformant (no shared statics, no ambient global).
The C `Assert(i < dstlen)` bounds checks are debug-only and do not affect
behavior; the port's fallible growth subsumes them.

### pg_is_ascii — MATCH
C: `while (*str) if (IS_HIGHBIT_SET(*str)) return false; ... return true`, where
`IS_HIGHBIT_SET(c)` = `((c) & HIGHBIT)`, `HIGHBIT = 0x80`. Port:
`!s.as_bytes().iter().any(|&c| c & 0x80 != 0)`. Same predicate, same per-byte
scan, same default-true.

### pg_strip_crlf — MATCH
C strips trailing `\n`/`\r` in place (`while len>0 && (str[len-1]=='\n' ||
'\r') str[--len]='\0'`) and returns the new length. Port returns
`s.trim_end_matches(['\n', '\r'])`; the trimmed slice's `.len()` is the returned
length. Both stop at the first non-CRLF byte scanning from the end. Verified
`"abc\r\n\r\n"` → `"abc"`, `"a\rb\n"` → `"a\rb"`.

## Seam audit (§3) and design conformance (§3b)

- **Owned seam crates (by C-source coverage):** the unit's only `c_source` is
  `string.c`, so the sole owner-seam crate is `common-string-seams`. (The grep
  for "string.c" also hit `backend-*-seams` crates — those are coincidental
  prose matches ("a string consumer", `store_cstring`), not `src/common/string.c`
  ownership.)
- `common-string-seams` declares exactly one seam: `pg_clean_ascii(mcx, s,
  alloc_flags) -> PgResult<PgString>`. Signature mirrors the C allocating
  failure surface (`Mcx` + `PgResult`) — conformant.
- **Installer:** `common_string::init_seams()` contains only
  `common_string_seams::pg_clean_ascii::set(pg_clean_ascii);` — a single `set()`,
  nothing else. Every owned-seam declaration is installed (1 of 1). No empty
  installer with seams outstanding.
- **Wiring:** `crates/seams-init/src/lib.rs:157` calls
  `common_string::init_seams();`. `seams-init` builds clean.
- **No outward seam calls** from this crate (leaf crate, no dependency cycles) —
  no thin-marshal violations, no logic-in-seam, no function bodies replaced by
  delegation. The single seam is *inward* (this crate provides
  `pg_clean_ascii` to its callers, e.g. `backend-tcop-backend-startup`).
- No invented opacity, no shared statics for per-backend globals, no
  ambient-global seams, no locks across `?`, no registry-shaped side tables, no
  unledgered divergence markers.

## Conclusion

All 5 functions MATCH (one fixed from DIVERGES and re-derived from scratch
against libc). The single owned seam is declared, installed by the owner's
`init_seams()`, and wired into `seams-init::init_all`. Zero seam findings, zero
design-conformance findings. Tests (10) pass; `cargo build -p seams-init` and
`cargo clippy -p common-string` succeed (remaining clippy notes are stylistic:
`c < 32 || c > 126` deliberately mirrors the C predicate; `result_large_err` is
workspace-wide).

**PASS.**
