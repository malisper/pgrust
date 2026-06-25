# Audit: backend-utils-adt-numutils

- C source: `src/backend/utils/adt/numutils.c` (postgres-18.3, 1312 lines)
- c2rust rendering: `c2rust-runs/backend-utils-adt-numutils/src/numutils.rs`
- Port: `crates/backend-utils-adt-numutils/src/lib.rs`
- Auditor: independent re-derivation from C + c2rust; fix round 1 applied
  (see Findings), fixed function family re-audited from scratch with a
  differential fuzz against a verbatim C transcription.

## Function inventory

The C file defines exactly 17 functions (2 static inline helpers + 15
externs) plus two static data tables (`DIGIT_TABLE[200]`, `hexlookup[128]`).
Cross-checked against the c2rust rendering, which contains the same 17 plus
inlined header helpers (`isdigit`/`isspace`/`isxdigit` ctype shims,
`pg_neg_u{16,32,64}_overflow`, `pg_leftmost_one_pos{32,64}` from
`common/int.h` / `port/pg_bitutils.h`) — those are accounted for as port
helpers, not separate inventory rows. The only `#if` in the file is
`#if PG_UINT32_MAX != ULONG_MAX` in `uint32in_subr` (active on LP64, present
in the c2rust rendering, ported).

| C function (numutils.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `decimalLength32` (44, static inline) | `decimal_length32` (47) | MATCH | `t = (msb_index+1)*1233/4096; t + (v >= PowersOfTen[t])`. `pg_leftmost_one_pos32` = `31 - leading_zeros`; all 10 powers-of-ten table entries verified digit-by-digit. Callers guard `v != 0`, same as C. |
| `decimalLength64` (63, static inline) | `decimal_length64` (60) | MATCH | Same formula, 20-entry u64 table verified (`1` .. `10^19`). |
| `pg_strtoint16` (121) | `pg_strtoint16` (94) | MATCH | Delegates to the `_safe` variant with NULL/`None` escontext, exactly as C. |
| `pg_strtoint16_safe` (127) | `pg_strtoint16_safe` (101) -> `parse_signed::<i16>` (171) | MATCH (after fix) | See "parse_signed equivalence" below. typname `"smallint"`; `\|MIN\|` = 32768, MAX = 32767. Differentially fuzzed (2M random strings over an adversarial alphabet + boundary corpus) against a verbatim transcription of the C fast+slow paths: 0 mismatches on value and error kind. |
| `pg_strtoint32` (382) | `pg_strtoint32` (106) | MATCH | Thin delegate. |
| `pg_strtoint32_safe` (388) | `pg_strtoint32_safe` (111) -> `parse_signed::<i32>` | MATCH (after fix) | typname `"integer"`; bounds 2147483648 / 2147483647 (from `<$ty>::MIN.unsigned_abs()` / `<$ty>::MAX`, the same values as `PG_INT32_MIN/MAX`). Boundary corpus checked for all four bases incl. `-0x80000000`, `0x80000000`(range), `0x80000000z`(invalid), `0b1<<31`, `0o20000000000`. |
| `pg_strtoint64` (643) | `pg_strtoint64` (116) | MATCH | Thin delegate. |
| `pg_strtoint64_safe` (649) | `pg_strtoint64_safe` (121) -> `parse_signed::<i64>` | MATCH (after fix) | typname `"bigint"`; bounds 9223372036854775808 / ...807. Boundary corpus incl. `-0x8000000000000000`, `0x8000000000000000`(range) and `...z`(invalid), `9223372036854775808x`(invalid) vs `9223372036854775810x`(range — per-digit guard fires). |
| `uint32in_subr` (897) | `uint32in_subr` (299) -> `parse_unsigned` (337) | MATCH | Models `strtoul(s, &endptr, 0)` + the surrounding checks. Differentially fuzzed (1M strings x {u32,u64} x {endloc,not}) against the platform's real `strtoul` wrapped in a verbatim transcription of the C: 0 mismatches on value, error kind, and endptr offset. Covers: base-0 detection (0x/0X hex only when a hex digit follows, bare-`0x` backtrack leaving endptr at the `x`; bare leading `0` octal incl. lone `"0"`; else decimal), leading isspace skip, `+`/`-` sign with modulo-2^64 negation, EINVAL/no-digit => invalid_syntax (endptr==s ⟺ no digits consumed), ERANGE (>2^64-1) => out_of_range, endloc tail vs trailing-whitespace-only check (checked *before* the narrowing check, same order as C), and the `#if PG_UINT32_MAX != ULONG_MAX` narrowing: accept cvt iff it equals the u32 result after zero- or sign-extension to 64 bits, else out_of_range (so `-1` => 0xFFFFFFFF, `-4294967296` => range error). `endloc` out-param rendered as a returned `&str` tail; on the soft-error path C leaves `*endloc` unset and the port returns `(0, "")` — caller must check the context, same contract. |
| `uint64in_subr` (984) | `uint64in_subr` (313) -> `parse_unsigned` | MATCH | `strtou64` = `strtoull`; same fuzz, no narrowing branch (matches C). u128 accumulator with overflow-vs-`u64::MAX` guard = ERANGE predicate; the port stops consuming at overflow where strtoul keeps consuming, but both then report the same out_of_range and discard the tail, so this is unobservable. |
| `pg_itoa` (1041) | `pg_itoa` (462) | MATCH | Widen to i32 and delegate to `pg_ltoa`, exactly as C. |
| `pg_ultoa_n` (1054) | `pg_ultoa_n_into` (470) + `pg_ultoa_n` (577) | MATCH | Zero => `"0"`/len 1; `decimalLength32` sizing; 4-digit chunks via DIGIT_TABLE pair blits, then 2-digit, then 1- or 2-digit head — transcribed index-for-index (`pos = olength - i`, `c0 = (c%100)<<1`, etc.). DIGIT_TABLE verified equal to the C 200-byte table. Fuzzed against `u32::to_string` (2M random values + all power-of-ten boundaries): identical. C caller-buffer+length API rendered as owned `String`. |
| `pg_ltoa` (1119) | `pg_ltoa` (584) | MATCH | `uvalue = (uint32)0 - uvalue` = `0u32.wrapping_sub(value as u32)` (correct for `i32::MIN`); `-` prefix; delegate; NUL terminator dropped (String). Fuzzed vs `i32::to_string`. |
| `pg_ulltoa_n` (1139) | `pg_ulltoa_n_into` (513) + `pg_ulltoa_n` (600) | MATCH | 8-digit chunk loop (`q = value/1e8`, `value3` u32, c/d split), then the u32 tail identical to `pg_ultoa_n` — transcribed exactly. Fuzzed vs `u64::to_string`. |
| `pg_lltoa` (1226) | `pg_lltoa` (607) | MATCH | u64 two's-complement negation + delegate; fuzzed vs `i64::to_string` incl. `i64::MIN`. |
| `pg_ultostr_zeropad` (1266) | `pg_ultostr_zeropad` (625) | MATCH | `value < 100 && minwidth == 2` DIGIT_TABLE shortcut; `len >= minwidth` => unpadded; else left-pad `minwidth - len` zeros (C memmove+memset, port builds the String directly — same output). Fuzzed vs `format!("{:0>w$}")` for w in 1..=12. C `Assert(minwidth > 0)` rendered as an always-on `assert!`; in C the assert is cassert-only, but on minwidth <= 0 the NDEBUG C behavior is only reachable via caller contract violation — acceptable strengthening, noted. |
| `pg_ultostr` (1306) | `pg_ultostr` (654) | MATCH | `pg_ultoa_n` + end pointer; the pointer-bump append idiom becomes returning the String for the caller to concatenate. |

Supporting items: `DIGIT_TABLE` (27) verified byte-for-byte; `hexlookup`
table (88) rendered as the `hexlookup()` match — verified equivalent for all
128 (and, unlike C, all 256) inputs: digits 0-9 => 0-9, `a-f`/`A-F` => 10-15,
everything else None/-1. `is_space` (723) = C-locale `isspace`
(space/\t/\n/\v/\f/\r), matching the ctype shim in the c2rust rendering.
`ereturn` helper (673) reproduces `errsave_start`/`errsave_finish`: with a
context, mark `error_occurred` (details only when `details_wanted`) and
return the error-path value (0); without one, hard error. SQLSTATEs verified
against `errcodes.txt`: 22003 NUMERIC_VALUE_OUT_OF_RANGE, 22P02
INVALID_TEXT_REPRESENTATION; both message format strings match C verbatim.

### parse_signed equivalence (the one nontrivial restructuring)

The C `pg_strtoint{16,32,64}_safe` are three copies of one algorithm with a
base-10 fast path that falls back to a slow path handling whitespace, `+`,
`0x/0o/0b`, and `_` separators. The port collapses them into one generic
implementation of the slow path with a u128 accumulator. Equivalence
re-derived:

- The fast path either (a) falls to the slow path (non-digit lead, or any
  non-NUL terminator), which re-parses from scratch, or (b) completes/errors
  on pure `-?[0-9]+` input — where the slow path's decimal branch performs
  the identical guard-accumulate-check sequence. So slow-path-only is exact.
- Per-digit guard: C `tmp > -(PG_INT*_MIN / base)` = port
  `tmp > T::min_unsigned_abs() / base` (C truncating division of a negative,
  negated). This admits tmp up to `|MIN| + base - 1`, which never wraps the
  C uintN accumulator (max 32783 for u16 hex, etc.) — so u128 == uintN here.
- Error precedence: out_of_range can fire mid-loop (guard); invalid_syntax
  fires for no-digits, bad underscore placement, or trailing junk; the final
  range check (`pg_neg_u*_overflow` when negative = `tmp > |MIN|`; else
  `tmp > MAX`) runs only after the trailing-junk checks. The port now
  reproduces this exact ordering (see Findings).
- Underscore rules: leading `_` rejected only in the decimal branch
  (`ptr == firstdigit` check exists only there — `0x_1` is legal in C and in
  the port); `_` must always be followed by a digit of the active base.
- `T::from_magnitude` is only reached with `tmp <= |MIN|` (neg) or
  `tmp <= MAX` (pos), where it equals C's `pg_neg_u*_overflow` negation /
  plain cast, including the `i*::MIN` two's-complement case.

## Findings (fix round 1)

**DIVERGES (fixed): sign-dependent tight per-digit overflow guard changed the
error kind for overflow-boundary values with trailing junk.** The original
port guarded each digit with `tmp > (limit - digit) / base` where `limit` was
|MIN| or MAX depending on sign — a *tighter* check than C's
`tmp > -(PG_INT*_MIN / base)`. Accepted values were identical, but for inputs
like `32768x`, `-32769_`, `0x8001z` (int16) C admits the final digit and then
reports **invalid_syntax** (22P02) on the trailing junk, while the port
reported **out_of_range** (22003). Confirmed empirically (failing test), then
fixed in `parse_signed_inner`: per-digit guard is now `tmp >
T::min_unsigned_abs() / base` and the real range check moved after the
syntax checks, mirroring C. Re-audited from scratch: regression test
`tests/junk_overflow.rs` plus a 2M-input differential fuzz against a verbatim
transcription of the C int16 fast+slow paths (0 mismatches) and boundary
corpora for i32/i64 in all four bases.

No other findings. (Note, not a finding: `pg_ultostr_zeropad` turns the
cassert-only `Assert(minwidth > 0)` into an unconditional panic on a
contract-violating caller.)

## Seam audit

- `CATALOG.tsv` marks the unit a leaf: deps `types-error` only. Confirmed:
  `Cargo.toml` depends solely on `types-error`; no
  `backend-utils-adt-numutils-seams` crate exists; no `seam-core` usage; no
  outward seam calls anywhere in the crate.
- `init_seams()` is empty (nothing to install) and is still invoked by
  `seams-init::init_all()` (`crates/seams-init/src/lib.rs:18`), so the wiring
  convention holds.
- No `set()` calls outside an owner; no logic hidden behind seams. Zero seam
  findings.

## Verdict

**PASS** (after fix round 1). All 17 functions MATCH; seam audit clean;
`cargo test -p backend-utils-adt-numutils` green (18 tests). Spot-check
re-derivations performed on `pg_ulltoa_n`'s 8-digit chunk arithmetic, the
`uint32in_subr` sign-extension acceptance window, and the DIGIT_TABLE
contents before sign-off.
