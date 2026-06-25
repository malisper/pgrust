# Audit: backend-utils-adt-cash

Unit: `backend-utils-adt-cash` (c_source `*/cash.c`)
Branch: `port/backend-utils-adt-cash`
Sources audited: `../pgrust/postgres-18.3/src/backend/utils/adt/cash.c` (1195 lines),
`../pgrust/c2rust-runs/backend-utils-adt-cash/src/cash.rs`,
`crates/backend-utils-adt-cash/src/lib.rs`, `crates/types-cash/src/lib.rs`.

Verdict: **PASS** (independent re-audit, 2026-06)

## Function inventory (every definition in cash.c)

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | append_num_word (static) | :38 | lib.rs:921 | MATCH | small[28]/big=small+18 tables exact; all 6 branches (<=20, even-100, >99 trio, <=99 trio) reproduced; `tu=value%100` computed before the `value` re-typing. |
| 2 | cash_pl_cash (static inline) | :90 | lib.rs:154 | MATCH | pg_add_s64_overflow -> "money out of range" / 22003. |
| 3 | cash_mi_cash (static inline) | :103 | lib.rs:164 | MATCH | pg_sub_s64_overflow path. |
| 4 | cash_mul_float8 (static inline) | :116 | lib.rs:174 | MATCH | rint(float8_mul); isnan||!FITS check; float8_mul via float seam. |
| 5 | cash_div_float8 (static inline) | :129 | lib.rs:186 | MATCH | rint(float8_div); same guard. |
| 6 | cash_mul_int64 (static inline) | :142 | lib.rs:198 | MATCH | pg_mul_s64_overflow. |
| 7 | cash_div_int64 (static inline) | :155 | lib.rs:208 | MATCH | i==0 -> division by zero / 22012, else c/i. |
| 8 | cash_in | :172 | lib.rs:355 | MATCH | See detailed notes below. |
| 9 | cash_out | :386 | lib.rs:547 | MATCH | See detailed notes below. |
| 10 | cash_recv | :591 | lib.rs:726 (doc) | SEAMED/N-A | Body is pure `pq_getmsgint64`; no cash logic. fmgr+pqformat wire layer wraps the raw Cash. No own logic dropped. |
| 11 | cash_send | :602 | lib.rs:726 (doc) | SEAMED/N-A | Pure `pq_begintypsend`/`pq_sendint64`/`pq_endtypsend`; no cash logic. |
| 12 | cash_eq | :617 | lib.rs:746 | MATCH | c1==c2 |
| 13 | cash_ne | :626 | lib.rs:751 | MATCH | |
| 14 | cash_lt | :635 | lib.rs:756 | MATCH | |
| 15 | cash_le | :644 | lib.rs:761 | MATCH | |
| 16 | cash_gt | :653 | lib.rs:766 | MATCH | |
| 17 | cash_ge | :662 | lib.rs:771 | MATCH | |
| 18 | cash_cmp | :671 | lib.rs:776 | MATCH | >:1, ==:0, else -1. |
| 19 | cash_pl | :689 | lib.rs:791 | MATCH | |
| 20 | cash_mi | :702 | lib.rs:796 | MATCH | |
| 21 | cash_div_cash | :715 | lib.rs:801 | MATCH | divisor==0 -> 22012; (f64)dividend / (f64)divisor. |
| 22 | cash_mul_flt8 | :735 | lib.rs:810 | MATCH | |
| 23 | flt8_mul_cash | :748 | lib.rs:815 | MATCH | delegates cash_mul_float8(c,f), arg order swapped to match C. |
| 24 | cash_div_flt8 | :761 | lib.rs:820 | MATCH | |
| 25 | cash_mul_flt4 | :774 | lib.rs:825 | MATCH | (float8) f widening. |
| 26 | flt4_mul_cash | :787 | lib.rs:830 | MATCH | |
| 27 | cash_div_flt4 | :801 | lib.rs:835 | MATCH | |
| 28 | cash_mul_int8 | :814 | lib.rs:840 | MATCH | |
| 29 | int8_mul_cash | :827 | lib.rs:845 | MATCH | |
| 30 | cash_div_int8 | :839 | lib.rs:850 | MATCH | |
| 31 | cash_mul_int4 | :852 | lib.rs:855 | MATCH | (int64) i widening. |
| 32 | int4_mul_cash | :865 | lib.rs:860 | MATCH | |
| 33 | cash_div_int4 | :879 | lib.rs:865 | MATCH | |
| 34 | cash_mul_int2 | :892 | lib.rs:870 | MATCH | (int64) s widening. |
| 35 | int2_mul_cash | :904 | lib.rs:875 | MATCH | |
| 36 | cash_div_int2 | :917 | lib.rs:880 | MATCH | |
| 37 | cashlarger | :929 | lib.rs:885 | MATCH | |
| 38 | cashsmaller | :944 | lib.rs:894 | MATCH | |
| 39 | cash_words | :960 | lib.rs:974 | MATCH | See detailed notes below. |
| 40 | cash_numeric | :1050 | lib.rs:1075 | MATCH | fpoint guard; int64_to_numeric; scale-factor loop; round-divisor-first trick (numeric_round then numeric_div then numeric_round) reproduced on the on-disk numeric image. numeric_round/_div are real numeric-owner kernels called directly. |
| 41 | numeric_cash | :1106 | lib.rs:1112 | MATCH | scale loop; numeric_mul; numeric_int8 (reconstructed faithfully, see below). |
| 42 | int4_cash | :1140 | lib.rs:1134 | MATCH | scale loop; int8mul overflow-checked core. |
| 43 | int8_cash | :1170 | lib.rs:1151 | MATCH | scale loop; int8mul overflow-checked core. |

Plus the C macro/inline cores reproduced in-crate: pg_{add,sub,mul}_s64_overflow,
pg_abs_s64, FLOAT8_FITS_IN_INT64, rint, isspace/isdigit on (unsigned char), int8mul
core (int8.c), numeric_int8_opt_error reconstruction (numeric.c).

## Detailed re-derivations of the non-trivial functions

### cash_in (:172)
- lconv preamble identical: frac_digits clamp to [0,10]->else 2; dsymbol = single-byte
  mon_decimal_point else '.'; ssymbol = mon_thousands_sep else ("," unless dsymbol==','
  then "."); csymbol/psymbol/nsymbol with $/+/- fallbacks. Checked field-for-field.
- Leading-whitespace/currency/sign/whitespace/currency/whitespace stripping order exact.
- Accumulation loop: digit consumed only while `!seen_dot || dec < fpoint`; value built in
  the NEGATIVE (pg_mul ...,10 then pg_sub digit); dec++ only after seen_dot; dsymbol sets
  seen_dot once; thousands-sep skipped. C `s += strlen(ssymbol)-1` (with for-loop `s++`)
  == port `s += ssymbol.len()` (while loop, no implicit increment); ssymbol is never empty
  so `.max(1)` is a no-op. else -> break. Exact.
- Round-off `isdigit && *s>='5'` -> pg_sub value,1 (negative build). Exact.
- `for (; dec<fpoint; dec++) pg_mul value,10`. Exact.
- Skip trailing digits; trailing-tail loop accepts space/')'/nsymbol(set sgn=-1)/psymbol/
  csymbol else INVALID 22P02. Exact, incl. error SQLSTATEs (22003 overflow, 22P02 syntax).
- Sign flip: sgn>0 and value==INT64_MIN -> overflow error; else result=-value; else result=value.
- ereturn soft-error semantics modeled via `errsave`/`ereturn` returning Ok(0) under a
  SoftErrorContext, hard Err otherwise. Matches C `ereturn(escontext, (Datum)0, ...)`.

### cash_out (:386)
- points clamp identical; mon_group clamp <=0||>6 -> 3.
- dsymbol/ssymbol/csymbol identical; sign fields chosen by value<0 (negative_sign else "-",
  n_*) vs >=0 (positive_sign, p_*).
- uvalue = pg_abs_s64(value).
- Right-to-left digit build: decimal point inserted when `points && digit_pos==0`; thousands
  sep when `digit_pos<0 && digit_pos%mon_group==0` (C and Rust both truncate-toward-zero on
  signed %, so -3%3==0). digit then uvalue/=10, digit_pos--. do/while `(uvalue||digit_pos>=0)`
  == loop break on `uvalue==0 && digit_pos<0`. Buffer reversed. Exact.
- sign_posn switch: cases 0,2,3,4 reproduced verbatim; case 1 and C `default` folded into the
  `_` arm. Each branch's csymbol/signsymbol/bufstr ordering and sep_by_space==1 / ==2 spacing
  verified against the five psprintf templates. Exact.

### cash_words (:960)
- value<0: C `value=-value` then `(uint64)value` recovers magnitude at INT64_MIN; port uses
  wrapping_neg then `as u64` -> identical (INT64_MIN -> 9223372036854775808), prepends "minus ".
- dollars/m0..m6 divisors (100, 100, 100000, 1e8, 1e11, 1e14, 1e17) %1000 exact.
- m6..m1 emission with " quadrillion "/" trillion "/.../" thousand " suffixes; m1 bare.
- dollars==0 -> "zero"; " dollar and "/" dollars and " on dollars==1; cents word; m0==1 ->
  " cent" else " cents". Exact.
- capitalize_first: pg_toupper on first byte (always an ASCII letter here) via the real
  port-pgstrcasecmp pg_toupper; in-place ASCII byte swap preserves UTF-8. Matches C
  `buf.data[0]=pg_toupper(...)`.

### numeric_int8 reconstruction (numeric.c:4664 numeric_int8_opt_error)
Cross-checked against numeric.c: NUMERIC_IS_SPECIAL first -> NaN "cannot convert NaN to
bigint" / Inf "cannot convert infinity to bigint", both 0A000 (FEATURE_NOT_SUPPORTED);
then init/set_var_from_num + numericvar_to_int64 (rounds to nearest internally); !ok ->
"bigint out of range" 22003. Reconstructed from the numeric owner's audited public kernels
(set_var_from_num, numericvar_to_int64) — the rounding logic stays numeric-owned, only the
wrapper/error-mapping lives here, matching the C wrapper. No logic dropped.

## Seam / wiring audit

- Owned inward seam crates: **none**. No `crates/backend-utils-adt-cash-seams` exists; cash.c
  declares no seams that other crates consume. The unit is a pure consumer. The
  `backend-utils-adt-pg-locale-seams` crate is owned by the locale subsystem, not cash.
- Therefore cash exports no `init_seams()` and is correctly absent from `seams-init::init_all()`.
  Both recurrence_guard tests (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`) PASS.
- Outward seam calls, each a thin marshal+delegate justified by a real unported owner:
  - `pglc_localeconv` (pg_locale.c owner unported) -> CashLconv snapshot.
  - `float8_mul` / `float8_div` (float owner unported) -> single call each, rint+range-check
    stay in-crate (pure inline macros, correctly NOT pushed across the seam).
  - numeric kernels (int64_to_numeric, numeric_mul/_div/_round, set_var_from_num,
    numericvar_to_int64): numeric owner IS ported, called directly (no seam).
- No own-logic stubs, no todo!()/unimplemented!(), no deferral panics. cash_recv/cash_send
  are pure pqformat wire glue with zero cash-specific logic (documented, not faked).

## Design conformance
- No invented opacity: CashLconv is a faithful working snapshot of the lconv subset
  (scalar `char`->i8, strings owned), explicitly not an ABI struct. Cash = i64 (utils/cash.h).
- Allocating paths (cash_numeric, numeric_cash, int4/int8_cash via numeric) carry `Mcx` and
  return `PgResult`. No shared statics, no ambient globals, no locks, no registry side tables.

## Gates
- `cargo test -p backend-utils-adt-cash`: 13 passed.
- `cargo test -p seams-init`: 2 passed (both recurrence guards).
- `cargo check --workspace`: clean (only pre-existing warnings in unrelated crates).
