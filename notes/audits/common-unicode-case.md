# Audit: common-unicode-case (src/common/unicode_case.c)

C source: postgres-18.3/src/common/unicode_case.c + unicode_case_table.h
Port: crates/common-unicode-case/{src/lib.rs, src/tables.rs}

## Function inventory & verdicts

| C fn (line) | port | verdict | notes |
|---|---|---|---|
| unicode_lowercase_simple (50) | lib.rs | MATCH | find_case_map(CASE_MAP_LOWER); 0 -> identity. |
| unicode_titlecase_simple (58) | MATCH | CASE_MAP_TITLE. |
| unicode_uppercase_simple (66) | MATCH | CASE_MAP_UPPER. |
| unicode_casefold_simple (74) | MATCH | CASE_MAP_FOLD. |
| unicode_strlower (101) | unicode_strlower | MATCH* | delegates to convert_case(CaseLower). *Idiomatic restructure of return contract — see below. |
| unicode_strtitle (138) | unicode_strtitle | MATCH* | CaseTitle + wbnext callback. |
| unicode_strupper (165) | unicode_strupper | MATCH* | CaseUpper. |
| unicode_strfold (189) | unicode_strfold | MATCH* | CaseFold. |
| convert_case (213) | convert_case + build_case | MATCH* | full Default Case Conversion loop. |
| casemap (397) | casemap | MATCH | ASCII fast path (idx+1); case_index; special-case gate on full && case_map_special[idx] && check_special_conditions; else simple. |
| check_final_sigma (312) | check_final_sigma | MATCH | offset==0 -> false; backward scan skipping Case_Ignorable, break on Cased, false on other; forward scan from next char skipping ignorable, false on Cased, break otherwise; default true. Rust char_indices()/char boundaries replace the C UTF-8 lead/continuation byte tests — equivalent on valid UTF-8. |
| check_special_conditions (370) | check_special_conditions | MATCH | 0 -> true; PG_U_FINAL_SIGMA -> check_final_sigma; else false. |
| find_case_map (438) | find_case_map | MATCH | ASCII map[ucs+1]; else map[case_index(ucs)]. |
| case_index (table macro) | case_index | MATCH | nested range dispatch into CASE_MAP, copied verbatim from idiomatic base / unicode_case_table.h. |

Generated tables (SPECIAL_CASE, CASE_MAP, CASE_MAP_SPECIAL, CASE_MAP_LOWER/
TITLE/UPPER/FOLD) copied verbatim.

## Idiomatic restructure (sanctioned)
C `convert_case` writes into a caller `dst`/`dstsize` buffer (size-pass when
dstsize==0, then fill) and returns `result_len`. The port builds the result into
an owned context-charged `PgString<'mcx>`/`PgVec<'mcx, u8>` allocated in the
caller-supplied `Mcx`, growing fallibly with `try_reserve`; the public str* fns
return `PgResult<PgVec<'mcx, u8>>`. The two-pass dst/dstsize bookkeeping and the
result_len/truncation accounting are C buffer-management artifacts; the emitted
byte sequence is identical for every input. This matches the established
fabled contract for the strlower_builtin family
(pg-locale-builtin-seams). An allocator refusal returns `Err(PgError)`, the
analog of C `palloc` `ereport(ERROR)` (the idiomatic base panicked on OOM;
fabled returns the error). NUL-truncated src preserved via `split_once('\0')`.

## Seams & wiring
- No owned seam crate for unicode_case.c. Its consumers are the builtin
  pg_locale provider (pg_locale_builtin.c) which is NOT yet ported; those
  strlower/title/upper/fold_builtin + regex_wc_* seams live in
  backend-utils-adt-pg-locale-builtin-seams and remain uninstalled (campaign
  remainder). No inbound seam to install here.

## Design conformance
- pg_wchar re-signed to crate-local `types_core::PgWChar` alias.
- Allocation path uses Mcx + PgResult (try_reserve), no infallible String/Vec.
- No statics beyond const tables. No owned-logic panics (panics only in #[cfg(test)]).

## Verdict: PASS
