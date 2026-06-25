# Audit: backend-utils-adt-regexp

Unit: `backend-utils-adt-regexp` (`src/backend/utils/adt/regexp.c`)
Crates: `backend-utils-adt-regexp`, `backend-utils-adt-regexp-seams`,
`backend-regex-core-seams`, `types-regex`.

Sources audited independently:
- C: `postgres-18.3/src/backend/utils/adt/regexp.c` (2082 lines)
- c2rust: `c2rust-runs/backend-utils-adt-regexp/src/regexp.rs`
- Port: `crates/backend-utils-adt-regexp/src/lib.rs`

## Inventory

C defines 50 named functions plus 2 inline static helpers reachable only
inside the file (`RE_wchar_execute`, `RE_execute`) — but those two are
themselves named statics, so the c2rust rendering enumerates 53 function
bodies total. Every one has a port counterpart. All `_no_*` opr_sanity
shim variants are present.

## Per-function table

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| RE_compile_and_cache (141) | lib.rs:98 | MATCH | Self-organizing MRU cache; scan-from-front match on (len,cflags,collation,bytes), move-to-front, compile-on-miss, evict-at-32, insert-at-0. thread_local cache (per-backend global per AGENTS.md). Engine state owned behind RegexHandle; eviction frees via pg_regfree seam (C's MemoryContextDelete). Insert-failure path frees the just-compiled handle (C gets this from child-of-current context unwind). |
| RE_wchar_execute (282) | lib.rs:199 | MATCH | pg_regexec seam; Matched/NoMatch/Failed arms; Failed → ERRCODE_INVALID_REGULAR_EXPRESSION "regular expression failed". |
| RE_execute (324) | lib.rs:220 | MATCH | mb2wchar then RE_wchar_execute at start_search 0. |
| RE_compile_and_execute (358) | lib.rs:230 | MATCH | nmatch<2 (pmatch.len()<2) ⇒ REG_NOSUB; compile+execute. |
| parse_re_flags (385) | lib.rs:254 | MATCH | All 14 option chars + default error; bit set/clear identical; 'm'|'n' merged; default error via pg_mblen_range. |
| nameregexeq (459) | lib.rs:339 | MATCH | REG_ADVANCED, nmatch 0. |
| nameregexne (473) | lib.rs:344 | MATCH | negation. |
| textregexeq (487) | lib.rs:349 | MATCH | |
| textregexne (501) | lib.rs:354 | MATCH | |
| nameicregexeq (522) | lib.rs:359 | MATCH | REG_ADVANCED\|REG_ICASE. |
| nameicregexne (536) | lib.rs:364 | MATCH | |
| texticregexeq (550) | lib.rs:369 | MATCH | |
| texticregexne (564) | lib.rs:374 | MATCH | |
| textregexsubstr (583) | lib.rs:380 | MATCH | 2 pmatch slots; re_nsub>0 ⇒ subexpr[1] else whole[0]; so<0\|\|eo<0 ⇒ NULL; text_substr(so+1, eo-so) via seam. |
| textregexreplace_noopt (642) | lib.rs:423 | MATCH | REG_ADVANCED, search 0, n 1, via replace_text_regexp seam. |
| textregexreplace (658) | lib.rs:434 | MATCH | leading-digit ⇒ invalid-option error + HINT; parse flags; n = glob?0:1. |
| textregexreplace_extended (700) | lib.rs:472 | MATCH | start>0/n>=0 validation; n deduced from glob when absent; start-1. Optional args modeled as Option (C PG_NARGS). |
| textregexreplace_extended_no_n (745) | lib.rs:526 | MATCH | delegates with n=None. |
| textregexreplace_extended_no_flags (752) | lib.rs:540 | MATCH | delegates with flags=None. |
| similar_escape_internal (768) | lib.rs:561 | MATCH | Full SIMILAR-TO→ARE transform: prefix `^(?:`, suffix `)$`, escape-double-quote part separators (`){1,1}?(` / `){1,1}(?:`, >2 ⇒ error), multibyte slow path, bracket-class parsing with charclass_pos states 1/2/3, %→.* _→. (→(?: and \.^$ escaping. Default escape `\`, empty escape ⇒ none, >1-char escape validated via pg_mbstrlen_with_len. |
| similar_to_escape_2 (1078) | lib.rs:742 | MATCH | Some(esc). |
| similar_to_escape_1 (1094) | lib.rs:752 | MATCH | None ⇒ default escape. |
| similar_escape (1112) | lib.rs:761 | MATCH | Non-strict: NULL pat ⇒ NULL; NULL esc ⇒ default. |
| regexp_count (1138) | lib.rs:777 | MATCH | start>0; reject user 'g' then force glob; nmatches. |
| regexp_count_no_start (1181) | lib.rs:806 | MATCH | |
| regexp_count_no_flags (1188) | lib.rs:817 | MATCH | |
| regexp_instr (1198) | lib.rs:828 | MATCH | start/n/endoption(0\|1)/subexpr validation; pos = (n-1)*npatterns (+subexpr-1) *2 (+endoption); match_locs[pos]>=0 ? +1 : 0; n>nmatches or subexpr>npatterns ⇒ 0. |
| regexp_instr_no_start (1291) | lib.rs:919 | MATCH | |
| regexp_instr_no_n (1298) | lib.rs:929 | MATCH | |
| regexp_instr_no_endoption (1305) | lib.rs:940 | MATCH | |
| regexp_instr_no_flags (1312) | lib.rs:952 | MATCH | |
| regexp_instr_no_subexpr (1319) | lib.rs:975 | MATCH | |
| regexp_like (1329) | lib.rs:999 | MATCH | reject 'g'; RE_compile_and_execute nmatch 0. |
| regexp_like_no_flags (1357) | lib.rs:1018 | MATCH | |
| regexp_match (1367) | lib.rs:1034 | MATCH | reject 'g' (+HINT); setup w/ subpatterns; nmatches==0 ⇒ NULL; one build_regexp_match_result. elems/nulls workspace allocated in build fn (behavior-identical restructuring). |
| regexp_match_no_flags (1403) | lib.rs:1062 | MATCH | |
| regexp_matches (1413) | lib.rs:1079 | MATCH (materialized SRF) | Does NOT reject 'g'. Materializes one row per match; per-call setup+build retained and public for a value-per-call driver. Equivalent to SRF_RETURN_NEXT loop. |
| regexp_matches_no_flags (1462) | lib.rs:1101 | MATCH | |
| setup_regexp_matches (1488) | lib.rs:1153 | MATCH | wchar conv; REG_NOSUB unless subpatterns; npatterns/pmatch_len from re_nsub; array_len 255/31 grow 2^n-1 with MaxAllocSize(0x3fffffff)/4 limit ⇒ ERRCODE_PROGRAM_LIMIT_EXCEEDED; degenerate filter `so<wide_len && eo>prev_match_end`; subpattern vs whole-match location capture; maxlen tracking (incl. fetching_unmatched); zero-length advance +1; trailing match_locs[array_idx]=wide_len; wide_str kept only when eml>1. i64 offsets truncated to i32 on store (matches C int match_locs). conv_buf subsumed by conversion seam (maxlen has no further consumer). |
| build_regexp_match_result (1692) | lib.rs:1315 | MATCH | loc = next_match*npatterns*2; per subpattern so/eo; <0 ⇒ NULL element; multibyte ⇒ wchar2mb(wide+so, eo-so); single-byte ⇒ text_substr(so+1, eo-so). Array construction (construct_md_array, TEXTOID) deferred to fmgr; returns element list. |
| regexp_split_to_table (1748) | lib.rs:1357 | MATCH (materialized SRF) | reject 'g' then force glob; setup(false,true,true); loop next_match<=nmatches (nmatches+1 rows). |
| regexp_split_to_table_no_flags (1801) | lib.rs:1385 | MATCH | |
| regexp_split_to_array (1812) | lib.rs:1398 | MATCH | same matching; accumArrayResult/makeArrayResult deferred to fmgr; returns element list. |
| regexp_split_to_array_no_flags (1851) | lib.rs:1426 | MATCH | |
| build_regexp_split_result (1863) | lib.rs:1438 | MATCH | startpos = next_match>0 ? match_locs[next_match*2-1] : 0 (<0 ⇒ "invalid match ending position"); endpos = match_locs[next_match*2] (<startpos ⇒ "invalid match starting position"); multibyte wchar2mb vs single-byte text_substr. |
| regexp_substr (1904) | lib.rs:1476 | MATCH | start/n>0/subexpr>=0; n>nmatches or subexpr>npatterns ⇒ NULL; pos math identical; so/eo<0 ⇒ NULL; text_substr(so+1, eo-so). |
| regexp_substr_no_start (1992) | lib.rs:1556 | MATCH | |
| regexp_substr_no_n (1999) | lib.rs:1566 | MATCH | |
| regexp_substr_no_flags (2006) | lib.rs:1577 | MATCH | |
| regexp_substr_no_subexpr (2013) | lib.rs:1589 | MATCH | |
| regexp_fixed_prefix (2025) | lib.rs:1609 | MATCH | REG_ADVANCED(+ICASE)\|REG_NOSUB; pg_regprefix seam: NoMatch⇒None, Prefix⇒(str,false), Exact⇒(str,true), Failed⇒ERRCODE_INVALID_REGULAR_EXPRESSION; wchar2mb back to db encoding. maxlen/Assert sizing subsumed by seam allocation. |

## Constants verified against headers

- Compile flags REG_BASIC..REG_BOSONLY in `types-regex` (octal) vs
  `regex/regex.h` lines 180-194: all exact.
- SQLSTATEs in `types-error` vs `errcodes.txt`: INVALID_ESCAPE_SEQUENCE
  22025, INVALID_PARAMETER_VALUE 22023, INVALID_REGULAR_EXPRESSION 2201B,
  INVALID_USE_OF_ESCAPE_CHARACTER 2200C, PROGRAM_LIMIT_EXCEEDED 54000: all exact.
- MAX_CACHED_RES 32 vs C 32; MaxAllocSize 0x3fffffff vs `mcx::MAX_ALLOC_SIZE`
  0x3FFF_FFFF: exact.
- pg_regoff_t = long (i64) → `RegMatch.rm_so/rm_eo: i64`; match_locs i32 store
  reproduces C's `int match_locs` truncation.

## Seam audit

Outward seams (all genuine cross-subsystem deps, thin marshal+delegate):
- `backend-regex-core-seams`: pg_regcomp / pg_regexec / pg_regprefix /
  pg_regfree — the Spencer regex engine (`backend/regex/*`), a separate unit;
  compiled state crosses as opaque `RegexHandle` (inherited opacity for
  `re_guts`, not invented). No logic in the seam path.
- `backend-utils-adt-varlena-seams`: text_substr / replace_text_regexp —
  varlena.c's replace_text_regexp itself calls back into RE_compile_and_cache,
  a real cycle. Thin delegations.
- `backend-utils-mb-mbutils-seams`: pg_mb2wchar_with_len / pg_wchar2mb_with_len
  / pg_mblen_range / pg_mbstrlen_with_len / pg_database_encoding_max_length —
  the multibyte subsystem. Thin.

Inward seams (`backend-utils-adt-regexp-seams`): RE_compile_and_cache,
RE_compile_and_execute, regexp_fixed_prefix — consumed by varlena.c,
like_support.c, jsonpath_exec.c (cycles). All three installed by
`init_seams()` (lib.rs:1680), which contains only `set()` calls.
`seams-init::init_all` calls it (`crates/seams-init/src/lib.rs:37`). No seam
left uninstalled; no `set()` outside the owner. No branching/node
construction/computation inside any seam path.

No function body was replaced by a "somewhere else" seam call: replace and
array construction are genuinely other units' logic; all in-file logic lives
in this crate.

## Design conformance

- All allocating functions take `Mcx` and return `PgResult`. ✓
- Cache statics (RegexpCacheMemoryContext / re_array / num_res) modeled as a
  `thread_local!` (per-backend global per AGENTS.md "Backend-global state"),
  not a shared static. ✓
- No invented opacity (RegexHandle is inherited engine opacity), no
  registry-shaped side tables, no locks across `?`, no unledgered divergence
  markers. ✓

## Build / tests

`cargo build`/`cargo test -p backend-utils-adt-regexp`: clean; 14 tests pass
(cache hit/eviction-frees, operators, count/instr/substr selection,
matches materialization, split, similar_to escape translation, flag parsing,
fixed-prefix, inward-seam installation).

## Verdict: PASS

Every function MATCH (the two SRFs materialized, fmgr/Datum/array marshaling
deferred to the fmgr layer per the unit notes; per-call pieces preserved and
public). Zero seam findings. Zero design-conformance findings.
