# Audit: common-saslprep

C unit: `src/common/saslprep.c` (SASLprep / RFC 4013, for SCRAM authentication).
Independent function-by-function audit vs the C and the c2rust rendering.

## Per-function table

| C function (saslprep.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `codepoint_range_cmp` (968) | folded into `is_code_in_table` closure, lib.rs | MATCH | C `*key<range[0]→-1`, `>range[1]→1`, else 0. Port `binary_search_by` comparator returns `Greater` when `code<first` — correct element-vs-key ordering. |
| `is_code_in_table` / `IS_CODE_IN_TABLE` (982/966) | `is_code_in_table` lib.rs | MATCH | Same out-of-range fast reject; `(u32,u32)` tuples replace flat pairs. |
| `pg_utf8_string_len` (1002) | merged into `pg_utf8_string_to_codepoints` (validate-count pass) | MATCH | `pg_utf_mblen`→`pg_utf_mblen_private`, `len<l \|\| !pg_utf8_islegal`→same; `None` = -1 = INVALID_UTF8. |
| `pg_saslprep` (1046) | `pg_saslprep` lib.rs | MATCH | All 4 steps + ASCII short-circuit + empty-reject; see traps below. |
| `utf8_to_unicode` (re-impl from pg_wchar.h) | `utf8_to_unicode` lib.rs | MATCH | Byte layout identical; invalid→0xffffffff. |
| `unicode_to_utf8` (re-impl from pg_wchar.h) | `unicode_to_utf8` lib.rs | MATCH | Identical masks/shifts; 1-4 byte branches. |
| `pg_utf_mblen` leading-byte (re-impl from wchar.c) | `utf8_leading_len` lib.rs | MATCH | Exact leading-byte length table. |

## Fidelity traps (all clear)

1. Prohibit (step 3) and bidi (step 4) iterate the MAPPED codepoints (`mapped`), not the
   normalized `output_chars` — matches C iterating `input_chars` post-mapping.
2. Final UTF-8 re-encode iterates the NORMALIZED `output_chars`.
3. Pure-ASCII short-circuits (raw high-bit test, returns input verbatim) before the
   empty-reject and prohibited/bidi checks — matches C `pg_is_ascii`+STRDUP.
   Empty-after-mapping → `Ok(None)` (= `goto prohibited`).
4. All 6 range tables referenced in the correct steps; tables independently verified as
   exact transcriptions (counts 6/8/36/396/34/360, zero value mismatches).
5. Allocating function is fallible: `PgResult<Option<Vec<u8>>>`; `try_reserve` →
   `mcx.oom(..)`, `unicode_normalize(mcx, …)?`. Matches palloc/ereport(ERROR) on OOM.
   C's explicit `MaxAllocSize/sizeof` pre-check is subsumed by `try_reserve` failing.
6. `unicode_normalize(UNICODE_NFKC, mapped)` — legitimate cross-crate call into the
   already-landed `common-unicode-norm-bitfields` (not a seam).

## Seam audit (clean)

- `backend-libpq-auth-scram-seams::pg_saslprep` is installed exactly once by
  `common_saslprep::init_seams()` (cross-crate install — saslprep.c's owner installs the
  saslprep seam even though the seam crate is named for the scram unit; correct per
  ownership-by-C-source-coverage). `seams-init` calls `common_saslprep::init_seams()`.
- Installer closure is a thin marshal (scratch `MemoryContext` → `pg_saslprep` → return
  `Option`). The `Err(_) → None` degrade is faithful: the seam can't carry an error, and
  the consumer (`pg_be_scram_build_secret`, `scram_verify_plain_password`) falls back to
  the raw password on `None` — matching C, where any non-SUCCESS rc proceeds with the raw
  password.

## Design conformance

Allocating function takes `Mcx` + returns `PgResult`; no shared statics, no locks across
`?`, no registry side-tables, no opacity inventions.

## Verdict: PASS

Every function MATCH; zero seam findings; all fidelity traps clear. Merge-eligible.
