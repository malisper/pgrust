# Audit: common-keywords

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** port/common-keywords
- **Unit C sources:** `src/common/keywords.c`, `src/common/kwlookup.c`
  (+ generated `kwlist_d.h`, header inline `src/include/common/kwlookup.h`)
- **Port crate:** `crates/common-keywords` (build.rs-generated table; category
  enum in `crates/types-core/src/keywords.rs`)

This audit is independent: the C, the c2rust rendering
(`c2rust-runs/common-batch9/src/{keywords,kwlookup}.rs`), and the Rust port were
each re-read and compared from scratch.

## 1. Function / data inventory

Enumerated from the C sources cross-checked against the c2rust run. The unit is
small: one real function (`ScanKeywordLookup`), one header inline
(`GetScanKeyword`), the build-time generated perfect-hash function
(`ScanKeywords_hash_func`), and the static data tables.

| # | C symbol | C location | Kind | Port location | Verdict | Notes |
|---|----------|-----------|------|---------------|---------|-------|
| 1 | `ScanKeywordLookup` | kwlookup.c:37 | function | lib.rs:120 `ScanKeywordLookup` | MATCH | Idiomatic, behavior-identical (see §2). |
| 2 | `GetScanKeyword` | kwlookup.h:38 (static inline) | inline | lib.rs:111 `GetScanKeyword` / `ScanKeywordList::keyword` (lib.rs:84) | MATCH | Returns n-th keyword text; bounds-checked. |
| 3 | `ScanKeywords_hash_func` | kwlist_d.h (generated) | static fn | subsumed into `ScanKeywordLookup` via direct keyword scan | MATCH (subsumed) | Perfect hash is an internal index-selection optimization; its only observable effect is which candidate is char-compared. Replaced by a direct case-insensitive scan that yields the identical accept/reject result on every input. See §2 analysis. |
| 4 | `ScanKeywords` (ScanKeywordList) | keywords.c via kwlist_d.h | static data | lib.rs:96 `ScanKeywords` | MATCH | Built from `kwlist.h`; `num_keywords=494`, `max_kw_len=17`. |
| 5 | `ScanKeywords_kw_string` / `_kw_offsets` | kwlist_d.h (generated) | static data | generated `SCAN_KEYWORDS_KW_STRING` / `_KW_OFFSETS` (build.rs) | MATCH | Same NUL-separated blob + offsets, regenerated from identical `kwlist.h`. |
| 6 | `ScanKeywordCategories[]` | keywords.c:29 | static data | lib.rs:104; generated `SCAN_KEYWORD_CATEGORIES` | MATCH | Per-keyword category from `kwlist.h` field 3; enum discriminants 0/1/2/3 match the C `#define`s. |
| 7 | `ScanKeywordBareLabel[]` | keywords.c:42 | static data | lib.rs:107; generated `SCAN_KEYWORD_BARE_LABEL` | MATCH | `BARE_LABEL`=true / `AS_LABEL`=false from `kwlist.h` field 4. |

No other function definitions exist in the unit (c2rust `kwlookup.rs` defines
only `ScanKeywordLookup` + the inline `GetScanKeyword`; `keywords.rs` is all
generated data + the hash function).

## 2. ScanKeywordLookup — behavioral equivalence

C control flow (kwlookup.c):
1. `len = strlen(str)`; if `len > max_kw_len` return -1.
2. `h = hash(str, len)` — perfect hash over the keyword set.
3. if `h < 0 || h >= num_keywords` return -1.
4. char-by-char compare downcased input (`'A'-'Z'`→`'a'-'z'` only, *not*
   `tolower`) against `GetScanKeyword(h)`; mismatch → -1; trailing-NUL check; else
   return `h`.

Port (lib.rs:120):
1. `bytes = c_string_prefix(str)` (truncate at first NUL = C `strlen` semantics);
   if `bytes.len() > max_kw_len` return -1.
2. `index = scan_keywords_hash_bytes(bytes)` — linear scan returning the index of
   the keyword whose bytes equal the ASCII-A-Z-downcased input, else -1.
3. if `index < 0 || index >= num_keywords` return -1.
4. re-verify with `ascii_keyword_eq` (length + per-byte A-Z-downcased equality);
   return `index` or -1.

The C accept predicate is exactly: input downcased over `'A'..'Z'` equals the
stored (already-lowercase) keyword bytes with matching length. The perfect hash
merely selects the single candidate to test; for a true keyword it selects the
right one, for a non-keyword it either lands out of range or on a candidate that
fails the char compare. The keyword blob is a set of distinct lowercase strings,
so an ASCII-case-insensitive match is unique. The port computes the same accept
predicate directly and returns the same unique index, -1 otherwise — provably
identical output for every input (including overlong, embedded-NUL, non-letter,
and high-bit bytes, which all map to "no match" the same way under both). The
A-Z-only downcasing (Turkish-locale concern from the C comment) is preserved by
`ascii_downcase` (lib.rs:182), which does not use locale `tolower`.

Dropping `ScanKeywords_hash_func` is therefore a behavior-preserving idiomatic
restructure of an internal optimization, not absent logic: the function's
observable contract (return matching keyword index else -1) lives fully in this
crate.

## 3. Seams and wiring

Per "ownership is by C-source coverage": the owned seam crates would be any
`crates/X-seams` where `X` maps to `keywords.c` or `kwlookup.c`. **None exist**
(`crates/*keyword*seams*` / `*kwlook*seams*` → no matches), and no other seam
crate references `ScanKeyword`/`kwlookup`. This is a leaf crate: `Cargo.toml`
depends only on `types-core`, with no cyclic dependency requiring a seam, and
consumers depend on it directly. The seam-coverage requirement is vacuously
satisfied (no owned seam crates, hence no uninstalled declarations and no
`init_seams()` required). **Zero seam findings.**

## 3b. Design conformance

- **No invented opacity** (types.md 6-7): `ScanKeywordList` is a real struct over
  borrowed `&'static` slices; `KeywordCategory` is a real four-variant enum with
  discriminants matching the C `#define`s — no stand-in handles or void* hacks.
- **No allocation:** lookup and accessors are pure reads over `'static` data; no
  `Mcx`/`PgResult` needed because nothing allocates or can `ereport(ERROR)`. The
  C `ScanKeywordLookup` cannot error either (returns -1), so the plain `i32`
  return mirrors the C failure surface correctly.
- **No shared mutable statics:** the data is immutable `static` (C `const`); no
  per-backend global modeled as a shared static.
- **No ambient-global seams, no locks, no registry side tables, no unledgered
  divergence markers.**
- **Data-table integrity:** `crates/common-keywords/kwlist.h` is byte-identical
  to `postgres-18.3/src/include/parser/kwlist.h` (verified by `diff`); the
  generated 494-entry table, offsets, categories and bare-label flags are
  re-derived from it at build time rather than transcribed.

## 4. Verdict

**PASS.** Every C function is MATCH (the generated perfect hash is subsumed into
a behavior-identical lookup). Zero seam findings; zero design-conformance
findings. `cargo test -p common-keywords` builds clean and all 6 tests pass.

CATALOG row set to `audited`.
