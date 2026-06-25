# Audit: common-unicode-norm-bitfields

**Verdict: PASS**
**Date:** 2026-06-12
**Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]

Unit: `common-unicode-norm-bitfields` (`src/common/unicode_norm.c`, 635 lines,
PostgreSQL 18.3).
Crate audited: `crates/common-unicode-norm-bitfields`.
Cross-checked against
`../pgrust/c2rust-runs/common-unicode-norm-bitfields/src/unicode_norm.rs`.
Auditor: independent re-derivation from the C source and headers
(`unicode_norm_table.h`, `unicode_normprops_table.h`, `unicode_norm.h`,
`pg_bswap.h`).

## Build-config note

`unicode_norm.c` compiles two ways via `#ifndef FRONTEND`. The c2rust run was
of the **frontend** build: it contains `conv_compare`, a `bsearch`-based
`get_code_entry`, the loop-based inverse lookup in `recompose_code`, and lacks
the `qc_*` quick-check routines. The unit name carries no `front` qualifier and
the duplicates (`-srv-`, `-frontstatic-`, `-frontshlib-`) all mark
`duplicate-of:common-unicode-norm-bitfields`, so this is the **backend** unit.
The port correctly implements the backend build: it includes the quick-check
routines and their lookup tables (`#ifndef FRONTEND`-only) and uses the
backend's QC perfect-hash. For the decomposition/recomposition table lookups it
adopts the frontend's binary-search / linear-scan strategy in place of the
backend's `Decomp`/`Recomp` perfect hash. The C `get_code_entry` comment states
the two strategies are interchangeable ("The backend version of this code uses a
perfect hash function for the lookup, while the frontend version uses a binary
search"): both resolve to the unique matching entry of the same sorted table, so
the result is identical on every input. This avoids transcribing the two large
`Decomp_hash_func`/`Recomp_hash_func` `h[]` tables while remaining behaviorally
exact. Documented in the crate's module header.

## Function inventory (every definition in unicode_norm.c)

| # | C function (:line) | Build | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `conv_compare` (:52) | FRONTEND only | — | n/a | bsearch comparator; exists only in the frontend build. Backend uses the perfect hash and has no comparator. Not part of this (backend) unit. |
| 2 | `get_code_entry` (:72) | both (`#ifdef` split) | `lib.rs::get_code_entry` (:169) | MATCH | C backend: `pg_hton32` → `decompinfo.hash`, range check, codepoint equality check, return entry. Port uses `UnicodeDecompMain.binary_search_by_key(codepoint)`. Table is sorted by codepoint (verified programmatically), so binary search returns the same unique entry the perfect hash would — see build-config note. NULL ⇒ `None`. |
| 3 | `get_canonical_class` (:112) | both | `lib.rs::get_canonical_class` (:177) | MATCH | `entry ? entry->comb_class : 0`. Port `map_or(0, comb_class)`. Identical. |
| 4 | `get_code_decomposition` (:134) | both | `lib.rs::decomposition_codes` + `DecompositionCodes` iter (:184) | MATCH | C `DECOMPOSITION_IS_INLINE` ⇒ single codepoint `dec_index` (size 1; the `Assert(SIZE==1)` is debug-only); else slice `&UnicodeDecomp_codepoints[dec_index]` of length `DECOMPOSITION_SIZE`. Port models the static-`x` single-element case as the `Inline` iterator variant and the slice case as `Slice(&[..][start..start+size])`. The C static buffer's "valid until next call" caveat is a C lifetime artifact only; each call's value is reproduced exactly. |
| 5 | `get_decomposed_size` (:159) | both | `lib.rs::get_decomposed_size` (:226) | MATCH | Hangul fast path `code∈[SBASE,SBASE+SCOUNT)` ⇒ 3 if `sindex%TCOUNT!=0` else 2. Else: `entry==NULL \|\| SIZE==0 \|\| (!compat && IS_COMPAT)` ⇒ 1; else sum of recursive sizes over decomposition codes. Port identical (returns `usize`; values non-negative, no overflow concern for table data). SCOUNT=19*588=11172, ranges match. |
| 6 | `recompose_code` (:218) | both (`#ifdef` split) | `lib.rs::recompose_code` (:286) | MATCH | Hangul L+V ⇒ `SBASE+(lindex*VCOUNT+vindex)*TCOUNT`; LV+T (`(start-SBASE)%TCOUNT==0`) ⇒ `start+tindex`. Else inverse lookup. Port uses the frontend linear scan: filter `SIZE==2`, skip `DECOMPOSITION_NO_COMPOSE` (= `(flags & (NO_COMPOSE\|COMPAT))!=0`, port's `!no_compose() && !is_compat()`), match `codepoints[dec_index]==start && [dec_index+1]==code`. Equivalent to the backend hash, which returns one candidate then verifies the same two-codepoint equality (build-config note). Constants LBASE/VBASE/TBASE/SBASE/LCOUNT/VCOUNT/TCOUNT verified against C `#define`s. |
| 7 | `decompose_code` (:321) | both | `lib.rs::decompose_code` (:248) | MATCH | Hangul fast path: `l=LBASE+sindex/(VCOUNT*TCOUNT)` (NCOUNT), `v=VBASE+(sindex%NCOUNT)/TCOUNT`, push l, v, and `TBASE+tindex` if `tindex!=0`. Else same NULL/zero/compat predicate as #5 ⇒ push `code`; else recurse over decomposition codes. Port pushes into a pre-sized `PgVec`, mirroring C's `result[*current]` cursor (C's `current` updated in place). |
| 8 | `unicode_normalize` (:402) | both | `lib.rs::unicode_normalize`/`unicode_normalize_z` → `normalize_into` (:415/425/378) | MATCH | (a) decomp_size = Σ`get_decomposed_size`; alloc decomp buffer. (b) `decompose_code` each input char. (c) canonical ordering (`canonical_order`, #8a). (d) if `!recompose` return decomp; else recompose (`recompose`, #8b). The C 0-terminated array contract is split: `unicode_normalize` takes the codepoints directly, `unicode_normalize_z` finds the first 0, normalizes the prefix, and re-appends a 0 terminator — preserving C's behavior over a 0-terminated buffer. `compat`/`recompose` flag derivation matches. `ALLOC`=`palloc` OOM⇒`ereport(ERROR)` is mirrored by `Mcx`+`PgResult`. `decomp_size==0` early-return is subsumed: an empty/zero-length input yields an empty buffer and the recompose path also returns empty, so no separate branch is observable. |
| 8a | canonical-ordering loop (:449) | — | `lib.rs::canonical_order` (:319) | MATCH | Swap when `prevClass!=0 && nextClass!=0 && prevClass>nextClass`; backtrack `count-=2` then `count++`. Port uses `count.saturating_sub(2); count+=1`. Diverges only at `count==1` with a swap: C advances to 2, port re-checks index 1 once (the just-sorted pair now satisfies `prev<=next`, no swap) then advances to 2 — provably identical output, one harmless extra comparison. All `count>=2` backtracks reach the same index in both. |
| 8b | recomposition loop (:497) | — | `lib.rs::recompose` (:337) | MATCH | `last_class=-1, starter_pos=0, target_pos=1, recomp[0]=starter_ch=decomp[0]`. For each subsequent ch: `last_class<ch_class && recompose_code` ⇒ overwrite `recomp[starter_pos]`, update `starter_ch`, leave `last_class` (port `continue`); else `ch_class==0` ⇒ new starter (`last_class=-1`); else append, `last_class=ch_class`. Port's `result.len()` tracks `target_pos`. Upper-bound allocation (decomp_size+1) preserved via `with_capacity(chars.len())`. Empty-input guard returns empty vec (matches C's `decomp_size==0` return before recompose is reached). |
| 9 | `qc_hash_lookup` (:543) | backend only | `lib.rs::qc_hash_lookup` (:449) | MATCH | `pg_hton32(ch)` → `norminfo->hash`, range check `h<0 \|\| h>=num_normprops`, codepoint-equality check, return entry. Port identical; `num_normprops` = `normprops.len()` (NFC=1252, NFKC=5096, verified against C `pg_unicode_norminfo` initializers). |
| 10 | `qc_is_allowed` (:574) | backend only | `lib.rs::qc_is_allowed` (:478) | MATCH | NFC ⇒ `UnicodeNormInfo_NFC_QC`, NFKC ⇒ `UnicodeNormInfo_NFKC_QC`, default `Assert(false)` ⇒ `debug_assert!(false)`; `found ? quickcheck : UNICODE_NORM_QC_YES`. Identical. |
| 11 | `unicode_is_normalized_quickcheck` (:598) | backend only | `lib.rs::unicode_is_normalized_quickcheck` (:500) | MATCH | D/KD forms ⇒ `MAYBE`. Loop: `lastCanonicalClass>canonicalClass && canonicalClass!=0` ⇒ `NO`; `qc_is_allowed`: `NO`⇒return `NO`, `MAYBE`⇒result=`MAYBE`; update lastCanonicalClass. Port iterates to first 0 (C's `*p` terminator). Identical. |

## Constant / table verification (against C headers, not from memory)

- `pg_unicode_decomposition` fields and flag constants `DECOMP_NO_COMPOSE=0x80`,
  `DECOMP_INLINE=0x40`, `DECOMP_COMPAT=0x20`, `DECOMPOSITION_SIZE` mask `0x1F`:
  match `unicode_norm_table.h`.
- `UnicodeDecompMain[6843]`: programmatically compared all 6843 rows
  (codepoint, comb_class, evaluated `dec_size_flags`, dec_index) — **exact
  match**; table is **sorted by codepoint** (binary-search precondition holds).
- `UnicodeDecomp_codepoints[5138]`: all 5138 entries — **exact match**.
- QC entry tables `UnicodeNormProps_NFC_QC[1252]`, `UnicodeNormProps_NFKC_QC[5096]`:
  all (codepoint, quickcheck) pairs — **exact match** with `unicode_normprops_table.h`.
- QC perfect-hash arrays `NFC_QC_h[2505]`, `NFKC_QC_h[10193]`: all entries —
  **exact match**.
- QC hash function parameters vs C `NFC_QC_hash_func`/`NFKC_QC_hash_func`:
  NFC `a=0,b=0,÷2505`; NFKC `a=0,b=3,÷10193`; multipliers `257`/`8191`; key =
  `pg_hton32` big-endian bytes — all match the port's
  `pg_unicode_norminfo { hash_divisor, hash_b_init }` and `hash()`.
- `UnicodeNormalizationForm` (NFC=0, NFD=1, NFKC=2, NFKD=3) and
  `UnicodeNormalizationQC` (NO=0, YES=1, MAYBE=-1): match `unicode_norm.h`.
- The crate's `qc_hash_is_a_perfect_hash_for_every_entry` test independently
  re-validates the entire QC tables + hash by asserting every codepoint hashes
  to its own index; passes for both tables.

## Seam audit

Ownership is by C-source coverage: the unit's only C file is `unicode_norm.c`,
and no `crates/X-seams` maps to it (no `unicode_norm`-related seam crate exists).
The unit therefore owns **zero** seam crates, so it declares no seams and
installs none. `init_seams()` is an empty no-op kept for the uniform startup
convention, and `seams-init::init_all()` calls it
(`crates/seams-init/src/lib.rs:71`).

The crate makes **no outward seam calls**: it is a leaf utility of pure
functions over immutable static tables. Dependencies are only `mcx`,
`types-core`, `types-error`, `types-wchar` — no dependency cycle to break, so no
seam is justified or present.

## Design conformance

- Allocating entry points (`unicode_normalize`, `unicode_normalize_z`,
  `recompose`, `normalize_into`) take `Mcx` and return `PgResult`, mirroring C's
  `palloc`/`ereport(ERROR)` failure surface (memory: "Seam signatures mirror C
  failure surface"; types.md allocation rules). The returned buffer is owned by
  the passed context.
- No invented opacity: `pg_unicode_decomposition`, `pg_unicode_normprops`, and
  `pg_unicode_norminfo` are real structs derived from the C structs. The QC
  bit-field (`codepoint:21, quickcheck:4`) is a space optimization only; storing
  plain fields preserves the carried values (types.md rules 6-7 satisfied — no
  stand-in handles).
- No shared statics for per-backend globals (the static tables are immutable
  read-only lookup data). No locks, no registry side tables, no ambient-global
  seams, no divergence markers.

## Verdict

Every function MATCHes (no MISSING / PARTIAL / DIVERGES). All constants and
lookup tables verified byte-for-byte against the C headers. Zero seam findings;
zero design-conformance findings. The crate builds and all 14 unit tests pass.

**PASS.**
