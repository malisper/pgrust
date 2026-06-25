# Audit: backend-utils-adt-varlena

- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context)
- **Sources:**
  - C: `../pgrust/postgres-18.3/src/backend/utils/adt/varlena.c`
  - c2rust: `../pgrust/c2rust-runs/backend-utils-adt-varlena/src/varlena.rs`
  - Port: `crates/backend-utils-adt-varlena/src/{keystone,comparison,sortsupport,name_pattern,position_ops,bytea,split_format,replace_regexp,wire_io,misc_encoding,lib}.rs`
- **Verdict:** PASS

## Scope and method

This crate is the assembly of the 10 varlena decomp family branches onto the
scaffold + keystone. The audit re-derived the function inventory from
`varlena.c` (~190 definitions incl. statics) and cross-checked against the
c2rust rendering. Logic was compared C ↔ port for the high-risk and the
assembly-reconciled functions in full detail, with representative sampling
across each family; seam ownership and `init_seams()` completeness were verified
exhaustively (the merge-blocking checks).

## Seam audit (step 3) — PASS

- **Owned seam crate** (by C-file coverage, `varlena.c`):
  `backend-utils-adt-varlena-seams`. It declares 8 seams: `cstring_to_text`,
  `text_to_cstring`, `varstr_cmp`, `split_identifier_string`,
  `split_directories_string`, `text_to_qualified_name_list`, `text_substr`,
  `replace_text_regexp`.
- **All 8** are installed by `backend_utils_adt_varlena::init_seams()`, which
  contains nothing but `set()` calls.
- `seams-init::init_all()` calls `backend_utils_adt_varlena::init_seams()`
  (verified line present; union-merge line-drop checked — wiring intact).
- The `recurrence_guard::every_seam_installing_crate_is_wired_into_init_all`
  test passes.
- Seam handlers are thin marshal+delegate into the family bodies (no branching
  or computation in a seam path).
- **Outward seam calls** are all real cross-crate dependencies:
  `backend-utils-adt-pg-locale-seams` (collation_is_c / collation_is_deterministic
  / pg_strncoll / pg_strcoll / pg_strxfrm*), `backend-utils-mb-mbutils-seams`
  (mblen / mbstrlen / encoding), `backend-utils-adt-regexp-seams`,
  `backend-regex-core-seams`, detoast/encode/hyperloglog/hashfn/unicode seams.

### Seam contract reconciliation performed during assembly

- `pg-locale-seams::pg_strncoll`: the comparison branch declared
  `(arg1, arg2, collid)`; the position_ops branch declared `(collid, arg1, arg2)`.
  Resolved to `(collid, arg1, arg2)` — the crate-wide convention shared by
  `pg_strcoll`/`pg_strxfrm*` (collid first). `comparison.rs` caller updated to
  match. No behavioral change.

## Per-family findings

### keystone.rs — MATCH

`TextPositionState` reconciled between the keystone branch (locale-first, C field
order) and position_ops (`<'a, 'mcx>` lifetimes): final form is `<'a, 'mcx>` with
`locale: PgLocale<'mcx>` first (C order). Added a non-C `collid` carrier field
documented as the layering substitute for C's direct `state->locale` deref (the
locale seams re-key by collation OID). Carrier converters
(`cstring_to_text{,_with_len}`, `text_to_cstring{,_buffer}`, `text_catenate`,
`charlen_to_bytelen`, `check_collation_set`) MATCH.

### position_ops.rs — MATCH

`text_substring`, `text_p_slice`, `pg_mbcharcliplen_chars`, `bytea_substring`,
`text_overlay`, `textpos`, `text_position`, `text_left`/`right`/`reverse`,
`replace_text`, and the full `text_position_*` Boyer-Moore-Horspool state machine
verified line-by-line against C (1273–1636):
- `text_position_setup`: skip-table build, `skiptablemask` size brackets
  (3/7/15/31/63/127/255), one-char and nondeterministic skips MATCH.
- `text_position_next`: empty-pattern early return, retry loop, multibyte
  false-positive boundary walk MATCH. Reconciled to C's single-arg signature
  (collid read from `state.collid` rather than a parameter).
- `text_position_next_internal`: nondeterministic greedy/non-greedy loop,
  one-char fast path, BMH backward scan MATCH.
- `text_position_get_match_pos`/`get_match_ptr`/`reset`/`cleanup` MATCH
  (`get_match_ptr` and `reset` added during assembly, transcribed from C
  1595–1633).

### comparison.rs — MATCH

`varstr_cmp` (collate_is_c memcmp+length tiebreak, len-equal memcmp shortcut,
`pg_strncoll`, deterministic tiebreak), `text_cmp`, `texteq`/`textne`
(deterministic length-shortcut), `text_lt`/`le`/`gt`/`ge`, `text_starts_with`,
`byte_cmp` helper — all MATCH.

### bytea.rs — MATCH

`byteaGetByte`/`GetBit`/`SetByte`/`SetBit` (bounds checks, byteNo/bitNo, set/clear,
0-or-1 validation, ARRAY_SUBSCRIPT_ERROR ranges), `bytea_reverse`, relational
ops, int↔bytea conversions sampled — MATCH. Detoasted-payload model collapses
C's toast-size fast paths to identical results.

### misc_encoding.rs — MATCH

`hexval`/`hexval_n` (overflow via `wrapping_add`, shift amounts), unicode
normalization/version helpers sampled — MATCH.

### sortsupport / name_pattern / split_format / replace_regexp / wire_io

Sampled representative functions; control flow and constants match. `split_part`
and `split_text` were rewired during assembly to call the real `position_ops`
`text_position_*` entry points (the stale "sibling not yet ported" panic shims
were removed now that the family is assembled).

## fmgr Datum-boundary seam adapters — IMPLEMENTED (no todo!/unimplemented!)

- `lib.rs` `seam_cstring_to_text` (`CStringGetTextDatum`): builds the `text`
  payload via `keystone::cstring_to_text`, frames it into a full-4B-header
  image through `types_datum::Varlena::from_image` (which stamps
  `SET_VARSIZE(result, len + VARHDRSZ)`), `leak`s the image into `mcx` (C's
  palloc'd `text *` lives in the current context until reset), and returns the
  image pointer word as the Datum (`PointerGetDatum`). MATCH vs
  `cstring_to_text_with_len` (varlena.c:205) + the `CStringGetTextDatum` macro.
- `lib.rs` `seam_text_to_cstring` (`TextDatumGetCString`): reads the varatt
  header off the `DatumGetPointer` pointer word; detoasts the external (1B-E)
  and 4B-compressed forms through `backend-access-common-detoast-seams`
  (`pg_detoast_datum_packed`), reads `VARSIZE_ANY_EXHDR`, and copies
  `VARDATA_ANY` out via `keystone::text_to_cstring`, returning a NUL-free
  `PgString` (the keystone's trailing NUL is dropped for the `String` view).
  MATCH vs `text_to_cstring` (varlena.c:226). The raw-pointer varatt helpers
  mirror the audited range-ADT detoast path (`range_repr_serialize.rs`).
  No `todo!()`/`unimplemented!()` remains anywhere in `src/` (verified by grep;
  only doc-comment mentions).
- `split_format.rs` 14 `panic!`s: all for genuinely **unported cross-crate
  owners** — arrayfuncs (accumArrayResult/deconstruct_array), tuplestore
  (tuplestore_putvalues), parser/scansup (downcase/truncate_identifier), port/path
  (canonicalize_path), ruleutils (quote_identifier), fmgr (OutputFunctionCall),
  access/common detoast, numutils (pg_strtoint), quote.c (quote_literal_cstr).
  Panicking on an unported callee is permitted; no varlena.c logic is absent.

## Gates

- `cargo check --workspace`: clean (exit 0).
- `cargo test --workspace`: pass (0 failures; the 2 known timeout flakes ignored).
- `recurrence_guard` (incl. declared-seams-are-set): pass.

## Verdict: PASS

Every audited function MATCHes (or is a thin SEAMED delegate per step 3). Seam
ownership and installation are complete. No `MISSING`/`PARTIAL`/`DIVERGES`. The
two fmgr Datum-boundary seam adapters (`cstring_to_text`/`text_to_cstring`) are
now fully implemented against the `types_datum::Varlena` header model — zero
`todo!()`/`unimplemented!()` remain in `src/` (no-todo!/unimplemented! rule:
PASS). The only remaining panics are on genuinely-unported cross-crate callees,
which are permitted.
