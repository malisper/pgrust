# microbatch-contribc — differential-fuzz coverage bank (lane p1-mb-contribc)

Crates: `crates/contrib/hstore` and `crates/backend/tsearch/wparser_def`.
Two targets, `hstore_diff` (5 selector arms) and `wparser_diff` (one
surface, encoding/ctype selector). Oracles: verbatim vendored PostgreSQL
18.3 C @ 62d6c7d3df, sed/awk-assembled by `scratchpad/assemble_hstorefam.sh`
and `scratchpad/assemble_wparserfam.sh` into
`fuzz/core/csrc/pg_hstorefam_io.c` / `pg_wparserfam_io.c` (provenance,
shims and carves in each file's header).

## Evidence
- `hstore_diff.lcov.gz` / `wparser_diff.lcov.gz` — full-corpus lcov
  (`fuzz/cov-export.sh`, committed-corpus replay, nightly llvm-cov).
- `summary.tsv` — per-crate/per-file equation: lcov DA lines = covered +
  exception rows, 0 unaccounted.
- Exception rows appended to `proofs/coverage/phase1-exceptions.tsv`
  (lane tag `p1-mb-contribc`).

## Campaign volume (local, macOS arm64, 2026-08-01)
- `hstore_diff`: 1.55M (bring-up) + 5.30M (round 1) + round 2 — 0
  divergences, 0 crashes after the three defect fixes below.
- `wparser_diff`: 1.68M (bring-up) + 6.35M (round 1) + round 2 — 0
  divergences, 0 crashes after the two fixes below.
- Fleet 10M CONFIRM: coordinator follow-up.

## REAL pgrust defects found (all fixed in-lane)
1. `hstore/src/parse.rs` `is_space` omitted `\v` (0x0b) from
   scanner_isspace's `{space}` class — `\va=>null` parsed the key as
   `"\va"` where C parses `"a"`. (C: src/backend/parser/scansup.c:117-126.)
2. `hstore/src/lib.rs` `fc_hstore_from_arrays` never checked `ARR_NDIM > 1`
   on either argument, nor ndim/dims[0]/lbound[0] equality — it compared
   deconstructed element counts only, accepting inputs C rejects with
   2202E and firing the null-key error where C raises the bounds error.
   (C: contrib/hstore/hstore_io.c:628-668.)
3. `wparser_def/src/parser.rs` `tparser_init` truncated BOTH wide arrays to
   the converted wchar count while `lenstr` counted the whole input, so
   `p_iswhat` indexed out of bounds (a PANIC = backend crash) whenever the
   conversion yielded fewer wchars than input bytes. C allocates
   `lenstr + 1` slots. (C: src/backend/tsearch/wparser_def.c:302-315.)

Hardening (not a divergence, found by reading the C while porting the
oracle): `hstore_recv`'s pair-count guard used `isize::MAX / sizeof(Pair)`
instead of C's `MaxAllocSize / sizeof(Pairs)`, admitting counts C rejects
with 54000 and then attempting multi-gigabyte reserves.

## Harness/infrastructure findings
- **Build-registration restore.** The `p1-microbatch-1` union merge landed
  `csrc/pg_{tzfam,miscfam,netfam,libfam,portfam}_io.c` but dropped ALL FIVE
  of their `fuzz/core/build.rs` registrations, so those targets could not
  link at main (undefined `pg_tzf_*` / `pg_mf_*` / `pg_nf_*` / `pg_diff_*`).
  Restored verbatim from their original commits in this lane. The
  `nm`-based dup-extern sweep across every oracle archive is clean.
- `pg_mblen_range` must be vendored per-TU, not resolved against
  `pg_wcharfam.c`: the wfam_ copy's illegal-sequence path longjmps through
  a `jmp_buf` only its own `wfam_x_*` entry shims arm, so reaching it from
  another TU jumps through an uninitialized buffer (SIGSEGV).

## Certified non-surface (multirange-tie-ruling pattern)
`hstoreUniquePairs` keeps ONE of several equal-key pairs and which one is
qsort-tie-order dependent — C runs `pg_qsort` (Bentley & McIlroy, unstable
at n>=7), the Rust port a stable sort; PostgreSQL documents the surviving
duplicate as unspecified. `hstore_diff` accepts a differing image ONLY when
the key sequences match and every divergent value is a certified candidate
for a DUPLICATED key, with the candidate multiset re-derived from the C
parse-only entry (arm 0) or from the decoded inputs (arms 1/2). Everything
else is a real divergence.

## Domain carves (documented in the driver headers)
- hstore arm 0: input is NUL-free (cstring contract).
- hstore arm 1: leading wire pair-count clamped to <= 65537 for alloc
  shaping; the real C limit boundary is driven by
  `tests::recv_pair_count_limit`.
- hstore arms 2-4: array images are driver-built and well-formed (corrupt
  array headers belong to `arrayfuncs_diff`).
- wparser: under a MULTIBYTE encoding the input is NUL-free. This is both a
  genuine C caller contract (a `text` value cannot contain a NUL) and a
  C-UB boundary: both wide arms stop converting at the NUL while `lenstr`
  keeps counting, so C then indexes UNINITIALIZED palloc'd wchar slots and
  there is no defined C answer. Single-byte encodings keep NULs in domain.
- wparser: input capped at 8 KiB, token stream at 4096 tokens (applied
  identically on both sides).

## Carved surfaces (exception rows, not measured by these targets)
- hstore: `hstore_from_record` / `hstore_populate_record` (composite
  typcache + per-column fmgr IO), `hstore_skeys`/`svals`/`each` (SRF
  machinery; the underlying walks run via akeys/avals/to_array),
  `hstore_to_jsonb(_loose)` (jsonb value machinery + numeric_in),
  `hstore_subscript_handler` (parse/exec plumbing), `hstore_gist.c` /
  `hstore_gin.c` opclasses, `hstore_compat.c` old-format upgrade (no
  producer on either side).
- wparser_def: `prsd_headline` and the whole ts_headline support half
  (`headline.rs` / wparser_def.c 1936-2725 — TSQuery + HeadlineParsedText
  from the layer above this crate), and the funcapi SRF faces in
  `builtins.rs` (`ts_parse_by*` / `ts_token_type_by*`).
