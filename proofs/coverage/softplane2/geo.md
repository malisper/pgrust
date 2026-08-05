# geo soft-plane lane evidence bank (proofs/sp2-geo, 2026-08-01)

SOFT-ERROR (escontext) differential plane for adt/geo — the
`pg_input_is_valid()` / `COPY ... ON_ERROR ignore` / errsave surface for
all 7 geo text input functions (point/box/lseg/line/path/poly/circle_in),
never differentially compared before (geo_io_diff passed NULL/None
escontext at all 7 call sites; wave-2 survey: 11 soft params / 20 errsave
in the family). Pattern: rangetypes_diff arm_text_in_soft (wave 1).

## What was armed

- C oracle (fuzz/core/csrc/pg_geo_io.c): the I/O-section `ereturn` shim
  upgraded from always-return-dummy to the ESCONTEXT-AWARE rangetypes
  split — record class, return dummy iff escontext != NULL, else longjmp
  (the real miscnodes.h semantics). 7 new `pg_diff_geo_*_in_soft` entry
  points run the verbatim bodies with a non-NULL sentinel node.
  BOTH-SIDES-ARMED WITNESS (the json_diff Rust-only-armed failure class):
  a TLS counter `pg_geo_soft_branch_hits` increments ONLY inside the
  escontext branch; test `geo_io_diff::tests::c_soft_capture_witness`
  drives one geo-ereturn-reaching bad literal per type through the soft
  entries and asserts a captured class 1 per type plus >= 7 branch hits.
- Rust: the SHIPPED `fc_{point,box,lseg,line,path,poly,circle}_in`
  wrappers, resolved from `adt_geo::builtins::GEO_BUILTINS` (no shipped
  code changed), driven with a `types_fmgr::ErrorSaveNode` in
  `fcinfo.context` — the exact production route
  (fcinfo.soft_error_context() -> io::*_in escontext threading).
- Planes per text exec, ON EVERY EXEC of the existing arms (no selector;
  corpus fully reused): (a) soft OCCURRED flag; (b) captured sqlstate
  CLASS (same 1/2/3/4 table as the thrown plane); (c) success-image
  identity under soft mode (bit-exact f64 struct fields; path/poly also
  npts/closed/boundbox); (d) per-side soft/hard verdict agreement
  (valid-in-soft iff valid-in-hard, independently for C and Rust);
  plus the hard-in-soft agreement edge (line_in's lseg_sl/line_construct
  float errors stay HARD in soft mode on both sides, per C's XXX
  comment). NOT compared: fcinfo.isnull on the soft-failure edge
  (rangetypes SOFT-ISNULL precedent — C is not self-consistent there).

## Local results

- cargo test -p decoder_fuzz geo_io_diff: 4/4 green
  (c_soft_capture_witness, soft_plane_smoke, arms_smoke,
  seed_corpus_replays_clean over the full committed corpus).
- Local fuzz smoke: 2.0M runs total (200k + 1.8M), exec/s ~9.4k.
  ONE divergence found, classified, carved (below); 0 remaining.
- 17 soft-failure seeds committed (bad coordinate count, garbage floats,
  unbalanced delimiters per family, negative circle radius, zero-A/B and
  same-points line specs, float-overflow literal, path open/closed edge
  shapes) + fuzzer-grown corpus delta committed.

## Divergence log

1. crash-7230cea5 `"Nan(.1,),1 0,.13"` (line, HARD plane): C oracle st 0
   (accepts) vs Rust 22P02. CLASSIFIED: macOS-oracle platform artifact —
   macOS strtod (gdtoa) consumes `nan(<anything-to-paren>)` including
   bytes glibc rejects from the n-char-sequence; the identical class was
   ground-truthed against docker postgres:18.3 (glibc rejects, 22P02) and
   ratified as the HOST-CONDITIONAL float_in carve
   (fuzz/core/src/diff.rs "ORACLE PLATFORM CARVE (2026-07-30)"). Extended
   here with the SAME KEY (lowercase contains "nan(") in take_text(),
   macOS-only, covering hard + soft planes (same strtod underneath, same
   key + the soft class check rides the shared comparator). The fleet
   (glibc) fuzzes nan( forms normally. NOT a pgrust defect; shipped Rust
   matches real PG. SQL repro (macOS-oracle-only, real PG returns f):
   `SELECT pg_input_is_valid('Nan(.1,),1 0,.13','line');`
   Docker re-confirmation from this lane: the local docker daemon was
   wedged (socket unresponsive; other lanes saturating it) — the ratified
   wave-0 docker ground-truth for this exact key stands as the evidence
   of record.

## Findings (banked, not fixed)

- SOFT-HARDNESS GAP (unreachable at driver cap, SQL-reachable in
  principle): C path_in/poly_in report "too many points requested"
  (54000) via ereturn — SOFT under escontext. Rust
  `io::check_points_overflow` (crates/backend/utils/adt/geo/src/io.rs:270-280)
  returns a hard Err unconditionally, and its callers path_in (io.rs:540)
  / poly_in (io.rs:608) do not thread escontext into it. To fire it needs
  npts > 2^27, i.e. a >=268 MB literal (< the 1 GB cstring cap, so
  `pg_input_is_valid(<268MB literal>,'path')` would hard-error where real
  PG returns false). Not a one-line fix (signature change; the same
  helper is used by recv paths where C throws HARD via ereport 22P03), so
  banked for a follow-up rather than patched here. The fuzz driver's 1
  KiB text cap keeps this plane dark to the fuzzer on both sides.

## Recursion-guard audit

VERDICT: NO RECURSION POSSIBLE — no guard needed, none exists, none
ported in (correct per the debug-assert-masking law: C geo_ops.c has no
check_stack_depth in any *_in body; `grep -c check_stack_depth
fuzz/core/csrc/pg_geo_io.c` = 0, and none in crates/backend/utils/adt/geo/src).
The soft call graph is strictly downward and iterative on both sides:
fc_*_in -> io::{point,box,lseg,line,path,poly,circle}_in ->
path_decode/pair_decode/line_decode -> single_decode ->
adt_float::float8in_internal (linear scanner). No *_decode function is
called from below itself; path/poly point loops are `for` loops; unlike
array_in/jsonpath there is no element-input re-entry
(InputFunctionCallSafe) anywhere in the family.

## Ledger prep — candidate retirements (coordinator serializes ledger edits)

proofs/coverage/phase1-exceptions.tsv has NO adt/geo rows today (grep
`adt/geo/` = 0), so there are no excepted rows to retire; the candidates
below are previously-UNMEASURED soft-shape lines that the recorded
per-file baselines (proofs/coverage/files/crates__backend__utils__adt__geo__src__{io,builtins}.rs.json,
fuzz ∪ regress ∪ kani) show uncovered and this plane now witnesses with
DA>0 (local lcov fuzz/coverage/geo_io_diff.lcov, corpus replay):

- crates/backend/utils/adt/geo/src/io.rs — 104 lines newly DA>0 vs the
  recorded baseline (505 lcov-covered vs 420 recorded). The SOFT-SHAPE
  subset (soft dummy returns / escontext edges the plane exists for):
  io.rs:139 (pair_decode soft dummy return), io.rs:336 and io.rs:343
  (line_decode soft dummy returns), io.rs:572 (path_in soft
  empty_path_image return), io.rs:579 (path_in trailing-junk soft
  ereturn edge), io.rs:662 (circle_in radius soft dummy return). The
  remainder is hard-plane coverage growth from the 2M-run corpus.
- crates/backend/utils/adt/geo/src/builtins.rs — 14 lines newly DA>0,
  notably builtins.rs:63 (the `escontext()` -> fcinfo.soft_error_context()
  accessor, the production soft-routing line) and the closing lines of all
  seven fc_*_in wrappers (118/131/144/157/170/183/196) now executed
  under the differential plane rather than regress alone.
  Full lists in this lane's lcov: fuzz/coverage/geo_io_diff.lcov
  (io.rs: 35 36 45 52 54 60 61 70-75 78-80 85 91 103 117 122 135 139 140
  144 149 154 156 160 163 183 187 192 193 199 203 218 221 230 236 255
  267 278 280 286 308 312 316 319 322 327 336 337 340 343 344 347 351
  354 358 360 365 375 382 398 405 410 420 437 439 444 448 452 472 482
  513 525 527 539 550 566 572 573 575 579 580 583 595 607 626 629 633
  644 648 649 654 658 662 663 668 682 687 688 698; builtins.rs: 58 63
  88 92 96 100 104 118 131 144 157 170 183 196).

## Fleet handoff

Lane branch proofs/sp2-geo; local smoke complete; fleet 10M floor is the
coordinator's CONFIRM (glibc host also re-opens the nan( key that is
carved dark on this macOS rig).
