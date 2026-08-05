# soft-plane wave 2: adt/numeric evidence bank (proofs/sp2-num, 2026-07-31)

New compared SOFT-ERROR (escontext) plane for numeric_in — the
`pg_input_is_valid('…','numeric')` / `COPY ... ON_ERROR ignore` surface.
Before this lane the numericfam harness FORBADE the soft path
(`.expect("no escontext")` at the numeric_in arm); numeric soft coverage
existed only indirectly as a range subtype (laneac rangetypes plane).

## What was built (commit 0dc3c7054d + corpus commit 749dbcb2bb)

- C oracle (fuzz/core/csrc/numericfam/): vendor/postgres.h `ereturn` is now
  escontext-aware — the errsave_start soft/hard demux, value-exact: errcode
  recorded either way, then `nfz_soft_save` marks a real T_ErrorSaveContext
  node and returns the dummy, else longjmp. New entry
  `pg_diff_numeric_in_soft` (text × typmod) arms the node on
  fcinfo->context, so the UNMODIFIED vendored numeric.c soft cascade
  (set_var_from_str / set_var_from_non_decimal_integer_str / apply_typmod /
  apply_typmod_special / make_result_opt_error) runs its actual escontext
  branches.
- Rust driver (fuzz/core/src/numericfam.rs `arm_in_soft`): runs on EVERY
  exec of the numeric_in arm (no selector, full corpus reuse). Drives BOTH
  shipped soft shapes: core `numeric_in(s, typmod, Some(&mut
  SoftErrorContext))` and the fc wrapper `fc_numeric_in` with a
  types_fmgr::ErrorSaveNode in fcinfo.context (uuid_diff fc-soft shape).
- Four comparisons (rangetypes arm_text_in_soft precedent):
  (a) soft OCCURRED flag (C esc.error_occurred vs Rust error_occurred(),
      core AND fc); (b) captured sqlstate — FULL MAKE_SQLSTATE value, not
      just the class (numericfam convention); (c) success-image identity
      under soft mode (C soft == C hard == Rust core == Rust fc varlena
      image); (d) per-side soft/hard verdict agreement against two real
      hard executions, plus thrown-vs-saved sqlstate identity per side.
  NOT compared: fcinfo.isnull on the soft-failure edge (rangetypes
  SOFT-ISNULL note applies: C is not self-consistent there and no caller
  reads the result).

## Both-sides-armed witness

`numericfam::tests::soft_plane_c_side_arms` (fuzz/core, passes): C
esc.error_occurred is set ONLY inside nfz_soft_save, so the asserted
nonzero saved classes prove the C escontext branch EXECUTES — 22P02 for
'abc', 22003 for '1e1000000000' (== the hard-mode sqlstate), 22003 via
apply_typmod for '123'::numeric(2,0), 22003 via apply_typmod_special for
'Infinity'::numeric(5,2); 'NaN'::numeric(5,2) and '1.25'::numeric(5,2)
save nothing and the soft image equals the hard image.
`soft_plane_smoke` drives the full four-way plane over the distinct
soft-fail classes × {-1, numeric(2,0), numeric(5,2)}.

## Corpus

23 soft-path seeds committed (fuzz/corpus/numeric_io_diff/soft-*):
garbage, exponent over/underflow, non-decimal junk (0x/0o/0b, empty and
invalid digits, hex double-underscore), underscore-separator errors
(1__2 / _1 / 1_), dot/sign/trailing-junk syntax errors, typmod violations
(numeric(2,0) × '123', rounding-overflow numeric(5,2) × '999.999'), and
the typmod special path (NaN / ±Infinity × numeric(5,2)) — plus the 2.0M
local growth delta.

## Results

Local smoke: 200k + 1.8M = 2.0M execs of numeric_io_diff at the lane tip,
ZERO divergences (soft plane live on every sel-0 exec). No docker
ground-truthing needed — pgrust's numeric soft-error contract is C-exact
on everything reached. Fleet 10M CONFIRM is the coordinator's leg.

## Recursion guard

VERDICT: NON-RECURSIVE, no guard needed. numeric_in →
set_var_from_str / set_var_from_non_decimal_integer_str (pure loops,
io.rs) → apply_typmod / apply_typmod_special (loops, ops.rs) →
make_result_opt_error (var.rs). No self- or mutual recursion on either
side (vendored numeric.c likewise has no check_stack_depth in this path),
so there is no deep-nesting soft/hard divergence class here (contrast
jsonpath).

## Ledger prep — candidate retirements (coordinator serializes; witness
lcov banked here: fuzz-numeric_io_diff-local-20260731.lcov.gz, the
adt/numeric slice of a full-corpus cov-export at the lane tip)

proofs/coverage/laneu/residual-laneu.tsv RECORDED rows (class "soft-error
escontext arms (drivers pass None)") now MEASURED — retire:
- crates/backend/utils/adt/numeric/src/io.rs:88  — Ok(None) after
  apply_typmod_special soft save; DA=74.
- crates/backend/utils/adt/numeric/src/io.rs:139 — Ok(None) after
  apply_typmod soft save; DA=166.
- crates/backend/utils/adt/numeric/src/builtins.rs:114 — fc_numeric_in
  soft-null arm (`None if had_esc`); DA=1082.

Reclassify (not straight retirements):
- crates/backend/utils/adt/numeric/src/ops.rs:134 and ops.rs:166 — the
  `false,` dummy-argument lines of the apply_typmod /
  apply_typmod_special ereturn calls. The ereturns now FIRE (sibling
  lines 132/133/135 DA=377; 164/165/167 DA=121) but these two rows stay
  DA-absent — instrument-unmappable continuation lines, not coverage
  gaps.

Stay RECORDED (measured DA=0, correctly):
- crates/backend/utils/adt/numeric/src/io.rs:125 — non-decimal
  OutOfRange ereturn; needs a >131k-digit hex literal, unreachable under
  the arm's 256-byte input cap on both sides.
- crates/backend/utils/adt/numeric/src/builtins.rs:115 — defensive panic
  guard (soft escape without escontext), unreachable by construction.

Also newly measured (were plain corpus-gap residual rows, not
soft-shaped): io.rs:112 DA=249, io.rs:115 DA=27, io.rs:122 DA=132,
io.rs:144 DA=30 — the numeric_in ereturn throw/save sites; not in the
residual list, no action needed.

## Carves

None extended: numeric_io_diff's ratified carves (non-UTF-8/NUL inputs at
the &str entry; size caps) are driver-side symmetric predicates evaluated
BEFORE either side runs, so they apply to the soft plane with the same
key automatically. No numeric-specific soft carve was needed (zero
divergences).
