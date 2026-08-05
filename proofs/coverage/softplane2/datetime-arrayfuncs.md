# Soft plane wave 2 — datetime family + arrayfuncs (coordinator's own targets)

Lane proofs/softerror-plane-2, 2026-07-31.

## Finding: three MORE one-sided "soft planes" (the json_diff failure mode)

The wave-1 survey listed adt_timestamp/adt_date (datetime_closeout_diff),
and arrayfuncs as soft-covered. All were ONE-SIDED:

- pg_datetime_io_io.c, pg_timestamp_io.c, pg_datetime_closeout.c all stubbed
  `errsave` as "escontext is always NULL here (hard-error shape): both
  throw" — C's own errsave branch had NEVER executed. The claimed soft faces
  (timestamp_diff ts_in/interval_in soft blocks, datetime_closeout in_face)
  compared Rust-soft vs Rust-hard only. C-hard≡Rust-hard plus
  Rust-soft≡Rust-hard does NOT imply C-soft≡Rust-soft: an ereport-vs-errsave
  site difference (hard where the other side softens) only shows under an
  ARMED C escontext.
- pg_arrayfuncs_io.c: errsave/ereturn both unconditionally longjmp'd; the
  arrayfuncs_diff soft block was honestly LABELED Rust-side-only, but the C
  branch was equally unexecuted, and the InputFunctionCallSafe shim always
  returned true (fmgr.c returns false on a soft element failure).

## What was armed (both sides, every exec, corpus reused)

- datetime_io_diff: pg_diff_{date,time,timetz}_in_soft; soft_face compares
  soft verdict, soften-vs-throw, captured class, success image, per-side
  soft/hard agreement. tz-database carve unchanged (soft leg runs only on
  in-domain execs, same key).
- timestamp_diff: pg_tsdiff_{timestamp,interval}_in_soft (timestamp +
  timestamptz + interval); same five comparisons appended to the existing
  Rust-only soft blocks (kept).
- arrayfuncs_diff arm 0: pg_diff_array_in_soft; ereturn sites AND the
  element soft path (pg_strtoint32_safe via the fixed InputFunctionCallSafe
  contract) now execute in C; image identity under soft mode compared.

Witness tests (C soft capture with nonzero class, no longjmp; valid-input
image non-perturbation): datetime_io_diff::tests::c_escontext_branch_executes,
timestamp_diff::tests::c_escontext_branch_executes,
arrayfuncs_diff::tests::c_escontext_branch_executes.

## Local smokes

datetime_io_diff 1.0M execs, timestamp_diff 400k, arrayfuncs_diff 600k —
ZERO divergences. pgrust's soften-vs-throw contract is C-exact on
date/time/timetz/timestamp/timestamptz/interval/array_in at these budgets.

## Recursion guards

All soft paths here are iterative (DecodeDateTime/DecodeInterval field
loops; array_in dimension/element loops — MAXDIM bounds nesting; no
recursive descent). No stack-depth guard needed on the soft path; nothing
debug_assert-gated.

Fleet CONFIRM (10M floor, one job per target) recorded in
fleet-confirms.md at the final lane sha.
