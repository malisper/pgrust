# jsonb-soft lane evidence bank (proofs/jsonb-soft-plane, 2026-07-31)

New compared SOFT-ERROR (escontext) plane for the jsonb/json family — the
`pg_input_is_valid` / `COPY ... ON_ERROR ignore` / errsave surface, never
differentially compared here before (laneac rangetypes precedent).

- jsonbio_diff: soft plane added, BOTH sides armed — C oracle via a marker
  escontext through DirectInputFunctionCallSafe (jsonbfam shim errsave/
  ereturn/SOFT_ERROR_OCCURRED upgraded from hard-only), Rust via the
  shipped fc_jsonb_in with a types_fmgr::ErrorSaveNode in fcinfo.context.
  Planes: soft OCCURRED flag, captured sqlstate class, success-image
  identity under soft mode, per-side soft/hard verdict agreement.
- json_diff: C side of the existing soft plane armed too
  (pg_diff_json_in_soft); soft sqlstate now compared C-soft vs Rust-soft.
- jsonpath_diff: already had a both-sides-armed soft plane (arm 0 mode&1);
  verified. Deep-nesting recursion guard verified release-effective on the
  soft path: gram.rs check_depth() calls ::stack_depth::check_stack_depth
  (runtime, not assert) and ereturns into the escontext; ratified carve
  (54001 vs C bison 42601) stands. Docker 18.3:
  pg_input_is_valid(repeat('[',12000)||'1'||repeat(']',12000),'jsonpath')
  = f, error 42601, server alive.

Results: local 5.3M execs (jsonbio_diff) + 3M (json_diff), ZERO
divergences — pgrust's soft-error contract is C-exact on this family.
Ground-truthed on docker postgres:18.3: '1e1000000' jsonb soft-fails
22003, '["\u0000"]' 22P05, '{bad' 22P02; json '1e1000000' valid (text
kept, no numeric conversion).

Lines newly MEASURED (fuzz-jsonbio_diff-local-20260731.lcov.gz):
- json/src/jsonapi.rs:970 (DA=69) — the flagfix RECORDED RESIDUAL
  (SemActionFailed from the jsonb_in scalar sem hook), retired.
- jsonb/src/builtins.rs 95, 97 (fc_jsonb_in escontext lines, DA=1744);
  jsonb/src/io.rs 112 (numeric soft None arm), 180/181 (escontext arms),
  187 (errsave_parse_error, DA=1675). Their excluded-state rows removed
  from phase1-exceptions.tsv in this commit.

Fleet CONFIRM (10M floor, one job per target) at lane sha 25ec54cae1 —
BOTH GREEN (rc 0, digests fetched from S3, campaign-stats banked on the
lane branch at proofs/coverage/jsonbsoft/fleet-stats-*.json):
- jsonbio_diff: pgrust-fuzz-campaign-1785546655-3e25-22725 —
  10,000,000/10,000,000 execs, 0 divergences, 0 sanitizer artifacts,
  corpus 6527 -> 8667 (delta committed), cov_lines 3504; fleet lcov
  re-confirms jsonapi.rs:970 DA=79 and all six retired jsonb lines.
- json_diff:    pgrust-fuzz-campaign-1785546669-5263-23167 —
  10,000,000/10,000,000 execs, 0 divergences, corpus 6099 -> 7278
  (delta committed), cov_lines 2577.
