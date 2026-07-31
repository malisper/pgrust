# arrayfuncs_diff divergence notes (lane p1-lanex)

Findings are recorded here per the campaign rule: divergences are findings,
not failures — the oracle is never weakened and the crate is never patched
from this lane. Each KNOWN-DIV below is pinned in the driver with an exact
expected shape (so the carve cannot hide new regressions) and has a witness
seed committed in fuzz/corpus/arrayfuncs_diff/.

## KNOWN-DIV-1: construct_md_array(ndims < 0) sqlstate

- Arm: 7 (construct_md_array), any elemsel.
- Witness seed: fuzz/corpus/arrayfuncs_diff/seed-div-1
  (bytes: [0x07, 0x00, 0xff x128] — raw ndims byte >= 250 decodes to a
  negative ndims probe).
- C (arrayfuncs.c 3508..3511): `ereport(ERROR,
  (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid number of
  dimensions: %d", ndims)))` -> sqlstate 22023 (oracle class 7).
- Rust (crates/backend/utils/adt/arrayfuncs/src/construct.rs:189-192):
  `PgError::error(format!("invalid number of dimensions: {ndims}"))` with
  NO `.with_sqlstate(...)` -> defaults to ERRCODE_INTERNAL_ERROR (XX000).
- Verdicts agree (both error); only the sqlstate plane diverges.
- Status: pgrust conformance bug (missing sqlstate). NOT SQL-reachable via
  array_in (its ndim is never negative), but construct_md_array is a
  library entry other code calls with computed ndims. Fix belongs to the
  crate owner: add `.with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)`.

## OBSERVATION (carved domain, not a plane divergence): array_set_slice
   nSubscripts == 0

- C's ndim==1 arm has `Assert(nSubscripts == 1)` (arrayfuncs.c 2929) —
  debug-only, compiled out under NDEBUG, after which the code proceeds and
  reads lowerProvided[0]/lowerIndx[0].
- The shipped Rust keeps that caller contract as an unconditional
  `assert!(n_subscripts == 1)` (element.rs:548), so nsub==0 panics in
  release where NDEBUG C proceeds.
- SQL subscripting always supplies >= 1 subscript, so this is outside the
  reachable domain; the driver carves nsub >= 1 for arm 5. Flagging under
  the debug-assert-masking law anyway: release-effective assert where C has
  a compiled-out Assert is a ported-in constraint the crate owner may want
  to delete or keep deliberately.
