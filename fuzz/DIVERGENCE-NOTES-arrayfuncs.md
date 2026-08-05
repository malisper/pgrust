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

## KNOWN-DIV-2: byval int4 Datum word width (sign- vs zero-extension)

- Arms: 2 (array_get_element) and 6 (deconstruct_array), esel=0 (int4).
- Witness seed: fuzz/corpus/arrayfuncs_diff/seed-div-2 (the first smoke's
  crash artifact; payload has a negative int4 element 0xf9f9f9f9).
- C: fetch_att -> Int32GetDatum -> (Datum)(int32)x SIGN-EXTENDS the 32-bit
  value into the 64-bit Datum word (elem[6] = 0xfffffffff9f9f9f9).
- Rust: arrayfuncs::foundation::fetch_att ZERO-EXTENDS byval words
  (0x00000000f9f9f9f9) — a DOCUMENTED deliberate convention
  (foundation.rs:190 comment: "byval reads the element word
  (zero-extended, consumers truncate)").
- The int4 VALUE is identical; only the upper 32 bits of the Datum word
  differ. Note the crate is two-faced about the convention:
  Datum::from_i32 (datum/src/datum.rs:58, `value as DatumWord`)
  sign-extends, so the same int4 value has two in-process word images
  depending on whether it came from from_i32 or fetch_att. Consumers that
  compare or hash RAW datum words for byval types would misfire; worth an
  owner audit.
- Driver handling: the value plane for byval int4 compares the TRUNCATED
  i32 (the width the type defines) on both sides, and this word-level
  deviation is carried here instead. The oracle is unweakened (it still
  reports C's exact word).

## SMOKE RESULT 2026-07-31

- First 200k-run smoke found KNOWN-DIV-2 within seconds (crash artifact
  above); after pinning, the full smoke completed clean (see final lane
  report for exec rate).

## KNOWN-DIV-3: array_in bare-sign dimension integer (errcode plane)

- Arm: 0 (array_in), any elemsel. Witness input: `[1:-]={1,2,3}`
  (seed: fuzz/corpus/arrayfuncs_diff/seed-div-3, the smoke-2 artifact).
- C (ReadDimensionInt, arrayfuncs.c 519..558): strtol consumes NOTHING for
  a bare sign with no digits (endptr = start), so the caller's p==q "no
  digits" check fires -> "Missing array dimension value." under
  ERRCODE_INVALID_TEXT_REPRESENTATION 22P02 (class 1).
- Rust (io.rs read_dimension_int, 152..185): the sign branch ADVANCES pos
  past '-' before checking for digits, so pos != before, the no-digits
  check never fires, ub parses as 0, and the error surfaces later as
  "upper bound cannot be less than lower bound" under
  ERRCODE_ARRAY_SUBSCRIPT_ERROR 2202E (class 3).
- Verdicts agree (both reject); errcode + message path diverge. Real
  pgrust parser conformance bug: `SELECT '[1:-]={1,2,3}'::int[]` returns a
  different SQLSTATE than PostgreSQL. Fix belongs to the crate owner:
  don't advance pos when no digits follow the sign (strtol contract).
- Driver handling: pinned NARROWLY — only for arm 0 inputs whose
  dimension prefix (bytes before the first '{') contains a sign byte not
  followed by a digit, and only for the exact class pair C=1/Rust=3.

## RESOLUTIONS (p1-lanex, 2026-07-31) — all three divergences FIXED in-lane

- KNOWN-DIV-1 FIXED: construct.rs ndims<0 now carries
  ERRCODE_INVALID_PARAMETER_VALUE (22023). Driver pin tightened to strict
  class parity. Regression test:
  tests::p1_lanex_regressions::construct_md_array_negative_ndims_sqlstate.
- KNOWN-DIV-2 FIXED (real cross-crate bug): foundation.rs fetch_att now
  SIGN-EXTENDS byval words exactly like C's CharGetDatum/Int16GetDatum/
  Int32GetDatum and like the executor's own
  types_tuple::tupmacs::fetch_att and spgist's local copy. The
  zero-extending version made array-fetched byval datums bit-unequal to
  Datum::from_i32 datums of the same value; scalar::datum_ops::
  datum_is_equal compares byval datums as FULL WORDS (v1 == v2), so a
  negative int4 deconstructed from an array (e.g. MCV/stats stavalues)
  never equaled the same heap-fetched value. Driver pins tightened to
  full Datum-word parity. Regression test:
  tests::p1_lanex_regressions::fetch_att_sign_extends_like_c.
- KNOWN-DIV-3 FIXED (SQL-reachable): io.rs read_dimension_int no longer
  consumes a bare sign (strtol endptr contract); '[1:-]={1,2,3}' now
  fails with 22P02 "Missing array dimension value." as in C.
  GROUND-TRUTHED on docker postgres:18.3 2026-07-31: ERROR malformed
  array literal / DETAIL Missing array dimension value. / LOCATION
  ReadArrayDimensions, arrayfuncs.c:452; '[-2:0]={1,2,3}' accepted.
  Driver pin removed (strict parity). Regression test:
  tests::p1_lanex_regressions::array_in_bare_sign_dimension_is_22p02.
  Bonus hardening in the same function: acc growth now saturates once
  past the overflow threshold (a >19-digit dimension previously
  overflowed the i64 accumulator — debug-build panic; verdict plane
  unchanged).

# ROUND 2 (extension pass, 2026-07-31) — builtin-table findings

## KNOWN-DIV-4: deconstruct_array_builtin accepts 5 element types C rejects

- Arm: 6 (deconstruct) with BUILTIN-TABLE MODE (mode bit 2).
- Witness seeds: fuzz/corpus/arrayfuncs_diff/seed-builtin-e{4,5,7,11}
  (int8, float4, name, xid).
- C carries TWO DIFFERENT hardcoded tables in the same file:
    * construct_array_builtin   (arrayfuncs.c 3380..3492): 12 rows —
      char, cstring, float4, float8, int2, int4, int8, name,
      oid|regtype, text, tid, xid.
    * deconstruct_array_builtin (arrayfuncs.c 3696..3764): 8 rows —
      char, cstring, float8, int2, int4, oid, text, tid.
      NO float4, int8, name, regtype, xid.
  Its default: arm is `elog(ERROR, "type %u not supported by
  deconstruct_array_builtin()")` — a real, recoverable error (XX000).
- The crate has ONE shared `construct.rs builtin_meta` (13 effective rows)
  used by `deconstruct_array_builtin`, so pgrust SUCCEEDS on float4/int8/
  name/regtype/xid where C ERRORS. Verdict-plane divergence.
- Reachability: deconstruct_array_builtin is called from catalog-facing
  code with hard-wired element types, so no SQL statement reaches the
  extra rows today; it is a latent conformance gap (and arguably a
  deliberate superset). Owner call: either mirror C's narrower table or
  document the widening.
- Driver handling: the arm asserts EXACTLY this shape (C class 9, Rust Ok)
  for the 5 superset rows, and does full three-plane parity for the 8 rows
  both sides share. The oracle is unweakened: C's elog(ERROR) is mapped to
  the error plane (class 9 = XX000, elog.c's default sqlstate) rather than
  abort(), which is what makes the divergence observable at all.

## KNOWN-DIV-5: builtin_meta PANICS where C elog(ERROR)s

- The crate's `builtin_meta` ends in `panic!("type {other} not supported by
  construct/deconstruct_array_builtin()")`; C's two tables end in
  `elog(ERROR, ...)`, which is a catchable ereport (XX000), not an abort.
- Concretely reachable difference: BOOLOID is in NEITHER C table and NOT in
  the crate's table — C would raise XX000, pgrust panics (in release too;
  `panic!`, not `debug_assert!`). Any future caller passing an unlisted oid
  crashes the backend instead of erroring the statement.
- Driver handling: a panic cannot be compared past, so the builtin routes
  are gated to metas inside the respective C table; the divergence is
  carried here rather than by weakening either side. Owner fix: return a
  PgError with ERRCODE_INTERNAL_ERROR instead of panicking.

## PLATFORM CARVE (not a divergence): 1-byte byval element value plane

- C's fetch_att 1-byte arm is CharGetDatum(*(const char *)T); plain `char`
  is SIGNED on macOS/arm64 and x86-64 Linux but UNSIGNED on Linux aarch64,
  which is where the fleet campaign runs. The same input therefore yields
  Datum 0xffff_ffff_ffff_ff80 locally and 0x80 on the fleet for element
  byte 0x80.
- The driver compares 1-byte byval metas (esel 2 "char", esel 10 bool) at
  u8 width — the width the TYPE defines — and asserts the full Datum word
  for every other width. Documented in the target header; this is C
  platform variance, not a pgrust deviation.

# COVERAGE RESIDUAL CLASSIFICATION (extension pass, 2026-07-31)

Per-file fuzz-covered SLOC-v2 lines, baseline -> final (local measurement,
`cargo +nightly fuzz coverage` over the banked corpus + merge-coverage.py
--sloc-rule v2):

| file | sloc | base | final | uncovered = zero-region + real |
|---|---|---|---|---|
| element.rs    | 764 | 708 | 744 | 20 = 15 + 5 |
| construct.rs  | 250 | 207 | 230 | 20 =  2 + 18 |
| foundation.rs | 115 |  88 |  95 | 20 =  1 + 19 |
| io.rs         | 550 | 419 | 421 | 129 = 8 + 121 |
| TOTAL         |     |1422 |1490 | (+68) |

ZERO-REGION vs REAL is measured, not asserted: `llvm-cov show` emits NO
coverage region for those lines at all (blank count column), so SLOC-v2
counts them while the instrumentation can never credit them. The
merge-coverage run confirms this independently ("macro attribution: 0
regions attributed to invocation lines"). Verified example — io.rs 222 is
blank while its enclosing arm shows 26 executions:

    218|  2.80k|        if ndim >= MAXDIM {
    219|     26|            return soft(
    221|     26|                PgError::error(alloc::format!(
    222|       |                    "number of array dimensions exceeds ..."
    224|     26|                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),

So the MAXDIM / size-exceeded / dims-mismatch arms named in the extension
brief ARE driven; their format!-body and bare-declaration lines are simply
unmeasurable. This is the same false-UNCOVERED class already recorded for
`fc*!` lines tree-wide.

## Classification of every remaining line

element.rs — 15 zero-region (format!/decl lines of arms that DO run:
185-186, 204-205, 423, 453-454, 630, 720, 728, 819, 980, 1063, 1070-1072).
REAL 5, all DEFENSIVE-UNREACHABLE and matching C's own dead branches:
  - 338, 603: `construct_empty_array` / `upper_lt_lower` in the
    fill-missing-subscripts loops. Both need dims[i] == 0 in a >=1-dim
    image, which construct_md_array (and C's) can never produce — a zero
    dim collapses the array to 0-dim. C carries the identical dead branch.
  - 346: the `?` error edge of slice_size, which cannot fail for a
    well-formed image.
  - 643, 700: the ndim>1 arms of array_set_slice (slice_size for
    olditemsize; insert_slice). STILL-COVERABLE — driven by the new big
    multi-dim seeds but not yet hit within 400k local execs; the fleet's
    10M budget is the right vehicle. Not defensive.

construct.rs — 2 zero-region (45-46). REAL 18:
  - 35: builtin_meta's panic arm — deliberately NOT driven (KNOWN-DIV-5:
    a panic cannot be compared past). CARVE by decision, and itself a
    reported finding.
  - 42-44, 48, 257: the >1 GiB `array size exceeds the maximum allowed`
    arm. Needs a real gigabyte of element data per iteration —
    DEFENSIVE at fuzz scale (an exec-rate/OOM tradeoff, not a gap).
  - 138: `array_get_n_items` Err inside array_contains_nulls, i.e. a
    corrupt header. CARVE (the corrupt-image plane is fenced because C
    reads out of bounds there).
  - 241-243, 245-247, 250, 285, 288-290: the detoast element paths —
    explicit seam CARVE, out of scope per the brief.

foundation.rs — 1 zero-region (250). REAL 19:
  - 31, 213, 223-224: `panic!` arms for an unknown typalign / unsupported
    byval length. DEFENSIVE (no valid meta reaches them; C has no
    counterpart check at all).
  - 101, 114-118, 120-121, 123: the ndim-out-of-range early return of
    read_dims_lbounds, and read_dims in full. CARVE — the early return
    needs a corrupt header (fenced), and read_dims' only non-test callers
    are builtins.rs:179/236 (fc wrapper layer, carved).
  - 163-167, 170: varsize_any's TOAST arms (1B_E / external / vartag).
    CARVE — toast plane out of scope.

io.rs — 8 zero-region (222, 235, 276-277, 458, 510-511, 518). REAL 121:
  - 744-882 (119 lines): array_recv / array_send. Explicit OUT-carve.
    In-scope io.rs is therefore ~430 lines, of which 421 are covered.
  - 18-23: call1_armed. NOT reachable from core entries — its only caller
    is builtins.rs:471 (`array_to_text` family), which needs
    catalog-backed get_type_io_data. BUILTINS CARVE, class it there.
  - 507-509, 513: the `nitems >= MAX_ARRAY_SIZE` guard inside
    ReadArrayStr — needs 268M parsed elements. DEFENSIVE at fuzz scale.
  - 546, 574-576: `mcx.oom()` / try_reserve failure arms. DEFENSIVE
    (allocator-failure injection, not a fuzz input).
  - 148, 644: `Ok(Some(img))` tail of array_in and the get_unchecked
    closure body inside array_out's element loop — both inside paths that
    demonstrably run thousands of times; llvm attributes their regions to
    the enclosing lines. Treat as ZERO-REGION-equivalent (region exists
    but is folded), i.e. a measurement artifact, not a gap.

## RESOLUTIONS round 2 (p1-lanex, RATIFIED Michael 2026-07-31)

- KNOWN-DIV-5 FIXED: builtin_meta's trailing panic! replaced with a
  PgError XX000 ("type %u not supported by construct_array_builtin()"),
  matching C's elog default arm — an unlisted oid (e.g. bool, in neither C
  table) now errors the statement instead of aborting the backend.
- KNOWN-DIV-4 FIXED: the shared table is split to mirror C's asymmetry
  exactly — builtin_meta = construct_array_builtin's table (13 oids incl.
  regtype), new deconstruct_builtin_meta = deconstruct_array_builtin's
  8-row strict subset with its own XX000 default arm. The 5 construct-only
  types now error through deconstruct_array_builtin on both sides.
- Driver: builtin routes opened to ALL metas (bool included) with strict
  class-9 parity; the KNOWN-DIV-4 pin and the builtin_route_ok table gate
  are gone. Regression tests: tests::p1_lanex_builtin_tables (2 tests).
- Also fixed in the driver this round: hollow-materialization defect in the
  construct arm's wide mode (dims product in 4097..=MaxArraySize was
  accepted by both sides but left unmaterialized, so C's size pass read
  unmaterialized memory — replay SEGV, harness defect not a finding).
