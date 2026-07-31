# fuzz/divergences/GAPS-p1-laneac.md — GENUINE FUZZ GAPS (NOT exceptions)

Lane p1-laneac. adt/rangetypes + adt/multirangetypes.
Opened 2026-07-31 with 33 gap lines; **26 closed, 2 reclassified as excepted,
5 still open** after the gap-closing pass. Numbers below are MEASURED
(`cargo fuzz coverage` over the committed corpus, merged under SLOC-v2), not
estimated.

A gap is reachable code the differential corpus never drives. Gaps are
deliberately NOT written as exception rows: an exception says "this line cannot
or must not be covered", and recording a reachable line that way is the
gate-blindness failure the campaign keeps getting burned by. Conversely, when
measurement PROVES a line is fenced, it moves out of this file and into
`proofs/coverage/phase1-exceptions.tsv` with the evidence.

## Accounting

| bucket | lines | % |
|---|---:|---:|
| fuzz-measured | 2406 | 80.68% |
| recorded executable exceptions | 571 | 19.15% |
| **genuine fuzz gaps (below)** | **5** | **0.17%** |
| accounted total | 2977 | 99.83% |

(in-scope non-test v2-SLOC = 2982; fuzz-measured was 2379/79.78% before this
pass. The exception count rose by 2 — see "Reclassified" — and 26 former gap
lines are now fuzz-measured.)

## STILL OPEN — 5 lines

### Toasted bound: external pointer / compressed — 4 lines

`crates/backend/utils/adt/rangetypes/src/lib.rs` 415, 416, 418, 419

`detoast_bound_packed`'s external-pointer and pglz-compressed arms. Both
drivers now feed SHORT-header images (that arm IS covered, and it closed
`arg_range`'s RangeArg::Owned path at builtins.rs:29/47), but never an external
TOAST pointer or a compressed datum.

WHY IT IS STILL OPEN, and the cost argument: the vendored oracle has neither
pglz nor any toast-fetch machinery (`grep -c pglz csrc/pg_rangetypes_io.c` = 0).
An external pointer additionally implies a toast RELATION on both sides. The
compressed case is the cheaper half — it needs only `pglz_decompress` vendored,
no toast table — and is the recommended next increment if these 4 lines are
wanted. Note these lines ARE exercised by the crate's own tests
(`rangetypes/src/tests.rs` `bound_detoast`: `external_bound_is_inlined`,
`compressed_bound_is_decompressed`), which count for nothing under the campaign
metric, so they remain gaps here rather than exceptions.

### daterange canonicalize soft edge — 1 line

`crates/backend/utils/adt/rangetypes/src/lib.rs` 535

`canonicalize`'s `F_DATERANGE_CANONICAL` soft edge (`return Ok(None)` when
`canonical_adjust_date` captures a soft error). Its int4 and int8 siblings
(:525, :530) ARE now covered, via soft-mode `range_in` with a bound at the type
maximum.

WHY THE OBVIOUS ROUTE DOES NOT WORK: the constructor arms cannot reach it.
BOTH implementations hardcode a NULL escontext at that call site —
`rangetypes.c` `range_constructor2/3` pass `NULL` to `make_range`, and the
shipped `fc_range_constructor2/3` pass `None` — so no soft error is capturable
through a constructor in either implementation. (The driver still runs the
constructors with an armed-but-ignored escontext, which pins exactly that
contract: if pgrust ever started threading it, C would throw hard while Rust
captured softly and the OCCURRED assert would fire.)

TO CLOSE: vendor `date_in`/`date_out` into the oracle so daterange joins the
text-io arms (`NPINS_IO` 3 -> 4), then the existing soft-mode `range_in` path
reaches it with a bound at the maximum valid date (2145031948; +1 leaves
`IS_VALID_DATE`). Bounded work, not a structural obstacle.

## RECLASSIFIED — measured to be fenced, now exception rows

- `multirangetypes/src/builtins.rs:186` — `multirange_constructor2`'s
  `argisnull(0)` arm. **Strict-unreachable**: `pg_proc.proisstrict = t` for oids
  4281/4282 (ground-truthed on postgres:18.3 — `select
  int4multirange(NULL::int4range)` yields NULL, not an error), so fmgr never
  enters the body. C's own comment says the same. C's arm is a bare `elog`
  (XX000) where pgrust raises 22004, so the two differ ONLY in a state real PG
  cannot construct; driving it briefly reported that as a divergence, which is
  what identified it. Its sibling at :220 (a NULL MEMBER inside a non-null
  array) IS reachable via the variadic form — `select int4multirange('[1,2)',
  NULL)` raises 22004 on 18.3 — and is now fuzzed, both sides agreeing.
- `multirangetypes/src/lib.rs:834` — `multirange_intersect_internal`'s
  empty-operand short circuit. **Fenced by the call site, C-identically**: the
  only in-scope caller (`fc_multirange_intersect`) returns
  `make_empty_multirange` before calling the internal, and verbatim C has the
  same two-level structure. The one caller that does reach it,
  `multirange_intersect_agg_transfn`, is an agg-state carve. Seeded
  empty-operand setop inputs left it uncovered, which is what proved the fence.

## CLOSED — 26 lines

- **Soft-error (escontext) plane, 13 of the 18 originally listed**: a whole new
  comparison plane (OCCURRED flag + captured sqlstate + valid-input image +
  soft/hard verdict agreement), on `range_in`, `multirange_in`, and the
  canonical family. io.rs 157/171/307/326/344, builtins.rs 105/467/472,
  lib.rs 525/530, mr io.rs 152, mr builtins.rs 80.
- **NULL-argument arms, 6**: rangetypes builtins.rs 173-176/189 via
  `range_constructor3` with a SQL-NULL flags argument (non-strict, so genuinely
  reachable: `select int4range(1,2,NULL)`); mr builtins.rs 220 via a NULL array
  member.
- **daterange dispatch, 1**: lib.rs 534, via daterange as a fourth pinned
  constructor instantiation.
- **Toast/short-header, 5**: rangetypes builtins.rs 29/47 (short-header outer
  range image reaching `arg_range`'s Owned arm); mr builtins.rs 226/228/229
  (short-form array members).
- **Misc, 3**: io.rs 81 (`range_parse_flags`' LENGTH check — the original note
  called this the invalid-character arm, but the characters were already
  covered; a fixed-2 driver could never reach the length check), mr lib.rs 395
  (`multirange_get_union_range` on an EMPTY multirange — both the driver AND the
  oracle entry short-circuited on `rangeCount == 0`, so the compare existed but
  could never see it).

## Kani infeasibility-proof candidates (from exception rows, not gaps)

Unchanged from the adjudication, plus the two reclassified rows above, which are
the same shape — fenced by control flow rather than a const the compiler folds:

- `rangetypes/src/lib.rs:353` — datum_write's toast-pointer panic, fenced by
  detoast_bound_packed flattening every by-ref bound first.
- `multirangetypes/src/lib.rs:240,245-252` — canonicalize's comparator
  error-capture, fenced by the btree comparators being total.
- `multirangetypes/src/lib.rs:271-272,283-284` — range_union_internal's
  empty-operand short circuits, fenced by the empty-skip above them.
- `multirangetypes/src/lib.rs:799` — range_minus_internal's non-overlap short
  circuit, fenced by the call site's overlap test.
- `multirangetypes/src/lib.rs:834` — as above (newly added).
