# fuzz/divergences/GAPS-p1-laneac.md — GENUINE FUZZ GAPS (NOT exceptions)

Lane p1-laneac, 2026-07-31. adt/rangetypes + adt/multirangetypes.

These 33 in-scope v2-SLOC lines are **reachable code that the differential corpus never
drives**. They are deliberately NOT written as exception rows in
`proofs/coverage/phase1-exceptions.tsv`: an exception says "this line cannot or must not be
covered", and every line below CAN be. Recording them as exceptions would be the
gate-blindness failure the campaign keeps getting burned by.

**Consequence: the done-gate for these two crates does NOT close at 100%.**

Accounting over the 2,982 in-scope non-test v2-SLOC lines of the two crates:

| bucket | lines | % |
|---|---:|---:|
| fuzz-measured (2 x 10M-exec CI cluster campaigns) | 2379 | 79.78% |
| recorded executable exceptions | 570 | 19.11% |
| **genuine fuzz gaps (this file)** | **33** | **1.11%** |
| accounted total | 2949 | 98.89% |

The exception census by class is in the report; the dominant class is `excluded-state`
(458 lines = the claim's ratified agg-state / SRF / engine:planner / typcache-subtype
carves), then `instrument-unmappable` (84 = fc*! macro invocation sites and multi-line
call continuations whose counts land on neighbouring lines), `unreachable-arm` (14),
`const-eval-only` (13 = the b() builtins table), `defensive-c-parity` (1).

## SOFT-ERROR (escontext) PLANE — 18 lines — THE DOMINANT GAP

- `crates/backend/utils/adt/rangetypes/src/io.rs` lines 157, 171, 307, 326, 344
- `crates/backend/utils/adt/rangetypes/src/builtins.rs` lines 105, 467, 472
- `crates/backend/utils/adt/rangetypes/src/lib.rs` lines 525, 530
- `crates/backend/utils/adt/multirangetypes/src/io.rs` lines 152
- `crates/backend/utils/adt/multirangetypes/src/builtins.rs` lines 80

NEITHER driver constructs a SoftErrorContext / ErrorSaveNode: `grep -n SoftErrorContext
fuzz/core/src/*_diff.rs` returns nothing. Every `return Ok(None)` soft-failure edge is
therefore undriven, and with it the whole soft-input plane that backs
pg_input_is_valid() and COPY ... ON_ERROR ignore. The C oracle already threads
escontext (it is a range_in/range_parse parameter), so this is purely a driver gap.
TO CLOSE: add a payload bit selecting soft vs hard error mode; on soft, pass an armed
ErrorSaveNode on the Rust side and the matching escontext on the C side, and compare a
FOURTH plane: (error_occurred flag, captured sqlstate) instead of the thrown verdict.
Arms needing it: range_in, multirange_in, range_constructor3 (flags), the fc_*_canonical
family, and make_range/canonicalize via an overflowing int4range/int8range bound
(i32::MAX / i64::MAX upper bound, which is what reaches lib.rs:525/:530).

## DATERANGE canonicalize DISPATCH — 2 lines

- `crates/backend/utils/adt/rangetypes/src/lib.rs` lines 534, 535

canonical_adjust_date itself is FULLY covered (lib.rs:649-671, every arm incl. both
IS_VALID_DATE overflow ereports) because the driver calls fc_daterange_canonical
directly. What is NOT covered is canonicalize()'s F_DATERANGE_CANONICAL dispatch arm,
because no daterange value is ever built THROUGH make_range.
TO CLOSE: add daterange as a fourth pinned instantiation to the image/constructor arms
(the typcache mock already has a daterange entry — it is used for the canonical/subdiff
arms — so this is mostly wiring an existing pin into build_image + the ctor arm).

## TOASTED / DETOASTED BOUND + ARGUMENT PATHS — 9 lines

- `crates/backend/utils/adt/rangetypes/src/lib.rs` lines 415, 416, 418, 419
- `crates/backend/utils/adt/rangetypes/src/builtins.rs` lines 29, 47
- `crates/backend/utils/adt/multirangetypes/src/builtins.rs` lines 226, 228, 229

detoast_bound_packed's external-pointer/compressed arm and the arg_range /
arg_multirange RangeArg::Owned detoast arm. Both drivers feed FLAT images only.
NOTE these paths ARE exercised by the crate's own tests (rangetypes/src/tests.rs
bound_detoast: external_bound_is_inlined / compressed_bound_is_decompressed) — but
in-crate tests count for NOTHING under the campaign metric, so they stay gaps here.
TO CLOSE: an arm that builds a toast-pointer / pglz-compressed numrange bound behind the
detoast seam (the tests.rs install_test_detoast harness is the model) and feeds the same
image to both sides. Moderate cost: the C oracle needs the matching detoast shim.

## NULL-ARGUMENT ERROR ARMS — 7 lines

- `crates/backend/utils/adt/rangetypes/src/builtins.rs` lines 173, 174, 175, 176, 189
- `crates/backend/utils/adt/multirangetypes/src/builtins.rs` lines 186, 220

null_flags_arg (range_constructor3 with a NULL flags argument) and null_member
(multirange_constructor2 with a NULL range member). The fc_call helper always passes
non-NULL Datums, so no arm ever sets argisnull.
TO CLOSE: cheap — give fc_call a per-argument null mask driven by a payload bit and let
the C side pass the same PG_ARGISNULL pattern. This also widens the fc-wrapper plane for
every other arm (all the strict-vs-nonstrict wrappers).

## MISC REACHABLE PATHS — 4 lines

- `crates/backend/utils/adt/rangetypes/src/io.rs` lines 81
- `crates/backend/utils/adt/multirangetypes/src/lib.rs` lines 395, 834

io.rs:81 = range_parse_flags's invalid-flags error (a 2-char flags string that is not one
of [] [) (] (); the driver only ever mints valid pairs). lib.rs:395 =
multirange_get_union_range on an EMPTY multirange (returns make_empty_range).
lib.rs:834 = multirange_intersect_internal's early return.
TO CLOSE: all three are seed/arm-input gaps, not structural — feed arbitrary 2-byte flag
strings to the constructor3 arm, and drive the internals arm with a zero-range image.

## Kani infeasibility-proof candidates (from the exception rows, not gaps)

Four `unreachable-arm` rows are fenced by CONTROL FLOW rather than a const the compiler
folds, so they do not meet the exceptions-file preamble's "every unreachable-arm row is
const-decided" bar. The preamble's own guidance is to prefer promotion to a Kani
infeasibility proof; these are the candidates:

- `rangetypes/src/lib.rs:353` — datum_write's toast-pointer panic, fenced by
  detoast_bound_packed (lib.rs:409) flattening every by-ref bound first.
- `multirangetypes/src/lib.rs:240,245-252` — multirange_canonicalize's comparator
  error-capture, fenced by int4/int8/numeric btree comparators being total.
- `multirangetypes/src/lib.rs:271-272,283-284` — range_union_internal's empty-operand
  short circuits, fenced by the empty-skip at lib.rs:260/:263.
- `multirangetypes/src/lib.rs:799` — range_minus_internal's non-overlap short circuit,
  fenced by the call site's overlap test.
