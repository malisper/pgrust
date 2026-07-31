# fuzz/divergences/GAPS-p1-laneac.md — GENUINE FUZZ GAPS (NOT exceptions)

Lane p1-laneac. adt/rangetypes + adt/multirangetypes.

## ZERO GAPS REMAIN. 2026-07-31, final state.

Opened with 33 gap lines; 31 were closed by extending the drivers, 2 were
proved fenced and moved to `proofs/coverage/phase1-exceptions.tsv` with
evidence. Every number below is MEASURED (`cargo fuzz coverage` over the
committed corpus, merged under SLOC-v2), and the accounting reconciles to zero
unaccounted and zero double-classified lines:

| file | sloc | fuzz | excepted | gap |
|---|---:|---:|---:|---:|
| multirangetypes/builtins.rs | 594 | 407 | 187 | 0 |
| multirangetypes/io.rs | 192 | 172 | 20 | 0 |
| multirangetypes/lib.rs | 649 | 621 | 28 | 0 |
| rangetypes/builtins.rs | 467 | 260 | 207 | 0 |
| rangetypes/io.rs | 319 | 296 | 23 | 0 |
| rangetypes/lib.rs | 425 | 341 | 84 | 0 |
| rangetypes/ops.rs | 336 | 314 | 22 | 0 |
| **total** | **2982** | **2411** | **571** | **0** |

fuzz + excepted = 2982 = sloc. (Baseline before the gap-closing passes:
2379 fuzz / 79.78%.) Exception census by class: excluded-state 458,
instrument-unmappable 83, unreachable-arm 16, const-eval-only 13,
defensive-c-parity 1. One stale instrument-unmappable row (lib.rs:504) was
DELETED when the make_range arm made the line measurable — an exception row a
measurement contradicts is removed, never left double-classified.

## How the last 5 closed

- **lib.rs 415/416/418/419 (compressed bound detoast)**: `pglz_decompress`
  vendored VERBATIM from src/common/pg_lzcompress.c @ 62d6c7d3df into the
  oracle (plus the 4B_C varatt macros and toast_compression.c's
  pglz_decompress_datum as a mechanical transform). The driver mints
  inline-compressed numrange bounds with the SHIPPED compressor
  (STRATEGY_ALWAYS — numeric images are under the 32-byte DEFAULT minimum; the
  compressed FORM is what is under test, not the compress-policy). Both sides
  consume identical bytes; decompression is the compared computation. This
  fixed a second latent oracle-fidelity hole: PG_DETOAST_DATUM_PACKED was the
  identity (upstream decompresses), and the FULL detoast needed decompression
  too because range_serialize compares bounds BEFORE it detoasts them, so
  numeric_cmp sees the compressed datum first.
- **lib.rs 535 (daterange canonicalize soft edge)**: the planned
  unreachable-arm row would have been FALSE — the fence is not total. Shipped
  range_in threads a live escontext into make_range -> canonicalize for any
  range type, so real PG reaches this line via
  `pg_input_is_valid('[2024-01-01,...huge...]','daterange')`. Closed instead
  with an internal-API arm (the mr oracle's precedent) driving make_range
  DIRECTLY, hard and soft, over int4range/int8range/daterange — make_range is
  the exact function real range_in calls, so the arm models a real caller
  shape. Constructors cannot serve here: verbatim C and shipped Rust alike
  hardcode a NULL escontext in range_constructor2/3.

The corrupt-pglz error (XX001) joined the shared errcode table as class 15
(12 = range side's last; 13/14 belong to the multirange half), both err_class
maps updated, and the cross_target_err_class_agreement guard extended.

## Residual limitation (NOT a line gap; recorded for honesty)

The EXTERNAL (VARTAG_ONDISK) form of a bound is still never fed. It shares
lines 415-419 with the compressed form (one `if 1B_E || compressed` block), so
the lines are fuzz-measured; what remains untested is the toast-FETCH behavior
behind them: detoast_attr would call the `toast_fetch_datum` seam, which has no
in-process producer — a real fetch enters C's toast_fetch_datum
(detoast.c) and reads a TOAST relation via the access-method stack, which no
in-process oracle can satisfy. This is the bytea-cmp / proofs/typcache-inst
"DETOASTING OUT OF SCOPE — images model the post-detoast caller contract"
fence. The crate's own tests (rangetypes/src/tests.rs bound_detoast,
external_bound_is_inlined) pin the Rust-side behavior through a mock store.

## Kani infeasibility-proof candidates (from exception rows)

Fenced by control flow rather than a const the compiler folds; the exceptions
preamble prefers promoting these to proofs:

- `rangetypes/src/lib.rs:353` — datum_write's toast-pointer panic, fenced by
  detoast_bound_packed flattening every by-ref bound first.
- `multirangetypes/src/lib.rs:240,245-252` — canonicalize's comparator
  error-capture, fenced by the btree comparators being total.
- `multirangetypes/src/lib.rs:271-272,283-284` — range_union_internal's
  empty-operand short circuits, fenced by the empty-skip above them.
- `multirangetypes/src/lib.rs:799` — range_minus_internal's non-overlap short
  circuit, fenced by the call site's overlap test.
- `multirangetypes/src/lib.rs:834` — multirange_intersect_internal's
  empty-operand short circuit, fenced by the caller's MultirangeIsEmpty
  pre-check (C-identical structure).
- `multirangetypes/src/builtins.rs:186` — constructor2's argisnull arm,
  strict-unreachable (proisstrict=t, ground-truthed on 18.3).
