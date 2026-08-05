# tsrank_diff divergences (lane p1-laneae)

Oracle: vendored PostgreSQL 18.3 C (csrc/tsvec/tsrank.c @ 62d6c7d3df,
shasum-verified). NOT Docker-adjudicated here — ground-truth per the
fuzzuproof-crate skill is the coordinator's step for anything that survives
triage as SQL-reachable.

## 1. ROBUSTNESS NOTE (harness-domain, SQL-unreachable): shared operand
##    distance breaks the Rust rank kernel's distance->item map

- Found by the first 300s local smoke (minimized:
  `[6, 1, 32, 32, 32, 97, ...]`, variant 6 = ts_rankcd_ttf, method 17):
  tsvector `'a':102`, generated tsquery image = `'a' | 'a':C` with BOTH
  QueryOperands pointing at ONE deduplicated pool string (equal
  `distance`). Rust returned 0e0 (0x00000000); C returned 1.4426951e-1
  (0x3e13bb63).
- Mechanism: C resolves a TS_execute operand to its per-item
  QueryRepresentation slot by POINTER IDENTITY
  (`QR_GET_OPERAND_DATA: val - GETQUERY(query)`, tsrank.c:560). The Rust
  port (crates/backend/utils/adt/tsrank/src/rank_cd.rs `QueryRep::item_index`)
  resolves by `binary_search_by_key(distance)` — ambiguous when two
  operands share a distance: the `'a'` (no weight filter) lookup can land
  on the `'a':C` slot whose `exists` stays false, so the cover search finds
  nothing and the rank collapses to 0.
- Triage: SQL-UNREACHABLE image shape. Real PG never emits shared
  distances: `tsqueryin`/`tsqueryrecv`/`QTN2QT` append every operand's
  string separately (no pool dedup), so `distance` is unique per operand
  in every stored tsquery. The ambiguity was an artifact of tsq_gen's
  pool dedup; the generator now matches the reachable envelope
  (`push_operand`, no dedup) and this input class is out of the campaign
  domain.
- Standing risk worth recording: the Rust kernel's assumption is
  "distance is injective over QI_VAL items" — true for every image PG can
  produce, but an adversarially crafted stored datum would rank wrong
  (not crash). Same trust boundary as every other stored-image reader in
  the family. tsquery image validation on the receive path belongs to
  adt/tsquery_core (p1-laneaf) — flagged to that lane.
- Regression seed: corpus keeps the minimized pre-fix input REMOVED
  (domain change); the smoke tests in tsrank_diff.rs pin the fixed
  generator via `smoke_all_variants`.

## 2. REAL DIVERGENCE (SQL-REACHABLE, pgrust-bug candidate): stable-vs-
##    unstable dedup survivor in SortAndUniqItems changes the rank VALUE

- Found by the second 300s smoke. Minimized driver input (hex):
  `200002201200000000362078202020202020000200000016006144322c322c332c342c
   354220713a39207a3a313030adadffffffffffffffffffffffadadadadadadadadadad
   f90f05ffff203f2020207a3a313000322000040000f90f00613a312068323a31303120
   623a31303281110003090130adad000000f90f05`
  → ts_rank_wttf, generated tsquery holding MULTIPLE operands with the
  SAME lexeme "a" but DIFFERENT (weight, prefix) flags. Rust
  3.3333333e-21 (0x1d7bdc0b) vs C 0e0 (0x00000000).
- Mechanism: `SortAndUniqItems` (tsrank.c:170) sorts collected
  QueryOperand pointers with PG's UNSTABLE `qsort_arg` keyed ONLY on the
  operand string (`compareQueryOperand` = tsCompareString, no flags),
  then dedups keeping the first of each equal run. When two operands
  share a lexeme but differ in weight/prefix, WHICH operand survives is
  a sort-tie artifact. The Rust port (rank.rs `sort_and_uniq_items`)
  uses a STABLE sort + dedup_by — a DIFFERENT, deterministic survivor.
  The survivor's `prefix` changes the `find_wordentry` match set and its
  `weight` feeds nothing in AND-rank but prefix does — here C's survivor
  matched entries (one pair's curw underflowed to exactly 0.0 with
  subnormal weights) while Rust's survivor matched none (res -1 →
  1e-20 → /len). Same root class as tsvector_core DIVERGENCE-1b
  (unstable qsort tie order), but here the tie changes a SCALAR RESULT,
  not just image layout — it cannot be ratified away as a non-surface.
- Reachability: REAL — `SELECT ts_rank(tv, 'a | a:C'::tsquery)` puts two
  same-lexeme operands with distinct flags into one query. C's own
  result is tie-order-arbitrary (qsort_arg implementation detail), so
  "parity" here means matching PG's specific qsort_arg tie behavior.
- Status: driver carve `has_flagged_lexeme_tie` (tsrank_diff.rs) skips
  the class pending adjudication; NOT Docker-ground-truthed yet.
  Options for the crate: (a) replicate pg_qsort_arg's tie behavior in
  sort_and_uniq_items (exact parity), (b) ratify a documented
  tie-survivor rule as a non-surface with a sorted-multiset-style gate
  (needs Michael). Coordinator to triage.

---

## ADJUDICATION of #2 (lane coordinator, 2026-07-31 — Docker postgres:18.3)

CONFIRMED pgrust-bug, FIXED. Ground truth that tie order is REAL PG
behavior (not an oracle artifact) — real 18.3 gives different ranks when
only the text order of two same-lexeme operands flips:

    ts_rank('aab:1 u:2', 'u|v|w|x|y|z|aa:*|aa') = 0.008684673
    ts_rank('aab:1 u:2', 'u|v|w|x|y|z|aa|aa:*') = 0.017369347
    ts_rank('aab:1 u:2', 'aa:*|aa') = 0
    ts_rank('aab:1 u:2', 'aa|aa:*') = 0.06079271

"Parity" therefore means matching PG's qsort_arg equal-key output order
exactly. Fix: `crates/backend/utils/adt/tsrank/src/qsort.rs` — verbatim
Rust port of lib/sort_template.h pg_qsort (same per-crate-copy convention
as gistproc/analyze/rangetypes_gist/brin_minmax_multi) — now used by
`sort_and_uniq_items` (rank.rs). Option (b) (ratify a tie-survivor rule)
rejected: the tie changes a scalar SQL result, and exact parity is
mechanically available.

Validation: driver carve `has_flagged_lexeme_tie` RETIRED (strict f32
plane restored for flagged-tie queries); full corpus replay clean;
fresh 301s smoke = 5,691,644 execs, zero divergences with the tie class
back in the domain.
