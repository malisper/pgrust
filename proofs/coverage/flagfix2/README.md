# excreview-flagfix2 evidence bank (2026-07-31)

Lane: ledger flag-resolution for `proofs/coverage/phase1-exceptions.tsv`,
resolving the excreview-banked FLAGGED rows (the third review pass, main
d6494a5209: 338 live FLAGGED tokens). Branch `proofs/excreview-flagfix2`.
Binding adjudication honored: regress-measured is NOT measured — every
"measured" disposition below cites proof (fulltree Kani capture) or fuzz
(lcov artifact) evidence only.

NOTE on the review lane's index files: `excreview-banked-flags.tsv` /
`excreview-banked-mech.tsv` line numbers reference a PRE-d6494a5209 ledger
state and do NOT match the live file — resolve against the ledger's own
FLAGGED tokens, not those indices (this lane's first join against the stale
indices produced 91 false "unmeasured" verdicts before re-anchoring).

## Dispositions (338 flagged rows)

- **136 retired — kani-measured** (proof-covered-unmeasured rows whose lines
  the published fulltree capture `proofs/coverage/files/*.json` already
  credits: 102 adt_date, 26 int, 6 jsonpath_exec, 1 varchar, 1 varlena;
  verified per-row against the kani arrays; 113 also visible as COVERED
  regions in `proofs/coverage/fulltree/kaniraw/`). Fulltree-join rule:
  rows deleted, lines already measured.
- **62 retired — kani double-counts** (macro-decl / fmt-cont / fn-signature
  rows whose lines the files/ kani arrays credit). Deleted.
- **13 retired — fuzz double-counts** (varlena `bytea_output` accessors
  902-907 and the `hashtext_nondeterministic` dispatch-gate lines 112-123;
  verified against the banked fleet lcovs on branch proofs/evidence-bank:
  fuzz-vltext/vlmisc/vlbytea_diff-fleet-*.lcov.gz). Deleted.
- **23 retired — varbit wrappers fuzz-measured (new evidence)**: the
  recv/send/typmodin fc-wrapper and cstring[]-parse rows cited proofs that
  bypass them. Resolution: ported the `varbit_diff` harness from
  proofs/p1-laned (not previously on main), added wrapper arms (sentinel
  family 0xFF: fc_bit_recv/fc_varbit_recv vs bits_recv core over independent
  StringInfo cursors; fc_bittypmodin/fc_varbittypmodin over driver-built 1-D
  cstring[] images vs anybit_typmodin; fc_bit_send/fc_varbit_send wire-equal
  + recv-roundtrip), seeded `fuzz/corpus/varbit_diff/flagfix2-*`, replayed
  under instrumentation. All 23 lines DA>0 in
  `fuzz-varbit_diff-local-20260731.lcov.gz` (this dir). Rows deleted.
- **6 re-filed — varbit fc_bit_typmodout 260-265** -> excluded-state: both
  callers (oids 2920/2921 bittypmodout/varbittypmodout) are
  excluded(blocked: format-machinery) — function-grain exclusion carried at
  line grain; the old proof citation named the wrong functions.
- **22 corrected — adt_date proof citations** (kept, resolved tokens):
  - 528-531 fc_interval_send: now cites datetime-b rem::eq_interval_send —
    green release-gate, PROMOTED 2026-07-31 (the flag's "no proved
    component" reflected the STALE routes row: USER_FACING_FUNCTIONS.tsv
    oid 2479 still reads wall+tested, predating the promotion — left for the
    proofs-program owner, flagged in the row text).
  - 545-550 cmp macro body: family citation fixed datetime-cmp ->
    proofs/interval-cmp (verified: check! cells drive fc_interval_eq..cmp).
  - 621-626 / 637-642 / 852-853 justify_hours / justify_interval /
    interval_time: justifications now state wall + proved(spots) honestly
    and name the spot cells (verified the harnesses call the fc wrappers).
- **22 re-filed — class mismatches**: pseudotypes 97-98 no_flinfo panic ->
  unreachable-arm (name/builtins.rs:101 precedent); pseudotypes 106-123
  unported_delegate! stubs -> phase1-carve:unported-stub (varchar
  fc_unported! precedent); int8 334-341 const fn b body -> const-eval-only;
  int8 346 -> const-eval-only table-head (srf carve was wrong; the srf row
  is line 347, table-excluded).
- **16 deleted — off-denominator** (brace/continuation/blank lines verified
  OFF the v2 denominator at current main via sloc_rules.py, incl. the three
  rows the stale capture sloc arrays contradicted: adt_date lib.rs
  1078/1083 + adt_datetime decode.rs 2001 — match-arm/else-brace lines).
- **4 int defensive-oom rows replaced (STALE-MECHANISM FINDING, see below)**.
- **7 line-drift re-anchors/deletions**: parser.rs 92->90, 166->164,
  171->169 (kept, re-anchored); io.rs 114->97 (+ companion rows 98-99);
  ops.rs:334 deleted-and-re-filed as instrument-false-red:expr-cont with
  fuzz evidence (see below); varlena lib.rs:714 deleted (the refpoint-retry
  construct it described is ALREADY ledgered at 721-732; 714 is the
  reachable empty-needle guard); int8 346 handled above.
- **2 C-citation fixes**: rangetypes lib.rs:353 now cites
  rangetypes.c:2793-2796 (identical elog exists; the old row claimed no C
  counterpart); varchar builtins.rs 15-16 flinfo checks re-filed
  unreachable-arm with honest "none (C fmgr derefs flinfo unchecked)".
  Also tsvector_core query.rs:68 (unrecognized QueryItem panic) re-filed
  unreachable-arm ("C reads QueryItem.type via unchecked struct pun").
- **1 deleted — malformed row** (stray TSV column header pasted as data).
- **Untouched (owned elsewhere)**: 13 MOOT mb/conv rows (remediation-lane
  ownership per the ledger header) and 9 input-dependent/fence rows (pglz
  >21.4MB, encode/decode MaxAllocSize, multirange_intersect fence — the
  boundary-guard audit lane owns these; possible real defects).

## New fuzz evidence (this dir)

- `fuzz-varbit_diff-local-20260731.lcov.gz` — targeted replay of the
  flagfix2 wrapper arms + seeds (harness ported/extended on this branch).
- `fuzz-rangetypes_diff-local-20260731.lcov.gz` — full-corpus
  rangetypes_diff replay at this branch tip + `flagfix2-union-*` seeds.
  Shows ops.rs 316/319 (union/merge empty short-circuits) DA 40/44 and the
  range_union_internal result expression EXECUTING 1513-1575x with line 334
  (the `Ok(UnionResult::New(` opener) carrying NO DA record — an
  instrument false-red, re-filed accordingly.

## Stale-mechanism finding (fabricated citation)

The four int `exception:defensive-oom` rows (lib.rs 145/286/287/308) all
cited "each arm is wrapped in types_error::never_reached! (created this
lane...)" — **no `never_reached!` exists anywhere in the tree**; the cited
mechanism was never built. What the lines actually are: 145/287 brace-only
(off-denominator, deleted), 286 the `?`-unwind success-path call line of
buildint2vector inside int2vectorin, 308 the output-loop space separator —
both reachable, neither an OOM arm. The REAL OOM arms (143-144, 280-281,
299-300) had NO rows; honest defensive-c-parity rows filed for them
(arrayfuncs io.rs:546 precedent). One more stale-mechanism datapoint for
the law: the comment/justification named a mechanism the code never had.

## Equation re-balance (delta form)

Full per-crate re-derivation from the published fulltree capture alone is
not possible: the capture's fuzz axis carries only the rf float/geo lcovs,
while the original lanes balanced their equations against lane fuzz lcovs
that are only partially banked as artifacts (evidence-bank has 10; the rest
live in per-lane records). This lane therefore states the balance as a
delta, verified row-by-row: **every deleted row's line is either (a)
measured under proof-or-fuzz by a NAMED artifact (files/*.json kani arrays;
evidence-bank fleet lcovs; the two local lcovs above), (b) off the v2
denominator at current main (sloc_rules.py), or (c) recorded below as a new
residual.** Added/edited rows keep their crates' excepted sets aligned
(no measured line remains excepted; every previously-excepted-now-vacated
line is measured or listed).

RECORDED residuals introduced (uncovered − excepted = residual, reasons):

- int/lib.rs 286 — success-path `?` line of buildint2vector call in
  int2vectorin; reachable on every successful parse; the int crate's
  fn_diff fuzz plane lives on an unmerged lane branch, no plane on main.
- int/lib.rs 308 — int2vectorout space-separator branch; same gap.
- tsvector_core/parser.rs 92 — reachable pg_mblen call (non-at-end);
  no tsvector fuzz plane on main.
- tsvector_core/parser.rs 166, 171 — reachable token-end return / oprisdelim
  dispatch; same gap.
- varlena/lib.rs 714 — text_position_next empty-needle early return;
  reachable regardless of encoding; no text_position fuzz plane on main
  (the banked vltext lcov does not reach it).

All six are thin, input-reachable lines whose crates lack an on-main fuzz
plane for the construct; each needs a follow-up fuzz arm (int2vector io,
tsvector parser, text_position) — none is exception-eligible, so they are
carried as residuals rather than re-excepted.

## Owed / follow-ups

- USER_FACING_FUNCTIONS.tsv oid 2479 (interval_send) status predates the
  2026-07-31 dark-harness promotion (SUITE datetime-b rem::eq_interval_send
  green) — routes-ledger owner should refresh it.
- adt_date `c_counterpart` date.c-vs-timestamp.c: the unambiguous subset
  (84 rows: interval_*/timestamp_*/timestamptz_*-prefixed functions, which
  live in timestamp.c upstream) was corrected in-place. Remaining date.c
  citations naming grouped/ambiguous entries ("comparison",
  "timetz_zone/..." bundles, time_support) were left — verifying each needs
  the vendored C tree, owed to a follow-up.
- The published fulltree capture predates the datetime-b kaniraw landing
  (ecee538958) and the adt_date/lib.rs bugfix drift (+6/+11 lines after
  ~line 112/787); a fulltree merge re-run must execute from the
  .wt-covfulltree layout (kaniraw region paths embed that root — a re-run
  from another worktree silently produces a ZERO-coverage join; this lane
  hit and reverted exactly that).
- Six residual lines above: drivable by three small fuzz arms.
