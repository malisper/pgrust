# p1-regexp — adt/regexp evidence banking (takeover of p1-laneag, 2026-07-31)

CHARTER: p1-laneag completed adt/regexp + adt/like and flipped both `done` on
main, but died before banking its evidence off the lane branch — exactly the
audit-failure class EVIDENCE-DEBT.md exists to prevent ("evidence on the lane
branch alone fails audit"). This lane (p1-regexp) took the claim over and
relocated everything of record to main:

- 305 adt/regexp per-line exception rows -> proofs/coverage/phase1-exceptions.tsv
  (the 89 adt/like rows were already relocated by the evidence-bank lane).
- 72 tested(differential) ledger rows (45 regexp + 27 like) -> proofs/USER_FACING_FUNCTIONS.tsv.
- 87 route rows -> docs/verification/phase1-routes.tsv; ranking DONE annotations.
- The laneag fuzz drivers + oracles (fuzz/core/csrc/regexfam verbatim Spencer
  engine, pg_regexp_io.c, pg_like_io.c), corpora of record (regexp_diff 5614,
  like_diff 2706), and the fleet artifact bank
  fuzz/fleet-evidence/regexp_diff-1785535304 (job
  pgrust-fuzz-campaign-1785535304-795f-37663, 3.65M execs / 25.8 CPU-h, ZERO
  wrapper-plane divergences).
- The evidence-rebuild lane's measured reconstructions
  (proofs/coverage/evidence-rebuild, from branch proofs/evidence-rebuild
  @ 4a558a43bf) including the adt/like lcov of record (278/367 EXACT).

## The measured leg rebuilt here (this directory)

adt/regexp's fuzz lcov was NEVER banked anywhere (the fleet artifact bank has
crashes/logs only — the runner's slow-unit triage outlived its deadline before
the coverage pass; the done-flip postdated the gate audit so the debt was never
enumerated). Rebuilt by the cov-resweep recipe: local full-corpus replay of the
committed corpus (fuzz/corpus/regexp_diff, 5614 units) under
`cargo fuzz coverage` at a tree whose adt/regexp crate sources are verified
bit-identical (git diff empty) to both the laneag gate tip (e6387a0c87) and the
fleet campaign sha (325985a437), joined with
`merge-coverage.py --sloc-rule v2 --exclude-const-tables` and the explicit
empty census (fuzz-only capture).

REPLAY DEVIATION (documented, not silent): the cargo-fuzz batch runner aborted
(exit 71) on batch-11 of 14. Root cause = CUMULATIVE RSS over the ~401-unit
batch (the corpus contains engine compile-bomb shapes; libfuzzer's rss/malloc
ceiling), NOT a crashing unit: all 401 batch-11 units replayed individually
exited rc=0 (see `replay-excluded.txt` — zero exclusions). Merge inputs = the
13 clean batch profraws + the 401 per-unit profraws; the aborted run's
batch-11.profraw was discarded.

RESULT: fuzz-measured 861/1166 (73.84%) — EXACT reproduction of the recorded
numerator. Cross-check: 305 uncovered SLOC lines match the 305 banked
exception rows 1:1 (unaccounted = 0; zero exception rows on covered lines).
Closed equation: 1166 = 861 fuzz-measured + 305 recorded executable exceptions.

RE2 linkage witnessed on this build (GL-STRAGG-1 class): the regexp_alt test
binary dynamically links libre2.11.dylib (otool -L) with cfg(have_re2) set;
the engine-GUC dispatch surface remains carved OUT per the laneag scope.

Files:
- fuzz-regexp_diff-local-20260731.lcov.gz — the lcov of record
- summary.json / files/ / scope.txt / excluded-tables.json — the v2 join
- replay-excluded.txt — units excluded from the replay merge (crash class)
