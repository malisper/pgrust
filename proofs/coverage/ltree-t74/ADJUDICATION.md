# contrib/ltree — floor adjudication + coverage number of record (task #74 closeout)

Date: 2026-08-03. Adjudicated per the t74 handoff acceptance criteria (anti-vacuity
rules included). All verdicts below were read from PRIMARY S3 artifacts with raw
`aws s3 cp` of `campaign-stats.json` (the only source of truth), NOT from the
fetch-script banner and NOT from any monitor channel. Job identity was verified
inside each fetched `MANIFEST.txt` (`job_name:` / `git_ref:`) before quoting numbers.

## Jobs

Both fuzz legs at code sha `33db5df8aed37a37209c99c6d3d744e6269d43f2`
(branch `final/ltree-cexact`; the 3 later commits to tip `c6418dc78feb` are
`#[cfg(test)]`+docs only, so the floor binary == this sha's code).

| leg | job | S3 verdict |
|---|---|---|
| FLOOR | `pgrust-fuzz-campaign-1785783435-3e89-34711` | outcome=complete, rc=0, ltree_diff outcome=run, execs 10,000,000/10,000,000, divergences_total 0, sanitizer_artifacts_total 0, crashed_early_total 0, corpus 7386->9266, cov_lines 3160, wall 10171s |
| CONFIRM | `pgrust-fuzz-campaign-1785783446-766b-34860` | outcome=complete, rc=0, ltree_diff outcome=run, execs 10,000,000/10,000,000, divergences_total 0, sanitizer_artifacts_total 0, crashed_early_total 0, corpus 7386->9251, cov_lines 3160, wall 11930s |

Acceptance criteria: ALL met on BOTH legs, including the anti-vacuity checks —
`targets[]` non-empty (1 target, outcome `run`, not `crashed-early`), execs at the
full 10M budget (not a short run), `cov_lines` 3160 (non-zero capture),
`corpus_out` > 0, `ltree_diff/divergences/` prefix EMPTY in S3 for both jobs, and
`ltree_diff/coverage.lcov` exists (14.5 MiB, 278,602 DA records; 2,737 DA records
across the 7 `crates/contrib/ltree/src/*.rs` SF blocks). Banked stats + MANIFESTs
are in this directory (`campaign-stats-{floor,confirm}.json`, `MANIFEST-{floor,confirm}.txt`).

## Coverage NUMBER OF RECORD

Source: the FLOOR job's own `ltree_diff/coverage.lcov` (the local pre-floor join is
explicitly NOT the record), joined with
`proofs/coverage/merge-coverage.py --sloc-rule v2` (const-table exclusion default)
against the `final/ltree-cexact` tree (head `c6418dc78feb`, crate sources code-identical
to the floor sha), scope `crates/contrib/ltree`, census closed with the explicit
EMPTY census (`proofs/coverage/evidence-rebuild/empty-census.tsv`; fuzz-only join,
zero expected kani harnesses -> `census_closed: true`).

```
1421 / 2321 whole-crate v2-SLOC = 61.22% fuzz-measured
ex-gist (gist.rs is a scope carve, OUT per claim row): 1421 / 1713 = 82.95%
accounting: 2321 = 1421 measured + 900 exception rows, residual 0
```

Per-file (sloc / fuzz): array.rs 54/50, crc.rs 12/6, gist.rs 608/0 (carved),
io.rs 645/627, lib.rs 506/282, op.rs 347/315, repr.rs 149/141. The floor capture
joins line-identical to the local pre-floor replay (same per-file split), so the
recorded local number was confirmed rather than replaced.

Artifacts here: `summary.json`, `verification-coverage.tsv`, `census.json`,
`excluded-tables.json` (the v2 reviewability condition), and the floor lcov trimmed
to the crate's SF blocks (`coverage-33db5df8ae-job1785783435-ltree.lcov.gz`) — the
trimmed lcov was re-joined and verified to produce the IDENTICAL
verification-coverage.tsv before banking.

## Exception rows (900)

Generated from THIS floor capture by the function-anchored generator (t74 scratch
`ltree-exc-gen.py`), NOT re-imported from the dead lane's line-anchored 887 rows
(19 of those were already stale at the lane's own tip). `needs_classification=0`.
Classes: 849 excluded-state (gist/index-AM environment + ltreeparentsel planner
carve), 33 instrument-unmappable, 7 unreachable-arm, 6 encoding-carve (non-C-ctype
crc fold arm, pinned unwitnessable under the campaign's C-ctype pin), 5
defensive-c-parity. Appended to `proofs/coverage/phase1-exceptions.tsv`.

## Mutants leg — TRAILING, not a gate

`pgrust-mutants-audit-1785783617-6aa9-77279` (charted at branch tip `c6418dc78feb`)
produced NO artifacts in S3 (no `mutants-audit/<sha>/` prefix for either the tip or
the floor sha; no key containing the job id). Died pre-artifact — consistent with
the JOB_EPHEMERAL 40Gi foot-gun (runner-advance fix pending, task #156). Resubmit
waits on the runner advance, or an operator-EXPORTED `JOB_EPHEMERAL=150Gi` before
the submit script. Per the handoff, the mutants audit is a trailing item and does
not gate this closeout; when it lands, survivors must be re-swept against the
committed `fuzz/corpus/ltree_diff` corpus via the differential rail (a raw survivor
count with `caught_by_rail=0` is NOT clean — gate-blindness law).
