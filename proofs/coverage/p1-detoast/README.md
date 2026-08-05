# p1-detoast evidence bank (crate backend/access/common/detoast)

Lane: p1-detoast, branch proofs/p1-detoast, target fuzz/fuzz_targets/detoast_diff.rs.
Fleet CONFIRM: job pgrust-fuzz-campaign-1785614490-49b8-75202 @
c202d0e64d4414574b56b1345edad9000923c1f7 — 10,000,000/10,000,000 execs,
0 divergences, 0 sanitizer artifacts, rc=0, outcome=run
(campaign-stats.json; capture-forensics.txt = the fixed self-checking capture).

- coverage.lcov.gz — the fleet full-corpus capture at the job sha (708-file corpus).
- summary.json / verification-coverage.tsv / *.lib.rs.json — merge-coverage.py
  scoped to the crate (v2 SLOC rule): sloc=258, fuzz-covered=210 (81.4%),
  48 residual lines ALL carried as executable-exception rows in
  proofs/coverage/phase1-exceptions.tsv (author p1-detoast): expanded-object
  arms, ondisk toast_fetch arms, indirect->external recursion (all
  excluded-state carves per the claim row), 4 mcx alloc-failure `)?;` closers
  (defensive-c-parity), init_seams registration shell.
- Corpus of record: fuzz/corpus/detoast_diff on proofs/p1-detoast @ cdc878afa9
  (merge-minimized union 831 -> 299 + 123 named witness/boundary seeds = 422).
- Injection sweep at plane creation: 3/3 planted defects flagged
  (value-bytes / errcode-class / size planes).
- Rendered-red-line audit: all 48 red lines eyeballed against source; zero
  unexplained (each is a named carve or defensive region).
