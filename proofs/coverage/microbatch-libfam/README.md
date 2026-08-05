# microbatch-libfam — differential-fuzz coverage bank (lane p1-mb-libfam)

Crates: crates/backend/lib/{hyperloglog,binaryheap,pairingheap,bloomfilter,
integerset}. One target: `libfam_diff` (selector byte, 5 arms). Oracle:
verbatim vendored PostgreSQL 18.3 C @ 62d6c7d3df (whole-file includes,
`cmp`-verified — see fuzz/core/csrc/pg_libfam_io.c header).

## Evidence
- `libfam_diff-local-20260731.lcov.gz` — full-corpus lcov (fuzz/cov-export.sh,
  1376-entry committed corpus replay, nightly llvm-cov).
- `summary.tsv` — per-crate equation: lcov DA lines = covered + exception
  rows, 0 unaccounted for all five crates. Exception rows appended to
  proofs/coverage/phase1-exceptions.tsv (8 rows, lane p1-mb-libfam).
- Exhaustive (cascade a0): hll add over the FULL u32 domain x bwidth {5,10},
  register-file compare every 2^16 adds — fuzz/campaigns/
  2026-07-31-libfam-exhaustive-hll.md (PASS, 2 x 2^32 adds).
- Local smoke: 2 rounds, 93,116 execs total, 0 divergences, 0 crashes
  (54,018 @ 7min + 39,098 @ 6min, laptop). Fleet 10M CONFIRM: pending
  (coordinator submits).

## Pre-share red-line eyeball (standing rule)
Every residual red line was read at source and classified — no bogus reds:
binaryheap 50/70 (capacity panic twins, C binaryheap.c:123/161, parity
unit-tested); hyperloglog 86 (rho j>b arithmetically-dead arm, C
hyperloglog.c:251-252); pairingheap 88 (merge b==INVALID dead arm, C
pairingheap.c:83-84); integerset 213-216 (max-levels error body, C
integerset.c:496-497, needs >2^60 entries).

## Carves (documented in the driver/module headers)
- intset memory_usage: executed both sides every exec; VALUE not compared
  (C GetMemoryChunkSpace = aset chunk accounting, malloc-layout non-surface).
- hll bwidth fuzzed at {4,5,6,10} (5/10 live consumers; 4/6 cover the alpha
  16/64 arms); C-only bwidth elog fence unit-tested.
- bloom total_elems >= 1 (C divides by it); work_mem in {0,1024,2048}
  (size bound only).
