# Preserved capture of 2026-07-30 (head 41ef1dd381, 7-crate scope)

The original seven-adt-crate capture (scope: float, geo, jsonb, network,
numeric, varbit, varlena; 21,986 SLOC). Its **fuzz** and **regress** axes are
still the measured numbers of record (those axes were not re-run in the
full-tree Kani capture that superseded the top-level summary.json):

    kani 1,321 (6.01%)  fuzz 494 (2.25%)  regress 15,598 (70.95%)  any 15,856 (72.12%)

The kani axis here is the PRE-instrument-fix, biased-low measurement
(macro attribution + fail-closed census landed after it) — superseded by the
full-tree capture at proofs/coverage/summary.json. The fuzz/regress per-file
line detail in files/ is line-aligned to head 41ef1dd381, NOT to current main.
