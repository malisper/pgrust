# p1-define coverage evidence (lane p1-new1, 2026-08-01)

Crate: crates/backend/commands/define (61 v2-SLOC).
Fleet campaign: pgrust-fuzz-campaign-1785614337-78c8-38739 @ c7d5565ff527d318caeebe76e708fa3473c6be35,
define_diff 10,000,000 execs, 0 divergences, 0 sanitizer artifacts, wall 54s.
Merge (proofs/coverage/merge-coverage.py, v2 + tables excluded): 56/61 fuzz-covered.
Residual 5 lines = the four unported-arm panics (lib.rs 38, 57-58, 60, 87),
carried as exception rows in proofs/coverage/phase1-exceptions.tsv.
Real defect found+fixed by the target pre-campaign: TypeName arrayBounds
"[]"-per-element vs C's once (parse_type.c appendTypeNameToBuffer).
