# EVIDENCE-DEBT — phase-1 done crates whose measured leg cannot be reconstructed (2026-07-31)

Recorded by the evidence-bank lane (branch `proofs/evidence-bank`; see
`proofs/coverage/evidence-bank/README.md` there for the full per-crate evidence
map). Per the gate audit (`docs/verification/phase1-gate-audit-2026-07-31.md` on
`docs/p1-gate-audit` @ f400e635ec) and the cov-resweep README's follow-up-debt
list (`proofs/coverage/cov-resweep/README.md`, banked on `proofs/evidence-bank`).
These are NOT fabricated or back-filled: each measured claim below rests on
fleet job IDs whose artifacts aged out (7-day lifecycle) or on captures that
were never banked. Debt is owed by the named lane owner (or a resweep with the
lane's corpus, same recipe as cov-resweep).

## Named by the audit + cov-resweep (charter list)

UPDATE 2026-07-31 (p1-regexp banking pass): the evidence-rebuild lane's measured
reconstructions (`proofs/evidence-rebuild` @ 4a558a43bf) are now BANKED ON MAIN at
`proofs/coverage/evidence-rebuild/` (this commit). That RESOLVES the four charter
rows below (scalar/xid8funcs, tsvector_core/tsrank, like) plus the census-close
re-runs for laneh/laney/lanek/lanem/laneo — see that dir's README for per-crate
verdicts and the two corpus-UNREPRODUCIBLE regressions it found (adt_timestamp
lib.rs:1034; formatting dch.rs x5), which remain OPEN as witness-seed debts.
Rows measured at LANE TIPS (xid8funcs, tsvector_core, tsrank) certify the lane
trees, not main — that drift flag stays open until those fixes land.

| crate(s) | lane | claimed measured | what is missing | owed |
|---|---|---|---|---|
| adt/scalar | p1-lanep | 470/743 fuzz | RESOLVED 2026-07-31: rebuilt 470/743 EXACT, banked evidence-rebuild/lanep | — |
| adt/xid8funcs | p1-lanep | 205/340-343 fuzz | RESOLVED 2026-07-31 (lane tip only): rebuilt 205/343 EXACT, banked evidence-rebuild/lanep | landing of lane fixes |
| adt/tsvector_core + adt/tsrank | p1-laneae | only JOINT 1802/2022 recorded | RESOLVED 2026-07-31 (lane tip only): per-crate 1282/1492 + 520/530 derived, banked evidence-rebuild/laneae | landing of lane fixes |
| adt/like | p1-laneag | 278/367 fuzz | RESOLVED 2026-07-31: rebuilt 278/367 EXACT, banked evidence-rebuild/laneag (fuzz-like_diff-local-20260731.lcov.gz) | — |
| adt/regexp | p1-laneag | 861/1166 fuzz | never enumerated by the audit (done-flip postdated it); fleet artifact bank has no coverage capture | RESOLVED 2026-07-31: local full-corpus regexp_diff replay banked proofs/coverage/p1-regexp (p1-regexp takeover) |

## Additional debts confirmed while banking (same class)

| crate(s) | lane | note |
|---|---|---|
| adt/cash | p1-lane0b | 446 claimed fuzz lines have no lcov anywhere (411 unaccounted per audit); corpus banked |
| common/string, archive, percentrepl, relpath, wait_error | p1-lanec | zero coverage artifacts anywhere; archive/percentrepl/wait_error show 0 measured on every on-main axis |
| adt_ascii + base64 | p1-lanee/g | no fuzz lcov ever banked (kani/regress axes on main only) |
| sha2/pglz/encode | p1-laneo | RESOLVED 2026-07-31: lcovs banked evidence-rebuild/census-close/laneo (287/315 + 208/231 + 213/220 EXACT; p1-regexp banking pass) |
| adt/varchar | p1-lanem | RESOLVED 2026-07-31: rebuilt 429/492 EXACT by fresh replay, banked evidence-rebuild/census-close/lanem (p1-regexp banking pass) |
| adt/rangetypes + multirangetypes | p1-laneac | corpus-reproducible only; "cargo fuzz coverage over the committed corpus" lcov never banked |
| adt/jsonpath + jsonpath_exec | p1-laneaa | fleet lcov proven PARTIAL capture (34.54% vs 88.38% local, same sha+corpus); local export of record not banked |
| adt/jsonb | p1-lanev | measured claims-only; cov-resweep local re-export verified >= recorded (join banked), lcov still owed |
| adt/arrayfuncs | p1-lanex | final-sha lcov never banked; only a stale pre-fix lcov misfiled at fuzz/corpus/jsonbio_diff/arrayfuncs.lcov |
| adt/array_userfuncs + rowtypes | p1-laneai | job stats JSONs on main; per-line lcov never banked; corpora banked (reproducible, not reconstructible) |
| md5/sha1/hmac/scram_common/cryptohashfuncs/keywords | p1-lanef | no lcov and no detail JSONs banked; cov-resweep local re-export == recorded (joins banked) |
| arrayutils/hashfn/pg_prng | p1-laneh | RESOLVED 2026-07-31: 3 lcovs banked evidence-rebuild/census-close/laneh (95/102, 163/164 line-table basis, 67/75; p1-regexp banking pass) |
| adt/int | p1-lanej | gate lcovs were /tmp paths (lost); cov-resweep local lcov banked (fuzz-int_diff-local-20260731.lcov.gz) — resolves the numerator, debt is the ORIGINAL capture only |

Rule this debt enforces going forward (audit conclusion): bank the final-sha
lcov + ledger rows on main AT DONE-GATE (adt/float / p1-lanead is the model).
