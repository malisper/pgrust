# backend/libpq/crypt — floor + artifact adjudication (p1-wavea, adopted lane)

Floor of record: **job pgrust-fuzz-campaign-1785640622-7a50-80926 @ 69310d2f473a91ab9c5f09e8e3dcd2d8f0a59988**
— 10,045,288 execs, 0 divergences, 0 sanitizer artifacts, 0 strays, rc=0
(c8g.4xlarge, FUZZ_FORK=14, activeDeadlineSeconds=14400 verified in-spec).

lcov of record: **job pgrust-fuzz-campaign-1785643895-09c1-32217 @ 4031fdb73aca028eaa5a682f42f6368367505ec6**
— 205,399-exec validation leg over the final 563-seed committed bank (+ S3
resume bank, corpus_in=603), 0 divergences, 0 sanitizer artifacts
(activeDeadlineSeconds=7200 verified in-spec). Needed because the floor
job's own coverage replay ran before the two post-floor directed witness
seeds existed; `src/lib.rs` is byte-identical across 69310d2f47 →
4031fdb73ac (only `src/tests.rs` and harness files changed afterwards), so
the equation below is valid at the final carve state.

Coverage equation: **133 DA lines on src/lib.rs = 110 fuzz-measured + 23
exception rows** (6 `PasswordType::from_guc` pgrust-only GUC plumbing, 16
`get_role_password` census-OUT syscache+clock carve, 1 `unreachable!()`
control-shape arm). miss-set == exception-set exactly; zero unexplained red.

## First floor attempt: job pgrust-fuzz-campaign-1785631457-094e-32271 @ 7c4d072f6d

Ran the full budget (10,000,008 execs, **0 value divergences**) but exited
rc=1 with 810 sanitizer artifacts + 1 crash artifact; its coverage replay
aborted (exit 77 → cov_lines=0). Adjudication:

1. **810 leak artifacts — ONE identical signature (oracle-glue defect, not
   pgrust)**: every artifact reports `140 byte(s) leaked in 1 allocation(s)`.
   Symbolized locally (macOS LSan, same seed):
   `malloc <- cryptofam_scram_build_secret <- pg_cryptbe_encrypt_password <-
   pg_cryptbe_w_encrypt_password <- decoder_fuzz::cryptbe_diff::diff_encrypt`.
   The vendored scram-common.c is compiled FRONTEND in the cryptofam family,
   so its `palloc` arm is raw `malloc`; the verbatim crypt.c callers treat
   the secret as palloc'd-and-forgotten. No pgrust frame in the stack —
   pgrust product code (crypt crate) uses mcx contexts and is leak-free.
   **Fix (69310d2f47)**: the cryptbe TU's `scram_build_secret` references now
   route through an arena-tracking shim (`pg_cryptbe_scram_build_secret`)
   freed by `pg_cryptbe_reset`; vendored bodies untouched. Witness: same
   seed LSan-clean post-fix; floor rerun 0 sanitizer artifacts.

2. **1 crash artifact `crash-da39a3ee...` (= SHA1 of the EMPTY input)**:
   replays clean on the fleet (its own repro.txt shows `Executed ... in 0 ms`
   with no fault) and locally. Fork-mode worker-exit noise from the leak
   pressure above; counted as `divergences: 1` by the runner's artifact
   classifier but carries no input bytes and reproduces no divergence.
   Retired by the leak fix (floor rerun: zero artifacts of any kind).

## fetch-script `bad_rows=1` false positive (both later jobs)

`fetch-fuzz-results.sh` greps stats rows for `"outcome": "complete"` and
counts the JOB-level outcome key as a row; the only target row is
`"outcome": "run"` with execs over budget. Same false positive adjudicated
by the tablesample and instrument units of this lane on the same day.

## Corpus

Floor-grown snapshot (579) ∪ committed bank (360 + 2 directed witness seeds)
merge-minimized 581 → 456; banked as minimized ∪ original committed bank =
563 committed (original directed witnesses preserved verbatim), + 3
leg-grown additions = 606. The 2 new directed seeds are line-witnessed under
llvm-cov: `bca49c0f83...` (>512-byte valid pre-encrypted SCRAM secret →
pass-through → "encrypted password is too long", ERRCODE 54000, lines
122-130) and `d95962b591...` (md5_crypt_verify computed-match → STATUS_OK,
line 166).
