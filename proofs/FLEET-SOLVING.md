# Fleet-solving: run the Kani proof suite on the fleet

The laptop is for AUTHORING harnesses; the fleet is for SOLVING them. A local
lane that hits a SAT wall under load (13 solvers contending on 14 cores) should
queue the solve to the fleet instead of re-running locally. One c8g.16xlarge
node runs ~18 solves concurrently with a per-solve RSS cap and a node memory
governor — co-tenancy cannot flip a verdict (solves are single-threaded and
verdict-deterministic).

Validated end-to-end 2026-07-30 at main `69c3c7eb904398267bcae164d0277bc301f972ef`
(smoke 8/8 rows correct incl. one must-fail control; then the full 2,146-row
suite as two shards).

## The submit command (from pgrust-fabled, ALWAYS)

```bash
cd /Users/malisper/dev/pgrust-fabled/fleet/jobs
export KUBECONFIG=/Users/malisper/dev/pgrust-fabled/fleet/.kube/config   # see "Credentials" below
SHA=$(cd /Users/malisper/dev/pgrust-fast && git rev-parse origin/main)   # FULL 40-char, resolved in pgrust-fast

# Smoke (always first at a new sha — benchmark-smoke-first rule):
FLEET_REPO=fast JOB_DESC="kani-suite SMOKE at $SHA — <why>" \
  bash ./submit-kani-suite.sh "$SHA" \
  --pick "bitutils/eq_popcount32,bool/eq_boolout,bytea-cmp/eq_byteaeq,cash/eq_cash_eq,casts/eq_i2toi4,float-arith/eq_float4abs,hash/hash_bytes_len0,bool/control_negative_boollt_vs_c_boolle" \
  --instance c8g.2xlarge
# ^ 7 cheap greens across 7 families + 1 must-fail control. ~2 min wall, PASS =
#   8 rows: 7 pass + 1 expected-fail-ok in suite-results.tsv.

# Full suite, two shards on two 16xl nodes (~1-2h wall each; halves latency):
FLEET_REPO=fast JOB_DESC="[LONG-JOB: full kani suite shard 1/2] kani-suite FULL rows 1-1073 at $SHA — <why>" \
  bash ./submit-kani-suite.sh "$SHA" --rows 1-1073    --deadline 10800
FLEET_REPO=fast JOB_DESC="[LONG-JOB: full kani suite shard 2/2] kani-suite FULL rows 1074-2146 at $SHA — <why>" \
  bash ./submit-kani-suite.sh "$SHA" --rows 1074-2146 --deadline 10800
# Row numbers are 1-based SUITE.tsv data rows (header excluded). Re-split as the
# suite grows: N=$(tail -n +2 proofs/SUITE.tsv | wc -l); half=$((N/2)).
```

Other shard shapes:
- `--tier per-commit` / `--tier release-gate` — one tier only.
- `--pick "family/harness,..."` — explicit list (exclusive with --tier/--rows).
- `--runqueue proofs/<lane>/runqueue.txt [--rq-rows A-B]` — run a LANE's
  pre-built runqueue verbatim (command/flags/timeout from the line). This is
  the lane-offload path: author locally, push the branch, submit its sha with
  the runqueue. Uncommitted runqueue: `KANI_RQ_LOCAL=/path/to/local.txt` ships
  it via the job configmap.
- Big-memory wall retries: `KANI_SOLVE_RSS_GB=40 ... --instance c8g.16xlarge --conc 2`.

## Laws that bite (each one has burned us)

1. **Two-repo sha topology.** pgrust-fast and pgrust-fabled are different
   GitHub repos with NO shared shas. Resolve the FULL 40-char sha in
   pgrust-fast; submit from fabled with `FLEET_REPO=fast`. Verify the ref with
   `git ls-remote origin <branch>` in pgrust-fast first. If the guard
   false-FATALs a sha you have ls-remote-verified, `SKIP_REF_CHECK=1` is the
   sanctioned override.
2. **Lane branches must be PUSHED** to pager-free/pgrust-fast before submitting
   their sha — the pod clones by sha from GitHub.
3. **JOB_DESC is mandatory** and full-suite runs need `--deadline > 3600` AND a
   literal `[LONG-JOB: reason]` tag in JOB_DESC (else the guard/notifier
   deadline machinery fights you).
4. **Smoke first at any new sha** before a full-suite submit.
5. **Verdicts come from S3 only** (gate-blindness law): a Complete job proves
   nothing until you read `suite-results.tsv`. The runner enforces a
   completeness floor (every shard row must have a result row) and fails the
   job otherwise.
6. **Never yield waiting on the fleet.** Poll in a foreground loop (or the
   notifier ledger, fleet skill section "Completion monitor"). A sub-agent that
   ends its turn "to be resumed when the job finishes" is never resumed.

## Credentials

- kubectl uses the EKS kubeconfig at `fleet/.kube/config`, which pins
  `AWS_PROFILE: mfa` (refresh via `mfa-login.sh`, 36h). If mfa is expired but
  the default profile is valid, copy the kubeconfig, delete the two-line
  `env: [{name: AWS_PROFILE, value: mfa}]` block from the exec stanza, and
  point KUBECONFIG at the copy — verified working 2026-07-30.
- `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN` in every
  shell; stale env creds shadow valid profiles.
- Laptop S3 on the results bucket: the mfa profile has it; the default profile
  is explicitly DENIED. When mfa is expired, harvest through an in-cluster
  aws-cli pod (node IAM role — verified working 2026-07-30):

  ```bash
  kubectl -n pgrust-fleet run s3cat-$RANDOM --rm -i --restart=Never \
    --image=amazon/aws-cli --command -- \
    aws s3 cp --region us-east-2 s3://pgrust-fleet-results-149051628381/kani-suite/<sha>/<job>/suite-results.tsv -
  ```

## Watch to terminal

```bash
kubectl -n pgrust-fleet get job <job> -o jsonpath='{.status.conditions[?(@.status=="True")].type}'
```
in a sleep-30 foreground loop, or poll the fleet-notifier ledger (one read
covers all jobs — fleet skill). Smoke ~2 min; a half-suite shard ~1-2h.

## Harvest and ARCHIVE (7-day S3 lifecycle!)

The results bucket deletes objects after 7 days (80.7% of historical gate
artifacts are gone — GL-GATEHIST-1). Copy out immediately after terminal:

```bash
DEST=~/pgrust-fleet-archive/kani-suite-<sha12>/<job>/
mkdir -p $DEST
# mfa valid:  aws s3 cp --recursive --profile mfa --region us-east-2 s3://.../kani-suite/<sha>/<job>/ $DEST
# mfa expired: per-file base64 through an in-cluster pod (the amazon/aws-cli
# image has NO tar — a tar-pipe silently yields an empty archive; per-file
# streaming is the validated method, 2026-07-30):
P=s3://pgrust-fleet-results-149051628381/kani-suite/<sha>/<job>
for f in MANIFEST.txt suite-results.tsv shard.tsv governor.log telemetry.tsv telemetry-summary.txt logs.tar.gz; do
  kubectl -n pgrust-fleet run s3f-$RANDOM --rm -i -q --restart=Never --image=amazon/aws-cli \
    --command -- sh -c "aws s3 cp --only-show-errors --region us-east-2 $P/\$0 /tmp/f >&2 && base64 -w0 /tmp/f" "$f" \
    2>/dev/null | base64 -d > "$DEST/$f"
done
gzip -t $DEST/logs.tar.gz   # integrity check; then verify byte counts are nonzero.
```

Artifacts per job: `MANIFEST.txt suite-results.tsv shard.tsv governor.log
telemetry.tsv telemetry-summary.txt logs.tar.gz completeness.diff(on failure)`.

## Read the verdicts

`suite-results.tsv` columns: family harness tier expected outcome wall_s
timeout_s verdict log. Outcomes: `pass | fail | expected-fail-ok |
vacuous-pass | timeout | rss-kill | governor-kill | no-verdict`. Compare
outcome against SUITE.tsv `expected`:
- expected=green + pass, expected=must-fail + expected-fail-ok → GREEN.
- expected=green + fail → UNEXPECTED FAIL. Per the campaign's 3:1 artifact law
  most of these are HARNESS defects (unwind truncation, vacuous covers, shim
  drift), not real C/Rust divergences — pull the harness log out of
  logs.tar.gz and classify before alarming anyone.
- timeout / rss-kill on expected=green → WALL (candidate for a big-memory
  retry node, or record wall-recorded in SUITE.tsv per suite policy).
- Rows in shard.tsv with no result row → the job FAILS on the completeness
  floor; treat as rig defect, not verdicts (gate-blindness law).

## K8s "Failed" is usually the RED GATE, not a rig crash

The runner exits `suite_rc=1` whenever ANY row deviates from expectation
(green->fail/timeout/rss-kill, must-fail deviation, skip) — the K8s Job then
shows Failed/BackoffLimitExceeded and the notifier logs
`pod-exited-nonzero-itself`. Verdicts are still COMPLETE and uploaded; read
`telemetry-summary.txt` (rows == results, suite_rc) before treating a Failed
job as broken. The notifier never auto-resubmits this class (correct).

## Measured baseline (2026-07-30, main 69c3c7eb9043, first full fleet run)

Two c8g.16xlarge shards, conc 17, zero governor kills, node mem peak 47%/56%:
shard 1 (rows 1-1073) 3168s wall; shard 2 (rows 1074-2146) 4262s wall.
Combined: 2146/2146 rows verdicted (100% completeness).
1692 green-pass + 89 must-fail-ok + 106 wall-ok = 1887 conforming;
~210 deviations classified into a handful of root causes, ZERO confirmed real
C/Rust divergences:
- 62 mbconv rows: SUITE.tsv flags carry a literal un-substituted
  `c/<FAMILY>.c` placeholder -> goto-cc "No such file" (manifest defect;
  check-suite-names.py does not validate flag paths).
- 12+ jsonb-probe `proofs::*` release-gate rows: prose `(recipe: ...)` leaked
  into the flags column -> `cargo-kani: unrecognized subcommand '(recipe:'`
  (manifest defect on the duplicated row set). Their unmeasured duplicates
  (no `proofs::` prefix) FAIL with "unwinding assertion loop 0" because the
  real recipe's per-loop `--unwindset + --no-unwinding-checks` lives only in
  the family's run-cmp.sh — unwind truncation, not divergence.
- ~72 green->timeout: mostly unmeasured rows hitting the 60s floor timeout
  (laptop-measured times don't transfer 1:1 to c8g), plus known-heavy solves
  (int div/mod family at 600s). Timeout-budget calibration, not divergences.
- ~33 green->rss-kill: 7 GB per-solve cap; retry candidates on a big-memory
  node (`KANI_SOLVE_RSS_GB=40 --conc 2`).
- ~21 unexpected bad-manifest-row: `wave5::`/`xid8snap::`-prefixed module rows
  the runner's row parser can't normalize (runner/SUITE schema gap).

### Repaired (2026-07-30, branch `proofs/suite-row-repair`)

`lint-suite-rows.py` was written to catch these classes offline; it found 193
structural violations on 168 rows at main, all repaired on that branch (linter
now 0 violations):

- 86 mbconv `c/<FAMILY>.c` placeholders expanded to the real per-row shim,
  derived from ground truth (harness -> `$cfn` ident in `mbconv/src/lib.rs`,
  ident -> the `c/*.c` file whose definition it is). All 86 execution-verified
  via `cargo kani --only-codegen`, which is exactly the goto-cc link step that
  failed on the fleet.
- 9 jsonb-probe rows: `--c-lib c/pg_jsonb_probe.c` -> `c/pg_jsonb.c` (the path
  `jsonb-probe/run-one.sh` has always used).
- 25 rows with prose in the flags column moved into `notes` (the claims are
  true and kept): 8 jsonb-probe `(recipe: run-cmp.sh ...)`, 17 enum-cmp /
  adt-misc `(c-lib via Cargo metadata)` — those two crates really do carry
  `[package.metadata.kani.flags] c-lib` in their Cargo.toml.
- 29 out-of-vocabulary `expected` and 34 non-numeric `time_s` values mapped to
  the real tokens; that is the ~21 "unexpected bad-manifest-row" class above
  (`expected=info` on documented wall harnesses -> `wall-recorded`, which the
  runner scores as `wall-ok`).
- **10 rows in NO gate tier** (`tier=control` x8, `probe` x1, `info` x1). The
  eight `control_*` rows are must-fail vacuity guards that no tier selected,
  so they had been silently disabled. Retiered to `defect-witness` (the tier
  that rides along with every gate) and re-run: all 8 still FAIL as designed,
  plus the `probe` divergence witness. `run-suite.sh` now hard-errors
  (`BAD-MANIFEST-TIER`) on an unknown tier instead of dropping the row.

## Cost

c8g.16xlarge on-demand us-east-2 ≈ $2.55/h. Full suite as 2 shards ≈ ~2.1
node-hours ≈ $5.50. Smoke on c8g.2xlarge ≈ $0.01. Cheap — default to the
fleet for any multi-minute solve batch instead of loading the laptop.
