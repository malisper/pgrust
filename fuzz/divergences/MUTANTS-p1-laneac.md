# Trailing mutation audit — p1-laneac (adt/rangetypes + adt/multirangetypes)

Started 2026-07-31 at gate sha df18c6b6f5 (both crates DONE, gate closed on
coverage + exceptions + the green 10M pair). Per the fuzzuproof-crate DONE GATE
item 3 this audit is TRAILING and non-blocking: it is an audit instrument, not a
metric, and the crates' `done` status does not wait on it.

Command (local, background, nice 19 — no fleet mutants job type exists yet; see
campaign task #56 for the memory knob that heavy crates need):

    cargo mutants -p adt_rangetypes -p adt_multirangetypes

Scope: 2,410 mutants. Baseline clean (35s build + 1s test).

## Survivor triage

Rules (unchanged, never waived inline): {add plane, add seeds, add spot proof,
mark arid}. A survivor demonstrating a real hole REOPENS the crate as a targeted
work item. `SURVIVED` means the corpus lacks a witnessing input — NOT that the
mutant is equivalent.

### S1 — `multirangetypes/src/lib.rs:69:36` `replace != with ==` — ARID

    let need = match flinfo.fn_extra_ref::<MultirangeInfo>() {
        Some(mi) => mi.mltrngtypid != mltrngtypid,   // <-- line 69, mutated
        None => true,                                 // <-- line 70, excepted
    };

Line 69 IS fuzz-covered; line 70 carries an `excluded-state` exception row. The
driver pre-seeds the fn_extra memo, so the `None` arm never runs and the `Some`
arm always compares a memo populated for the SAME type — the predicate is
therefore always false in every driven exec. Mutating it to `==` makes it always
true, which re-runs `MultirangeInfo::lookup(mltrngtypid)`; under the pinned
typcache mock that returns identical info, so observable behaviour is unchanged
and the mutant survives.

Killing it requires a memo populated for a DIFFERENT multirange type than the one
requested — i.e. mixing subtypes on one flinfo, which is exactly the **typcache
subtype dispatch carve** this lane ratified. ARID: no plane, seed, or proof is
owed, because the discriminating input lives in the carved region.

PREDICTION for the rest of the run: the whole `cached_multirange_info` /
`cached_range_info` / `flinfo_ri` / `flinfo_mi` memo family should survive for
this same reason. Survivors there are expected and arid. Survivors ANYWHERE ELSE
— particularly in io.rs, ops.rs, or the canonicalize/serialize paths — are NOT
covered by this argument and must be triaged individually with
`proofs/coverage/mutkill.sh <file> <line> <old> <new> <target>`.

### Remaining survivors

NOT OBTAINED. Corrected 2026-07-31 after checking rather than assuming: the
local background run is NO LONGER RUNNING (no `cargo-mutants` process, no
`mutants.out*` directory anywhere under the worktree), so the "run in flight"
line above was stale the moment the process died. Only S1 was ever triaged.

Four fleet `mutants-audit` submissions were also attempted and ALL FOUR FAILED
without producing any S3 artifact:

| job | scope | outcome |
|---|---|---|
| 1785528350-44fc-61712 | both crates | Failed 42m |
| 1785530899-00d7-93190 | rangetypes | Failed 15m (container exit 127) |
| 1785532169-1097-73298 | rangetypes retry | Failed 16m |
| 1785530917-0077-93515 | multirangetypes | Failed 54m |

This is a JOB-TYPE OUTAGE, not a lane defect: fleet-wide on 2026-07-31 the
mutants-audit type stood at 53 Failed vs 13 Complete (~80% failure), sibling
lanes' pods were OOMKilled, and `JOB_MEM=54Gi` was passed but NOT honored (the
pod reported 24Gi/12Gi limits). Pods are GC'd before logs can be read and no
artifacts are uploaded on failure, so there is nothing further to diagnose from
the laptop.

STATUS: the audit is OWED. Both crates remain `done` — the gate closes on
coverage + exceptions + the green 10M pair, and per the fuzzuproof-crate DONE
GATE item 3 the mutation audit is a trailing audit instrument that explicitly
does not gate. Re-run once the job type is repaired; S1's ARID verdict and the
memo-family prediction above stand and should be re-checked against the real
survivor list.

TOOLING DEFECT worth fixing alongside: `scripts/fetch-mutants-results.sh`
reports `MISSING mutants-summary.json — job incomplete?` for a job that has
already **Failed**, so it printed "incomplete" for 40 minutes over a dead job.
The verdict must come from job status, never from that trailer — same
gate-blindness class as the false "no survivors" trailer p1-laner found.
