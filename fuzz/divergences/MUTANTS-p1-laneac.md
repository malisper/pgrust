# Trailing mutation audit — p1-laneac (adt/rangetypes + adt/multirangetypes)

Started 2026-07-31 at gate sha df18c6b6f5 (both crates DONE, gate closed on
coverage + exceptions + the green 10M pair). Per the fuzzuproof-crate DONE GATE
item 3 this audit is TRAILING and non-blocking: it is an audit instrument, not a
metric, and the crates' `done` status does not wait on it.

Command (local, background, nice 19 — no CI cluster mutants job type exists yet; see
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

PENDING — run in flight (~2,410 mutants at ~7s each). Triage completes when the
run does; the claims-row note carries `mutants-audit pending` until then, with
status `done` per the gate.
