# Full-tier Kani coverage smoke test — PRE-REGISTERED PREDICTION

Written and committed BEFORE any harness was run under `--coverage`.
Purpose: make the methodology falsifiable. If the measured number lands
outside the stated interval, the instrument or the model of it is wrong,
and we say so instead of reporting the number.

Base sha: `1be0a47331` (branch `proofs/coverage-smoke`, off
`proofs/verification-coverage`). The `crates/backend/utils/adt/float`
sources are byte-identical at `41ef1dd381` (the prior capture's
`summary.json.head_sha`), `cf2caf1bec` (the COVERAGE.md-quoted head),
`1be0a47331`, and internal main `4b69bcd5a2` — verified with
`git diff --stat`. So the prior per-line numbers are directly comparable
and no line-number rebasing is needed.

## Crate under test: `adt_float` (crates/backend/utils/adt/float)

### Why NOT varbit / network / numeric

The task proposed varbit, network, or numeric on the theory that each was
sampled by only ONE proof family while other fast-tier families also
exercise it. Enumerating the actual per-commit tier refutes that for all
three. Method: a family can only contribute COVERED regions in a crate if
its harnesses *call into* that crate — Kani harnesses call specific
functions, so a mere Cargo link edge (which nearly every proof family has
to every adt crate, via `fmgr_core`'s builtins tables) contributes
nothing but UNCOVERED regions. Filtering families to those with a real
`<crate> = { path = ... }` dependency AND `<crate>::` call sites, then
counting distinct `per-commit`-tier harnesses in SUITE.tsv:

| crate   | families touching it (per-commit harness count)                                                                  | prev run | full |
|---------|------------------------------------------------------------------------------------------------------------------|----------|------|
| varbit  | varbit-rows (27), bytea-varbit (3), small-fams (6 — but its varbit harness `eq_bit_bit_count` is tier `unmeasured`) | 27       | 30   |
| network | network (15); spgist-inet has **no SUITE.tsv rows at all**                                                          | 15       | 15   |
| numeric | numeric-probe (17); numeric-arith and jsonb-probe have 0 per-commit rows                                            | 17       | 17   |
| geo     | geo-cmp (26)                                                                                                        | 26       | 26   |
| jsonb   | jsonb-gin (0), jsonb-probe (0) — no per-commit tier exists                                                           | 8 (RG)   | 0    |
| **float** | float-cmp (32), float-arith (13), **casts (22)**, **hash-rows (30)**; float-agg has 0 per-commit rows            | **45**   | **97** |
| varlena | bytea-cmp (26), text-slice (19), text-cmp (9), hash-rows (30), pseudotypes (55), typcache-inst (45), small-fams (6), bytea-varbit (3), + 4 families with 0 per-commit rows | 54 | 193 |

So the sampling hypothesis is **only true for float and varlena**. varbit
(+3), network (+0), numeric (+0), geo (+0) were already at their full
per-commit-tier harness sets in the prior capture; running "everything"
cannot move them.

float is the right smoke-test subject: it is the crate with a genuine,
large, *affordable* gap — 45 -> 97 harnesses (2.16x) — and the two added
families aim squarely at the region the prior capture named as float's
biggest uncovered block (`builtins.rs:159-250`, macro-generated
cast/wrapper shims), which `casts` exists to prove. varlena's 193-harness
set (incl. the 9 text-slice harnesses that already walled under coverage)
does not fit a couple of hours. float is 2,215 SLOC — small enough to
hand-audit end to end for the ground-truth check.

Baseline to beat (prior capture, float crate): **kani 197 / 2,215 SLOC = 8.89%**.
Per file: `lib.rs` 156/341, `funcs.rs` 30/663, `builtins.rs` 11/490,
`io.rs` 0/496, `aggregates.rs` 0/225.

## THE PREDICTION

Running all 97 per-commit harnesses across float-cmp, float-arith, casts,
hash-rows under `--coverage -Z source-coverage`:

- **Point estimate: 250 covered SLOC = 11.3%.**
- **80% interval: 220-300 covered SLOC (9.9%-13.5%).**
- Directional claim: the full-tier number is **higher but NOT
  qualitatively different** — it stays in single-to-low-double digits. The
  prior 8.9% is *not* mostly a sampling artifact.

Per-file predictions (the falsifiable part):

| file          | sloc | prior | predicted | reasoning |
|---------------|------|-------|-----------|-----------|
| builtins.rs   | 490  | 11    | 40-90     | `casts` proves 14 float cast wrappers (`fc_ftod`, `fc_i4tod`, `fc_dtoi4`, `fc_dtoi8`, `fc_ftoi8`, `fc_i8tod`, `fc_i8tof`, `fc_dtof`, ...) and hash-rows 4 more (`fc_hashfloat4/8`(`extended`)). These are `fc1t!`/`fc1!` macro *argument list* lines — one source line per builtin — so ~18 argument lines plus whatever macro-definition body lines light up. The 159-250 band is exactly `fc1t!`'s cast list. |
| funcs.rs      | 663  | 30    | 45-75     | hash-rows adds `hashfloat4/8` + extended bodies; casts adds `dtoi4/dtoi2/ftoi4/ftoi2/dtof/i8tof` range-check bodies. Small functions, ~15-45 lines total. |
| lib.rs        | 341  | 156   | 156-175   | already 46% covered by float-cmp/float-arith; casts/hash-rows reuse the same datum/error scaffolding. Little headroom. |
| io.rs         | 496  | 0     | **0**     | No family in the set calls `float4in/float8in/float8out`. This is the fuzz axis (`float_in_diff`/`float_out_diff` covered 361 lines here), not the Kani axis. **A nonzero number here would mean the attribution is wrong.** |
| aggregates.rs | 225  | 0     | **0**     | `float-agg` has zero per-commit rows. Same falsifier. |

Sum of point estimates: 65 + 60 + 165 + 0 + 0 = 290. I am shading the
headline point estimate DOWN to 250 against that, because macro-generated
wrappers are the one construct where I most expect Kani's source spans to
land on the macro *definition* rather than the invocation lines, which
would leave most of `builtins.rs`'s 490 lines dark no matter how many
cast wrappers get proved.

### What would count as a surprise (i.e. methodology problem, not a result)

1. **Measured < 197** for float. Monotonic impossibility — 97 harnesses
   is a superset of the 45. Would mean the prior capture's kaniraw set,
   the region->line mapping, or the file attribution is unstable.
2. **Any nonzero coverage in `io.rs` or `aggregates.rs`.** No harness in
   the set can reach them. Nonzero = spans bleeding across files, most
   likely via macro expansion or inlining attribution.
3. **> 400 (18%)**, i.e. builtins.rs largely lighting up. Would mean the
   prior capture UNDER-counted badly and the sampling hypothesis is right
   after all, which flips the recommendation on the full-tree run.
4. **Walled fraction >> 8%.** Prior: 14/165 (8.5%), concentrated in
   string-heavy harnesses (text-slice 9/14, jsonb 3/8). float/int
   arithmetic harnesses are scalar and bitblast small; I predict
   **0-3 of 97 wall** at a 600s per-harness timeout. A high wall rate in
   scalar families would mean coverage instrumentation is far more
   corrosive than the prior capture suggested.

### Wall-time prediction

SUITE.tsv baseline solve times for the 97 harnesses sum to 278.7s
(float-cmp 6.0, float-arith 35.0, casts 102.6, hash-rows 135.1). At the
prior capture's observed 3-10x coverage inflation that is 14-46 min of
solve, plus 4 family builds. **Predicted total wall: 45-100 min** on this
laptop (host load permitting).
