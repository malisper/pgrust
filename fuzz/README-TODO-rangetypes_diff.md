# README-TODO: rangetypes_diff (crates/backend/utils/adt/rangetypes)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `range_in` | 3834 | `rangetypes.c` |
| `range_out` | 3835 | `rangetypes.c` |
| `range_recv` | 3836 | `rangetypes.c` |
| `range_send` | 3837 | `rangetypes.c` |
| `range_constructor2` | 3840 | `rangetypes.c` |
| `range_constructor3` | 3841 | `rangetypes.c` |
| `range_lower` | 3848 | `rangetypes.c` |
| `range_upper` | 3849 | `rangetypes.c` |
| `range_empty` | 3850 | `rangetypes.c` |
| `range_lower_inc` | 3851 | `rangetypes.c` |
| `range_upper_inc` | 3852 | `rangetypes.c` |
| `range_lower_inf` | 3853 | `rangetypes.c` |
| `range_upper_inf` | 3854 | `rangetypes.c` |
| `range_adjacent` | 3862 | `rangetypes.c` |
| `range_overleft` | 3865 | `rangetypes.c` |
| `range_overright` | 3866 | `rangetypes.c` |
| `range_union` | 3867 | `rangetypes.c` |
| `range_intersect` | 3868 | `rangetypes.c` |
| `range_minus` | 3869 | `rangetypes.c` |
| `range_merge` | 4057 | `rangetypes.c` |
| `hash_range` | 3902 | `rangetypes.c` |
| `hash_range_extended` | 3417 | `rangetypes.c` |
| `int4range_canonical` | 3914 | `rangetypes.c` |
| `int8range_canonical` | 3928 | `rangetypes.c` |
| `daterange_canonical` | 3915 | `rangetypes.c` |
| `int4range_subdiff` | 3922 | `rangetypes.c` |
| `int8range_subdiff` | 3923 | `rangetypes.c` |
| `numrange_subdiff` | 3924 | `rangetypes.c` |
| `daterange_subdiff` | 3925 | `rangetypes.c` |
| `tsrange_subdiff` | 3929 | `rangetypes.c` |
| `tstzrange_subdiff` | 3930 | `rangetypes.c` |

## STATUS (lane p1-laneac, 2026-07-31) — FLEET-READY

Submit the >=10M campaign; nothing here gates it.

FINAL SMOKE (post-leak-fix, LSan enabled): **2,500,000 execs, release, 178 s,
ZERO crashes and ZERO divergences**, with `-detect_leaks=1` on for the whole run
and the only leak report being libFuzzer's own watchdog thread (56 bytes, no
frames in this target). A prior 3,000,000-exec run was equally clean on the
value/verdict/sqlstate planes. Replay rail green
(17,125 runs, cov 4111 / ft 10349).

No arm of this target sorts or reduces over multiple ranges (ops and setops take
exactly two, accessors/hash/canonical take one, subdiff takes two scalars), so
the sibling lane's `multirange_in` tie-order divergence — C's unstable
`qsort_arg` picking a different surviving representation among value-equal
numeric bounds — has no analogue here. LEAK EVIDENCE (claim corrected 2026-07-31 — see the note below): after the
`RNG_RETTYPE` fix, an LSan-enabled local run enumerating the FULL report shows
56 bytes in 2 allocations, both inside libFuzzer's own `FuzzerDriver` watchdog
thread, with ZERO frames in this target, the shipped crates, or the oracle.
The fleet's first 10M attempt reported exactly the 360 bytes this fix removes
and nothing else, so the watchdog allocations are not reported there.

NOT VACUOUS, and shown rather than assumed (`PGRUST_FUZZ_RT_STATS=1`):
  hand-built images compared, int4/int8/num = 202,387 / 143,351 / 296,960
  ctor-built  images compared, int4/int8/num =  50,100 /  28,921 /  59,424
  ctor_declined = 15,115 (legitimate lower > upper)
Two image layouts are fuzzed on a payload bit: the hand builder (arbitrary
flags byte — the full range_deserialize flags lattice) and the shipped
fc_range_constructor3 (exactly what a stored range carries, packed short
headers included). A unit gate asserts every (layout x instantiation) cell is
actually built, and that the two numrange layouts genuinely differ.

Shipped-code defects found: P1 (release-blocker `range_recv` SEGV) and P2
(unvalidated pre-allocation), both fixed by the coordinator at 3c129c2bb6 and
ground-truthed on `postgres:18.3`. Every other stop was a HARNESS defect with
the shipped Rust correct — H1-H5 plus the sibling-reported layout gap are logged
in `fuzz/divergences/rangetypes_diff/FINDINGS.md`.

MERGE NOTE for the sibling multirange oracle: the lane assembler now EMITS the
additive `TypeCacheEntry.rngtype` field, so regenerating
`csrc/pg_rangetypes_io.c` keeps `csrc/pg_multirangetypes_io.c` compiling.

### Correction: my earlier "leaks clean" line was not supported

The first version of this file claimed leaks were clean on the strength of (a) a
filtered grep of an LSan report whose tail happened to show only libFuzzer
frames, and (b) a control run of `quote_diff` showing the same trace. Neither
supports the conclusion: I never enumerated the whole report, and a control that
shares a libFuzzer-side allocation says nothing about THIS target's own. The
fleet then killed the 10M campaign at 8 execs on a 24-byte-per-call
`Box::leak(AggFnArgTypes)` in `ops_flinfo` — a leak my own laptop CAN detect
(re-verified: `-detect_leaks=1` names `ops_flinfo` directly), so this was an
evidence-handling failure on my part, not a platform limitation.

For the record, the reviewing lane's stated reason ("macOS has no
LeakSanitizer") is not the cause: LSan runs here and pinpointed the leak once
the full report was read. The lesson is the narrower and more transferable one —
**enumerate the whole sanitizer report before claiming a class is clean, and
never generalize from a control that cannot exhibit your own defect.**

NOT done in this lane: the adt/multirangetypes half (sibling branch
proofs/p1-laneac-mr) and the fleet campaign itself.

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_rangetypes_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [x] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [x] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
- [x] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
- [x] Uncomment the `.file("csrc/pg_rangetypes_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/rangetypes_diff.rs`)

- [x] Fill each `*_diff` arm (restructured to 11 selector arms; see module header): C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] fc-wrapper plane per arm via `fc_call` / `adt_rangetypes::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin rangetypes_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/rangetypes_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/rangetypes_diff/` (149 seeds committed) (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run rangetypes_diff` — floor for any fuzz-only claim:
      >=10M execs or 24h CPU per family, all planes compared; record the
      campaign size in the ledger row.
- [x] Ground-truth law (P1/P2 replayed on `postgres:18.3`): no divergence recorded from the vendored oracle
      alone — replay against `postgres:18.3` Docker; triage Csmith-style
      (pgrust-bug / oracle-platform-variance carve / upstream-bug).

## 5. Bookkeeping (every commit) and done-gate

- [ ] Ledger rows (`proofs/USER_FACING_FUNCTIONS.tsv`) with the standardized
      qualifier grammar; every harness in `proofs/SUITE.tsv`;
      `proofs/lint-suite-rows.py` clean.
- [ ] Flip `docs/verification/phase1-routes.tsv` statuses as functions land
      (evidence = this target's name); update the crate's claim row in
      `phase1-claims.tsv` and its `phase1-ranking.tsv` row. Pull before
      editing shared TSVs and RE-READ after any pull before writing.
- [ ] Done-gate: coverage merge 100% in-scope v2-SLOC under proof-union-fuzz
      or recorded executable exception; rendered-red-line eyeball audit;
      `cargo mutants` pilot on fuzz-only regions; replay rail in CI.
