# README-TODO: multirangetypes_diff (crates/backend/utils/adt/multirangetypes)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `multirange_in` | 4231 | `multirangetypes.c` |
| `multirange_out` | 4232 | `multirangetypes.c` |
| `multirange_recv` | 4233 | `multirangetypes.c` |
| `multirange_send` | 4234 | `multirangetypes.c` |
| `multirange_constructor0` | 4280 | `multirangetypes.c` |
| `multirange_constructor1` | 4281 | `multirangetypes.c` |
| `multirange_constructor2` | 4282 | `multirangetypes.c` |
| `multirange_lower` | 4235 | `multirangetypes.c` |
| `multirange_upper` | 4236 | `multirangetypes.c` |
| `multirange_empty` | 4237 | `multirangetypes.c` |
| `multirange_lower_inc` | 4238 | `multirangetypes.c` |
| `multirange_upper_inc` | 4239 | `multirangetypes.c` |
| `multirange_lower_inf` | 4240 | `multirangetypes.c` |
| `multirange_upper_inf` | 4241 | `multirangetypes.c` |
| `multirange_eq` | 4244 | `multirangetypes.c` |
| `multirange_cmp` | 4273 | `multirangetypes.c` |
| `multirange_contains_elem` | 4249 | `multirangetypes.c` |
| `multirange_contains_range` | 4250 | `multirangetypes.c` |
| `multirange_contains_multirange` | 4251 | `multirangetypes.c` |
| `multirange_overlaps_multirange` | 4248 | `multirangetypes.c` |
| `multirange_adjacent_multirange` | 4256 | `multirangetypes.c` |
| `multirange_before_multirange` | 4260 | `multirangetypes.c` |
| `multirange_union` | 4270 | `multirangetypes.c` |
| `multirange_minus` | 4271 | `multirangetypes.c` |
| `multirange_intersect` | 4272 | `multirangetypes.c` |
| `hash_multirange` | 4278 | `multirangetypes.c` |
| `hash_multirange_extended` | 4279 | `multirangetypes.c` |
| `range_merge_from_multirange` | 4228 | `multirangetypes.c` |

## STATUS (lane p1-laneac, 2026-07-31)

Oracle vendored + driver implemented + seeds banked + **2.5M-exec release smoke
CLEAN** (zero crashes, zero value/verdict/sqlstate divergences; arm64/macOS,
`PGRUST_FUZZ_CSANCOV=1`, 537 seeds + dict, 111 s at 22.5k exec/s). Ready for the
>=10M-exec fleet campaign. Findings + carves:
`fuzz/divergences/multirangetypes_diff/FINDINGS.md`.

Structure: `csrc/pg_multirangetypes_io.c` **#includes** `csrc/pg_rangetypes_io.c`
so both oracles are ONE translation unit, and `build.rs` compiles only this
file. multirangetypes.c calls fourteen rangetypes.c statics plus the shared
typcache mock / palloc arena / ereport shim / StringInfo shims; re-vendoring
them would drift, extern-promoting ~40 statics would be a large edit to a
concurrently-owned file. The ONE edit to `pg_rangetypes_io.c` is the additive
`TypeCacheEntry.rngtype` field, marked in place — **re-apply it if that file is
regenerated from its own assembler**.

Generator committed under `csrc/gen/` (assemble_mr.py + the two hand-written
`.in` parts), so this oracle is reproducible from the repo.

CROSS-LANE: **H1 in FINDINGS.md blocks the sibling `rangetypes_diff` campaign** —
its `build_image` pads before packed-short numeric bounds, which PG's
`datum_write` never does, and C's `range_deserialize` then SEGVs in
`numeric_cmp`. Reproducer banked. Previously masked by the P1 recv crash.

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_multirangetypes_io.c` at every
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
- [x] Uncomment the `.file("csrc/pg_multirangetypes_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/multirangetypes_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] fc-wrapper plane per arm via `fc_call` / `adt_multirangetypes::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin multirangetypes_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/multirangetypes_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/multirangetypes_diff/` (537 seeds + 4265 smoke-retained, committed) (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [x] Sancov on the C oracle side too (PGRUST_FUZZ_CSANCOV=1, verified in the smoke) (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run multirangetypes_diff` — floor for any fuzz-only claim:
      >=10M execs or 24h CPU per family, all planes compared; record the
      campaign size in the ledger row.
- [ ] Ground-truth law: no divergence recorded from the vendored oracle
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
