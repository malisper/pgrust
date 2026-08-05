# README-TODO: pg_prng_diff (crates/common/pg_prng)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `pg_prng_seed` | 0 | `pg_prng.c` |
| `pg_prng_fseed` | 0 | `pg_prng.c` |
| `pg_prng_seed_check` | 0 | `pg_prng.c` |
| `pg_prng_uint64` | 0 | `pg_prng.c` |
| `pg_prng_uint64_range` | 0 | `pg_prng.c` |
| `pg_prng_int64` | 0 | `pg_prng.c` |
| `pg_prng_int64p` | 0 | `pg_prng.c` |
| `pg_prng_int64_range` | 0 | `pg_prng.c` |
| `pg_prng_uint32` | 0 | `pg_prng.c` |
| `pg_prng_int32` | 0 | `pg_prng.c` |
| `pg_prng_int32p` | 0 | `pg_prng.c` |
| `pg_prng_double` | 0 | `pg_prng.c` |
| `pg_prng_double_normal` | 0 | `pg_prng.c` |
| `pg_prng_bool` | 0 | `pg_prng.c` |

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_pg_prng_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      reset `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_pg_prng_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/pg_prng_diff.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `pg_prng::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin pg_prng_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/pg_prng_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/pg_prng_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run pg_prng_diff` — floor for any fuzz-only claim:
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
