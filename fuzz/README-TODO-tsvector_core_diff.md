# README-TODO: tsvector_core_diff (crates/backend/utils/adt/tsvector_core)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `tsvectorin` | 3610 | `tsvector.c` |
| `tsvectorout` | 3611 | `tsvector.c` |
| `tsvectorsend` | 3638 | `tsvector.c` |
| `tsvectorrecv` | 3639 | `tsvector.c` |
| `tsvector_lt` | 3616 | `tsvector_op.c` |
| `tsvector_le` | 3617 | `tsvector_op.c` |
| `tsvector_eq` | 3618 | `tsvector_op.c` |
| `tsvector_ne` | 3619 | `tsvector_op.c` |
| `tsvector_ge` | 3620 | `tsvector_op.c` |
| `tsvector_gt` | 3621 | `tsvector_op.c` |
| `tsvector_cmp` | 3622 | `tsvector_op.c` |
| `tsvector_strip` | 3623 | `tsvector_op.c` |
| `tsvector_setweight` | 3624 | `tsvector_op.c` |
| `tsvector_concat` | 3625 | `tsvector_op.c` |
| `tsvector_length` | 3711 | `tsvector_op.c` |
| `tsvector_filter` | 3319 | `tsvector_op.c` |
| `tsvector_setweight_by_filter` | 3320 | `tsvector_op.c` |
| `tsvector_delete_str` | 3321 | `tsvector_op.c` |
| `tsvector_delete_arr` | 3323 | `tsvector_op.c` |
| `tsvector_to_array` | 3326 | `tsvector_op.c` |
| `array_to_tsvector` | 3327 | `tsvector_op.c` |
| `ts_match_vq` | 3634 | `tsvector_op.c` |
| `ts_match_qv` | 3635 | `tsvector_op.c` |

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_tsvector_core_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_tsvector_core_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/tsvector_core_diff.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `adt_tsvector_core::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin tsvector_core_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/tsvector_core_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/tsvector_core_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run tsvector_core_diff` — floor for any fuzz-only claim:
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
