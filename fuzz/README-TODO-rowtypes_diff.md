# README-TODO: rowtypes_diff (crates/backend/utils/adt/rowtypes)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `record_in` | 2290 | `rowtypes.c` |
| `record_out` | 2291 | `rowtypes.c` |
| `record_recv` | 2402 | `rowtypes.c` |
| `record_send` | 2403 | `rowtypes.c` |
| `record_image_cmp` | 3187 | `rowtypes.c` |
| `record_image_eq` | 3181 | `rowtypes.c` |
| `hash_record` | 6192 | `rowtypes.c` |
| `hash_record_extended` | 6193 | `rowtypes.c` |
| `record_larger` | 6375 | `rowtypes.c` |
| `record_smaller` | 6376 | `rowtypes.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_rowtypes_io.c` at every
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
- [x] Uncomment the `.file("csrc/pg_rowtypes_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/rowtypes_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] fc-wrapper plane per arm (arms drive the fc_* wrappers directly — wrapper IS the compared entry point) via `fc_call` / `adt_rowtypes::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin rowtypes_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/rowtypes_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/rowtypes_diff/` (59 hand seeds + fuzz-grown; committed) (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run rowtypes_diff` — floor for any fuzz-only claim:
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

## Extension pass 2 (p1-laneai, 2026-07-31)

- [x] Arms 10-16: record_eq/ne/lt/gt/le/ge/btrecordcmp (C: verbatim record_eq
      + wrappers vendored, static-prefixed; eq operator resolved through the
      pinned amop/operator seams -> myint4eq/mytexteq codecs; hash opfamily
      strategy-1 rows pinned to the SAME operators — resolve_hash_proc checks
      a determined eq_opr against the hash family's member, so the pinned
      catalog fragment must be self-consistent like the real one).
- [x] Arms 17-21: record_image_ne/lt/gt/le/ge (verbatim one-line wrappers).
- [x] Descriptors 5 (bool,int2,int8) + 6 (fix8,bool): byval widths 1/2/8 for
      the datum_image masking arms + a fixed-length BY-REF column; new codecs
      (bool/int2/int8/fix8) transcribed identically both sides (SECTION D).
- [x] Anonymous-record typmod (-1) mode: flags bit 5, record_in + record_recv
      not-implemented arms, hard + soft (C entry treats soft ereturn-without-
      isnull as the soft-error verdict, mirroring SOFT_ERROR_OCCURRED).
- [x] fn_extra memo-hit second calls for record_send + record_recv.
- [x] Coverage (local corpus): rowtypes/lib.rs 904/944 SLOC (95.8%); the 40
      residual lines are all classified (11 instrument-unmappable verified
      no-DA-record lines, 14 trait-declaration lines, 3 const-context
      builtins-table lines, 12 defensive unreachable-arm lines: cstring
      attlen -2 column arms + embedded-NUL defense) — zero reachable gaps.
- [ ] Fleet 10M campaign on the extended harness (parent owns).
