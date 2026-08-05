# README-TODO: libfam_diff (crates/backend/lib/integerset)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `initHyperLogLog` | 0 | `hyperloglog.c` |
| `addHyperLogLog` | 0 | `hyperloglog.c` |
| `estimateHyperLogLog` | 0 | `hyperloglog.c` |
| `binaryheap_allocate` | 0 | `binaryheap.c` |
| `binaryheap_add` | 0 | `binaryheap.c` |
| `binaryheap_add_unordered` | 0 | `binaryheap.c` |
| `binaryheap_build` | 0 | `binaryheap.c` |
| `binaryheap_remove_first` | 0 | `binaryheap.c` |
| `binaryheap_remove_node` | 0 | `binaryheap.c` |
| `binaryheap_replace_first` | 0 | `binaryheap.c` |
| `pairingheap_add` | 0 | `pairingheap.c` |
| `pairingheap_remove_first` | 0 | `pairingheap.c` |
| `pairingheap_remove` | 0 | `pairingheap.c` |
| `bloom_create` | 0 | `bloomfilter.c` |
| `bloom_add_element` | 0 | `bloomfilter.c` |
| `bloom_lacks_element` | 0 | `bloomfilter.c` |
| `bloom_prop_bits_set` | 0 | `bloomfilter.c` |
| `intset_create` | 0 | `integerset.c` |
| `intset_add_member` | 0 | `integerset.c` |
| `intset_is_member` | 0 | `integerset.c` |
| `intset_begin_iterate` | 0 | `integerset.c` |
| `intset_iterate_next` | 0 | `integerset.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_libfam_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [x] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
- [x] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
- [x] Uncomment the `.file("csrc/pg_libfam_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/libfam_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] (N/A) fc-wrapper plane per arm — none of the five crates has a builtins.rs / fc_* surface via `fc_call` / `integerset::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin libfam_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/libfam_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/libfam_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [x] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run libfam_diff` — floor for any fuzz-only claim:
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
