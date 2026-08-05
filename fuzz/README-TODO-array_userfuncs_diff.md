# README-TODO: array_userfuncs_diff (crates/backend/utils/adt/array_userfuncs)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `array_append` | 378 | `array_userfuncs.c` |
| `array_prepend` | 379 | `array_userfuncs.c` |
| `array_cat` | 383 | `array_userfuncs.c` |
| `array_position` | 3277 | `array_userfuncs.c` |
| `array_position_start` | 3278 | `array_userfuncs.c` |
| `array_positions` | 3279 | `array_userfuncs.c` |
| `trim_array` | 6172 | `array_userfuncs.c` |
| `array_reverse` | 6381 | `array_userfuncs.c` |
| `array_shuffle` | 6215 | `array_userfuncs.c` |
| `array_sample` | 6216 | `array_userfuncs.c` |
| `array_agg_array_serialize` | 6297 | `array_userfuncs.c` |
| `array_agg_array_deserialize` | 6298 | `array_userfuncs.c` |
| `array_agg_array_combine` | 6296 | `array_userfuncs.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_array_userfuncs_io.c` at every
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
- [x] Uncomment the `.file("csrc/pg_array_userfuncs_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/array_userfuncs_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] fc-wrapper plane per arm via `fc_call` / `array_userfuncs::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin array_userfuncs_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/array_userfuncs_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/array_userfuncs_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run array_userfuncs_diff` — floor for any fuzz-only claim:
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

## Lane p1-laneai status (2026-07-31)

Steps 1-3 COMPLETE. Local state:
  - `cargo check --manifest-path fuzz/Cargo.toml --bin array_userfuncs_diff` clean
    with the build.rs gate uncommented.
  - `cargo test --manifest-path fuzz/core/Cargo.toml array_userfuncs_diff` — 4/4
    green (arms_smoke, text_arms_smoke, cat_witness_pairs, selector_soup).
  - `cargo +nightly fuzz run array_userfuncs_diff -- -runs=400000` clean
    (exit 0, no crash/timeout artifacts); ~8.3k exec/s, cov 2996 edges.
  - corpus: 481 inputs after `cargo fuzz cmin` (48 hand-seeded shapes +
    coverage-guided finds, including every reproducer found during bring-up),
    1.9 MB, committed.

Comparison planes live: result varlena byte image / i32 subscript, Ok-vs-Err
verdict, errcode class, serialize wire byte-law, serialize->deserialize
round-trip, and the fc-wrapper==core plane on every arm.

### Findings (both banked under fuzz/divergences/array_userfuncs_diff/)

  - DIV-1 OPEN, pgrust-bug: array_position/array_positions do a checked
    `lbs[0] - 1`, so every overflow-checked build panics on a valid array with
    lower bound i32::MIN, where C wraps under -fwrapv and real 18.3 returns
    the wrapped position (ground-truthed on postgres:18.3). Release behaviour
    matches C; debug/test/fuzz builds crash. Two sites, lib.rs:281 and :319.
  - DIV-2 CLOSED, upstream-bug (pgrust already fixes it): 18.3's
    array_agg_array_combine gates the whole null-bitmap merge on state2 having
    a bitmap, so when state1 has one and state2 does not the appended items'
    null bits are never written and the result bitmap exposes uninitialized
    heap. That value plane is carved for exactly that shape.

### Harness input-domain bounds (each an out-of-contract fix, not a carve of
### behaviour; all documented at their definition site)

  - images MAXALIGN-aligned (`Aligned`) — the element walk aligns on absolute
    addresses, so a Vec<u8>-aligned image desynchronises reader and writer;
  - `ArrayCheckBounds` respected: `dims[i] + lb[i]` must not overflow, clamped
    after dims are final;
  - varlena elements padded after the last element too (a real one-element
    text array is 32 bytes, not 30 — ground-truthed);
  - declared `nbytes`/`aitems` bounded on the raw-deserialize arm so both sides
    stop allocating gigabytes before erroring.

### Remaining (steps 4-5, parent lane owns)

  - [ ] fleet campaign at the >=10M-exec floor (one job for this target),
        `PGRUST_FUZZ_CSANCOV=1` for C-side sancov union coverage.
  - [ ] ledger rows + `proofs/SUITE.tsv` registration + routes-row status flips.
  - [ ] coverage merge / rendered-red audit / mutants audit.
