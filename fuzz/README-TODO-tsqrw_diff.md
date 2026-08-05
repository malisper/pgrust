# README-TODO: tsqrw_diff (crates/backend/utils/adt/tsquery_rewrite)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `tsquery_rewrite` | 3684 | `tsquery_rewrite.c` |

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_tsqrw_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      reset `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_tsqrw_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/tsqrw_diff.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `adt_tsquery_rewrite::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin tsqrw_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/tsqrw_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/tsqrw_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run tsqrw_diff` — floor for any fuzz-only claim:
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

---

## Lane-F oracle plan (p1-lanef, 2026-07-31) — READ BEFORE STEP 1

Vendored VERBATIM already (committed, NOT yet compile-gated):
`core/csrc/tsqrw/tsquery_rewrite.c` + `core/csrc/tsqrw/tsquery_util.c` @
`62d6c7d3df` (the QTNode machinery `tsquery_rewrite` depends on:
`QT2QTN`/`QTN2QT`/`QTNCopy`/`QTNSort`/`QTNTernary`/`QTNBinary`/`QTNFree`/
`QTNClearFlags`/`QTNodeCompare`), plus upstream `ts_type.h`/`ts_utils.h` under
`csrc/tsqrw/include/tsearch/`.

**Chosen oracle shape (decided after reading the C):** compare tsquery
**varlena images**, not text.

- Feed both sides the SAME tsquery image bytes, built in Rust from the fuzz
  input (the crate already has the image reader `TsQueryRef`), so the C
  **parser is not needed** — `tsqueryin` would drag in the dictionary/GUC
  cache (`ts_cache`, text-search configuration lookup), which is a
  session-state dependency the phase-1 filter excludes and which
  `tsquery_rewrite` itself does not use.
- Compare the returned image byte-for-byte (`finish_tree`/`copy_image` on the
  Rust side vs `QTN2QT` output on the C side) + the Ok/Err verdict. Text
  output (`tsqueryout` -> `infix()`) is deliberately NOT a plane: it needs
  encoding/mb state, and the image is the stronger surface anyway.
- The three-argument form (oid 3684, `fc_tsquery_rewrite`) is the whole
  in-scope surface. `fc_tsquery_rewrite_query` (oid 3685, `lib.rs:234-328`) is
  the NAMED CARVE — `SPI_connect`/`SPI_prepare`/`SPI_cursor_fetch` over a user
  query is executor state, not a pure function (the C counterpart's
  `#include "executor/spi.h"` at `tsquery_rewrite.c:18` is the same boundary).

Remaining header work for step 1: `ts_utils.h` pulls `nodes/pg_list.h`,
`tsearch/ts_public.h`, `fmgr.h`, `utils/memutils.h`. Do NOT vendor that web —
write `csrc/tsqrw/shim_fe/` stand-ins carrying ONLY the verbatim definitions
these two .c files touch (`QTNode`, `QueryItem`/`QueryOperand`/`QueryOperator`,
`TSQuery`/`TSQueryData`, `QTN_*` flag macros, `TSQUERY_TOO_BIG`, the
`COMPUTESIZE`/`GETQUERY`/`GETOPERAND` accessors), each with a provenance
comment naming the upstream header and line. Then remove the `#error` gates,
uncomment the `.file(...)` lines in `core/build.rs`, and run the
`cc -fsyntax-only` -> native -> link gate before ANY verdict (standing rule).

Status at handoff: **oracle vendored + plan fixed; driver not implemented.**
Routes rows for this crate are `planned`, claim row on main is `claimed`.
