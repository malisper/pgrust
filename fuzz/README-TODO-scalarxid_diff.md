# README-TODO: scalarxid_diff (crates/backend/utils/adt/scalar)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `tidin` | 1350 | `tid.c` |
| `tidout` | 1351 | `tid.c` |
| `tideq` | 1265 | `tid.c` |
| `tidne` | 1266 | `tid.c` |
| `tidlt` | 2799 | `tid.c` |
| `tidgt` | 2800 | `tid.c` |
| `tidle` | 2801 | `tid.c` |
| `tidge` | 2802 | `tid.c` |
| `bttidcmp` | 2794 | `tid.c` |
| `tidlarger` | 2795 | `tid.c` |
| `tidsmaller` | 2796 | `tid.c` |
| `xidout` | 51 | `xid.c` |
| `xideq` | 68 | `xid.c` |
| `xidneq` | 3315 | `xid.c` |
| `xid8cmp` | 5071 | `xid.c` |
| `xid8eq` | 5068 | `xid.c` |
| `xid8ne` | 5069 | `xid.c` |
| `xid8lt` | 5070 | `xid.c` |
| `xid8gt` | 5075 | `xid.c` |
| `xid8le` | 5076 | `xid.c` |
| `xid8ge` | 5077 | `xid.c` |
| `xid8_larger` | 5078 | `xid.c` |
| `xid8_smaller` | 5079 | `xid.c` |
| `oideq` | 184 | `oid.c` |
| `oidne` | 185 | `oid.c` |
| `oidlt` | 716 | `oid.c` |
| `oidle` | 717 | `oid.c` |
| `oidge` | 1638 | `oid.c` |
| `oidgt` | 1639 | `oid.c` |
| `oidlarger` | 1641 | `oid.c` |
| `oidsmaller` | 1642 | `oid.c` |
| `oidin` | 1798 | `oid.c` |
| `oidout` | 1799 | `oid.c` |
| `oidvectorin` | 54 | `oid.c` |
| `oidvectorout` | 55 | `oid.c` |

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_scalarxid_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      reset `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_scalarxid_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/scalarxid_diff.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `adt_scalar::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin scalarxid_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/scalarxid_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/scalarxid_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run scalarxid_diff` — floor for any fuzz-only claim:
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
