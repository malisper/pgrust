# README-TODO: int_diff (crates/backend/utils/adt/int)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `int2in` | 38 | `int.c` |
| `int2out` | 39 | `int.c` |
| `int2vectorin` | 40 | `int.c` |
| `int2vectorout` | 41 | `int.c` |
| `int4in` | 42 | `int.c` |
| `int4out` | 43 | `int.c` |
| `int2recv` | 2404 | `int.c` |
| `int4recv` | 2406 | `int.c` |
| `int2send` | 2405 | `int.c` |
| `int4send` | 2407 | `int.c` |
| `int4gcd` | 5044 | `int.c` |
| `int4lcm` | 5046 | `int.c` |
| `int4div` | 154 | `int.c` |
| `int2mod` | 155 | `int.c` |
| `int4mod` | 156 | `int.c` |
| `int42div` | 173 | `int.c` |
| `int2div` | 153 | `int.c` |
| `int24div` | 172 | `int.c` |
| `int4pl` | 177 | `int.c` |
| `int4mi` | 181 | `int.c` |
| `int4mul` | 141 | `int.c` |
| `int2pl` | 176 | `int.c` |
| `int2mi` | 180 | `int.c` |
| `int2mul` | 152 | `int.c` |
| `int24pl` | 178 | `int.c` |
| `int24mi` | 182 | `int.c` |
| `int24mul` | 170 | `int.c` |
| `int42pl` | 179 | `int.c` |
| `int42mi` | 183 | `int.c` |
| `int42mul` | 171 | `int.c` |
| `int4um` | 212 | `int.c` |
| `int2um` | 213 | `int.c` |
| `int4abs` | 1251 | `int.c` |
| `int2abs` | 1253 | `int.c` |
| `int4inc` | 766 | `int.c` |
| `i2toi4` | 313 | `int.c` |
| `i4toi2` | 314 | `int.c` |
| `int4larger` | 768 | `int.c` |
| `int4smaller` | 769 | `int.c` |
| `int2larger` | 770 | `int.c` |
| `int2smaller` | 771 | `int.c` |
| `int4and` | 1898 | `int.c` |
| `int4or` | 1899 | `int.c` |
| `int4xor` | 1900 | `int.c` |
| `int4not` | 1901 | `int.c` |
| `int4shl` | 1902 | `int.c` |
| `int4shr` | 1903 | `int.c` |
| `int2and` | 1892 | `int.c` |
| `int2or` | 1893 | `int.c` |
| `int2xor` | 1894 | `int.c` |
| `int2not` | 1895 | `int.c` |
| `int2shl` | 1896 | `int.c` |
| `int2shr` | 1897 | `int.c` |
| `in_range_int4_int4` | 4128 | `int.c` |
| `in_range_int4_int2` | 4129 | `int.c` |
| `in_range_int4_int8` | 4127 | `int.c` |
| `in_range_int2_int4` | 4131 | `int.c` |
| `in_range_int2_int2` | 4132 | `int.c` |
| `in_range_int2_int8` | 4130 | `int.c` |
| `int4eq` | 65 | `int.c` |
| `int4ne` | 144 | `int.c` |
| `int4lt` | 66 | `int.c` |
| `int4le` | 149 | `int.c` |
| `int4gt` | 147 | `int.c` |
| `int4ge` | 150 | `int.c` |
| `int2eq` | 63 | `int.c` |
| `int2ne` | 145 | `int.c` |
| `int2lt` | 64 | `int.c` |
| `int2le` | 148 | `int.c` |
| `int2gt` | 146 | `int.c` |
| `int2ge` | 151 | `int.c` |
| `int24eq` | 158 | `int.c` |
| `int24ne` | 164 | `int.c` |
| `int24lt` | 160 | `int.c` |
| `int24le` | 166 | `int.c` |
| `int24gt` | 162 | `int.c` |
| `int24ge` | 168 | `int.c` |
| `int42eq` | 159 | `int.c` |
| `int42ne` | 165 | `int.c` |
| `int42lt` | 161 | `int.c` |
| `int42le` | 167 | `int.c` |
| `int42gt` | 163 | `int.c` |
| `int42ge` | 169 | `int.c` |
| `int4_bool` | 2557 | `int.c` |
| `bool_int4` | 2558 | `int.c` |

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_int_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      reset `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_int_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/int_diff.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `adt_int::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin int_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/int_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/int_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run int_diff` — floor for any fuzz-only claim:
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
