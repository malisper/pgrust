# Running the PostgreSQL regression suite against pgrust

This file records what a fresh clone contains for reproducing the
"46,066/46,066" regression-suite result in the top-level README, and what it
does not contain. See issue #65.

## What is in the repository

- The upstream corpus. `crates/postgres-18.6-reference/` is an unmodified
  `git archive` of PostgreSQL tag `REL_18_6`
  (see `crates/postgres-18.6-reference/README-WHY-THIS-IS-HERE.md`).
  The regression corpus is `crates/postgres-18.6-reference/src/test/regress`
  (`sql/`, `expected/`, `parallel_schedule`). `parallel_schedule` lists 231
  tests. Isolation specs are in
  `crates/postgres-18.6-reference/src/test/isolation/specs`.
- The server binary. The `postgres` binary comes from the `main_main` crate
  (`crates/backend/main/main_main`). `Cargo.toml` defines a `fast-profile`
  build profile. The build command used in `proofs/COVERAGE.md` is:

      cargo build -p main_main --bin postgres --profile fast-profile

  which writes `target/fast-profile/postgres`.
- `regress/overlay/sql/`: 154 copies of upstream `sql/` files with added
  `-- pgrust:` comment annotations (`pgrust:rowsort`, `pgrust:ruled <id>`).
  The runner uses these in place of the upstream files.
- `regress/isolation-overlay/`: extra isolation specs and expected output,
  plus `schedule.extra`, appended after the upstream isolation schedule.
- `regress/tap-overlay/recovery/`: overlay files for the recovery TAP tests.

## Which corpus the recorded runs used

The documents in the tree describe two different corpora:

- The vendored tree and the top-level README name PostgreSQL 18.6, with 231
  tests in `parallel_schedule`.
- `proofs/TESTING-STRATEGY.md` section 1.1 and `proofs/COVERAGE.md` describe
  runs against PostgreSQL 18.3, with 230 scheduled tests, read from an
  out-of-tree path, `../pgrust-reference/vendor/postgres-src/src/test/regress`.

According to
`crates/postgres-18.6-reference/README-WHY-THIS-IS-HERE.md`, the reference
moved from 18.3 to 18.6 on 2026-09-01. The latest dated pass recorded in
`proofs/TESTING-STRATEGY.md` ("pg_regress ALL: 230/230 ok", about 2026-07-21)
predates that move, so the run evidence in `proofs/` refers to the 18.3
corpus. Nothing in the repository says which corpus produced the 46,066
figure, and without the runner it cannot be recomputed from a clone.

## External prerequisites

From `proofs/COVERAGE.md` ("How to regenerate") and
`proofs/TESTING-STRATEGY.md` section 1.1, the runner uses the C PostgreSQL 18
`initdb`, `psql` and `pg_regress` binaries, not pgrust builds of them. On
macOS the documented source is Homebrew `postgresql@18`:

- `PGINSTALL=/opt/homebrew/opt/postgresql@18/bin`
- `PG_REGRESS=/opt/homebrew/lib/postgresql@18/pgxs/src/test/regress/pg_regress`

`pg_regress` runs in `--use-existing` mode against a running pgrust
postmaster, with `--expecteddir` pointing at the upstream `expected/` files.

## What is missing

The runner script and the documents the top-level README links to are not in
the repository as of this commit:

- `scripts/pg-regress-fast.sh`, the runner. The README, the reference-tree
  README and `proofs/` all name it, and it is the script that applies the
  overlay, starts the server and calls `pg_regress`. There is no `scripts/`
  directory.
- `docs/conformance/README.md` and `docs/conformance/regress-gate.md`, which
  the README cites for the exact commands, what is counted, and how the
  46,066 figure is derived. There is no `docs/conformance/` directory.
- `proofs/coverage/pg-regress-cov.sh`, the coverage variant of the runner.
- `scripts/lane-gates.sh`, `scripts/lane-epq-inc1-e2e.sh` (the isolation
  overlay builder named in `regress/isolation-overlay/schedule.extra`), and
  the `notes/` files cited as run evidence.
- CI configuration. There is no `.github/` directory;
  `proofs/TESTING-STRATEGY.md` states the runs were CI cluster jobs submitted
  by hand.

Without `scripts/pg-regress-fast.sh` there is no command in this repository
that runs the suite, and the derivation of the number 46,066 (the
`parallel_schedule` has 231 tests) cannot be checked from the clone. The
environment variables shown in `proofs/COVERAGE.md` (`PGRUST_BIN`,
`NO_BUILD`, `PGINSTALL`, `PG_REGRESS`, `REGRESS_SRC`, `OUTDIR`) describe
that script's interface, but the script that reads them is absent.

`fixtures/pg-regress/` and `fixtures/pg-regress-mini/` exist, but nothing in
the tree references them.
