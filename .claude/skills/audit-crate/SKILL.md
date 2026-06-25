---
name: audit-crate
description: Function-by-function logic audit of a ported crate against the original Postgres C and the c2rust translation. Hard merge blocker - a crate may not merge until this passes clean. Use after port-crate and before any merge.
---

# audit-crate

Input: a ported crate / catalog unit. The audit is **independent** of the port:
re-derive everything from the sources; do not trust the port's comments, its
self-review, or a green build. Output is a written report; a clean report is a
precondition for merging the crate.

## 1. Build the function inventory — enumerate, never categorize

From the C source (`c_sources` in `CATALOG.tsv`, under
`../pgrust/postgres-18.3/`): list **every** function definition, including
statics and inline helpers. Cross-check the list against
`../pgrust/c2rust-runs/<unit>/src/*.rs` (c2rust kept everything the build kept;
note that it ran post-preprocessor, so `#if` branches outside the build config
exist only in the original C). Auditing by category, wildcard, or "the rest are
trivial" is how missing cores slipped through before — every function gets a row.

## 2. Compare each function

For each function, read all three: the C, the c2rust rendering, and the Rust
port. The port should be idiomatic, but the **logic must match exactly**:

- control flow: every branch, loop bound, early return, fallthrough, goto-shaped
  path has a counterpart;
- error paths: every `ereport`/`elog` maps to an error with the same SQLSTATE
  and severity; error conditions fire under the same predicates;
- constants: OIDs, NodeTags, flag bits, limits, magic numbers, format strings —
  verify values against the C headers, not from memory (transcribed tables are a
  recurring silent-corruption bug);
- edge cases: NULL/empty handling, overflow checks, sign/width of integer
  conversions, off-by-one boundaries;
- idiomatic restructuring (iterators for loops, `Result` for error codes, owned
  values for pointer juggling) passes only if behavior is provably identical on
  every input.

Verdicts: `MATCH`, `DIVERGES` (with the exact behavioral difference),
`MISSING`, `PARTIAL` (logic simplified/approximated), `SEAMED` (call delegated
across a seam — see step 3).

## 3. Audit the seams and wiring

**Ownership is by C-source coverage.** Enumerate the unit's owned seam crates
as: every `crates/X-seams` where `X` maps to any C file in this unit's
`c_sources` (per-file seam crates for combined units included) — never by
matching the unit's crate name. Every declaration in every owned seam crate
must be installed by the crate's `init_seams()`; an empty installer with
owned seam crates outstanding is an automatic FAIL.

- Every outward seam call is justified by a real dependency cycle (a direct dep
  must actually fail) and is thin marshal + delegate: argument conversion, one
  call, result conversion. Any branching, node construction, or computation in a
  seam path is a finding — that logic belongs in a crate.
- `crates/<unit>-seams` declarations all get installed by the crate's
  `init_seams()`, which contains nothing but `set()` calls, and
  `seams-init::init_all()` calls it. An uninstalled seam or a `set()` outside
  the owner is a finding.
- A function whose *body* was replaced by a seam call to "somewhere else" is not
  `SEAMED`, it is `MISSING` — the logic must live in this crate.

## 3b. Design conformance (also merge-blocking)

Logic parity is necessary, not sufficient. The audit also FAILS on violations
of the repo's architecture rules, judged against the diff. Check the
neighbor-dependency decisions first (AGENTS.md "When your unit needs something
an unported neighbor owns" — that table is where most debt enters): invented opacity
(types.md rules 6-7), allocating functions/seams without `Mcx` + `PgResult`,
shared statics for per-backend globals, ambient-global seams, locks held
across `?` without guards, registry-shaped side tables, and unledgered
divergence markers. Cite the rule for each finding; fix-and-re-audit applies
the same as logic findings.

## 4. Verdict and report

Write `audits/<crate>.md`: the full per-function table (C location, port
location, verdict, notes), the seam audit, and a pass/fail verdict.

- **PASS** requires every function `MATCH` (or `SEAMED` per step 3's rules) and
  zero seam findings. `MISSING`/`PARTIAL`/`DIVERGES` = **FAIL** — there are no
  acceptable deferrals; panicking on an unported *callee* is fine, absent logic
  is not.
- On FAIL: fix the port (or hand the findings back), then **re-audit the fixed
  functions from scratch**. Repeat until clean.
- On PASS: set the unit's `CATALOG.tsv` row to `audited`. Only then may the
  crate merge.

Spot-check the auditor too: re-derive a sample of `MATCH` verdicts in detail
before signing off — auditors that skim produce false greens, and a false PASS
here costs more than a slow audit.
