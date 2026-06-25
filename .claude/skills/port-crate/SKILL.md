---
name: port-crate
description: Port one catalog unit into this repo - copy the src-idiomatic crate, complete 100% of the logic, convert to per-owner seam crates, and wire init_seams() immediately. Use when porting or re-porting a crate.
---

# port-crate

Input: a catalog unit (e.g. `backend-commands-vacuum`), or several that
src-idiomatic combined into one crate. The product is a crate in `crates/` with
**all** of the unit's logic, its seams declared in per-owner seam crates, and its
own seams installed — wired in this same change, not later.

## 0. Confirm it isn't already done

Check `CATALOG.tsv`, then verify against the live tree: grep `crates/` for the
unit's function names (it may live under a combined/renamed crate). Docs and
prior claims are not evidence.

## 1. Gather the sources

- C ground truth: `c_sources` from `CATALOG.tsv` / `../pgrust/manifests/<unit>.json`,
  files under `../pgrust/postgres-18.3/`.
- c2rust rendering: `../pgrust/c2rust-runs/<unit>/src/*.rs`.
- src-idiomatic base: find the crate(s) covering this unit in
  `../pgrust/src-idiomatic/crates/` — match by name first, then by grepping for
  the C function names (coverage may be split or combined).
- Old wiring: grep `../pgrust/src-idiomatic/crates/seams/src/` and
  `../pgrust/src-idiomatic/crates/seam-init/src/` for this unit's function names.
  You need this both to know the crate's external surface and to reclaim logic
  that leaked into the wiring layer (step 2e).

## 2. Copy and transform

Copy the src-idiomatic crate into `crates/<unit>` as the base — it is mostly
what we want. Then:

**a. Complete the logic — 100%, no deferrals.** Enumerate every function defined
in the C file(s) (`grep -n '^[A-Za-z_].*(' file.c` plus static fns; cross-check
against the c2rust crate). Every one must be present with its full logic. Where
src-idiomatic stubbed, simplified, or deferred (it does, even where comments say
otherwise), port the missing logic from the C using the c2rust rendering as a
guide. It is fine for a code path to panic because a *callee's crate* isn't
ported yet; it is not fine for this crate's own logic to be missing or
approximated. Loud panic beats silent stub.

**b. Re-seam calls outward.** For each call into another crate:
- If a direct cargo dependency does not create a cycle, depend directly and call
  the function. Default to this.
- If it would cycle: call through the owner's seam crate
  `crates/<owner>-seams`, creating it (decls only; deps: `types`, `seam-core`)
  or extending it as needed. Calls to not-yet-ported owners also go through the
  owner's seam crate — they panic until the owner lands, which is correct.
- Old code calling `seams::<module>::<fn>::call(...)` or per-wave seam crates
  gets rewritten to either a direct call or `<owner>_seams::<fn>::call(...)`.

**c. Declare seams inward.** If other crates will need to call this crate across
a cycle (the frontier report names cycle partners; old `seams/src/` decls for
this unit's functions are the inventory), create `crates/<unit>-seams` with those
declarations.

**d. Install immediately.** Give this crate `pub fn init_seams()` that `set()`s
**every** seam in `crates/<unit>-seams` to the real functions, add the crate to
`seams-init`'s dependencies, and add the one `<unit>::init_seams();` line to
`init_all()`. Wiring is part of this change, never a follow-up.

**e. Reclaim leaked logic.** Read the old `seam-init` wire files touching this
unit. Any logic there — dispatch matches, node construction, branching adapters,
fallback computation — belongs in THIS crate: port it in as real functions.
Seams here are thin marshal + delegate only; an `init_seams()` contains nothing
but `set()` calls.

**f. Design-conformance pass.** Before the gate, run these greps over YOUR
diff and justify or fix every hit — judgment misses what greps catch:

- `grep -nE 'type \w+ = (Oid|usize|u(8|16|32|64)|i(8|16|32|64));'` — stand-in
  alias for a typed C pointer/enum? Define the real type (AGENTS.md decision
  table; types.md rules 6-7).
- `grep -n '&\[u8\]'` in seam decls / pub signatures — byte blob for data the
  C types out? Same.
- `grep -nE 'format!|\.to_string\(\)|\.to_owned\(\)|vec!|\.clone\(\)|String::new|Vec::new'`
  — on a path whose C pallocs, each is an infallible allocation: switch to the
  fallible mcx pattern, or justify (test code, error-message construction at
  a return-Err site, genuinely non-allocating C counterpart).
- `grep -nE 'static |Atomic|Mutex|OnceCell|lazy_static'` — per-backend C
  global? thread_local. Query-lifecycle state? Ctx/owner value, not ambient.
- `grep -nE 'pub fn \w+\(\) ->' crates/*-seams/` — zero-arg getter seam for a
  foreign global? Parameter instead.
- `grep -nE 'unwrap|panic!|unreachable!|todo!'` — owned-logic panic standing
  in for an error path? FATAL/PANIC C sites are Err(PgError) per AGENTS.md.
- `grep -niE 'for now|simplified|hack|workaround|TODO|FIXME'` — bank it in
  DESIGN_DEBT.md or fix it; silent divergence comments are findings.
- Any lock/pin acquired where release is not a `Drop`: restructure to a guard
  or with-callback before porting onward (AGENTS.md "Locks and held
  resources").

**g. Trim comments.** Keep comments that state a constraint the code can't show
(and the C file's own substantive comments where they explain the algorithm).
Strip porting-history narration, wave/audit breadcrumbs, "faithful to C line N"
notes, and per-line explanations.

## 3. Types

Copy type definitions this crate needs from
`../pgrust/src-idiomatic/crates/types` into the layered `crates/types-*` stack
(`types-core` → `types-datum`/`types-wchar` → `types-tuple` → ...), preserving
module names. Placement rules are in `docs/types.md`: lowest crate that stays
acyclic, new small `types-<subsystem>` crate when nothing fits, copy only the
items the port consumes — never recreate a god types crate.

## 4. Gate

Batch all edits, then one gate at the end: `cargo check -p <unit> -p <unit>-seams
-p seams-init` (workspace check only if shared types moved). Run the crate's
tests if it has quick ones.

## 5. Record and stop

Update the unit's `CATALOG.tsv` row: `status=ported`, `crate`, mapping notes.
Commit on a branch. **Do not merge** — `/audit-crate` must pass first; it is a
hard blocker. Do not push without being asked.
