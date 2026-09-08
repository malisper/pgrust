# PL/pgSQL SRF tuplestore resource ownership

Known finding: FP-pl_exec_p2-7. Base: 81b20df13f6 (current main at assignment).
Current code inspection reproduces both missing ownership captures; CI cluster replay
is pending, so this is not a claim of runtime verification or closure.

Tests-only ref: 762a7484bb39aab7b545f332ef01970a5f33825e,
`audit-remediation/plpgsql-before-20260904`.

## Fix

Capture the function-entry resource owner in PL/pgSQL Estate and pass it to
Tuplestore construction. Tuplestore records its creation owner, refreshes that
owner on pool reuse, and reinstates it around initial spill-file creation.
Restore the active owner before propagating a file-creation error. BufFile
retains the same owner for serial-file segment extension, preserving ownership
when a tuplestore crosses 1 GiB. FileSet behavior is unchanged.

C18.6 reference: pl_exec.c:3697–3704 and 4013; tuplestore.c:279 and 853–867;
buffile.c:125 and 156–172. No panic/assert suppression; no new unsafe code.

Existing-PR search (2026-09-04): no competing tuplestore owner remediation.
PR 1508 touches PL/pgSQL grammar/scanner only, not this execution path.

## Verification

All Rust builds/server execution delegated to coordinator's CI cluster submissions.
No local cargo compilation or server execution.

- `cargo metadata --locked --no-deps`: dependency graph validation only.
- `git diff --check`: local whitespace validation.
- CI `TEST_CRATES="tuplestore fd plpgsql"`.
- `spill_keeps_creation_owner_across_child_release_and_pool_reuse`: commit and
  abort child release, owner restoration, post-block append/read, temp cleanup,
  and pool reuse after the previous owner has been deleted.
- `buffile_segment_rollover_keeps_creation_owner`: sparse seek to 1 GiB minus
  four bytes, eight-byte write crossing the real segment boundary, child abort,
  and exact read-back across both segments.
- `crates/pl/plpgsql/tests/sql/tuplestore_exception_owner.sql`: original
  200000-row repro twice, store creation inside/outside EXCEPTION block,
  block commit/abort, append after exit, and session liveness.
  Expected original counts: 200000 twice; boundary count/sum(length):
  2001/200005, 2002/200011, 2002/200011, 2003/200017.
  Run byte comparison against C18.6 before and after, fresh server after
  historical repro failure if the baseline process dies.

## Rebase recipe

Conflicts may touch plpgsql exec.rs/Cargo.toml, tuplestore lib.rs/Cargo.toml/tests,
fd buffile.rs/tests, Cargo.lock. Preserve the function-entry capture and owner
refresh on both fresh and pooled stores; restore active owner on errors before
`?`. Keep the segment-extension saved-owner behavior. Merge dependencies as
sets; only new lockfile dependency is plpgsql -> types_resowner. Reverify touched
crate suites plus grammar corpus on rebase; repeat SQL if semantic conflicts.

No automerge authorization is exercised here; independent review and CI remain
mandatory before the coordinator arms it.
