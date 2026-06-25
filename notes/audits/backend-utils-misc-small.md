# Audit: backend-utils-misc-small

Branch: `port/backend-utils-misc-small`
C sources (CATALOG): `*/help_config.c, */pg_rusage.c, */queryenvironment.c,
*/sampling.c, */stack_depth.c`

## Scope

Per CATALOG, four of the five C files were already ported and **merged on
main** under their own crates and audited there:

| C file | Crate (on main) | Status |
|---|---|---|
| help_config.c | backend-utils-misc-help-config | merged/audited (not touched by this branch) |
| pg_rusage.c | backend-utils-misc-pg-rusage | merged/audited (not touched) |
| queryenvironment.c | backend-utils-misc-queryenvironment | merged/audited (not touched) |
| sampling.c | backend-utils-misc-sampling | merged/audited (not touched) |
| stack_depth.c | **backend-utils-misc-stack-depth** (NEW, this branch) | audited here |

`git diff main..HEAD` touches only: the new crate `backend-utils-misc-stack-depth`
(lib.rs + tests.rs + Cargo.toml), one added seam decl in
`backend-utils-misc-guc-file-seams` (`guc_check_errhint`), the seams-init wiring
(Cargo.toml + init_all + recurrence allowlist), CATALOG.tsv, Cargo.lock. The
four pre-merged crates are untouched, so this audit focuses on `stack_depth.c`
and confirms presence of the others.

Presence confirmation of pre-merged files (not re-audited; already PASS on main):
- help-config: `GucInfoMain` family present.
- pg-rusage: `pg_rusage_init`, `pg_rusage_show` present.
- queryenvironment: `create_queryEnv`, `register_ENR`, `unregister_ENR`,
  `get_ENR`, get_visible/ENRMetadataGetTupDesc present.
- sampling: all 10 BlockSampler/reservoir/anl functions present.

## Function inventory — stack_depth.c

c2rust note: `set_stack_base` is absent from the c2rust rendering (the build
selected the `HAVE__BUILTIN_FRAME_ADDRESS` `#if` branch — c2rust ran
post-preprocessor). The C file defines it; the port implements it. All 7 C
functions accounted for.

| C function (line) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `set_stack_base` (43) | `set_stack_base` (115) | MATCH | `old = base; base = <frame addr>; return old`. Frame addr via `#[inline(never)] current_stack_addr()` (address of a local) — same value C reads via `__builtin_frame_address(0)`/`&stack_base`. |
| `restore_stack_base` (76) | `restore_stack_base` (124) | MATCH | `stack_base_ptr = base`. |
| `check_stack_depth` (94) | `check_stack_depth` (137) | MATCH | `if stack_is_too_deep()` -> `ereport(ERROR, ERRCODE_STATEMENT_TOO_COMPLEX, errmsg "stack depth limit exceeded", errhint "...(currently %dkB)...")`. SQLSTATE 54001 verified vs errcodes.txt. Returns `Err(PgError)` (ereport(ERROR) -> spine error), the repo convention. Hint format string + `max_stack_depth` arg match exactly. |
| `stack_is_too_deep` (108) | `stack_is_too_deep` (159) | MATCH | distance = `base - &local`, abs value (`abs_diff`), trouble iff `depth > max_stack_depth_bytes && base != NULL`. NULL-guard placed last (matches C comment/order). |
| `check_max_stack_depth` (145) | `check_max_stack_depth` (184) | MATCH | `newval_bytes = newval*1024`; if `rlimit>0 && newval_bytes > rlimit-STACK_DEPTH_SLOP` -> GUC_check_errdetail + GUC_check_errhint, return false; else true. STACK_DEPTH_SLOP = 512*1024 verified. Unused C params `extra`/`source` dropped (`source` kept as `_source`); C body never reads them — behavior identical. errdetail/errhint delegate via guc-file-seams (cross-cycle, thin marshal+delegate). |
| `assign_max_stack_depth` (162) | `assign_max_stack_depth` (208) | MATCH | `max_stack_depth_bytes = newval*1024`. Unused `extra` dropped; C never reads it. |
| `get_stack_depth_rlimit` (179) | `get_stack_depth_rlimit` (224) | MATCH | one-time cache (`val==0` sentinel); `getrlimit(RLIMIT_STACK)`: `<0`->-1, `RLIM_INFINITY`->SSIZE_MAX, `>=SSIZE_MAX`->SSIZE_MAX (unsigned-overflow guard), else `rlim_cur`. Uses `libc::getrlimit` directly (genuine OS boundary, matches storage-file-fd convention — no invented seam). SSIZE_MAX = isize::MAX. |

Constants verified against headers: `STACK_DEPTH_SLOP = 512*1024`,
`ERRCODE_STATEMENT_TOO_COMPLEX = 54001`, `RLIMIT_STACK`/`RLIM_INFINITY` via
`libc`, `WIN32_STACK_RLIMIT = 4*1024*1024` (carried, unused on Unix).

## Design conformance

- File-scope C statics (`max_stack_depth`, `max_stack_depth_bytes`,
  `stack_base_ptr`, cached rlimit) are per-backend globals -> modelled as
  `thread_local` `Cell`s. No shared statics for per-backend state. OK.
- `pg_stack_base_t` modelled as `usize` (address only, never dereferenced —
  C only subtracts two such addresses). This resolves the real `char*`
  semantics rather than inventing opacity. OK.
- No `Mcx`/`PgResult` allocation rules implicated (nothing allocates).
- Two genuine OS boundaries (current SP, getrlimit) use `libc` directly,
  consistent with the tree. No ambient-global seam, no registry side table,
  no lock-across-`?`.
- No own-logic stubs, no `todo!`/`unimplemented!`, no deferred/SEAMED-escape of
  this file's own logic. The only seam calls are the two GUC check-error
  channel writes (`guc_check_errdetail`/`guc_check_errhint`), owned by guc-file,
  which holds the `GUC_check_errdetail_string`/`GUC_check_errhint_string`
  backend-local state — thin marshal+delegate across a real cycle.

## Seam & wiring audit

Owned seam crate (by C-file coverage): `backend-utils-misc-stack-depth-seams`,
which declares exactly `check_stack_depth() -> PgResult<()>`. Installed by this
crate's `init_seams()` via `check_stack_depth::set(check_stack_depth)`. OK.

`init_seams()` also installs the `max_stack_depth` GUC machinery into the
guc-tables slots (pre-existing slots): `hooks::check_max_stack_depth`,
`hooks::assign_max_stack_depth`, `vars::max_stack_depth` (get/set accessors).
These slots are owned by the GUC subsystem; stack_depth.c is their hook
provider, so installing them from this owner is correct.

`backend-utils-misc-guc-file-seams::guc_check_errhint` is a new **outward**
declaration consumed here but **owned/installed by guc-file** (mirrors the
existing `guc_check_errdetail`). It is allowlisted as legitimately-unset in
seams-init's recurrence guard, matching `guc_check_errdetail`. OK.

`seams-init::init_all()` calls `backend_utils_misc_stack_depth::init_seams()`
(added). recurrence_guard both tests pass.

## Gates

- `cargo check --workspace`: PASS (only pre-existing warnings in unrelated
  backend-access-common-printtup).
- `cargo test -p backend-utils-misc-stack-depth`: PASS (8/8).
- `cargo test -p seams-init`: PASS (recurrence_guard 2/2 —
  every_seam_installing_crate_is_wired_into_init_all +
  every_declared_seam_is_installed_by_its_owner).

## Verdict: PASS

All 7 stack_depth.c functions MATCH; the four sibling files are present in
their already-merged/audited crates and untouched by this branch. Seams owned,
declared, and installed; wiring and recurrence guards green.
