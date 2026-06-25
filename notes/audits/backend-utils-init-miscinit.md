# Audit: backend-utils-init-miscinit

- **Date**: 2026-06-13
- **Model**: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Auditor**: independent `/audit-crate` re-audit after the seam reconciliation
  round (re-derived from the C; does not trust the port's comments or the prior
  audit's verdicts).
- **C source**: `src/backend/utils/init/miscinit.c` (1952 lines)
- **c2rust**: `../pgrust/c2rust-runs/backend-utils-init-miscinit/src/miscinit.rs`
- **Port**: `crates/backend-utils-init-miscinit/src/{lib.rs,lockfile.rs,process.rs}`
- **Owned seam crate**: `crates/backend-utils-init-miscinit-seams` (the only
  `X-seams` crate whose `X` maps to `miscinit.c`).

## Top-line verdict: **PASS**

The prior audit (2026-06-12) was FAIL on exactly one finding-class (N2): three
owned-seam declarations were uninstalled because their signatures under-specified
the C failure surface. This round reconciled those three to the real C contract
and installed them, then installed four further declarations that the
merge-from-main introduced. The crate now has **zero** uninstalled owned-seam
declarations, every miscinit.c function `MATCH`es, and both workspace gates are
green.

## A. Resolution of the prior FAIL (N2) — the three reconciled seams

Per *seam-signatures-mirror-c-failure-surface*, each declaration was reshaped to
mirror the real C function, then installed via `set()` in `init_seams()`:

| Seam | Old decl | New decl (C-faithful) | C justification |
|---|---|---|---|
| `init_standalone_process` | `(argv0: &str)` | `(argv0: &str) -> PgResult<()>` | `InitStandaloneProcess` (miscinit.c:175) `elog(FATAL)`s on `find_my_exec` failure |
| `superuser` | `() -> bool` | `(Mcx) -> PgResult<bool>` | `superuser()` (superuser.c) = `superuser_arg(GetUserId())`; AUTHOID syscache read can `ereport(ERROR)` |
| `has_rolreplication` | `(Oid) -> bool` | `(Mcx, Oid) -> PgResult<bool>` | `has_rolreplication` (miscinit.c:739) does an AUTHOID syscache lookup + `superuser_arg` |

In-crate backing:
- `process::InitStandaloneProcess` already returned `PgResult<()>` — installed directly.
- `has_rolreplication` already had the in-crate shape `(Mcx, Oid) -> PgResult<bool>` — installed directly.
- `superuser(_mcx) -> PgResult<bool>` added in-crate as `superuser_arg(GetUserId())`; the
  `Mcx` mirrors the C catalog-lookup surface (the read happens inside the
  `superuser_arg` owner). Installed.

Consumers adapted to the reconciled signatures (merged crates; main's shapes
follow the corrected C contract):
- `backend-bootstrap-bootstrap`: `init_standalone_process::call(..)?` (in a `PgResult` fn).
- `backend-utils-fmgr-core`: `fmgr_security_definer_body` `superuser::call(mcx)?`.
- `backend-replication-slot`: `CheckSlotPermissions` now takes `Mcx`; `has_rolreplication::call(mcx, user_id)?`;
  the `check_slot_permissions` seam in `backend-replication-slot-seams` reshaped
  to `(Mcx, Oid)` (mcx dep added to that Cargo.toml). No external callers of
  `check_slot_permissions` to thread.
- `backend-commands-foreigncmds`: all four `superuser::call(mcx)?` sites + the
  `superuser_arg::call(..)?` site; `AlterForeignDataWrapperOwner_internal`
  threads `Mcx` (its two callers pass `mcx`).
- `backend-commands-opclasscmds`: three `superuser_arg::call(..)?` sites
  (the seam returns `PgResult`; the calls lacked `?` post-merge).

## B. New finding this round (fixed) — four more uninstalled owned-seam decls

The merge-from-main grew the owned seam crate from 27 to 32 declarations. Beyond
the three above, four newly-merged declarations were uninstalled (an uninstalled
owned-seam declaration is an automatic FAIL). All four were installed this round,
each delegating to in-crate logic:

- `set_database_path(&str)` — direct write of the globals.c `DatabasePath`
  (the `ProcessCommittedInvalidationMessages` recovery "quick hack" that bypasses
  `SetDatabasePath`'s one-shot Assert).
- `clear_database_path()` — clears `DatabasePath` to `None` (pairs with the above).
- `set_my_backend_type_wal_summarizer()` — `SetMyBackendType(BackendType::WalSummarizer)`.
- `am_wal_summarizer_process() -> bool` — `GetMyBackendType() == BackendType::WalSummarizer`.

## C. Seam audit — all 32 owned declarations installed

`init_seams()` is `set()`-only and `seams-init::init_all()` calls it
(`crates/seams-init/src/lib.rs:63`). A declared-vs-installed diff over
`backend-utils-init-miscinit-seams` is now **empty** — all 32 declarations are
installed. Outward seam calls (syscache, guc, superuser, varlena, dfmgr, path,
exec, fileperm, latch, proc, pmsignal, interrupt, procarray, parallel, inval,
ipc, sysv-shmem, pqcomm, waiteventset) remain thin marshal+delegate against real
unported owners — no branching/computation in seam paths (re-confirmed). The
crit-section / interrupt / `superuser_arg` / `superuser` declarations are
non-miscinit functions (miscadmin.h macros over globals.c counters; superuser.c)
that earlier consumers declared in this crate; miscinit bridges them to the real
owners' values/seams until globals.c / superuser.c land — pre-existing bridge
disposition, not a logic gap.

## D. Function inventory — all 51 functions MATCH

All 51 miscinit.c function definitions enumerated; none MISSING, none whose body
was replaced by an external seam call. Detailed re-derivation spot-checks
(C ↔ Rust side-by-side) all MATCH:

| C function | C line | Verdict | Notes |
|---|---|---|---|
| InitStandaloneProcess | 175 | MATCH | seam now `-> PgResult<()>`; sequence + path resolution + sigmask preserved |
| has_rolreplication | 739 | MATCH | superuser_arg early-return; AUTHOID lookup; seam now `(Mcx, Oid) -> PgResult<bool>` |
| CreateLockFile | 1210 (static) | MATCH | retry loop (cap 100); all exits FATAL; file-access SQLSTATE; LOCK_FILE_EXISTS + hint; shmem scan @ key 7; reverse-order proc_exit |
| checkDataDir | 347 | MATCH | stat ENOENT/other (file-access SQLSTATE); S_ISDIR; euid; PG_MODE_MASK_GROUP=0o027; SetDataDirectoryCreatePerm+umask |
| GetBackendTypeDesc | 263 | MATCH | all 18 strings verified; B_INVALID → "not initialized"; exhaustive enum match |
| ValidatePgVersion | 1770 | MATCH | ENOENT → INVALID_PARAMETER_VALUE; non-ENOENT → file-access SQLSTATE; major-mismatch FATAL (lone `+`/`-` token note is cosmetic — same severity+SQLSTATE) |

The remaining 45 functions (user-id state machine, ClientConnectionInfo
estimate/serialize/restore, the lock-file family, processing-mode/backend-type
accessors, library preload, etc.) were confirmed present with local logic and
matched the prior audit's MATCH verdicts (re-confirmed, none replaced by external
seams). The prior audit's logic fixes (F1/F2/F3 + N1 file-access SQLSTATEs)
remain in place.

## E. Gates

- `cargo check --workspace`: clean (no errors; only pre-existing unrelated warnings).
- `cargo test --workspace`: **1121 passed, 0 failed** (incl. miscinit's 10 unit tests).
  - Note: the merge surfaced a pre-existing duplicate `relowner` struct-literal
    field (on `main`) in two unrelated test files
    (`backend-access-brin-tuple/src/tests.rs`,
    `backend-commands-copyfromparse/src/tests.rs`); the spurious second field was
    removed so the workspace test build compiles. Unrelated to miscinit logic.

## F. Disposition

**PASS.** Logic clean (all 51 functions MATCH); all 32 owned-seam declarations
installed (the 3 reconciled to the C failure surface + 4 merge-introduced
installed); consumers adapted; both gates green. CATALOG row set to `audited`.
