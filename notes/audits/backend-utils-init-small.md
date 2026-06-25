# Audit: backend-utils-init-small

- **Unit**: `backend-utils-init-small` — `src/backend/utils/init/globals.c` + `src/backend/utils/init/usercontext.c`
- **Port crates**: `crates/backend-utils-init-small` (logic), `crates/backend-utils-init-small-seams` (this unit's seam, pre-declared by the nodeMaterial port), `crates/backend-utils-init-miscinit-seams`, `crates/backend-utils-adt-acl-seams` (new outward seam declarations), `crates/types-net` (vocabulary)
- **C sources**: `../pgrust/postgres-18.3/src/backend/utils/init/{globals.c,usercontext.c}`
- **c2rust**: `../pgrust/c2rust-runs/backend-utils-init-small/src/{globals.rs,usercontext.rs}`
- **Auditor**: independent re-derivation from the C and c2rust sources; constants verified against the PG 18.3 headers, not from memory.
- **Audit rounds**: initial audit FAILED (1 finding, below); fixed in this branch and re-audited → **PASS**.

## 1. Function inventory

`globals.c` contains **zero** function definitions — it is exclusively global
variable definitions (cross-checked against the c2rust rendering, which has
only `static mut` items and no `fn`). `usercontext.c` contains exactly two
functions. The c2rust rendering confirms both lists; the only
`#if`-conditional item is `postgres_exec_path` (`EXEC_BACKEND`), absent from
the c2rust build and intentionally compiled out of the port (documented in
the module header) — same as the unix build.

## 2. Per-function table

| # | C function | C location | Port location | Verdict | Notes |
|---|------------|-----------|---------------|---------|-------|
| 1 | `SwitchToUntrustedUser` | usercontext.c:32 | `usercontext.rs::SwitchToUntrustedUser` | MATCH | Step-by-step against C and c2rust: (1) `GetUserIdAndSecContext(&save_userid, &save_sec_context)` → seam returning the tuple, stored into `context`. (2) `!member_can_set_role(save_userid, userid)` → ereport ERROR, sqlstate `42501` (`ERRCODE_INSUFFICIENT_PRIVILEGE` verified in errcodes.txt; `make_sqlstate(*b"42501")` matches c2rust's MAKE_SQLSTATE expansion), message `role "%s" cannot SET ROLE to "%s"` with `GetUserNameFromId(save_userid,false)` / `GetUserNameFromId(userid,false)` in the same order, location `("usercontext.c", 45, "SwitchToUntrustedUser")` (lineno 45 verified via c2rust errfinish args; basename normalization is the error crate's audited convention). Name-lookup errors propagate as `Err`, the same error C would surface (in C the inner `ereport(ERROR)` fired during errmsg-arg evaluation longjmps past the outer report). `noerr=false` contract means `Ok(None)` is impossible; the `expect` is unreachable. (3) `member_can_set_role(userid, save_userid)` true → `SetUserIdAndSecContext(userid, save_sec_context)`, `save_nestlevel = -1` (`USER_CONTEXT_NO_NEST_LEVEL == -1`). (4) else → `sec_context |= SECURITY_RESTRICTED_OPERATION` (0x2, verified miscadmin.h), `SetUserIdAndSecContext(userid, sec_context)`, `save_nestlevel = NewGUCNestLevel()`. Identical control flow, predicates, constants, call order. |
| 2 | `RestoreUserContext` | usercontext.c:86 | `usercontext.rs::RestoreUserContext` | MATCH | `save_nestlevel != -1` → `AtEOXact_GUC(false, save_nestlevel)`; then unconditionally `SetUserIdAndSecContext(save_userid, save_sec_context)`. Same order, same predicate, `isCommit=false` literal preserved. |

## 3. Variable inventory (globals.c)

Every variable definition in globals.c, checked name / width / initializer
against the C source and headers (the c2rust rendering agrees on all
initializers):

| C variable | Default | Port | Verdict |
|---|---|---|---|
| `FrontendProtocol` | 0 | delegated accessor → `backend_utils_error::config::frontend_protocol` (default 0) | MATCH (single store, see §5 finding 1) |
| `InterruptPending`, `QueryCancelPending`, `ProcDiePending`, `CheckClientConnectionPending`, `ClientConnectionLost`, `IdleInTransactionSessionTimeoutPending`, `TransactionTimeoutPending`, `IdleSessionTimeoutPending`, `ProcSignalBarrierPending`, `LogMemoryContextPending`, `IdleStatsUpdateTimeoutPending` | false ×11 | `thread_local` `Cell<bool>` ×11, false | MATCH |
| `InterruptHoldoffCount`, `QueryCancelHoldoffCount` | 0 | `Cell<u32>`, 0 | MATCH |
| `CritSectionCount` | 0 | delegated → `config::crit_section_count` (default 0; errfinish's ERROR-recovery `CritSectionCount = 0` hits the same store, as in C) | MATCH |
| `MyProcPid`, `MyStartTime`, `MyStartTimestamp` | 0 (static init) | `Cell<i32>` / `Cell<pg_time_t>` / `Cell<TimestampTz>`, 0 | MATCH |
| `MyClientSocket`, `MyProcPort`, `MyLatch` | NULL | `Option<ClientSocket>` / `Option<Box<Port>>` / `Option<Latch>`, `None`; `IsSet`/`Take`/`With` mirror the C pointer idioms | MATCH |
| `MyCancelKey[MAX_CANCEL_KEY_LENGTH]` | zeros | `[u8; 32]` zeros (`MAX_CANCEL_KEY_LENGTH == 32`, procsignal.h:67) | MATCH |
| `MyCancelKeyLength`, `MyPMChildSlot` | 0 | `Cell<i32>`, 0 | MATCH |
| `DataDir`, `DatabasePath` | NULL | `Option<String>`, `None` | MATCH |
| `data_directory_mode` | `PG_DIR_MODE_OWNER` = `S_IRWXU` = 0o700 (file_perm.h:32) | 0o700 | MATCH |
| `OutputFileName[MAXPGPATH]` | zeros | delegated → `config::output_file_name` (`None` ⇔ empty buffer); array view reconstructed on read, `MAXPGPATH == 1024` (pg_config_manual.h:100) | MATCH |
| `my_exec_path`, `pkglib_path` | zeros | `[u8; 1024]` zeros | MATCH |
| `postgres_exec_path` | `EXEC_BACKEND` only | compiled out (matches unix build / c2rust) | MATCH |
| `MyProcNumber`, `ParallelLeaderProcNumber` | `INVALID_PROC_NUMBER` = -1 (procnumber.h:26) | -1 | MATCH |
| `MyDatabaseId`, `MyDatabaseTableSpace` | `InvalidOid` = 0 | 0 | MATCH |
| `MyDatabaseHasLoginEventTriggers` | false | false | MATCH |
| `PostmasterPid` | 0 | 0 (`pid_t` = i32) | MATCH |
| `IsPostmasterEnvironment`, `IsBinaryUpgrade` | false | false | MATCH |
| `IsUnderPostmaster` | false | delegated → `config::is_under_postmaster` (default false) | MATCH |
| `ExitOnAnyError` | false | delegated → `config::exit_on_any_error` (default false) | MATCH |
| `DateStyle` | `USE_ISO_DATES` = 1 (miscadmin.h:237) | 1 | MATCH |
| `DateOrder` | `DATEORDER_MDY` = 2 (miscadmin.h:245) | 2 | MATCH |
| `IntervalStyle` | `INTSTYLE_POSTGRES` = 0 (miscadmin.h:257) | 0 | MATCH |
| `enableFsync` | true | true | MATCH |
| `allowSystemTableMods` | false | false | MATCH |
| `work_mem` | 4096 | 4096 | MATCH |
| `hash_mem_multiplier` | 2.0 | 2.0 (f64) | MATCH |
| `maintenance_work_mem` | 65536 | 65536 | MATCH |
| `max_parallel_maintenance_workers` | 2 | 2 | MATCH |
| `NBuffers` | 16384 | 16384 | MATCH |
| `MaxConnections` | 100 | 100 | MATCH |
| `max_worker_processes` / `max_parallel_workers` | 8 / 8 | 8 / 8 | MATCH |
| `MaxBackends` | 0 | 0 | MATCH |
| `VacuumBufferUsageLimit` | 2048 | 2048 | MATCH |
| `VacuumCostPageHit/Miss/Dirty` | 1 / 2 / 20 | 1 / 2 / 20 | MATCH |
| `VacuumCostLimit` | 200 | 200 | MATCH |
| `VacuumCostDelay` | 0 | 0.0 (f64) | MATCH |
| `VacuumCostBalance`, `VacuumCostActive` | 0, false | 0, false | MATCH |
| `commit_timestamp_buffers` | 0 | 0 | MATCH |
| `multixact_member_buffers` / `multixact_offset_buffers` | 32 / 16 | 32 / 16 | MATCH |
| `notify_buffers` / `serializable_buffers` | 16 / 32 | 16 / 32 | MATCH |
| `subtransaction_buffers` / `transaction_buffers` | 0 / 0 | 0 / 0 | MATCH |

All variables are `thread_local` (backend == thread per repo convention), so
the C per-process semantics are preserved per backend.

## 4. Types audit

- `UserContext` (`types-core::init`): `save_userid`/`save_sec_context`/`save_nestlevel` — matches `utils/usercontext.h`. `uninitialized()` stand-in for the C uninitialized stack declaration is safe and documented.
- `SECURITY_LOCAL_USERID_CHANGE` 0x1, `SECURITY_RESTRICTED_OPERATION` 0x2, `SECURITY_NOFORCE_RLS` 0x4 — verified against miscadmin.h.
- `UserAuth` enum values 0..15 — verified against the c2rust rendering of hba.h (uaReject..uaOAuth).
- `types-net`: `SockAddr` (128-byte `sockaddr_storage` + salen), `ClientSocket`, `Port` field-for-field against the c2rust `Port` struct (non-GSS / OpenSSL build branches documented; pointer fields → `Option`, `List*` → `Vec`), `HbaLine` field-for-field against `libpq/hba.h` including the `AuthToken` lists and `radius*` split lists, `Latch` field-for-field against `storage/latch.h` (WIN32 `event` correctly absent), `SCRAM_MAX_KEY_LEN == 32` verified via scram-common.h. Vocabulary only, no logic. MATCH.
- Constants used by the unit (`MAX_CANCEL_KEY_LENGTH`, `MAXPGPATH`, `PG_DIR_MODE_OWNER`, `USE_ISO_DATES`, `DATEORDER_MDY`, `INTSTYLE_POSTGRES`, `INVALID_PROC_NUMBER`, `InvalidOid`) all verified against headers (values above).

## 5. Findings and fixes

**Finding 1 (initial audit, DIVERGES → fixed).** `globals.rs` declared its own
`thread_local` cells for `FrontendProtocol`, `CritSectionCount`,
`IsUnderPostmaster`, `ExitOnAnyError`, and `OutputFileName`, but the
already-merged elog.c port (`backend-utils-error/src/config.rs`) keeps its own
backend-local store for exactly these globals.c variables, which
`errstart` (PANIC promotion on `CritSectionCount > 0`, FATAL promotion on
`ExitOnAnyError`), `errfinish` (writes `CritSectionCount = 0` during ERROR
recovery), `DebugFileOpen` (`OutputFileName`), `send_message_to_frontend`
(`FrontendProtocol`), and the stderr fallback (`IsUnderPostmaster`) read. In C
each is one variable; the port had two unlinked copies, so e.g.
`SetExitOnAnyError(true)` would never promote ERROR→FATAL in elog. **Fix**:
the five accessors now delegate to the `backend_utils_error::config` store
(the store errfinish already writes), keeping one variable per C variable;
`OutputFileName` keeps its C-shaped `[u8; MAXPGPATH]` API by converting on
the boundary (`None`/empty ⇔ all-zero buffer, strlcpy-style truncation at
`MAXPGPATH-1`), plus a `SetOutputFileNameStr` convenience. Regression test
`elog_visible_globals_share_a_single_store` added. Re-audited from scratch
after the fix: defaults unchanged (config defaults are 0/false/None), widths
unchanged, behavior now single-store. MATCH.

No other findings.

## 6. Seam audit

Outward seams (each justified by a real cycle: miscinit.c, acl.c, and guc.c
all read globals.c state and report errors through elog, so direct deps on
future ports of those units would cycle back through this layer; none of the
owners are ported yet — calls panic loudly until they land, which is the
sanctioned behavior):

- `backend-utils-init-miscinit-seams`: `get_user_id_and_sec_context`, `set_user_id_and_sec_context`, `get_user_name_from_id` — signatures match the C prototypes (out-params → tuple; `char *` + pstrdup → `PgResult<Option<PgString>>` with the `noerr` contract documented). Declaration-only crate; no logic. Call sites are thin: argument pass-through, one call, result conversion. OK.
- `backend-utils-adt-acl-seams`: `member_can_set_role(Oid, Oid) -> PgResult<bool>` — matches acl.c prototype; catalog-lookup fallibility surfaced as `PgResult`. Declaration-only. OK.
- `backend-utils-misc-guc-seams` (pre-existing crate, extended): `new_guc_nest_level() -> i32`, `at_eoxact_guc(bool, i32) -> PgResult<()>` — match guc.c prototypes. Declaration-only. OK.
- Inward seam: `backend-utils-init-small-seams::work_mem` (declared earlier by the nodeMaterial port) is installed by this crate's `init_seams()`, which contains exactly one `set()` call and nothing else; `seams-init::init_all()` calls `backend_utils_init_small::init_seams()` (verified). OK.
- No `set()` calls outside the owner anywhere in production code (grep-verified); the only other `set()`s are test fakes inside this crate's own test binary, consistent with prior audited crates.
- No seam-call body replacement: both usercontext.c functions keep their full logic in this crate; only the genuinely-foreign calls cross seams.

## 7. Build & tests

- `cargo build --workspace`: clean.
- `cargo test -p backend-utils-init-small`: 10/10 pass (defaults vs globals.c, per-thread isolation, seam installation, single-store linkage, and all four usercontext paths: reciprocal switch, one-way switch with SECURITY_RESTRICTED_OPERATION + nest level, 42501 refusal with exact message, name-lookup error propagation, restore with/without nest level).
- `cargo clippy`: only the repo-wide pre-existing `result_large_err` lint (PgError size), present in every crate returning `PgResult`.

## 8. Verdict

**PASS** (after 1 fix round). Every function MATCH; every variable MATCH;
zero outstanding seam findings.
