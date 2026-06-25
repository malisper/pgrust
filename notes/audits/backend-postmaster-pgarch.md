# Audit: backend-postmaster-pgarch

- **Unit:** backend-postmaster-pgarch
- **C source:** `src/backend/postmaster/pgarch.c`
- **Branch:** `port/backend-postmaster-pgarch`
- **Date:** 2026-06-12 (independently re-confirmed 2026-06-13)
- **Model:** Opus 4.8 (claude-opus-4-8[1m])
- **Verdict:** **PASS** (was FAIL; F1 + F2 fixed 2026-06-12, see "Fix" below;
  re-confirmed by a fresh from-scratch re-audit 2026-06-13 — see "Re-confirmation")

Independent re-audit per `.claude/skills/audit-crate/SKILL.md`. Re-derived from
the C, the c2rust rendering
(`../pgrust/c2rust-runs/backend-postmaster-pgarch/src/pgarch.rs`), and the Rust
port (`crates/backend-postmaster-pgarch/src/lib.rs`). Not relying on the port's
comments or the prior audit (which verdicted MATCH on a function this re-audit
finds MISSING — see F1).

## Function inventory and verdicts (17 functions)

| # | C function (pgarch.c) | Kind | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `PgArchShmemSize` (157) | extern | `PgArchShmemSize` (358) | MATCH | `size_of::<PgArchData>()`; add_size from 0. |
| 2 | `PgArchShmemInit` (168) | extern | `PgArchShmemInit` (366) | MATCH | OnceLock `get_or_init`; !found branch zeroes, sets pgprocno=INVALID_PROC_NUMBER(-1), force_dir_scan=0. |
| 3 | `PgArchCanRestart` (197) | extern | `PgArchCanRestart` (383) | MATCH | function-static `last_pgarch_start_time`; `(unsigned)(curtime-last) < 10` via wrapping_sub as u32. |
| 4 | `PgArchiverMain` (217) | extern, noreturn | `PgArchiverMain`/`pg_archiver_main_inner` (404/419) | MATCH | signal installs, AuxiliaryProcessMainCommon, on_shmem_exit(pgarch_die), advertise pgprocno, alloc arch_files+heap, LoadArchiveLibrary, MainLoop, proc_exit(0). XLogArchivingActive Assert is debug-only (elided, acceptable). archive_context create elided — see F2. |
| 5 | `PgArchWakeup` (281) | extern | `PgArchWakeup` (493) | MATCH | reads pgprocno, SetLatch on allProcs[pgprocno].procLatch via proc seam; INVALID guard. |
| 6 | `pgarch_waken_stop` (297) | static (sig handler) | `pgarch_waken_stop` (505) | MATCH | sets ready_to_stop, SetLatch(MyLatch). |
| 7 | `pgarch_MainLoop` (310) | static | `pgarch_MainLoop` (522) | MATCH | do/while→loop; ResetLatch, ready_to_stop snapshot, ProcessPgArchInterrupts, SIGTERM 60s grace, copy loop, WaitLatch (WL flags + WAIT_EVENT_ARCHIVER_MAIN + 60*1000), WL_POSTMASTER_DEATH→stop. |
| 8 | `pgarch_ArchiverCopyLoop` (380) | static | `pgarch_ArchiverCopyLoop` (578) | **MISSING** | The `check_configured_cb` "not configured" block (C 421-433) is entirely absent. See F1. |
| 9 | `pgarch_archiveXlog` (516) | static | `pgarch_archiveXlog`/`archive_error_cleanup` (685/732) | PARTIAL | sigsetjmp→Result/Err catch; error-cleanup suite mirrored in C order. BUT success-path `MemoryContextReset(archive_context)` (C 609-610) + the switch around the callback are dropped. See F2. |
| 10 | `pgarch_readyXlog` (644) | static | `pgarch_readyXlog` (768) | MATCH | force_dir_scan exchange; cached-files revisit w/ stat (ENOENT skip, other→ERROR errcode_for_file_access); heap reset; AllocateDir/ReadDir walk; basenamelen=len-6, MIN/MAX_XFN∈[16,40], strspn(VALID_XFN_CHARS)≥len, ".ready" suffix; heap insert/evict/build; fill ascending; return highest. Max-heap semantics (root=lowest priority via comparator) verified vs PG binaryheap sift logic. |
| 11 | `ready_file_comparator` (780) | static | `ready_file_comparator` (905) | MATCH | IsTLHistoryFileName (8 upper-hex + ".history"), history wins (-1/1), else byte strcmp. |
| 12 | `PgArchForceDirScan` (803) | extern | `PgArchForceDirScan` (923) | MATCH | store(1, SeqCst) ≅ pg_atomic_write_membarrier_u32. |
| 13 | `pgarch_archiveDone` (817) | static | `pgarch_archiveDone` (932) | MATCH | StatusFilePath .ready/.done; rename; WARNING errcode_for_file_access on failure. |
| 14 | `pgarch_die` (846) | static | `pgarch_die` (951) | MATCH | pgprocno = INVALID_PROC_NUMBER. |
| 15 | `ProcessPgArchInterrupts` (860) | static | `ProcessPgArchInterrupts` (960) | MATCH | ProcSignalBarrierPending→ProcessProcSignalBarrier; LogMemoryContextPending→ProcessLogMemoryContextInterrupt; ConfigReloadPending: pstrdup lib, clear flag, ProcessConfigFile(PGC_SIGHUP), both-set ERROR(ERRCODE_INVALID_PARAMETER_VALUE), libChanged→LOG+proc_exit(0). |
| 16 | `LoadArchiveLibrary` (912) | static | `LoadArchiveLibrary` (1013) | MATCH | both-set ERROR; shell_archive_init vs load_external_function; NULL-init ERROR; archive_file_cb required ERROR; palloc0 state; startup_cb; before_shmem_exit(shutdown cb). |
| 17 | `pgarch_call_module_shutdown_cb` (954) | static | `pgarch_call_module_shutdown_cb` (1072) | MATCH | calls shutdown_cb if defined. |

Inlined header helpers (IsTLHistoryFileName, StatusFilePath, strspn/strcmp,
snprintf truncation) are reproduced in-crate and check out.

Globals: `XLogArchiveLibrary`/`XLogArchiveCommand` GUCs seamed to xlog.
`last_sigterm_time`, `ready_to_stop`, `arch_files`, `ArchiveCallbacks`,
`archive_module_state` modeled as per-backend thread-locals (single-thread
archiver) — appropriate. **`arch_module_check_errdetail_string` (C 96) is NOT
modeled** — a direct consequence of F1.

## Findings

### F1 (MISSING — merge blocker): `check_configured_cb` block dropped from `pgarch_ArchiverCopyLoop`

C 421-433 (in the build; confirmed in c2rust `pgarch.rs:1521-1549`):

```c
/* Reset variables that might be set by the callback */
arch_module_check_errdetail_string = NULL;

/* can't do anything if not configured ... */
if (ArchiveCallbacks->check_configured_cb != NULL &&
    !ArchiveCallbacks->check_configured_cb(archive_module_state))
{
    ereport(WARNING,
            (errmsg("\"archive_mode\" enabled, yet archiving is not configured"),
             arch_module_check_errdetail_string ?
             errdetail_internal("%s", arch_module_check_errdetail_string) : 0));
    return;
}
```

The Rust `pgarch_ArchiverCopyLoop` goes straight from `ProcessPgArchInterrupts()?`
(lib.rs ~602) to building `pathname` (~605). There is **no** `check_configured_cb`
invocation, no `arch_module_check_errdetail_string` reset, and no
`"archive_mode" enabled, yet archiving is not configured` WARNING/early-return.
`grep` for `check_configured`/`errdetail_string`/`arch_module` in the port
returns nothing.

Behavioral divergence: when an archive module reports it is not configured (e.g.
shell archiving with an empty `archive_command`), C emits the WARNING and returns
from the copy loop without attempting to archive; the port falls through and
calls `archive_file_cb` anyway. The `ArchiveModuleCallbacks` type in
`types-pgarch` *declares* `check_configured_cb`, so the callee exists — the
calling logic is simply absent. Absent logic = FAIL (SKILL step 4; no acceptable
deferral). The prior audit recorded this function as MATCH with the block
dismissed as "module-owned"; that was a false green.

### F2 (PARTIAL — merge blocker): archive_context lifecycle dropped from `pgarch_archiveXlog`

C runs the callback inside `archive_context`: `oldcontext =
MemoryContextSwitchTo(archive_context)` (531) and, on the **success** path,
`MemoryContextReset(archive_context)` (609-610) (the error path also resets it,
585). The port elides the create (in PgArchiverMain), the switch, and the
success-path reset, commenting that the lifecycle is "owned by mmgr." That is
inaccurate: `archive_context` is an archiver-private
`AllocSetContextCreate(TopMemoryContext, "archiver", ...)` and resetting it after
each file is real archiver logic (bounds per-file allocation across the copy
loop). No reset occurs on the success path. Logic simplified/approximated →
PARTIAL → FAIL.

## Seam audit (step 3 / 3b)

- **Owned seam crates:** the only `X-seams` mapping to `pgarch.c` is
  `crates/backend-postmaster-pgarch-seams` (sole decl `pg_archiver_main`). It is
  installed by `init_seams()` (lib.rs 1160:
  `backend_postmaster_pgarch_seams::pg_archiver_main::set(PgArchiverMain)`), and
  `crates/seams-init/src/lib.rs:35` calls `backend_postmaster_pgarch::init_seams()`.
  No uninstalled decls; no `set()` outside the owner. **OK.**
- **Outward seam calls** (auxprocess, ipc, latch, proc, pmsignal, procsignal,
  mcxt, guc-file, ps-status, pgsleep, xlog-guc, file/fd, timeout, lwlock,
  condvar, waitevent, aio-core, resowner, dynahash, pqsignal, shell-archive,
  dfmgr) are thin marshal+delegate; no branching/node-construction/computation
  inside a seam path. The error-cleanup suite and `report_archiver` are in-crate
  sequences of seam calls (callee logic lives in the owners), which is correct.
  **No seam findings.**
- **Design conformance (3b):** `PgArchData` is real shmem with atomic interior in
  `types-pgarch` (no shared-static-for-per-backend). No invented opacity —
  `ArchiveModuleState.private_data` is a genuine C `void *` extension slot,
  preserved. Allocating/erroring paths thread `PgResult`. No locks held across
  `?`. **No 3b findings.**

## Conclusion

**PASS.** F1 and F2 were fixed at root (see "Fix" below). The other 15 functions
were already MATCH and the seams/wiring + design conformance are clean.

## Fix (2026-06-12)

### F1 — `check_configured_cb` "not configured" path restored

`pgarch_ArchiverCopyLoop` now mirrors C 421-433 between `ProcessPgArchInterrupts()`
and the orphan-stat block: it resets `arch_module_check_errdetail_string` to
`None`, then, if `ArchiveCallbacks->check_configured_cb` is `Some`, calls it via
`with_module_state`; on a `false` result it `ereport(WARNING)`s `"archive_mode"
enabled, yet archiving is not configured`, attaches `errdetail_internal(detail)`
when the module set one, and `return`s from the copy loop (no archive attempt).
The errdetail global is modeled in-crate as a per-backend
`thread_local RefCell<Option<String>>` (`arch_module_check_errdetail_string`,
pgarch.c:96) with a public `set_arch_module_check_errdetail()` setter for the
`arch_module_check_errdetail()` macro (archive_module.h) that modules use.

### F2 — `archive_context` lifecycle restored

`archive_context` is now a per-backend `thread_local Cell<Option<MemoryContextHandle>>`.
`PgArchiverMain` creates it via the new `create_archiver_memcxt` mcxt seam
(`AllocSetContextCreate(TopMemoryContext, "archiver", ...)`). `pgarch_archiveXlog`
switches into it (`MemoryContextSwitchTo`) before `archive_file_cb` and, on the
**success** path, switches back and `MemoryContextReset(archive_context)` (C
609-610); the error path (`archive_error_cleanup`) switches back, `FlushErrorState`,
then `MemoryContextReset(archive_context)` (C 583-587), in C order. The
create/switch/reset are mmgr-owned (opaque `MemoryContextHandle`, same
DESIGN_DEBT shape as the logical-decoding context); the calling sequence is
in-crate archiver logic. New seam decls `create_archiver_memcxt` and
`MemoryContextReset` added to `backend-utils-mmgr-mcxt-seams` (mmgr owner
installs them when it lands).

Gate: `cargo check --workspace` + `cargo test --workspace` both clean.

## Re-confirmation (2026-06-13, independent from-scratch re-audit)

Re-derived the full 17-function inventory from `pgarch.c`, the c2rust
`pgarch.rs`, and the port (`crates/backend-postmaster-pgarch/src/lib.rs`) without
relying on the earlier audit or the port's comments. Both prior FAIL findings are
fixed at root and stay fixed:

- **F1 — `check_configured_cb` "not configured" path:** present in
  `pgarch_ArchiverCopyLoop` (lib.rs 626-655), in C order between
  `ProcessPgArchInterrupts()?` and the orphan-stat block: resets
  `arch_module_check_errdetail_string` to `None` (628), gates on
  `check_configured_cb` being `Some` and returning `false` (633-638), emits the
  `WARNING` `"archive_mode" enabled, yet archiving is not configured` with an
  `errdetail_internal` when the module set one (643-651), and `return`s without
  attempting to archive (653). Matches C 421-433 exactly.

- **F2 — `archive_context` lifecycle:** `archive_context` is a per-backend
  `Cell<Option<MemoryContextHandle>>` (124-125); `PgArchiverMain` creates it via
  `create_archiver_memcxt` (503-504, C 265-267); `pgarch_archiveXlog` switches
  into it before `archive_file_cb` (750), and on the **success** path switches
  back and `MemoryContextReset`s it (772-773, C 609-610), with the error path
  (`archive_error_cleanup`, 819-823) switching back, `FlushErrorState`, then
  `MemoryContextReset` in C order (C 581-585). No reset is dropped.

Spot-checked constants against headers: `MIN_XFN_CHARS=16`, `MAX_XFN_CHARS=40`,
`VALID_XFN_CHARS="0123456789ABCDEF.history.backup.partial"`
(`postmaster/pgarch.h` 25-27), `INVALID_PROC_NUMBER=-1` (`storage/procnumber.h`
26) — all match the port (lib.rs 75-77, 40). Re-verified the max-heap
sift_up/sift_down direction against `ready_file_comparator` (root = comparator
maximum = lowest archival priority, evicted when a higher-priority `.ready` file
arrives) — matches PG `binaryheap`.

Seams re-checked: sole owned seam crate `backend-postmaster-pgarch-seams` (one
decl `pg_archiver_main`) is installed by `init_seams()` (lib.rs 1229) and
`seams-init` calls it (lib.rs 35); no uninstalled/foreign `set()`. The F2 fix's
`create_archiver_memcxt` + `MemoryContextReset` decls live in
`backend-utils-mmgr-mcxt-seams` (mmgr-owned, not pgarch-owned), opaque
`MemoryContextHandle` of the same established DESIGN_DEBT shape as the
logical-decoding context handles — inherited opacity, not invented; the mmgr
owner installs them when it lands. Outward calls remain thin marshal+delegate.

Gate re-run: `cargo check -p backend-postmaster-pgarch -p
backend-utils-mmgr-mcxt-seams -p types-pgarch` clean; `cargo test -p
backend-postmaster-pgarch` = 10 passed, 0 failed. **Verdict stands: PASS.**
