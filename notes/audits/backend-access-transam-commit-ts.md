# Audit: backend-access-transam-commit-ts

- **Unit:** `backend-access-transam-commit-ts`
- **C source:** `src/backend/access/transam/commit_ts.c` (PostgreSQL 18.3)
- **c2rust oracle:** `../pgrust/c2rust-runs/backend-access-transam-commit-ts/src/commit_ts.rs`
- **Port:** `crates/backend-access-transam-commit-ts/src/lib.rs`
- **Branch:** `port/backend-access-transam-commit-ts`
- **Verdict:** PASS
- **Date:** 2026-06-13
- **Auditor model:** Claude Fable 5

## 1. Function inventory & verdicts

The completeness oracle (c2rust) defines 35 commit_ts.c functions (the remaining
c2rust `fn`s are c2rust-emitted inline accessor helpers — `ObjectIdGetDatum`,
`Int64GetDatum`, `TransactionIdToCTsPage`, etc. — folded into the Rust port as
inline helpers / direct ops, not separate public functions). Every one is
present in-crate. `commit_ts_desc`/`commit_ts_identify` live in `committsdesc.c`,
which CATALOG assigns to `backend-rmgrdesc-small` (confirmed) — correctly NOT in
this unit.

| # | C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | TransactionIdToCTsPage (72) | `TransactionIdToCTsPage` | MATCH | `xid / COMMIT_TS_XACTS_PER_PAGE`, i64 cast |
| 2 | TransactionIdToCTsEntry (macro 77) | `TransactionIdToCTsEntry` | MATCH | `xid % per_page` |
| 3 | TransactionTreeSetCommitTsData (141) | `TransactionTreeSetCommitTsData` | MATCH | active-flag no-op, newestXact pick, page-grouped loop, cached-value LWLock update, newestCommitTsXid advance — all faithful |
| 4 | SetXidCommitTsInPage (222) | `SetXidCommitTsInPage` | MATCH | bank-lock acquire, ReadPage(write=true), set head+subxids, page_dirty=true, release |
| 5 | TransactionIdSetCommitTs (249) | `TransactionIdSetCommitTs` | MATCH | entryno, packed 10-byte write into page_buffer; Assert→debug_assert |
| 6 | TransactionIdGetCommitTsData (274) | `TransactionIdGetCommitTsData` | MATCH | invalid→ERROR (ERRCODE_INVALID_PARAMETER_VALUE), non-normal→None, cached-value path, range check, SLRU read; `*ts != 0` → Option |
| 7 | GetLatestCommitTsData (360) | `GetLatestCommitTsData` | MATCH | LW_SHARED, disabled→error, returns (xid,ts,nodeid) |
| 8 | error_commit_ts_disabled (381) | `error_commit_ts_disabled` | MATCH | ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE; RecoveryInProgress()→primary-hint vs local-hint (via xlog-seams::recovery_in_progress) |
| 9 | pg_xact_commit_timestamp (397) | `pg_xact_commit_timestamp` | MATCH | found→ts else None(NULL) |
| 10 | pg_last_committed_xact (420) | `pg_last_committed_xact` | MATCH | non-normal xid→all-NULL row (None); fmgr/tupdesc glue lifts to typed return |
| 11 | pg_xact_commit_timestamp_origin (464) | `pg_xact_commit_timestamp_origin` | MATCH | not-found→all-NULL row (None) |
| 12 | CommitTsShmemBuffers (506) | `CommitTsShmemBuffers` | MATCH | 0→Autotune(512,1024) else Min(Max(16,n),SLRU_MAX_ALLOWED_BUFFERS) |
| 13 | CommitTsShmemSize (519) | `CommitTsShmemSize` | MATCH | SlruShmemSize(buffers,0)+sizeof(Shared) |
| 14 | CommitTsShmemInit (530) | `CommitTsShmemInit` | MATCH | autotune SetConfigOption DYNAMIC_DEFAULT→OVERRIDE fallback; SimpleLruInit + PagePrecedes + UnitTests; shared default init |
| 15 | check_commit_ts_buffers (584) | `check_commit_ts_buffers` | MATCH | delegates check_slru_buffers; (ok,detail) lifts GUC_check_errdetail out-param |
| 16 | BootStrapCommitTs (596) | `BootStrapCommitTs` | MATCH | empty body, faithful |
| 17 | ZeroCommitTsPage (615) | `ZeroCommitTsPage` | MATCH | SimpleLruZeroPage + optional WriteZeroPageXlogRec |
| 18 | StartupCommitTs (632) | `StartupCommitTs` | MATCH | → ActivateCommitTs |
| 19 | CompleteCommitTsInitialization (642) | `CompleteCommitTsInitialization` | MATCH | track_commit_timestamp ? Activate : Deactivate |
| 20 | CommitTsParameterChange (664) | `CommitTsParameterChange` | MATCH | newvalue/active toggling |
| 21 | ActivateCommitTs (705) | `ActivateCommitTs` | MATCH | bootstrap skip, already-active early-out, latest_page_number, oldest/newest seed, segment create, active=true |
| 22 | DeactivateCommitTs (785) | `DeactivateCommitTs` | MATCH | reset shared, clear oldest/newest, SlruScanDirectory(DeleteAll) under CommitTsLock |
| 23 | CheckPointCommitTs (827) | `CheckPointCommitTs` | MATCH | SimpleLruWriteAll(true) |
| 24 | ExtendCommitTs (849) | `ExtendCommitTs` | MATCH | Assert(!InRecovery), active no-op, first-XID-of-page (incl FirstNormalTransactionId), ZeroCommitTsPage(!InRecovery) |
| 25 | TruncateCommitTs (890) | `TruncateCommitTs` | MATCH | cutoffPage, ReportPresence gate, WriteTruncateXlogRec, SimpleLruTruncate |
| 26 | SetCommitTsLimit (916) | `SetCommitTsLimit` | MATCH | future-protection oldest/newest update; else-branch seeds both |
| 27 | AdvanceOldestCommitTsXid (943) | `AdvanceOldestCommitTsXid` | MATCH | conditional oldest advance |
| 28 | CommitTsPagePrecedes (977) | `CommitTsPagePrecedes` | MATCH | xid1/xid2 = page*per_page + FirstNormal+1; wrapping arith; two TransactionIdPrecedes |
| 29 | WriteZeroPageXlogRec (996) | `WriteZeroPageXlogRec` | MATCH | XLogRegisterData(&pageno,8) + XLogInsert(RM_COMMIT_TS_ID, ZEROPAGE) via xloginsert-seams |
| 30 | WriteTruncateXlogRec (1007) | `WriteTruncateXlogRec` | MATCH | 12-byte xl_commit_ts_truncate (pageno8+oldestXid4, no struct pad) + XLogInsert TRUNCATE |
| 31 | commit_ts_redo (1023) | `commit_ts_redo` | MATCH | ZEROPAGE / TRUNCATE branches; AdvanceOldest + latest_page_number override + Truncate; unknown→PANIC; Assert(!HasAnyBlockRefs)→`max_block_id() < 0` |
| 32 | committssyncfiletag (1070) | `committssyncfiletag` | MATCH | SlruSyncFileTag, errno captured into FileTagOpResult |

(Static-helper / inline functions 1-2 above are the c2rust inline accessors;
all remaining 33 named functions accounted for. Total = 35 oracle functions.)

## 2. Constants verified against headers

- `RM_COMMIT_TS_ID = 18` — rmgrlist.h, 0-based index of `PG_RMGR(RM_COMMIT_TS_ID,...)` (XLOG=0 … COMMIT_TS=18). MATCH.
- `COMMIT_TS_ZEROPAGE = 0x00`, `COMMIT_TS_TRUNCATE = 0x10` — commit_ts.h lines 46-47. MATCH.
- `CommitTsLock` offset `39` — lwlocklist.h `PG_LWLOCK(39, CommitTs)`. MATCH.
- `SizeOfCommitTimestampEntry = 10` (TimestampTz 8 + RepOriginId 2, no pad) — commit_ts.c. MATCH.
- `SizeOfCommitTsTruncate = 12` (offsetof(oldestXid)=8 + sizeof(TransactionId)=4) — commit_ts.h. MATCH (port uses 12-byte buffer).
- `DT_NOBEGIN = i64::MIN` (TIMESTAMP_NOBEGIN). MATCH.

## 3. Seam & wiring audit

Owned inward seam crate: `backend-access-transam-commit-ts-seams` — 6
declarations, **all 6 installed** by this crate's `init_seams()`:
`commit_ts_redo`, `transaction_tree_set_commit_ts_data`,
`transaction_id_get_commit_ts_data`, `committssyncfiletag`,
`commit_ts_shmem_size`, `commit_ts_shmem_init`. `init_seams()` is wired into
`seams-init::init_all()`. (The `backend-access-rmgrdesc-committsdesc-seams`
crate is owned by `backend-rmgrdesc-small`, not this unit — correctly not
installed here.) Each installed seam is thin marshal+delegate over
`with_commit_ts_state`; no branching/computation in seam bodies. The
`check_commit_ts_buffers` GUC hook install marshals the `(ok, detail)` tuple
back to the C `GUC_check_errdetail`/`bool` contract.

Outward seams are all justified cross-crate edges (varsup TransamVariables,
xloginsert WAL, guc SetConfigOption, miscinit bootstrap-mode, xlog
RecoveryInProgress, xlogrecovery InRecovery, init-small my_proc_number, SLRU
consumed directly). Seam signatures mirror the C failure surface (`PgResult`
where the C can `ereport(ERROR)`); no invented opacity; per-backend/shared state
modeled as `OnceLock<Mutex<CommitTsState>>` reached via `with_commit_ts_state`
(not a raw shared static for per-backend globals).

## 4. Design conformance

No invented handles; `CommitTsState` owns the real `SlruCtlData` +
`CommitTimestampShared` structs. Allocating entrypoint `CommitTsShmemInit`
returns `PgResult`. `CommitTsLock` is the real fixed MainLWLockArray lock (bare
acquire/release, C-faithful SLRU discipline). No registry-shaped side tables.

## 5. Gate

- `cargo check --workspace` — clean (only unrelated `printtup` warnings).
- `cargo test --workspace` — exit 0, **0 failures** (no timeout flakes hit this run).
- `recurrence_guard` (`seams-init`) — PASS after fix below.

### Fix applied during audit (one round)

The `recurrence_guard` initially flagged 2 declared-but-uninstalled seams that
this unit `::call`s with **fully-qualified** paths (where prior callers used
aliased imports the guard's textual matcher could not attribute, so the debt was
hidden, not absent):

- `backend-access-transam-xlog :: recovery_in_progress` — owner body is an
  `xlog_driver_deferred!` panic-stub (the deferred xlog-driver / `XLogCtl`
  shmem `SharedRecoveryState`), same class as the already-allowlisted
  `startup_xlog`/`xlog_shmem_init` entries.
- `backend-utils-misc-guc-file :: guc_check_errdetail` — `GUC_check_errdetail()`
  is a guc.c check-hook global writer, mis-homed (not bodied in the guc-file
  crate); already called by `namespace`/`user`.

Neither is owned by commit_ts, and `mirror-pg-and-panic` forbids altering those
ported/audited bodies to force a `::set`. Per the guard's own remediation, added
both to `CONTRACT_RECONCILE_PENDING` with matching `DESIGN_DEBT.md` lines.
This is pre-existing, named debt on other owners — verified the guard PASSES on
`main` HEAD (those owners' seams are reached only via aliases there) and that the
call sites pre-date this crate.
