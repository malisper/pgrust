# Audit: backend-access-transam-varsup

- Date: 2026-06-13
- Model: Claude Fable 5 (Opus 4.8 [1m])
- Verdict: **PASS**

C source: `src/backend/access/transam/varsup.c` (716 lines, PG 18.3).
Port: `crates/backend-access-transam-varsup/src/lib.rs`.
Owned seam crate: `crates/backend-access-transam-varsup-seams` (varsup.c is the
unit's only `c_source`, so this is the sole owned seam crate; ownership by
C-source coverage).

Independent re-derivation from the C, the c2rust rendering
(`c2rust-runs/backend-access-transam-varsup/src/varsup.rs`), and the Rust port.
Did not trust the port's comments, the prior audit, or the green build.

## Function inventory

Completeness oracle = the c2rust run. It contains 10 `extern "C"` functions
plus the static `SetNextObjectId` (post-preprocessor). Two functions exist only
in the original C outside the build config: `c2rust_pg_write_barrier` (transpile
shim) and `AssertTransactionIdInAllowableRange` (`#ifdef USE_ASSERT_CHECKING`).

| C function (loc) | Port (fn) | Verdict | Notes |
|---|---|---|---|
| `c2rust_pg_write_barrier` (31) | — | N/A | transpile no-op shim; the real `pg_write_barrier()` between subxid array fill and count bump is proc-owned logic behind `store_subxid_in_proc` |
| `VarsupShmemSize` (51) | `VarsupShmemSize` | MATCH | `size_of::<TransamVariablesData>()` |
| `VarsupShmemInit` (57) | `VarsupShmemInit` | MATCH | `!IsUnderPostmaster` → zero the singleton (default); attaching backend leaves as-found. ShmemInitStruct/found Asserts subsumed by the const-zeroed `Mutex` singleton model |
| `GetNewTransactionId` (87) | `GetNewTransactionId` | MATCH | parallel-mode ERROR, bootstrap fast path (store top xid + return epoch0 bootstrap fxid), recovery ERROR, XidGenLock excl, vac-limit branch with lock-release/copy-shared/65536 autovac signal/stop-ERROR (PROGRAM_LIMIT_EXCEEDED + hint, both datname/OID arms)/warn-WARNING (both arms, `xidWrapLimit - xid`)/re-acquire+reload, ExtendCLOG/CommitTs/SUBTRANS, FullTransactionIdAdvance, proc publication (top vs subxid via seam), release. Subxid cache/overflow + write-barrier correctly SEAMED to proc (touches MyProc->subxids / ProcGlobal->subxidStates) |
| `ReadNextFullTransactionId` (298) | `ReadNextFullTransactionId` | MATCH | XidGenLock SHARED read; infallible C signature → LWLock-overflow surfaced as panic |
| `AdvanceNextFullTransactionIdPastXid` (314) | `AdvanceNextFullTransactionIdPastXid` | MATCH | startup/standalone assert, lock-free fast return, TransactionIdAdvance, epoch++ on `xid < next_xid` (`unlikely`), excl write under lock |
| `AdvanceOldestClogXid` (365) | `AdvanceOldestClogXid` | MATCH | XactTruncationLock excl; advance only on TransactionIdPrecedes |
| `SetTransactionIdLimit` (382) | `SetTransactionIdLimit` | MATCH | wrap=`oldest + (MaxTransactionId>>1)` (+= FirstNormal if <), stop=`wrap-3000000` (**-= FirstNormal**), warn=`wrap-40000000` (**-= FirstNormal**), vac=`oldest + autovacuum_freeze_max_age` (+= FirstNormal); excl write of all 6 limits + read curXid; DEBUG1 internal log; autovac signal on (vac & UnderPostmaster & !InRecovery); warn-WARNING gated by !InRecovery with datname only when IsTransactionState (both arms). Sign of the FirstNormal adjustment per-limit matches C exactly |
| `ForceTransactionIdLimitUpdate` (527) | `ForceTransactionIdLimitUpdate` | MATCH | SHARED read of 4 fields; 4 early `true`s; final `!SearchSysCacheExists1(DATABASEOID,...)` → `database_datdba(...).is_none()` |
| `GetNewObjectId` (565) | `GetNewObjectId` | MATCH | recovery ERROR; OidGenLock excl; `<FirstNormalObjectId` wraparound with postmaster vs standalone(`<FirstGenbkiObjectId`) nesting; prefetch WAL on `oidCount==0` (`nextOid + VAR_OID_PREFETCH=8192`, oidCount=8192); result=nextOid; nextOid++/oidCount-- (wrapping). WAL insert lifted out of data-`Mutex` but LWLock (real serializer) still held — behaviorally identical |
| `SetNextObjectId` (633, static) | `SetNextObjectId` | MATCH | postmaster ERROR; OidGenLock excl; `nextOid > target` → ERROR `too late to advance OID counter to %u, it is now %u`; set nextOid + oidCount=0 |
| `StopGeneratingPinnedObjectIds` (662) | `StopGeneratingPinnedObjectIds` | MATCH | `SetNextObjectId(FirstUnpinnedObjectId)` |
| `AssertTransactionIdInAllowableRange` (683, USE_ASSERT_CHECKING) | `AssertTransactionIdInAllowableRange` | MATCH | gated on `cfg!(debug_assertions)` (mirrors C macro `((void)true)` in non-assert builds); valid assert, normal early-return, memory-barrier-ordered read, `Follows||PrecedesOrEquals` assert |
| `XLogRedoNextOid` (xlog.c redo arm) | `XLogRedoNextOid` | MATCH (extra) | not in varsup.c; the XLOG_NEXTOID redo body, correctly homed here since it mutates this crate's owned TransamVariables under OidGenLock |

### transam.h arithmetic helpers (in-crate, per CATALOG note)

`XidFromFullTransactionId`, `EpochFromFullTransactionId`,
`FullTransactionIdFromEpochAndXid`, `FullTransactionIdPrecedes`,
`TransactionIdAdvance`, `FullTransactionIdAdvance`, `TransactionIdIsValid`,
`TransactionIdIsNormal`, `TransactionIdPrecedes`, `TransactionIdPrecedesOrEquals`,
`TransactionIdFollowsOrEquals` — all MATCH the transam.h macro/inline semantics
(wrapping i32 diff comparisons; FullTransactionIdAdvance skips special XIDs when
viewed as 32-bit). `am_startup_process` = `MyBackendType == B_STARTUP`.

## Seam audit

Owned seam crate `backend-access-transam-varsup-seams` declares 9 seams; all 9
are installed by `init_seams()`, which contains only `set()` calls:
`read_next_full_transaction_id`, `read_next_transaction_id`,
`get_new_transaction_id`, `advance_next_full_transaction_id_past_xid`,
`advance_next_full_xid_past_xid`, `varsup_shmem_size`, `varsup_shmem_init`,
`get_new_object_id`, `stop_generating_pinned_object_ids`. No uninstalled
declaration; no `set()` on these seams outside the owner (grep-verified).
`seams-init::init_all()` calls `backend_access_transam_varsup::init_seams()`.

Outward seam calls all justified by real cross-unit dependencies and are thin
marshal+delegate (no branching/computation in seam paths):
clog/commit_ts/subtrans extend (SLRU), xlog (recovery_in_progress/in_recovery/
xlog_put_next_oid), proc (store_top_xid/store_subxid — proc-array publication),
pmsignal (autovac signal), dbcommands (get_database_name), syscache
(database_datdba for DATABASEOID existence), miscinit (is_bootstrap_processing_mode),
init-small (my_proc_number/is_under_postmaster/is_postmaster_environment/
my_backend_type), xact (is_in_parallel_mode/is_transaction_state).

## Design conformance

- Allocating path (`get_database_name`) carries `Mcx` and returns `PgResult` —
  conforms. The infallible C seams (Read/Advance/Shmem) are panic/`Ok`-wrapped
  per the should-never-happen LWLock-overflow rationale, not stubbed.
- `TransamVariables` is process-shared `Mutex<TransamVariablesData>`; genuine
  cross-backend serialization is the real LWLock via lwlock RAII guards — not a
  shared static standing in for per-backend global, not an ambient-global seam.
- ERROR → `Err(PgError)` with matching SQLSTATE (PROGRAM_LIMIT_EXCEEDED on the
  wraparound stop path); WARNING/DEBUG1 via backend-utils-error. No invented
  opacity, no registry side-table, no unledgered divergence marker.

## Gate

- `cargo check --workspace`: clean (warnings only, none in this unit).
- `cargo test --workspace`: pass; no FAILED. recurrence_guard
  (`every_declared_seam_is_installed_by_its_owner`,
  `every_seam_installing_crate_is_wired_into_init_all`) pass.

## Verdict: PASS

Every function MATCH or SEAMED-per-rules; zero seam findings; zero design
findings. CATALOG status set to `audited`.
