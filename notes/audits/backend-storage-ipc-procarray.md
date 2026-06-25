# Audit: backend-storage-ipc-procarray

C source: `src/backend/storage/ipc/procarray.c`
Branch: `decomp/backend-storage-ipc-procarray` (keystone scaffold + F0 fill + main pre-sync)
Verdict: **FAIL (full crate)** — NEEDS_DECOMP, only the F0 keystone family is filled.
Audited: 2026-06-13 (Claude Fable 5 / Opus 4.8 1M)

## Situation

procarray.c is a 6-family NEEDS_DECOMP decomposition. The keystone scaffold
(`936dba33`) created six family modules, each panicking
`panic!("decomp: <fn> not yet filled")` until its fill stage lands. Of the six
families, **only F0 was provided as a PASS family branch
(`decomp/backend-storage-ipc-procarray-f0`)** and merged. The other five
families (F1–F5) remain scaffold own-logic stubs.

These remaining `panic!("...not yet filled")` are **own-logic stubs**, not
genuine cross-unit/cross-subsystem seam-and-panic: each one is the real
procarray algorithm (slot add/remove, GetSnapshotData, ComputeXidHorizons, the
visibility/lookup/count helpers, the KnownAssignedXids ring) that must live in
this crate. Per audit-crate §4 these are MISSING and therefore the crate cannot
be `audited`. The crate stays `needs-decomp` with the residual families noted.

## F0 — shmem_model (FILLED, clean)

The keystone family is fully filled — zero `panic!`/`todo!`/`unimplemented!`
remain in `shmem_model.rs`.

| C fn | Port | Verdict |
|------|------|---------|
| `ProcArrayShmemSize` | shmem_model::ProcArrayShmemSize | MATCH |
| `ProcArrayShmemInit` | shmem_model::ProcArrayShmemInit | MATCH |
| `GetMaxSnapshotXidCount` | shmem_model::GetMaxSnapshotXidCount | MATCH |
| `GetMaxSnapshotSubxidCount` | shmem_model::GetMaxSnapshotSubxidCount | MATCH |
| `FullXidRelativeTo` (static) | shmem_model::FullXidRelativeTo | MATCH |
| `TransactionIdOlder` / FullTransactionId arithmetic helpers | shmem_model | MATCH |
| `PROCARRAY_MAXPROCS` / `TOTAL_MAX_CACHED_SUBXIDS` macros | shmem_model | MATCH |

The real `ProcArrayStruct`, the dense per-slot mirror, the `GlobalVisState`
struct, and the file-static process-locals are modeled here. F0 also extended
`backend-access-transam-varsup-seams` / `backend-access-transam-varsup` (the
varsup helpers F0 reaches) — those land via the F0 branch.

## F1–F5 — residual families (MISSING / own-logic stubs, NOT seams)

All of the following are `panic!("decomp: X not yet filled")` own-logic stubs.
Their `init_seams()` installers `set()` the (panicking) impls, so the
`seams-init` recurrence guards pass and the workspace compiles; the logic itself
is absent.

- **F1 membership.rs (8):** ProcArrayAdd, ProcArrayRemove, ProcArrayEndTransaction,
  ProcArrayEndTransactionInternal, ProcArrayClearTransaction,
  MaintainLatestCompletedXid, MaintainLatestCompletedXidRecovery,
  ProcArrayGroupClearXid.
- **F2 snapshot.rs (15):** GetSnapshotData, GetSnapshotDataReuse,
  ProcArrayInstallImportedXmin, ProcArrayInstallRestoredXmin,
  GetRunningTransactionData, GetOldestActiveTransactionId,
  GetOldestSafeDecodingTransactionId, ProcArraySetReplicationSlotXmin,
  ProcArrayGetReplicationSlotXmin, GetReplicationHorizons,
  ProcArrayLockAcquireExclusive, ProcArrayLockRelease, MarkProcInLogicalDecoding,
  ProcArrayClearLogicalDecodingFlag, GetConflictingVirtualXIDs.
- **F3 horizons.rs (12):** ComputeXidHorizons, GlobalVisHorizonKindForRel,
  GetOldestNonRemovableTransactionId, GetOldestTransactionIdConsideredRunning,
  GlobalVisTestFor, GlobalVisTestShouldUpdate, GlobalVisUpdate,
  GlobalVisUpdateApply, GlobalVisTestIsRemovableFullXid,
  GlobalVisTestIsRemovableXid, GlobalVisCheckRemovableFullXid,
  GlobalVisCheckRemovableXid.
- **F4 visibility_lookup.rs (22):** TransactionIdIsInProgress,
  TransactionIdIsActive, ProcNumberGetProcPid, ProcStatus,
  ProcNumberGetTransactionIds, BackendPidGetProcRole, BackendPidGetProcWithLock,
  BackendXidGetPid, IsBackendPid, GetCurrentVirtualXIDs,
  GetVirtualXIDsDelayingChkpt, HaveVirtualXIDsDelayingChkpt,
  CancelVirtualTransaction, SignalVirtualTransaction, MinimumActiveBackends,
  CountDBBackends, CountDBConnections, CancelDBBackends, CountUserBackends,
  CountOtherDBBackends, TerminateOtherDBBackends, XidCacheRemoveRunningXids.
- **F5 knownassignedxids.rs (21):** KnownAssignedXidsCompress,
  KnownAssignedXidsAdd, KnownAssignedXidsSearch, KnownAssignedXidExists,
  KnownAssignedXidsRemove, KnownAssignedXidsRemoveTree,
  KnownAssignedXidsRemovePreceding, KnownAssignedXidsGet,
  KnownAssignedXidsGetAndSetXmin, KnownAssignedXidsGetOldestXmin,
  KnownAssignedXidsDisplay, KnownAssignedXidsReset,
  KnownAssignedXidsIdleMaintenance, KnownAssignedTransactionIdsIdleMaintenance,
  RecordKnownAssignedTransactionIds, ExpireTreeKnownAssignedTransactionIds,
  ExpireAllKnownAssignedTransactionIds, ExpireOldKnownAssignedTransactionIds,
  ProcArrayApplyRecoveryInfo, ProcArrayInitRecovery, ProcArrayApplyXidAssignment.

Total residual own-logic stubs: **78** across 5 families.

## Seam audit

The inward seam crate `backend-storage-ipc-procarray-seams` (80 decls) is fully
installed: each family `init_seams()` `set()`s its owned seams, `lib::init_seams()`
calls all six, and `seams-init::init_all()` wires the unit. Both recurrence
guards (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) pass. No duplicate `pub fn`
(E0428) in the seam crate. This wiring is correct; the failure is purely the
absent F1–F5 logic, not seam wiring.

## Gate

- `cargo check --workspace`: PASS (warnings only)
- `cargo test -p backend-storage-ipc-procarray`: PASS (0 tests)
- `cargo test -p seams-init`: PASS (both recurrence guards)
- Pre-sync merge of `refs/heads/main` (9bd4a852): clean, no conflicts.

## Verdict

**Full crate FAIL → CATALOG status `needs-decomp`.** F0 (shmem_model) is audited
clean and merged. Residual families to fill: F1 membership, F2 snapshot,
F3 horizons, F4 visibility_lookup, F5 knownassignedxids (78 own-logic stubs).
No false seam-and-panic to convert; the stubs are genuinely this crate's own
algorithms.
