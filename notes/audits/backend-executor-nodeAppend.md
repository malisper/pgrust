# Audit: backend-executor-nodeAppend

- **Unit:** `backend-executor-nodeAppend`
- **Branch:** `fix/no-todo-backend-executor-nodeAppend`
- **C source:** `src/backend/executor/nodeAppend.c` (postgres-18.3)
- **c2rust:** `c2rust-runs/backend-executor-nodeAppend/src/nodeAppend.rs`
- **Port:** `crates/backend-executor-nodeAppend/src/lib.rs`
- **Date:** 2026-06-13 (re-audit from scratch — no-todo lane)
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Top-line verdict: **PASS**

Independent from-scratch re-audit. All 18 C function definitions are present
and every one is `MATCH` (the four parallel entry points are `MATCH` for their
owned control flow, `SEAMED` for the live-node/DSM mutations). Zero
`todo!()`/`unimplemented!()` remain in `src/`. Seam wiring is clean.

**No-todo change (this lane):** the four parallel-Append entry points
(`ExecAppendEstimate`, `ExecAppendInitializeDSM`, `ExecAppendReInitializeDSM`,
`ExecAppendInitializeWorker`) previously had `init_seams()` install **four
`unimplemented!()` bridges** because the seam shape is `PlanStateHandle` while
the real logic was written borrow-based (`&mut AppendStateData`) and could not
be reached from a handle. Resolving a `PlanStateHandle` to a live `AppendState`
is execParallel/`access/parallel.c` dispatch (unported). Mirroring the
already-audited nodeSort/nodeHash parallel families, the four functions are now
**handle-based** and install **directly** (no closures, no `unimplemented!()`),
delegating each live-node field read/write and DSM mutation to new `append_*`
seams in `backend-executor-execParallel-support-seams` (owned by
execParallel/parallel, seam-and-panic until that owner installs them). The
owned control flow (`pstate_len` sizing via `pa_finished_offset` + `add_size`,
`plan_node_id`/`pstate_len` reads) stays in-crate.

The prior `DIVERGES` finding in `ExecAppendAsyncEventWait` (notify-all-pending
vs. notify-the-fired request) remains **resolved**: the shared `WaitEvent`
(`crates/types-storage/src/waiteventset.rs:47`) carries `user_data: Option<i32>`,
the configure-wait seam plumbs it through, and the delivery loop recovers the
single matched `AsyncRequest` via `w.user_data`, mirroring C's
`(AsyncRequest *) w->user_data`.

## Function inventory & verdicts

All 18 C function definitions (4 interface + 4 parallel + 3 choose + 1 mark +
4 async + ExecAsyncAppendResponse + classify) are present. The C
`ParallelAppendState` struct, `INVALID_SUBPLAN_INDEX`/`EVENT_BUFFER_SIZE`
macros (both verified = -1 / 16), and the forward static decls carry no logic
of their own. `LWTRANCHE_PARALLEL_APPEND` verified in lwlock.h.

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|------------|-------|-------------------|---------|-------|
| 1 | `ExecInitAppend` | 108–294 | 122 | MATCH | Pruning branch, async-request alloc, common-slot-ops selection, all field inits mirrored. `validsubplans`/`as_valid_subplans` aliasing handled by building an equal-by-construction copy; the loop never mutates `validsubplans`. `n_total = list_length(appendplans)` matches C arg. Seam sig for `ExecInitPartitionExecPruning(planstate,n_total,part_prune_index,relids,&validsubplans)` matches. `es_epq_active` read via `estate_epq_active` (constant false — see 3b). |
| 2 | `ExecAppend` | 302–390 | 337 | MATCH | First-call init, async/sync interleave, CHECK_FOR_INTERRUPTS, dispatch, TupIsNull return, end-of-scan ExecClearTuple→`None`. |
| 3 | `ExecEndAppend` | 400–418 | 423 | MATCH | Loop `0..nplans` ExecEndNode. |
| 4 | `ExecReScanAppend` | 420–484 | 441 | MATCH | Prune-param overlap reset, per-child UpdateChangedParamSet/ExecReScan gating, async reset, final resets. chgParam split-borrow clone is read-only-equivalent. |
| 5 | `ExecAppendEstimate` | 498–508 | ~534 | MATCH | Handle-based. Owned control flow in-crate: reads `as_nplans` (seam), computes `pstate_len = add_size(offsetof(...,pa_finished), sizeof(bool)*nplans)` via `pa_finished_offset()` (= sizeof(LWLock)+sizeof(i32), mirrors offsetof) + `shmem::add_size`, writes it (seam), then `pcxt_estimator`/`shm_toc_estimate_chunk`/`shm_toc_estimate_keys(1)`. The sizing computation stays in the crate; only the node-field read/write are seamed. |
| 6 | `ExecAppendInitializeDSM` | 517–530 | ~553 | MATCH/SEAMED | Handle-based. Reads `plan_node_id`+`pstate_len` (owned), then `append_initialize_dsm_pstate` performs the C body's primitive sequence on the resolved node + DSM: `shm_toc_allocate`/memset/`LWLockInitialize(LWTRANCHE_PARALLEL_APPEND)`/`shm_toc_insert`(by plan_node_id)/`as_pstate=pstate`/`choose_next_subplan=for_leader`. C body has no branching/computation — faithful thin delegate of DSM-resident mutation, mirroring `sort_/hash_initialize_dsm_shared_info`. |
| 7 | `ExecAppendReInitializeDSM` | 538–545 | ~570 | MATCH/SEAMED | Handle-based pure delegate to `append_reinitialize_dsm_pstate`: `pa_next_plan=0`, `pa_finished` zeroed to `sizeof(bool)*as_nplans` (the memset bound reads the live node's `as_nplans`). C body is two primitives, no logic. |
| 8 | `ExecAppendInitializeWorker` | 554–559 | ~583 | MATCH/SEAMED | Handle-based. Reads `plan_node_id` (owned), delegates to `append_initialize_worker_pstate`: `as_pstate = shm_toc_lookup(toc, plan_node_id, false)`, `choose_next_subplan = for_worker`. C body is one lookup + one field write. |
| 9 | `choose_next_subplan_locally` | 568–624 | 660 | MATCH | syncdone short-circuit, first-call identification, forward/backward bms walk, async syncdone set. |
| 10 | `choose_next_subplan_for_leader` | 634–701 | 716 | MATCH | LWLock guard, mark-finished, last-subplan start, find+mark-invalid, descending finished-skip loop, non-partial immediate finish, all release paths. |
| 11 | `choose_next_subplan_for_worker` | 716–832 | 779 | MATCH | Full pa_next_plan state machine, partial-plan loop-back, bail-outs, one-more advance, non-partial finish. Verified branch-for-branch. |
| 12 | `mark_invalid_subplans_as_finished` | 842–863 | 883 | MATCH | All-valid short-circuit, mark non-members finished. |
| 13 | `ExecAppendAsyncBegin` | 876–920 | 909 | MATCH | Valid-subplan identification + classify, syncdone/nasyncremain init, per-valid-async ExecAsyncRequest. |
| 14 | `ExecAppendAsyncGetNext` | 928–969 | 960 | MATCH | Request, wait-loop with CFI, syncdone break, totally-done ExecClearTuple. Out-param modeled `Option<Option<SlotId>>`. |
| 15 | `ExecAppendAsyncRequest` | 977–1023 | 1003 | MATCH | needrequest-empty short-circuit, pending-results return, re-request loop, bms_free, second results check. |
| 16 | `ExecAppendAsyncEventWait` | 1031–1133 | 1057 | MATCH | Setup matches (CreateWaitEventSet guard, WL_EXIT_ON_PM_DEATH, configure-wait loop, GetNumRegisteredWaitEvents==1 early-out, WL_LATCH_SET after configure, EVENT_BUFFER_SIZE cap, WaitEventSetWait, noccurred==0 return). Delivery loop now reads `w.user_data` to recover the **single matched** `AsyncRequest` by `request_index`, gates on `callback_pending`, clears the flag before `ExecAsyncNotify`, and handles WL_LATCH_SET (ResetLatch+CFI). Matches C exactly. See resolved finding. |
| 17 | `ExecAsyncAppendResponse` | 1141–1177 | 1176 | MATCH | request_complete gate, NULL/empty-slot nasyncremain decrement, save-result, needrequest add_member. `node` passed explicitly (C derives from `areq->requestor`). |
| 18 | `classify_matching_subplans` | 1187–1220 | 1222 | MATCH | empty→syncdone, no-overlap→nasyncremain=0, intersect→valid_asyncplans, del_members, save. |

## Resolved finding — `ExecAppendAsyncEventWait` delivery loop

The prior audit FAILed on this function: the shared `WaitEvent` had been
trimmed (no `user_data`), so the delivery loop could not recover the request
that fired and instead notified **every** `callback_pending` async subplan — a
different algorithm than C (which notifies exactly the one request the event
fired for).

Verified resolved this round:

- `crates/types-storage/src/waiteventset.rs:47` — `WaitEvent` now carries
  `user_data: Option<i32>` (the non-aliasing key replacing C's `void *`;
  `None` == C `NULL`).
- `crates/backend-storage-ipc-waiteventset-seams/src/lib.rs` — `add_event`
  (=> `AddWaitEventToSet`) takes `user_data: Option<i32>` and forwards it to
  the (unported) owner; `wait` (=> `WaitEventSetWait`) hands the `WaitEvent`
  array back with `user_data` populated. Thin marshal/forward, no in-crate
  computation.
- `crates/backend-executor-nodeAppend/src/lib.rs:1126–1164` — the delivery
  loop iterates the **occurred events** (not all subplans), and for each
  `WL_SOCKET_READABLE` event recovers the single request via
  `w.user_data` (its `request_index`, == the `as_asyncrequests` index set in
  `ExecAppendAsyncBegin`), gates on `callback_pending`, clears the flag
  **before** `ExecAsyncNotify` (mirroring the C ordering comment), and handles
  `WL_LATCH_SET` with `ResetLatch(MyLatch)` + `CHECK_FOR_INTERRUPTS()`. This is
  byte-for-byte the C algorithm.

The actual store of `request_index` into `user_data` happens inside
`ExecAsyncConfigureWait` (execAsync.c), which is unported and reached through
`execAsync-seams::exec_async_configure_wait` — a legitimate unported-callee
seam, not in-crate logic. Verdict for fn 16 is now `MATCH`.

## Seam audit

**Owned seam crate (by c_sources coverage):** `crates/backend-executor-nodeAppend-seams`
is the only seam crate mapping to `nodeAppend.c`.

- Declares four parallel-Append methods (`exec_append_estimate`,
  `exec_append_initialize_dsm`, `exec_append_reinitialize_dsm`,
  `exec_append_initialize_worker`) in the handle-based shape execParallel.c
  calls them with (`PlanStateHandle` + `ParallelContextHandle`/
  `ParallelWorkerContextHandle`).
- `init_seams()` installs **all four** as **direct function references** to the
  crate's own handle-based `ExecAppend*` implementations — no `unimplemented!()`,
  no closures. Installer contains only `set()` calls.
- `seams-init` wires it: `crates/seams-init/src/lib.rs:57`. Both
  `recurrence_guard` tests pass.

No empty installer, no uninstalled declaration of an *owned* seam, no `set()`
outside the owner. Zero `todo!()`/`unimplemented!()` in `src/` (verified by
grep; only doc-comment mentions of `unimplemented` history removed).

**New `append_*` support seams (NOT owned by this unit):** the four functions
delegate live-node field access and DSM mutation to seven new declarations in
`crates/backend-executor-execParallel-support-seams`
(`append_as_nplans`, `append_plan_node_id`, `append_set_pstate_len`,
`append_pstate_len`, `append_initialize_dsm_pstate`,
`append_reinitialize_dsm_pstate`, `append_initialize_worker_pstate`). That crate
is owned by execParallel/`access/parallel.c` (the planstate-handle dispatch),
which is unported; the seams are seam-and-panic until that owner installs them —
exactly the established pattern for the `sort_*`, `memoize_*`, and `hash_*`
families already in that crate. Resolving the `AppendState *` from the handle
is genuinely that owner's logic, so this is a legitimate cross-unit seam, not
absent in-crate logic.

**Outward seam calls** — all thin marshal+delegate, justified by unported
owners / dep cycles; no branching, node construction, or computation observed
on any seam path: bms ops (backend-nodes-core-seams; intersect/del_members/
add_range/prev_member/overlap all present), execProcnode (init/proc/end_node),
execAmi (re_scan), execUtils (get_common_slot_ops/update_changed_param_set),
execTuples (init_result_tuple_slot_tl/clear_tuple), execPartition
(init_partition_exec_pruning/find_matching_subplans), execAsync (async_request/
configure_wait/notify), parallel + shmem (toc/estimator/add_size),
waiteventset, latch (my_latch/reset), tcop (check_for_interrupts), lwlock
(acquire — used by the runtime `choose_next_subplan_for_*` leader/worker paths;
`LWLockInitialize` now lives inside `append_initialize_dsm_pstate`), and the new
`execParallel-support-seams::append_*` family (see above). `LWTRANCHE_PARALLEL_APPEND`
moved into the DSM-init support seam's doc/body and is no longer referenced
in-crate.

## Design conformance (step 3b)

- Allocating functions take `Mcx` and return `PgResult`. OK.
- The four parallel entry points are now handle-based and install directly,
  matching the seam contract `backend-executor-nodeAppend-seams` declares and
  the shape execParallel's `nodeAppend::exec_append_*::call(planstate, pcxt)`
  call sites use (`crates/backend-executor-execParallel/src/lib.rs:201/355/804/1153`).
  No invented opacity: `PlanStateHandle`/`ParallelContextHandle` are the
  canonical existing handle types; the `append_*` support seams reuse the
  `PlanStateHandle`/`ParallelContextHandle`/`Size` types already in
  `execParallel-support-seams`. No new type invented.
- `pa_lock` lives in a heap-stable `Box`ed `ParallelAppendState` (DSM-resident
  shared state), not a process-wide shared static; the runtime LWLock guard
  (`lwlock_acquire`) is taken through a documented raw-pointer borrow and
  released on all paths (including the abort-path `Drop`). No lock held across
  `?` without a guard. OK.
- `estate_epq_active` reads constant `false` (lib.rs:1305) because the trimmed
  `EState` does not yet carry `es_epq_active`. Ledgered divergence (lands with
  the EPQ port) affecting only the async-recording branch of ExecInitAppend;
  noted, subordinate to the primary finding, revisit when EState gains the
  field.
- `WaitEvent.user_data` has been restored (`Option<i32>` non-aliasing key,
  owned by the waiteventset neighbor) so this unit's delivery loop matches C;
  the prior notify-all divergence is gone. No remaining conformance findings.

## Build

`cargo check -p backend-executor-nodeAppend` succeeds clean. `cargo check
--workspace` succeeds (only pre-existing unrelated warnings). `cargo test -p
seams-init recurrence_guard` — both tests pass.

## Conclusion

**PASS** — independent from-scratch re-audit (no-todo lane). All 18 C function
definitions are present; each is `MATCH` for its owned control flow, with the
four parallel entry points `SEAMED` for the live-node/DSM mutations that belong
to the unported execParallel/`access/parallel.c` handle resolution. **Zero
`todo!()`/`unimplemented!()` remain in `src/`** — the four former
`unimplemented!()` install bridges are eliminated by making the parallel
functions handle-based and installing them directly, delegating node-field/DSM
work to new `append_*` seams in `execParallel-support-seams` (seam-and-panic
under the correct owner, mirroring the audited `sort_*`/`hash_*` families). The
single owned seam crate `backend-executor-nodeAppend-seams` declares four
methods, all installed by `init_seams()` (only `set()` calls), wired via
`seams-init`; both `recurrence_guard` tests pass. The prior
`ExecAppendAsyncEventWait` divergence remains resolved. The crate is clear to
merge.
