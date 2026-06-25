# Audit: backend-replication-logical-launcher

C source: `src/backend/replication/logical/launcher.c` (PostgreSQL 18.3).
c2rust run: `../pgrust/c2rust-runs/backend-replication-logical-launcher/`.
Port: `crates/backend-replication-logical-launcher/{lib.rs,state.rs,tests.rs}`.

This audit covers the launcher port as reconciled onto `main`'s DSA/dshash
model (`reconcile/dsa-launcher`): the launcher's last-start-times area and table
are the real `*mut DsaArea` / `*mut DshashTable` substrate pointers from
`types-storage` (opacity inherited, not invented), reached through the existing
`backend-utils-mmgr-dsa-seams` and `backend-lib-dshash-seams`. The
launcher-branch `types-dsa` opaque `DsaAreaHandle`/`DshashTableHandle` tokens and
the parallel `lib-dshash-seams` crate were dropped.

## Function inventory (launcher.c)

| C function | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `get_subscription_list` | `get_subscription_list` | SEAMED | catalog scan via `subscription::*`; assembles `Subscription` rows, same fields C sets |
| `WaitForReplicationWorkerAttach` | `WaitForReplicationWorkerAttach` | MATCH | wait loop; `get_background_worker_pid` (void-returning tuple, no `?`), `wait_latch_my_latch`, generation recheck |
| `logicalrep_worker_find` | `logicalrep_worker_find` | MATCH | slot scan under LogicalRepWorkerLock; returns slot index (C `LogicalRepWorker *`) |
| `logicalrep_workers_find` | `logicalrep_workers_find` | MATCH | collects matching slot indices |
| `logicalrep_worker_launch` | `logicalrep_worker_launch` | MATCH | bgworker register via `register_dynamic_background_worker`; bgw fields per C |
| `logicalrep_worker_stop_internal` | `logicalrep_worker_stop_internal` | MATCH | SIGTERM + wait loop |
| `logicalrep_worker_stop` | `logicalrep_worker_stop` | MATCH | |
| `logicalrep_pa_worker_stop` | `logicalrep_pa_worker_stop` | SEAMED | parallel-apply via `pa::*` |
| `logicalrep_worker_wakeup` | `logicalrep_worker_wakeup` | MATCH | |
| `logicalrep_worker_wakeup_ptr` | `logicalrep_worker_wakeup_ptr` | MATCH | `set_latch_for_proc_pid` |
| `logicalrep_worker_attach` | `logicalrep_worker_attach` | MATCH | sets MyLogicalRepWorker slot (thread_local) |
| `logicalrep_worker_detach` | `logicalrep_worker_detach` | MATCH | uses worker-seams `have_walrcv_conn`/`walrcv_disconnect`/`have_stream_fileset`/`fileset_delete_all`, `pa_*` error-mq seams, `lock_release_all` gated by `initializing_apply_worker` |
| `logicalrep_worker_cleanup` | `logicalrep_worker_cleanup` | MATCH | zeroes slot |
| `logicalrep_launcher_onexit` | `logicalrep_launcher_onexit` | MATCH | |
| `logicalrep_worker_onexit` | `logicalrep_worker_onexit` | MATCH | |
| `logicalrep_sync_worker_count` | `logicalrep_sync_worker_count` | MATCH | |
| `logicalrep_pa_worker_count` | `logicalrep_pa_worker_count` | MATCH | |
| `ApplyLauncherShmemSize` | `ApplyLauncherShmemSize` | MATCH | `SIZEOF_LOGICAL_REP_CTX_STRUCT` 16 + n*`SIZEOF_LOGICAL_REP_WORKER` 128, MAXALIGN |
| `ApplyLauncherRegister` | `ApplyLauncherRegister` | MATCH | guarded by `max_logical_replication_workers()==0 || is_binary_upgrade()` |
| `ApplyLauncherShmemInit` | `ApplyLauncherShmemInit` | MATCH | inits slot array; `last_start_dsa=DSA_HANDLE_INVALID`, `last_start_dsh=DSHASH_HANDLE_INVALID` |
| `logicalrep_launcher_attach_dshmem` | `logicalrep_launcher_attach_dshmem` | MATCH | **reconciled**: `dsa_create`->`*mut DsaArea`, `dsa_pin`/`dsa_pin_mapping` (`PgResult`, `?`), `dshash_create(area, dsh_params())`, publishes `dsa_get_handle`/`dshash_get_hash_table_handle`; attach branch symmetric |
| `ApplyLauncherSetWorkerStartTime` | `ApplyLauncherSetWorkerStartTime` | MATCH | **reconciled**: `dshash_find_or_insert(table,&subid_key)` -> `DshashEntryGuard`; writes `last_start_time` through `*mut LauncherLastStartTimesEntry`; `guard.release()` == C `dshash_release_lock` |
| `ApplyLauncherGetWorkerStartTime` | `ApplyLauncherGetWorkerStartTime` | MATCH | **reconciled**: `dshash_find(table,&subid_key,false)` -> `Option<guard>`; `None` => 0 (C `entry==NULL`), else read+release |
| `ApplyLauncherForgetWorkerStartTime` | `ApplyLauncherForgetWorkerStartTime` | MATCH | **reconciled**: `dshash_delete_key(table,&subid_key)`, result discarded (C `(void)`) |
| `AtEOXact_ApplyLauncher` | `AtEOXact_ApplyLauncher` | MATCH | installed into inward seam `at_eoxact_apply_launcher` (void; the only callee, `SetLatch` via `kill(2)`, is infallible) |
| `ApplyLauncherWakeupAtCommit` | `ApplyLauncherWakeupAtCommit` | MATCH | |
| `ApplyLauncherWakeup` | `ApplyLauncherWakeup` | MATCH | `pg_kill(launcher_pid, SIGUSR1)` direct libc (procsignal precedent) |
| `ApplyLauncherMain` | `ApplyLauncherMain` | MATCH | supervisor loop; signal handlers, `background_worker_initialize_connection`, naptime via `wait_latch_my_latch` |
| `IsLogicalLauncher` | `IsLogicalLauncher` | MATCH | |
| `GetLeaderApplyWorkerPid` | `GetLeaderApplyWorkerPid` | MATCH | |
| `pg_stat_get_subscription` | `pg_stat_get_subscription` | SEAMED | SRF; `funcapi::srf_arg0_oid`/`cstring_get_text_datum` for the trimmed fmgr plumbing |

GUC getters/setters (`max_logical_replication_workers`,
`max_sync_workers_per_subscription`,
`max_parallel_apply_workers_per_subscription`) and the per-backend flags
(`on_commit_launcher_wakeup`, MyLogicalRepWorker slot) are `thread_local!`
per-backend state — MATCH against C's file-scope GUC vars.

## DSA / dshash reconciliation detail

- `dsh_params()` mirrors launcher.c:78 exactly: `key_size = sizeof(Oid)`,
  `entry_size = sizeof(LauncherLastStartTimesEntry)`,
  `key_kind = DshashKeyKind::Binary` (the `dshash_memcmp`/`dshash_memhash`/
  `dshash_memcpy` helper set), `tranche_id = LWTRANCHE_LAUNCHER_HASH`.
- The backend-local mappings are `*mut DsaArea` / `*mut DshashTable` (the real
  substrate pointers, never dereferenced by the launcher) — opacity inherited
  from `types-storage`, not an invented handle.
- `last_start_dsa: dsa_handle` (= `dsm_handle`, u32) and
  `last_start_dsh: dshash_table_handle` (= `dsa_pointer`, u64) in the shared
  control block match the C `LogicalRepCtxStruct` field widths.

## Seam audit

- Inward seams owned: `backend-replication-logical-launcher-seams` — all 13
  declarations installed by `init_seams()` (verified: each `s::*::set(...)`
  present, including the void `at_eoxact_apply_launcher` adapter). `init_seams()`
  is wired into `seams-init::init_all()`.
- Outward seam calls are thin marshal+delegate over real dependency cycles:
  dsa (`backend-utils-mmgr-dsa-seams`), dshash (`backend-lib-dshash-seams`),
  bgworker, latch, lwlock, lock, procarray, tcop, timestamp, funcapi,
  init-small, guc-file, origin, walreceiver, worker, applyparallelworker,
  pg-subscription, ipc. Each owner installs its own; calls panic until the
  owner lands (mirror-PG-and-panic).
- `backend-lib-dshash-seams` was extended (not forked): the key generalized to
  the raw `const void *key` bytes (`&[u8]`), and `dshash_find` (read-only,
  `Option<guard>`) + `dshash_delete_key` added — both real dshash.c primitives.
  The dsm-registry consumer was adapted to pass `name.as_bytes()`.

## Design conformance

- Opacity inherited, never introduced: PASS — `*mut DsaArea`/`*mut DshashTable`,
  no invented tokens.
- Allocating seams take `Mcx` + return `PgResult`: PASS (`cstring_get_text_datum`).
- Per-backend globals are `thread_local!`; cross-backend control block is a
  process-global `Mutex<LogicalRepCtx>` interlocked by LogicalRepWorkerLock:
  PASS.
- Partition lock never held across `?`: PASS — `DshashEntryGuard` releases on
  the explicit `release()` at the C `dshash_release_lock` site, and on `Drop`
  for the error path.

## Verdict: PASS

Every launcher.c function is MATCH or SEAMED per the rules above; no MISSING /
PARTIAL / DIVERGES. `cargo check --workspace` clean; `cargo test` green
(launcher 5/5, dsm-registry 5/5).
