# Audit: backend-storage-sync

Unit: `backend-storage-sync` (`src/backend/storage/sync/sync.c`)
Crates: `backend-storage-sync`, `backend-storage-sync-seams`,
`backend-storage-smgr-md-seams`, `types-sync`.

Audit method: re-derived the full function inventory from
`postgres-18.3/src/backend/storage/sync/sync.c` (619 lines), cross-checked
against the c2rust rendering (`c2rust-runs/backend-storage-sync/src/sync.rs`)
and the port. Constants verified against `include/storage/sync.h` and
`include/storage/fd.h`, not from memory.

## Constants verified against headers

| Constant | C value (header) | Port | Verdict |
|---|---|---|---|
| `SYNC_REQUEST` | 0 (`sync.h:25`) | `SyncRequest = 0` | MATCH |
| `SYNC_UNLINK_REQUEST` | 1 (`sync.h:26`) | `SyncUnlinkRequest = 1` | MATCH |
| `SYNC_FORGET_REQUEST` | 2 (`sync.h:27`) | `SyncForgetRequest = 2` | MATCH |
| `SYNC_FILTER_REQUEST` | 3 (`sync.h:28`) | `SyncFilterRequest = 3` | MATCH |
| `SYNC_HANDLER_MD..NONE` | 0..5 (`sync.h:37-42`) | `Md=0..None=5` | MATCH |
| `FSYNCS_PER_ABSORB` | 10 (`sync.c:78`) | 10 | MATCH |
| `UNLINKS_PER_ABSORB` | 10 (`sync.c:79`) | 10 | MATCH |
| `FILE_POSSIBLY_DELETED` (non-Win) | `err == ENOENT` (`fd.h:89`) | `err == ENOENT` | MATCH |
| `CycleCtr` | `uint16` (`sync.c:54`) | `u16` | MATCH |
| `FileTag` layout | i16 handler, i16 forknum, RelFileLocator, u64 segno (`sync.h:51-56`) | identical | MATCH |

The `syncsw[]` table (`sync.c:95-118`): MD defines all three callbacks; CLOG /
COMMIT_TS / MULTIXACT_OFFSET / MULTIXACT_MEMBER define only `sync_syncfiletag`;
NONE has no row. The port's three dispatch helpers reproduce this exactly:
`sync_filetag` routes all five real handlers; `unlink_filetag` /
`filetag_matches` route only MD and Err on the rest (C would deref a NULL fn
pointer there, which `sync.c` never does).

## Per-function table

| C function (loc) | Port (loc) | Verdict | Notes |
|---|---|---|---|
| file statics `pendingOps`/`pendingUnlinks`/`pendingOpsCxt`/`sync_cycle_ctr`/`checkpoint_cycle_ctr` (sync.c:70-75) + fn-static `sync_in_progress` (sync.c:288) | `SyncState` thread_local (lib.rs:102-134) | MATCH | Process-local in C, not shmem; correctly a `thread_local!` per backend-global-state rule; owned `HashMap`/`Vec` per mctx-design decision 5; `pending_ops: Option` reproduces the `pendingOps != NULL` test. |
| `PendingFsyncEntry` (sync.c:56-61) | struct (lib.rs:75-80) | MATCH | `tag` is the map key (HASH_BLOBS), so not duplicated in the value. |
| `PendingUnlinkEntry` (sync.c:63-68) | struct (lib.rs:85-92) | MATCH | tag stored inline (list element). |
| `syncsw[]` vtable (sync.c:95-118) | `sync_filetag`/`unlink_filetag`/`filetag_matches` (lib.rs:191-238) | SEAMED | Dispatch on `handler` owned in-crate; each arm delegates to the owner's seam (md/clog/commit-ts/multixact). Thin marshal+delegate, no branching in seams. |
| `InitSync` (sync.c:123-158) | `init_sync` (lib.rs:250-259) | MATCH | `create_pending_ops` = caller's `!IsUnderPostmaster||AmCheckpointerProcess()` (no ambient getter). Hash-table create + `pendingUnlinks=NIL` → fresh `HashMap`/`Vec`. `pendingOpsCxt`/critical-section flag absorbed by owned collections. |
| `SyncPreCheckpoint` (sync.c:176-194) | `sync_pre_checkpoint` (lib.rs:262-271) | MATCH | Absorb (seam) then `checkpoint_cycle_ctr++` (wrapping, u16). |
| `SyncPostCheckpoint` (sync.c:201-280) | `sync_post_checkpoint` (lib.rs:275-350) | MATCH | foreach with index `i`; skip-canceled `continue`; break on `cycle_ctr==checkpoint_cycle_ctr`; unlink via seam, `errno!=ENOENT` → WARNING (else swallowed); mark canceled; per-`UNLINKS_PER_ABSORB` absorb. `lc==NULL` → `clear()`; else `ntodelete=list_cell_number(...,lc)=stop_index`, `drain(0..stop_index)` — verified against `list_cell_number`/`list_delete_first_n` semantics. |
| `ProcessSyncRequests` (sync.c:285-475) | `process_sync_requests` (lib.rs:356-564) | MATCH | `!pendingOps` → `elog(ERROR)`; absorb; stale-cycle refresh under `sync_in_progress`; `sync_cycle_ctr++`, `sync_in_progress=true`; key-snapshot scan (within hash_seq_search "unspecified for new entries" contract); new-entry `continue`; debug_assert cycle+1; `enableFsync` gate; periodic absorb; retry loop re-reads live `canceled`; success timing/`log_checkpoints` DEBUG1; `!FILE_POSSIBLY_DELETED||failures>0` → `data_sync_elevel(ERROR)` seam → Err (CheckpointStats and `sync_in_progress=false` correctly skipped, leaving the failure-retry state); else DEBUG1 + absorb + `failures++`; hash-remove or "pendingOps corrupted" ERROR; CheckpointStats via seam; `sync_in_progress=false`. |
| `RememberSyncRequest` (sync.c:486-571) | `remember_sync_request` (lib.rs:570-703) | MATCH | FORGET: HASH_FIND → set canceled. FILTER: same-handler candidate scan + match-seam → cancel fsync + unlink entries (handler pre-checked before match, as in C). UNLINK: append (fallible reserve). SYNC: HASH_ENTER semantics — unchanged cycle_ctr if existing non-canceled (oldest-request invariant), re-init if canceled, insert with `sync_cycle_ctr` if new. |
| `RegisterSyncRequest` (sync.c:579-619) | `register_sync_request` (lib.rs:709-737) | MATCH | `pendingOps!=NULL` → local Remember, return true. Else loop: ForwardSyncRequest seam; break on `ret||!retryOnError` (C `ret||(!ret&&!retryOnError)` is equivalent); WaitLatch retry seam. |

## Seam audit

Outward seams (all justified by a real cycle, thin marshal+delegate):
- checkpointer: `absorb_sync_requests`, `forward_sync_request`,
  `checkpoint_stats_set` — checkpointer ↔ sync cycle.
- file: `data_sync_elevel(ERROR)` — fd.c.
- latch: `wait_latch_register_sync_request` — fixed-arg `WaitLatch(NULL, ...)`
  baked into a dedicated seam (no LatchHandle); correct.
- md: `mdsyncfiletag`/`mdunlinkfiletag`/`mdfiletagmatches` (smgr-md-seams).
- clog/commit-ts/multixact: `*syncfiletag` (one arm each).

Signatures of all dependency seams confirmed present and matching
(`backend-postmaster-checkpointer-seams`, `backend-storage-file-seams`,
`backend-storage-ipc-latch-seams`, `backend-access-transam-clog-seams`,
`backend-access-transam-commit-ts-seams`,
`backend-access-transam-multixact-seams`). No branching, node construction, or
computation in any seam path — the `syncsw` dispatch logic lives in-crate;
seams only marshal `FileTag` ↔ `FileTagOpResult`.

Inward seams (`backend-storage-sync-seams`): `init_sync`,
`sync_pre_checkpoint`, `sync_post_checkpoint`, `process_sync_requests`,
`register_sync_request`. All five installed by `init_seams()` (lib.rs:768-774),
which contains nothing but `set()` calls. `seams-init::init_all` calls
`backend_storage_sync::init_seams()` (seams-init/src/lib.rs:46). No `set()`
outside the owner. `RememberSyncRequest` is reached only inward through
`RegisterSyncRequest` (matching C: it is a checkpointer-side callback with no
external callers in this unit), so it has no public seam — correct.

## Design conformance

- Per-backend globals → `thread_local!` `SyncState`, not shared statics. PASS.
- No ambient-global getter seams: `IsUnderPostmaster`/`AmCheckpointerProcess`
  and `enableFsync`/`log_checkpoints` are passed in as params. PASS.
- Allocating paths (`pending_ops`/`pending_unlinks` growth) use `try_reserve`
  → `Err(PgError)` instead of abort; `PgResult` returns. PASS.
- No locks held across `?`; no registry side tables; no invented opacity
  (`FileTag` mirrors the C struct fields, which `sync.c` itself treats as
  opaque-but-public). PASS.

## Verdict: PASS

Every function MATCH (or SEAMED per the dispatch-table rule); zero seam
findings; all constants verified against headers. Build clean; 18 unit tests
pass. Spot-checked in detail: the `SyncPostCheckpoint` `list_cell_number` /
`list_delete_first_n` index arithmetic, the `ProcessSyncRequests` failure path
leaving `sync_in_progress=true` (the wraparound-refresh mechanism), and the
`RememberSyncRequest` SYNC oldest-cycle_ctr invariant — all confirmed.
