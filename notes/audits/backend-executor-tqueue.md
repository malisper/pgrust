# Audit: backend-executor-tqueue

C source: `src/backend/executor/tqueue.c` (210 LOC, 8 functions).
Port: `crates/backend-executor-tqueue/src/lib.rs`.
c2rust cross-check: `../pgrust/c2rust-runs/backend-executor-tqueue/` (8 fns,
no `#if`-gated extras).

## Function table

| C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `tqueueReceiveSlot` (54) | `tqueueReceiveSlot` | MATCH | `ExecFetchSlotMinimalTuple`→`shm_mq_send(...,false,false)`; `SHM_MQ_DETACHED`→`Ok(false)`, `!= SUCCESS`→`ereport(ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "could not send tuple to shared-memory queue")`, else `Ok(true)`. `should_free`/`pfree(tuple)` is internal to the execTuples owner (fetch-copy seam). |
| `tqueueStartupReceiver` (83) | `tqueueStartupReceiver` | MATCH | do-nothing. (`typeinfo` arg dropped — body never reads it; `operation` kept for the dispatch-callback shape.) |
| `tqueueShutdownReceiver` (92) | `tqueueShutdownReceiver` | MATCH | detach if `queue != NULL`, then `queue = None`. |
| `tqueueDestroyReceiver` (105) | `tqueueDestroyReceiver` | MATCH | detach if still attached, then `pfree(self)` = owned drop. |
| `CreateTupleQueueDestReceiver` (119) | `CreateTupleQueueDestReceiver` | MATCH | `palloc0`+set callbacks/`mydest=DestTupleQueue`/`queue=handle`; owned receiver parked in registry, named by `DestReceiverHandle`. Callbacks are the module fns, dispatched by the (unported) dest layer — same as C's fn-pointer table. |
| `CreateTupleQueueReader` (139) | `CreateTupleQueueReader` | MATCH | `palloc0`+`queue=handle`; parked in reader registry. |
| `DestroyTupleQueueReader` (155) | `DestroyTupleQueueReader` | MATCH | `pfree(reader)` = owned drop; underlying `shm_mq` deliberately NOT touched (caller's responsibility). |
| `TupleQueueReaderNext` (176) | `TupleQueueReaderNext` | MATCH | `*done=false`; `shm_mq_receive(...,nowait)`; `DETACHED`→`(None,done=true)`; `WOULD_BLOCK`→`(None,false)`; `SUCCESS`→`(Some(bytes),false)`. The `Assert(result==SUCCESS)` / `Assert(tuple->t_len==nbytes)` are debug-only in C; bytes come from the owner-side receive copy. |

All 8 functions present with full logic. No MISSING / PARTIAL / DIVERGES.

## Seam audit

Owned seam crate: `backend-executor-tqueue-seams` (maps to `tqueue.c`). All five
declarations are installed by `init_seams()`, which contains only `set()` calls:
`create_tuple_queue_reader`, `destroy_tuple_queue_reader`,
`create_tuple_queue_dest_receiver`, `receiver_destroy`,
`tuple_queue_reader_next` (the last added by this port for the future
nodeGather consumer). `seams-init::init_all()` calls
`backend_executor_tqueue::init_seams()`. recurrence_guard (both checks) passes.

Outward seams (justified — direct deps would cycle through the parallel
substrate / execTuples): `backend-storage-ipc-shm-mq-seams` (`shm_mq_send`,
`shm_mq_receive_nowait`, `shm_mq_detach`) and
`backend-executor-execTuples-seams` (`exec_fetch_slot_minimal_tuple_copy`). Each
seam path is thin marshal+delegate (no branching/computation in the seam
wrappers). Two new shm-mq seams (`shm_mq_send`, `shm_mq_receive_nowait`) were
added to the shm-mq seam crate and installed by the `backend-storage-ipc-shm-mq`
owner; the pre-existing `shm_mq_receive` seam was nowait-hardcoded and could not
carry `TupleQueueReaderNext`'s caller-chosen `nowait`.

## Design conformance

- Per-backend C globals (the receiver/reader objects, named by raw pointer in C)
  → `thread_local!` registries (OPTION (i), mirroring `backend-storage-ipc-shm-mq`).
- Allocating/raising paths return `PgResult`; the `ereport(ERROR)` site maps to
  the same SQLSTATE/severity. No `todo!`/`unimplemented!`/`unreachable!`/`panic!`/
  `unwrap`. The four `expect()` are registry-liveness invariants on a handle id
  (same precedent as the shm-mq owner's `.expect("live shm_mq_handle id")`).
- Opacity inherited: handles are the repo's real `ShmMqAttachHandle` /
  `DestReceiverHandle` / `TupleQueueReaderHandle`; minimal-tuple wire bytes use
  the canonical `MinimalTupleData::to_minimal_bytes`. No invented opacity, no
  stand-in type aliases.

## Verdict: PASS
