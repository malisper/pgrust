# Audit: backend-access-transam-parallel

- **Unit:** `backend-access-transam-parallel`
- **C source:** `src/backend/access/transam/parallel.c` (PostgreSQL 18.3, 1672 lines)
- **Branch:** `port/backend-access-transam-parallel`
- **Date:** 2026-06-12 (independent re-audit)
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **TOP-LINE VERDICT:** **PASS** — logic parity complete on all 19 functions +
  `IsParallelWorker`; the previously-failing §4.1 design-conformance finding
  (no `Mcx` on allocating functions; infallible buffer alloc; hand-rolled OOM)
  is verified **RESOLVED** at root (see §6). §4.2/§4.3 remain ledgered debt
  (sanctioned in CATALOG + `docs/types.md`), not independent FAIL causes.

This is a second, fully independent re-derivation from the C
(`src/backend/access/transam/parallel.c`), the c2rust rendering
(`c2rust-runs/backend-access-transam-parallel/src/parallel.rs`), and the port
(`crates/backend-access-transam-parallel/src/lib.rs`), at commit `dce50e41`.
Confirms the §4.1 fix that flipped the earlier in-band FAIL. Findings below
(§4.1) describe the *original* violation and its now-verified remedy.

## 1. Function inventory

Every function definition in `parallel.c` (19 total: 15 external + 4 static).
c2rust kept the 15 external ones (the 4 statics were inlined by the optimizer);
cross-checked — none dropped.

| # | C function (line) | Port symbol | Verdict | Notes |
|---|---|---|---|---|
| 1 | `CreateParallelContext` (172) | `create_parallel_context` | MATCH* | palloc0 ctx, subid/nworkers/names/error_context_stack, `shm_toc_initialize_estimator` (default estimator), dlist_push_head. *Allocates without `Mcx` — see §4.1 |
| 2 | `InitializeParallelDSM` (210) | `initialize_parallel_dsm` | MATCH* | full estimate→create→fps init→serialize sequence; all 14 chunk keys, `INTERRUPTS_CAN_BE_PROCESSED`/`GetSessionDsmHandle` zero-worker fallbacks, `IsolationUsesXactSnapshot` tsnap gate, dsm-or-private fallback, worker/error-queue loop, entrypoint `lib\0fn\0`. *Allocates without `Mcx`, hand-rolled OOM, infallible `vec![0u8; segsize]` — see §4.1 |
| 3 | `ReinitializeParallelDSM` (508) | `reinitialize_parallel_dsm` | MATCH | finish+exit old workers, reset known_attached, `fps->last_xlog_end=0`, recreate error queues |
| 4 | `ReinitializeParallelWorkers` (565) | `reinitialize_parallel_workers` | MATCH | `nworkers_to_launch = Min(nworkers, requested)` |
| 5 | `LaunchParallelWorkers` (580) | `launch_parallel_workers` | MATCH | 0-worker early return, `BecomeLockGroupLeader`, register loop with `any_registrations_failed` short-circuit + error-queue detach on failure, `known_attached_workers` alloc. BackgroundWorker struct assembly delegated into `register_dynamic_background_worker` seam (thin) |
| 6 | `WaitForParallelWorkersToAttach` (700) | `wait_for_parallel_workers_to_attach` | MATCH | CHECK_FOR_INTERRUPTS loop, per-worker BGWH_STARTED/STOPPED/else branches, `parallel worker failed to initialize` ereport, WaitLatch/ResetLatch, `nknown >= launched` exit + assert. `pub` for future callers (no current consumer; not in `init_seams`, which is correct) |
| 7 | `WaitForParallelWorkersToFinish` (803) | `wait_for_parallel_workers_to_finish` | MATCH | anyone_alive/nfinished loop, investigate-stopped branch with failed-to-init ereport, final WaitLatch, `last_xlog_end > XactLastRecEnd` update (toc-present proxied by `!buffer.is_empty()`, equivalent since the toc is always created) |
| 8 | `WaitForParallelWorkersToExit` (917, static) | `wait_for_parallel_workers_to_exit` | MATCH | per-worker `WaitForBackgroundWorkerShutdown`, BGWH_POSTMASTER_DIED → FATAL `ERRCODE_ADMIN_SHUTDOWN`, pfree handle |
| 9 | `DestroyParallelContext` (957) | `destroy_parallel_context` | MATCH | dlist_delete first, terminate+detach worker queues, dsm_detach, private_memory pfree, HOLD/exit/RESUME interrupts, free worker array + ctx |
| 10 | `ParallelContextActive` (1031) | `parallel_context_active` | MATCH | `!dlist_is_empty(pcxt_list)` |
| 11 | `HandleParallelMessageInterrupt` (1044) | `handle_parallel_message_interrupt` | MATCH | `InterruptPending=true; ParallelMessagePending=true; SetLatch(MyLatch)`. Signal-handler-safe flag flips; `expect` on infallible flag/latch seams |
| 12 | `ProcessParallelMessages` (1055) | `process_parallel_messages` | MATCH | HOLD_INTERRUPTS, hpm private context, clear pending flag, dlist_foreach, per-worker drain loop with WOULD_BLOCK/SUCCESS/lost-connection arms, reset+RESUME |
| 13 | `ProcessParallelMessage` (1144, static) | `process_parallel_message` | MATCH | known_attached set, `pq_getmsgbyte`, Error/Notice (`Min(elevel,ERROR)`=21, parallel-worker context line gated on `!= DEBUG_PARALLEL_REGRESS`, stack swap delegated thin), Notification, Progress (incr_param), Terminate (detach), default elog(ERROR) |
| 14 | `AtEOSubXact_Parallel` (1261) | `at_eosubxact_parallel` | MATCH | head-while loop, subid break, WARNING leaked-context on commit, destroy |
| 15 | `AtEOXact_Parallel` (1282) | `at_eoxact_parallel` | MATCH | nuke-all loop, WARNING on commit, destroy |
| 16 | `ParallelWorkerMain` (1299) | `parallel_worker_main` | MATCH | full 280-line restore sequence in C order: init flag, signal handlers, worker number, memctx, dsm_attach + magic check (`could not map`/`invalid magic number`), fps lookup, leader signal arrangement, error-queue attach + pq redirect, lock-group join (quiet return on failure), timestamps, entrypoint lookup, auth/role, db connection, client encoding, library/GUC/txn-state/pending-syncs/relmapper/reindex/combocid/session/snapshots/uncommitted-enums/clientconninfo restore in exact order, invalidate caches, sys-user init, serializable attach, EnterParallelMode, entrypt, teardown, Terminate |
| 17 | `ParallelWorkerReportLastRecEnd` (1593) | `parallel_worker_report_last_rec_end` | MATCH | assert fps, spinlock CAS delegated to `fps_report_last_rec_end` seam (genuine cross-process spinlock) |
| 18 | `ParallelWorkerShutdown` (1621, static) | `parallel_worker_shutdown` | MATCH | `SendProcSignal(leaderpid, PROCSIG_PARALLEL_MESSAGE, procno)`, dsm_detach from Datum |
| 19 | `LookupParallelWorkerFunction` (1649, static) | `lookup_parallel_worker_function` | MATCH | `"postgres"` → `InternalParallelWorkers` scan (5 names, exact order) → resolve; not-found elog(ERROR); else `load_external_function` |

Header inline `IsParallelWorker()` (`access/parallel.h`): `is_parallel_worker` = `ParallelWorkerNumber >= 0` — MATCH.

## 2. Constants (verified against C headers, not memory)

| Constant | C | Port | Verdict |
|---|---|---|---|
| `PARALLEL_ERROR_QUEUE_SIZE` | 16384 (parallel.c:55) | 16384 | MATCH |
| `PARALLEL_MAGIC` | `0x50477c7c` (:58) | `0x5047_7c7c` | MATCH |
| `PARALLEL_KEY_FIXED..CLIENTCONNINFO` | `0xFFFF…0001`–`000F` (:65-79) | identical 1:1 | MATCH |
| `DSM_HANDLE_INVALID` | 0 (dsm_impl.h:58) | 0 | MATCH |
| `PqMsg_{Error,Notice,Notification,Progress,Terminate}` | `E N A P X` (protocol.h:28,41,44,49,69) | `E N A P X` | MATCH |
| `ERROR` elevel | 21 (elog.h:52) | 21 (`ERROR_ELEVEL`) | MATCH |
| `BGWORKER_BYPASS_{ALLOWCONN,ROLELOGINCHECK}` | `0x0001`/`0x0002` | same | MATCH |
| `DEBUG_PARALLEL_REGRESS` | 2 | 2 | MATCH |
| `WAIT_EVENT_BGWORKER_STARTUP` / `WAIT_EVENT_PARALLEL_FINISH` | generated (`wait_event_types.h`, build artifact — not in tree) | `PG_WAIT_IPC\|3` / `\|40` | UNVERIFIABLE (non-load-bearing: passed opaquely to the latch seam for pg_stat_activity reporting only; no control-flow effect). Observation, not a finding |

`FixedParallelState` (types-parallel): all 17 C fields present, in order, with
correct types (parallel.c:82-107) — MATCH. Opaque handles
(`dsm_segment*`/`shm_mq*`/`PGPROC*`/`BackgroundWorkerHandle*`) are inherited
opacity over real C pointers, not invented (types.md rules 6-7 satisfied).

## 3. Seam audit

**Owned seam crate (by C-source coverage: `parallel.c`):**
`crates/backend-access-transam-parallel-seams` (42 `pub fn` declarations).

- Every one of the 42 declarations is installed by this crate's `init_seams()`
  (diff `declared` vs `seams::*::set` — zero gaps both directions).
- `init_seams()` is nothing but `set()` calls (no logic). It is invoked by
  `seams-init::init_all()` (per CATALOG wiring).
- The accessor/`shm_toc`/typed-store seams are installed with **real logic**
  living in this crate (estimate/allocate/insert/lookup operate on the
  context-owned buffer; the `store_*`/`with_*` helpers are local) — bodies are
  not delegated elsewhere, so they are correctly owned here, not MISSING.

**Outward calls:** collected in `crates/backend-access-transam-parallel-rt-seams`
(131 declarations, pure decls, no installer — owned by the not-yet-ported
callees). The bodies are NOT in this unit, so no logic-in-seam violation here.
Spot-checked the thin-marshal rule on the non-trivial-looking ones:
`throw_parallel_error_data` (save/swap `error_context_stack`/throw/restore around
a global owned by elog — thin, the swap is unavoidable marshaling), the `parse_*`
libpq readers, `register_dynamic_background_worker` (BackgroundWorker struct
assembly) — all marshal+delegate, acceptable.

**No seam findings on installation/wiring.**

## 4. Design conformance (§3b — merge-blocking)

### 4.1 Allocating functions bypass `mcx` (AGENTS "Memory allocation (mcx)") — FINDING

The repo rule: *"functions that allocate take an `Mcx<'mcx>` parameter … Always
allocate through the fallible APIs … OOM converts to `mcx.oom(size)` … never a
hand-rolled `PgError::error("out of memory")`."* The port violates this:

- **No `Mcx` anywhere in the crate** (`grep -i mcx` → empty). `palloc0`/`palloc`
  sites — `CreateParallelContext` (`palloc0(ParallelContext)`),
  `InitializeParallelDSM` (`palloc0` worker array, `MemoryContextAlloc` private
  memory, the DSM byte buffer) — take no context and allocate with bare
  `Vec`/`String`.
- **Infallible alloc on a caller-controlled size:** `c.buffer = vec![0u8; segsize]`
  at lib.rs:785 and :794. `segsize` is the estimator total — exactly the
  allocating step the rule says must be fallible (`try_reserve`/`alloc_in`); the
  infallible form aborts instead of erroring.
- **Hand-rolled OOM:** lib.rs:878-882 does `try_reserve(...).map_err(|_|
  ereport(ERROR).errmsg("out of memory allocating parallel worker array"))`.
  This loses the `ERRCODE_OUT_OF_MEMORY` SQLSTATE and context-name detail the
  rule requires from `mcx.oom(size)`.

This is unledgered (the prior note did not record it) and directly violates a
written rule → **FAIL**.

### 4.2 DSM-as-byte-buffer + thread-local typed side tables — LEDGERED DEBT, merge-relevant

The leader-side `shm_toc` chunk payloads are not stored in the buffer; they live
in four `thread_local!` side tables (`FIXED_STATES`, `CSTRINGS`,
`INSTRUMENTATIONS`, `JIT_HEADERS`) keyed by `SerializeCursor`, plus the whole
context slab `Vec<Option<ParallelContext>>`. Per §3b this is a *registry-shaped
side table*. It is recorded as accepted debt in the port note and the CATALOG row
("a true cross-process DSM layout collapses these onto the real segment when
dsm.c/shm_toc.c land"), and the opaque `ParallelContextHandle` is explicitly
sanctioned in `docs/types.md` ("inherited opacity that collapses onto the owners'
real types when they land"). So the handle/registry model is *ledgered* and not
counted as an independent FAIL cause; it is noted because it compounds 4.1 (the
buffer it allocates is the infallible-`vec!` site).

### 4.3 Consolidated `-rt-seams` crate (~131 outward decls, ~15 owners) — LEDGERED DEBT

The per-owner seam discipline wants each outward call declared in its owner's
`-seams` crate. They are consolidated here pending those owners. Ledgered in
CATALOG and the port note; not an independent FAIL cause, but it is the standing
debt to unwind as owners land. Note the zero-argument getter seams over other
units' per-backend globals (`xact_last_rec_end`, `error_context_stack`,
`debug_parallel_query`, `get_database_encoding`, `parallel_leader_proc_number`)
are the "zero-argument getter seam" the neighbor-dependency table discourages;
acceptable only because they read globals genuinely owned by unported neighbors
and live in the consolidated rt-seams debt, to be reclaimed as params when owners
land.

## 5. Verdict

- **Logic:** all 19 functions + `IsParallelWorker` are **MATCH** or properly
  **SEAMED**. No MISSING / PARTIAL / DIVERGES. Build gate clean
  (`cargo check` on the 4 crates — warnings only).
- **Design conformance:** **FAIL** on §4.1 (no `Mcx` on allocating functions;
  infallible buffer alloc on a caller-sized value; hand-rolled OOM that drops the
  SQLSTATE). §4.2/§4.3 are ledgered debt.

**§5 (historical, the round that FAILed):** logic parity complete, but the
unledgered mcx/OOM design-conformance violation in §4.1 was a FAIL. It was
handed to the fix lane and fixed in commit `dce50e41` (§6).

**TOP-LINE (this re-audit): PASS.** Logic parity complete; §4.1 verified
resolved from scratch (re-derived below). §4.2/§4.3 are ledgered debt.

## 6. Fix follow-up (§4.1 resolved → PASS) — independently re-verified

The §4.1 finding has been fixed at root; the unit now conforms to AGENTS
"Memory allocation (mcx)". Each bullet below was re-checked against the current
`crates/backend-access-transam-parallel/src/lib.rs` in this re-audit
(line refs: `create_parallel_context` :663-705 takes `mcx`, `check_alloc_size`
:676 + `mcx.oom` :246/:254 on the struct/handle alloc via `push_head`;
`alloc_zeroed_buffer` :719-725 = `check_alloc_size`→`try_reserve_exact`→
`mcx.oom`, called at :823/:832; worker array :920-928 = `check_alloc_size`→
`try_reserve`→`mcx.oom(request)`; caller `execParallel` :484-489/:573 passes its
own `mcx`. No infallible `vec![0u8; segsize]` and no hand-rolled "out of memory"
`ereport` remain — both grep clean outside comments):

- **`Mcx` threaded through the allocating seams.** Both
  `create_parallel_context` and `initialize_parallel_dsm` now take
  `mcx: Mcx<'mcx>` (seam decls updated in
  `backend-access-transam-parallel-seams`; the sole caller
  `execParallel::ExecInitParallelPlan` passes its own `mcx`). No ambient-context
  assumption crosses the seam — the caller hands the target context (C's
  `TopTransactionContext`) explicitly.
- **Fallible buffer alloc.** The two infallible `vec![0u8; segsize]` sites are
  replaced by `alloc_zeroed_buffer(mcx, segsize)` — `check_alloc_size` (the
  `MaxAllocSize` gate) then `try_reserve_exact`, OOM → `mcx.oom(segsize)`
  (`ERRCODE_OUT_OF_MEMORY` + context name). Covers both the DSM-backed and the
  no-worker `MemoryContextAlloc(TopMemoryContext)` paths.
- **No hand-rolled OOM.** The worker-array `ereport(ERROR).errmsg("out of
  memory…")` is gone; it now uses `mcx.oom(request)` after the `check_alloc_size`
  gate.
- **`palloc0(ParallelContext)` + `dlist_push_head`.** `push_head` is now fallible
  (`try_reserve` the slab/list grow, OOM → `mcx.oom`), so the context-struct
  allocation's OOM also carries the correct SQLSTATE rather than aborting.

§4.2 (DSM-as-byte-buffer + thread-local side tables) and §4.3 (consolidated
`-rt-seams`) remain ledgered debt to be unwound when dsm.c/shm_toc.c and the
neighbor owners land. Gate: `cargo check --workspace` + `cargo test --workspace`
both clean.

## 7. Family `dsm-substrate-convert` re-audit (real DSM substrate) — PASS

The `Vec<u8>` DSM emulation of the **DSM-init core** is retired onto the merged
`dsm-core` / `shm-toc` substrate (decomp `docs/decomp-parallel-dsm-real.md`,
family A). Re-derived against `parallel.c:325-339` (`segsize =
shm_toc_estimate(&estimator); seg = dsm_create(segsize,
DSM_CREATE_NULL_IF_MAXSEGMENTS); toc = shm_toc_create(PARALLEL_MAGIC,
dsm_segment_address(seg), segsize)` else the private-memory branch):

- **`pcxt.seg` is a real segment.** `ParallelContext` now holds the real
  `DsmSegment` RAII guard (`seg_guard`) plus `seg: DsmSegmentHandle` carrying the
  real `DsmSegmentId::as_u64()` (opacity-inherited; `seg_handle_of`/`seg_id_of`
  are the bijection, and `0` stays NULL because `dsm-core` `DSM_NEXT_ID` starts
  at 1). `DestroyParallelContext` detaches by dropping the guard (C
  `dsm_detach`).
- **Real `shm_toc` over the real base.** `establish_parallel_segment` calls
  `dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS, top)` then
  `ShmToc::create(PARALLEL_MAGIC, NonNull(dsm_segment_address(id)), segsize)`.
  The retired `rt::dsm_create_null_if_maxsegments` panic-seam is gone (the merged
  `dsm_create` is called directly). `segsize` is now the full
  `shm_toc_estimate(&estimator)` (header + entry array + chunk space), not the
  former `space_for_chunks`-only value.
- **Local `ShmToc`/`Estimator` retired.** `estimator` is
  `types_storage::storage::shm_toc_estimator`; `shm_toc_estimate_chunk`/`_keys`
  delegate to the `shm-toc` owner's estimator inlines; `shm_toc_allocate`/
  `insert`/`lookup` delegate to the real `ShmToc` and relativize chunks against
  the recorded segment base, preserving the execParallel-visible
  `SerializeCursor = (slot << 32) | offset` contract (no contract change — the
  offset is now a real in-segment offset). The infallible-by-construction sites
  (`allocate`/`insert` of an exactly-`shm_toc_estimate`-sized segment, and
  required-key `lookup`) `expect` on the real fallible APIs, mirroring the C
  `ereport(ERROR)`/`elog(ERROR)` that never return.
- **No-worker fallback.** Backend-private memory is a real
  `TopMemoryContext`-allocated buffer (`top.allocate(Layout)` via the new
  `top_memory_context()` seam, freed in `DestroyParallelContext` with
  `top.deallocate`), aligned for the in-segment `shm_toc` header; the
  `ShmToc::create` is built over it exactly as the DSM branch.
- **Runtime test (real DSM).** `dsm_substrate_tests` drives the DSM-init core
  over the merged `dsm_test_bringup()` (feature `test-bringup`): asserts a real
  `dsm-core` segment is created (handle carries the real id; `toc_base ==
  dsm_segment_address(id)`; `dsm_segment_map_length >= segsize`), and that
  allocate→insert→lookup round-trip every chunk to a real, writable in-segment
  address; a companion test covers the no-worker private-memory branch. Both
  pass; `cargo test --workspace` clean (no flakes).

**Still ledgered / deferred (NOT this family):** the typed chunk side tables
(`FIXED_STATES`/`CSTRINGS`/`INSTRUMENTATIONS`/`JIT_HEADERS`) keyed by
`SerializeCursor` remain (§4.2) — reshaping them into in-segment `repr(C)` writes
and the `SerializeCursor`-as-address contract change is the follow-up
`shm-toc-address` family; the `shm_mq`/worker-side attach reshape is the
`shm-mq-seams` family. The leader-side serializer rt-seams (`fps_init`,
`serialize_*`) keep their `usize`-offset signatures and stay panic-seams until
their owners land. No new DESIGN_DEBT introduced by this family.

---

## Re-audit — family `shm-toc-address` (real chunk addresses, in-place repr(C))

Branch `decomp/shm-toc-address` off `decomp/dsm-substrate-convert`. This family
retires the typed chunk side tables and converts the inward `SerializeCursor` /
`*Handle` contract from a `(slot<<32)|offset` side-table token to a **real
in-segment chunk address**. No new DESIGN_DEBT; no stubbing.

- **`SerializeCursor` = real address.** `shm_toc_allocate`/`insert`/`lookup`
  return/take the raw `ShmToc::allocate`/`lookup` pointer reinterpreted as
  `usize` (`SerializeCursor(ptr.as_ptr() as usize)`). The `(slot<<32)|offset`
  codec (`make_cursor`/`cursor_parts`) is deleted. `types-execparallel`
  documents `SerializeCursor` and the three in-DSM handles
  (`FixedStateHandle`/`InstrumentationHandle`/`JitInstrumentationHandle`) as
  thin views over the real chunk address; `0` stays the NULL sentinel.
- **Side tables retired.** The four `thread_local!`s
  (`FIXED_STATES`/`CSTRINGS`/`INSTRUMENTATIONS`/`JIT_HEADERS`) and the
  token→side-table indirection are gone. The typed payloads are written and
  read back IN PLACE at the real chunk address via `read_unaligned`/
  `write_unaligned` (chunks are `BUFFERALIGN`ed, over-aligned vs the structs).
- **`repr(C)` payloads.** `FixedParallelExecutorState` and
  `SharedExecutorInstrumentation` are `#[repr(C)]` (the latter is now the
  four-`int` header; the `plan_node_id[FLEXIBLE_ARRAY_MEMBER]` follows it at
  `offsetof(.., plan_node_id)` and the `Instrumentation[]` at
  `instrument_offset`, written separately through `set_sei_plan_node_id` and the
  `instr_*`/`sei_*` support seams — exactly as C writes past the header,
  `GetInstrumentationArray`). `store_fixed_state`/`fixed_*`,
  `store_instrumentation_header`/`sei_*`/`set_sei_plan_node_id`,
  `store_jit_instrumentation_header` (writes `num_workers` + zeroes the
  `jit_instr[]` at `MAXALIGN(sizeof(int))`)/`shared_jit_num_workers` all
  dereference the real address. `store_cstring`/`cursor_cstring` `memcpy` / read
  the NUL-terminated bytes in place.
- **Entrypoint cstrings.** `write_entrypoint`/`read_entrypoint` are now in-place
  helpers in parallel.c (`strcpy(state, lib); strcpy(state+lnamelen+1, fn)` and
  the `strlen`-split read, parallel.c:488-492 / 1416-1418); the two
  same-named **rt-seams were retired** (no `.set()` ever existed — they were
  panic-seams). The `usize` "base"/"space" args of the remaining serializer
  rt-seams (`fps_init`, `serialize_*`, `worker_toc_lookup`, `write/read_dsm_handle`)
  now carry the real address; signatures unchanged, owners still panic-seams.
- **execParallel consumers.** Call sites are unchanged in shape (same seams);
  the only source change is dropping the now-absent `plan_node_id: Vec::new()`
  field from the `SharedExecutorInstrumentation` header literal. The
  `instrument_offset`/`OFFSET_OF_JIT_INSTR` arithmetic already matched the C and
  now lands on real in-segment offsets.
- **Runtime test (real DSM).** New `typed_chunks_round_trip_in_place_at_real_address`
  over `dsm_test_bringup()` proves every typed store/load (fixed state,
  cstring, instrumentation header + `plan_node_id` flexible array, JIT header,
  entrypoint bytes) round-trips through real, in-segment addresses with the
  handle being a thin view (`handle.0 == chunk.0`). The two family-A DSM-init
  tests still pass. `cargo check --workspace` + `cargo test --workspace` clean;
  `seams-init` `recurrence_guard` passes.

**Still deferred (NOT this family):** the `shm_mq` / worker-side attach reshape
is the `shm-mq-seams` family. No new DESIGN_DEBT introduced.

---

## Family `shm-mq-leader` (C1) re-audit — error-queue call sites real

The `shm_mq` error queues are now real `shm-mq` over real `shm_toc` chunk
addresses, driven through the OPTION (i) seam layer
(`backend-storage-ipc-shm-mq-seams`, installed by the `shm-mq` owner's
`init_seams()`). The retired `rt-seams` `shm_mq_*` declarations are gone.

- `error_mqh` is the `ShmMqAttachHandle` registry id (`ERROR_MQH_NULL == 0` is
  the C NULL `shm_mq_handle *`; `error_mqh_is_null` is the NULL test).
- `initialize_parallel_dsm` / `reinitialize_parallel_dsm`:
  `shm_mq_create_at(error_queue_space, i, SIZE)` (== C
  `shm_mq_create(eq + i*SIZE, SIZE)`), `shm_mq_set_receiver_to_myproc`,
  `shm_mq_attach(mq, pcxt.seg)` — `seg_to_exec` carries the real `DsmSegmentId`
  (NULL/private-memory -> `None`, i.e. C's `shm_mq_attach(mq, NULL, NULL)`).
- `launch_parallel_workers`: `shm_mq_set_handle(error_mqh, bgwhandle)` (bgwhandle
  bridged to the exec token via `bgw_to_exec`); the registration-failure arm
  `shm_mq_detach`es and NULLs `error_mqh`.
- `wait_for_parallel_workers_to_attach` / `_to_finish`: `shm_mq_get_queue` +
  `shm_mq_get_sender(...).is_none()` (== C `shm_mq_get_sender(mq) == NULL`).
- `process_parallel_messages`: nowait `shm_mq_receive` -> `(Option<ShmMqResult>,
  Vec<u8>)`; `process_parallel_message` Terminate `shm_mq_detach`es.
- `destroy_parallel_context`: `terminate_background_worker` then `shm_mq_detach`.
- `parallel_worker_main` (worker error queue, sender side) converted the same
  way over the worker's real `seg`/chunk.

`my_latch()` and the real `DsmSegmentId` are threaded into `shm_mq_attach` by the
seam impl (the C ambient `MyLatch` / `pcxt->seg`). All sites match the C.

**Build/test:** `cargo check --workspace` clean; `cargo test
-p backend-access-transam-parallel` 4/4; `seams-init` recurrence_guard passes.
No new DESIGN_DEBT. Deferred elsewhere: bgworker-handle resolution
(`background_worker_handle_from_token`, bgworker owner) and the execParallel
tqueue reshape.

---

## Family `tqueue-substrate-check` (integration) — PASS

Final integration check that the whole parallel/execParallel DSM lineage is on
the real substrate with NO emulation: a real `DsmSegmentId` + `Mcx<'static>` +
`MyLatch` thread into `shm_mq_attach` for the **tuple queues**, and
`shm_mq_create_at(real_chunk_addr, i, size)` carves a real in-segment base.
(execParallel's tuple-queue path is audited in
`audits/backend-executor-execParallel.md` §`tqueue-substrate-check`.)

- **New runtime test (real DSM).** `tuple_queue_roundtrip_over_real_dsm`
  (`dsm_substrate_tests`, feature `test-bringup`) drives the verbatim
  `ExecParallelSetupTupleQueues` loop (`shm_toc_allocate(mul_size(SIZE,
  nworkers))` → per-worker `shm_mq_create_at` + `shm_mq_set_receiver_to_myproc` +
  `shm_mq_attach(mq, Some(seg))`) and the `ExecParallelGetReceiver` worker cast
  (`shm_mq_at` + `shm_mq_set_sender_to_myproc` + `shm_mq_attach(mq, Some(seg))`)
  over a real `dsm-core` segment, then round-trips a tuple frame worker→leader
  through the real `shm_mq` ring. `shm_mq_attach`'s seam impl threads the real
  `DsmSegmentId` (`seg_id_of`), `top_memory_context()` (`Mcx<'static>`) and
  `my_latch()` (`MyLatch`) into `real_attach`.

- **No emulation anywhere in the lineage.** Verified by grep:
  `dsm_create_null_if_maxsegments` exists only as a "retired/gone" comment in
  `parallel-rt-seams`; every `emulat`/`side table`/`Vec<u8>` mention in
  `parallel`/`execParallel`/`types-execparallel` is a comment asserting the
  feature is *retired*; the `thread_local!`s that remain (`G` parallel registry,
  `WORKER_ATTACHED`) hold the real `ParallelContext`/`DsmSegment`+`ShmToc`, not
  emulated chunk payloads; the dsm/shm_mq/shm_toc rt-seams are retired (real
  `dsm_create`/`dsm_attach`/`ShmToc`/OPTION-(i) `shm-mq-seams` used directly).
  No new DESIGN_DEBT in the parallel/execParallel lineage.

**Build/test:** `cargo check --workspace` clean; `cargo test
-p backend-access-transam-parallel` 6/6 (incl. the new tuple-queue test);
`cargo test --workspace` green (recurrence_guard incl. declared-seams-are-set).
