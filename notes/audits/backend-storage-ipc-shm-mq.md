# Audit: backend-storage-ipc-shm-mq

- **Unit:** `backend-storage-ipc-shm-mq` (`src/backend/storage/ipc/shm_mq.c`)
- **Crates:** `crates/backend-storage-ipc-shm-mq` (logic),
  `crates/backend-postmaster-bgworker-seams` + `crates/types-bgworker` (new
  vocabulary/seam crates landed with the port)
- **C source:** `../pgrust/postgres-18.3/src/backend/storage/ipc/shm_mq.c`
  (1352 lines)
- **c2rust:** `../pgrust/c2rust-runs/backend-storage-ipc-shm-mq/src/shm_mq.rs`
- **Audited at:** branch `port/backend-storage-ipc-shm-mq`, fix commit
  `74ebd82` on top of port commit `ab32b92`
- **Auditor basis:** independent inventory from the C file, cross-checked
  against the c2rust rendering (function counts match: 21 shm_mq functions;
  the three `c2rust_pg_*_barrier` shims are transpile artifacts standing in
  for the barrier macros).

## Function inventory and verdicts

| # | C function (shm_mq.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `shm_mq_create` (201) | `shm_mq_create` (392) | MATCH | MAXALIGN_DOWN of size; Assert(size > data_offset) → debug_assert; header init field-for-field (SpinLockInit → `Spinlock::new`, counters 0, detached false, `mq_ring_offset = data_offset - offsetof(mq_ring)`); PGPROC* NULL → `INVALID_PROC_NUMBER`. |
| 2 | `shm_mq_set_receiver` (230) | (422) | MATCH | store under spinlock, read sender under same lock, SetLatch(sender) after release iff non-NULL. RAII guard dropped before the latch call, matching C release point. |
| 3 | `shm_mq_set_sender` (248) | (436) | MATCH | symmetric to receiver. |
| 4 | `shm_mq_get_receiver` (266) | (450) | MATCH | read under spinlock; NULL → `None`. |
| 5 | `shm_mq_get_sender` (281) | (458) | MATCH | symmetric. |
| 6 | `shm_mq_attach` (314) | (477) | MATCH | Assert(receiver==MyProc \|\| sender==MyProc) → debug_assert via `my_proc_number()` seam; all 12 handle fields initialized identically; `mqh_context = CurrentMemoryContext` → caller-passed `Mcx`; ambient `MyLatch` → explicit `my_latch` parameter; `on_dsm_detach(seg, shm_mq_detach_callback, PointerGetDatum(mq))` iff seg present (direct dep on dsm-core; the extra `Mcx<'static>` is dsm-core's callback-record allocation context, its OOM the `Err`). |
| 7 | `shm_mq_set_handle` (343) | (518) | MATCH | Assert(handle == NULL) → debug_assert is_none. |
| 8 | `shm_mq_send` (353) | (533) | MATCH | one-element iovec wrapper over sendv. |
| 9 | `shm_mq_sendv` (385) | (556) | MATCH (after fix 74ebd82) | Size sum; `nbytes > MaxAllocSize` → ereport ERROR 54000 "cannot send a message of size %zu via shared memory queue"; length-word loop with DETACHED state reset, partial accumulation, completion at sizeof(Size); data do/while: iov-advance / tmpbuf-combine / chunk branches with the controlling expression `mqh_partial_bytes < nbytes` evaluated after **every** branch (see Findings #1 — the original port's tmpbuf `continue` skipped it); post-loop reset, detached check, receiver read with/without spinlock per `mqh_counterparty_attached`, flush when `force_flush \|\| send_pending > ring_size >> 2` (inc_bytes_written + SetLatch(receiver) + reset). |
| 10 | `shm_mq_receive` (596) | (756) | MATCH | counterparty-attach gate (nowait: `counterparty_gone` computed **before** `get_sender` to preserve the documented race ordering; blocking: `wait_internal(&mq_sender)` else mark detached); consume-pending flush at `> ring_size / 4`; length-word loop incl. split-word reassembly path (MQH_INITIAL_BUFSIZE alloc, lengthbytes clamp, MAXALIGN consume accounting); `nbytes > MaxAllocSize` → ereport ERROR 54000 "invalid message size %zu in shared memory queue"; single-chunk fast path returning a shmem-backed slice; buffer growth `Min(pg_nextpower2_size_t(nbytes), MaxAllocSize)` with pfree-then-alloc order preserved; wrap-copy loop with `rb = min(rb, still_needed)` clamp and `MAXALIGN(rb)` consume accounting; out-params `*nbytesp`/`*datap` → returned `(result, &[u8])`. |
| 11 | `shm_mq_wait_for_attach` (844) | (982) | MATCH | victim = sender if I'm receiver else (assert sender==me) receiver; wait_internal → SUCCESS/DETACHED. |
| 12 | `shm_mq_detach` (867) | (1006) | MATCH | flush send_pending via inc_bytes_written first; detach_internal; cancel_on_dsm_detach(seg, callback, PointerGetDatum(mq)) iff segment; the C pfrees of mqh_buffer and the handle are the drop of the consumed `ShmMqHandle`. |
| 13 | `shm_mq_detach_internal` (906, static) | (1038) | MATCH | victim chosen under spinlock (receiver if I'm sender, else assert + sender), `mq_detached = true` under lock, SetLatch(victim) after release iff non-NULL. |
| 14 | `shm_mq_get_queue` (929) | (524) | MATCH | accessor. |
| 15 | `shm_mq_send_bytes` (938, static) | (1062) | MATCH | per-iteration rb/wb (+send_pending) reads, used/available math identical (64-bit usize == C Size/uint64 arithmetic); `pg_compiler_barrier()` → `compiler_fence(SeqCst)` before the detached check; not-attached-and-full branch (nowait: counterparty_gone → DETACHED, receiver NULL → WOULD_BLOCK; blocking: wait_internal else mark detached); full branch (inc_bytes_written(send_pending), SetLatch(mq_receiver) read lock-free — C NULL-derefs there, port `expect`-panics, same can't-happen contract —, send_pending=0, nowait → WOULD_BLOCK, else WaitLatch(MyLatch, WL_LATCH_SET\|WL_EXIT_ON_PM_DEATH, 0, WAIT_EVENT_MESSAGE_QUEUE_SEND) + ResetLatch + CHECK_FOR_INTERRUPTS); copy branch (`offset = wb % ringsize`, `sendnow = Min(available, ringsize-offset)`, `pg_memory_barrier()` → `fence(SeqCst)` before the ring memcpy, `send_pending += MAXALIGN(sendnow)`); `*bytes_written` out-param → returned tuple. |
| 16 | `shm_mq_receive_bytes` (1103, static) | (1185) | MATCH | written/read(+consume_pending) reads; done test `used >= bytes_needed \|\| offset + used >= ringsize` with `*nbytesp = Min(used, ringsize-offset)` and `pg_read_barrier()` → `fence(Acquire)` before returning the data pointer; detached path re-reads mq_bytes_written after a read barrier and `continue`s if it advanced; consume_pending flush; nowait → WOULD_BLOCK; WaitLatch(..., WAIT_EVENT_MESSAGE_QUEUE_RECEIVE) + ResetLatch + CHECK_FOR_INTERRUPTS. |
| 17 | `shm_mq_counterparty_gone` (1203, static) | (1262) | MATCH | detached → true; handle → GetBackgroundWorkerPid status not in {STARTED, NOT_YET_STARTED} → set detached + true; else false. |
| 18 | `shm_mq_wait_internal` (1242, static) | (1297) | MATCH | per-iteration: pointer checked under spinlock (the `PGPROC **ptr` arg becomes the `WaitTarget` enum naming the same two fields); detached → false (checked before the success break, same order); handle status check; WaitLatch(..., WAIT_EVENT_MESSAGE_QUEUE_INTERNAL) + ResetLatch + CHECK_FOR_INTERRUPTS. |
| 19 | `shm_mq_inc_bytes_read` (1294, static) | (1352) | MATCH | `pg_read_barrier()` → `fence(Acquire)` before the read-then-store (deliberately not fetch_add, as in C); sender read lock-free, Assert(non-NULL) → expect, SetLatch(sender). |
| 20 | `shm_mq_inc_bytes_written` (1327, static) | (1379) | MATCH | `pg_write_barrier()` → `fence(Release)` before read-then-store. |
| 21 | `shm_mq_detach_callback` (1347, static) | (1398) | MATCH | DatumGetPointer(arg) → queue base; detach_internal only (never touches the local handle, per the C comment). |

File-scope data: `shm_mq_minimum_size = MAXALIGN(offsetof(shm_mq, mq_ring)) +
MAXIMUM_ALIGNOF` → `shm_mq_minimum_size()` const fn over
`offset_of!(InSegmentShmMq, mq_ring)` — MATCH (computed from the port's own
repr(C) layout, which is the layout actually placed in segments on both
sides). `MQH_INITIAL_BUFSIZE = 8192` — MATCH.

## Constants verified against headers / c2rust

- `ERRCODE_PROGRAM_LIMIT_EXCEEDED` = `54000` (errcodes; c2rust MAKE_SQLSTATE
  expansion for '5','4','0','0','0') — both ereports use it with ERROR
  severity and the exact C format strings.
- `MaxAllocSize` = `0x3FFFFFFF` (`mcx::MAX_ALLOC_SIZE`, memutils.h).
- `WL_LATCH_SET` = 1<<0, `WL_EXIT_ON_PM_DEATH` = 1<<5
  (storage/waiteventset.h).
- `WAIT_EVENT_MESSAGE_QUEUE_INTERNAL/RECEIVE/SEND` = 134217761 / 134217763 /
  134217764 = `PG_WAIT_IPC | 33/35/36` — match c2rust's generated values
  (the gap at |34 is MESSAGE_QUEUE_PUT_MESSAGE, unused here).
- `BgwHandleStatus` Started/NotYetStarted/Stopped/PostmasterDied = 0..3
  (postmaster/bgworker.h enum order).
- `MAXIMUM_ALIGNOF` = 8, `MAXALIGN`/`MAXALIGN_DOWN` standard 8-byte forms.
- `pg_nextpower2_size_t` semantics match pg_bitutils.h (num==1 → 1, else
  next power of two), unit-tested.
- Barrier mapping: `pg_memory_barrier` → `fence(SeqCst)`, `pg_read_barrier` →
  `fence(Acquire)`, `pg_write_barrier` → `fence(Release)`,
  `pg_compiler_barrier` → `compiler_fence(SeqCst)`; each use site pairs as
  the C comments describe.

## Representation decisions (behavior-preserving)

- `shm_mq` header lives in-segment as `#[repr(C)] InSegmentShmMq` with the
  real `slock_t` (`backend-storage-lmgr-s-lock::Spinlock`), `AtomicU64`
  counters (relaxed loads/stores + explicit fences = C
  pg_atomic_read/write_u64 + barrier macros), `AtomicBool` `mq_detached`.
  `PGPROC *` identities are `ProcNumber` in an `AtomicI32`
  (`INVALID_PROC_NUMBER` = NULL); waking a peer goes
  `proc_latch(procno)` → `set_latch(handle)` = C
  `SetLatch(&proc->procLatch)`.
- `shm_mq_handle` → backend-private `ShmMqHandle<'mcx>`; `mqh_buffer` +
  `mqh_buflen` collapse into `Option<PgVec<'mcx, u8>>` (len == buflen at all
  times: `alloc_buffer` resizes to capacity); allocation failures surface as
  the mcx OOM `PgResult`, matching `MemoryContextAlloc` ereport behavior.
- Ambient C globals threaded explicitly: `MyLatch` → `my_latch` captured at
  attach; `MyProc` identity → `my_proc_number()` seam; no shared statics.
- Out-params (`*bytes_written`, `*nbytesp`, `*datap`) → returned tuples;
  `shm_mq_receive`'s `*datap` becomes a `&[u8]` borrowing the handle, which
  enforces exactly the C lifetime contract ("valid until the next receive").

## Seam and wiring audit

- **Outward calls** (all thin marshal + delegate; no logic in seam paths):
  - `backend-storage-ipc-latch-seams`: existing `reset_latch`; new
    `set_latch`, `wait_latch` (latch.c is unported; declaration signatures
    verified against latch.c — `WaitLatch` returns the event mask, can
    ereport, hence `PgResult<i32>`).
  - `backend-storage-lmgr-proc-seams`: new `proc_latch(ProcNumber) ->
    LatchHandle` = `&GetPGProcByNumber(n)->procLatch` (proc.c unported; pure
    lookup, infallible — correct).
  - `backend-postmaster-bgworker-seams` (new crate): `
    get_background_worker_pid(handle) -> (BgwHandleStatus, i32)` — matches
    the C signature (status + out-param pid); infallible as in C.
  - `backend-utils-init-small-seams::my_proc_number`,
    `backend-tcop-postgres-seams::check_for_interrupts` — existing seams,
    correct owners.
  - `on_dsm_detach`/`cancel_on_dsm_detach`: **direct dependency** on
    `backend-storage-ipc-dsm-core` (already ported; no cycle — dsm-core does
    not depend on shm-mq). Correct: no seam invented where a direct dep
    works.
- **Inward seams:** none declared (no cyclic consumer yet). `init_seams()`
  is an empty placeholder and `seams-init::init_all()` calls it — uniform
  coverage, nothing uninstalled. No `set()` calls outside the owner (test
  harness installs *other* owners' seams under `#[cfg(test)]`, which is the
  established pattern).
- **types-bgworker** (new): `BgwHandleStatus` (values verified) and
  `BackgroundWorkerHandle{slot: i32, generation: u64}` — the real bgworker.c
  struct fields, no invented opacity (types.md 6-7 ok).
- Design rules: allocating path takes `Mcx` + returns `PgResult` (3b ok);
  spinlock held via RAII guard and never across `?` or a seam call; no
  registry side tables; no ambient-global seams; per-backend state lives in
  the handle.

## Findings and resolutions

1. **DIVERGES (fixed in `74ebd82`)** — `shm_mq_sendv` data loop. The C loop
   is a do/while; `continue` jumps to the controlling expression
   `mqh_partial_bytes < nbytes` (c2rust renders all three branches falling
   through to a single bottom-of-loop check). The original port used Rust
   `continue` after the tmpbuf branch, which skipped the bottom check and
   re-evaluated `iov[which_iov]` at the top. When the tmpbuf inner loop
   exhausts the iovec array (`which_iov == iovcnt`; e.g. iov =
   `[4-byte chunk, zero-length chunk]`), C exits the loop and returns
   SHM_MQ_SUCCESS while the port panicked on an out-of-bounds index. Fixed
   by restructuring to the exact c2rust if/else-if/else shape with the
   single bottom condition; regression test
   `sendv_with_trailing_zero_length_iovec` fails on the pre-fix code and
   passes after. Re-audited the rewritten function from scratch against the
   C and c2rust: branch order, the first-branch early `break`, both
   DETACHED reset orderings (partial-then-flag vs flag-then-partial — kept
   per-branch as in C), and the bottom check all match.

No other findings. Spot-checks re-derived in full: `shm_mq_receive`
(including the split-length-word path and the wrap-copy loop),
`shm_mq_send_bytes`, `shm_mq_receive_bytes` (detached re-read loop),
`shm_mq_wait_internal` (detached-before-success ordering).

## Build / test

- `cargo check -p backend-storage-ipc-shm-mq -p seams-init`: clean.
- `cargo test -p backend-storage-ipc-shm-mq`: 14/14 pass (including the new
  regression test).
- `cargo clippy` fails in unrelated pre-existing crate
  `backend-storage-lmgr-lwlock` (present before this port; verified by
  stashing the changes).

## Verdict

**PASS** (after one fix round). All 21 functions MATCH; seams justified,
thin, and wired; constants verified against headers and c2rust.

---

## Family `shm-mq-leader` re-audit (seam layer install, OPTION (i))

Family C1 installs `backend-storage-ipc-shm-mq`'s `init_seams()` for the first
time: the OPTION (i) `PgBox<ShmMqHandle>`-registry-backed seam layer
(`backend-storage-ipc-shm-mq-seams`) that the parallel orchestration
(`access/transam/parallel.c` error queues) and the parallel executor
(`execParallel.c` tuple queues) consume. No `shm_mq.c` *logic* changed; the
21-function audit above still holds. New surface audited:

- **Registry (`seam_layer::Registry`)** — the backend-private `shm_mq_handle *`
  is the owned `PgBox<'static, ShmMqHandle<'static>>` parked here, named across
  the seam by a 1-based `ShmMqAttachHandle` id (`0` == C NULL). `shm_mq_detach`
  takes the box out (frees the slot) and consumes it, so the box `Drop` owns the
  pfree of the handle + its `mqh_buffer` (OPTION (i)); the `on_dsm_detach`
  callback registered in `shm_mq_attach` is flags-only (`shm_mq_detach_callback`
  -> `shm_mq_detach_internal`), exactly C.
- **Token bridges** — `ShmMqHandle`(token).0 IS the real in-segment base
  (`shm_toc` chunk address); `shm_mq_create_at(chunk, i, size)` is
  `shm_mq_create(chunk + i*size, size)` over that real address.
  `DsmSegmentHandle`(exec).0 IS the real `DsmSegmentId` (opacity-inherited);
  `shm_mq_attach` threads it + `my_latch()` + `TopMemoryContext` into the real
  `shm_mq_attach`, matching C `shm_mq_attach(mq, seg, NULL)` with the ambient
  `MyLatch`/`CurrentMemoryContext`.
- **`shm_mq_attach` fallibility** — now returns `PgResult` (C `palloc` of the
  handle + `on_dsm_detach` registration can `ereport`). `shm_mq_receive` returns
  `PgResult` (oversized-message `ereport`, interrupt processing); the others are
  infallible in C. execParallel's two `shm_mq_attach` call sites updated with
  `?` (both in `PgResult` contexts); no tqueue reshape (deferred family).
- **`shm_mq_set_handle` bgwhandle bridge** — the seam takes the execParallel
  `BackgroundWorkerHandle` id; the impl resolves it to the real
  `types_bgworker::BackgroundWorkerHandle` via the bgworker-owned
  `background_worker_handle_from_token` seam (seam-and-panic until the bgworker
  registration path lands — sibling-spine dep, consistent with the rest of the
  bgworker integration which is uniformly unported here).

### Leader error-queue paths (`parallel.c`) — converted, re-audited

`initialize_parallel_dsm` / `reinitialize_parallel_dsm` (create+set_receiver+
attach loop), `launch_parallel_workers` (set_handle / detach-on-failure),
`wait_for_parallel_workers_to_attach` + `wait_for_parallel_workers_to_finish`
(get_queue / get_sender NULL test), `process_parallel_messages` (nowait
receive) and `process_parallel_message` (Terminate detach),
`destroy_parallel_context` (terminate+detach) now drive the real `shm-mq` over
real chunk addresses through these seams; `error_mqh` is the `ShmMqAttachHandle`
id (`ERROR_MQH_NULL == 0`). The worker error queue in `parallel_worker_main`
(create_at + set_sender + attach over the real `seg`/chunk) is converted the
same way. The retired `rt-seams` `shm_mq_*` decls are gone. All match the C line
for line (`parallel.c:475-477,548-550,630,647-648,741-742,755-756,874-875,
974-979,1105-1111,1241-1242,1380-1381`).

### Build / test (family C1)

- `cargo check --workspace`: clean.
- `cargo test -p backend-storage-ipc-shm-mq`: 14/14.
- `cargo test -p backend-access-transam-parallel`: 4/4 (real-DSM bringup).
- `cargo test -p seams-init` recurrence_guard: pass.

### Verdict

**PASS** — seam layer faithful to OPTION (i); no `shm_mq.c` logic change; leader
+ worker error-queue call sites real over real chunk addresses; no stubs, no
emulation tokens. The only deferred edges are the bgworker-handle resolution
(`set_handle`) and the execParallel tqueue reshape — both owned elsewhere.
